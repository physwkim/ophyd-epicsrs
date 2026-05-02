"""ophyd-async SignalBackend implementation backed by epics-rs.

A single ``EpicsRsSignalBackend`` class handles both CA and PVA — chosen
via the ``protocol`` constructor arg. Internally it dispatches to either
:class:`EpicsRsPV` (CA) or :class:`EpicsRsPvaPV` (PVA) and uses their
``*_async`` methods so everything runs on the shared tokio runtime.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum
from typing import Any

from bluesky.protocols import Reading
from event_model import DataKey
from ophyd_async.core import (
    Callback,
    NotConnectedError,
    SignalDatatypeT,
)
from ophyd_async.epics.core._util import EpicsOptions, EpicsSignalBackend

_logger = logging.getLogger(__name__)

from ophyd_epicsrs._native import (
    EpicsRsContext,
    EpicsRsPvaContext,
)

from ._converter import Converter, _is_typed_pvfield_payload, make_converter


class EpicsRsProtocol(Enum):
    """Protocol selector for :class:`EpicsRsSignalBackend`."""

    CA = "ca"
    PVA = "pva"


# Module-level lazy singletons. Both share the same Rust shared_runtime,
# so the second one to construct does NOT spin up a separate executor.
_ca_context: EpicsRsContext | None = None
_pva_context: EpicsRsPvaContext | None = None


def _get_ca_context() -> EpicsRsContext:
    global _ca_context
    if _ca_context is None:
        _ca_context = EpicsRsContext()
    return _ca_context


def _get_pva_context() -> EpicsRsPvaContext:
    global _pva_context
    if _pva_context is None:
        _pva_context = EpicsRsPvaContext()
    return _pva_context


class EpicsRsSignalBackend(EpicsSignalBackend[SignalDatatypeT]):
    """ophyd-async backend over epics-rs (CA + PVA)."""

    def __init__(
        self,
        datatype: type[SignalDatatypeT] | None,
        read_pv: str = "",
        write_pv: str = "",
        options: EpicsOptions | None = None,
        protocol: EpicsRsProtocol = EpicsRsProtocol.CA,
    ):
        self.protocol = protocol
        self._read_pv_native = self._make_native_pv(read_pv)
        if write_pv == read_pv:
            self._write_pv_native = self._read_pv_native
        else:
            self._write_pv_native = self._make_native_pv(write_pv)
        self._monitor_callback: Callback | None = None
        self._converter: Converter = make_converter(datatype)
        super().__init__(datatype, read_pv, write_pv, options)

    def _make_native_pv(self, pv_name: str):
        if self.protocol is EpicsRsProtocol.PVA:
            return _get_pva_context().create_pv(pv_name)
        return _get_ca_context().create_pv(pv_name)

    # ----- SignalBackend ABC -----

    def source(self, name: str, read: bool) -> str:
        scheme = self.protocol.value
        pv = self.read_pv if read else self.write_pv
        return f"{scheme}://{pv}"

    async def connect(self, timeout: float):
        # Wrap the entire connect sequence in asyncio.wait_for so the
        # user-supplied timeout becomes the absolute wall-clock budget,
        # not a per-phase budget. Without this, four sequential phases
        # (connect, cache native_type, schema fetch, initial metadata)
        # each capped at `timeout` would let connect take up to 4×timeout.
        await asyncio.wait_for(self._connect_inner(timeout), timeout=timeout)

    async def _connect_inner(self, timeout: float) -> None:
        if self.read_pv != self.write_pv:
            ok_r, ok_w = await asyncio.gather(
                self._read_pv_native.connect_async(timeout),
                self._write_pv_native.connect_async(timeout),
            )
        else:
            ok_r = await self._read_pv_native.connect_async(timeout)
            ok_w = ok_r
        if not (ok_r and ok_w):
            raise NotConnectedError(self.source("", read=True))

        # Populate native_type via channel.info() (no CA read) for both
        # the read AND write PVs. Calling on both ensures each prefetch
        # task started by EpicsRsPV::new is consumed and that put paths
        # skip the info() round trip. PVA's cache_native_type_async is
        # a no-op (string-form pvput needs no cache) so safe to call.
        # Per-phase cap is small because the outer wait_for is the
        # real budget — these are best-effort fast-paths.
        phase_to = min(timeout, 2.0)
        if self.read_pv != self.write_pv:
            await asyncio.gather(
                self._read_pv_native.cache_native_type_async(timeout=phase_to),
                self._write_pv_native.cache_native_type_async(timeout=phase_to),
            )
        else:
            await self._write_pv_native.cache_native_type_async(timeout=phase_to)

        # Schema validation — for converters that declare typed columns
        # (currently only _TableConverter), fetch the IOC schema and
        # check column names + dtypes match.
        if hasattr(self._converter, "validate_against_schema"):
            try:
                schema = await self._read_pv_native.get_field_desc_async(
                    timeout=phase_to
                )
            except Exception:  # noqa: BLE001 — transient I/O
                schema = None
            if schema is not None:
                self._converter.validate_against_schema(
                    schema, source=self.source("", read=True)
                )

        # Pull initial metadata so the converter can cache enum_strs,
        # Table column types, etc.
        try:
            md = await self._read_pv_native.get_reading_async(
                timeout=phase_to, form="ctrl"
            )
        except Exception:  # noqa: BLE001 — transient I/O, retry on next get
            md = None
        if md is not None:
            self._converter.update_metadata(md, source=self.source("", read=True))

    async def put(self, value: SignalDatatypeT | None):
        if value is None:
            raw = await self._read_pv_native.get_value_async()
            wire = self._converter.to_wire(self._converter.to_python(raw))
        else:
            wire = self._converter.to_wire(value)

        # Honor EpicsOptions.wait — bool or callable.  When False, we use
        # put_nowait_async (CA fire-and-forget / PVA spawn) so busy-record
        # / acquire PVs don't deadlock waiting for write_notify ack.
        wait_opt = self.options.wait
        if callable(wait_opt):
            wait = bool(wait_opt(value))
        else:
            wait = bool(wait_opt)

        # Typed-PvField payload (e.g. Table → NTTable wire structure):
        # _TableConverter.to_wire produces a marker dict carrying both
        # the column data and per-column dtype hints. Route to the
        # typed pvput_pv_field path on PVA — it builds a properly-typed
        # PvStructure even from empty columns. The Rust put methods
        # raise PyRuntimeError / PyTimeoutError on failure (no more
        # silent bool result), so we don't need a separate `if not ok`
        # gate here — exceptions propagate to the user verbatim.
        if _is_typed_pvfield_payload(wire):
            data = wire["data"]
            dtypes = wire.get("dtypes") or None
            struct_id = wire.get("struct_id", "")
            if not hasattr(self._write_pv_native, "put_pv_field_async"):
                raise RuntimeError(
                    "Typed-PvField writes require the PVA backend; "
                    f"{self.source('', read=False)} is not PVA-backed"
                )
            if wait:
                await self._write_pv_native.put_pv_field_async(data, dtypes, struct_id)
            else:
                await self._write_pv_native.put_pv_field_nowait_async(
                    data, dtypes, struct_id
                )
        elif wait:
            await self._write_pv_native.put_async(wire)
        else:
            await self._write_pv_native.put_nowait_async(wire)

    # Generous backend-level timeout — asyncio.wait_for at the Signal
    # layer is the user-facing gate. We pass a long timeout here so the
    # Rust call does NOT silently return None / time out under the
    # asyncio wait_for; either the IOC responds, asyncio cancels, or
    # the backend raises (now never returns None).
    _GET_TIMEOUT = 60.0

    async def get_value(self) -> SignalDatatypeT:
        raw = await self._read_pv_native.get_value_async(timeout=self._GET_TIMEOUT)
        return self._converter.to_python(raw)

    async def get_setpoint(self) -> SignalDatatypeT:
        raw = await self._write_pv_native.get_value_async(timeout=self._GET_TIMEOUT)
        return self._converter.to_python(raw)

    async def get_reading(self) -> Reading[SignalDatatypeT]:
        md = await self._read_pv_native.get_reading_async(
            timeout=self._GET_TIMEOUT, form="time"
        )
        severity = md.get("severity", 0)
        return {
            "value": self._converter.to_python(md["value"], md),
            "timestamp": md.get("timestamp", 0.0),
            "alarm_severity": -1 if severity > 2 else severity,
        }

    async def get_datakey(self, source: str) -> DataKey:
        md = await self._read_pv_native.get_reading_async(
            timeout=self._GET_TIMEOUT, form="ctrl"
        )
        # Apply the converter so dtype reflects the user-requested type
        # (e.g. requesting `int` on a DBR_DOUBLE PV records as integer).
        typed_value = self._converter.to_python(md["value"], md)
        dtype, shape, dtype_numpy = self._converter.datakey_dtype(typed_value)
        datakey: DataKey = {
            "source": source,
            "dtype": dtype,
            "shape": shape,
            "dtype_numpy": dtype_numpy,
        }
        if "units" in md:
            datakey["units"] = md["units"]
        if "precision" in md:
            datakey["precision"] = md["precision"]
        if "enum_strs" in md:
            datakey["choices"] = list(md["enum_strs"])
        limits: dict[str, dict[str, Any]] = {}
        for source_lo, source_hi, target in (
            ("lower_disp_limit", "upper_disp_limit", "display"),
            ("lower_warning_limit", "upper_warning_limit", "warning"),
            ("lower_alarm_limit", "upper_alarm_limit", "alarm"),
            ("lower_ctrl_limit", "upper_ctrl_limit", "control"),
        ):
            if source_lo in md or source_hi in md:
                limits[target] = {
                    "low": md.get(source_lo, 0.0),
                    "high": md.get(source_hi, 0.0),
                }
        if limits:
            datakey["limits"] = limits  # type: ignore[typeddict-unknown-key]
        return datakey

    def set_callback(
        self, callback: Callback[Reading[SignalDatatypeT]] | None
    ) -> None:
        if callback is None:
            self._read_pv_native.clear_monitors()
            self._monitor_callback = None
            return
        if self._monitor_callback is not None:
            raise RuntimeError(
                f"Cannot set a callback on {self.source('', read=True)} when one is already set"
            )
        self._monitor_callback = callback

        converter = self._converter
        # Capture the asyncio event loop at registration time. The Rust
        # monitor dispatch fires _wrapped from a Rust-owned OS thread, but
        # ophyd-async's Signal cache callback touches asyncio.Event /
        # asyncio.Queue, which are not thread-safe. Bridge via
        # call_soon_threadsafe so the user callback runs on the loop thread.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        # First-detection guard: when the captured loop closes mid-session
        # (typical: asyncio.run() exits while a monitor is still active),
        # the dispatch thread keeps receiving events at the IOC's monitor
        # rate. Calling the user callback would touch asyncio.Event /
        # Queue inside ophyd-async's cache without a running loop — UB.
        # Drop further events, warn ONCE, and proactively clear the
        # underlying monitor so the IOC stops streaming to us at all.
        loop_closed_handled = [False]
        native_pv = self._read_pv_native
        source = self.source("", read=True)

        def _wrapped(**kwargs):
            severity = kwargs.get("severity", 0)
            reading: Reading = {
                "value": converter.to_python(kwargs.get("value"), kwargs),
                "timestamp": kwargs.get("timestamp", 0.0),
                "alarm_severity": -1 if severity > 2 else severity,
            }
            if loop is not None and not loop.is_closed():
                loop.call_soon_threadsafe(callback, reading)
            elif loop is None:
                # set_callback was called from a sync context — caller
                # accepts that the callback runs on the Rust dispatch
                # thread. Best-effort direct call.
                try:
                    callback(reading)
                except Exception:  # noqa: BLE001
                    _logger.exception(
                        "monitor callback for %s raised", source
                    )
            elif not loop_closed_handled[0]:
                loop_closed_handled[0] = True
                _logger.debug(
                    "monitor event for %s arrived after the registered "
                    "asyncio loop closed — clearing the subscription "
                    "and dropping further events",
                    source,
                )
                # Tear down the subscription on the Rust side so the
                # IOC stops streaming events to a dead loop. Best-effort:
                # clear_monitors itself can race with this very callback.
                try:
                    native_pv.clear_monitors()
                except Exception:  # noqa: BLE001
                    pass
            # else: silently drop — handler already cleaned up.

        self._read_pv_native.add_monitor_callback(_wrapped)
