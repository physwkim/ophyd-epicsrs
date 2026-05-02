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

        # Populate native_type for the WRITE PV via channel.info() (a
        # coordinator query — no CA read).  Without this, the first
        # put_nowait_async call would do a 5s blocking pre-read to
        # discover the DbFieldType, defeating wait=False semantics on
        # busy records and failing entirely on write-only PVs.  PVA's
        # implementation is a no-op (string-form pvput needs no cache).
        await self._write_pv_native.cache_native_type_async(
            timeout=min(timeout, 2.0)
        )

        # Schema validation — for converters that declare typed columns
        # (currently only _TableConverter), fetch the IOC schema and
        # check column names + dtypes match. Mismatch raises TypeError
        # at connect time so the user gets an immediate, clear error
        # rather than a confusing server-side reject when they later put.
        if hasattr(self._converter, "validate_against_schema"):
            try:
                schema = await self._read_pv_native.get_field_desc_async(
                    timeout=min(timeout, 2.0)
                )
            except Exception:  # noqa: BLE001 — transient I/O
                schema = None
            if schema is not None:
                # propagate TypeError (schema mismatch is a user-side
                # declaration error and connect should fail fast)
                self._converter.validate_against_schema(
                    schema, source=self.source("", read=True)
                )

        # Pull initial metadata so the converter can cache enum_strs,
        # Table column types, etc. Transient I/O errors here are OK
        # (we just degrade to runtime fetch later), but if metadata IS
        # received and the converter rejects it (e.g. StrictEnum choices
        # mismatch) that TypeError propagates as a connect failure —
        # caller is asking for a typed signal that does not match the IOC.
        try:
            md = await self._read_pv_native.get_reading_async(
                timeout=min(timeout, 2.0), form="ctrl"
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
        # PvStructure even from empty columns.
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
                ok = await self._write_pv_native.put_pv_field_async(
                    data, dtypes, struct_id
                )
            else:
                ok = await self._write_pv_native.put_pv_field_nowait_async(
                    data, dtypes, struct_id
                )
        elif wait:
            ok = await self._write_pv_native.put_async(wire)
        else:
            ok = await self._write_pv_native.put_nowait_async(wire)
        if not ok:
            raise RuntimeError(f"put to {self.source('', read=False)} failed")

    async def get_value(self) -> SignalDatatypeT:
        raw = await self._read_pv_native.get_value_async()
        return self._converter.to_python(raw)

    async def get_setpoint(self) -> SignalDatatypeT:
        raw = await self._write_pv_native.get_value_async()
        return self._converter.to_python(raw)

    async def get_reading(self) -> Reading[SignalDatatypeT]:
        md = await self._read_pv_native.get_reading_async(form="time")
        if md is None:
            raise RuntimeError(f"could not read {self.source('', read=True)}")
        severity = md.get("severity", 0)
        return {
            "value": self._converter.to_python(md["value"], md),
            "timestamp": md.get("timestamp", 0.0),
            "alarm_severity": -1 if severity > 2 else severity,
        }

    async def get_datakey(self, source: str) -> DataKey:
        md = await self._read_pv_native.get_reading_async(form="ctrl")
        if md is None:
            raise RuntimeError(f"could not read datakey for {source}")
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
            # No running loop: caller is using this backend outside an
            # asyncio context. Fall back to direct invocation; if the
            # callback later schedules onto a loop it must do so itself.
            loop = None

        def _wrapped(**kwargs):
            severity = kwargs.get("severity", 0)
            reading: Reading = {
                "value": converter.to_python(kwargs.get("value"), kwargs),
                "timestamp": kwargs.get("timestamp", 0.0),
                "alarm_severity": -1 if severity > 2 else severity,
            }
            if loop is not None and not loop.is_closed():
                loop.call_soon_threadsafe(callback, reading)
            else:
                # Best-effort direct call (no loop captured / loop closed)
                try:
                    callback(reading)
                except Exception:  # noqa: BLE001
                    _logger.exception(
                        "monitor callback for %s raised", self.source("", read=True)
                    )

        self._read_pv_native.add_monitor_callback(_wrapped)
