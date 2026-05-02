"""ophyd-async SignalBackend implementation backed by epics-rs.

A single ``EpicsRsSignalBackend`` class handles both CA and PVA — chosen
via the ``protocol`` constructor arg. Internally it dispatches to either
:class:`EpicsRsPV` (CA) or :class:`EpicsRsPvaPV` (PVA) and uses their
``*_async`` methods so everything runs on the shared tokio runtime.
"""

from __future__ import annotations

import asyncio
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

from ophyd_epicsrs._native import (
    EpicsRsContext,
    EpicsRsPvaContext,
)

from ._converter import Converter, make_converter


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
        ok = await self._write_pv_native.put_async(wire)
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

        def _wrapped(**kwargs):
            severity = kwargs.get("severity", 0)
            reading: Reading = {
                "value": converter.to_python(kwargs.get("value"), kwargs),
                "timestamp": kwargs.get("timestamp", 0.0),
                "alarm_severity": -1 if severity > 2 else severity,
            }
            callback(reading)

        self._read_pv_native.add_monitor_callback(_wrapped)
