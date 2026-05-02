"""ophyd-async backend powered by epics-rs (Rust CA + PVA).

This subpackage exposes ``EpicsRsSignalBackend`` — a `SignalBackend[T]`
implementation that adapts the async ``EpicsRsPV`` / ``EpicsRsPvaPV``
methods to ophyd-async's :class:`~ophyd_async.core.Signal` interface.

Use the factory functions instead of constructing the backend directly:

    from ophyd_epicsrs.detector import epicsrs_signal_rw

    sig = epicsrs_signal_rw(float, "IOC:motor.RBV", "IOC:motor.VAL")
    # or pva://...
    sig = epicsrs_signal_rw(float, "pva://IOC:nt:scalar")

The factories return standard ``SignalRW`` / ``SignalR`` / ``SignalW`` /
``SignalX`` instances, so they drop straight into ophyd-async's
``StandardDetector``, ``StandardReadable``, plan stubs, etc.
"""

from ._factory import (
    EpicsRsProtocol,
    epicsrs_signal_r,
    epicsrs_signal_rw,
    epicsrs_signal_rw_rbv,
    epicsrs_signal_w,
    epicsrs_signal_x,
)
from ._signal_backend import EpicsRsSignalBackend

__all__ = [
    "EpicsRsProtocol",
    "EpicsRsSignalBackend",
    "epicsrs_signal_r",
    "epicsrs_signal_rw",
    "epicsrs_signal_rw_rbv",
    "epicsrs_signal_w",
    "epicsrs_signal_x",
]
