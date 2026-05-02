"""Factory functions that mirror ``ophyd_async.epics.core.epics_signal_*``.

These return standard ``SignalRW`` / ``SignalR`` / ``SignalW`` / ``SignalX``
instances backed by :class:`EpicsRsSignalBackend`. Drop-in replacement for
the aioca/p4p-backed factories — same call signature, same return type.
"""

from __future__ import annotations

from collections.abc import Callable

from ophyd_async.core import (
    DEFAULT_TIMEOUT,
    SignalDatatypeT,
    SignalR,
    SignalRW,
    SignalW,
    SignalX,
    get_unique,
)
from ophyd_async.epics.core._util import EpicsOptions, get_pv_basename_and_field

from ._signal_backend import EpicsRsProtocol, EpicsRsSignalBackend


def _split_protocol(pv: str) -> tuple[EpicsRsProtocol, str]:
    """Strip any ``ca://`` / ``pva://`` prefix and return the protocol enum."""
    if "://" in pv:
        scheme, bare = pv.split("://", 1)
        try:
            return EpicsRsProtocol(scheme), bare
        except ValueError as exc:
            msg = f"Unknown protocol prefix in {pv!r}; expected 'ca://' or 'pva://'"
            raise ValueError(msg) from exc
    return EpicsRsProtocol.CA, pv


def _backend(
    datatype: type[SignalDatatypeT] | None,
    read_pv: str,
    write_pv: str,
    options: EpicsOptions | None = None,
) -> EpicsRsSignalBackend[SignalDatatypeT]:
    r_proto, r_pv = _split_protocol(read_pv)
    w_proto, w_pv = _split_protocol(write_pv)
    protocol = get_unique({read_pv: r_proto, write_pv: w_proto}, "protocols")
    return EpicsRsSignalBackend(datatype, r_pv, w_pv, options, protocol=protocol)


def epicsrs_signal_rw(
    datatype: type[SignalDatatypeT],
    read_pv: str,
    write_pv: str | None = None,
    name: str = "",
    timeout: float = DEFAULT_TIMEOUT,
    attempts: int = 1,
    wait: bool | Callable[[SignalDatatypeT], bool] = True,
) -> SignalRW[SignalDatatypeT]:
    """``SignalRW`` backed by 1 or 2 EPICS PVs via epics-rs."""
    backend = _backend(
        datatype, read_pv, write_pv or read_pv, EpicsOptions(wait=wait)
    )
    return SignalRW(backend, name=name, timeout=timeout, attempts=attempts)


def epicsrs_signal_rw_rbv(
    datatype: type[SignalDatatypeT],
    write_pv: str,
    read_suffix: str = "_RBV",
    name: str = "",
    timeout: float = DEFAULT_TIMEOUT,
    attempts: int = 1,
    wait: bool | Callable[[SignalDatatypeT], bool] = True,
) -> SignalRW[SignalDatatypeT]:
    """``SignalRW`` with separate readback PV (``write_pv + read_suffix``)."""
    base_pv, field = get_pv_basename_and_field(write_pv)
    if field is not None:
        read_pv = f"{base_pv}{read_suffix}.{field}"
    else:
        read_pv = f"{write_pv}{read_suffix}"
    return epicsrs_signal_rw(
        datatype,
        read_pv,
        write_pv,
        name=name,
        timeout=timeout,
        attempts=attempts,
        wait=wait,
    )


def epicsrs_signal_r(
    datatype: type[SignalDatatypeT],
    read_pv: str,
    name: str = "",
    timeout: float = DEFAULT_TIMEOUT,
) -> SignalR[SignalDatatypeT]:
    """``SignalR`` backed by 1 EPICS PV via epics-rs."""
    backend = _backend(datatype, read_pv, read_pv)
    return SignalR(backend, name=name, timeout=timeout)


def epicsrs_signal_w(
    datatype: type[SignalDatatypeT],
    write_pv: str,
    name: str = "",
    timeout: float = DEFAULT_TIMEOUT,
    attempts: int = 1,
    wait: bool | Callable[[SignalDatatypeT], bool] = True,
) -> SignalW[SignalDatatypeT]:
    """``SignalW`` backed by 1 EPICS PV via epics-rs."""
    backend = _backend(datatype, write_pv, write_pv, EpicsOptions(wait=wait))
    return SignalW(backend, name=name, timeout=timeout, attempts=attempts)


def epicsrs_signal_x(
    write_pv: str,
    name: str = "",
    timeout: float = DEFAULT_TIMEOUT,
) -> SignalX:
    """``SignalX`` (executable trigger) backed by 1 EPICS PV via epics-rs."""
    backend = _backend(None, write_pv, write_pv)
    return SignalX(backend, name=name, timeout=timeout)
