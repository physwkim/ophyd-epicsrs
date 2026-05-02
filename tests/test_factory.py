"""Unit tests for ophyd_epicsrs.ophyd_async._factory.

Verifies factory functions return the right Signal type, dispatch on
pv:// prefix, and propagate the wait option through to the backend.
Offline (no IOC required).
"""

from __future__ import annotations

import pytest
from ophyd_async.core import SignalR, SignalRW, SignalW, SignalX

from ophyd_epicsrs.ophyd_async import (
    EpicsRsProtocol,
    epicsrs_signal_r,
    epicsrs_signal_rw,
    epicsrs_signal_rw_rbv,
    epicsrs_signal_w,
    epicsrs_signal_x,
)
from ophyd_epicsrs.ophyd_async._signal_backend import EpicsRsSignalBackend


def _backend(sig) -> EpicsRsSignalBackend:
    return sig._connector.backend


def test_signal_rw_returns_typed_signal():
    sig = epicsrs_signal_rw(float, "IOC:foo")
    assert isinstance(sig, SignalRW)
    assert _backend(sig).protocol is EpicsRsProtocol.CA


def test_signal_r_w_x_types():
    assert isinstance(epicsrs_signal_r(int, "IOC:r"), SignalR)
    assert isinstance(epicsrs_signal_w(int, "IOC:w"), SignalW)
    assert isinstance(epicsrs_signal_x("IOC:x"), SignalX)


def test_pva_prefix_dispatch():
    sig = epicsrs_signal_rw(float, "pva://IOC:nt:scalar")
    b = _backend(sig)
    assert b.protocol is EpicsRsProtocol.PVA
    assert b.read_pv == "IOC:nt:scalar"  # prefix stripped
    assert b.source("x", read=True) == "pva://IOC:nt:scalar"


def test_ca_explicit_prefix_stripped():
    sig = epicsrs_signal_rw(float, "ca://IOC:m1.RBV")
    b = _backend(sig)
    assert b.protocol is EpicsRsProtocol.CA
    assert b.read_pv == "IOC:m1.RBV"


def test_split_protocol_mismatch_raises():
    # ophyd-async's get_unique surfaces TypeError ("Differing protocols")
    with pytest.raises((TypeError, ValueError)):
        epicsrs_signal_rw(float, "pva://IOC:r", "ca://IOC:w")


def test_unknown_prefix_raises():
    with pytest.raises(ValueError, match="Unknown protocol"):
        epicsrs_signal_rw(float, "tango://IOC:foo")


def test_rw_rbv_appends_suffix_to_write_pv():
    sig = epicsrs_signal_rw_rbv(float, "IOC:gain", read_suffix="_RBV")
    b = _backend(sig)
    assert b.read_pv == "IOC:gain_RBV"
    assert b.write_pv == "IOC:gain"


def test_rw_rbv_suffix_with_field():
    sig = epicsrs_signal_rw_rbv(float, "IOC:m1.VAL", read_suffix="_RBV")
    # Field path: write_pv = "IOC:m1.VAL" → read_pv = "IOC:m1_RBV.VAL"
    assert _backend(sig).read_pv == "IOC:m1_RBV.VAL"


def test_wait_option_propagates_through_factory():
    sig = epicsrs_signal_rw(int, "IOC:busy", wait=False)
    assert _backend(sig).options.wait is False

    def non_zero(v: int) -> bool:
        return v != 0

    sig2 = epicsrs_signal_rw(int, "IOC:busy", wait=non_zero)
    assert _backend(sig2).options.wait is non_zero
