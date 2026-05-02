"""Unit tests for ophyd_epicsrs._shim — the legacy ophyd surface.

Offline (no IOC). Covers:
- protocol prefix splitting (`pva://` / `ca://` / naked)
- _on_connection_change deduplicates same-state calls
- get_pv() dispatch wires the right native PV class
- Drop semantics: 200x create+drop on legacy caget/caput pattern does
  not leak Python objects (proxy for the Rust task leak fix in c1ec52b)
"""

from __future__ import annotations

import gc
from unittest.mock import Mock

import pytest

from ophyd_epicsrs._shim import (
    EpicsRsShimPV,
    _split_protocol,
    get_pv,
    setup,
)
import ophyd_epicsrs._shim as shim


# ---------- Protocol split ----------


@pytest.mark.parametrize(
    "pvname,expected",
    [
        ("FOO:BAR", ("ca", "FOO:BAR")),
        ("ca://FOO:BAR", ("ca", "FOO:BAR")),
        ("pva://FOO:BAR", ("pva", "FOO:BAR")),
        # Edge: nested-looking name
        ("pva://nested://x", ("pva", "nested://x")),
    ],
)
def test_split_protocol(pvname, expected):
    assert _split_protocol(pvname) == expected


# ---------- get_pv dispatch ----------


def _ensure_setup():
    if shim._dispatcher is None:
        setup(None)


def test_get_pv_ca_default():
    _ensure_setup()
    pv = get_pv("FAKE:CA:1")
    assert isinstance(pv, EpicsRsShimPV)
    assert pv.pvname == "FAKE:CA:1"
    assert type(pv._pv).__name__ == "EpicsRsPV"


def test_get_pv_pva_prefix_strips():
    _ensure_setup()
    pv = get_pv("pva://FAKE:PVA:1")
    assert pv.pvname == "FAKE:PVA:1"  # prefix removed
    assert type(pv._pv).__name__ == "EpicsRsPvaPV"


def test_get_pv_ca_explicit_prefix():
    _ensure_setup()
    pv = get_pv("ca://FAKE:CA:2")
    assert pv.pvname == "FAKE:CA:2"
    assert type(pv._pv).__name__ == "EpicsRsPV"


# ---------- _on_connection_change dedupe ----------


def test_on_connection_change_dedupes_same_state():
    """The user callback must fire at most once per state transition."""
    _ensure_setup()
    cb = Mock()
    pv = get_pv("FAKE:DEDUPE", connection_callback=cb)
    cb.reset_mock()  # ignore any setup-time fires

    # Two consecutive True calls — only the first should reach the user cb
    pv._on_connection_change(True)
    pv._on_connection_change(True)
    assert cb.call_count == 1

    # State transition fires
    pv._on_connection_change(False)
    assert cb.call_count == 2

    # Idle False → no extra fires
    pv._on_connection_change(False)
    assert cb.call_count == 2


# ---------- Drop / leak proxy ----------


def test_create_and_drop_loop_does_not_leak_threads():
    """Quantitative leak check: 500 PV create+drop cycles should not
    accumulate Python OS threads (each Rust EpicsRsPV starts at most
    one dispatch_thread; Drop aborts all spawned tokio tasks AND lets
    the dispatch thread exit cleanly when its rx Sender is dropped).

    Threshold is conservative — typical leak would add ~one thread per
    cycle (500+) so a >50 delta over 500 iterations would flag a
    regression with high confidence.
    """
    import threading

    _ensure_setup()

    # Baseline: spawn one PV first to flush any one-time dispatcher /
    # tracing initialisation thread creation.
    _ = get_pv("WARMUP:THREADS")
    gc.collect()
    baseline = threading.active_count()

    for i in range(500):
        pv = get_pv(f"DROP:THREADS:{i}")
        # Trigger callback paths that spawn tasks (the original leak risk).
        pv._pv.set_connection_callback(lambda c: None)
        pv._pv.set_access_callback(lambda r, w: None)
        del pv
    gc.collect()

    after = threading.active_count()
    delta = after - baseline
    # Tight threshold: if even 1% of PVs leaked a thread, delta would
    # be 5 — well within range. Drop should keep thread count flat.
    assert delta < 5, (
        f"thread count grew by {delta} over 500 PV cycles "
        f"(baseline={baseline}, after={after}) — possible leak in Drop"
    )


def test_release_pvs_clears_handlers():
    """release_pvs() should be idempotent and accept any number of PVs."""
    _ensure_setup()
    pv1 = get_pv("RELEASE:1")
    pv2 = get_pv("pva://RELEASE:2")
    shim.release_pvs(pv1, pv2)
    # Calling again must not raise
    shim.release_pvs(pv1, pv2)
