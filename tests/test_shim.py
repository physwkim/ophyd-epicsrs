"""Unit tests for ophyd_epicsrs._shim — the legacy ophyd surface.

Offline (no IOC). Covers:
- protocol prefix splitting (`pva://` / `ca://` / naked)
- _on_connection_change deduplicates same-state calls
- get_pv() dispatch wires the right native PV class
- Drop semantics: 500x create+drop on legacy caget/caput pattern does
  not leak Python OS threads (proxy for the Rust task leak fix in c1ec52b)
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


def test_get_pv_pva_prefix_preserved():
    """The shim's pvname must match the original string (incl. prefix).
    ophyd indexes ``_received_first_metadata`` / ``_signals`` by the
    pvname it was originally handed; reporting the stripped version
    breaks the connection-callback bookkeeping. The bare name only
    needs to reach the underlying native PV."""
    _ensure_setup()
    pv = get_pv("pva://FAKE:PVA:1")
    assert pv.pvname == "pva://FAKE:PVA:1"  # original preserved
    assert type(pv._pv).__name__ == "EpicsRsPvaPV"


def test_get_pv_ca_explicit_prefix_preserved():
    _ensure_setup()
    pv = get_pv("ca://FAKE:CA:2")
    assert pv.pvname == "ca://FAKE:CA:2"  # original preserved
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

    Threshold is tight — < 5 thread delta over 500 cycles. Even a 1 %
    leak rate (one thread per 100 PVs) would push delta to ~5 and
    flag this as a regression. Earlier revisions used a much looser
    threshold that would pass even with substantial leaks.
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
    """release_pvs() actually clears the registered callbacks and the
    auto-monitor flag, and is idempotent across repeated calls.
    """
    _ensure_setup()
    cb = Mock()
    pv = get_pv("RELEASE:1", connection_callback=cb)
    cb.reset_mock()  # ignore any setup-time fires

    # Drive a state transition so we can verify cb is wired.
    pv._on_connection_change(True)
    assert cb.call_count == 1, "callback should have fired once before release"

    shim.release_pvs(pv)

    # After release: connection callbacks are cleared, auto_monitor is off.
    assert pv._conn_callbacks == [], "release should clear conn callbacks"
    assert pv._callbacks == {}, "release should clear monitor callbacks"
    assert pv.auto_monitor is None, "release should clear auto_monitor"

    # Subsequent state transitions must NOT reach the user callback.
    cb.reset_mock()
    pv._on_connection_change(False)  # transition to dedupe-False
    pv._on_connection_change(True)   # transition back
    assert cb.call_count == 0, "callback must not fire after release"

    # Rust-side liveness: re-registering a callback after release_pvs
    # must work (i.e. release didn't permanently break the underlying
    # native PV's callback machinery — only cleared current state).
    new_cb = Mock()
    pv._pv.set_connection_callback(lambda c: new_cb(c))
    # Just verifying the call doesn't raise — actual emission needs an IOC.

    # Idempotency: repeated release must not raise.
    shim.release_pvs(pv)


# ---------- safe_call_or! misuse guard ----------


def test_safe_call_or_default_is_gil_free():
    """`safe_call_or!`'s `$default` is evaluated OUTSIDE the
    `catch_unwind` guard. A default that calls `Python::with_gil` will
    re-trigger the same finalize-time panic the guard was meant to
    absorb. The macro docstring warns about this, but docs alone don't
    stop a future contributor from reintroducing the bug.

    This test greps the Rust source for the misuse pattern. It is
    intentionally narrow — it only catches the literal
    `safe_call_or!(Python::with_gil(...), Python::with_gil(...))`
    shape that bit us once; cleverer obfuscations will still slip
    through. It exists so the *exact* mistake we caught in review
    can't silently come back.
    """
    import re
    from pathlib import Path

    src_dir = Path(__file__).parent.parent / "crates" / "ophyd-epicsrs" / "src"
    pattern = re.compile(
        r"safe_call_or!\s*\(\s*Python::with_gil",
        re.MULTILINE,
    )
    offenders = []
    for rs in src_dir.rglob("*.rs"):
        # Skip the macro definition itself — the docstring legitimately
        # mentions `Python::with_gil` near `safe_call_or!`.
        if rs.name == "safe_log.rs":
            continue
        text = rs.read_text()
        if pattern.search(text):
            offenders.append(str(rs.relative_to(src_dir.parent.parent.parent)))
    assert not offenders, (
        f"safe_call_or!'s `default` must not call Python::with_gil "
        f"(it would re-trigger the finalize panic). Offending files: "
        f"{offenders}"
    )
