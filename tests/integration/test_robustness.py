"""Robustness / failure-mode tests.

Covers behaviour under conditions where pyepics + aioca/p4p commonly
have rough edges: nonexistent PVs, repeated rapid create/drop, and
the same PV accessed concurrently from sync ophyd and ophyd-async
(the design's stated "single backend → one circuit" advantage).
"""

from __future__ import annotations

import asyncio
import gc
import threading
import time

import pytest


# ---------- Nonexistent PV behaviour ----------


def test_nonexistent_pv_connect_returns_false_within_timeout(ca_ctx):
    """wait_for_connection on a fictional PV must return False inside
    the timeout window (not raise, not hang)."""
    pv = ca_ctx.create_pv("mini:does:not:exist:RBV")
    t0 = time.perf_counter()
    ok = pv.wait_for_connection(timeout=1.0)
    dt = time.perf_counter() - t0
    assert ok is False
    # Must respect the timeout — small overshoot is acceptable.
    assert dt < 1.5, f"timeout overshot: {dt:.2f} s for 1 s timeout"


def test_nonexistent_pv_get_returns_none(ca_ctx):
    """Without a connection, get_with_metadata should report None
    rather than blocking or raising."""
    pv = ca_ctx.create_pv("mini:also:not:real")
    pv.wait_for_connection(timeout=0.5)
    r = pv.get_with_metadata(timeout=0.5)
    assert r is None


# ---------- Sync + async coexistence on same PV ----------


def test_legacy_ophyd_and_ophyd_async_share_circuit(ophyd_setup):
    """The headline architectural promise: legacy ophyd EpicsSignal +
    ophyd-async epicsrs_signal_rw on the same PV name go through one
    epics-rs backend, share one TCP virtual circuit, and don't fight
    each other.

    Direct verification of "one circuit" requires looking at the
    IOC's perspective; from the client we settle for: both surfaces
    work simultaneously, both report the same value within a small
    window."""
    from ophyd_epicsrs.ophyd_async import epicsrs_signal_r

    legacy = ophyd_setup.EpicsSignalRO("mini:current", name="legacy_beam")
    legacy.wait_for_connection(timeout=3.0)

    async def run_async_side():
        async with __import__(
            "ophyd_async.core", fromlist=["init_devices"]
        ).init_devices():
            modern = epicsrs_signal_r(float, "mini:current")
        await modern.connect()
        return await modern.get_value()

    v_legacy = legacy.get()
    v_async = asyncio.run(run_async_side())

    print(f"\n  legacy={v_legacy:.3f}  async={v_async:.3f}")
    assert 400 < v_legacy < 600
    assert 400 < v_async < 600
    assert abs(v_legacy - v_async) < 5.0


# ---------- Rapid create/drop ----------


def test_rapid_create_drop_no_thread_leak(ca_ctx):
    """200 short-lived PVs against the live IOC. Thread count must
    stay flat (proxy for the Rust task-leak fix that the unit test
    checks against a fake server)."""
    baseline = threading.active_count()
    for i in range(200):
        pv = ca_ctx.create_pv(f"mini:current")
        pv.wait_for_connection(timeout=2.0)
        del pv
    gc.collect()
    time.sleep(0.5)  # let any pending Drop work finish
    after = threading.active_count()
    delta = after - baseline
    print(f"\n  thread delta after 200 PV cycles: {delta}")
    assert delta < 10, (
        f"thread count grew by {delta} (baseline={baseline}, after={after})"
    )


# ---------- Concurrent get + monitor on same PV ----------


def test_concurrent_get_and_monitor(ca_ctx):
    """get_with_metadata while a monitor callback is firing on the
    same PV must not deadlock and must return current values."""
    pv = ca_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)

    monitor_count = [0]

    def cb(**kwargs):
        monitor_count[0] += 1

    pv.set_monitor_callback(cb)

    # Hammer get_with_metadata for 2 s while monitor delivers events.
    # The 2 s window gives the subscribe handshake comfortable margin
    # over the 100 ms IOC update period, so we still see plenty of
    # events even when a previous test left residual subscription
    # state on the IOC.
    deadline = time.time() + 2.0
    n_gets = 0
    while time.time() < deadline:
        r = pv.get_with_metadata(timeout=1.0)
        assert r is not None
        n_gets += 1

    pv.clear_monitors()
    print(f"\n  in 2s: {n_gets} gets + {monitor_count[0]} monitor events")
    assert n_gets > 50  # sanity — should easily clear this on localhost
    # Conservative floor — beam_current updates every 100 ms and we
    # watch for 2 s, so 10–20 is the typical range. Threshold of 3
    # tolerates subscribe-latency / scheduler-jitter days.
    assert monitor_count[0] >= 3


# ---------- Disconnect callback on IOC restart simulation ----------


def test_connection_callback_fires_on_initial_connect(ca_ctx):
    """When a callback is registered before the channel finishes
    connecting, it must still fire once with conn=True."""
    pv = ca_ctx.create_pv("mini:current")
    seen: list[bool] = []

    def cb(connected: bool):
        seen.append(connected)

    pv.set_connection_callback(cb)
    pv.wait_for_connection(timeout=3.0)
    # Allow the dispatch thread to deliver the initial state.
    time.sleep(0.3)
    assert True in seen, f"never saw conn=True; got {seen}"
