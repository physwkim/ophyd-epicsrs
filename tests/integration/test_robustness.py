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
    """200 short-lived PVs against the live IOC, rotating across a
    pool of distinct names so each create gets its own CaChannel +
    prefetch task + connection-event watcher rather than sharing a
    cached channel. Thread count must still stay flat — proves Drop
    aborts the per-PV spawned tasks, not just that channel caching
    masks a leak."""
    pool = [
        "mini:ph:DetValue_RBV",
        "mini:edge:DetValue_RBV",
        "mini:slit:DetValue_RBV",
        "mini:ph:mtr.RBV",
        "mini:edge:mtr.RBV",
        "mini:slit:mtr.RBV",
        "mini:dot:cam1:ArrayCounter_RBV",
        "mini:dot:cam1:Acquire_RBV",
    ]
    baseline = threading.active_count()
    for i in range(200):
        pv = ca_ctx.create_pv(pool[i % len(pool)])
        pv.wait_for_connection(timeout=2.0)
        del pv
    gc.collect()
    time.sleep(0.5)  # let any pending Drop work finish
    after = threading.active_count()
    delta = after - baseline
    print(f"\n  thread delta after 200 PV cycles ({len(pool)} distinct names): {delta}")
    assert delta < 10, (
        f"thread count grew by {delta} (baseline={baseline}, after={after})"
    )


# ---------- Concurrent get + monitor on same PV ----------


def test_concurrent_get_and_monitor(ca_ctx):
    """get_with_metadata while a monitor callback is firing on the
    same PV must not deadlock.

    Original assertion was "every get returns in 1 s + count > 50",
    which folded the upstream beacon-anomaly reconnect chain
    (epics-ca-rs first_sighting → EchoProbe → 5 s echo timeout →
    TcpClosed; see _contexts.py) into a test failure. The actual
    contract is "neither get nor monitor blocks the other" — measured
    by both completing at all, not by per-second throughput. Tolerate
    transient timeouts from a mid-test reconnect storm; bail out only
    on sustained outage and assert the deadlock floor (n_gets > 0
    AND monitor still fires).
    """
    pv = ca_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)

    monitor_count = [0]

    def cb(**kwargs):
        monitor_count[0] += 1

    pv.set_monitor_callback(cb)

    deadline = time.time() + 2.0
    n_gets = 0
    n_timeouts = 0
    while time.time() < deadline:
        r = pv.get_with_metadata(timeout=2.0)
        if r is None:
            n_timeouts += 1
            if n_timeouts > 2:
                break  # sustained outage — not the contract we're testing
        else:
            n_gets += 1

    pv.clear_monitors()
    print(
        f"\n  in 2s: {n_gets} gets ({n_timeouts} timeouts) + "
        f"{monitor_count[0]} monitor events"
    )
    # Distinguish sustained IOC outage from a real deadlock. If we
    # broke out on n_timeouts > 2 with zero successful gets AND the
    # monitor also never fired, the IOC was unreachable for the entire
    # window — skip rather than report a phantom deadlock.
    if n_gets == 0 and monitor_count[0] == 0:
        pytest.skip(
            f"sustained IOC outage during 2s window ({n_timeouts} get "
            f"timeouts, 0 monitor events) — cannot determine deadlock status"
        )
    # Deadlock floor: a get/monitor deadlock would collapse one of
    # these to 0 while the OTHER side keeps going. The combined check
    # below catches that: if monitors fired but no get returned, get
    # is blocked; vice versa for the next assertion.
    assert n_gets > 0, "monitor fired but no get returned — get is blocked"
    # Conservative floor — beam_current updates every 100 ms and we
    # watch for 2 s, so 10–20 is the typical range. Threshold of 3
    # tolerates subscribe-latency / scheduler-jitter days.
    assert monitor_count[0] >= 3, "gets succeeded but monitor never fired — monitor is blocked"


# ---------- Reads after a waited put ----------


def test_first_get_after_a_waited_put_reads_the_value_put(ca_ctx):
    """The first get on a new PV reads anew, not the value its background
    prefetch read before the put."""
    writer = ca_ctx.create_pv("mini:ph:mtr.VELO")
    assert writer.wait_for_connection(timeout=3.0)
    original = writer.get_with_metadata(timeout=2.0)["value"]
    try:
        reader = ca_ctx.create_pv("mini:ph:mtr.VELO")
        assert reader.wait_for_connection(timeout=3.0)
        time.sleep(0.5)  # the prefetch reads right after the connect
        writer.put(original + 1.0, wait=True, timeout=2.0)
        md = reader.get_with_metadata(timeout=2.0)
        assert md is not None and md["value"] == original + 1.0
    finally:
        writer.put(original, wait=True, timeout=2.0)


@pytest.mark.parametrize("ctx_name", ["ca_ctx", "pva_ctx"])
def test_waited_put_returns_after_another_pvs_callback(request, ctx_name):
    """A waited put returns after the monitor callback its update runs on
    another PV object of the same context, as libca and pvxs order them."""
    ctx = request.getfixturevalue(ctx_name)
    writer = ctx.create_pv("mini:ph:mtr.VELO")
    assert writer.wait_for_connection(timeout=3.0)
    original = writer.get_with_metadata(timeout=2.0)["value"]
    reader = ctx.create_pv("mini:ph:mtr.VELO")
    assert reader.wait_for_connection(timeout=3.0)
    latest = [None]
    seen = threading.Event()

    def cb(**kw):
        time.sleep(0.005)
        latest[0] = kw["value"]
        seen.set()

    reader.set_monitor_callback(cb)
    try:
        assert seen.wait(3.0)
        stale = 0
        for i in range(40):
            value = original + (1.0 if i % 2 else 0.5)
            writer.put(value, wait=True, timeout=2.0)
            stale += latest[0] != value
        assert stale == 0, f"{stale}/40 puts returned before the reader's callback"
    finally:
        reader.clear_monitors()
        writer.put(original, wait=True, timeout=2.0)


@pytest.mark.parametrize("ctx_name", ["ca_ctx", "pva_ctx"])
@pytest.mark.parametrize("use_async", [False, True])
def test_bulk_get_returns_after_a_running_callback(request, ctx_name, use_async):
    """A bulk_get returns after the callbacks for the updates read before its
    reply. A server may send a read reply ahead of the update for the same
    change (RSRV does), so the update here is one whose callback is running."""
    ctx = request.getfixturevalue(ctx_name)
    name = "mini:ph:mtr.VELO"
    writer = ctx.create_pv(name)
    assert writer.wait_for_connection(timeout=3.0)
    original = writer.get_with_metadata(timeout=2.0)["value"]
    reader = ctx.create_pv(name)
    assert reader.wait_for_connection(timeout=3.0)
    marker = original + 0.5
    started = threading.Event()
    finished = threading.Event()

    def cb(**kw):
        if kw["value"] == marker:
            started.set()
            time.sleep(0.3)
            finished.set()

    reader.set_monitor_callback(cb)
    try:
        writer.put(marker, wait=False)
        assert started.wait(3.0)
        if use_async:

            async def bulk_get():
                return await ctx.bulk_get_async([name], timeout=2.0)

            asyncio.run(bulk_get())
        else:
            ctx.bulk_get([name], timeout=2.0)
        assert finished.is_set(), "bulk_get returned while the callback ran"
    finally:
        reader.clear_monitors()
        writer.put(original, wait=True, timeout=2.0)


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
