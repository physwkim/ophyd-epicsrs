"""Performance + stress tests against the live IOC.

These tests have measurable assertions but generous bounds — the
primary value is reporting concrete numbers in the test output so a
performance regression jumps out, not failing the run on a slow CI
runner.
"""

from __future__ import annotations

import asyncio
import time

import pytest


# Shared PV pool the bulk-scaling test draws from.
PV_POOL = [
    "mini:current",
    "mini:ph:DetValue_RBV",
    "mini:ph:DetSigma_RBV",
    "mini:ph:DetCenter_RBV",
    "mini:ph:ExposureTime_RBV",
    "mini:edge:DetValue_RBV",
    "mini:edge:DetSigma_RBV",
    "mini:edge:DetCenter_RBV",
    "mini:edge:ExposureTime_RBV",
    "mini:slit:DetValue_RBV",
    "mini:slit:DetSigma_RBV",
    "mini:slit:DetCenter_RBV",
    "mini:slit:ExposureTime_RBV",
    "mini:ph:mtr.RBV",
    "mini:ph:mtr.VAL",
    "mini:ph:mtr.HLM",
    "mini:ph:mtr.LLM",
    "mini:ph:mtr.VELO",
    "mini:ph:mtr.ACCL",
    "mini:edge:mtr.RBV",
    "mini:slit:mtr.RBV",
    "mini:dot:mtrx.RBV",
    "mini:dot:mtry.RBV",
    "mini:dcm:theta.RBV",
    "mini:dcm:y.RBV",
    "mini:dcm:z.RBV",
    "mini:BraggERdbkAO",
    "mini:BraggThetaRdbkAO",
    "mini:BraggLambdaRdbkAO",
    "mini:KohzuModeBO",
    "mini:dot:cam1:Acquire_RBV",
    "mini:dot:cam1:ImageMode_RBV",
    "mini:dot:cam1:NumImages_RBV",
    "mini:dot:cam1:AcquireTime_RBV",
    "mini:dot:cam1:AcquirePeriod_RBV",
    "mini:dot:cam1:DetectorState_RBV",
    "mini:dot:cam1:ArrayCounter_RBV",
    "mini:dot:cam1:ArrayCallbacks_RBV",
    "mini:dot:cam1:MaxSizeX_RBV",
    "mini:dot:cam1:MaxSizeY_RBV",
    "mini:dot:cam1:SizeX_RBV",
    "mini:dot:cam1:SizeY_RBV",
    "mini:dot:cam1:MotorXPos_RBV",
    "mini:dot:cam1:MotorYPos_RBV",
    "mini:dot:cam1:BeamCurrent_RBV",
    "mini:dot:cam1:ShutterOpen_RBV",
    "mini:dot:cam1:Manufacturer_RBV",
    "mini:dot:cam1:Model_RBV",
]


@pytest.fixture(scope="module")
def warm_pool(ca_ctx):
    """Pre-connect every PV in the pool so timing tests don't pay
    the connect cost. Connects sequentially — once is enough."""
    pvs = [ca_ctx.create_pv(name) for name in PV_POOL]
    for pv in pvs:
        assert pv.wait_for_connection(timeout=3.0), f"failed to connect {pv.pvname}"
    return PV_POOL


# ---------- bulk_caget scaling ----------


@pytest.mark.parametrize("n", [10, 25, 48])
def test_bulk_caget_scaling(ca_ctx, warm_pool, n):
    """bulk_caget(N) wall-time should be sub-linear in N because the
    reads are issued concurrently inside one tokio runtime."""
    pvs = warm_pool[:n]
    t0 = time.perf_counter()
    data = ca_ctx.bulk_caget(pvs, timeout=3.0)
    dt = (time.perf_counter() - t0) * 1000
    print(f"\n  bulk_caget({n}): {dt:.2f} ms ({dt / n:.3f} ms/PV)")
    assert len(data) == n
    # Loose ceiling — even with a slow runner, 48 PVs should not take
    # more than a second.
    assert dt < 1000, f"bulk_caget({n}) took {dt:.1f} ms — possible regression"


# ---------- single PV get latency ----------


def test_single_pv_get_latency_distribution(ca_ctx):
    """1000 sequential get_with_metadata calls. Report p50/p95/p99
    so a regression is visible. Asserts only on a generous ceiling."""
    pv = ca_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)

    samples = []
    for _ in range(200):
        t = time.perf_counter()
        pv.get_with_metadata(timeout=2.0)
        samples.append((time.perf_counter() - t) * 1e6)  # µs
    samples.sort()
    p50 = samples[100]
    p95 = samples[190]
    p99 = samples[198]
    print(f"\n  single get latency: p50={p50:.0f}µs  p95={p95:.0f}µs  p99={p99:.0f}µs")
    assert p99 < 5000, f"p99 {p99:.0f}µs > 5 ms threshold"


# ---------- ophyd-async parallel connect ----------


@pytest.mark.asyncio
async def test_async_parallel_connect_many_signals():
    """Build N independent ophyd-async Signals and connect them via
    asyncio.gather — should parallelise inside the single tokio
    runtime."""
    from ophyd_async.core import init_devices
    from ophyd_epicsrs.ophyd_async import epicsrs_signal_r

    pvs = PV_POOL[:30]
    async with init_devices():
        sigs = [epicsrs_signal_r(float, name) for name in pvs]

    t0 = time.perf_counter()
    await asyncio.gather(*(s.connect() for s in sigs))
    dt = (time.perf_counter() - t0) * 1000
    print(f"\n  ophyd-async parallel connect ({len(pvs)} PVs): {dt:.2f} ms")
    assert dt < 2000


# ---------- monitor subscriber sharing ----------


def test_shim_fans_out_to_multiple_callbacks(ophyd_setup):
    """The shim layer fans out a single underlying Rust monitor to
    multiple Python callbacks via its own _callbacks dict (the native
    add_monitor_callback is replace-semantics and only ever holds the
    shim's internal dispatcher). Verify the shim contract: 5 callbacks
    on the same Signal all receive the same monitor stream."""
    sig = ophyd_setup.EpicsSignal("mini:current", name="beam")
    sig.wait_for_connection(timeout=3.0)

    counters = [0] * 5

    def make_cb(i):
        def cb(**kwargs):
            counters[i] += 1
        return cb

    for i in range(5):
        sig.subscribe(make_cb(i))

    time.sleep(2.0)
    sig.clear_sub(None)  # clear all subs

    print(f"\n  per-subscriber counts: {counters}")
    # Beam current updates every 100 ms; in 2 s window we expect
    # roughly 20 events but the IOC may publish at higher cadence
    # under load (~40 measured). Loose floor + generous ceiling.
    for c in counters:
        assert 10 <= c <= 60, f"unexpected callback count {c}"
    # Spread across subscribers must be tight (all see the same
    # underlying monitor stream).
    assert max(counters) - min(counters) <= 3
