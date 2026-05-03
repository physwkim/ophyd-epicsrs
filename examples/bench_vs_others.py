"""Comparative benchmark: ophyd-epicsrs vs aioca (CA) and p4p (PVA).

aioca is the default CA backend ophyd-async picks up; p4p is the
default PVA backend. Measuring the same workload across all four
clients tells you what you gain by routing ophyd / ophyd-async
through ophyd-epicsrs instead of the upstream defaults.

Workloads:
    1. Single-PV warm-get latency  — N sequential reads of one PV,
       report p50 / p95 / p99 in microseconds.
    2. Bulk parallel gets          — read M PVs concurrently, report
       wall time (μs) and per-PV cost.
    3. Connect time                — fresh PV → wait_for_connection
       for N PVs in parallel, report wall ms.
    4. Monitor throughput          — subscribe to one PV for T seconds,
       count delivered events.

Prerequisite: mini-beamline IOC running on PV prefix ``mini:``.
Both CA and PVA are served by the same IOC, so the comparison is
apples-to-apples on the wire. Install benchmark deps:

    pip install aioca p4p

Run:

    python examples/bench_vs_others.py
"""

from __future__ import annotations

import asyncio
import statistics
import time
from collections.abc import Callable
from contextlib import contextmanager

# ---------------------------------------------------------------------------
# Workload parameters
# ---------------------------------------------------------------------------
SINGLE_PV = "mini:current"

# Build a 100-PV pool from mini-beamline. Mix of detector readbacks,
# motor record fields (8 motors × multiple fields), AreaDetector cam1
# parameters, DCM/Bragg readbacks, and the beam current. Every entry
# is a real PV on the mini-beamline IOC — no synthetic fillers.
_MOTORS = (
    "mini:ph:mtr",
    "mini:edge:mtr",
    "mini:slit:mtr",
    "mini:dot:mtrx",
    "mini:dot:mtry",
    "mini:dcm:theta",
    "mini:dcm:y",
    "mini:dcm:z",
)
_MOTOR_FIELDS = ("RBV", "VAL", "HLM", "LLM", "VELO", "ACCL", "EGU", "DESC")
_DET_STATIONS = ("ph", "edge", "slit")
_DET_FIELDS = (
    "DetValue_RBV", "DetSigma_RBV", "DetCenter_RBV", "ExposureTime_RBV",
    "DetSigma", "DetCenter", "ExposureTime",
)
_CAM_RBVS = (
    "Acquire_RBV", "NumImages_RBV", "AcquireTime_RBV", "AcquirePeriod_RBV",
    "ArrayCounter_RBV", "ImageMode_RBV", "ArrayCallbacks_RBV",
    "DetectorState_RBV", "MaxSizeX_RBV", "MaxSizeY_RBV",
    "SizeX_RBV", "SizeY_RBV", "MotorXPos_RBV", "MotorYPos_RBV",
    "BeamCurrent_RBV", "ShutterOpen_RBV", "Manufacturer_RBV", "Model_RBV",
    "DataType_RBV", "ColorMode_RBV", "BinX_RBV", "BinY_RBV",
    "MinX_RBV", "MinY_RBV", "ArraySizeX_RBV", "ArraySizeY_RBV",
    "ArraySize_RBV", "TimeStamp_RBV",
)

PV_POOL = (
    [
        "mini:current",
        "mini:BraggERdbkAO",
        "mini:BraggThetaRdbkAO",
        "mini:BraggLambdaRdbkAO",
        "mini:KohzuModeBO",
    ]
    + [f"{m}.{f}" for m in _MOTORS for f in _MOTOR_FIELDS]
    + [f"mini:{s}:{f}" for s in _DET_STATIONS for f in _DET_FIELDS]
    + [f"mini:dot:cam1:{f}" for f in _CAM_RBVS]
)
assert len(PV_POOL) >= 100, f"PV_POOL only has {len(PV_POOL)} PVs, need ≥100"
PV_POOL = PV_POOL[:100]

N_GET_SAMPLES = 500              # per single-PV latency run
BULK_N = (10, 20, 50, 100)       # parallel-get fan-out sizes
MONITOR_SECONDS = 3.0            # subscribe-and-count window


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------
def percentiles(samples_us: list[float]) -> tuple[float, float, float]:
    s = sorted(samples_us)
    n = len(s)
    return s[n // 2], s[int(n * 0.95)], s[int(n * 0.99)]


def fmt_us(p50: float, p95: float, p99: float) -> str:
    return f"p50={p50:6.0f}µs  p95={p95:6.0f}µs  p99={p99:6.0f}µs"


@contextmanager
def section(title: str):
    print(f"\n{'─' * 72}\n  {title}\n{'─' * 72}")
    yield
    print()


# ---------------------------------------------------------------------------
# CA: ophyd-epicsrs
# ---------------------------------------------------------------------------
def bench_epicsrs_ca():
    from ophyd_epicsrs import get_ca_context

    ctx = get_ca_context()

    # Warm
    pv = ctx.create_pv(SINGLE_PV)
    pv.wait_for_connection(timeout=5.0)
    pv.get_with_metadata(timeout=2.0)

    # Single warm get
    samples = []
    for _ in range(N_GET_SAMPLES):
        t = time.perf_counter()
        pv.get_with_metadata(timeout=2.0)
        samples.append((time.perf_counter() - t) * 1e6)
    print(f"  single warm get:    {fmt_us(*percentiles(samples))}")

    # Bulk parallel via bulk_caget — re-creates channels every call,
    # so each invocation pays connect overhead. Reflects the worst-case
    # one-shot caget(list) workload with no channel reuse.
    pre = [ctx.create_pv(name) for name in PV_POOL]
    for p in pre:
        p.wait_for_connection(timeout=5.0)
    for n in BULK_N:
        names = PV_POOL[:n]
        ctx.bulk_caget(names, timeout=3.0)  # warm
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            ctx.bulk_caget(names, timeout=3.0)
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med = statistics.median(per_call_us)
        print(f"  bulk_caget({n:3d}):    {med:7.0f}µs (median, {med / n:6.1f}µs/PV)")

    # Bulk via bulk_get_pvs — uses the cached channels, libca-style
    # frame batching. The path you'd take in a fly-scan flyer that
    # already holds the PV objects.
    for n in BULK_N:
        subset = pre[:n]
        ctx.bulk_get_pvs(subset, timeout=3.0)  # warm
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            ctx.bulk_get_pvs(subset, timeout=3.0)
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med = statistics.median(per_call_us)
        print(f"  bulk_get_pvs({n:3d}):  {med:7.0f}µs (median, {med / n:6.1f}µs/PV)")

    # Connect time for N FRESH channels (no read yet). aioca / p4p
    # don't expose connect-without-get, so they bench `connect+1get`
    # instead — adjust expectations accordingly when comparing.
    for n in BULK_N:
        names = PV_POOL[:n]
        t0 = time.perf_counter()
        fresh = [ctx.create_pv(name) for name in names]
        for p in fresh:
            p.wait_for_connection(timeout=5.0)
        wall_ms = (time.perf_counter() - t0) * 1000
        print(f"  connect {n:2d} PVs:     {wall_ms:7.1f}ms")

    # Monitor throughput on a fast-updating PV
    counter = [0]

    def cb(**kwargs):
        counter[0] += 1

    pv.set_monitor_callback(cb)
    time.sleep(MONITOR_SECONDS)
    pv.clear_monitors()
    rate = counter[0] / MONITOR_SECONDS
    print(f"  monitor events:     {counter[0]:5d} in {MONITOR_SECONDS:.1f}s ({rate:.1f}/s)")


# ---------------------------------------------------------------------------
# CA: aioca
# ---------------------------------------------------------------------------
async def bench_aioca():
    import aioca

    # Warm
    await aioca.caget(SINGLE_PV)

    # Single warm get
    samples = []
    for _ in range(N_GET_SAMPLES):
        t = time.perf_counter()
        await aioca.caget(SINGLE_PV)
        samples.append((time.perf_counter() - t) * 1e6)
    print(f"  single warm get:    {fmt_us(*percentiles(samples))}")

    # Bulk parallel via asyncio.gather
    for name in PV_POOL:
        await aioca.caget(name)  # warm cache
    for n in BULK_N:
        names = PV_POOL[:n]
        await aioca.caget(names)  # warm
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            await aioca.caget(names)  # aioca accepts a list
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med = statistics.median(per_call_us)
        print(f"  caget({n:2d} PVs):      {med:7.0f}µs (median, {med / n:6.1f}µs/PV)")

    # Connect-only — aioca doesn't expose a connect-without-get,
    # so we measure the first caget on a fresh client. Drop the
    # process-wide channel cache via aioca.purge_channel_cache so
    # each iteration is genuinely cold.
    for n in BULK_N:
        names = PV_POOL[:n]
        aioca.purge_channel_caches()
        t0 = time.perf_counter()
        await aioca.caget(names)
        wall_ms = (time.perf_counter() - t0) * 1000
        print(f"  connect+1get {n:2d}:    {wall_ms:7.1f}ms")

    # Monitor throughput
    counter = [0]

    def cb(value):
        counter[0] += 1

    sub = aioca.camonitor(SINGLE_PV, cb)
    await asyncio.sleep(MONITOR_SECONDS)
    sub.close()
    rate = counter[0] / MONITOR_SECONDS
    print(f"  monitor events:     {counter[0]:5d} in {MONITOR_SECONDS:.1f}s ({rate:.1f}/s)")


# ---------------------------------------------------------------------------
# PVA: ophyd-epicsrs
# ---------------------------------------------------------------------------
async def bench_epicsrs_pva():
    from ophyd_epicsrs import get_pva_context

    ctx = get_pva_context()

    # Warm
    pv = ctx.create_pv(SINGLE_PV)
    assert pv.wait_for_connection(timeout=5.0)
    await pv.get_value_async()

    # Single warm get (async)
    samples = []
    for _ in range(N_GET_SAMPLES):
        t = time.perf_counter()
        await pv.get_value_async(timeout=2.0)
        samples.append((time.perf_counter() - t) * 1e6)
    print(f"  single warm get:    {fmt_us(*percentiles(samples))}")

    # Bulk parallel via asyncio.gather (per-PV future, hits the
    # central PvaClient queues N times)
    pvs = [ctx.create_pv(n) for n in PV_POOL]
    for p in pvs:
        assert p.wait_for_connection(timeout=5.0)
    for n in BULK_N:
        subset = pvs[:n]
        await asyncio.gather(*(p.get_value_async(timeout=3.0) for p in subset))  # warm
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            await asyncio.gather(*(p.get_value_async(timeout=3.0) for p in subset))
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med = statistics.median(per_call_us)
        print(f"  gather({n:3d} PVs):    {med:7.0f}µs (median, {med / n:6.1f}µs/PV)")

    # bulk_pvaget — by-name path (re-creates channels each call)
    for n in BULK_N:
        names = PV_POOL[:n]
        ctx.bulk_pvaget(names, timeout=3.0)  # warm
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            ctx.bulk_pvaget(names, timeout=3.0)
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med = statistics.median(per_call_us)
        print(f"  bulk_pvaget({n:3d}):   {med:7.0f}µs (median, {med / n:6.1f}µs/PV)")

    # bulk_get_pvs_pva — pre-cached PV objects, single Rust spawn
    for n in BULK_N:
        subset = pvs[:n]
        ctx.bulk_get_pvs_pva(subset, timeout=3.0)  # warm
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            ctx.bulk_get_pvs_pva(subset, timeout=3.0)
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med = statistics.median(per_call_us)
        print(f"  bulk_get_pvs_pva({n:3d}): {med:7.0f}µs (median, {med / n:6.1f}µs/PV)")

    # Connect time
    for n in BULK_N:
        names = PV_POOL[:n]
        t0 = time.perf_counter()
        fresh = [ctx.create_pv(name) for name in names]
        for p in fresh:
            p.wait_for_connection(timeout=5.0)
        wall_ms = (time.perf_counter() - t0) * 1000
        print(f"  connect {n:2d} PVs:     {wall_ms:7.1f}ms")

    # Monitor throughput
    counter = [0]

    def cb(**kwargs):
        counter[0] += 1

    pv.set_monitor_callback(cb)
    await asyncio.sleep(MONITOR_SECONDS)
    pv.clear_monitors()
    rate = counter[0] / MONITOR_SECONDS
    print(f"  monitor events:     {counter[0]:5d} in {MONITOR_SECONDS:.1f}s ({rate:.1f}/s)")


# ---------------------------------------------------------------------------
# PVA: p4p
# ---------------------------------------------------------------------------
def bench_p4p():
    from p4p.client.thread import Context

    ctx = Context("pva")

    # Warm
    ctx.get(SINGLE_PV)

    # Single warm get
    samples = []
    for _ in range(N_GET_SAMPLES):
        t = time.perf_counter()
        ctx.get(SINGLE_PV)
        samples.append((time.perf_counter() - t) * 1e6)
    print(f"  single warm get:    {fmt_us(*percentiles(samples))}")

    # Bulk parallel: p4p.Context.get() with a list dispatches to a
    # background thread pool — same fan-out semantics as
    # asyncio.gather here.
    for name in PV_POOL:
        ctx.get(name)  # warm
    for n in BULK_N:
        names = PV_POOL[:n]
        ctx.get(names)  # warm
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            ctx.get(names)
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med = statistics.median(per_call_us)
        print(f"  get({n:2d} PVs):        {med:7.0f}µs (median, {med / n:6.1f}µs/PV)")

    # Connect time — close + recreate context so the channel cache
    # is genuinely empty.
    for n in BULK_N:
        names = PV_POOL[:n]
        cold_ctx = Context("pva")
        t0 = time.perf_counter()
        cold_ctx.get(names)
        wall_ms = (time.perf_counter() - t0) * 1000
        print(f"  connect+1get {n:2d}:    {wall_ms:7.1f}ms")
        cold_ctx.close()

    # Monitor throughput
    counter = [0]

    def cb(value):
        counter[0] += 1

    sub = ctx.monitor(SINGLE_PV, cb)
    time.sleep(MONITOR_SECONDS)
    sub.close()
    rate = counter[0] / MONITOR_SECONDS
    print(f"  monitor events:     {counter[0]:5d} in {MONITOR_SECONDS:.1f}s ({rate:.1f}/s)")

    ctx.close()


# ---------------------------------------------------------------------------
# Main — sequential so each client owns the IOC traffic exclusively
# ---------------------------------------------------------------------------
async def main():
    print(
        f"PV pool: {len(PV_POOL)} PVs   "
        f"single-get samples: {N_GET_SAMPLES}   "
        f"monitor window: {MONITOR_SECONDS}s"
    )

    with section("CA: ophyd-epicsrs"):
        bench_epicsrs_ca()
    with section("CA: aioca"):
        await bench_aioca()
    with section("PVA: ophyd-epicsrs"):
        await bench_epicsrs_pva()
    with section("PVA: p4p"):
        bench_p4p()


if __name__ == "__main__":
    asyncio.run(main())
