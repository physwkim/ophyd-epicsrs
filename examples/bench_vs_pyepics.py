"""Side-by-side benchmark: pyepics vs ophyd-epicsrs against the
mini-beamline IOC. Not a pytest test — runs as a script and prints a
markdown-ready table to stdout.

Both backends measure the exact same PV pool, on the exact same
machine, with warm channels (each PV is connected and one read is
issued before timing starts). For the pyepics side, every timed
``PV.get`` call passes ``use_monitor=False`` so we measure a fresh
CA round-trip — pyepics's default ``PV.get()`` returns the locally
cached monitor value (~2 µs Python attribute access), which is a
different operation from ophyd-epicsrs's ``get_with_metadata`` and
makes the two columns non-comparable. ``epics.caget()`` already
defaults to ``use_monitor=False`` so the loop variant needs no
flag.

Usage:
    python examples/bench_vs_pyepics.py
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time

import epics  # pyepics

from ophyd_epicsrs._native import EpicsRsContext


# A representative PV pool: scalars, motor record fields, mbbi enums,
# stringin records, and a wide spread of names. Every PV is on the
# mini-beamline IOC and reads as a single atomic value.
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
    "mini:dot:cam1:MotorXPos",
    "mini:dot:cam1:MotorYPos",
    "mini:dot:cam1:BeamCurrent",
    "mini:dot:cam1:Acquire",
    "mini:dot:cam1:ImageMode",
]
SINGLE_PV = "mini:current"
N_GET_SAMPLES = 200


def _percentiles(samples_us: list[float]) -> tuple[float, float, float]:
    s = sorted(samples_us)
    n = len(s)
    return (s[n // 2], s[(n * 95) // 100], s[(n * 99) // 100])


def bench_pyepics():
    print("\n=== pyepics ===")

    # ── Single PV warm get latency (fresh CA read, not cached monitor)
    pv = epics.PV(SINGLE_PV)
    pv.wait_for_connection(timeout=5.0)
    pv.get(use_monitor=False)  # warm
    samples = []
    for _ in range(N_GET_SAMPLES):
        t = time.perf_counter()
        pv.get(use_monitor=False)
        samples.append((time.perf_counter() - t) * 1e6)
    p50, p95, p99 = _percentiles(samples)
    print(f"single warm get:  p50={p50:.0f}µs  p95={p95:.0f}µs  p99={p99:.0f}µs  (fresh)")

    # ── Sequential gets across PV_POOL[:N] (fresh reads) ──────────────
    # Pre-connect + warm
    pvs = [epics.PV(name) for name in PV_POOL]
    for p in pvs:
        p.wait_for_connection(timeout=5.0)
        p.get(use_monitor=False)
    for n in (10, 25, 48):
        subset = pvs[:n]
        t0 = time.perf_counter()
        for p in subset:
            p.get(use_monitor=False)
        dt_ms = (time.perf_counter() - t0) * 1000
        print(
            f"sequential get(N={n}):  {dt_ms:.2f} ms  ({dt_ms / n * 1000:.1f} µs/PV)"
        )

    # ── caget (cold-cache, fresh PV each time) ────────────────────────
    # epics.caget() under the hood reuses pyepics's process-wide PV
    # cache, so this is also "warm" once the PV has been seen.
    for n in (10, 25, 48):
        names = PV_POOL[:n]
        t0 = time.perf_counter()
        for nm in names:
            epics.caget(nm)
        dt_ms = (time.perf_counter() - t0) * 1000
        print(
            f"epics.caget loop(N={n}):  {dt_ms:.2f} ms  ({dt_ms / n * 1000:.1f} µs/PV)"
        )

    # Disconnect to release CA channels before the rust client takes over
    for p in pvs:
        p.disconnect()
    pv.disconnect()


def bench_epicsrs():
    print("\n=== ophyd-epicsrs (CA backend) ===")

    # Benchmark intentionally constructs a fresh context (private API
    # import) to measure cold-start cost in isolation; production code
    # uses ophyd_epicsrs.get_ca_context() — see _contexts.py.
    ctx = EpicsRsContext()
    pv = ctx.create_pv(SINGLE_PV)
    pv.wait_for_connection(timeout=5.0)
    pv.get_with_metadata(timeout=2.0)  # warm

    # ── Single PV warm get latency (always fresh CA read) ────────────
    samples = []
    for _ in range(N_GET_SAMPLES):
        t = time.perf_counter()
        pv.get_with_metadata(timeout=2.0)
        samples.append((time.perf_counter() - t) * 1e6)
    p50, p95, p99 = _percentiles(samples)
    print(f"single warm get:  p50={p50:.0f}µs  p95={p95:.0f}µs  p99={p99:.0f}µs  (fresh)")

    # ── Sequential get_with_metadata across PV_POOL[:N] ───────────────
    pvs = [ctx.create_pv(name) for name in PV_POOL]
    for p in pvs:
        p.wait_for_connection(timeout=5.0)
        p.get_with_metadata(timeout=2.0)
    for n in (10, 25, 48):
        subset = pvs[:n]
        t0 = time.perf_counter()
        for p in subset:
            p.get_with_metadata(timeout=2.0)
        dt_ms = (time.perf_counter() - t0) * 1000
        print(
            f"sequential get(N={n}):  {dt_ms:.2f} ms  ({dt_ms / n * 1000:.1f} µs/PV)"
        )

    # ── bulk_get(N) — warm + median-of-20, matching bench_vs_aioca_p4p.py
    # so the two scripts report the same steady-state metric. The first
    # call's result is asserted non-None so the timing rows can't be
    # silently faking a fast path on empty dicts.
    for n in (10, 25, 48):
        names = PV_POOL[:n]
        warm_result = ctx.bulk_get(names, timeout=3.0)  # warm + sanity check
        assert all(v is not None for v in warm_result.values()), (
            f"bulk_get(N={n}) returned None for "
            f"{[k for k, v in warm_result.items() if v is None]}"
        )
        per_call_us = []
        for _ in range(20):
            t = time.perf_counter()
            ctx.bulk_get(names, timeout=3.0)
            per_call_us.append((time.perf_counter() - t) * 1e6)
        med_us = statistics.median(per_call_us)
        print(
            f"bulk_get(N={n}):  {med_us / 1000:.2f} ms  ({med_us / n:.1f} µs/PV, median of 20)"
        )

    # ── bulk_get_async(N) — awaitable variant; one asyncio.run covers
    # all N so event-loop setup amortizes.
    async def _time_all_bulk_async():
        out = []
        for nn in (10, 25, 48):
            nms = PV_POOL[:nn]
            await ctx.bulk_get_async(nms, timeout=3.0)  # warm
            per_call_us_a = []
            for _ in range(20):
                t = time.perf_counter()
                await ctx.bulk_get_async(nms, timeout=3.0)
                per_call_us_a.append((time.perf_counter() - t) * 1e6)
            out.append((nn, statistics.median(per_call_us_a)))
        return out

    for n, med_us in asyncio.run(_time_all_bulk_async()):
        print(
            f"bulk_get_async(N={n}):  {med_us / 1000:.2f} ms  ({med_us / n:.1f} µs/PV, median of 20)"
        )


if __name__ == "__main__":
    print(f"PV pool size: {len(PV_POOL)}")
    print(f"Sample count for latency: {N_GET_SAMPLES}")
    addr_list = os.environ.get("EPICS_CA_ADDR_LIST", "(unset — broadcast)")
    auto_addr = os.environ.get("EPICS_CA_AUTO_ADDR_LIST", "(unset — default YES)")
    print(f"EPICS_CA_ADDR_LIST: {addr_list}")
    print(f"EPICS_CA_AUTO_ADDR_LIST: {auto_addr}")
    bench_pyepics()
    bench_epicsrs()
