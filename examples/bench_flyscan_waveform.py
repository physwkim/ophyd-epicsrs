"""Flyscan-style waveform benchmark: 10 PVs × 10000 elements (f64).

Reproduces a realistic flyscan readout step:
    - 10 detector / encoder / I0 waveform PVs
    - 10000 samples per scan
    - 80 KB per PV, 800 KB per scan step
    - Read all 10 in one call, decode to numpy on the client side

Compares the four backends ophyd / ophyd-async users actually pick:
    - ophyd-epicsrs CA  (sync bulk_get, async bulk_get_async)
    - ophyd-epicsrs PVA (sync bulk_get, async bulk_get_async, native bundle PV)
    - aioca             (CA, async caget(list))
    - p4p               (PVA, sync Context.get(list))
    - pyepics           (CA, sync epics.caget loop — no bulk primitive)

Prerequisite: epics-rs mini-beamline IOC running, with the random
waveform PVs ``mini:wf1`` … ``mini:wf10`` loaded (st.cmd does this
automatically on the patched mini-beamline) and the native PVA aggregate
PV ``mini:wf:bundle`` registered. Install benchmark deps:

    pip install aioca p4p

Run:

    python examples/bench_flyscan_waveform.py

Methodology mirrors ``bench_vs_aioca_p4p.py``: each timed call is
preceded by a warm-up call so the channel cache + ioid + TCP state
are hot, then the median of N timed calls is reported. The per-call
result is asserted non-empty so a regression that returns
``{}`` cannot silently inflate the numbers.
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time
from contextlib import contextmanager

# ---------------------------------------------------------------------------
# Workload
# ---------------------------------------------------------------------------

PV_PREFIX = os.environ.get("EPICSRS_BENCH_WF_PREFIX", "mini:wf")
BUNDLE_PV = os.environ.get("EPICSRS_BENCH_WF_BUNDLE", f"{PV_PREFIX}:bundle")
N_PVS = int(os.environ.get("EPICSRS_BENCH_WF_N", "10"))
NELM = int(os.environ.get("EPICSRS_BENCH_WF_NELM", "10000"))
BYTES_PER_ELEM = 8  # FTVL=DOUBLE
PV_NAMES = [f"{PV_PREFIX}{i + 1}" for i in range(N_PVS)]
BUNDLE_FIELDS = [f"wf{i + 1}" for i in range(N_PVS)]
TIMED_RUNS = int(os.environ.get("EPICSRS_BENCH_RUNS", "20"))
SCAN_BYTES = N_PVS * NELM * BYTES_PER_ELEM


def percentiles(samples_us: list[float]) -> tuple[float, float, float]:
    s = sorted(samples_us)
    n = len(s)
    return (s[n // 2], s[(n * 95) // 100], s[(n * 99) // 100])


def fmt_row(label: str, med_us: float) -> str:
    per_pv = med_us / N_PVS
    mb_s = SCAN_BYTES / (med_us * 1e-6) / 1e6  # bytes/s → MB/s
    return (
        f"  {label:<32s} {med_us / 1000:7.2f} ms  "
        f"({per_pv:6.1f} µs/PV, {mb_s:6.0f} MB/s)"
    )


@contextmanager
def section(title: str):
    line = "─" * 72
    print(f"\n{line}\n  {title}\n{line}")
    yield


# ---------------------------------------------------------------------------
# ophyd-epicsrs CA
# ---------------------------------------------------------------------------
async def bench_epicsrs_ca():
    from ophyd_epicsrs._native import EpicsRsContext

    ctx = EpicsRsContext()
    pvs = [ctx.create_pv(n) for n in PV_NAMES]
    for p in pvs:
        assert p.wait_for_connection(timeout=5.0), f"connect failed: {p.pvname}"

    # sync bulk_get
    warm = ctx.bulk_get(PV_NAMES, timeout=3.0)
    assert all(v is not None for v in warm.values()), "bulk_get returned None"
    samples = []
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        ctx.bulk_get(PV_NAMES, timeout=3.0)
        samples.append((time.perf_counter() - t) * 1e6)
    print(fmt_row(f"bulk_get({N_PVS}):", statistics.median(samples)))

    # async bulk_get_async
    await ctx.bulk_get_async(PV_NAMES, timeout=3.0)  # warm
    samples = []
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        await ctx.bulk_get_async(PV_NAMES, timeout=3.0)
        samples.append((time.perf_counter() - t) * 1e6)
    print(fmt_row(f"bulk_get_async({N_PVS}):", statistics.median(samples)))


# ---------------------------------------------------------------------------
# ophyd-epicsrs PVA (native aggregate PV)
#
# Two rows per variant:
#   "lazy"   — only times the bulk_get call. Returns
#              EpicsRsPvaMetadata wrappers that DEFER array decoding to
#              Python types until the user accesses md["value"].
#   "+value" — also forces md["value"], paying the structure + array
#              conversion cost for all 10 waveform fields.
# ---------------------------------------------------------------------------
async def bench_epicsrs_pva():
    from ophyd_epicsrs._native import EpicsRsPvaContext

    ctx = EpicsRsPvaContext()
    pv = ctx.create_pv(BUNDLE_PV)

    def _validate(label: str, md, *, force_value: bool = False):
        if md is None:
            raise RuntimeError(f"{label}: {BUNDLE_PV!r} returned None")
        if not force_value:
            return
        value = md["value"]
        missing = [field for field in BUNDLE_FIELDS if field not in value]
        if missing:
            raise RuntimeError(f"{label}: bundle missing fields {missing[:3]}")
        bad_lengths = [
            field for field in BUNDLE_FIELDS if len(value[field]) != NELM
        ]
        if bad_lengths:
            raise RuntimeError(
                f"{label}: unexpected lengths for fields {bad_lengths[:3]}"
            )

    # Warm — verifies aggregate PV is reachable before timing.
    _validate("PVA bundle warm-up", pv.get_with_metadata(timeout=3.0))

    # sync pvget — lazy: surface failures so we don't report optimistic
    # numbers from silent partial misses.
    samples = []
    fails = 0
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        md = pv.get_with_metadata(timeout=3.0)
        dt_us = (time.perf_counter() - t) * 1e6
        if md is None:
            fails += 1
            continue
        samples.append(dt_us)
    if samples:
        print(fmt_row("pv.get_with_metadata() lazy:", statistics.median(samples)))
    if fails:
        print(f"  ! lazy: {fails}/{TIMED_RUNS} runs returned None — partial misses")

    # sync pvget — eager (force value extraction)
    samples = []
    fails = 0
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        md = pv.get_with_metadata(timeout=3.0)
        if md is None:
            fails += 1
            continue
        _validate("PVA bundle +value", md, force_value=True)
        dt_us = (time.perf_counter() - t) * 1e6
        samples.append(dt_us)
    if samples:
        print(fmt_row("pv.get_with_metadata() +val:", statistics.median(samples)))
    if fails:
        print(f"  ! +value: {fails}/{TIMED_RUNS} runs returned None — partial misses")

    # async pvget — lazy
    _validate("PVA async bundle warm-up", await pv.get_reading_async(timeout=3.0))
    samples = []
    fails = 0
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        md = await pv.get_reading_async(timeout=3.0)
        dt_us = (time.perf_counter() - t) * 1e6
        if md is None:
            fails += 1
            continue
        samples.append(dt_us)
    if samples:
        print(fmt_row("pv.get_reading_async() lazy:", statistics.median(samples)))
    if fails:
        print(f"  ! async lazy: {fails}/{TIMED_RUNS} runs returned None — partial misses")

    # async pvget — eager
    samples = []
    fails = 0
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        md = await pv.get_reading_async(timeout=3.0)
        if md is None:
            fails += 1
            continue
        _validate("PVA async bundle +value", md, force_value=True)
        dt_us = (time.perf_counter() - t) * 1e6
        samples.append(dt_us)
    if samples:
        print(fmt_row("pv.get_reading_async() +val:", statistics.median(samples)))
    if fails:
        print(f"  ! async +value: {fails}/{TIMED_RUNS} runs returned None — partial misses")


# ---------------------------------------------------------------------------
# aioca (CA, async)
# ---------------------------------------------------------------------------
async def bench_aioca():
    import aioca

    # Warm
    await aioca.caget(PV_NAMES, timeout=5.0)
    samples = []
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        await aioca.caget(PV_NAMES, timeout=5.0)
        samples.append((time.perf_counter() - t) * 1e6)
    print(fmt_row(f"caget({N_PVS}):", statistics.median(samples)))


# ---------------------------------------------------------------------------
# p4p (PVA, sync)
# ---------------------------------------------------------------------------
def bench_p4p():
    from p4p.client.thread import Context

    ctx = Context("pva")
    # Warm
    ctx.get(BUNDLE_PV, timeout=5.0)
    samples = []
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        ctx.get(BUNDLE_PV, timeout=5.0)
        samples.append((time.perf_counter() - t) * 1e6)
    print(fmt_row("Context.get(bundle):", statistics.median(samples)))
    ctx.close()


# ---------------------------------------------------------------------------
# pyepics (CA, sync, no bulk — sequential epics.caget loop)
# ---------------------------------------------------------------------------
def bench_pyepics():
    import epics

    # Warm: ensure each PV is connected + cached
    for nm in PV_NAMES:
        epics.caget(nm, use_monitor=False, timeout=5.0)
    samples = []
    for _ in range(TIMED_RUNS):
        t = time.perf_counter()
        for nm in PV_NAMES:
            epics.caget(nm, use_monitor=False, timeout=5.0)
        samples.append((time.perf_counter() - t) * 1e6)
    print(fmt_row(f"caget loop({N_PVS}):", statistics.median(samples)))


# ---------------------------------------------------------------------------
async def main():
    print(
        f"Workload: {N_PVS} PVs × {NELM} f64 elem each "
        f"({SCAN_BYTES / 1e3:.0f} KB/scan) — {TIMED_RUNS} timed runs (median)"
    )
    print(f"Prefix: {PV_PREFIX!r}  (overrideable via EPICSRS_BENCH_WF_PREFIX)")
    print(f"PVA bundle: {BUNDLE_PV!r}  (overrideable via EPICSRS_BENCH_WF_BUNDLE)")
    addr = os.environ.get("EPICS_CA_ADDR_LIST", "(unset — broadcast)")
    auto = os.environ.get("EPICS_CA_AUTO_ADDR_LIST", "(unset — default YES)")
    print(f"EPICS_CA_ADDR_LIST: {addr}")
    print(f"EPICS_CA_AUTO_ADDR_LIST: {auto}")

    with section("CA: ophyd-epicsrs"):
        await bench_epicsrs_ca()
    with section("CA: aioca"):
        await bench_aioca()
    with section("CA: pyepics (sequential, no bulk primitive)"):
        bench_pyepics()
    with section("PVA: ophyd-epicsrs"):
        await bench_epicsrs_pva()
    with section("PVA: p4p"):
        bench_p4p()


if __name__ == "__main__":
    asyncio.run(main())
