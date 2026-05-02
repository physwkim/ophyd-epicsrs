# ophyd-epicsrs

Rust EPICS backend for [ophyd](https://github.com/bluesky/ophyd) and [ophyd-async](https://github.com/bluesky/ophyd-async) — supports both Channel Access (CA) and pvAccess (PVA).

Replaces pyepics (`Python → ctypes → libca.so`) with [epics-rs](https://github.com/epics-rs/epics-rs) (`Python → PyO3 → Rust client`), releasing the GIL during all network I/O. CA and PVA share a single tokio runtime — no separate `aioca` + `p4p` binding stacks. Sync (legacy ophyd) and async (ophyd-async, asyncio) call paths share the same runtime, channel cache, and monitor subscriptions.

## Installation

```bash
pip install ophyd-epicsrs
```

Building from source requires a Rust toolchain (1.85+):

```bash
pip install maturin
maturin develop
```

## Usage

Call `use_epicsrs()` once at startup, before constructing any ophyd Signals or Devices:

```python
from ophyd_epicsrs import use_epicsrs
use_epicsrs()

# All ophyd devices now use the Rust CA backend
import ophyd
motor = ophyd.EpicsMotor("IOC:m1", name="motor1")
motor.wait_for_connection(timeout=5)
print(motor.read())
```

`use_epicsrs()` assigns `ophyd.cl` directly. It must be called before any
`Signal` or `Device` is constructed, since they capture `ophyd.cl.get_pv`
at construction time.

## PVA support

PVs are dispatched by name prefix (pvxs / ophyd-async convention):

```python
import ophyd
from ophyd_epicsrs import use_epicsrs
use_epicsrs()

# CA (default — preserves existing ophyd code)
sig_ca = ophyd.EpicsSignal("IOC:foo")
sig_ca = ophyd.EpicsSignal("ca://IOC:foo")    # explicit prefix also works

# PVA (NTScalar / NTScalarArray / NTEnum)
sig_pva = ophyd.EpicsSignal("pva://IOC:bar")
```

The PVA backend supports the standard NT (Normative Type) shapes:
NTScalar, NTScalarArray, NTEnum, and NTTable (with typed `PvField`
columns derived from `Table.__annotations__` so dtype information is
preserved through the wire format). The NTScalar `value`, `alarm.severity`,
`alarm.status`, `timeStamp.{secondsPastEpoch, nanoseconds}`, and
`display.{units, precision, limitLow, limitHigh}` fields are projected
onto the ophyd metadata dict so existing Signals/Devices receive the
same keys they expect from CA.

NTNDArray (the raw image-carrying PV) is not decoded into a numpy
array on the Python side. This matches how ophyd-async's standard
`StandardDetector` pattern uses areaDetector PVs — image bytes go
from the camera's HDF5 plugin straight to disk, and bluesky receives
Resource/Datum events rather than ndarrays. The companion control
PVs (`ArrayCounter_RBV`, `Capture_RBV`, `FilePath`, `AcquireTime`,
etc.) are NTScalar / NTEnum / string and work today. Live-preview or
alignment paths that *do* want frames in Python are not yet covered.

## ophyd-async support (`ophyd_epicsrs.ophyd_async`)

For [ophyd-async](https://github.com/bluesky/ophyd-async)-based devices,
the package ships factory functions that return standard ophyd-async
`SignalR` / `SignalRW` / `SignalW` / `SignalX` instances backed by
epics-rs. No fork required — they drop straight into `StandardDetector`,
`StandardReadable`, plan stubs, etc.

```python
from ophyd_epicsrs.ophyd_async import (
    epicsrs_signal_r,
    epicsrs_signal_rw,
    epicsrs_signal_rw_rbv,
    epicsrs_signal_w,
    epicsrs_signal_x,
)

# Bare name and ca://… → CA backend; pva://… → PVA backend.
sig_ca  = epicsrs_signal_rw(float, "IOC:motor.RBV", "IOC:motor.VAL")
sig_pva = epicsrs_signal_rw(float, "pva://IOC:nt:scalar")

await sig_pva.connect()
await sig_pva.set(0.5)
print(await sig_pva.get_value())
```

Under the hood, `EpicsRsSignalBackend` implements ophyd-async's
`SignalBackend[T]` ABC and routes by URL prefix to the appropriate
native client. The package includes datatype-aware converters covering
the full ophyd-async type surface: `bool`, `int`, `float`, `str`,
`Enum` / `StrictEnum` / `SubsetEnum` / `SupersetEnum`, `npt.NDArray`,
`Sequence`, and `Table`. IOC schema is validated against the requested
datatype at connect time using PVA `pvinfo`, so type mismatches surface
as a clear error during `connect()` rather than a silent corruption at
first read.

## Async surface

Both `EpicsRsPV` (CA) and `EpicsRsPvaPV` (PVA) expose `*_async` methods
that return Python awaitables, in addition to the sync methods used by
ophyd. The async path goes through `pyo3-async-runtimes` and shares the
same tokio runtime as the sync path — no runtime fragmentation, same
channel cache, mixed use against the same PV is safe.

```python
from ophyd_epicsrs._native import EpicsRsContext, EpicsRsPvaContext
import asyncio

ctx_ca = EpicsRsContext()
ctx_pva = EpicsRsPvaContext()

async def main():
    pv_ca = ctx_ca.create_pv("IOC:motor.RBV")
    pv_pva = ctx_pva.create_pv("IOC:nt:scalar")

    # Wait for connection in parallel
    ok_ca, ok_pva = await asyncio.gather(
        pv_ca.connect_async(timeout=5.0),
        pv_pva.connect_async(timeout=5.0),
    )

    # Read value (scalar) or full reading (value + alarm + timestamp + display)
    val = await pv_ca.get_value_async()
    reading = await pv_pva.get_reading_async()

    # Write — returns True on success
    ok = await pv_ca.put_async(0.5)

asyncio.run(main())
```

Available async methods on both CA and PVA wrappers:

- `connect_async(timeout) -> bool`
- `get_value_async(timeout) -> Any`
- `get_reading_async(timeout, form) -> dict | None`
- `put_async(value, timeout) -> bool`
- `connect_and_prefetch_async(timeout) -> None` — single round-trip
  connect + metadata fetch
- `get_field_desc_async(timeout) -> dict | None` — PVA `pvinfo`
  introspection (CA: returns `None`)

The sync surface (`wait_for_connection`, `get_with_metadata`, `put`, etc.)
remains unchanged — existing ophyd code works exactly as before.

## Parallel PV Read (bulk_caget)

Read multiple PVs concurrently in a single call. All CA requests are sent simultaneously using tokio async, completing in one network round-trip instead of N sequential reads.

```python
from ophyd_epicsrs import EpicsRsContext

ctx = EpicsRsContext()
data = ctx.bulk_caget([
    "IOC:enc_wf",
    "IOC:I0_wf",
    "IOC:ROI1:total_wf",
    "IOC:ROI2:total_wf",
    # ... 수십~수백 개 PV
], timeout=5.0)
# Returns dict: {"IOC:enc_wf": array, "IOC:I0_wf": array, ...}
```

### Fly Scan Acceleration

Combine `bulk_caget` with [bluesky-dataforge](https://github.com/physwkim/bluesky-dataforge)'s `AsyncMongoWriter` for maximum fly scan throughput:

```python
from ophyd_epicsrs import EpicsRsContext
from bluesky_dataforge import AsyncMongoWriter
import numpy as np
import time

ctx = EpicsRsContext()
writer = AsyncMongoWriter("mongodb://localhost:27017", "metadatastore")
RE.subscribe(writer)  # replaces RE.subscribe(db.insert)

# In your flyer's collect_pages():
def collect_pages(self):
    # 1. Parallel PV read — all waveforms in ~1ms
    pvnames = [self.enc_wf_pv, self.i0_wf_pv]
    pvnames += [f"ROI{r}:total_wf" for r in range(1, self.numROI + 1)]
    raw = ctx.bulk_caget(pvnames)

    # 2. Deadtime correction (numpy, fast)
    enc = np.array(raw[self.enc_wf_pv])[:self.numPoints]
    i0 = np.array(raw[self.i0_wf_pv])[:self.numPoints]
    rois = {f"ROI{r}": np.array(raw[f"ROI{r}:total_wf"])[:self.numPoints]
            for r in range(1, self.numROI + 1)}

    # 3. Yield single EventPage — one bulk insert instead of N row inserts
    now = time.time()
    ts = [now] * self.numPoints
    data = {"ENC": enc.tolist(), "I0": i0.tolist(), **{k: v.tolist() for k, v in rois.items()}}
    timestamps = {k: ts for k in data}

    yield {
        "data": data,
        "timestamps": timestamps,
        "time": ts,
        "seq_num": list(range(1, self.numPoints + 1)),
    }
    # → AsyncMongoWriter receives EventPage
    # → Rust background thread: BSON conversion + insert_many
    # → Python is free to start the next scan immediately

writer.flush()  # wait for all pending inserts after scan
```

**Before (sequential):**
```
read PV1 (30ms) → read PV2 (30ms) → ... → read PV50 (30ms) = 1500ms
yield row1 → db.insert (5ms) → yield row2 → db.insert (5ms) → ... = 500ms
Total: ~2000ms
```

**After (parallel + EventPage):**
```
bulk_caget(50 PVs) = 1ms
numpy deadtime = 1ms
yield 1 EventPage → AsyncMongoWriter.enqueue → 0.1ms
Total: ~2ms (Python free), MongoDB insert continues in background
```

## Performance

### Versus pyepics — honest like-for-like benchmark

Run on the same machine, same mini-beamline IOC, same PV pool, same
connection state (warm). Reproducible from
`tests/integration/bench_vs_pyepics.py`:

| Operation | pyepics | epicsrs | Comment |
|-----------|---------|---------|---------|
| Single PV cached get (`PV.get()` after monitor) | **p50 2 µs** | p50 61 µs | pyepics returns the cached monitor value with no network round-trip; `EpicsRsPV.get_with_metadata` always issues a fresh CA read. Different semantics. |
| Single PV fresh CA read | ~100 µs avg (`epics.caget()`) | **p50 61 µs** | Same network round-trip; epicsrs releases the GIL during the read. |
| Sequential fresh reads, 48 PVs (`caget` loop) | 4.71 ms | **2.28 ms** | 2.1× — fewer C↔Python crossings + per-call GIL releases. |
| `bulk_caget(48)` | n/a (no bulk primitive) | **2.60 ms** | Same wall time as the sequential loop above because the IOC + LAN are fast enough that the per-PV GIL drops dominate; the *real* `bulk_caget` win is at higher latencies (see flyer scenario below). |

**Where the much larger speedups show up:**

- **Sluggish IOC / WAN link, 50 PVs.** Sequential pyepics adds N
  round-trip latencies; `bulk_caget` adds one. At 30 ms RTT this is
  the difference between ~1.5 s and ~30 ms — the original "1500×"
  number in earlier README revisions came from this regime, but it
  was not labelled honestly.
- **Device connect with many PVs.** The legacy ophyd path issues
  per-PV `wait_for_connection` calls serialised by the GIL.
  `bulk_connect_and_prefetch` collects all unconnected PVs and
  connects them concurrently in a single tokio call — see
  *Device-level bulk connect* below.
- **Mixed sync + async usage in the same process.** With pyepics +
  aioca/p4p, you pay for two separate EPICS stacks (separate
  channels, separate threads). With epicsrs both surfaces share one
  backend.

**Where pyepics wins:** the cached-monitor `PV.get()` path. If your
hot loop is reading a value that already has an active monitor and
you don't need fresh metadata, pyepics's in-process cache is hard to
beat. `EpicsRsShimPV.get` (the legacy-ophyd surface) does cache
monitor values too, so the gap mostly closes when you go through the
ophyd Signal layer rather than calling `_native.get_with_metadata`
directly.

The put→get improvement (single-owner writer task + `TCP_NODELAY`)
remains unchanged from earlier releases — it eliminates the ~45 ms
head-of-line blocking that occurred when reads waited for writes to
flush.

### Reproducible mini-beamline measurements

The integration suite (`tests/integration/test_performance.py`)
measures a fixed set of operations against the mini-beamline IOC from
[epics-rs/examples/mini-beamline](https://github.com/epics-rs/epics-rs/tree/main/examples/mini-beamline).
Anyone can reproduce these numbers — just run the integration suite
locally with the IOC up, or trigger the nightly CI workflow. Numbers
below are local Apple Silicon, IOC and tests on the same host:

| Operation | Result |
|-----------|--------|
| Single `get_with_metadata` latency (200 samples) | p50 **54 µs** · p95 **81 µs** · p99 **135 µs** |
| `bulk_caget(10)` | **0.63 ms** (63 µs/PV) |
| `bulk_caget(25)` | **1.32 ms** (53 µs/PV) |
| `bulk_caget(48)` | **2.63 ms** (55 µs/PV) |
| ophyd-async parallel connect (30 PVs) | **4.1 ms** |
| ophyd-async parallel connect (3 × StandardReadable, 9 PVs) | **3.4 ms** |
| ophyd sync `Device` connect (4 components) | **55 ms** |
| ophyd sync `Device` connect (DCM, 9 PVs incl. 3 motors) | **51 ms** |
| ophyd sync `Device` connect (areaDetector cam, 11 PVs) | **52 ms** |

Note the ~15× gap between ophyd-async parallel connect (4 ms for 30
PVs) and ophyd sync `Device.wait_for_connection` (~50 ms for 9–11 PVs):
both go through the same `_native` backend, but the sync path issues
per-PV `wait_for_connection` calls serialised by the GIL, while the
async path's `asyncio.gather(...)` overlaps every PV's connect inside
the single tokio runtime. Mixed sync + async usage works (same
backend, same circuit per IOC), so the recommended migration path is
"new device classes in ophyd-async, legacy classes left as-is".

## Advantages over pyepics backend

### Zero-latency monitor callbacks

In the pyepics backend, all monitor callbacks are queued through ophyd's dispatcher thread:

```
EPICS event → C libca → pyepics callback → dispatcher queue → ophyd callback
```

This queuing introduces latency. When a motor moves fast, the DMOV (done-moving) signal transitions 0→1 quickly, but the callback is stuck behind hundreds of RBV position updates in the queue. This causes `EpicsMotor.move(wait=True)` to return before the motor actually stops — the well-known **"another set call is still running"** problem.

The epicsrs backend eliminates this by firing monitor callbacks **directly from the Rust thread**, bypassing the dispatcher queue entirely:

```
EPICS event → Rust tokio → ophyd callback (direct)
```

Rust's thread safety guarantees (Send/Sync traits, GIL-aware PyO3) make this safe without additional locking. The result: DMOV transitions are never missed, regardless of motor speed.

### No PV cache — safe Device re-creation

The pyepics backend caches PV objects by name. Creating a second ophyd Device with the same PV prefix (e.g. switching xspress3 detector channels) causes subscription conflicts because two Devices share one PV object.

The epicsrs backend creates a fresh PV object per `get_pv()` call. The Rust runtime handles TCP connection sharing (virtual circuits) at the transport layer, so there is no performance penalty. Multiple Devices with the same PV prefix work independently.

### Device-level bulk connect

When an ophyd Device (e.g. areaDetector with 200+ PVs) calls `wait_for_connection()`, the epicsrs backend collects all unconnected PVs and connects them in a single bulk operation:

```
pyepics:   PV1 connect+read → PV2 connect+read → ... → PV200 connect+read
           200 sequential GIL round-trips, each blocking on network I/O

epicsrs:   collect 200 PVs → bulk_connect_and_prefetch(200 PVs)
           1 GIL release → tokio: 200 connects + 200 reads in parallel → 1 GIL return
```

This is a structural advantage that pyepics cannot match: libca processes CA reads sequentially at the Python level (`PV.get()` blocks one at a time), while epicsrs crosses the Python↔Rust boundary once and runs all network I/O concurrently in the tokio runtime.

The speedup scales with PV count — a 200-PV areaDetector Device initializes in ~30ms instead of several seconds.

### GIL-released bulk read

`bulk_caget` reads multiple PVs concurrently using tokio `join_all`, completing in a single network round-trip with the GIL released. See the [Parallel PV Read](#parallel-pv-read-bulk_caget) section above.

## Reliability

Spawned tokio tasks (monitor delivery, connection-event watchers,
pyo3-log forwarding) may execute callbacks into Python while the
interpreter is being finalized — typically during pytest fixture
teardown or normal process exit. A `Python::with_gil` call in that
window panics; in a spawned task that panic would normally crash the
process.

Every such call site is wrapped with `safe_warn!` / `safe_call!` /
`safe_call_or!` macros that `catch_unwind` the panic, increment a
process-wide counter, and write a one-line stderr notice on the *first*
caught panic. The counter is exposed for telemetry:

```python
from ophyd_epicsrs import caught_panic_count
print(caught_panic_count())  # 0 in normal operation
```

`panic = "unwind"` is enforced at compile time via a
`#[cfg(panic = "abort")] compile_error!` so a downstream `Cargo.toml`
cannot silently disarm the guards.

## Architecture

```
ophyd (sync)              ophyd-async (asyncio)
  │                         │
  └── ophyd.cl              └── ophyd_epicsrs.ophyd_async
        │                         │ (EpicsRsSignalBackend)
        └── ophyd_epicsrs._shim   │
              │                   │
              └─→ ophyd_epicsrs._native (PyO3 bindings) ←─┘
                    │
                    ├── EpicsRsContext / EpicsRsPV       (CA)
                    └── EpicsRsPvaContext / EpicsRsPvaPV (PVA)
                                │
                                └── epics-rs (pure Rust, no libca.so)
                                      └── shared tokio runtime
```

### GIL behavior

| Operation | GIL |
|-----------|-----|
| CA / PVA get / put | **released** — `py.allow_threads()` → tokio async |
| Monitor receive | **released** — tokio background task |
| Monitor callback → Python | **held** — dispatch thread |
| Connection wait | **released** — tokio async |
| bulk_caget | **released** — tokio join_all |
| `*_async` methods | **released** — `pyo3-async-runtimes` future |

### Key types

- **`EpicsRsContext`** / **`EpicsRsPvaContext`** — Shared tokio runtime + CA / PVA client. One of each per session.
- **`EpicsRsPV`** / **`EpicsRsPvaPV`** — PV channel wrappers. Sync surface (`wait_for_connection`, `get_with_metadata`, `put`, `add_monitor_callback`) plus `*_async` siblings.
- **`ophyd_epicsrs.ophyd_async.EpicsRsSignalBackend`** — `ophyd-async` `SignalBackend` implementation; routes `pva://` / `ca://` / bare names to the appropriate native client and applies the datatype-aware converter for the requested ophyd-async type. The factory functions (`epicsrs_signal_rw` etc.) wrap this and are the recommended entry point.

## Logging

Rust-side `tracing` events are bridged to Python's `logging` module via
[`pyo3-log`](https://crates.io/crates/pyo3-log). Standard configuration
applies:

```python
import logging
logging.getLogger("ophyd_epicsrs.ca").setLevel(logging.WARN)
logging.getLogger("ophyd_epicsrs.pva").setLevel(logging.DEBUG)
```

`pyo3-log` caches the level lookup for ~30 s. Call
`ophyd_epicsrs.reset_log_cache()` after changing levels at runtime to
force re-check on the next event.

## Requirements

- Python >= 3.10
- ophyd >= 1.9 (vanilla PyPI — no fork required)
- ophyd-async >= 0.16 (only required if you use `ophyd_epicsrs.ophyd_async`)
- bluesky >= 1.13
- [epics-rs](https://github.com/epics-rs/epics-rs) >= 0.13 (bundled at build time)
- Rust toolchain >= 1.85 (build-time only)

## Related

- [bluesky-dataforge](https://github.com/physwkim/bluesky-dataforge) — Rust-accelerated document subscriber + async MongoDB writer
- [epics-rs](https://github.com/epics-rs/epics-rs) — Pure Rust EPICS implementation

## License

BSD 3-Clause
