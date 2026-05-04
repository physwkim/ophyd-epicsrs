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
from ophyd_epicsrs import get_ca_context, get_pva_context
import asyncio

async def main():
    pv_ca = get_ca_context().create_pv("IOC:motor.RBV")
    pv_pva = get_pva_context().create_pv("IOC:nt:scalar")

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

## Parallel PV Read (`bulk_get` / `bulk_get_async`)

Read multiple PVs concurrently in a single call. Per-server READ_NOTIFY frames are batched into one TCP write (libca-style flush) and the GIL is released for the entire round-trip — measured at **1.4 µs/PV (CA)** and **0.7 µs/PV (PVA)** at 100 PVs.

Both protocols share the same API. CA → `EpicsRsContext`, PVA → `EpicsRsPvaContext`. Each context exposes:

- `bulk_get(names, timeout=5.0) -> dict[str, value | None]` — sync (blocks the calling thread, GIL released; safe for ophyd / threading workflows)
- `bulk_get_async(names, timeout=5.0) -> Awaitable[dict]` — returns a Python awaitable (asyncio / ophyd-async / RunEngine integration; does not block the event loop)

```python
from ophyd_epicsrs import get_ca_context

ctx = get_ca_context()
data = ctx.bulk_get([
    "IOC:enc_wf",
    "IOC:I0_wf",
    "IOC:ROI1:total_wf",
    "IOC:ROI2:total_wf",
    # ... 수십~수백 개 PV
], timeout=5.0)
# Returns dict: {"IOC:enc_wf": array, "IOC:I0_wf": array, ...}

# asyncio / ophyd-async path
async def collect():
    data = await ctx.bulk_get_async(names, timeout=5.0)
    ...
```

Failed PVs (timeout / disconnect) appear in the dict as `None` — the caller can distinguish "missing key" from "read failed".

### Fly Scan Acceleration

Combine `bulk_get` with [bluesky-dataforge](https://github.com/physwkim/bluesky-dataforge)'s `AsyncMongoWriter` for maximum fly scan throughput:

```python
from ophyd_epicsrs import get_ca_context
from bluesky_dataforge import AsyncMongoWriter
import numpy as np
import time

ctx = get_ca_context()
writer = AsyncMongoWriter("mongodb://localhost:27017", "metadatastore")
RE.subscribe(writer)  # replaces RE.subscribe(db.insert)

# In your flyer's collect_pages():
def collect_pages(self):
    # 1. Parallel PV read — all waveforms in ~1ms
    pvnames = [self.enc_wf_pv, self.i0_wf_pv]
    pvnames += [f"ROI{r}:total_wf" for r in range(1, self.numROI + 1)]
    raw = ctx.bulk_get(pvnames)

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

## Performance

All numbers below are reproducible. Same machine (Apple Silicon),
same mini-beamline IOC over LAN, same PV pool, warm channel state,
median of 20 timed calls. Single-PV latency rows use 200/500 samples.
Run `examples/bench_vs_aioca_p4p.py` and `examples/bench_vs_pyepics.py`
to regenerate.

### CA: ophyd-epicsrs vs aioca

100 PVs from the mini-beamline IOC. Both backends issue fresh CA
READ_NOTIFY round-trips (no value cache).

| Operation | aioca | ophyd-epicsrs | ratio |
|-----------|-------|---------------|-------|
| Single warm get (fresh CA read) | p50 **97 µs** | p50 **71 µs** | 1.4× |
| Bulk read 100 PVs (sync) | 2434 µs (24.3 µs/PV) | **142 µs** (1.4 µs/PV) | **17×** |
| Bulk read 100 PVs (async) | n/a (no bulk primitive) | **163 µs** (1.6 µs/PV) | — |
| Connect 100 PVs | 40.4 ms (`connect+1get`) | **5.5 ms** | **7×** |

aioca has no `connect-without-get`, so its "connect" line includes
one full read. Even bench_aioca's `caget(list)` issues N concurrent
ops via `asyncio.gather` — the 17× gap on bulk read comes from
batching frames into a single TCP write per server and avoiding
per-PV libca callback dispatch.

### PVA: ophyd-epicsrs vs p4p

| Operation | p4p | ophyd-epicsrs | ratio |
|-----------|-----|---------------|-------|
| Single warm get (fresh) | p50 **107 µs** | p50 **91 µs** | 1.2× |
| Per-PV `await` × 100 (gather) | 2703 µs (27.0 µs/PV) | 2539 µs (25.4 µs/PV) | 1.1× |
| Bulk read 100 PVs (sync) | n/a | **72 µs** (0.7 µs/PV) | **38×** vs `Context.get(list)` |
| Bulk read 100 PVs (async) | n/a | **90 µs** (0.9 µs/PV) | — |
| Connect 100 PVs | 15.6 ms (`connect+1get`) | **1.9 ms** | **8×** |

`Context.get([list])` in p4p does parallel issue under the hood but
each PV is its own op object with its own callback — no protocol-
level batched get exists in PVA, so the win is again in how
ophyd-epicsrs collapses N ops into one Rust spawn + one `pvget_many`.

### CA: ophyd-epicsrs vs pyepics

48 PVs, fresh CA reads on both sides (pyepics calls use
`use_monitor=False` so we are NOT comparing against the cached
attribute path).

| Operation | pyepics | ophyd-epicsrs | ratio |
|-----------|---------|---------------|-------|
| Single warm get (fresh CA read) | p50 **63 µs** | p50 **96 µs** | 0.66× |
| Sequential 48 × `PV.get` | 2.95 ms (61 µs/PV) | 3.05 ms (64 µs/PV) | 0.97× |
| Loop 48 × `epics.caget(name)` | 5.09 ms (106 µs/PV) | n/a — use `bulk_get` | — |
| Bulk read 48 PVs (sync) | n/a (no bulk primitive) | **0.08 ms** (1.8 µs/PV) | **60×** vs `epics.caget` loop |
| Bulk read 48 PVs (async) | n/a | **0.11 ms** (2.3 µs/PV) | — |

pyepics is the closest competitor on single-PV fresh-read latency —
libca via ctypes is a thin wrapper, so the network round-trip
dominates and there is no Python overhead to remove. **The dramatic
gains live in the bulk APIs and connect time, not single-PV
latency.**

### Where the gains come from

- **Bulk reads (1.4 µs/PV CA, 0.7 µs/PV PVA at 100 PVs).** Per-server
  READ_NOTIFY frames are batched into one TCP write (libca-style
  flush). aioca/p4p issue per-PV ops via `asyncio.gather` — each
  pays callback dispatch + GIL crossings.
- **Connect time.** aioca/p4p have no `connect-without-get`, so
  connect cost includes a full read round-trip. ophyd-epicsrs
  separates them; raw connect on 100 PVs = 5.5 ms (CA) / 1.9 ms (PVA).
- **Single tokio runtime for CA + PVA.** No double EPICS stack
  overhead in mixed sync/async workflows. With pyepics + aioca/p4p
  you carry separate channels and separate threads per backend.
- **GIL-released bulk paths.** Both `bulk_get` (sync, blocks calling
  thread but releases GIL) and `bulk_get_async` (asyncio-friendly)
  issue all network I/O outside the GIL.

### Where it does NOT help

- **Single warm cached-monitor reads.** pyepics `PV.get()` with
  `auto_monitor=True` returns a Python attribute (~2 µs). The
  ophyd-epicsrs counterpart is `EpicsRsShimPV.get` going through the
  ophyd Signal layer (which caches monitor values too); bare
  `EpicsRsPV.get_with_metadata` always hits the wire by design.
- **Single fresh CA read.** Within ±50% of pyepics (±25% of aioca).
  Network round-trip dominates; per-call Python overhead is in the
  noise.

### `bulk_get` vs `bulk_get_async`

The async variant adds ~15-25 µs over the sync variant per call
(asyncio task scheduling + `future_into_py_fast` overhead). Use
`bulk_get_async` when you need the calling event loop to keep
servicing other coroutines during the round-trip — the canonical
case is a RunEngine plan that interleaves bulk reads with monitor
callbacks, motor-move waits, or other devices' I/O. For sync ophyd
or threading workflows, `bulk_get` is strictly cheaper.

### Reproducibility & honest framing

- Single-PV `single warm get` rows are intentionally **fresh** reads
  on both sides. pyepics `PV.get()` defaults to
  `use_monitor=True` (returns the cached monitor attribute, ~2 µs);
  the bench script overrides this with `use_monitor=False`, and
  ophyd-epicsrs's `get_with_metadata` always issues a wire round-trip
  by design.
- Bulk paths are warmed once before the timed loop populates the
  internal channel cache — the median-of-20 number is the
  steady-state per-call cost, NOT including the first call's connect
  and channel-cache fill (~1 ms for 100 PVs).
- The benchmark scripts assert `all(v is not None for v in result.values())`
  before timing, so a regressed empty-dict fast path cannot silently
  inflate the numbers.

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

`bulk_get` (sync) and `bulk_get_async` (asyncio) read multiple PVs concurrently using tokio `join_all`, completing in a single network round-trip with the GIL released. See the [Parallel PV Read](#parallel-pv-read-bulk_get--bulk_get_async) section above.

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
| bulk_get / bulk_get_async | **released** — tokio join_all |
| `*_async` methods | **released** — `pyo3-async-runtimes` future |

### Key types

- **`EpicsRsContext`** / **`EpicsRsPvaContext`** — Shared tokio runtime + CA / PVA client. **Process-wide singleton** — obtain via `ophyd_epicsrs.get_ca_context()` / `get_pva_context()`, do not construct directly. Multiple clients in one process trip spurious `first_sighting` beacon anomalies in `epics-ca-rs` that drop healthy TCP circuits under load. `shutdown_all()` releases the singletons (refuses while any PV wrapper is alive).
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
