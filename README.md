# ophyd-epicsrs

Rust EPICS Channel Access backend for [ophyd](https://github.com/bluesky/ophyd).

Replaces pyepics (`Python → ctypes → libca.so`) with [epics-rs](https://github.com/epics-rs/epics-rs) (`Python → PyO3 → Rust CA client`), releasing the GIL during all network I/O.

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

```python
import ophyd
ophyd.set_cl("epicsrs")

# Or via environment variable
# OPHYD_CONTROL_LAYER=epicsrs

# All ophyd devices now use the Rust CA backend
motor = ophyd.EpicsMotor("IOC:m1", name="motor1")
motor.wait_for_connection(timeout=5)
print(motor.read())
```

Or with the convenience function:

```python
from ophyd_epicsrs import use_epicsrs_backend
use_epicsrs_backend()
```

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

### GIL-released bulk operations

`bulk_caget` reads multiple PVs concurrently using tokio `join_all`, completing in a single network round-trip with the GIL released. See the [Parallel PV Read](#parallel-pv-read-bulk_caget) section above.

## Architecture

```
ophyd (Python)
  └── _epicsrs_shim.py          ophyd control layer interface
        └── ophyd_epicsrs        this package
              └── _native.so     PyO3 bindings
                    └── epics-rs pure Rust CA/PVA client (no libca.so)
```

### GIL behavior

| Operation | GIL |
|-----------|-----|
| CA get / put | **released** — `py.allow_threads()` → tokio async |
| CA monitor receive | **released** — tokio background task |
| Monitor callback → Python | **held** — dispatch thread |
| Connection wait | **released** — tokio async |
| bulk_caget | **released** — tokio join_all |

### Key types

- **`EpicsRsContext`** — Shared tokio runtime + CA client. One per session.
- **`EpicsRsPV`** — PV channel wrapper with `wait_for_connection`, `get_with_metadata`, `put`, `add_monitor_callback`.

## Requirements

- Python >= 3.8
- ophyd >= 1.7
- [epics-rs](https://github.com/epics-rs/epics-rs) (bundled at build time)

## Related

- [bluesky-dataforge](https://github.com/physwkim/bluesky-dataforge) — Rust-accelerated document subscriber + async MongoDB writer
- [epics-rs](https://github.com/epics-rs/epics-rs) — Pure Rust EPICS implementation

## License

BSD 3-Clause
