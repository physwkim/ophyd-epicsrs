# Detector Module Roadmap

The next step for ophyd-epicsrs: introduce a new `ophyd_epicsrs.detector` module that selectively ports ophyd-async's detector abstractions, while keeping vanilla ophyd for motors, scalers, and plain Signals with zero migration.

## Background

### Current ophyd-epicsrs (v0.5.x)
- Swaps ophyd's control layer to a Rust CA backend via `use_epicsrs()`
- All existing ophyd Devices and plans run unchanged
- Performance gains: GIL-free I/O, `bulk_caget` (~1500x), `bulk_connect_and_prefetch`
- Limitation: still inherits ophyd's `ADBase` / `ADComponent` / Plugin tree, so adding a new detector carries heavy boilerplate and plugin-chain rewiring

### Where ophyd-async wins on detectors
ophyd-async's `StandardDetector` decomposes a detector along **three orthogonal axes**:

| Axis | Responsibility | ABC |
|------|----------------|-----|
| Trigger | prepare internal / edge / level trigger modes | `DetectorTriggerLogic` |
| Arm | arm / wait_for_idle / disarm lifecycle | `DetectorArmLogic` |
| Data | produce a `ReadableDataProvider` (step) or `StreamableDataProvider` (fly); multiple instances allowed per detector | `DetectorDataLogic` |

When the same camera must be used across (trigger mode × output format) variants, ophyd's class hierarchy explodes combinatorially while ophyd-async assembles at instance time — additive, not multiplicative cost. `PathProvider` complements this by centralizing site-wide file-path policy at a single point.

This modeling does not exist in upstream ophyd, and AI code generation cannot reproduce it through pattern application alone — it is an architectural decision.

## Decision

- **Do not fork ophyd-async** — tracking ~30k LOC upstream would dilute focus and forfeit the zero-migration USP.
- **Do not migrate to a fully async API** — preserve existing ophyd users and code.
- **Port only ophyd-async's detector layer into ophyd-epicsrs** — invest where the architectural payoff is highest.

bluesky's RunEngine already supports mixed plans containing both `Status` (sync ophyd) and `AsyncStatus` (async detector) devices, so user plan code requires no changes.

## Architecture

```
User code
  ├── motor, scaler, plain Signal
  │     → ophyd (vanilla) + use_epicsrs()         [unchanged]
  │
  └── new detector
        → ophyd_epicsrs.detector.StandardDetector
            ├── TriggerLogic
            ├── ArmLogic
            ├── DataLogic [multiple]
            └── PathProvider
                  ↓
            await sig.set / get  (pyo3-asyncio)
                  ↓
            EpicsRsPV  (Rust, tokio)
                  ↓
            epics-rs CA / PVA   [shared Arc<Runtime>]
```

Core principles:
1. **Single shared Rust runtime** — both entry points (sync `EpicsRsShimPV` and async `EpicsRsPV.*_async`) use the same `Arc<Runtime>`. Avoids runtime fragmentation.
2. **No PV cache** — the same PV may be accessed concurrently from sync ophyd and the async detector module without conflict; channel sharing is already handled at the transport layer.
3. **bluesky mixed plans** — `Status` and `AsyncStatus` coexist in a single plan; users do not need to know which protocol a device implements.

## Build Plan

### LOC estimate (~3,500 LOC, 1–2 person-months)

| Component | LOC | Location |
|-----------|-----|----------|
| pyo3-asyncio bridge (`put_async`, `get_async`, `monitor_async`) | ~300 | extend `crates/ophyd-epicsrs/src/pv.rs` |
| Logic ABCs + StandardDetector | ~600 | `python/ophyd_epicsrs/detector/_core.py` |
| PathProvider family | ~400 | `python/ophyd_epicsrs/detector/_path.py` |
| AsyncStatus | ~150 | `python/ophyd_epicsrs/detector/_status.py` |
| areaDetector core (HDF5 DataLogic + ROI + Stats) | ~1000 | `python/ophyd_epicsrs/detector/adcore/` |
| Tests (mock SignalBackend, no IOC required) | ~1000 | `python/ophyd_epicsrs/detector/tests/` |

### Phases

**Phase 1 — Async PV primitives (Rust)**: Add `pyo3-asyncio` based async methods to `EpicsRsPV` while preserving the existing sync surface. Verify single-runtime sharing.

**Phase 2 — Core abstraction (Python)**: Port `_detector.py`, `_path_providers.py`, and `_status.py` from ophyd-async. Define TriggerLogic / ArmLogic / DataLogic ABCs, `StandardDetector`, `AsyncStatus`, and the `PathProvider` family.

**Phase 3 — areaDetector adcore**: Port the HDF5 writer DataLogic and standard plugins (ROI, Stats). Reference `ophyd-async/src/ophyd_async/epics/adcore/`.

**Phase 4 — Real camera integration**: End-to-end validation with a production beamline camera (Pilatus, Eiger, or similar).

## Out of Scope

The following are intentionally **not** ported:

- ophyd-async's motor / sim / plan_stub modules — vanilla ophyd is sufficient
- The full `Signal[T]` abstraction — wrap only as needed inside detector code
- ophyd-async's fastcs / PandABlocks integration — evaluate separately later
- Tango backend — EPICS only

## Technical Considerations

### pyo3-asyncio cancellation
On Python `task.cancel()`, the corresponding Rust Future must `Drop` cleanly so any in-flight CA request aborts. Verify cancel-safety in epics-rs CA client paths.

### Event loop binding
pyo3-asyncio bridges Python's asyncio loop with the tokio runtime. Awaiting on the wrong thread can deadlock against the GIL. Enforce a convention: every async entry point goes through `pyo3_asyncio::tokio::future_into_py`.

### bluesky AsyncStatus protocol
Match the exact signature of `bluesky.protocols.Status` and `AsyncStatus`. Confirm RunEngine's `_wait_for` path handles our implementation correctly.

## Success Criteria

1. Existing ophyd user code runs unchanged, line for line
2. A new detector can be defined in ~200 LOC (3–5x reduction vs. ophyd)
3. Logic components are unit-testable without an IOC
4. bluesky mixed plans (sync motor + async detector) work end to end
5. The same PV can be accessed concurrently from sync and async paths

## References

- ophyd-async `StandardDetector`: `ophyd-async/src/ophyd_async/core/_detector.py:326`
- ophyd-async `PathProvider`: `ophyd-async/src/ophyd_async/core/_path_providers.py:60`
- ophyd-async `SignalBackend`: `ophyd-async/src/ophyd_async/core/_signal_backend.py:63`
- Current ophyd-epicsrs Rust PV: `crates/ophyd-epicsrs/src/pv.rs`
- Current ophyd-epicsrs Python shim: `python/ophyd_epicsrs/_shim.py`
