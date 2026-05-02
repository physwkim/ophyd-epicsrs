# Detector Module Roadmap

The next step for ophyd-epicsrs: introduce a new `ophyd_epicsrs.detector` module that selectively ports ophyd-async's detector abstractions, while keeping vanilla ophyd for motors, scalers, and plain Signals with zero migration.

## Background

### Current ophyd-epicsrs (v0.5.x)
- Swaps ophyd's control layer to a Rust CA backend via `use_epicsrs()`
- All existing ophyd Devices and plans run unchanged
- Performance gains: GIL-free I/O, `bulk_caget` (~1500x), `bulk_connect_and_prefetch`
- Limitation: still inherits ophyd's `ADBase` / `ADComponent` / Plugin tree, so adding a new detector carries heavy boilerplate and plugin-chain rewiring

### Backend readiness (epics-rs, post 2026-04-29 closeout)
The detector module can build on epics-rs without waiting on backend hardening:
- **CA**: P1–P8 stability overhaul (multi-NIC discovery via per-interface UDP tasks + `if-addrs` beacon fanout, `EPICS_CA_NAME_SERVERS` long-lived TCP, server-side TCP keepalive + inactivity timeout, bounded monitor queue with backpressure, full ECA error table, max-channels / max-subs caps, beacon chained-frame walking) plus G1–G4 closeout — functionally on par with C `libca`/`rsrv` for both single- and multi-subnet deployments.
- **PVA**: `EPICS_PVA{,S}_*` env vars, multi-NIC beacon fanout, `Channel::alternatives` multi-server failover, NT builders, type cache decode — functionally on par with `pvxs` for production workloads.
- **Exceeds upstream**: capability tokens with revocation, TLS cert hot reload, signed beacons + verifier, drain mode, chaos harness, differential tests vs `softIoc`, end-to-end benchmarks.

**Implication**: a unified Rust backend covers both CA and PVA from day 1 — the detector module does not need a CA-only first pass followed by a PVA second pass.

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
- **Do not port detector / path / status abstractions** — superseded by the decision below.
- **Use ophyd-async as a required dependency and ship a `SignalBackend` adapter only.** Users get `StandardDetector`, `PathProvider`, `AsyncStatus`, and every existing ophyd-async detector wrapper (Pilatus, Eiger, PandABlocks, …) for free, running on the Rust epics-rs transport via `EpicsRsSignalBackend`.

bluesky's RunEngine already supports mixed plans containing both `Status` (sync ophyd) and `AsyncStatus` (async ophyd-async) devices, so user plan code requires no changes.

The work that genuinely needs to live in this repo is **semantic parity with `aioca` / `p4p` at the SignalBackend layer**: `wait`, callback event-loop affinity, enum / table / datakey conversion, cancellation / timeout. Detector composition and file-path policy belong upstream.

## Architecture

```
User code
  ├── motor, scaler, plain Signal
  │     → ophyd (vanilla) + use_epicsrs()         [unchanged]
  │
  └── new detector
        → ophyd_async.core.StandardDetector  (or any upstream detector class)
            ├── TriggerLogic / ArmLogic / DataLogic[]   ← upstream
            └── PathProvider                            ← upstream
                  ↓
            ophyd_async.core.Signal[T]
                  ↓ (backend swap)
            ophyd_epicsrs.detector.EpicsRsSignalBackend
                  ↓
            EpicsRsPV  /  EpicsRsPvaPV  (Rust, tokio)
                  ↓
            epics-rs   [unified CA + PVA via shared Arc<Runtime>]
```

Core principles:
1. **Single shared Rust runtime** — both entry points (sync `EpicsRsShimPV` and async `EpicsRsPV.*_async` / `EpicsRsPvaPV.*_async`) use the same `Arc<Runtime>`. Avoids runtime fragmentation.
2. **No PV cache** — the same PV may be accessed concurrently from sync ophyd and the async backend without conflict; channel sharing is already handled at the transport layer.
3. **bluesky mixed plans** — `Status` and `AsyncStatus` coexist in a single plan; users do not need to know which protocol a device implements.
4. **Unified CA + PVA backend** — unlike ophyd-async (which uses `aioca` for CA and `p4p` for PVA, two FFI surfaces with different threading models), ophyd-epicsrs exposes both protocols through one Rust runtime. The single `EpicsRsSignalBackend` dispatches by `pv://` prefix; modern detectors (NTNDArray-based areaDetector, PVA-native PandABlocks) work without a parallel binding stack.

## Build Plan

### LOC estimate (~400 LOC adapter + ~300 LOC tests, ~2 person-weeks)

| Component | LOC | Location | Status |
|-----------|-----|----------|--------|
| pyo3-async-runtimes bridge (`*_async` methods) | ~300 | `crates/ophyd-epicsrs/src/{pv,pva,runtime}.rs` | done |
| Async `EpicsRsSignalBackend` (CA + PVA dispatch) | ~200 | `python/ophyd_epicsrs/detector/_signal_backend.py` | done |
| Factory functions (`epicsrs_signal_{r,rw,rw_rbv,w,x}`) | ~120 | `python/ophyd_epicsrs/detector/_factory.py` | done |
| Datatype-aware converters (bool / int / float / str / Enum / Array1D / Sequence / Table) | ~340 | `python/ophyd_epicsrs/detector/_converter.py` | done |
| Tests against softIoc + mock | ~300 | `python/ophyd_epicsrs/detector/tests/` | next |

### Phases

**Phase 1 — Async PV primitives (Rust)**: Add `pyo3-async-runtimes` based async methods to `EpicsRsPV` and `EpicsRsPvaPV` while preserving the existing sync surface. Verify single-runtime sharing.  ✅ done

**Phase 2 — `EpicsRsSignalBackend` adapter (Python)**: Implement ophyd-async's `SignalBackend[T]` ABC over the async PV primitives. Includes datatype-aware converters (Bool / Int / Float / Str / Enum / Array1D / Sequence / Table) and `pv://`-prefix protocol dispatch.  ✅ done

**Phase 3 — Semantic parity with `aioca` / `p4p`**: Make the adapter behave indistinguishably from `aioca`-backed and `p4p`-backed signals for the test suites that ship in ophyd-async.  Includes (in this branch): `EpicsOptions.wait` plumbing, `loop.call_soon_threadsafe` callback bridge, enum bidirectional conversion, non-blocking PVA monitor registration.  ✅ done

**Phase 4 — Real camera integration**: End-to-end validation with a production beamline camera (Pilatus, Eiger, or similar) constructed from upstream ophyd-async, backend-swapped to `EpicsRsSignalBackend`.  next

## Out of Scope

The following are intentionally **not** built in this repo — they live upstream in ophyd-async:

- `StandardDetector`, `TriggerLogic`, `ArmLogic`, `DataLogic` — use upstream
- `PathProvider` family — use upstream
- `AsyncStatus`, `WatchableAsyncStatus` — use upstream
- Per-detector wrappers (Pilatus, Eiger, Aravis, PandABlocks, …) — use upstream
- Motor / sim / plan_stub modules — use vanilla ophyd
- Tango backend — EPICS only

CA and PVA are **both in scope** from day 1 via the shared epics-rs backend.

## Technical Considerations

### Cancellation
On Python `task.cancel()`, the corresponding Rust Future must `Drop` cleanly so any in-flight CA or PVA request aborts. Verify cancel-safety in epics-rs CA and PVA client paths (one known minor gap: CA G1 "TCP send timeout" SHOULD-FIX from the 2026-04-29 re-audit; tracked separately in epics-rs).

### Event loop binding
`pyo3-async-runtimes` bridges Python's asyncio loop with the tokio runtime. Awaiting on the wrong thread can deadlock against the GIL. Convention: every async entry point goes through `pyo3_async_runtimes::tokio::future_into_py`. Monitor callbacks fire on a Rust dispatch thread; `EpicsRsSignalBackend.set_callback` captures the running loop and uses `loop.call_soon_threadsafe` to deliver each `Reading` on the loop thread (asyncio.Event / Queue inside ophyd-async's signal cache are not thread-safe).

### bluesky AsyncStatus protocol
ophyd-async ships its own `AsyncStatus` implementation; this repo does not provide one.

### `EpicsOptions.wait` semantics
`wait=False` (and callable variants — e.g. busy / acquire records that hang on Acquire=0) routes through `put_nowait_async`, which on CA fires `CA_PROTO_WRITE` (no notify) and on PVA spawns the put without awaiting the response.

## Success Criteria

1. Existing ophyd user code runs unchanged, line for line
2. ophyd-async detector classes constructed from upstream work backend-swapped onto `EpicsRsSignalBackend`
3. The adapter passes ophyd-async's existing `aioca` / `p4p` SignalBackend test suites (semantic parity)
4. bluesky mixed plans (sync motor + async detector) work end to end
5. The same PV can be accessed concurrently from sync and async paths

## References

- ophyd-async `StandardDetector`: `ophyd-async/src/ophyd_async/core/_detector.py:326`
- ophyd-async `PathProvider`: `ophyd-async/src/ophyd_async/core/_path_providers.py:60`
- ophyd-async `SignalBackend`: `ophyd-async/src/ophyd_async/core/_signal_backend.py:63`
- Current ophyd-epicsrs Rust PV: `crates/ophyd-epicsrs/src/pv.rs`
- Current ophyd-epicsrs Python shim: `python/ophyd_epicsrs/_shim.py`
- epics-rs CA stability: P1–P8 overhaul commit `d280e1cb` (2026-04-28), G1–G4 closeout commit `5a2c019` (2026-04-29)
- epics-rs PVA stability: post-2026-04-28 closeout, P-G1..P-G4 closeout commit `3ab410e` (2026-04-29)
