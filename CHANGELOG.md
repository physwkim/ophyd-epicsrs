# Changelog

## v0.9.0 (2026-05-06)

CA bulk-read hot-path tuning + bulk API consolidation. The four
distinct bulk methods from v0.8.0 (`bulk_caget`, `bulk_get_pvs`,
`bulk_pvaget`, `bulk_get_pvs_pva`) collapse into a single uniform
shape on each context, plus an async variant for asyncio /
ophyd-async callers.

### Public API — breaking

- **`EpicsRsContext.bulk_caget` → `bulk_get`** (CA, sync). Same
  semantics; the CA-specific suffix dropped now that PVA mirrors the
  same name. Internally now uses a bounded by-name `CaChannel` cache
  (LRU, 4096 entries) so repeated calls skip CA create/search and
  route hot channels through `CaClient::get_many_with_timeout`.
- **`EpicsRsContext.bulk_get_pvs` removed.** The pre-cached-PV variant
  is no longer needed because `bulk_get`'s channel cache already
  delivers the same hot-path performance from PV names.
- **`EpicsRsPvaContext.bulk_pvaget` → `bulk_get`** (PVA, sync).
- **`EpicsRsPvaContext.bulk_get_pvs_pva` removed**, same reason.
- **New: `bulk_get_async(names, timeout=5.0)`** on both CA and PVA
  contexts. Returns a Python awaitable — drop-in for asyncio /
  ophyd-async / RunEngine integration; does not block the event
  loop.

Migration: rename `bulk_caget` → `bulk_get`, `bulk_pvaget` →
`bulk_get`, and replace `bulk_get_pvs*` calls with the by-name
`bulk_get` (the channel cache makes the explicit pre-cached-PV path
redundant).

### CA backend — bulk_get hot-path

- Bounded by-name `CaChannel` cache (`BulkGetCache`, LRU 4096) shared
  by sync + async paths so repeat-name reads skip CA create/search.
- One-shot retry on `Disconnected` / `ChannelNotFound`: PVs that miss
  the first `get_many` pass `wait_connected` once and the batch
  re-issues `get_many` for just the recovered channels — a stale cache
  entry no longer poisons the whole batch.
- Shared `run_bulk_get` core for both `bulk_get` and `bulk_get_async`,
  so they have identical batching + retry semantics.

### PVA backend — bulk_get correctness

- `bulk_get` now spawns N concurrent `pvget` tokio tasks instead of
  routing through `PvaClient::pvget_many`. The batched warm-path in
  `pvget_many` reuses the same `ioid` for every GET after the first
  INIT+GET; against an `epics-bridge-rs` qsrv-fronted IOC the server
  rejects every reused-ioid GET, so `pvget_many` returned `None` on
  every PV after the first. `op_get` (called by per-PV `pvget`) has
  an internal cold-path fallback when the warm GET fails, so the
  per-PV path is correct.
- Trade-off: we lose the single-TCP-write batching `pvget_many`
  offered on the happy path. Concurrent op_get hops still parallelise
  cleanly via tokio scheduling. Acceptable until `pvget_many` gains
  its own warm-failure cold fallback upstream.

### Benchmarks

- `examples/bench_flyscan_waveform.py` — fly-scan waveform throughput
  benchmark (300 lines).
- `tests/integration/bench_vs_pyepics.py` → `examples/bench_vs_pyepics.py`
  (moved out of the test tree; honest like-for-like vs pyepics).
- `tests/integration/bench_vs_others.py` → `examples/bench_vs_aioca_p4p.py`
  (renamed; comparative bench vs aioca / p4p, atexit `os._exit(0)`
  guard so racing CA/PVA libraries no longer segfault on shutdown
  and clobber the output).

### README

- "Parallel PV Read" section rewritten around the unified `bulk_get`
  / `bulk_get_async` API. Documents both protocols, sync + async,
  with the failure-as-`None` semantics called out.

## v0.8.0 (2026-05-04)

Major performance pass — every Rust↔Python hot path on the CA + PVA
side has been optimised. Across 100-PV bulk reads on localhost,
ophyd-epicsrs is now **5×** faster than aioca (CA) and **11×** faster
than p4p (PVA); single warm get is at parity or better.

### New public APIs

- **`EpicsRsContext.bulk_get_pvs(pvs, timeout)`** — CA bulk read for
  pre-cached PVs. Routes through epics-ca-rs's `get_many` (libca-style
  per-server frame batching). 100 PVs in ~400 µs vs ~4.6 ms via
  `bulk_caget`.
- **`EpicsRsPvaContext.bulk_pvaget(pvnames, timeout)`** — PVA bulk
  read by name (re-creates channels each call).
- **`EpicsRsPvaContext.bulk_get_pvs_pva(pvs, timeout)`** — PVA bulk
  read for pre-cached PVs. Routes through epics-pva-rs's
  `pvget_many` with per-server combined-frame batching + warm-GET
  intro cache. 100 PVs in ~210 µs (~2 µs/PV).

### Performance wins

- **PVA single warm get**: 166 µs → 97 µs (1.7× faster).
- **PVA bulk(100)**: 2.4 ms → 210 µs (11× faster, beats CA bulk).
- **CA single warm get**: 80 µs → 71 µs.
- **CA bulk(100)**: 4.6 ms → 400 µs (11× faster).

Driven by:
- (A) `DirectServerWriter` sync send path for PVA writer
- (B) `IoidSlot::TwoShot(VecDeque<oneshot>)` — per-op alloc removed
- (C) `DashMap<u32, IoidSlot>` lock-free response router
- (D) Warm-GET introspection cache (`Reusable` slot + `cached_get` +
  `IoidGuard::defuse`) — skips INIT round-trip on subsequent gets
- (E) Per-server frame batching in `pvget_many` (groups N GETs into
  one TCP write, mirrors `CaClient::get_many`)
- (F) `pyo3-async-runtimes::tokio::future_into_py_fast` (custom fork
  patch — no cancellation propagation, no contextvars, no panic-
  detection wrapper) for short-lived single-shot async futures
- Direct CPython FFI fast path in `pvfield_to_py` for the common
  NTScalar value types (Double, Long, Int)
- Lock-free `is_ntenum` (`AtomicU8`) replaces `Arc<Mutex<Option<bool>>>`
- Lazy `EpicsRsPvaMetadata` mapping wrapper — only materialises a
  full Python dict when callers iterate / `.pop` / `.get` for a
  missing key

### New examples + docs

- **`examples/bench_vs_others.py`** — comparative benchmark vs aioca
  (CA) and p4p (PVA). Reports single warm get latency (p50/p95/p99),
  bulk parallel scaling (10 / 20 / 50 / 100 PVs), connect time, and
  monitor throughput. Adds the `bulk_get_pvs` / `bulk_pvaget` /
  `bulk_get_pvs_pva` rows so the recommended API is the visible
  winner.
- **`docs/pva_single_get_async_perf.md`** — explains the PVA async
  single-get bridge cost breakdown, why we shipped
  `future_into_py_fast` (97 µs, true async), and why we rejected the
  resolved-coroutine helper bypass (87 µs but breaks `gather()`).

### Dependencies

- **epics-rs 0.13.6 → 0.14.0** (transitive: `epics-base-rs`,
  `epics-ca-rs`, `epics-pva-rs`, `epics-macros-rs` all to 0.14.0).
  Brings the bulk `caget_many` / `get_many` (CA) and `pvget_many`
  (PVA, now batched) APIs this release exposes.
- **pyo3-async-runtimes** switched from crates.io 0.24 to a git
  branch on `physwkim/pyo3-async-runtimes:perf-opt` (forked from
  v0.24.0). Adds `future_into_py_fast` for the short-lived single-
  shot use case. Will move back to crates.io once the patch lands
  upstream.

## v0.7.0 (2026-05-03)

Minor version bump — significant additions to the test surface and
example scripts. No public API breaks.

### Tests — vendored upstream ophyd suite

New ``tests/integration/upstream/`` directory with 13 ophyd test
files adapted to run against the mini-beamline IOC instead of the
caproto / XF:31IDA test IOCs the originals expect:

- Pure-logic tests (no IOC needed): ``test_device``, ``test_hints``,
  ``test_kind``, ``test_log``, ``test_main``, ``test_ophydobj``,
  ``test_positioner``, ``test_status``, ``test_utils``,
  ``test_versioning``, ``test_sim``.
- Real-IOC tests with mini-beamline PV remap: ``test_epicsmotor``
  (against ``mini:ph:mtr``), ``test_signalpositioner``,
  ``test_signal`` (read_only / read_write / pair_set / pair_rbv /
  bool_enum mapped to mini PVs).
- ``conftest.py`` wires ``use_epicsrs_backend()`` (new alias),
  ``TEST_CL=epicsrs``, and the mini-beamline-flavoured ``motor``
  / ``signal_test_ioc`` / ``cleanup`` fixtures every upstream test
  expects.

Total integration test count rose from 118 → 353 across 3 stability
runs, all clean (transient IOC reachability is the only skip
trigger).

### Examples

- ``examples/mini_beamline.py`` — end-to-end CA / sync ophyd demo:
  composite Devices (PointDetector, DCM, XYStage), RunEngine with
  ``count`` / ``scan`` / ``rel_scan`` / ``grid_scan`` / DCM theta
  move. Inline document_printer subscriber so the script runs
  without a databroker.
- ``examples/mini_beamline_async.py`` — ophyd-async + PVA
  counterpart: ``StandardReadable`` composites, ``StrictEnum``
  NTEnum round-trip exercising the ``pvput_field("value.index")``
  routing added in v0.6.7, ``init_devices()`` parallel connect
  (~40 ms for 8 PVA devices on localhost vs ~2 s for the 10 CA
  devices in the sister script).

### Public API

- ``use_epicsrs_backend = use_epicsrs`` alias added so external
  consumers (e.g. the ``physwkim/epicsrs-tests`` repo) can use
  either name.

### Dependencies

- **epics-rs 0.13.4 → 0.13.6** (transitive: ``epics-base-rs``,
  ``epics-ca-rs``, ``epics-pva-rs``, ``epics-macros-rs`` all to
  0.13.6).

## v0.6.9 (2026-05-03)

### Bug fixes

- **`bulk_caget` per-read timeout**: `CaChannel::get()` had no built-in
  deadline, so `bulk_caget(timeout=N)` only applied `N` to
  `wait_connected` — a single channel stuck mid-reconnect (upstream
  beacon-anomaly chain — see `python/ophyd_epicsrs/_contexts.py`)
  would block the whole bulk call for ~30 s while the rest of the
  parallel reads sat idle. Now wraps each `ch.get()` in
  `tokio::time::timeout(dur, …)` so reads fail fast at the user-
  supplied budget.

### Dependencies

- **epics-rs 0.13.1 → 0.13.4** (transitive: `epics-base-rs`,
  `epics-ca-rs`, `epics-pva-rs`, `epics-macros-rs` all to 0.13.4).

### Test suite hygiene

- **README**: replaced direct `EpicsRsContext()` / `EpicsRsPvaContext()`
  examples with `get_ca_context()` / `get_pva_context()` so users can't
  silently bypass the singleton (4 sites). Key types section now
  documents the singleton invariant.
- **Performance tests**: hardened against beacon-anomaly storms — perf
  budgets keep their soft warning print but skip the test on a 5×
  ceiling miss, so a transient outage no longer surfaces as a perf
  regression in CI.
- **`test_bulk_caget_many_pvs`**: skips after the single retry if the
  IOC is genuinely unreachable (was failing on persistent None).
- **`test_dcm_device_composition`**: skips on connect timeout instead
  of failing — the contract under test is Device composition, not
  connect latency under network turbulence.
- **`test_quad_bpm_device`**: removed (the IOC db never exposed those
  PVs under any of the candidate prefixes, so the test only ever
  skipped).

## v0.6.8 (2026-05-03)

### Dependency upgrade

- **epics-rs 0.13.0 → 0.13.1**. Pulls in the new `EpicsValue::Int64` /
  `EpicsValue::Int64Array` variants (DBR_INT64 native support, EPICS
  V7+ field type) plus everything in
  [epics-base-rs / epics-ca-rs / epics-pva-rs 0.13.1].

- **DBR_INT64 conversion arms** added to `convert.rs` — Python
  `int` ↔ `EpicsValue::Int64` (i64) for both scalar and array paths.
  Long-typed (i32) and Int64-typed (i64) PVs are now both routed to
  the right `EpicsValue` variant by the cached `native_type`.

## v0.6.7 (2026-05-03)

### Architecture — single CaClient / PvaClient per process

Three independent contexts (pyepics-compat shim, ophyd-async backend,
test fixture) each constructed their own `EpicsRsContext` /
`EpicsRsPvaContext`. Every fresh `CaClient` saw each IOC's first
beacon as a `first_sighting=true` anomaly
(`epics-ca-rs/src/client/beacon_monitor.rs:327`), which fired an
`EchoProbe` against every operational channel. Under load the IOC
could miss the 5 s echo deadline → `TcpClosed` →
`handle_disconnect` → "restored N subscriptions" reconnect storm
and timed-out gets/puts. With three clients × N IOCs the spurious
anomaly count multiplied accordingly.

- **`python/ophyd_epicsrs/_contexts.py`**: new module owning the
  process-wide singleton CA + PVA contexts. `_shim.py`,
  `_signal_backend.py`, and the test `ca_ctx` / `pva_ctx` fixtures
  all route through `get_ca_context()` / `get_pva_context()`.

- **Public API surface** (`__init__.py`): `EpicsRsContext` /
  `EpicsRsPvaContext` removed from `__all__` so user code can't
  silently bypass the singleton. `get_ca_context`, `get_pva_context`,
  and `shutdown_all` are the canonical entry points. `EpicsRsPV` /
  `EpicsRsPvaPV` stay public for `isinstance` / type annotations on
  `create_pv()` return values. Pyclass docstrings now warn against
  direct instantiation.

- **`shutdown_all()`** for long-running daemons that want to release
  the runtime / sockets when finished. Refuses (`RuntimeError`)
  while any `EpicsRsPV` / `EpicsRsPvaPV` wrapper is alive — without
  this guard the singleton slot would empty but the old `CaClient`
  would persist (PVs strongly reference it), and the next
  `get_ca_context()` would construct a SECOND client, re-triggering
  the multi-client anomaly chain. Backed by per-context
  `pv_count: Arc<AtomicUsize>` (`is_unused()` on the pyclass) that
  PVs increment in `create_pv` and decrement in `Drop`.

- **`EpicsRsContext::new`** releases the GIL via `py.allow_threads`
  while waiting for the spawned `CaClient::new` task — repeater
  registration / UDP setup no longer blocks other Python threads.

### Bug fixes — PVA NTEnum (11th-pass review)

- **`cache_native_type_async` cache-hit guard**: skip the pvget when
  `is_ntenum` is already cached (reconnect / non-NTEnum signals no
  longer pay a redundant round-trip).

- **`tracing::warn!` on pvget failure**: `cache_native_type_async`
  was silently swallowing transient I/O failures, leaving the user
  with a cold cache and `put_async(int)` quietly falling back to
  plain `pvput` (rejected by the IOC). Now logs a warning so the
  failure is at least visible.

- **`detect_ntenum_shape` forward-compat**: changed
  `struct_id == "epics:nt/NTEnum:1.0"` to
  `starts_with("epics:nt/NTEnum:")` so a future minor bump (1.1, ...)
  doesn't get misclassified as a different NT type.

- **`record_ntenum_into` extraction**: replaced the four inlined
  copies of the `is_ntenum` cache update with a single free function
  that both `&self` and `async move` paths share.

### Test hardening

Four tests intermittently failed in CI under the upstream beacon-
anomaly chain (all four passed in isolation). Hardened against
transient reconnect storms while preserving real regression
detection:

- **`test_ca_large_int16_waveform`**: was polling AreaDetector's
  `ArrayCounter_RBV` alone — bumps when the plugin RECEIVES the
  NDArray but `ArrayData` populates only after `processCallbacks`
  finishes the copy. Now polls BOTH counter AND array length.

- **`test_concurrent_get_and_monitor`**: tolerates up to 2 transient
  get timeouts; skips on full sustained outage; deadlock floor only
  fires when one side made progress and the other did not (the
  actual contract under test).

- **`test_bulk_caget_many_pvs`**: retries once with a generous
  timeout if any of the 15 PVs reads as `None` from a transient
  reconnect.

- **`test_async_device_parallel_connect_three_detectors`**: 2 s
  ceiling bumped to 5 s, with a 1 s soft warning print to surface
  persistent slowdowns without failing on a one-shot beacon storm.

## v0.6.6 (2026-05-02)

### Bug fixes — PVA NTEnum robustness (10th-pass review)

- **Write-PV NTEnum detection** (`cache_native_type_async`): was a
  no-op for PVA. For split read/write signals
  (`epicsrs_signal_rw(MyEnum, "pva://X_rbv", "pva://X_cmd")`) the
  write PV's `is_ntenum` flag was never set, so `put_async(int)` fell
  through to plain `pvput` and was silently rejected by the IOC.
  `cache_native_type_async` now does a one-shot `pvget` to detect
  the channel shape at connect time (failures are non-fatal).

- **Monitor-delta false negative** (`detect_ntenum_shape`): monitor
  events in epics-pva-rs deliver full structures, but as a defensive
  measure the detection logic now uses `struct_id` as the authoritative
  classifier. Previously, a partial structure (value sub-field absent
  from a delta) made `try_extract_ntenum` return `None`, which flipped
  the cache from `Some(true)` → `Some(false)` and broke the next
  `put_async(int)`. Now: `"epics:nt/NTEnum:1.0"` → confirmed NTEnum,
  other `"epics:nt/…"` → confirmed non-NTEnum, empty/unknown struct_id
  with failed extraction → `None` (no new information, cache preserved).

- **Code de-duplication**: extracted `detect_ntenum_shape(field: &PvField)`
  free function; all five detection sites now call it instead of
  inlining the pattern.

### Fixes

- Stale cross-reference in `test_pva_ntenum_put_index` docstring
  corrected to `test_async_pva_ntenum_via_ophyd_async_strict_enum`.

## v0.6.5 (2026-05-03)

### Bug fixes — async path completeness for the v0.6.4 changes

v0.6.4 fixed two real bugs (PVA NTEnum int put, CA CTRL-fields
cache) but only on the synchronous code path. Code review surfaced
that the async surface was untouched, so any caller using
`put_async` / `put_nowait_async` / `get_value_async` /
`get_reading_async` (which is everything that goes through
`ophyd_epicsrs.ophyd_async` and thus every ophyd-async signal) saw
the original broken behaviour. v0.6.5 fixes that:

- **PVA `put_async` / `put_nowait_async`**: now apply the same
  NTEnum routing as sync `put`. Without this, every
  `await sig.set(MyEnum.X)` against a PVA NTEnum signal silently
  no-op'd at the wire, with no error returned.

- **PVA `is_ntenum` cache**: now populated from `get_value_async`,
  `get_reading_async`, and the monitor dispatch path — not just
  sync `get_with_metadata`. Without this, async-only callers (the
  normal ophyd-async pattern: connect → subscribe → set) would
  never trigger NTEnum detection, so the put-routing fix above
  would have nothing to consult.

- **CA `cache_native_type_async`** consumed the prefetch and threw
  away its CTRL fields. The downstream `get_reading_async("ctrl")`
  in `EpicsRsSignalBackend.connect` then had to round-trip the IOC
  again to recover units / precision / limits / enum_strs. Capture
  those fields too — now the SignalBackend connect path is a
  single CTRL fetch, not two.

- **CA prefetch task** now eagerly populates `cached_ctrl` the
  moment the background DBR_CTRL read completes. async-only callers
  that never go through sync `get_with_metadata` previously got an
  empty CTRL cache on their first `get_reading_async("time")`,
  which silently dropped enum_strs / units / limits.

### Tests

- New `test_async_pva_ntenum_int_put_routes_via_field_path` in
  `tests/integration/test_pva_specific.py` — drives the async-only
  flow that the previous suite missed.
- New `test_async_pva_ntenum_via_ophyd_async_strict_enum` — full
  ophyd-async path with a `StrictEnum`-typed `SignalRW` against an
  NTEnum PV (`mini:KohzuModeBO`), exercising the
  `_EnumConverter.to_wire` → `put_async(int)` chain end-to-end.
- `test_rapid_create_drop_no_thread_leak` now rotates across 8
  distinct PV names instead of reusing a single name 200×, so each
  iteration creates a fresh CaChannel + per-PV spawned tasks
  (proves Drop semantics, not just channel-cache reuse).
- `_speed_up_sim_motors` fixture now snapshots and restores each
  motor's original VELO at teardown, so a long-running local IOC
  isn't left in a non-default state after the suite.
- All test + shim + signal-backend call sites switched from the
  back-compat alias `add_monitor_callback` to the canonical
  `set_monitor_callback`. The alias remains available.

## v0.6.4 (2026-05-03)

### Bug fixes

- **CA: cache CTRL fields across reads** — the first DBR_CTRL read
  (background prefetch or explicit `form="ctrl"`) ships
  `units`, `precision`, the `*_disp_limit` family, the
  `*_ctrl_limit` family, and `enum_strs`. Every subsequent
  `get_with_metadata` falls back to DBR_TIME, which carries only
  value + alarm + timestamp — so those fields silently disappeared
  from the metadata dict on the second and later calls. For mbbi /
  mbbo PVs the symptom was particularly visible: `char_value`
  degraded from the labelled string to the raw int index. A
  `cached_ctrl: Arc<Mutex<CachedCtrl>>` is now populated on every
  CTRL read and merged into every TIME read (sync get, async get,
  and monitor dispatch).

- **PVA: NTEnum int put silently no-op** — `EpicsRsPvaPV.put` used
  string-form `pvput(name, "1")` for every value. NTEnum has its
  scalar in `value.index` (the top-level value is a `{index,
  choices}` structure), and the server has no scalar slot for the
  parsed int — so the write was silently rejected, no error
  returned, no value change. The wrapper now caches NTEnum shape on
  the first `get_with_metadata` and routes int / bool puts through
  `pvput_field("value.index", ...)`. A put issued before any read
  still uses the string-form path; in practice callers do at least
  one read first (connect prefetch, status poll, etc.).

### API additions

- **`set_monitor_callback`** added on both `EpicsRsPV` and
  `EpicsRsPvaPV` as the canonical name for the monitor-callback
  registration. `add_monitor_callback` is preserved as a
  back-compat alias. The "add" prefix was always misleading: the
  implementation has set semantics (one callback, overwritten on
  re-register). Multi-callback fan-out belongs at the shim layer,
  which already wraps a single dispatcher around its Python-side
  `_callbacks` dict.

### Tests

- Integration suite expanded from 15 → 56 tests across 7 new files
  (bluesky plans, ophyd-async StandardReadable, multi-axis devices,
  wire-level datatype coverage, performance, robustness, PVA
  specifics). Wall time stays low (~25 s aggregate) thanks to a
  session-wide motor `VELO=500` bump that replaces the `motor.template`
  default of 1 unit/s.

## v0.6.3 (2026-05-03)

### Bug fixes

- **Shim's `pvname` now preserves the `pva://` / `ca://` prefix** —
  `get_pv("pva://NAME")` used to construct `EpicsRsShimPV` with the
  stripped bare name as its `pvname`. ophyd indexes per-PV state
  (`_received_first_metadata`, `_signals`) by the pvname string ophyd
  was originally handed; the moment the connection callback fires for
  a `pva://`-prefixed Signal, that lookup raised `KeyError`. Existing
  CA-only code is unaffected because no prefix means no name change.
  Caught by a fresh integration test against the mini-beamline IOC.

### Tests

- New live integration suite (`tests/integration/test_mini_beamline.py`,
  15 tests) running against the `mini-beamline` IOC from
  `epics-rs/examples/mini-beamline`. Covers CA + PVA × native + ophyd
  (sync) + ophyd-async (asyncio) frontends, plus `bulk_caget` timing,
  motor↔detector CP-link contrast, and an areaDetector
  Acquire/ArrayCounter cycle. The suite skips itself if the IOC is
  not reachable, so it is safe to run in dev environments without
  one.

### CI

- New `integration.yml` workflow: clones epics-rs, builds the
  mini-beamline IOC, starts it on loopback, runs the integration
  suite, then tears it down. Triggers on push to `main`, a 05:17 UTC
  nightly cron, and manual dispatch — PRs do *not* trigger it
  because the cold cargo build is ~5 min. `~/.cargo/registry` and
  `epics-rs/target/` are cached on `epics-rs/Cargo.lock`. IOC log is
  uploaded as an artifact on failure.

### Docs

- README: rephrased the NTNDArray paragraph to describe what the PVA
  backend does and does not cover today, without overstating the gap
  versus aioca + p4p (ophyd-async's `StandardDetector` pattern also
  doesn't pull NTNDArray frames into Python).

## v0.6.2 (2026-05-03)

### Breaking

- **`ophyd_epicsrs.detector` → `ophyd_epicsrs.ophyd_async`**: the
  subpackage that provides ophyd-async integration (SignalBackend
  adapter + datatype-aware converters + factory functions) was
  originally named `detector` based on a planned scope that included
  porting ophyd-async's `StandardDetector` / `TriggerLogic` /
  `PathProvider` layers. The actual scope shipped is just the
  `EpicsSignalBackend` adapter and its converters — pure
  ophyd-async integration glue, no detector abstractions. Renamed
  for accuracy.

  Migration: replace `from ophyd_epicsrs.detector import …` with
  `from ophyd_epicsrs.ophyd_async import …`. The exported names
  (`EpicsRsSignalBackend`, `epicsrs_signal_r`, `epicsrs_signal_rw`,
  `epicsrs_signal_rw_rbv`, `epicsrs_signal_w`, `epicsrs_signal_x`,
  `EpicsRsProtocol`) and their signatures are unchanged.

### Internal

- README updated: section renamed to "ophyd-async support
  (`ophyd_epicsrs.ophyd_async`)" and all path references updated.
- Tests updated to import from the new module path.

## v0.6.1 (2026-05-03)

### CI

- **aarch64-linux wheel build**: inject
  `CFLAGS_aarch64_unknown_linux_gnu="-D__ARM_ARCH=8"` so the manylinux
  aarch64 cross-toolchain can assemble `ring`'s pregenerated ARM ASM.
  v0.6.0's release workflow failed on the aarch64-linux matrix entry
  because `ring 0.17` (transitive via PVA → rustls → ring, new since
  v0.5.x) ships pregenerated ASM that `#error`s out without
  `__ARM_ARCH` defined, and the cross-toolchain doesn't set it.
  v0.6.0 was therefore never published to PyPI; v0.6.1 is the first
  installable release of the v0.6 line.

## v0.6.0 (2026-05-03)

### New features

- **PVA backend** (`EpicsRsPvaContext`, `EpicsRsPvaPV`): full pvAccess
  support alongside the existing CA path. The legacy shim's `get_pv`
  dispatches on `pva://` prefix; bare names and `ca://` continue to use
  the CA backend. PVA classes are now re-exported from the package root.
- **Async surface** via `pyo3-async-runtimes` on a shared tokio runtime:
  every blocking native method now has an `_async` sibling
  (`get_async`, `put_async`, `connect_and_prefetch_async`,
  `get_field_desc_async`, …) returning a Python awaitable. Sync and async
  paths share one runtime so there is no extra thread overhead.
- **`ophyd_epicsrs.detector` adapter**: `EpicsRsSignalBackend`, an
  `ophyd-async` `SignalBackend` implementation that lets ophyd-async
  Devices drive PVs through epics-rs with no fork. Includes datatype-
  aware converters (`Bool`, `Int`, `Float`, `Str`, `Enum` /
  `StrictEnum` / `SubsetEnum` / `SupersetEnum`, `NumpyArray`,
  `Sequence`, `Table`) and IOC schema validation at connect time via
  PVA `pvinfo`.
- **Typed `PvField` writes**: `Table → NTTable` round-trip uses
  `Table.__annotations__` to produce correctly-typed `PvField` columns
  on the wire (no more dtype-loss through Python lists).
- **Long-string and StrictEnum handling**: long-string CA channels and
  the full `ophyd-async` enum hierarchy are recognised and routed to
  the appropriate converter at connect time.

### Reliability

- **Panic-safe spawned tasks** (`safe_warn!`, `safe_call!`,
  `safe_call_or!` in `safe_log.rs`): every spawned tokio task that
  reaches Python (callback dispatch, monitor delivery, pyo3-log
  forwarding) is wrapped so a `Python::with_gil` panic during
  interpreter finalize cannot crash the process. Caught panics are
  counted (`caught_panic_count()`) and the first one writes a single-
  line stderr notice. `panic = "unwind"` is enforced at compile time
  by a `#[cfg(panic = "abort")] compile_error!` so a downstream
  `Cargo.toml` cannot silently disarm the guards.
- **Drop semantics**: every native PV's `Drop` impl now aborts spawned
  tokio tasks and lets the dispatch thread exit cleanly when its rx
  Sender is dropped. Verified by a 500-cycle leak test that fails on
  even a 1 % thread-leak rate.
- **Monitor generation tokens**: monitor dispatch threads check an
  atomic generation counter so a late delivery from a previous
  subscription cannot fire callbacks against the new generation.
- **Connection callback dedupe**: the shim's `_on_connection_change`
  now drops duplicate same-state events, fixing a long-standing
  flapping-callback issue under reconnection storms.
- **Self-healing PVA monitors**: `pva.rs` matches the CA-side resubscribe
  pattern so PVA monitors recover after IOC restart / network blip.

### Bug fixes

- **`get_field_desc_async` (CA stub)**: previous implementation called
  `Python::with_gil` in both the body and the `safe_call_or!` default;
  the default would re-trigger the same finalize panic the guard was
  meant to absorb. Default is now a GIL-free `PyErr`.
- **PVA put silent-corruption holes**: closed four cases where a
  failed put returned `True` or a missing field returned `None`
  instead of raising.
- **`asyncio` loop blocking**: `resolve_native_type` no longer runs
  inside the asyncio thread; sync inspection moved to a tokio
  blocking task.
- **NTEnum bidirectional mapping**: `to_wire` resolves label → index
  and `to_python` returns the `NTEnum` dict shape ophyd-async expects.
- **Busy-record honor `EpicsOptions.wait`**: prevents the well-known
  busy-record deadlock when a put is dispatched against a record that
  is already processing.

### Tests

- 60 unit tests across `test_converter.py`, `test_factory.py`, and
  `test_shim.py` — covers protocol prefix routing, get_pv dispatch,
  connection-change dedupe, Drop / leak quantification (500-cycle
  thread-count delta), and `release_pvs` semantics.
- New `test_safe_call_or_default_is_gil_free` greps the Rust source
  for the exact `safe_call_or!(Python::with_gil(...), …)` misuse that
  was caught in review, so the regression cannot silently return.

### Internal

- Rust logging path standardised on `tracing` (with the `log` feature)
  bridged to Python's `logging` via `pyo3-log`. No process-wide stderr
  subscriber, no Jupyter red-box noise, no double-reporting alongside
  `PyRuntimeError`.
- `reset_log_cache()` exposed so runtime changes to Python logger
  levels are picked up immediately by Rust-side `tracing` events
  instead of waiting for `pyo3-log`'s ~30 s TTL.
- `caught_panic_count()` (and `_native.caught_panic_count()` for the
  mid-import case) added for telemetry on the panic-guard counter.

## v0.5.2 (2026-05-01)

### Fixes

- Add `readme = "README.md"` to `pyproject.toml` so PyPI displays the project description.

## v0.5.1 (2026-05-01)

### Breaking changes

- **`install()` renamed to `use_epicsrs()`**: the entry-point introduced in
  v0.5.0 has been renamed for clarity. Replace `from ophyd_epicsrs import install; install()`
  with `from ophyd_epicsrs import use_epicsrs; use_epicsrs()`.

## v0.5.0 (2026-05-01)

### Breaking changes

- **No more ophyd fork required**: dependency switched from
  `ophyd @ git+https://github.com/physwkim/ophyd.git@feature/epicsrs-backend`
  to vanilla `ophyd>=1.9` from PyPI. Existing users of the fork must
  call `ophyd_epicsrs.use_epicsrs()` once at startup before constructing any
  Signals/Devices; previously the fork auto-registered via
  `ophyd.set_cl("epicsrs")`.

### New features

- **`ophyd_epicsrs.use_epicsrs()`**: explicit one-call registration of the
  epics-rs control layer. Bypasses `ophyd.set_cl` (vanilla ophyd has no
  "epicsrs" branch) and assigns `ophyd.cl` directly.
- **In-package shim** (`ophyd_epicsrs._shim`): the full `EpicsRsShimPV`,
  `get_pv`, `caget`, `caput`, `setup`, `release_pvs`, `get_dispatcher`
  surface now ships with this package instead of living inside an ophyd
  fork. Backend changes (e.g. epics-rs version bumps) and shim changes
  are co-located.
- **Vanilla-ophyd `process_pending` fallback**: shim's
  `wait_for_connection` flushes the dispatcher queue using
  `EventDispatcher.process_pending` when present (forks) and a sentinel
  insertion against the documented `_threads` / `_utility_threads`
  layout otherwise.

## v0.4.3 (2026-05-01)

### Improvements

- **epics-rs 0.13.0**: switch from git pin to crates.io release. New transitive variants `EpicsValue::StringArray(Vec<String>)` (DBR_STRING with `count > 1` — used by mbbo/mbbi choice arrays, NTNDArray dim labels) and `ConnectionEvent::Unresponsive` (echo timeout while TCP is up) are now handled.
- **DBR_CTRL prefetch**: background prefetch upgraded from `DBR_TIME` to `DBR_CTRL` so units, precision, limits, and `enum_strs` are available immediately after connection.
- **`get_with_metadata` race fix**: awaits the pending prefetch handle before starting a fresh CA read, eliminating a copy-on-clone metadata race.

### Behavior notes

- `ConnectionEvent::Unresponsive` is logged but does NOT fire the Python connection callback. The state is reversible (`Connected ↔ Unresponsive`) per the epics-rs state machine; a real disconnect still fires `Disconnected` separately.
- `py_to_epics_value` for `DbFieldType::String` now accepts `list[str]` and produces `StringArray` (previously dead path that always raised `TypeError` on sequence input).

## v0.4.2 (2026-04-06)

### Improvements
- **epics-rs transport rewrite**: Update to epics-rs v0.7.11 — single-owner writer task replaces shared Mutex, TCP_NODELAY eliminates ~45ms put→get stall, batch frame coalescing reduces TCP segments under load.

## v0.4.1 (2026-04-06)

### Bug Fixes

- **Put failure callback**: Non-blocking put with callback now always fires the completion callback on success AND failure/timeout, passing a `success` bool. Previously, failure silently dropped the callback, leaving ophyd's `set()` permanently locked ("Another set() call is still in progress").
- **Self-healing connection task**: Connection event task uses a loop that resubscribes automatically when the broadcast channel closes (e.g. during epics-rs reconnection cycle). Previously, the task died silently and reconnection events were never delivered to ophyd, causing permanent `DisconnectedError`.
- **Self-healing monitor task**: Monitor task resubscribes automatically when the subscription ends (IOC restart, network blip). Previously, value updates stopped permanently after any disruption.
- **Task liveness check**: `is_finished()` replaces `is_some()` for detecting dead task handles, allowing restart of silently-exited background tasks.

## v0.4.0 (2026-04-05)

### New Features

- **`bulk_connect_and_prefetch`**: Device-level parallel PV initialization. Collects all unconnected PVs from a Device and connects + fetches metadata concurrently in one Rust call. Phase 1: all PVs `wait_connected` in parallel. Phase 2: connected PVs fetch `channel_info` + `DBR_TIME` in parallel. One GIL release for all PVs.
- **Lightweight prefetch**: Background prefetch uses `DBR_TIME` instead of `DBR_CTRL`. CTRL metadata (enum_strs, limits, units) is fetched lazily by ophyd only when needed, halving per-PV connection cost.
- **numpy scalar handling**: `py_to_epics_value` detects numpy scalars (`ndim==0`) and calls `.item()` to convert to native Python types.

### Bug Fixes

- **`auto_monitor=False` cache bug**: `get()` returned stale cached value even without an active monitor. Now checks `self.auto_monitor` before using cache, forcing a CA read when no monitor is running.
- **Connection callback race**: `emit_current_connection_state` / `emit_current_access_state` fire callbacks as lightweight async tasks (no `block_on`) if the channel connected before Python registered callbacks.
- **Char waveform null terminator**: String writes to FTVL=CHAR waveforms include `\0` so IOC doesn't read stale bytes from previous values.
- **Search engine stuck PVs**: `wait_for_connection` recreates the PV if the first attempt fails, working around channels with stale search state.
- **`put(use_complete=True, callback=None)`**: Uses blocking write_notify instead of fire-and-forget, matching pyepics behavior.

## v0.3.0 (2026-04-04)

### New Features

- **Background prefetch**: metadata fetch starts at PV creation time, running concurrently in tokio. By the time Python calls `wait_for_connection`, data is already cached.
- **`connect_and_prefetch`**: single async operation replaces three sequential blocking calls (channel_info + ctrlvars + value read). One GIL release instead of three.
- **Array/waveform put**: `py_to_epics_value` handles list, tuple, and numpy array inputs for all native types.
- **Char waveform string writes**: string values include null terminator, preventing stale byte remnants on IOC readback.
- **`posixseconds`/`nanoseconds`**: `snapshot_to_pydict` includes these fields for pyepics metadata compatibility.

### Bug Fixes

- **enum char_value**: `snapshot_to_pydict` resolves enum index to label via `enum_strs`. Monitor dispatch omits `char_value` for DBR_TIME_ENUM (no labels available), letting the Python shim resolve from cache.
- **Fire-and-forget put**: `put(wait=False)` without callback spawns async task without releasing GIL, preventing areaDetector trigger race where monitor could set `_status=None` before `trigger()` returns.
- **Connection callback race**: `emit_current_connection_state` / `emit_current_access_state` fire callbacks if the channel connected before Python registered them (background prefetch race).
- **`get_channel_info`**: uses `CaChannel::info()` (coordinator query) instead of a CA read, returning host, access rights, native type, and element count without network round-trip.

### Improvements

- **epics-rs umbrella crate**: dependency changed from individual `epics-base-rs` + `epics-ca-rs` to `epics-rs = { features = ["ca"] }`.
- **epics-rs search engine**: adaptive RTT-based retry (Jacobson/Karels), batch UDP, AIMD congestion control, beacon anomaly detection.
- **Internal CA timeouts**: raised from 5s to 30s. Actual timeout controlled by Python caller.

## v0.2.0 (2026-03-28)

- Initial release with ophyd control layer support.
- `EpicsRsPV`: connect, get, put, monitor via Rust CA client.
- `EpicsRsContext`: shared tokio runtime + `CaClient`.
- `bulk_caget`: parallel PV reads.
- Monitor dispatch via per-PV mpsc queue + Python thread.
