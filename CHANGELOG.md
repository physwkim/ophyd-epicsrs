# Changelog

## v0.5.0 (2026-05-01)

### Breaking changes

- **No more ophyd fork required**: dependency switched from
  `ophyd @ git+https://github.com/physwkim/ophyd.git@feature/epicsrs-backend`
  to vanilla `ophyd>=1.9` from PyPI. Existing users of the fork must
  call `ophyd_epicsrs.install()` once at startup before constructing any
  Signals/Devices; previously the fork auto-registered via
  `ophyd.set_cl("epicsrs")`.

### New features

- **`ophyd_epicsrs.install()`**: explicit one-call registration of the
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
