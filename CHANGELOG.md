# Changelog

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
