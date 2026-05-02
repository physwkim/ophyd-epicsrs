use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use epics_rs::base::server::snapshot::{DbrClass, Snapshot};
use epics_rs::base::types::DbFieldType;
use epics_rs::base::types::EpicsValue;
use epics_rs::ca::client::CaChannel;

use crate::convert::{epics_value_to_py, py_to_epics_value, snapshot_to_pydict};

/// Monitor event queued from tokio task → Python thread.
struct MonitorEvent {
    pvname: String,
    snapshot: Snapshot,
}

/// Why `resolve_native_type` failed. Lets `put_async` distinguish a
/// timeout (caller should warm the cache or extend the budget) from a
/// channel-level failure (PV disconnected / protocol error — telling
/// the user "extend timeout" would be wrong advice).
enum ResolveError {
    Timeout,
    ChannelFailure(String),
}

/// Background prefetch result: channel info + full CTRL metadata.
pub(crate) struct PrefetchResult {
    pub(crate) native_type: DbFieldType,
    pub(crate) type_name: String,
    pub(crate) element_count: u32,
    pub(crate) host: String,
    pub(crate) read_access: bool,
    pub(crate) write_access: bool,
    pub(crate) snapshot: Snapshot,
}

/// Rust-backed PV object for ophyd's control layer.
///
/// Monitor events are queued via std::sync::mpsc (no GIL needed in tokio)
/// and dispatched to Python callbacks from a dedicated Python thread.
#[pyclass(name = "EpicsRsPV")]
pub struct EpicsRsPV {
    runtime: Arc<Runtime>,
    pub(crate) channel: Arc<CaChannel>,
    #[pyo3(get)]
    pub(crate) pvname: String,
    /// Arc so cache_native_type_async's spawned task can share the slot.
    native_type: Arc<Mutex<Option<DbFieldType>>>,
    monitor_task: Mutex<Option<JoinHandle<()>>>,
    py_monitor_callback: Arc<Mutex<Option<PyObject>>>,
    /// Generation token incremented per `add_monitor_callback`. Each
    /// dispatch thread captures its expected token at spawn time and
    /// refuses to fire the callback once the canonical token has
    /// advanced — so a slow OLD dispatch thread cannot deliver
    /// stale-rx events into the NEW callback during a `set_callback`
    /// resubscribe.
    monitor_generation: Arc<std::sync::atomic::AtomicU64>,
    connection_callback: Arc<Mutex<Option<PyObject>>>,
    access_callback: Arc<Mutex<Option<PyObject>>>,
    connection_task: Mutex<Option<JoinHandle<()>>>,
    /// Queue for monitor events (tokio → Python thread)
    monitor_tx: Arc<Mutex<Option<std::sync::mpsc::Sender<MonitorEvent>>>>,
    /// Python dispatch thread handle
    dispatch_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Background prefetch: starts on PV creation, completes before Python asks
    pub(crate) prefetch_handle: Mutex<Option<JoinHandle<Option<PrefetchResult>>>>,
    /// Fire-and-forget probe tasks spawned by emit_current_*_state.
    /// Tracked so Drop can abort in-flight `Python::with_gil` after
    /// interpreter finalize — same teardown race as monitor_setup_task.
    emit_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl EpicsRsPV {
    pub fn new(runtime: Arc<Runtime>, channel: CaChannel, pvname: String) -> Self {
        let ch = Arc::new(channel);

        // Start background prefetch immediately — runs concurrently with
        // all other PVs' prefetches in the tokio runtime (no GIL needed).
        // Short timeouts on CA reads so slow PVs fail fast and don't
        // block bulk_connect_and_prefetch from returning promptly.
        let prefetch_ch = ch.clone();
        let prefetch_handle = runtime.spawn(async move {
            // Wait for connection (up to 30s)
            if prefetch_ch
                .wait_connected(Duration::from_secs(30))
                .await
                .is_err()
            {
                return None;
            }
            // Channel info (coordinator query, not a CA read)
            let info = match tokio::time::timeout(Duration::from_secs(2), prefetch_ch.info()).await
            {
                Ok(Ok(i)) => i,
                _ => return None,
            };
            // DBR_CTRL read: value + alarm + timestamp + units/precision/limits.
            // Fetching CTRL upfront eliminates the race where describe() runs
            // before the lazy CTRL fetch completes (EpicsSignalRO after copy()).
            let snapshot = match tokio::time::timeout(
                Duration::from_secs(2),
                prefetch_ch.get_with_metadata(DbrClass::Ctrl),
            )
            .await
            {
                Ok(Ok(s)) => s,
                _ => return None,
            };
            Some(PrefetchResult {
                native_type: info.native_type,
                type_name: format!("{:?}", info.native_type).to_lowercase(),
                element_count: info.element_count,
                host: info.server_addr.to_string(),
                read_access: info.access_rights.read,
                write_access: info.access_rights.write,
                snapshot,
            })
        });

        Self {
            runtime,
            channel: ch,
            pvname,
            native_type: Arc::new(Mutex::new(None)),
            monitor_task: Mutex::new(None),
            py_monitor_callback: Arc::new(Mutex::new(None)),
            monitor_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            connection_callback: Arc::new(Mutex::new(None)),
            access_callback: Arc::new(Mutex::new(None)),
            connection_task: Mutex::new(None),
            monitor_tx: Arc::new(Mutex::new(None)),
            dispatch_thread: Mutex::new(None),
            prefetch_handle: Mutex::new(Some(prefetch_handle)),
            emit_tasks: Mutex::new(Vec::new()),
        }
    }

    /// Spawn an async task and block the current OS thread waiting for result.
    fn spawn_wait<F, T>(&self, fut: F) -> PyResult<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            let result = fut.await;
            let _ = tx.send(result);
        });
        rx.recv()
            .map_err(|_| PyRuntimeError::new_err("runtime task failed"))
    }

    /// Resolve the PV's native DbFieldType, populating the cache if empty.
    /// Falls back to `channel.info()` (a coordinator query — NO CA read)
    /// rather than `get_with_metadata`, so write-only PVs and busy
    /// records work without paying for or failing on a synchronous read.
    /// `timeout_secs` bounds the info() call.
    fn resolve_native_type_sync(&self, py: Python<'_>, timeout_secs: f64) -> PyResult<DbFieldType> {
        if let Some(t) = *self.native_type.lock() {
            return Ok(t);
        }
        let channel = self.channel.clone();
        // Use the caller's full timeout budget. Earlier revisions had
        // a hard 5 s cap here that was inconsistent with the async
        // path (which uses a deadline) and silently truncated long
        // user-supplied timeouts on the legacy sync path.
        let dur = Duration::from_secs_f64(timeout_secs);
        let info = py.allow_threads(|| {
            self.spawn_wait(async move { tokio::time::timeout(dur, channel.info()).await })
        })?;
        match info {
            Ok(Ok(i)) => {
                *self.native_type.lock() = Some(i.native_type);
                Ok(i.native_type)
            }
            _ => Err(PyRuntimeError::new_err(
                "cannot determine PV native type for put (channel.info() failed)",
            )),
        }
    }

    /// Async variant of `resolve_native_type_sync` — does NOT block the
    /// asyncio event loop. Used inside `future_into_py` blocks so the
    /// caller's `await pv.put_async(...)` actually yields while we
    /// fetch channel info.
    ///
    /// Returns `ResolveError::Timeout` and `ResolveError::ChannelFailure`
    /// as distinct variants so callers (e.g. `put_async`) can produce
    /// accurate user-facing diagnostics rather than blaming the user's
    /// timeout budget for what was actually a disconnect / protocol
    /// error.
    async fn resolve_native_type(
        cache: Arc<Mutex<Option<DbFieldType>>>,
        channel: Arc<CaChannel>,
        timeout_secs: f64,
    ) -> Result<DbFieldType, ResolveError> {
        if let Some(t) = *cache.lock() {
            return Ok(t);
        }
        let dur = Duration::from_secs_f64(timeout_secs);
        match tokio::time::timeout(dur, channel.info()).await {
            Ok(Ok(i)) => {
                *cache.lock() = Some(i.native_type);
                Ok(i.native_type)
            }
            Ok(Err(e)) => Err(ResolveError::ChannelFailure(e.to_string())),
            Err(_) => Err(ResolveError::Timeout),
        }
    }

    /// Best-effort injection of the current connection state for a newly
    /// registered callback. This avoids blocking Python startup while still
    /// covering the race where prefetch connected the channel before callback
    /// registration completed.
    fn emit_current_connection_state(&self) {
        let channel = self.channel.clone();
        let conn_cb_ref = self.connection_callback.clone();

        let handle = self.runtime.spawn(async move {
            if channel.info().await.is_ok() {
                crate::safe_call!(Python::with_gil(|py| {
                    let guard = conn_cb_ref.lock();
                    if let Some(cb) = &*guard {
                        let callback = cb.clone_ref(py);
                        drop(guard);
                        let _ = callback.call1(py, (true,));
                    }
                }));
            }
        });
        self.emit_tasks.lock().push(handle);
    }

    /// Best-effort injection of the current access-rights state for a newly
    /// registered callback. This is separate from connection injection because
    /// ophyd registers the two callbacks sequentially.
    fn emit_current_access_state(&self) {
        let channel = self.channel.clone();
        let access_cb_ref = self.access_callback.clone();

        let handle = self.runtime.spawn(async move {
            if let Ok(info) = channel.info().await {
                crate::safe_call!(Python::with_gil(|py| {
                    let guard = access_cb_ref.lock();
                    if let Some(cb) = &*guard {
                        let callback = cb.clone_ref(py);
                        drop(guard);
                        let _ =
                            callback.call1(py, (info.access_rights.read, info.access_rights.write));
                    }
                }));
            }
        });
        self.emit_tasks.lock().push(handle);
    }
}

#[pymethods]
impl EpicsRsPV {
    /// Block until the PV is connected, releasing the GIL while waiting.
    fn wait_for_connection(&self, py: Python<'_>, timeout: f64) -> PyResult<bool> {
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        py.allow_threads(|| {
            self.spawn_wait(async move { channel.wait_connected(dur).await.is_ok() })
        })
    }

    /// Wait for the background prefetch (started at PV creation) to complete.
    /// Returns all metadata in a single dict, or falls back to synchronous fetch.
    #[pyo3(signature = (timeout=5.0))]
    fn connect_and_prefetch(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<PyObject>> {
        let dur = Duration::from_secs_f64(timeout);

        // Take the background prefetch handle (if still pending)
        let handle = self.prefetch_handle.lock().take();
        if let Some(handle) = handle {
            // Await the background task — just waiting, no new CA reads
            let result = py.allow_threads(|| {
                self.spawn_wait(async move { tokio::time::timeout(dur, handle).await })
            })?;
            if let Ok(Ok(Some(prefetch))) = result {
                *self.native_type.lock() = Some(prefetch.native_type);
                let dict = snapshot_to_pydict(py, &prefetch.snapshot);
                let dict_ref = dict.downcast_bound::<pyo3::types::PyDict>(py).unwrap();
                let _ = dict_ref.set_item("ftype", prefetch.native_type as u16);
                let _ = dict_ref.set_item("type", &prefetch.type_name);
                let _ = dict_ref.set_item("count", prefetch.element_count);
                let _ = dict_ref.set_item("host", &prefetch.host);
                let _ = dict_ref.set_item("read_access", prefetch.read_access);
                let _ = dict_ref.set_item("write_access", prefetch.write_access);
                return Ok(Some(dict));
            }
        }

        // Fallback: synchronous fetch (prefetch failed or already consumed)
        let channel = self.channel.clone();
        let result = py.allow_threads(|| {
            self.spawn_wait(async move {
                channel.wait_connected(dur).await?;
                let info = channel.info().await?;
                let snapshot = tokio::time::timeout(dur, channel.get_with_metadata(DbrClass::Ctrl))
                    .await
                    .map_err(|_| epics_rs::base::error::CaError::Timeout)??;
                Ok::<_, epics_rs::base::error::CaError>((info, snapshot))
            })
        })?;

        match result {
            Ok((info, snapshot)) => {
                *self.native_type.lock() = Some(info.native_type);
                let dict = snapshot_to_pydict(py, &snapshot);
                let dict_ref = dict.downcast_bound::<pyo3::types::PyDict>(py).unwrap();
                let _ = dict_ref.set_item("ftype", info.native_type as u16);
                let _ = dict_ref.set_item("type", format!("{:?}", info.native_type).to_lowercase());
                let _ = dict_ref.set_item("count", info.element_count);
                let _ = dict_ref.set_item("host", info.server_addr.to_string());
                let _ = dict_ref.set_item("read_access", info.access_rights.read);
                let _ = dict_ref.set_item("write_access", info.access_rights.write);
                Ok(Some(dict))
            }
            Err(_) => Ok(None),
        }
    }

    /// Get channel-level metadata without performing a CA read.
    fn get_channel_info(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let channel = self.channel.clone();
        let result = py.allow_threads(|| self.spawn_wait(async move { channel.info().await }))?;
        match result {
            Ok(info) => {
                *self.native_type.lock() = Some(info.native_type);
                let dict = pyo3::types::PyDict::new(py);
                let _ = dict.set_item("ftype", info.native_type as u16);
                let _ = dict.set_item("type", format!("{:?}", info.native_type).to_lowercase());
                let _ = dict.set_item("count", info.element_count);
                let _ = dict.set_item("host", info.server_addr.to_string());
                let _ = dict.set_item("read_access", info.access_rights.read);
                let _ = dict.set_item("write_access", info.access_rights.write);
                Ok(Some(dict.into_any().unbind()))
            }
            Err(_) => Ok(None),
        }
    }

    /// Get PV value with full metadata.
    #[pyo3(signature = (timeout=2.0, form="time", count=0))]
    fn get_with_metadata(
        &self,
        py: Python<'_>,
        timeout: f64,
        form: &str,
        count: u32,
    ) -> PyResult<Option<PyObject>> {
        // If background prefetch is still pending, await it first.
        // Prefetch uses DBR_CTRL (superset of TIME), so it satisfies any form.
        // This avoids starting a fresh CA read when the prefetch is about to
        // complete — critical for copy() where get_ctrlvars(timeout=1) races
        // against channel connection.
        if count == 0 {
            let handle = self.prefetch_handle.lock().take();
            if let Some(handle) = handle {
                let dur = Duration::from_secs_f64(timeout);
                let result = py.allow_threads(|| {
                    self.spawn_wait(async move { tokio::time::timeout(dur, handle).await })
                })?;
                if let Ok(Ok(Some(prefetch))) = result {
                    *self.native_type.lock() = Some(prefetch.native_type);
                    return Ok(Some(snapshot_to_pydict(py, &prefetch.snapshot)));
                }
            }
        }

        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        let class = match form {
            "ctrl" | "control" => DbrClass::Ctrl,
            _ => DbrClass::Time,
        };

        let result = py.allow_threads(|| {
            self.spawn_wait(async move {
                tokio::time::timeout(dur, channel.get_with_metadata_count(class, count)).await
            })
        })?;

        match result {
            Ok(Ok(snapshot)) => {
                *self.native_type.lock() = Some(snapshot.value.dbr_type());
                Ok(Some(snapshot_to_pydict(py, &snapshot)))
            }
            Ok(Err(e)) => {
                tracing::warn!(target: "ophyd_epicsrs.ca", pv = %self.pvname, "get_with_metadata failed: {e}");
                Ok(None)
            }
            Err(_) => {
                tracing::warn!(target: "ophyd_epicsrs.ca", pv = %self.pvname, "get_with_metadata timed out");
                Ok(None)
            }
        }
    }

    #[pyo3(signature = (timeout=1.0))]
    fn get_timevars(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<PyObject>> {
        self.get_with_metadata(py, timeout, "time", 0)
    }

    #[pyo3(signature = (timeout=1.0))]
    fn get_ctrlvars(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<PyObject>> {
        self.get_with_metadata(py, timeout, "ctrl", 0)
    }

    /// Write a value to the PV.
    #[pyo3(signature = (value, wait=false, timeout=300.0, callback=None))]
    fn put(
        &self,
        py: Python<'_>,
        value: &Bound<'_, pyo3::PyAny>,
        wait: bool,
        timeout: f64,
        callback: Option<PyObject>,
    ) -> PyResult<()> {
        let native = self.resolve_native_type_sync(py, timeout)?;
        let epics_val = py_to_epics_value(value, native)?;
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);

        if wait {
            // Blocking put — wait for write_notify response
            let result = py.allow_threads(|| {
                self.spawn_wait(
                    async move { tokio::time::timeout(dur, channel.put(&epics_val)).await },
                )
            })?;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(PyRuntimeError::new_err(format!("put failed: {e}"))),
                Err(_) => return Err(PyRuntimeError::new_err("put timed out")),
            }
            // Completion callback after write confirmed
            if let Some(cb) = callback {
                cb.call0(py)?;
            }
        } else if let Some(cb) = callback {
            // Non-blocking put with callback — use write_notify, fire callback on ack.
            // Always fire the callback to unblock ophyd's set() state machine,
            // even on failure/timeout — otherwise ophyd thinks set() is still
            // in progress and the next set() raises RuntimeError.
            // Pass success=True/False so the shim can propagate failure.
            let pvname = self.pvname.clone();
            self.runtime.spawn(async move {
                let success = match tokio::time::timeout(dur, channel.put(&epics_val)).await {
                    Ok(Ok(())) => true,
                    Ok(Err(e)) => {
                        crate::safe_warn!(target: "ophyd_epicsrs.ca", pv = %pvname, "put error: {e}");
                        false
                    }
                    Err(_) => {
                        crate::safe_warn!(target: "ophyd_epicsrs.ca", pv = %pvname, "put timed out");
                        false
                    }
                };
                crate::safe_call!(Python::with_gil(|py| {
                    let _ = cb.call1(py, (success,));
                }));
            });
        } else {
            // Fire-and-forget put (CA_PROTO_WRITE) — spawn and return immediately.
            // Must NOT release the GIL here: areaDetector trigger() does
            // _status = Status(); put(1, wait=False); return _status
            // If we release the GIL, monitor thread can fire _acquire_changed
            // and set _status=None before put returns → trigger() returns None.
            let pvname = self.pvname.clone();
            self.runtime.spawn(async move {
                if let Err(e) = channel.put_nowait(&epics_val).await {
                    crate::safe_warn!(target: "ophyd_epicsrs.ca", pv = %pvname, "put_nowait error: {e}");
                }
            });
        }

        Ok(())
    }

    /// Register a monitor callback.
    ///
    /// Events flow: tokio monitor task → mpsc queue → Python dispatch thread → callback.
    /// This avoids GIL acquisition in tokio tasks, preventing deadlocks with put().
    fn add_monitor_callback(&self, py: Python<'_>, callback: PyObject) {
        *self.py_monitor_callback.lock() = Some(callback.clone_ref(py));

        {
            let guard = self.monitor_task.lock();
            if let Some(ref handle) = *guard {
                if !handle.is_finished() {
                    return; // still running
                }
                // task died — will restart below
            }
        }

        // Bump the generation BEFORE creating the new dispatch thread.
        // Any old dispatch thread that's still draining its rx will see
        // its own captured generation differ from this one and bail
        // before invoking the (potentially new) callback. (`gen` is a
        // reserved keyword in edition 2024, hence `generation`.)
        let generation = self
            .monitor_generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        // Create the event queue
        let (tx, rx) = std::sync::mpsc::channel::<MonitorEvent>();
        *self.monitor_tx.lock() = Some(tx.clone());

        // Start Python dispatch thread — reads from queue, calls Python callback
        let cb_ref = self.py_monitor_callback.clone();
        let gen_ref = self.monitor_generation.clone();
        let dispatch = std::thread::spawn(move || {
            while let Ok(event) = rx.recv() {
                // Race guard: if a NEW set_callback ran while we still
                // had pending events, the canonical generation has
                // advanced past ours — the new dispatch thread owns
                // the callback now. Drop without firing.
                if gen_ref.load(std::sync::atomic::Ordering::SeqCst) != generation {
                    continue;
                }
                crate::safe_call!(Python::with_gil(|py| {
                    let guard = cb_ref.lock();
                    let callback = match &*guard {
                        Some(cb) => cb.clone_ref(py),
                        None => return,
                    };
                    drop(guard);

                    let snap = &event.snapshot;
                    let kwargs = pyo3::types::PyDict::new(py);

                    // Core fields
                    let _ = kwargs.set_item("pvname", &event.pvname);
                    let _ = kwargs.set_item("value", epics_value_to_py(py, &snap.value));

                    // EPICS timestamp
                    let ts = snap
                        .timestamp
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64();
                    let _ = kwargs.set_item("timestamp", ts);

                    // Alarm status/severity
                    let _ = kwargs.set_item("status", snap.alarm.status);
                    let _ = kwargs.set_item("severity", snap.alarm.severity);

                    // char_value: string representation.
                    // For enum PVs, resolve via snapshot enum_strs if present.
                    // DBR_TIME_ENUM lacks enum_strs, so enum values are sent
                    // without char_value — the Python shim resolves from cache.
                    match &snap.value {
                        EpicsValue::Enum(idx) => {
                            if let Some(ref ei) = snap.enums {
                                if let Some(label) = ei.strings.get(*idx as usize) {
                                    let _ = kwargs.set_item("char_value", label.as_str());
                                }
                            }
                            // else: omit char_value, Python shim uses cached enum_strs
                        }
                        EpicsValue::CharArray(v) => {
                            // Char waveform → null-terminated string
                            let end = v.iter().position(|&b| b == 0).unwrap_or(v.len());
                            let s = String::from_utf8_lossy(&v[..end]);
                            let _ = kwargs.set_item("char_value", s.as_ref());
                        }
                        other => {
                            let _ = kwargs.set_item("char_value", format!("{other}"));
                        }
                    }

                    let _ = callback.call(py, (), Some(&kwargs));
                }));
            }
        });
        // Replace the previous dispatch thread handle. The race between
        // an OLD dispatch thread still draining rx and the NEW callback
        // is closed by the generation check above; here we just reap
        // the old JoinHandle (in a background thread, since the OLD
        // thread might have queued events to process before exiting).
        if let Some(old) = self.dispatch_thread.lock().take() {
            let _ = std::thread::Builder::new()
                .name("ophyd-epicsrs-dispatch-join".into())
                .spawn(move || {
                    let _ = old.join();
                });
        }
        *self.dispatch_thread.lock() = Some(dispatch);

        // Start tokio monitor task with auto-resubscribe.
        // If the subscription ends (IOC restart, network blip), the task
        // resubscribes instead of dying permanently.
        let channel = self.channel.clone();
        let pvname = self.pvname.clone();

        let handle = self.runtime.spawn(async move {
            loop {
                let monitor = match channel.subscribe().await {
                    Ok(m) => m,
                    Err(e) => {
                        crate::safe_debug!("{pvname}: subscribe failed ({e}), retrying...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let mut monitor = monitor;
                while let Some(result) = monitor.recv().await {
                    if let Ok(snapshot) = result {
                        let event = MonitorEvent {
                            pvname: pvname.clone(),
                            snapshot,
                        };
                        if tx.send(event).is_err() {
                            return; // dispatch thread gone — exit permanently
                        }
                    }
                }
                // Subscription ended (IOC restart, network blip) — resubscribe
                crate::safe_debug!("{pvname}: monitor stream ended, resubscribing");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        *self.monitor_task.lock() = Some(handle);
    }

    /// Set a connection callback.
    fn set_connection_callback(&self, py: Python<'_>, callback: PyObject) {
        *self.connection_callback.lock() = Some(callback.clone_ref(py));
        self._start_event_task();
        self.emit_current_connection_state();
    }

    /// Set an access rights callback.
    fn set_access_callback(&self, py: Python<'_>, callback: PyObject) {
        *self.access_callback.lock() = Some(callback.clone_ref(py));
        self._start_event_task();
        self.emit_current_access_state();
    }

    /// Start the background task for connection/access events.
    /// Self-healing: if the broadcast channel closes (e.g. epics-rs
    /// reconnection cycle), the task resubscribes automatically.
    fn _start_event_task(&self) {
        let mut guard = self.connection_task.lock();
        if let Some(ref handle) = *guard {
            if !handle.is_finished() {
                return; // still running
            }
            // task died — will restart below
        }

        let channel = self.channel.clone();
        let conn_cb_ref = self.connection_callback.clone();
        let access_cb_ref = self.access_callback.clone();
        let pvname = self.pvname.clone();
        let handle = self.runtime.spawn(async move {
            loop {
                let mut rx = channel.connection_events();
                while let Ok(event) = rx.recv().await {
                    use epics_rs::ca::client::ConnectionEvent;
                    match event {
                        ConnectionEvent::Connected => {
                            crate::safe_call!(Python::with_gil(|py| {
                                let guard = conn_cb_ref.lock();
                                if let Some(cb) = &*guard {
                                    let callback = cb.clone_ref(py);
                                    drop(guard);
                                    let _ = callback.call1(py, (true,));
                                }
                            }));
                        }
                        ConnectionEvent::Disconnected => {
                            crate::safe_call!(Python::with_gil(|py| {
                                let guard = conn_cb_ref.lock();
                                if let Some(cb) = &*guard {
                                    let callback = cb.clone_ref(py);
                                    drop(guard);
                                    let _ = callback.call1(py, (false,));
                                }
                            }));
                        }
                        ConnectionEvent::AccessRightsChanged { read, write } => {
                            crate::safe_call!(Python::with_gil(|py| {
                                let guard = access_cb_ref.lock();
                                if let Some(cb) = &*guard {
                                    let callback = cb.clone_ref(py);
                                    drop(guard);
                                    let _ = callback.call1(py, (read, write));
                                }
                            }));
                        }
                        ConnectionEvent::Unresponsive => {
                            // Echo timed out — TCP still up, no callback emitted
                        }
                    }
                    crate::safe_debug!("{pvname}: connection event: {event:?}");
                }
                // Broadcast channel closed — resubscribe after brief pause
                crate::safe_debug!("{pvname}: connection event stream ended, resubscribing");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        *guard = Some(handle);
    }

    fn clear_monitors(&self) {
        *self.py_monitor_callback.lock() = None;
        // Drop the sender to unblock the dispatch thread
        *self.monitor_tx.lock() = None;
        if let Some(handle) = self.monitor_task.lock().take() {
            handle.abort();
        }
        // dispatch_thread will exit when rx is dropped
    }

    fn disconnect(&self) {
        self.clear_monitors();
        *self.connection_callback.lock() = None;
        *self.access_callback.lock() = None;
        if let Some(handle) = self.connection_task.lock().take() {
            handle.abort();
        }
    }

    // ===== async surface (pyo3-async-runtimes) =====
    //
    // These methods return Python awaitables. They share the same tokio
    // runtime, CaClient, and CaChannel cache as the sync methods above —
    // mixed sync+async use against the same PV is safe.

    /// Async: wait until the PV is connected. Returns True on success,
    /// False on timeout. Mirrors `wait_for_connection` (sync).
    fn connect_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(channel.wait_connected(dur).await.is_ok())
        })
    }

    /// Async no-op for CA — provided for symmetry with EpicsRsPvaPV so
    /// callers can invoke the same method on both protocol wrappers.
    /// CA has no introspection equivalent to PVA's pvinfo (DBR types
    /// are scalar/scalar-array only), so this returns None.
    #[pyo3(signature = (timeout=2.0))]
    fn get_field_desc_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let _ = timeout;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // safe_call_or!'s `default` is evaluated OUTSIDE the
            // catch_unwind guard — it must therefore be panic-free
            // and GIL-free. The earlier version called
            // `Python::with_gil` in BOTH the body and the default,
            // which would re-trigger the same finalize panic that
            // the guard was meant to absorb. Use a GIL-free
            // PyRuntimeError as the fallback (PyErr::new_err defers
            // message realisation until display).
            crate::safe_call_or!(
                Err::<PyObject, PyErr>(PyRuntimeError::new_err(
                    "get_field_desc_async on CA: Python::with_gil panicked \
                     while constructing the no-op None return (CA has no \
                     pvinfo equivalent — this is an intentional no-op; \
                     the panic itself is most likely an interpreter \
                     finalize race)",
                )),
                Python::with_gil(|py| Ok::<PyObject, PyErr>(py.None()))
            )
        })
    }

    /// Async: populate the cached native_type. Strategy in order:
    ///
    /// 1. Cache hit — return immediately (no I/O).
    /// 2. Drain the background prefetch_handle started at PV creation
    ///    — the prefetch already did wait_connected + info() + a CTRL
    ///    read, so consuming it costs ~0 round trips.
    /// 3. Fall back to channel.info() (coordinator query, no CA read).
    ///
    /// Returns True on success, False on timeout — used by
    /// SignalBackend.connect to ensure subsequent puts (especially
    /// put_nowait_async on busy records) don't pay the channel.info()
    /// latency on the put path.
    #[pyo3(signature = (timeout=2.0))]
    fn cache_native_type_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        let cache = self.native_type.clone();
        // Drain the prefetch handle once if it's still pending — this
        // recovers the work the constructor's spawn already did and
        // avoids issuing a redundant info() round trip.
        let prefetch = self.prefetch_handle.lock().take();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if cache.lock().is_some() {
                return Ok(true);
            }
            // Try the prefetch first.
            if let Some(handle) = prefetch {
                if let Ok(Ok(Some(result))) = tokio::time::timeout(dur, handle).await {
                    *cache.lock() = Some(result.native_type);
                    return Ok(true);
                }
            }
            // Fallback: standalone channel.info() query.
            match tokio::time::timeout(dur, channel.info()).await {
                Ok(Ok(info)) => {
                    *cache.lock() = Some(info.native_type);
                    Ok(true)
                }
                _ => Ok(false),
            }
        })
    }

    /// Async: read the PV value. Raises `TimeoutError` on timeout and
    /// `RuntimeError` on protocol error — never returns silently with
    /// a None that the caller might mistake for a successful read.
    /// Also caches the discovered native_type so subsequent puts can
    /// skip the channel.info() step.
    #[pyo3(signature = (timeout=10.0))]
    fn get_value_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        let cache = self.native_type.clone();
        let pvname = self.pvname.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(dur, channel.get()).await {
                Ok(Ok((_dbr, val))) => {
                    *cache.lock() = Some(val.dbr_type());
                    crate::safe_call_or!(
                        Err(PyRuntimeError::new_err(format!(
                            "get on {pvname}: panic in Python::with_gil during value conversion"
                        ))),
                        Python::with_gil(|py| Ok::<PyObject, PyErr>(
                            crate::convert::epics_value_to_py(py, &val)
                        ))
                    )
                }
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "get on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "get on {pvname} timed out after {timeout}s"
                ))),
            }
        })
    }

    /// Async: read value + metadata. Raises on timeout / error rather
    /// than returning None silently.
    #[pyo3(signature = (timeout=10.0, form="time"))]
    fn get_reading_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
        form: &str,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        let class = match form {
            "ctrl" | "control" => DbrClass::Ctrl,
            _ => DbrClass::Time,
        };
        let cache = self.native_type.clone();
        let pvname = self.pvname.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(dur, channel.get_with_metadata(class)).await {
                Ok(Ok(snapshot)) => {
                    *cache.lock() = Some(snapshot.value.dbr_type());
                    crate::safe_call_or!(
                        Err(PyRuntimeError::new_err(format!(
                            "get_reading on {pvname}: panic in Python::with_gil during snapshot conversion"
                        ))),
                        Python::with_gil(|py| Ok::<PyObject, PyErr>(
                            crate::convert::snapshot_to_pydict(py, &snapshot)
                        ))
                    )
                }
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "get_reading on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "get_reading on {pvname} timed out after {timeout}s"
                ))),
            }
        })
    }

    /// Async: write a value. Returns True on success, False on failure
    /// or timeout. Always waits for write_notify ack.
    #[pyo3(signature = (value, timeout=300.0))]
    fn put_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, pyo3::PyAny>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let value_owned: Py<pyo3::PyAny> = value.clone().unbind();
        let cache = self.native_type.clone();
        let channel = self.channel.clone();
        let pvname = self.pvname.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Deadline-based budget: total wall-clock time for resolve+put
            // must fit inside `timeout`. Without this, a cache miss could
            // burn 5 s on resolve_native_type AND another `timeout` s on
            // the put itself.
            let deadline = std::time::Instant::now() + Duration::from_secs_f64(timeout);
            let resolve_budget = (deadline - std::time::Instant::now())
                .as_secs_f64()
                .min(5.0);
            let native = match Self::resolve_native_type(cache, channel.clone(), resolve_budget)
                .await
            {
                Ok(t) => t,
                Err(ResolveError::Timeout) => {
                    return Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                        "put on {pvname}: native-type resolution exhausted the {timeout}s budget \
                         before the put could be issued (consider warming the cache via \
                         cache_native_type_async at connect, or pass a larger timeout)"
                    )));
                }
                Err(ResolveError::ChannelFailure(msg)) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "put on {pvname}: channel.info() failed before put could be issued: {msg} \
                         (PV likely disconnected — verify the IOC is reachable)"
                    )));
                }
            };
            let epics_val = crate::safe_call_or!(
                Err(PyRuntimeError::new_err(format!(
                    "put on {pvname}: panic in Python::with_gil during value conversion"
                ))),
                Python::with_gil(|py| {
                    let v = value_owned.bind(py);
                    crate::convert::py_to_epics_value(v, native)
                })
            )?;
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "put on {pvname}: native-type resolution consumed the entire {timeout}s budget"
                )));
            }
            match tokio::time::timeout(remaining, channel.put(&epics_val)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "put on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "put on {pvname} timed out at the wire after {remaining:?} \
                     (total budget was {timeout}s)"
                ))),
            }
        })
    }

    /// Async: fire-and-forget write (CA_PROTO_WRITE, no notify ack).
    /// Mirrors the sync `put(value, wait=False)` path. Returns True
    /// once the write request has been queued; does NOT wait for the
    /// IOC to confirm. Use this for busy-record / acquire PVs where
    /// `put_async` (which waits for write_notify) would deadlock.
    #[pyo3(signature = (value))]
    fn put_nowait_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, pyo3::PyAny>,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let value_owned: Py<pyo3::PyAny> = value.clone().unbind();
        let cache = self.native_type.clone();
        let channel = self.channel.clone();
        let pvname = self.pvname.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Tight 1 s cap on resolve_native_type — "nowait" semantics
            // mean the caller does NOT want to block on a slow IOC.
            // Most callers either pre-warm via cache_native_type_async
            // at connect or don't care; the rare cache-miss path
            // accepts a 1 s ceiling rather than the previous 5 s.
            let native = match Self::resolve_native_type(cache, channel.clone(), 1.0).await {
                Ok(t) => t,
                Err(ResolveError::Timeout) => {
                    return Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                        "put_nowait on {pvname}: native-type resolution exceeded 1 s \
                         (warm the cache via cache_native_type_async at connect)"
                    )));
                }
                Err(ResolveError::ChannelFailure(msg)) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "put_nowait on {pvname}: channel.info() failed: {msg}"
                    )));
                }
            };
            let epics_val = crate::safe_call_or!(
                Err(PyRuntimeError::new_err(format!(
                    "put_nowait on {pvname}: panic in Python::with_gil during value conversion"
                ))),
                Python::with_gil(|py| {
                    let v = value_owned.bind(py);
                    crate::convert::py_to_epics_value(v, native)
                })
            )?;
            // Raise on definitive errors — silently swallowing made
            // bluesky plans proceed against unwritten PVs.
            match channel.put_nowait(&epics_val).await {
                Ok(()) => Ok(()),
                Err(e) => Err(PyRuntimeError::new_err(format!(
                    "put_nowait on {pvname} failed: {e}"
                ))),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!("EpicsRsPV('{}')", self.pvname)
    }
}

impl Drop for EpicsRsPV {
    /// Abort every spawned task and drop monitor channels.
    ///
    /// Without this, every PV that the Python wrapper GCs leaks all of
    /// its background tasks: the connection_task / monitor_task self-
    /// healing loops never terminate (they retry forever), and any
    /// fire-and-forget emit_current_*_state probe can outlive the
    /// interpreter and panic on `Python::with_gil`. Particularly bad
    /// for the legacy `_shim.caget`/`caput` path which creates a fresh
    /// EpicsRsShimPV for every call — without this, every caget is a
    /// permanent task leak.
    fn drop(&mut self) {
        // Drop callback Pythons first so any in-flight task that races
        // through `Python::with_gil` finds the slot empty (no-op call).
        *self.connection_callback.lock() = None;
        *self.access_callback.lock() = None;
        *self.py_monitor_callback.lock() = None;
        *self.monitor_tx.lock() = None;

        if let Some(h) = self.monitor_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.connection_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.prefetch_handle.lock().take() {
            h.abort();
        }
        for h in self.emit_tasks.lock().drain(..) {
            h.abort();
        }
        // dispatch_thread will exit when monitor_tx Sender is dropped.
    }
}
