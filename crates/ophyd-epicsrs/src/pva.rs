//! PVA (pvAccess) backend mirroring the CA EpicsRsPV/EpicsRsContext surface.
//!
//! Architecture:
//! - `EpicsRsPvaContext` owns a shared `Arc<Runtime>` (passed in from
//!   the existing CA context, or created standalone) and a `PvaClient`.
//! - `EpicsRsPvaPV` wraps a PV name. Operations dispatch through the
//!   client — channel caching is handled internally by `PvaClient`.
//! - Monitor events flow via `pvmonitor_handle` returning a
//!   `SubscriptionHandle`; we route events through the same Python
//!   dispatch-thread pattern as the CA path so callbacks fire without
//!   GIL contention from the tokio task.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use epics_rs::pva::client_native::context::PvaClient;
use epics_rs::pva::client_native::ops_v2::SubscriptionHandle;
use epics_rs::pva::pvdata::{FieldDesc, PvField, ScalarType};

use crate::pva_convert::{pvfield_to_metadata, pvfield_to_py};

/// Monitor event queued from tokio task → Python thread.
struct PvaMonitorEvent {
    pvname: String,
    field: PvField,
}

/// Shared PVA context — Runtime + PvaClient.
#[pyclass(name = "EpicsRsPvaContext")]
pub struct EpicsRsPvaContext {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) client: PvaClient,
}

#[pymethods]
impl EpicsRsPvaContext {
    #[new]
    fn new() -> PyResult<Self> {
        // Share the process-wide tokio runtime with CA + async surface.
        let runtime = crate::runtime::shared_runtime();

        // PvaClient::new() and the underlying builder are sync, so no
        // need to spawn for construction. Background tasks (search engine,
        // connection pool) are spawned lazily on first channel use.
        let client = PvaClient::new()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create PVA client: {e}")))?;

        Ok(Self { runtime, client })
    }

    /// Create a PVA channel wrapper for the given name.
    fn create_pv(&self, pvname: &str) -> EpicsRsPvaPV {
        EpicsRsPvaPV::new(
            self.runtime.clone(),
            self.client.clone(),
            pvname.to_string(),
        )
    }

    fn __repr__(&self) -> String {
        "EpicsRsPvaContext(active)".to_string()
    }
}

/// Rust-backed PVA PV object for ophyd's control layer.
#[pyclass(name = "EpicsRsPvaPV")]
pub struct EpicsRsPvaPV {
    runtime: Arc<Runtime>,
    client: PvaClient,
    #[pyo3(get)]
    pub(crate) pvname: String,
    /// Stored monitor handle so dropping the PV aborts the subscription.
    /// Wrapped in Arc so the spawned subscribe task can install it
    /// asynchronously without blocking the calling Python thread.
    monitor_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
    /// JoinHandle for the spawned subscribe task — aborted by
    /// clear_monitors so an in-flight subscribe does not later try to
    /// re-acquire the GIL after Python has finalized.
    monitor_setup_task: Mutex<Option<JoinHandle<()>>>,
    /// JoinHandle for set_access_callback's spawned probe task. Same
    /// teardown race as monitor_setup_task — aborted by disconnect.
    access_setup_task: Mutex<Option<JoinHandle<()>>>,
    /// Python dispatch thread for monitor callbacks.
    dispatch_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Sender to dispatch thread; None = no monitor active.
    monitor_tx: Arc<Mutex<Option<std::sync::mpsc::Sender<PvaMonitorEvent>>>>,
    /// Single Python monitor callback (overwritten on re-register).
    py_monitor_callback: Arc<Mutex<Option<PyObject>>>,
    /// Connection callback set from Python side.
    connection_callback: Arc<Mutex<Option<PyObject>>>,
    /// Access-rights callback set from Python side. Stored in Arc so the
    /// access-setup task reads through it — disconnect can clear the
    /// slot and the post-await callback fire becomes a no-op even if
    /// the spawned task survived past the abort signal.
    access_callback: Arc<Mutex<Option<PyObject>>>,
    /// Background task watching the connect handle for state changes.
    connection_task: Mutex<Option<JoinHandle<()>>>,
    /// Cached connection state — exposed to Python as a fast read.
    connected: Arc<Mutex<bool>>,
}

impl EpicsRsPvaPV {
    fn new(runtime: Arc<Runtime>, client: PvaClient, pvname: String) -> Self {
        Self {
            runtime,
            client,
            pvname,
            monitor_handle: Arc::new(Mutex::new(None)),
            monitor_setup_task: Mutex::new(None),
            access_setup_task: Mutex::new(None),
            dispatch_thread: Mutex::new(None),
            monitor_tx: Arc::new(Mutex::new(None)),
            py_monitor_callback: Arc::new(Mutex::new(None)),
            connection_callback: Arc::new(Mutex::new(None)),
            access_callback: Arc::new(Mutex::new(None)),
            connection_task: Mutex::new(None),
            connected: Arc::new(Mutex::new(false)),
        }
    }

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
}

#[pymethods]
impl EpicsRsPvaPV {
    /// Block until the channel is connected (resolved + Active state).
    fn wait_for_connection(&self, py: Python<'_>, timeout: f64) -> PyResult<bool> {
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        let connected_ref = self.connected.clone();
        let result = py.allow_threads(|| {
            self.spawn_wait(async move { tokio::time::timeout(dur, client.pvconnect(&name)).await })
        })?;
        let ok = matches!(result, Ok(Ok(_)));
        *connected_ref.lock() = ok;
        Ok(ok)
    }

    /// Get value with metadata as an ophyd-compatible dict.
    /// `form` is accepted for CA-shim compatibility but ignored — PVA
    /// always returns NTScalar with alarm + timestamp + display.
    #[pyo3(signature = (timeout=2.0, form="time", count=0))]
    fn get_with_metadata(
        &self,
        py: Python<'_>,
        timeout: f64,
        form: &str,
        count: u32,
    ) -> PyResult<Option<PyObject>> {
        let _ = (form, count);
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        let result = py.allow_threads(|| {
            self.spawn_wait(async move { tokio::time::timeout(dur, client.pvget(&name)).await })
        })?;
        match result {
            Ok(Ok(field)) => Ok(Some(pvfield_to_metadata(py, &field))),
            Ok(Err(e)) => {
                eprintln!("pvget({}) failed: {e}", self.pvname);
                Ok(None)
            }
            Err(_) => {
                eprintln!("pvget({}) timed out", self.pvname);
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

    /// Channel info passthrough — for parity with the CA wrapper.
    /// PVA does not have a separate non-IO info call, so this just
    /// resolves the channel and returns the server address.
    fn get_channel_info(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let client = self.client.clone();
        let name = self.pvname.clone();
        let result =
            py.allow_threads(|| self.spawn_wait(async move { client.pvconnect(&name).await }))?;
        match result {
            Ok(addr) => {
                let dict = PyDict::new(py);
                let _ = dict.set_item("type", "pva");
                let _ = dict.set_item("host", addr.to_string());
                let _ = dict.set_item("read_access", true);
                let _ = dict.set_item("write_access", true);
                Ok(Some(dict.into_any().unbind()))
            }
            Err(_) => Ok(None),
        }
    }

    /// Best-effort prefetch: just call get and return the dict.
    /// PVA has no separate "prefetch" path; the channel cache covers warm-up.
    #[pyo3(signature = (timeout=5.0))]
    fn connect_and_prefetch(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<PyObject>> {
        self.get_with_metadata(py, timeout, "ctrl", 0)
    }

    /// Write a value to the PV. Uses string-form pvput which the server
    /// parses into the destination type — works for scalars and string-
    /// formatted arrays. For typed put without string round-trip, use
    /// `put_typed` (TODO).
    #[pyo3(signature = (value, wait=false, timeout=300.0, callback=None))]
    fn put(
        &self,
        py: Python<'_>,
        value: &Bound<'_, pyo3::PyAny>,
        wait: bool,
        timeout: f64,
        callback: Option<PyObject>,
    ) -> PyResult<()> {
        // Convert Python value to a string the PVA server can parse.
        // For lists/arrays we serialize as comma-separated values, which
        // pvxs / pvput accept for ScalarArray.
        let value_str = python_value_to_pvput_string(value)?;

        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);

        if wait {
            let result = py.allow_threads(|| {
                self.spawn_wait(async move {
                    tokio::time::timeout(dur, client.pvput(&name, &value_str)).await
                })
            })?;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(PyRuntimeError::new_err(format!("pvput failed: {e}"))),
                Err(_) => return Err(PyRuntimeError::new_err("pvput timed out")),
            }
            if let Some(cb) = callback {
                cb.call0(py)?;
            }
        } else if let Some(cb) = callback {
            let pvname = self.pvname.clone();
            self.runtime.spawn(async move {
                let success =
                    match tokio::time::timeout(dur, client.pvput(&pvname, &value_str)).await {
                        Ok(Ok(())) => true,
                        Ok(Err(e)) => {
                            eprintln!("[pvput] {pvname} error: {e}");
                            false
                        }
                        Err(_) => {
                            eprintln!("[pvput] {pvname} timed out");
                            false
                        }
                    };
                Python::with_gil(|py| {
                    let _ = cb.call1(py, (success,));
                });
            });
        } else {
            let pvname = self.pvname.clone();
            self.runtime.spawn(async move {
                if let Err(e) = tokio::time::timeout(dur, client.pvput(&pvname, &value_str)).await {
                    eprintln!("[pvput] {pvname} error: {e}");
                }
            });
        }
        Ok(())
    }

    /// Register a monitor callback. Replaces any previous callback.
    fn add_monitor_callback(&self, py: Python<'_>, callback: PyObject) {
        // Always swap the callback first — the dispatch thread reads
        // through this Arc<Mutex>, so subsequent monitor events will
        // automatically use the new callback even if a setup task is
        // still in flight.
        *self.py_monitor_callback.lock() = Some(callback.clone_ref(py));

        // Skip starting a new subscription if one is either already
        // installed (monitor_handle) OR currently being set up
        // (monitor_setup_task). Checking only monitor_handle would race:
        // during pvmonitor_handle().await the handle is still None and a
        // second registration would spawn a duplicate setup task and
        // overwrite monitor_setup_task — losing the abort handle for
        // the first one.
        if self.monitor_handle.lock().is_some() || self.monitor_setup_task.lock().is_some() {
            return;
        }

        let (tx, rx) = std::sync::mpsc::channel::<PvaMonitorEvent>();
        *self.monitor_tx.lock() = Some(tx.clone());

        // Python dispatch thread
        let cb_ref = self.py_monitor_callback.clone();
        let dispatch = std::thread::spawn(move || {
            while let Ok(event) = rx.recv() {
                Python::with_gil(|py| {
                    let guard = cb_ref.lock();
                    let callback = match &*guard {
                        Some(cb) => cb.clone_ref(py),
                        None => return,
                    };
                    drop(guard);
                    let kwargs = PyDict::new(py);
                    let md = pvfield_to_metadata(py, &event.field);
                    let md_ref = md.downcast_bound::<PyDict>(py).unwrap();
                    // Spread metadata keys into kwargs
                    let _ = kwargs.set_item("pvname", &event.pvname);
                    for (k, v) in md_ref.iter() {
                        let _ = kwargs.set_item(k, v);
                    }
                    let _ = callback.call(py, (), Some(&kwargs));
                });
            }
        });
        *self.dispatch_thread.lock() = Some(dispatch);

        // Tokio monitor task — fire callback by sending events through the queue.
        let client = self.client.clone();
        let pvname = self.pvname.clone();
        let connected_ref = self.connected.clone();
        let conn_cb_ref = self.connection_callback.clone();

        // Spawn the subscribe asynchronously and install the resulting
        // SubscriptionHandle from inside the spawned task. We do NOT
        // block the calling Python thread waiting for it — a slow IOC
        // or unresolvable PV would otherwise hang add_monitor_callback
        // indefinitely. The handle slot uses Arc so the spawned task
        // can write to it without lifetime gymnastics.
        let pvname_for_call = pvname.clone();
        let pvname_for_event = pvname.clone();
        let handle_slot = self.monitor_handle.clone();
        let setup = self.runtime.spawn(async move {
            match client
                .pvmonitor_handle(&pvname_for_call, move |_desc, value| {
                    let event = PvaMonitorEvent {
                        pvname: pvname_for_event.clone(),
                        field: value.clone(),
                    };
                    let _ = tx.send(event);
                })
                .await
            {
                Ok(handle) => {
                    *handle_slot.lock() = Some(handle);
                    *connected_ref.lock() = true;
                    // Only acquire the GIL if there's actually a callback to fire.
                    // After clear_monitors / interpreter shutdown, conn_cb_ref may
                    // be empty — skipping with_gil avoids panics during teardown.
                    let cb = conn_cb_ref
                        .lock()
                        .as_ref()
                        .map(|c| Python::with_gil(|py| c.clone_ref(py)));
                    if let Some(cb) = cb {
                        Python::with_gil(|py| {
                            let _ = cb.call1(py, (true,));
                        });
                    }
                }
                Err(e) => {
                    eprintln!("[pvmonitor_handle] {pvname_for_call}: subscribe failed: {e}");
                }
            }
        });
        *self.monitor_setup_task.lock() = Some(setup);
    }

    /// Set a connection callback. Best-effort: PVA's connect builder
    /// fires on_connect/on_disconnect, but we attach a lightweight
    /// background task that polls pvconnect once and emits Connected.
    /// Disconnect events surface via the monitor task ending.
    fn set_connection_callback(&self, py: Python<'_>, callback: PyObject) {
        *self.connection_callback.lock() = Some(callback.clone_ref(py));

        // If a connection task already exists, just leave the new cb registered.
        if self.connection_task.lock().is_some() {
            // Emit current state immediately so callers see it.
            let connected = *self.connected.lock();
            let _ = callback.call1(py, (connected,));
            return;
        }

        let client = self.client.clone();
        let pvname = self.pvname.clone();
        let cb_ref = self.connection_callback.clone();
        let connected_ref = self.connected.clone();

        let handle = self.runtime.spawn(async move {
            // One-shot connect probe with bounded timeout. After this,
            // disconnects are surfaced through the monitor task (if active).
            let connected =
                tokio::time::timeout(Duration::from_secs(30), client.pvconnect(&pvname))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .is_some();
            *connected_ref.lock() = connected;
            Python::with_gil(|py| {
                let guard = cb_ref.lock();
                if let Some(cb) = &*guard {
                    let cb_clone = cb.clone_ref(py);
                    drop(guard);
                    let _ = cb_clone.call1(py, (connected,));
                }
            });
        });
        *self.connection_task.lock() = Some(handle);
    }

    /// Access rights callback — PVA does not surface separate access
    /// rights, so we fire (true, true) once after connection.
    ///
    /// Teardown safety:
    /// - The callback is stored in an Arc<Mutex<Option<PyObject>>> slot,
    ///   not captured directly into the spawn closure. disconnect()
    ///   can clear the slot, and the spawned task checks it under lock
    ///   before reaching Python::with_gil — so a callback already past
    ///   the pvconnect await still becomes a no-op if disconnect ran.
    /// - pvconnect is bounded by a 30s timeout so the task cannot live
    ///   indefinitely waiting for an unresolvable PV.
    /// - The JoinHandle is stored in access_setup_task and aborted by
    ///   disconnect (and replaced+aborted by re-registration).
    fn set_access_callback(&self, py: Python<'_>, callback: PyObject) {
        *self.access_callback.lock() = Some(callback.clone_ref(py));

        let pvname = self.pvname.clone();
        let client = self.client.clone();
        let cb_ref = self.access_callback.clone();
        let task = self.runtime.spawn(async move {
            // Bounded probe — a PV that never resolves should not pin
            // a spawned task forever.
            let _ = tokio::time::timeout(Duration::from_secs(30), client.pvconnect(&pvname)).await;

            // Atomically read+clone the callback under the same lock.
            // If disconnect() cleared the slot during the await, this
            // returns None and we skip Python::with_gil entirely
            // (avoids interpreter-finalize panics).
            let cb = cb_ref
                .lock()
                .as_ref()
                .map(|c| Python::with_gil(|py| c.clone_ref(py)));
            if let Some(cb) = cb {
                Python::with_gil(|py| {
                    let _ = cb.call1(py, (true, true));
                });
            }
        });
        // Abort any previous probe so a re-registration cannot leave
        // an orphan firing into a stale callback.
        if let Some(prev) = self.access_setup_task.lock().replace(task) {
            prev.abort();
        }
        let _ = py;
    }

    fn clear_monitors(&self) {
        *self.py_monitor_callback.lock() = None;
        *self.monitor_tx.lock() = None;
        // Abort the spawned subscribe task before it can install a
        // SubscriptionHandle (or fire the connection callback) — this
        // prevents a teardown race where pvmonitor_handle resolves
        // after Python is finalized and Python::with_gil panics.
        if let Some(task) = self.monitor_setup_task.lock().take() {
            task.abort();
        }
        // Drop any installed SubscriptionHandle to stop the subscription.
        let _ = self.monitor_handle.lock().take();
        // Dispatch thread will exit when rx sender is dropped.
    }

    fn disconnect(&self) {
        self.clear_monitors();
        // Clear callback slots BEFORE aborting tasks so any task that
        // is past the abort signal but not yet at with_gil sees the
        // slot empty and skips the Python call.
        *self.connection_callback.lock() = None;
        *self.access_callback.lock() = None;
        // Abort spawned probe tasks to prevent post-finalize Python::with_gil
        // panics. Same pattern as monitor_setup_task.
        if let Some(handle) = self.connection_task.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.access_setup_task.lock().take() {
            handle.abort();
        }
        *self.connected.lock() = false;
    }

    // ===== async surface (pyo3-async-runtimes) =====
    //
    // Mirrors the CA EpicsRsPV async methods. Same shared runtime,
    // same PvaClient channel cache as the sync methods above.

    /// Async: wait until the channel is connected. Returns True/False.
    fn connect_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        let connected_ref = self.connected.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ok = matches!(
                tokio::time::timeout(dur, client.pvconnect(&name)).await,
                Ok(Ok(_))
            );
            *connected_ref.lock() = ok;
            Ok(ok)
        })
    }

    /// Async no-op for PVA — provided for symmetry with EpicsRsPV so
    /// callers (e.g. EpicsRsSignalBackend.connect) can invoke the same
    /// method on both protocol wrappers. PVA put goes through the
    /// string-form pvput which has no native-type cache to populate.
    #[pyo3(signature = (timeout=2.0))]
    fn cache_native_type_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let _ = timeout;
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(true) })
    }

    /// Async: introspect the PV's structure via PvaClient::pvinfo.
    ///
    /// Returns a Python nested dict mirroring the PVA `FieldDesc`:
    ///
    /// ```text
    /// {"kind": "structure", "struct_id": "epics:nt/NTTable:1.0",
    ///  "fields": [
    ///    ("labels", {"kind": "scalar_array", "scalar_type": "string"}),
    ///    ("value",  {"kind": "structure", "struct_id": "",
    ///                "fields": [("a", {"kind": "scalar_array",
    ///                                  "scalar_type": "byte"}), ...]}),
    ///    ...
    ///  ]}
    /// ```
    ///
    /// Used by EpicsRsSignalBackend.connect to validate typed-PvField
    /// payloads (Table column names + dtypes) against the IOC schema
    /// at connect time, before any put hits the wire. Returns None on
    /// pvinfo timeout / error.
    #[pyo3(signature = (timeout=2.0))]
    fn get_field_desc_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(dur, client.pvinfo(&name)).await {
                Ok(Ok(desc)) => Python::with_gil(|py| {
                    Ok::<PyObject, PyErr>(field_desc_to_py(py, &desc).into_any().unbind())
                }),
                _ => Python::with_gil(|py| Ok::<PyObject, PyErr>(py.None())),
            }
        })
    }

    /// Async: read the PV value (NTScalar value field, or whole PvField).
    /// Raises on timeout / error rather than returning None silently —
    /// callers (e.g. EpicsRsSignalBackend) must not see a `None` that
    /// looks like a successful read.
    ///
    /// For NTEnum the returned dict is unwrapped to the integer index
    /// for parity with the monitor callback path (extract_ntenum).
    #[pyo3(signature = (timeout=10.0))]
    fn get_value_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let client = self.client.clone();
        let name = self.pvname.clone();
        let pvname = name.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(dur, client.pvget(&name)).await {
                Ok(Ok(field)) => Python::with_gil(|py| {
                    // NTEnum handling: top-level Structure with a
                    // `value` substructure containing {index, choices}.
                    // Surface the int index (consistent with monitor).
                    if let epics_rs::pva::pvdata::PvField::Structure(s) = &field {
                        if let Some((idx, _choices)) = crate::pva_convert::try_extract_ntenum(s) {
                            return Ok::<PyObject, PyErr>(
                                idx.into_pyobject(py).unwrap().into_any().unbind(),
                            );
                        }
                    }
                    // NTScalar: extract `value`; otherwise the whole field.
                    let value_field = match &field {
                        epics_rs::pva::pvdata::PvField::Structure(s) => s
                            .get_field("value")
                            .cloned()
                            .unwrap_or_else(|| field.clone()),
                        _ => field.clone(),
                    };
                    Ok::<PyObject, PyErr>(pvfield_to_py(py, &value_field))
                }),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "pvget on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "pvget on {pvname} timed out after {timeout}s"
                ))),
            }
        })
    }

    /// Async: read value + metadata as ophyd-compatible dict.
    /// Raises on timeout / error.
    #[pyo3(signature = (timeout=10.0, form="time"))]
    fn get_reading_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
        form: &str,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let _ = form;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let pvname = name.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(dur, client.pvget(&name)).await {
                Ok(Ok(field)) => Python::with_gil(|py| {
                    Ok::<PyObject, PyErr>(crate::pva_convert::pvfield_to_metadata(py, &field))
                }),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "pvget on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "pvget on {pvname} timed out after {timeout}s"
                ))),
            }
        })
    }

    /// Async: write a value via string-form pvput. Returns True on success.
    #[pyo3(signature = (value, timeout=300.0))]
    fn put_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, pyo3::PyAny>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let value_str = python_value_to_pvput_string(value)?;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::time::timeout(dur, client.pvput(&name, &value_str)).await;
            Ok(matches!(result, Ok(Ok(()))))
        })
    }

    /// Async: short-bounded pvput for `wait=False` semantics.
    ///
    /// PVA has no wire-level fire-and-forget primitive (every PUT
    /// expects a PUT_RESPONSE), so the closest we can offer is a put
    /// with a tight default timeout (5 s) that surfaces the real
    /// outcome to the caller — `True` on server ack, `False` on
    /// timeout / error. This is the trade-off for busy / acquire PVs
    /// where waiting forever for ack causes deadlock: bounded wait,
    /// honest result.
    ///
    /// (The earlier spawn-and-return approach was changed because it
    /// silently swallowed value/type/permission errors — the user's
    /// plan would proceed as if the write succeeded.)
    #[pyo3(signature = (value))]
    fn put_nowait_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, pyo3::PyAny>,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let value_str = python_value_to_pvput_string(value)?;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let pvname = name.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(Duration::from_secs(5), client.pvput(&pvname, &value_str))
                .await
            {
                Ok(Ok(())) => Ok(true),
                Ok(Err(e)) => {
                    tracing::warn!(target: "ophyd_epicsrs::pvput_nowait", pv = %pvname, "put failed: {e}");
                    Ok(false)
                }
                Err(_) => {
                    tracing::warn!(target: "ophyd_epicsrs::pvput_nowait", pv = %pvname, "timed out (5s)");
                    Ok(false)
                }
            }
        })
    }

    /// Async: typed put for structured PVA targets (e.g. NTTable).
    /// Unlike `put_async` (string-form pvput), this constructs a
    /// properly-typed `PvField` from the Python value using
    /// `pva_put::py_to_pvfield`, then dispatches via `pvput_pv_field`.
    ///
    /// Parameters
    /// ----------
    /// value : dict | list | scalar
    ///     The value to write. Dicts become PvStructure with the given
    ///     `struct_id`; lists become ScalarArrayTyped using `dtype_hints`
    ///     (column → numpy dtype string / "string") to resolve the
    ///     element type — critical for empty columns where no inference
    ///     is possible.
    /// dtype_hints : dict[str, str], optional
    ///     Per-field dtype overrides. Keys match field names in the
    ///     value dict; values are numpy dtype strings (e.g. "<i1",
    ///     "<f8") or the literal "string".
    /// struct_id : str, optional
    ///     PVA struct_id stamped onto the top-level PvStructure
    ///     (e.g. "epics:nt/NTTable:1.0"). Empty string leaves it unset.
    /// timeout : float, optional
    ///     pvput timeout in seconds (default 300.0).
    #[pyo3(signature = (value, dtype_hints=None, struct_id="", timeout=300.0))]
    fn put_pv_field_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, pyo3::PyAny>,
        dtype_hints: Option<&Bound<'_, PyDict>>,
        struct_id: &str,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let hints = extract_dtype_hints(dtype_hints)?;
        let field = crate::pva_put::py_to_pvfield(value, &hints, struct_id)?;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::time::timeout(dur, client.pvput_pv_field(&name, &field)).await;
            Ok(matches!(result, Ok(Ok(()))))
        })
    }

    /// Async: short-bounded typed put for `wait=False` semantics.
    /// Same trade-off as `put_nowait_async`: 5 s bounded wait so the
    /// caller sees the real outcome instead of having errors silently
    /// swallowed.
    #[pyo3(signature = (value, dtype_hints=None, struct_id=""))]
    fn put_pv_field_nowait_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, pyo3::PyAny>,
        dtype_hints: Option<&Bound<'_, PyDict>>,
        struct_id: &str,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let hints = extract_dtype_hints(dtype_hints)?;
        let field = crate::pva_put::py_to_pvfield(value, &hints, struct_id)?;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let pvname = name.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(
                Duration::from_secs(5),
                client.pvput_pv_field(&pvname, &field),
            )
            .await
            {
                Ok(Ok(())) => Ok(true),
                Ok(Err(e)) => {
                    tracing::warn!(target: "ophyd_epicsrs::pvput_pv_field_nowait", pv = %pvname, "put failed: {e}");
                    Ok(false)
                }
                Err(_) => {
                    tracing::warn!(target: "ophyd_epicsrs::pvput_pv_field_nowait", pv = %pvname, "timed out (5s)");
                    Ok(false)
                }
            }
        })
    }

    fn __repr__(&self) -> String {
        format!("EpicsRsPvaPV('{}')", self.pvname)
    }
}

/// Convert a `FieldDesc` to a Python nested dict for schema introspection.
fn field_desc_to_py<'py>(py: Python<'py>, desc: &FieldDesc) -> Bound<'py, PyDict> {
    let dict = PyDict::new(py);
    match desc {
        FieldDesc::Scalar(st) => {
            let _ = dict.set_item("kind", "scalar");
            let _ = dict.set_item("scalar_type", scalar_type_name(*st));
        }
        FieldDesc::ScalarArray(st) => {
            let _ = dict.set_item("kind", "scalar_array");
            let _ = dict.set_item("scalar_type", scalar_type_name(*st));
        }
        FieldDesc::Structure { struct_id, fields } => {
            let _ = dict.set_item("kind", "structure");
            let _ = dict.set_item("struct_id", struct_id.as_str());
            let pairs: Vec<(String, Bound<'py, PyDict>)> = fields
                .iter()
                .map(|(n, f)| (n.clone(), field_desc_to_py(py, f)))
                .collect();
            let _ = dict.set_item("fields", pairs);
        }
        FieldDesc::StructureArray { struct_id, fields } => {
            let _ = dict.set_item("kind", "structure_array");
            let _ = dict.set_item("struct_id", struct_id.as_str());
            let pairs: Vec<(String, Bound<'py, PyDict>)> = fields
                .iter()
                .map(|(n, f)| (n.clone(), field_desc_to_py(py, f)))
                .collect();
            let _ = dict.set_item("fields", pairs);
        }
        FieldDesc::Union {
            struct_id,
            variants,
        } => {
            let _ = dict.set_item("kind", "union");
            let _ = dict.set_item("struct_id", struct_id.as_str());
            let pairs: Vec<(String, Bound<'py, PyDict>)> = variants
                .iter()
                .map(|(n, f)| (n.clone(), field_desc_to_py(py, f)))
                .collect();
            let _ = dict.set_item("variants", pairs);
        }
        FieldDesc::UnionArray {
            struct_id,
            variants,
        } => {
            let _ = dict.set_item("kind", "union_array");
            let _ = dict.set_item("struct_id", struct_id.as_str());
            let pairs: Vec<(String, Bound<'py, PyDict>)> = variants
                .iter()
                .map(|(n, f)| (n.clone(), field_desc_to_py(py, f)))
                .collect();
            let _ = dict.set_item("variants", pairs);
        }
        FieldDesc::Variant => {
            let _ = dict.set_item("kind", "variant");
        }
        FieldDesc::VariantArray => {
            let _ = dict.set_item("kind", "variant_array");
        }
        FieldDesc::BoundedString(max_len) => {
            let _ = dict.set_item("kind", "bounded_string");
            let _ = dict.set_item("max_len", *max_len);
        }
    }
    dict
}

fn scalar_type_name(st: ScalarType) -> &'static str {
    match st {
        ScalarType::Boolean => "boolean",
        ScalarType::Byte => "byte",
        ScalarType::Short => "short",
        ScalarType::Int => "int",
        ScalarType::Long => "long",
        ScalarType::UByte => "ubyte",
        ScalarType::UShort => "ushort",
        ScalarType::UInt => "uint",
        ScalarType::ULong => "ulong",
        ScalarType::Float => "float",
        ScalarType::Double => "double",
        ScalarType::String => "string",
    }
}

/// Extract the optional dtype-hint Python dict into a Rust HashMap.
fn extract_dtype_hints(
    hints: Option<&Bound<'_, PyDict>>,
) -> PyResult<std::collections::HashMap<String, String>> {
    let mut out = std::collections::HashMap::new();
    if let Some(d) = hints {
        for (k, v) in d.iter() {
            out.insert(k.extract()?, v.extract()?);
        }
    }
    Ok(out)
}

/// Convert a Python value to a string suitable for `PvaClient::pvput`.
/// pvxs accepts comma-separated values for arrays.
fn python_value_to_pvput_string(value: &Bound<'_, pyo3::PyAny>) -> PyResult<String> {
    // numpy scalar → Python scalar
    if value.hasattr("dtype").unwrap_or(false) && value.hasattr("ndim").unwrap_or(false) {
        let ndim: i32 = value.getattr("ndim").and_then(|v| v.extract()).unwrap_or(0);
        if ndim == 0 {
            if let Ok(native) = value.call_method0("item") {
                return python_value_to_pvput_string(&native);
            }
        } else {
            // numpy array → list of strings
            let lst: Vec<Bound<'_, pyo3::PyAny>> = value.try_iter()?.collect::<PyResult<_>>()?;
            let parts: Vec<String> = lst
                .iter()
                .map(python_value_to_pvput_string)
                .collect::<PyResult<_>>()?;
            return Ok(parts.join(","));
        }
    }
    if let Ok(seq) = value.downcast::<pyo3::types::PyList>() {
        let parts: Vec<String> = seq
            .iter()
            .map(|v| python_value_to_pvput_string(&v))
            .collect::<PyResult<_>>()?;
        return Ok(parts.join(","));
    }
    if let Ok(seq) = value.downcast::<pyo3::types::PyTuple>() {
        let parts: Vec<String> = seq
            .iter()
            .map(|v| python_value_to_pvput_string(&v))
            .collect::<PyResult<_>>()?;
        return Ok(parts.join(","));
    }
    // Scalar fallback — Python's str() representation works for most
    // numeric / boolean / string types and matches pvput's parser.
    if let Ok(b) = value.extract::<bool>() {
        return Ok(if b {
            "true".to_string()
        } else {
            "false".to_string()
        });
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(s);
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(f.to_string());
    }
    let s = value.str()?.to_string();
    Ok(s)
}
