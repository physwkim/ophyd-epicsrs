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
use epics_rs::pva::pvdata::PvField;

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
        EpicsRsPvaPV::new(self.runtime.clone(), self.client.clone(), pvname.to_string())
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
    monitor_handle: Mutex<Option<SubscriptionHandle>>,
    /// Python dispatch thread for monitor callbacks.
    dispatch_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Sender to dispatch thread; None = no monitor active.
    monitor_tx: Arc<Mutex<Option<std::sync::mpsc::Sender<PvaMonitorEvent>>>>,
    /// Single Python monitor callback (overwritten on re-register).
    py_monitor_callback: Arc<Mutex<Option<PyObject>>>,
    /// Connection callback set from Python side.
    connection_callback: Arc<Mutex<Option<PyObject>>>,
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
            monitor_handle: Mutex::new(None),
            dispatch_thread: Mutex::new(None),
            monitor_tx: Arc::new(Mutex::new(None)),
            py_monitor_callback: Arc::new(Mutex::new(None)),
            connection_callback: Arc::new(Mutex::new(None)),
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
        rx.recv().map_err(|_| PyRuntimeError::new_err("runtime task failed"))
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
            self.spawn_wait(async move {
                tokio::time::timeout(dur, client.pvconnect(&name)).await
            })
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
            self.spawn_wait(async move {
                tokio::time::timeout(dur, client.pvget(&name)).await
            })
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
        let result = py.allow_threads(|| {
            self.spawn_wait(async move { client.pvconnect(&name).await })
        })?;
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
    fn connect_and_prefetch(
        &self,
        py: Python<'_>,
        timeout: f64,
    ) -> PyResult<Option<PyObject>> {
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
                let success = match tokio::time::timeout(dur, client.pvput(&pvname, &value_str)).await {
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
        *self.py_monitor_callback.lock() = Some(callback.clone_ref(py));

        // If a monitor task already exists, just swap the callback.
        if self.monitor_handle.lock().is_some() {
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

        // Spawn the monitor + capture its SubscriptionHandle synchronously.
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let pvname_for_call = pvname.clone();
        let pvname_for_event = pvname.clone();
        self.runtime.spawn(async move {
            let result = client.pvmonitor_handle(&pvname_for_call, move |_desc, value| {
                let event = PvaMonitorEvent {
                    pvname: pvname_for_event.clone(),
                    field: value.clone(),
                };
                let _ = tx.send(event);
            }).await;
            let _ = handle_tx.send(result);

            // After successful subscribe, mark connected and fire connection cb.
            // Note: pvmonitor_handle returns immediately with the handle; the
            // subscription stays alive via SubscriptionHandle held by Python.
        });

        if let Ok(Ok(handle)) = handle_rx.recv() {
            *self.monitor_handle.lock() = Some(handle);
            *connected_ref.lock() = true;
            // Fire connection callback if set
            Python::with_gil(|py| {
                let guard = conn_cb_ref.lock();
                if let Some(cb) = &*guard {
                    let cb_clone = cb.clone_ref(py);
                    drop(guard);
                    let _ = cb_clone.call1(py, (true,));
                }
            });
        }
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
            let connected = tokio::time::timeout(
                Duration::from_secs(30),
                client.pvconnect(&pvname),
            )
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
    fn set_access_callback(&self, py: Python<'_>, callback: PyObject) {
        let cb = callback;
        // Fire a fast best-effort on background task.
        let pvname = self.pvname.clone();
        let client = self.client.clone();
        self.runtime.spawn(async move {
            let _ = client.pvconnect(&pvname).await;
            Python::with_gil(|py| {
                let _ = cb.call1(py, (true, true));
            });
        });
        let _ = py;
    }

    fn clear_monitors(&self) {
        *self.py_monitor_callback.lock() = None;
        *self.monitor_tx.lock() = None;
        // Drop the SubscriptionHandle to abort the monitor task.
        let _ = self.monitor_handle.lock().take();
        // Dispatch thread will exit when rx sender is dropped.
    }

    fn disconnect(&self) {
        self.clear_monitors();
        *self.connection_callback.lock() = None;
        if let Some(handle) = self.connection_task.lock().take() {
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
            let ok = matches!(tokio::time::timeout(dur, client.pvconnect(&name)).await, Ok(Ok(_)));
            *connected_ref.lock() = ok;
            Ok(ok)
        })
    }

    /// Async: read the PV value (NTScalar value field, or whole PvField).
    /// Returns Python value, or None on failure/timeout.
    #[pyo3(signature = (timeout=2.0))]
    fn get_value_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::time::timeout(dur, client.pvget(&name)).await;
            match result {
                Ok(Ok(field)) => Python::with_gil(|py| {
                    // Extract `.value` for NTScalar; fall back to whole field.
                    let value_field = match &field {
                        epics_rs::pva::pvdata::PvField::Structure(s) => s
                            .get_field("value")
                            .cloned()
                            .unwrap_or_else(|| field.clone()),
                        _ => field.clone(),
                    };
                    Ok(crate::pva_convert::pvfield_to_py(py, &value_field))
                }),
                _ => Python::with_gil(|py| Ok(py.None())),
            }
        })
    }

    /// Async: read value + metadata as ophyd-compatible dict.
    #[pyo3(signature = (timeout=2.0, form="time"))]
    fn get_reading_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
        form: &str,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let _ = form;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::time::timeout(dur, client.pvget(&name)).await;
            match result {
                Ok(Ok(field)) => Python::with_gil(|py| {
                    Ok(crate::pva_convert::pvfield_to_metadata(py, &field))
                }),
                _ => Python::with_gil(|py| Ok(py.None())),
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

    fn __repr__(&self) -> String {
        format!("EpicsRsPvaPV('{}')", self.pvname)
    }
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
        return Ok(if b { "true".to_string() } else { "false".to_string() });
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

// Suppress dead code warning until Python wrapper exposes typed put paths.
#[allow(dead_code)]
fn _touch_imports() {
    let _: Option<&dyn Fn(Python<'_>, &PvField) -> PyObject> = Some(&pvfield_to_py);
}
