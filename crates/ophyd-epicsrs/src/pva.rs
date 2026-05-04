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
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

const NTENUM_UNKNOWN: u8 = 0;
const NTENUM_TRUE: u8 = 1;
const NTENUM_FALSE: u8 = 2;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use epics_rs::pva::client_native::context::PvaClient;
use epics_rs::pva::client_native::ops_v2::SubscriptionHandle;
use epics_rs::pva::pvdata::{FieldDesc, PvField, ScalarType};

use crate::pva_convert::{pvfield_to_py, EpicsRsPvaMetadata};

/// Monitor event queued from tokio task → Python thread.
struct PvaMonitorEvent {
    pvname: String,
    field: PvField,
}

/// Shared core for the sync and async ``bulk_get`` paths.
///
/// Uses concurrent per-PV ``pvget`` (spawned as N tokio tasks) instead
/// of ``PvaClient::pvget_many``: the latter takes a batched warm-path
/// shortcut that, against an ``epics-bridge-rs`` qsrv-fronted IOC,
/// fails systematically because the server does not honour reused
/// ``ioid`` GET frames after the first INIT+GET. ``op_get`` (called
/// by ``pvget``) carries an internal cold-path fallback when the warm
/// GET returns an error status, so per-PV ``pvget`` works correctly.
/// Failures (timeout / per-PV error) surface as ``None``.
///
/// Performance trade-off: we lose the single-TCP-write batching that
/// ``pvget_many`` offered on the happy path. For 10 PVs against a
/// qsrv bridge this means roughly N concurrent op_get calls instead
/// of one batched call — still issued in parallel via tokio task
/// scheduling, just with N writer-task hops instead of one. The
/// correct-but-slightly-slower path is acceptable until ``pvget_many``
/// gains its own warm-failure cold fallback upstream.
async fn run_bulk_get_pva(
    client: PvaClient,
    pvnames: Vec<String>,
    dur: Duration,
) -> Vec<(String, Option<PvField>)> {
    let mut handles = Vec::with_capacity(pvnames.len());
    for name in &pvnames {
        let client = client.clone();
        let name_owned = name.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::timeout(dur, client.pvget(&name_owned))
                .await
                .ok()
                .and_then(|r| r.ok())
        }));
    }

    let mut results = Vec::with_capacity(pvnames.len());
    for (name, h) in pvnames.into_iter().zip(handles) {
        let result = h.await.ok().flatten();
        results.push((name, result));
    }
    results
}

/// Shared PVA context — Runtime + PvaClient.
///
/// Do NOT construct this directly. Use ``ophyd_epicsrs.get_pva_context()``
/// — see the symmetric note on ``EpicsRsContext`` for the singleton
/// rationale (one ``Client`` per protocol per process).
#[pyclass(name = "EpicsRsPvaContext")]
pub struct EpicsRsPvaContext {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) client: PvaClient,
    /// Live ``EpicsRsPvaPV`` wrapper count — same role as
    /// ``EpicsRsContext::pv_count``. Surfaces through ``is_unused()``
    /// so ``shutdown_all`` can refuse to drop the singleton while
    /// channels are still alive.
    pv_count: Arc<AtomicUsize>,
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

        Ok(Self {
            runtime,
            client,
            pv_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a PVA channel wrapper for the given name.
    fn create_pv(&self, pvname: &str) -> EpicsRsPvaPV {
        self.pv_count.fetch_add(1, Ordering::AcqRel);
        EpicsRsPvaPV::new(
            self.runtime.clone(),
            self.client.clone(),
            pvname.to_string(),
            self.pv_count.clone(),
        )
    }

    /// True iff no ``EpicsRsPvaPV`` wrappers created from this context
    /// are currently alive.
    fn is_unused(&self) -> bool {
        self.pv_count.load(Ordering::Acquire) == 0
    }

    /// Bulk PVA read — concurrent ``pvget`` on N PV names.
    ///
    /// Spawns N tokio tasks that each call ``PvaClient::pvget``;
    /// channels are cached inside ``PvaClient`` so PVs on the same
    /// server share one TCP connection. Bypasses
    /// ``PvaClient::pvget_many`` — see ``run_bulk_get_pva`` for the
    /// rationale (qsrv bridge rejects pvget_many's reused-ioid warm
    /// path on every call after the first). Returns
    /// ``{pvname: metadata | None}`` — successes map to an
    /// ``EpicsRsPvaMetadata`` (lazy dict-like wrapper), failures
    /// (timeout / per-PV error) to ``None``.
    ///
    /// The GIL is released while the concurrent GETs run on tokio.
    #[pyo3(signature = (pvnames, timeout=5.0))]
    fn bulk_get(
        &self,
        py: Python<'_>,
        pvnames: Vec<String>,
        timeout: f64,
    ) -> PyResult<PyObject> {
        let client = self.client.clone();
        let dur = Duration::from_secs_f64(timeout);

        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            let results = run_bulk_get_pva(client, pvnames, dur).await;
            let _ = tx.send(results);
        });

        let results = py.allow_threads(move || {
            rx.recv()
                .map_err(|_| PyRuntimeError::new_err("bulk_get failed"))
        })?;

        let dict = PyDict::new(py);
        for (pvname, maybe_field) in results {
            match maybe_field {
                Some(field) => {
                    let md = EpicsRsPvaMetadata::new(field);
                    dict.set_item(&pvname, md.into_pyobject(py).unwrap())?;
                }
                None => dict.set_item(&pvname, py.None())?,
            }
        }
        Ok(dict.into_any().unbind())
    }

    /// Async variant of ``bulk_get`` — returns a Python awaitable.
    ///
    /// Drop-in for asyncio / ophyd-async callers. Same semantics as
    /// the sync ``bulk_get`` but does not block the calling thread,
    /// so a single asyncio loop can interleave bulk reads with other
    /// device work.
    #[pyo3(signature = (pvnames, timeout=5.0))]
    fn bulk_get_async<'py>(
        &self,
        py: Python<'py>,
        pvnames: Vec<String>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let client = self.client.clone();
        let dur = Duration::from_secs_f64(timeout);

        // Same fast-path async wrapper used by per-PV
        // ``get_value_async`` — see that method for the trade-offs.
        pyo3_async_runtimes::tokio::future_into_py_fast(py, async move {
            let results = run_bulk_get_pva(client, pvnames, dur).await;
            Python::with_gil(|py| {
                let dict = PyDict::new(py);
                for (pvname, maybe_field) in results {
                    match maybe_field {
                        Some(field) => {
                            let md = EpicsRsPvaMetadata::new(field);
                            dict.set_item(&pvname, md.into_pyobject(py).unwrap())?;
                        }
                        None => dict.set_item(&pvname, py.None())?,
                    }
                }
                Ok::<PyObject, PyErr>(dict.into_any().unbind())
            })
        })
    }

    fn __repr__(&self) -> String {
        "EpicsRsPvaContext(active)".to_string()
    }
}

/// Rust-backed PVA PV object for ophyd's control layer.
///
/// Returned by ``EpicsRsPvaContext.create_pv(name)``. Do not construct
/// directly; obtain the parent context via
/// ``ophyd_epicsrs.get_pva_context()``.
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
    /// Generation token incremented per `add_monitor_callback`. Each
    /// dispatch thread captures its expected token at spawn time and
    /// refuses to fire the callback once the canonical token has
    /// advanced — same race-guard pattern as EpicsRsPV (see pv.rs).
    monitor_generation: Arc<std::sync::atomic::AtomicU64>,
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
    /// NTEnum-shape detection result, populated from the first pvget
    /// (or any subsequent get/monitor that sees a structure carrying
    /// the `epics:nt/NTEnum:1.0` `struct_id` plus a `value.{index,
    /// choices}` substructure). `None` = not yet known. Used by `put`
    /// to route an int / bool value through `pvput_field("value.index",
    /// ...)` instead of plain `pvput` — string-form pvput on NTEnum is
    /// silently rejected by the server (the field path tells the
    /// server precisely where to write).
    is_ntenum: Arc<AtomicU8>,
    /// Shared with the parent ``EpicsRsPvaContext`` — decremented in
    /// Drop so ``EpicsRsPvaContext::is_unused()`` reflects live wrappers.
    pv_count: Arc<AtomicUsize>,
}

/// Classify a top-level PvField as NTEnum or not.
///
/// * `Some(true)`  — `struct_id == "epics:nt/NTEnum:1.0"`, or field-shape
///                   confirms NTEnum (index + choices sub-fields present).
/// * `Some(false)` — `struct_id` positively identifies a different NT type
///                   (starts with `"epics:nt/"` but is not NTEnum).
/// * `None`        — non-structure, or structure with empty/unknown struct_id
///                   whose NTEnum shape could not be confirmed.
///
/// Callers must treat `None` as "no new information" and must NOT flip
/// a cached `Some(true)` to `Some(false)` — monitor deltas can arrive
/// as partial structures (e.g. `value` sub-field absent) that look
/// non-NTEnum even though the channel is NTEnum.
/// Write the result of `detect_ntenum_shape` into `slot`.
///
/// Extracted so both the `&self` sync path and `async move` async paths
/// (which cannot borrow `self`) share a single call-site pattern.
fn record_ntenum_into(slot: &AtomicU8, field: &PvField) {
    if let Some(detected) = detect_ntenum_shape(field) {
        slot.store(if detected { NTENUM_TRUE } else { NTENUM_FALSE }, Ordering::Relaxed);
    }
}

fn detect_ntenum_shape(field: &PvField) -> Option<bool> {
    let s = match field {
        PvField::Structure(s) => s,
        _ => return None,
    };
    if s.struct_id.starts_with("epics:nt/NTEnum:") {
        // Covers 1.0 and any future minor bumps (shape-compatible per spec).
        // NOTE: a hypothetical NTEnum:2.x with a breaking shape change
        // (e.g. `value.choice` instead of `value.choices`) would still
        // match this prefix, but the downstream `value.index` put-routing
        // would silently break against such a server. The PVA spec has
        // not bumped a major NT version since publication; revisit if
        // that changes.
        return Some(true);
    }
    if s.struct_id.starts_with("epics:nt/") {
        // Known non-NTEnum NT type — struct_id is authoritative.
        return Some(false);
    }
    // struct_id absent: fall back to field-shape detection.
    // Only return Some(true) when extraction positively succeeds;
    // return None (not Some(false)) when it fails so we don't clear
    // an already-established Some(true) from a partial delta.
    if crate::pva_convert::try_extract_ntenum(s).is_some() {
        Some(true)
    } else {
        None
    }
}

impl EpicsRsPvaPV {
    fn new(
        runtime: Arc<Runtime>,
        client: PvaClient,
        pvname: String,
        pv_count: Arc<AtomicUsize>,
    ) -> Self {
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
            monitor_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            connection_callback: Arc::new(Mutex::new(None)),
            access_callback: Arc::new(Mutex::new(None)),
            connection_task: Mutex::new(None),
            connected: Arc::new(Mutex::new(false)),
            is_ntenum: Arc::new(AtomicU8::new(NTENUM_UNKNOWN)),
            pv_count,
        }
    }

    /// Run a future on the tokio runtime and block until done.
    ///
    /// Uses spawn+mpsc rather than Handle::block_on because the future's
    /// .await points (writer channel sends, reader channel recvs) are
    /// faster when running on a tokio worker thread (intra-scheduler
    /// wakeup ~1-3µs) vs an external thread (OS park/unpark ~5-10µs).
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

    /// Update `is_ntenum` from a freshly-read PvField.
    fn record_ntenum_shape(&self, field: &PvField) {
        record_ntenum_into(&self.is_ntenum, field);
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
            Ok(Ok(field)) => {
                if self.is_ntenum.load(Ordering::Relaxed) == NTENUM_UNKNOWN {
                    self.record_ntenum_shape(&field);
                }
                Ok(Some(EpicsRsPvaMetadata::new(field).into_pyobject(py).unwrap().into_any().unbind()))
            }
            Ok(Err(e)) => {
                tracing::warn!(target: "ophyd_epicsrs.pva", pv = %self.pvname, "pvget failed: {e}");
                Ok(None)
            }
            Err(_) => {
                tracing::warn!(target: "ophyd_epicsrs.pva", pv = %self.pvname, "pvget timed out");
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
    ///
    /// NTEnum special case: when the channel is known (from a prior
    /// get) to be NTEnum and the value is a Python int / bool, the
    /// write is dispatched via `pvput_field("value.index", ...)`.
    /// String-form pvput on NTEnum is silently rejected by the server
    /// because the top-level value is a structure, not a scalar — the
    /// dotted-path form tells the server precisely where to write.
    #[pyo3(signature = (value, wait=false, timeout=300.0, callback=None))]
    fn put(
        &self,
        py: Python<'_>,
        value: &Bound<'_, pyo3::PyAny>,
        wait: bool,
        timeout: f64,
        callback: Option<PyObject>,
    ) -> PyResult<()> {
        // NTEnum int-put: route through value.index field-path.
        let route_ntenum_index = self.is_ntenum.load(Ordering::Relaxed) == NTENUM_TRUE && py_value_is_intlike(value);
        let value_str = python_value_to_pvput_string(value)?;

        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);

        // Closure capturing the routing decision so the wait/cb/fire-
        // and-forget arms below stay short. tokio runtime is free to
        // pick which to invoke.
        let do_put = move |client: PvaClient, name: String, value_str: String| async move {
            if route_ntenum_index {
                tokio::time::timeout(dur, client.pvput_field(&name, "value.index", &value_str))
                    .await
            } else {
                tokio::time::timeout(dur, client.pvput(&name, &value_str)).await
            }
        };

        if wait {
            let result = py.allow_threads(|| self.spawn_wait(do_put(client, name, value_str)))?;
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
                let success = match do_put(client, pvname.clone(), value_str).await {
                    Ok(Ok(())) => true,
                    Ok(Err(e)) => {
                        crate::safe_warn!(target: "ophyd_epicsrs.pva", pv = %pvname, "pvput error: {e}");
                        false
                    }
                    Err(_) => {
                        crate::safe_warn!(target: "ophyd_epicsrs.pva", pv = %pvname, "pvput timed out");
                        false
                    }
                };
                crate::safe_call!(Python::with_gil(|py| {
                    let _ = cb.call1(py, (success,));
                }));
            });
        } else {
            let pvname = self.pvname.clone();
            self.runtime.spawn(async move {
                if let Err(e) = do_put(client, pvname.clone(), value_str).await {
                    crate::safe_warn!(target: "ophyd_epicsrs.pva", pv = %pvname, "pvput error: {e}");
                }
            });
        }
        Ok(())
    }

    /// Register the monitor callback.
    ///
    /// **Set semantics, not add**: only one callback at a time —
    /// matches the CA-side wrapper. Multi-callback fan-out belongs at
    /// the shim layer, which keeps a Python-side dict and registers
    /// one dispatcher with the native PV.
    fn set_monitor_callback(&self, py: Python<'_>, callback: PyObject) {
        self.add_monitor_callback(py, callback);
    }

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

        // Bump generation BEFORE installing the new dispatch thread —
        // an OLD thread still draining its rx will see its captured
        // generation differ and bail before invoking the (potentially
        // new) callback. Same race-guard pattern as EpicsRsPV.
        let generation = self
            .monitor_generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        let (tx, rx) = std::sync::mpsc::channel::<PvaMonitorEvent>();
        *self.monitor_tx.lock() = Some(tx.clone());

        // Python dispatch thread
        let cb_ref = self.py_monitor_callback.clone();
        let gen_ref = self.monitor_generation.clone();
        let is_ntenum_ref = self.is_ntenum.clone();
        let dispatch = std::thread::spawn(move || {
            while let Ok(event) = rx.recv() {
                if gen_ref.load(std::sync::atomic::Ordering::SeqCst) != generation {
                    continue;
                }
                // Feed is_ntenum cache from monitor events too. An
                // ophyd-async setup typically connects, subscribes,
                // and only then puts — so the first read may already
                // be a monitor delivery rather than an explicit
                // get_value_async.
                if is_ntenum_ref.load(Ordering::Relaxed) == NTENUM_UNKNOWN {
                    record_ntenum_into(&is_ntenum_ref, &event.field);
                }
                crate::safe_call!(Python::with_gil(|py| {
                    let guard = cb_ref.lock();
                    let callback = match &*guard {
                        Some(cb) => cb.clone_ref(py),
                        None => return,
                    };
                    drop(guard);
                    let kwargs = PyDict::new(py);
                    let md = crate::pva_convert::pvfield_to_metadata(py, &event.field, None);
                    let md_ref = md.downcast_bound::<PyDict>(py).unwrap();
                    // Spread metadata keys into kwargs
                    let _ = kwargs.set_item("pvname", &event.pvname);
                    for (k, v) in md_ref.iter() {
                        let _ = kwargs.set_item(k, v);
                    }
                    let _ = callback.call(py, (), Some(&kwargs));
                }));
            }
        });
        // Reap any previous dispatch thread on a background join — the
        // OLD thread will exit naturally when its rx Senders are gone.
        if let Some(old) = self.dispatch_thread.lock().take() {
            let _ = std::thread::Builder::new()
                .name("ophyd-epicsrs-dispatch-join".into())
                .spawn(move || {
                    let _ = old.join();
                });
        }
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
                    // Skip with_gil entirely if no callback is registered.
                    // Even when one is registered, wrap with_gil itself in
                    // safe_call! so a finalising interpreter doesn't kill
                    // the runtime.
                    crate::safe_call!({
                        let cb = conn_cb_ref
                            .lock()
                            .as_ref()
                            .map(|c| Python::with_gil(|py| c.clone_ref(py)));
                        if let Some(cb) = cb {
                            Python::with_gil(|py| {
                                let _ = cb.call1(py, (true,));
                            });
                        }
                    });
                }
                Err(e) => {
                    crate::safe_warn!(target: "ophyd_epicsrs.pva", pv = %pvname_for_call, "pvmonitor_handle subscribe failed: {e}");
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
            crate::safe_call!(Python::with_gil(|py| {
                let guard = cb_ref.lock();
                if let Some(cb) = &*guard {
                    let cb_clone = cb.clone_ref(py);
                    drop(guard);
                    let _ = cb_clone.call1(py, (connected,));
                }
            }));
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
            // returns None and we skip Python::with_gil entirely.
            // safe_call! also guards against interpreter-finalize panic.
            crate::safe_call!({
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

    /// Warm the NTEnum shape cache with a one-shot pvget.
    ///
    /// CA uses this to consume a background DBR_CTRL prefetch and store
    /// the native type. PVA has no "native type" to cache, but for NTEnum
    /// channels this is the only place the WRITE PV gets its `is_ntenum`
    /// flag populated — without it, `put_async(int)` on a split read/write
    /// signal (e.g. `epicsrs_signal_rw(MyEnum, "pva://X_rbv", "pva://X_cmd")`)
    /// would route through plain pvput and be silently rejected by the IOC.
    #[pyo3(signature = (timeout=2.0))]
    fn cache_native_type_async<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let client = self.client.clone();
        let name = self.pvname.clone();
        let dur = Duration::from_secs_f64(timeout);
        let is_ntenum = self.is_ntenum.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Skip the pvget if a prior read (monitor / get_value_async /
            // get_reading_async) already populated the cache. This avoids
            // a redundant round-trip on reconnect and on non-NTEnum PVs
            // where the overhead buys nothing.
            if is_ntenum.load(Ordering::Relaxed) != NTENUM_UNKNOWN {
                return Ok(true);
            }
            match tokio::time::timeout(dur, client.pvget(&name)).await {
                Ok(Ok(field)) => record_ntenum_into(&is_ntenum, &field),
                Ok(Err(e)) => tracing::warn!(
                    pv = %name,
                    "cache_native_type_async pvget failed — is_ntenum stays unknown, \
                     NTEnum put routing may fall back to plain pvput: {e}"
                ),
                Err(_) => tracing::warn!(
                    pv = %name,
                    "cache_native_type_async pvget timed out after {timeout}s — \
                     is_ntenum stays unknown, NTEnum put routing may fall back to plain pvput"
                ),
            }
            Ok(true)
        })
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
            // PyErr::new_err is GIL-free at construction (the message
            // is realised lazily), so it's safe to use as the
            // safe_call_or! default — even if the GIL acquisition
            // itself panics during a finalize race.
            let panic_err = || {
                Err::<PyObject, PyErr>(PyRuntimeError::new_err(
                    "get_field_desc_async: panic in Python::with_gil",
                ))
            };
            match tokio::time::timeout(dur, client.pvinfo(&name)).await {
                Ok(Ok(desc)) => crate::safe_call_or!(
                    panic_err(),
                    Python::with_gil(|py| Ok::<PyObject, PyErr>(
                        field_desc_to_py(py, &desc).into_any().unbind()
                    ))
                ),
                _ => crate::safe_call_or!(
                    panic_err(),
                    Python::with_gil(|py| Ok::<PyObject, PyErr>(py.None()))
                ),
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
        let is_ntenum = self.is_ntenum.clone();
        // `future_into_py_fast` skips add_done_callback / Cancellable /
        // outer-spawn / scope (~15-25µs/call savings vs the standard
        // `future_into_py`). Safe here because the future is short-
        // lived, the caller does not cancel via asyncio, and we don't
        // use contextvars across the await.
        pyo3_async_runtimes::tokio::future_into_py_fast(py, async move {
            match tokio::time::timeout(dur, client.pvget(&name)).await {
                Ok(Ok(field)) => {
                    let cached = is_ntenum.load(Ordering::Relaxed);
                    let is_nt = if cached != NTENUM_UNKNOWN {
                        cached == NTENUM_TRUE
                    } else {
                        record_ntenum_into(&is_ntenum, &field);
                        is_ntenum.load(Ordering::Relaxed) == NTENUM_TRUE
                    };
                    crate::safe_call_or!(
                        Err(PyRuntimeError::new_err(format!(
                            "pvget on {pvname}: panic in Python::with_gil during value conversion"
                        ))),
                        Python::with_gil(|py| {
                            if is_nt {
                                if let PvField::Structure(s) = &field {
                                    if let Some((idx, _choices)) =
                                        crate::pva_convert::try_extract_ntenum(s)
                                    {
                                        return Ok::<PyObject, PyErr>(
                                            idx.into_pyobject(py).unwrap().into_any().unbind(),
                                        );
                                    }
                                }
                            }
                            let value_field: &PvField = match &field {
                                PvField::Structure(s) => s.get_field("value").unwrap_or(&field),
                                _ => &field,
                            };
                            Ok::<PyObject, PyErr>(pvfield_to_py(py, value_field))
                        })
                    )
                }
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
        let is_ntenum = self.is_ntenum.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(dur, client.pvget(&name)).await {
                Ok(Ok(field)) => {
                    // Feed is_ntenum cache (same reason as get_value_async).
                    if is_ntenum.load(Ordering::Relaxed) == NTENUM_UNKNOWN {
                        record_ntenum_into(&is_ntenum, &field);
                    }
                    crate::safe_call_or!(
                        Err(PyRuntimeError::new_err(format!(
                            "pvget on {pvname}: panic in Python::with_gil during reading conversion"
                        ))),
                        Python::with_gil(|py| Ok::<PyObject, PyErr>(
                            EpicsRsPvaMetadata::new(field).into_pyobject(py).unwrap().into_any().unbind()
                        ))
                    )
                }
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
    ///
    /// NTEnum routing matches the sync `put`: when the channel is
    /// known (from a prior get) to be NTEnum and the Python value is
    /// int / bool, the write is dispatched via
    /// `pvput_field("value.index", ...)` so the IOC actually accepts
    /// it. Without this, every ophyd-async `await sig.set(MyEnum.X)`
    /// against a PVA NTEnum signal would silently no-op.
    #[pyo3(signature = (value, timeout=300.0))]
    fn put_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, pyo3::PyAny>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let route_ntenum_index = self.is_ntenum.load(Ordering::Relaxed) == NTENUM_TRUE && py_value_is_intlike(value);
        let value_str = python_value_to_pvput_string(value)?;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let pvname = name.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = if route_ntenum_index {
                tokio::time::timeout(dur, client.pvput_field(&name, "value.index", &value_str))
                    .await
            } else {
                tokio::time::timeout(dur, client.pvput(&name, &value_str)).await
            };
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "pvput on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "pvput on {pvname} timed out after {timeout}s"
                ))),
            }
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
        // Same NTEnum routing as put / put_async — see put_async for
        // the rationale. ophyd-async's `set(wait=False)` lands here.
        let route_ntenum_index = self.is_ntenum.load(Ordering::Relaxed) == NTENUM_TRUE && py_value_is_intlike(value);
        let value_str = python_value_to_pvput_string(value)?;
        let client = self.client.clone();
        let name = self.pvname.clone();
        let pvname = name.clone();
        let bound = Duration::from_secs(5);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = if route_ntenum_index {
                tokio::time::timeout(bound, client.pvput_field(&name, "value.index", &value_str))
                    .await
            } else {
                tokio::time::timeout(bound, client.pvput(&name, &value_str)).await
            };
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "pvput_nowait on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "pvput_nowait on {pvname} timed out (5s bound)"
                ))),
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
        let pvname = name.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match tokio::time::timeout(dur, client.pvput_pv_field(&name, &field)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "pvput_pv_field on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "pvput_pv_field on {pvname} timed out after {timeout}s"
                ))),
            }
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
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(format!(
                    "pvput_pv_field_nowait on {pvname} failed: {e}"
                ))),
                Err(_) => Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "pvput_pv_field_nowait on {pvname} timed out (5s bound)"
                ))),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!("EpicsRsPvaPV('{}')", self.pvname)
    }
}

impl Drop for EpicsRsPvaPV {
    /// Abort every spawned task and drop monitor channels — same
    /// rationale as `EpicsRsPV::drop` (see pv.rs). Critical because
    /// PVA wrappers can be created freely and the connection_task /
    /// monitor task self-heal loops would otherwise outlive the wrapper
    /// and pin the runtime indefinitely.
    fn drop(&mut self) {
        *self.connection_callback.lock() = None;
        *self.access_callback.lock() = None;
        *self.py_monitor_callback.lock() = None;
        *self.monitor_tx.lock() = None;

        let _ = self.monitor_handle.lock().take();
        if let Some(h) = self.monitor_setup_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.access_setup_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.connection_task.lock().take() {
            h.abort();
        }

        // Decrement the parent context's live-PV counter — see
        // ``EpicsRsPvaContext::pv_count``.
        self.pv_count.fetch_sub(1, Ordering::AcqRel);
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
/// pvxs accepts comma-separated values for arrays. Strings containing
/// commas, quotes, or whitespace are JSON-escaped + quoted so the
/// pvxs parser sees a single element instead of splitting them.
/// Cheap "is this value an integer-shaped scalar?" check used to decide
/// whether `put` should route through `pvput_field("value.index", ...)`
/// for an NTEnum channel. Accepts plain Python int + bool, plus numpy
/// 0-d int / bool scalars. Floats and strings deliberately fall through
/// to plain `pvput` — passing 1.5 to NTEnum would be wrong, and a string
/// label is meant for the choices array, not the index.
fn py_value_is_intlike(value: &Bound<'_, pyo3::PyAny>) -> bool {
    use pyo3::types::{PyBool, PyInt};
    if value.is_instance_of::<PyBool>() || value.is_instance_of::<PyInt>() {
        return true;
    }
    // numpy 0-d int / bool — call .item() and re-check.
    if value.hasattr("dtype").unwrap_or(false)
        && value.hasattr("ndim").unwrap_or(false)
        && value
            .getattr("ndim")
            .and_then(|v| v.extract::<i32>())
            .unwrap_or(0)
            == 0
        && let Ok(native) = value.call_method0("item")
    {
        return native.is_instance_of::<PyBool>() || native.is_instance_of::<PyInt>();
    }
    false
}

fn python_value_to_pvput_string(value: &Bound<'_, pyo3::PyAny>) -> PyResult<String> {
    // numpy scalar → Python scalar
    if value.hasattr("dtype").unwrap_or(false) && value.hasattr("ndim").unwrap_or(false) {
        let ndim: i32 = value.getattr("ndim").and_then(|v| v.extract()).unwrap_or(0);
        if ndim == 0 {
            if let Ok(native) = value.call_method0("item") {
                return python_value_to_pvput_string(&native);
            }
        } else {
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
    // Strict bool check first — `value.is_instance_of::<PyBool>()` is
    // True only for the singleton True/False, not for arbitrary ints.
    // Without this guard, `extract::<bool>()` succeeds for any int and
    // would route int values into the "true"/"false" branch.
    if value.is_instance_of::<pyo3::types::PyBool>() {
        let b: bool = value.extract()?;
        return Ok(if b {
            "true".to_string()
        } else {
            "false".to_string()
        });
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(escape_pvput_string(&s));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(f.to_string());
    }
    let s = value.str()?.to_string();
    Ok(escape_pvput_string(&s))
}

/// Escape a string for the pvxs value parser. Wraps in double quotes
/// and JSON-escapes the contents whenever the raw value would be
/// ambiguous (commas → ScalarArray split, quotes → premature
/// termination, leading whitespace → trim). For "safe" strings we
/// keep the unquoted form to remain compatible with parsers that don't
/// accept quoted scalars.
fn escape_pvput_string(s: &str) -> String {
    let needs_quoting = s.is_empty()
        || s.contains(',')
        || s.contains('"')
        || s.contains('\\')
        || s.contains('\n')
        || s.starts_with(' ')
        || s.ends_with(' ');
    if !needs_quoting {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}
