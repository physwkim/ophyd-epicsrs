use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Runtime;

use epics_rs::ca::client::CaClient;

use crate::convert::epics_value_to_py;
use crate::pv::EpicsRsPV;

/// Shared EPICS CA context — holds a tokio Runtime and CaClient.
///
/// The runtime is kept alive for the lifetime of this context.
/// CaClient's background tasks (coordinator, transport, search) run
/// as spawned tasks on this runtime and must stay alive between
/// Python calls.
#[pyclass(name = "EpicsRsContext")]
pub struct EpicsRsContext {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) client: Arc<CaClient>,
}

#[pymethods]
impl EpicsRsContext {
    #[new]
    fn new() -> PyResult<Self> {
        // Use the process-wide shared runtime so sync (this) and async
        // (pyo3-async-runtimes) entry points share one tokio executor.
        let runtime = crate::runtime::shared_runtime();

        // Create CaClient inside a spawned task so background tasks
        // are properly rooted in the runtime's thread pool,
        // not in a block_on context that may interfere with IO polling.
        let client = {
            let (tx, rx) = std::sync::mpsc::channel();
            runtime.spawn(async move {
                let result = CaClient::new().await;
                let _ = tx.send(result);
            });
            rx.recv()
                .map_err(|_| PyRuntimeError::new_err("runtime channel closed"))?
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("failed to create CA client: {e}"))
                })?
        };

        Ok(Self {
            runtime,
            client: Arc::new(client),
        })
    }

    /// Create a PV channel for the given name.
    fn create_pv(&self, pvname: &str) -> EpicsRsPV {
        let channel = self.client.create_channel(pvname);
        EpicsRsPV::new(self.runtime.clone(), channel, pvname.to_string())
    }

    /// Read multiple PVs in parallel. Returns a dict of {pvname: value}.
    ///
    /// All PVs are connected and read concurrently using tokio::join_all.
    /// This is much faster than reading PVs one by one.
    ///
    /// Parameters
    /// ----------
    /// pvnames : list[str]
    ///     List of PV names to read.
    /// timeout : float, optional
    ///     Timeout in seconds (default: 5.0).
    ///
    /// Returns
    /// -------
    /// dict
    ///     {pvname: value} for all successfully read PVs.
    ///
    /// Example
    /// -------
    ///     ctx = EpicsRsContext()
    ///     data = ctx.bulk_caget(["PV:enc_wf", "PV:I0_wf", "PV:ROI1", ...])
    ///     # All PVs read in parallel — ~1 round-trip time instead of N
    #[pyo3(signature = (pvnames, timeout=5.0))]
    fn bulk_caget(
        &self,
        py: Python<'_>,
        pvnames: Vec<String>,
        timeout: f64,
    ) -> PyResult<PyObject> {
        let client = self.client.clone();
        let dur = Duration::from_secs_f64(timeout);

        // Spawn all reads in parallel, collect results
        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            let mut handles = Vec::with_capacity(pvnames.len());
            for name in &pvnames {
                let ch = client.create_channel(name);
                let pvname = name.clone();
                handles.push(tokio::spawn(async move {
                    if ch.wait_connected(dur).await.is_err() {
                        return (pvname, None);
                    }
                    match ch.get().await {
                        Ok((_dbr, val)) => (pvname, Some(val)),
                        Err(_) => (pvname, None),
                    }
                }));
            }

            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                if let Ok(result) = handle.await {
                    results.push(result);
                }
            }
            let _ = tx.send(results);
        });

        // Wait for all results (GIL released)
        let rx = parking_lot::Mutex::new(rx);
        let results = py.allow_threads(|| {
            rx.lock()
                .recv()
                .map_err(|_| PyRuntimeError::new_err("bulk_caget failed"))
        })?;

        // Convert to Python dict — failed PVs are included as None
        // so callers can distinguish "missing key" from "read failed".
        let dict = PyDict::new(py);
        for (pvname, maybe_val) in results {
            match maybe_val {
                Some(val) => dict.set_item(&pvname, epics_value_to_py(py, &val))?,
                None => dict.set_item(&pvname, py.None())?,
            }
        }
        Ok(dict.into_any().unbind())
    }

    /// Connect multiple PVs in parallel and collect available metadata.
    ///
    /// Priority: connection first, metadata second.
    /// 1. Wait for all channels to be connected (fast, all in parallel)
    /// 2. For connected channels, grab prefetch result if already done
    /// 3. If prefetch not ready, return connection-only result (metadata
    ///    fetched lazily by ophyd when needed)
    ///
    /// Returns a dict: {pvname: {metadata...} or True (connected, no metadata)
    ///                  or None (not connected)}.
    #[pyo3(signature = (pvs, timeout=5.0))]
    fn bulk_connect_and_prefetch(
        &self,
        py: Python<'_>,
        pvs: Vec<Py<EpicsRsPV>>,
        timeout: f64,
    ) -> PyResult<PyObject> {
        use crate::pv::PrefetchResult;
        use epics_rs::ca::client::CaChannel;

        // Extract channels + prefetch handles while holding GIL
        type PvTask = (
            String,
            Arc<CaChannel>,
            Option<tokio::task::JoinHandle<Option<PrefetchResult>>>,
        );
        let tasks: Vec<PvTask> = pvs.iter()
            .map(|pv| {
                let pv_ref = pv.borrow(py);
                (
                    pv_ref.pvname.clone(),
                    pv_ref.channel.clone(),
                    pv_ref.prefetch_handle.lock().take(),
                )
            })
            .collect();
        drop(pvs);

        let dur = Duration::from_secs_f64(timeout);

        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            // Phase 1: wait_connected for ALL channels in parallel (fast)
            type ConnectHandle = (
                String,
                Arc<CaChannel>,
                Option<tokio::task::JoinHandle<Option<PrefetchResult>>>,
                tokio::task::JoinHandle<bool>,
            );
            let mut connect_handles: Vec<ConnectHandle> =
                Vec::with_capacity(tasks.len());
            for (pvname, ch, prefetch) in tasks {
                let ch_clone = ch.clone();
                let handle = tokio::spawn(async move {
                    ch_clone.wait_connected(dur).await.is_ok()
                });
                connect_handles.push((pvname, ch, prefetch, handle));
            }

            // Phase 2: collect results — grab prefetch metadata if ready
            let mut results = Vec::with_capacity(connect_handles.len());
            for (pvname, ch, prefetch, conn_handle) in connect_handles {
                let connected = conn_handle.await.unwrap_or(false);
                if !connected {
                    results.push((pvname, None));
                    continue;
                }
                // Try to get prefetch result (non-blocking or very short wait)
                let md = if let Some(handle) = prefetch {
                    // Give prefetch 100ms to finish — it's been running since
                    // PV creation, so it's likely done or nearly done.
                    match tokio::time::timeout(Duration::from_millis(100), handle).await {
                        Ok(Ok(result)) => result,
                        _ => None,
                    }
                } else {
                    None
                };
                results.push((pvname, Some((ch, md))));
            }
            let _ = tx.send(results);
        });

        // Wait for all results (GIL released)
        let rx = parking_lot::Mutex::new(rx);
        let results = py.allow_threads(|| {
            rx.lock()
                .recv()
                .map_err(|_| PyRuntimeError::new_err("bulk_connect_and_prefetch failed"))
        })?;

        let dict = PyDict::new(py);
        for (pvname, maybe_result) in results {
            match maybe_result {
                Some((_ch, Some(result))) => {
                    // Connected + metadata available
                    let md = crate::convert::snapshot_to_pydict(py, &result.snapshot);
                    let md_ref = md.downcast_bound::<PyDict>(py).unwrap();
                    let _ = md_ref.set_item("ftype", result.native_type as u16);
                    let _ = md_ref.set_item("type", &result.type_name);
                    let _ = md_ref.set_item("count", result.element_count);
                    let _ = md_ref.set_item("host", &result.host);
                    let _ = md_ref.set_item("read_access", result.read_access);
                    let _ = md_ref.set_item("write_access", result.write_access);
                    dict.set_item(&pvname, md)?;
                }
                Some((_ch, None)) => {
                    // Connected but no metadata yet — return True so Python
                    // knows it's connected and can fire the callback
                    dict.set_item(&pvname, true)?;
                }
                None => {
                    // Not connected
                    dict.set_item(&pvname, py.None())?;
                }
            }
        }
        Ok(dict.into_any().unbind())
    }

    fn __repr__(&self) -> String {
        "EpicsRsContext(active)".to_string()
    }
}
