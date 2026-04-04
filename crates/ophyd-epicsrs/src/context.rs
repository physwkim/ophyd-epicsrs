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
        // Build a multi-threaded runtime that stays alive.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("failed to create tokio runtime: {e}"))
            })?;

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
            runtime: Arc::new(runtime),
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

    /// Connect and fetch initial metadata for multiple PVs in parallel.
    ///
    /// Phase 1: all PVs wait_connected concurrently.
    /// Phase 2: connected PVs fetch channel_info + DBR_TIME concurrently.
    ///
    /// Returns a dict: {pvname: {metadata...} or None for failed PVs}.
    /// One GIL release for ALL PVs instead of one per PV.
    #[pyo3(signature = (pvnames, timeout=5.0))]
    fn bulk_connect_and_prefetch(
        &self,
        py: Python<'_>,
        pvnames: Vec<String>,
        timeout: f64,
    ) -> PyResult<PyObject> {
        let client = self.client.clone();
        let dur = Duration::from_secs_f64(timeout);

        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            // Phase 1: connect all PVs in parallel
            let mut channels: Vec<(String, epics_rs::ca::client::CaChannel)> = Vec::with_capacity(pvnames.len());
            let mut connect_handles = Vec::with_capacity(pvnames.len());
            for name in &pvnames {
                let ch = client.create_channel(name);
                let pvname = name.clone();
                let ch_clone = ch.clone();
                connect_handles.push((pvname, ch, tokio::spawn(async move {
                    ch_clone.wait_connected(dur).await.is_ok()
                })));
            }
            for (pvname, ch, handle) in connect_handles {
                let connected = handle.await.unwrap_or(false);
                if connected {
                    channels.push((pvname, ch));
                }
            }

            // Phase 2: fetch info + DBR_TIME for connected PVs in parallel
            let mut fetch_handles = Vec::with_capacity(channels.len());
            for (pvname, ch) in channels {
                fetch_handles.push(tokio::spawn(async move {
                    let info = match ch.info().await {
                        Ok(i) => i,
                        Err(_) => return (pvname, None),
                    };
                    let snapshot = match tokio::time::timeout(
                        dur, ch.get_with_metadata(epics_rs::base::server::snapshot::DbrClass::Time)
                    ).await {
                        Ok(Ok(s)) => s,
                        _ => return (pvname, None),
                    };
                    (pvname, Some((info, snapshot)))
                }));
            }

            let mut results = Vec::with_capacity(pvnames.len());
            for handle in fetch_handles {
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
                .map_err(|_| PyRuntimeError::new_err("bulk_connect_and_prefetch failed"))
        })?;

        let dict = PyDict::new(py);
        for (pvname, maybe_result) in results {
            match maybe_result {
                Some((info, snapshot)) => {
                    let md = crate::convert::snapshot_to_pydict(py, &snapshot);
                    let md_ref = md.downcast_bound::<PyDict>(py).unwrap();
                    let _ = md_ref.set_item("ftype", info.native_type as u16);
                    let _ = md_ref.set_item("type", format!("{:?}", info.native_type).to_lowercase());
                    let _ = md_ref.set_item("count", info.element_count);
                    let _ = md_ref.set_item("host", info.server_addr.to_string());
                    let _ = md_ref.set_item("read_access", info.access_rights.read);
                    let _ = md_ref.set_item("write_access", info.access_rights.write);
                    dict.set_item(&pvname, md)?;
                }
                None => {
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
