use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use epics_base_rs::client::CaClient;

use crate::pv::EpicsRsPV;

/// Shared EPICS CA context — holds a tokio Runtime and CaClient.
/// All PVs created from this context share the same runtime and client.
#[pyclass(name = "EpicsRsContext")]
pub struct EpicsRsContext {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) client: Arc<CaClient>,
}

#[pymethods]
impl EpicsRsContext {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create tokio runtime: {e}")))?;
        let client = runtime
            .block_on(async { CaClient::new().await })
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create CA client: {e}")))?;
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

    fn __repr__(&self) -> String {
        "EpicsRsContext(active)".to_string()
    }
}
