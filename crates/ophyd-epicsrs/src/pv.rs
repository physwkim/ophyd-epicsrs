use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use epics_base_rs::client::CaChannel;
use epics_base_rs::server::snapshot::DbrClass;
use epics_base_rs::types::DbFieldType;

use crate::convert::{epics_value_to_py, py_to_epics_value, snapshot_to_pydict};

/// Rust-backed PV object for ophyd's control layer.
///
/// Wraps an epics-rs CaChannel and provides sync methods that release the GIL.
#[pyclass(name = "EpicsRsPV")]
pub struct EpicsRsPV {
    runtime: Arc<Runtime>,
    channel: CaChannel,
    #[pyo3(get)]
    pvname: String,
    native_type: Mutex<Option<DbFieldType>>,
    monitor_task: Mutex<Option<JoinHandle<()>>>,
    py_monitor_callback: Arc<Mutex<Option<PyObject>>>,
    connection_callback: Arc<Mutex<Option<PyObject>>>,
    connection_task: Mutex<Option<JoinHandle<()>>>,
}

impl EpicsRsPV {
    pub fn new(runtime: Arc<Runtime>, channel: CaChannel, pvname: String) -> Self {
        Self {
            runtime,
            channel,
            pvname,
            native_type: Mutex::new(None),
            monitor_task: Mutex::new(None),
            py_monitor_callback: Arc::new(Mutex::new(None)),
            connection_callback: Arc::new(Mutex::new(None)),
            connection_task: Mutex::new(None),
        }
    }
}

#[pymethods]
impl EpicsRsPV {
    /// Block until the PV is connected, releasing the GIL while waiting.
    /// Returns True if connected, False if timed out.
    fn wait_for_connection(&self, py: Python<'_>, timeout: f64) -> bool {
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        py.allow_threads(|| {
            self.runtime.block_on(async {
                channel.wait_connected(dur).await.is_ok()
            })
        })
    }

    /// Get PV value with full metadata (timestamp, alarm, units, limits).
    /// Uses DBR_CTRL to get all metadata. Releases GIL during CA read.
    /// Returns a dict with ophyd-compatible keys, or None on timeout.
    #[pyo3(signature = (timeout=2.0, form="time"))]
    fn get_with_metadata(
        &self,
        py: Python<'_>,
        timeout: f64,
        form: &str,
    ) -> PyResult<Option<PyObject>> {
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);
        let class = match form {
            "ctrl" | "control" => DbrClass::Ctrl,
            _ => DbrClass::Time,
        };

        let result = py.allow_threads(|| {
            self.runtime.block_on(async {
                tokio::time::timeout(dur, channel.get_with_metadata(class)).await
            })
        });

        match result {
            Ok(Ok(snapshot)) => {
                // Cache the native type for put operations
                *self.native_type.lock() = Some(snapshot.value.dbr_type());
                Ok(Some(snapshot_to_pydict(py, &snapshot)))
            }
            Ok(Err(_)) | Err(_) => Ok(None),
        }
    }

    /// Get time metadata only (timestamp + alarm status/severity).
    #[pyo3(signature = (timeout=1.0))]
    fn get_timevars(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<PyObject>> {
        self.get_with_metadata(py, timeout, "time")
    }

    /// Get control metadata (units, limits, precision, enum_strs).
    #[pyo3(signature = (timeout=1.0))]
    fn get_ctrlvars(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<PyObject>> {
        self.get_with_metadata(py, timeout, "ctrl")
    }

    /// Write a value to the PV. Releases GIL during CA write.
    #[pyo3(signature = (value, wait=false, timeout=300.0, callback=None))]
    fn put(
        &self,
        py: Python<'_>,
        value: &Bound<'_, pyo3::PyAny>,
        wait: bool,
        timeout: f64,
        callback: Option<PyObject>,
    ) -> PyResult<()> {
        // Determine the native type (cached from previous get, or query now)
        let native = {
            let cached = self.native_type.lock();
            match *cached {
                Some(t) => t,
                None => {
                    // Need to discover the type first
                    drop(cached);
                    let channel = self.channel.clone();
                    let dur = Duration::from_secs_f64(timeout.min(5.0));
                    let snap = py.allow_threads(|| {
                        self.runtime.block_on(async {
                            tokio::time::timeout(dur, channel.get_with_metadata(DbrClass::Plain))
                                .await
                        })
                    });
                    match snap {
                        Ok(Ok(s)) => {
                            let t = s.value.dbr_type();
                            *self.native_type.lock() = Some(t);
                            t
                        }
                        _ => {
                            return Err(PyRuntimeError::new_err(
                                "cannot determine PV native type for put",
                            ));
                        }
                    }
                }
            }
        };

        let epics_val = py_to_epics_value(value, native)?;
        let channel = self.channel.clone();
        let dur = Duration::from_secs_f64(timeout);

        let result = py.allow_threads(|| {
            self.runtime.block_on(async {
                if wait {
                    tokio::time::timeout(dur, channel.put(&epics_val)).await
                } else {
                    // Fire and forget — still send, just don't wait for completion
                    tokio::time::timeout(dur, channel.put(&epics_val)).await
                }
            })
        });

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(PyRuntimeError::new_err(format!("put failed: {e}"))),
            Err(_) => return Err(PyRuntimeError::new_err("put timed out")),
        }

        if let Some(cb) = callback {
            let pvname = self.pvname.clone();
            cb.call1(py, (pvname,))?;
        }

        Ok(())
    }

    /// Register a monitor callback. The callback is called from a background
    /// tokio task whenever the PV value changes.
    fn add_monitor_callback(&self, py: Python<'_>, callback: PyObject) {
        // Store the callback
        *self.py_monitor_callback.lock() = Some(callback.clone_ref(py));

        // If already monitoring, don't start another task
        if self.monitor_task.lock().is_some() {
            return;
        }

        let channel = self.channel.clone();
        let cb_ref = self.py_monitor_callback.clone();
        let pvname = self.pvname.clone();

        let handle = self.runtime.spawn(async move {
            let monitor = match channel.subscribe().await {
                Ok(m) => m,
                Err(_) => return,
            };

            let mut monitor = monitor;
            while let Some(result) = monitor.recv().await {
                if let Ok(value) = result {
                    let should_break = Python::with_gil(|py| {
                        let guard = cb_ref.lock();
                        let callback = match &*guard {
                            Some(cb) => cb.clone_ref(py),
                            None => return true,
                        };
                        drop(guard);

                        let kwargs = pyo3::types::PyDict::new(py);
                        kwargs.set_item("pvname", &pvname).unwrap();
                        kwargs
                            .set_item("value", epics_value_to_py(py, &value))
                            .unwrap();
                        kwargs
                            .set_item(
                                "timestamp",
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs_f64(),
                            )
                            .unwrap();
                        let _ = callback.call(py, (), Some(&kwargs));
                        false
                    });
                    if should_break {
                        break;
                    }
                }
            }
        });

        *self.monitor_task.lock() = Some(handle);
    }

    /// Set a connection callback. Called when connection state changes.
    fn set_connection_callback(&self, py: Python<'_>, callback: PyObject) {
        *self.connection_callback.lock() = Some(callback.clone_ref(py));

        // If already watching, don't start another task
        if self.connection_task.lock().is_some() {
            return;
        }

        let mut rx = self.channel.connection_events();
        let cb_ref = self.connection_callback.clone();
        let pvname = self.pvname.clone();

        let handle = self.runtime.spawn(async move {
            while let Ok(event) = rx.recv().await {
                let connected = matches!(
                    event,
                    epics_base_rs::client::ConnectionEvent::Connected
                );
                Python::with_gil(|py| {
                    let guard = cb_ref.lock();
                    if let Some(cb) = &*guard {
                        let callback = cb.clone_ref(py);
                        drop(guard);
                        let _ = callback.call1(py, (connected,));
                    }
                });
                // Log for debugging
                tracing::debug!("{pvname}: connection event: {event:?}");
            }
        });

        *self.connection_task.lock() = Some(handle);
    }

    /// Remove all monitor callbacks and stop the monitor task.
    fn clear_monitors(&self) {
        *self.py_monitor_callback.lock() = None;
        if let Some(handle) = self.monitor_task.lock().take() {
            handle.abort();
        }
    }

    /// Disconnect the PV (stop monitoring and clear state).
    fn disconnect(&self) {
        self.clear_monitors();
        *self.connection_callback.lock() = None;
        if let Some(handle) = self.connection_task.lock().take() {
            handle.abort();
        }
    }

    fn __repr__(&self) -> String {
        format!("EpicsRsPV('{}')", self.pvname)
    }
}
