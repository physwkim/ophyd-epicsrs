use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Runtime;

use epics_rs::base::types::EpicsValue;
use epics_rs::ca::CaError;
use epics_rs::ca::client::{CaChannel, CaClient};

use crate::convert::epics_value_to_py;
use crate::pv::EpicsRsPV;

const BULK_GET_CACHE_LIMIT: usize = 4096;

#[derive(Default)]
struct BulkGetCache {
    channels: HashMap<String, CaChannel>,
    order: VecDeque<String>,
}

impl BulkGetCache {
    fn get_or_create(&mut self, client: &CaClient, pvname: &str) -> CaChannel {
        if let Some(channel) = self.channels.get(pvname) {
            return channel.clone();
        }

        let channel = client.create_channel(pvname);
        self.channels.insert(pvname.to_string(), channel.clone());
        self.order.push_back(pvname.to_string());

        while self.channels.len() > BULK_GET_CACHE_LIMIT {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.channels.remove(&oldest);
        }

        channel
    }
}

/// Run a batched CA read with a one-shot retry for channels that came
/// back disconnected/missing on the first pass. Shared between the
/// sync and async ``bulk_get`` paths so they have identical semantics.
async fn run_bulk_get(
    client: Arc<CaClient>,
    snapshots: Vec<(String, CaChannel)>,
    dur: Duration,
) -> Vec<(String, Option<EpicsValue>)> {
    let (names, channels): (Vec<String>, Vec<_>) = snapshots.into_iter().unzip();
    let mut raw = client.get_many_with_timeout(&channels, dur).await;

    let retry_indices: Vec<usize> = raw
        .iter()
        .enumerate()
        .filter_map(|(idx, result)| {
            if matches!(
                result,
                Err(CaError::Disconnected) | Err(CaError::ChannelNotFound(_))
            ) {
                Some(idx)
            } else {
                None
            }
        })
        .collect();

    if !retry_indices.is_empty() {
        let mut wait_handles = Vec::with_capacity(retry_indices.len());
        for idx in retry_indices {
            let channel = channels[idx].clone();
            wait_handles.push(tokio::spawn(async move {
                (idx, channel.wait_connected(dur).await.is_ok())
            }));
        }

        let mut ready_indices = Vec::new();
        let mut ready_channels = Vec::new();
        for handle in wait_handles {
            if let Ok((idx, true)) = handle.await {
                ready_indices.push(idx);
                ready_channels.push(channels[idx].clone());
            }
        }

        if !ready_channels.is_empty() {
            let retried = client.get_many_with_timeout(&ready_channels, dur).await;
            for (idx, result) in ready_indices.into_iter().zip(retried) {
                raw[idx] = result;
            }
        }
    }

    names
        .into_iter()
        .zip(raw.into_iter())
        .map(|(name, res)| (name, res.ok().map(|(_dbr, val)| val)))
        .collect()
}

/// Shared EPICS CA context — holds a tokio Runtime and CaClient.
///
/// Do NOT construct this directly. Use ``ophyd_epicsrs.get_ca_context()``
/// — multiple ``CaClient`` instances per process trip spurious
/// ``first_sighting`` beacon anomalies (epics-ca-rs/beacon_monitor.rs)
/// that drop healthy TCP circuits under load.
///
/// The runtime is kept alive for the lifetime of this context.
/// CaClient's background tasks (coordinator, transport, search) run
/// as spawned tasks on this runtime and must stay alive between
/// Python calls.
#[pyclass(name = "EpicsRsContext")]
pub struct EpicsRsContext {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) client: Arc<CaClient>,
    /// Hidden channel cache for by-name ``bulk_get``.
    ///
    /// This is intentionally not a Python PV-object cache: ophyd still
    /// gets a fresh ``EpicsRsPV`` wrapper from ``create_pv``. The cache
    /// only lets repeated one-shot by-name reads skip CA create/search
    /// and reuse the same hot ``get_many_with_timeout`` path on every
    /// call.
    bulk_get_cache: Mutex<BulkGetCache>,
    /// Live ``EpicsRsPV`` wrapper count. Incremented in ``create_pv``
    /// and decremented in ``EpicsRsPV::drop``. Surfaces through
    /// ``is_unused()`` so ``ophyd_epicsrs.shutdown_all()`` can refuse
    /// to drop the singleton while channels are still alive — without
    /// it, the next ``get_ca_context()`` would silently spawn a second
    /// ``CaClient`` and re-trigger the multi-client beacon-anomaly bug
    /// (the singleton fix in 001c605 was meant to prevent).
    pv_count: Arc<AtomicUsize>,
}

#[pymethods]
impl EpicsRsContext {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        // Use the process-wide shared runtime so sync (this) and async
        // (pyo3-async-runtimes) entry points share one tokio executor.
        let runtime = crate::runtime::shared_runtime();

        // Create CaClient inside a spawned task so background tasks
        // are properly rooted in the runtime's thread pool, not in a
        // block_on context that may interfere with IO polling.
        // Release the GIL while waiting on the spawned task — repeater
        // registration and UDP setup can take milliseconds, and other
        // Python threads must not be blocked on context construction.
        let runtime_for_spawn = runtime.clone();
        let client = py.allow_threads(move || -> PyResult<CaClient> {
            let (tx, rx) = std::sync::mpsc::channel();
            runtime_for_spawn.spawn(async move {
                let result = CaClient::new().await;
                let _ = tx.send(result);
            });
            rx.recv()
                .map_err(|_| PyRuntimeError::new_err("runtime channel closed"))?
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create CA client: {e}")))
        })?;

        Ok(Self {
            runtime,
            client: Arc::new(client),
            bulk_get_cache: Mutex::new(BulkGetCache::default()),
            pv_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// True iff no ``EpicsRsPV`` wrappers created from this context
    /// are currently alive. Used by ``shutdown_all`` to refuse
    /// dropping the singleton while channels are still in use.
    fn is_unused(&self) -> bool {
        self.pv_count.load(Ordering::Acquire) == 0
    }

    /// Create a PV channel for the given name.
    fn create_pv(&self, pvname: &str) -> EpicsRsPV {
        let channel = self.client.create_channel(pvname);
        self.pv_count.fetch_add(1, Ordering::AcqRel);
        EpicsRsPV::new(
            self.runtime.clone(),
            channel,
            pvname.to_string(),
            self.pv_count.clone(),
        )
    }

    /// Read multiple PVs in parallel. Returns a dict of {pvname: value}.
    ///
    /// Reuses a bounded by-name channel cache, then routes hot channels
    /// through ``CaClient::get_many_with_timeout``. That groups the
    /// per-server READ_NOTIFY frames into a single TCP write per server
    /// (libca-style bulk flush). New or disconnected cached channels
    /// are connected and retried once.
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
    ///     {pvname: value} — successfully-read PVs map to the value,
    ///     failures (timeout / disconnect / type error) map to ``None``.
    #[pyo3(signature = (pvnames, timeout=5.0))]
    fn bulk_get(&self, py: Python<'_>, pvnames: Vec<String>, timeout: f64) -> PyResult<PyObject> {
        let client = self.client.clone();
        let dur = Duration::from_secs_f64(timeout);

        let snapshots: Vec<(String, CaChannel)> = {
            let mut cache = self.bulk_get_cache.lock();
            pvnames
                .into_iter()
                .map(|pvname| {
                    let channel = cache.get_or_create(&client, &pvname);
                    (pvname, channel)
                })
                .collect()
        };

        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            let results = run_bulk_get(client, snapshots, dur).await;
            let _ = tx.send(results);
        });

        // Wait for all results (GIL released). `py.allow_threads`'s
        // closure must be `Send`, and `&Receiver` is not. Move `rx` in
        // so the closure owns it — no Mutex needed.
        let results = py.allow_threads(move || {
            rx.recv()
                .map_err(|_| PyRuntimeError::new_err("bulk_get failed"))
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

    /// Async variant of ``bulk_get`` — returns a Python awaitable.
    ///
    /// Drop-in for asyncio / ophyd-async callers: keeps the RunEngine
    /// loop free to schedule other work while the batched READ_NOTIFY
    /// round-trip is in flight. The channel-cache snapshot happens
    /// synchronously under the GIL before the awaitable is returned,
    /// so the awaiter never blocks on cache contention.
    #[pyo3(signature = (pvnames, timeout=5.0))]
    fn bulk_get_async<'py>(
        &self,
        py: Python<'py>,
        pvnames: Vec<String>,
        timeout: f64,
    ) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let client = self.client.clone();
        let dur = Duration::from_secs_f64(timeout);

        let snapshots: Vec<(String, CaChannel)> = {
            let mut cache = self.bulk_get_cache.lock();
            pvnames
                .into_iter()
                .map(|pvname| {
                    let channel = cache.get_or_create(&client, &pvname);
                    (pvname, channel)
                })
                .collect()
        };

        // `future_into_py_fast` skips add_done_callback / Cancellable /
        // outer-spawn / scope (~15-25µs/call savings vs the standard
        // `future_into_py`). Same trade-offs as the per-PV
        // ``get_value_async`` path: short-lived future, no asyncio
        // cancellation, no contextvar propagation across the await.
        pyo3_async_runtimes::tokio::future_into_py_fast(py, async move {
            let results = run_bulk_get(client, snapshots, dur).await;
            Python::with_gil(|py| {
                let dict = PyDict::new(py);
                for (pvname, maybe_val) in results {
                    match maybe_val {
                        Some(val) => dict.set_item(&pvname, epics_value_to_py(py, &val))?,
                        None => dict.set_item(&pvname, py.None())?,
                    }
                }
                Ok::<PyObject, PyErr>(dict.into_any().unbind())
            })
        })
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
        let tasks: Vec<PvTask> = pvs
            .iter()
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
            let mut connect_handles: Vec<ConnectHandle> = Vec::with_capacity(tasks.len());
            for (pvname, ch, prefetch) in tasks {
                let ch_clone = ch.clone();
                let handle =
                    tokio::spawn(async move { ch_clone.wait_connected(dur).await.is_ok() });
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

        // Wait for all results (GIL released) — same `move` pattern as
        // bulk_get; closure owns rx, no Mutex needed.
        let results = py.allow_threads(move || {
            rx.recv()
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
