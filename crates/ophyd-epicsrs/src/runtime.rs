//! Process-wide tokio runtime shared between sync and async entry points.
//!
//! The same runtime drives:
//! - sync methods (via spawn + std::sync::mpsc::channel)
//! - async methods (via pyo3_async_runtimes::tokio::future_into_py)
//!
//! Single runtime ensures sync `EpicsRsShimPV` (legacy ophyd path) and
//! async `EpicsRsPV.*_async` (future detector module path) share the
//! same epics-rs channel cache, RTT estimator, search engine, and
//! connection pool — no runtime fragmentation.

use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

static SHARED_RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

/// Get (and lazily create) the process-wide tokio runtime.
///
/// The first call also registers this runtime with pyo3-async-runtimes
/// so that `pyo3_async_runtimes::tokio::future_into_py` can poll Rust
/// futures on it.
pub fn shared_runtime() -> Arc<Runtime> {
    SHARED_RUNTIME
        .get_or_init(|| {
            let rt = Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("ophyd-epicsrs")
                    .build()
                    .expect("failed to build tokio runtime"),
            );

            // Register the same Runtime reference with pyo3-async-runtimes.
            // SAFETY: the Arc<Runtime> is owned by the static OnceLock for
            // the lifetime of the process, so the inner Runtime is never
            // dropped. The &'static reference we hand to pyo3-async-runtimes
            // is therefore genuinely 'static.
            let static_ref: &'static Runtime = unsafe { &*Arc::as_ptr(&rt) };
            pyo3_async_runtimes::tokio::init_with_runtime(static_ref)
                .expect("failed to register tokio runtime with pyo3-async-runtimes");

            rt
        })
        .clone()
}
