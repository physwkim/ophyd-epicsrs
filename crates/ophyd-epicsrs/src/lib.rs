pub mod context;
pub mod convert;
pub mod pv;
pub mod pva;
pub mod pva_convert;
pub mod pva_put;
pub mod runtime;
#[macro_use]
pub mod safe_log;

use pyo3::prelude::*;
use std::sync::OnceLock;

/// Captured at module init so `reset_log_cache()` can clear pyo3-log's
/// LoggersAndLevels cache on demand. Without this, runtime changes to
/// Python logger levels are not picked up for ~30 s.
static LOG_RESET: OnceLock<pyo3_log::ResetHandle> = OnceLock::new();

#[pymodule]
#[pyo3(name = "_native")]
fn ophyd_epicsrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Bridge Rust `log` events (which `tracing`'s "log" feature emits
    // when no Subscriber is registered) into Python's logging module.
    // Library users get full control via the standard pattern:
    //
    //     import logging
    //     logging.getLogger("ophyd_epicsrs.ca").setLevel(logging.WARN)
    //     logging.getLogger("ophyd_epicsrs.pva").setLevel(logging.DEBUG)
    //
    // No stderr writer, no Jupyter red-box noise, no double-reporting
    // (errors are already raised as PyRuntimeError to the caller).
    //
    // try_init so a sibling pyo3 crate that already initialised the
    // logger keeps its handler — failure here is benign.
    if let Ok(reset) = pyo3_log::try_init() {
        let _ = LOG_RESET.set(reset);
    }
    m.add_class::<context::EpicsRsContext>()?;
    m.add_class::<pv::EpicsRsPV>()?;
    m.add_class::<pva::EpicsRsPvaContext>()?;
    m.add_class::<pva::EpicsRsPvaPV>()?;
    m.add_function(wrap_pyfunction!(reset_log_cache, m)?)?;
    m.add_function(wrap_pyfunction!(caught_panic_count, m)?)?;
    m.add_function(wrap_pyfunction!(_reset_panic_count_for_test, m)?)?;
    Ok(())
}

/// Clear the pyo3-log level cache. Call after changing Python logger
/// levels at runtime so the next `tracing::*!` from Rust re-checks
/// the Python side instead of using stale cached levels (~30 s TTL).
///
/// Returns True if the reset was applied; False if pyo3-log was not
/// installed (typically because a sibling pyo3 crate beat us to
/// `try_init`). False is informational, not an error.
#[pyfunction]
fn reset_log_cache() -> bool {
    if let Some(handle) = LOG_RESET.get() {
        handle.reset();
        true
    } else {
        false
    }
}

/// Number of panics caught by `safe_warn!` / `safe_call!` since
/// process start. Useful for telemetry — if this is non-zero, the
/// pyo3-log → Python::with_gil bridge has hit interpreter-finalize
/// races (or another panic source). The first such panic also writes
/// a one-line notice to stderr; subsequent ones only increment.
#[pyfunction]
fn caught_panic_count() -> u64 {
    safe_log::caught_panic_count()
}

/// Test-only: zero the panic counter. Lets unit tests check
/// "this operation incremented by N" without coupling to any prior
/// test's running totals. Not for production use (no thread guard,
/// no separation between tests in a parallel runner).
#[pyfunction]
fn _reset_panic_count_for_test() {
    safe_log::reset_panic_count_for_test();
}
