pub mod context;
pub mod convert;
pub mod pv;
pub mod pva;
pub mod pva_convert;
pub mod pva_put;
pub mod runtime;

use pyo3::prelude::*;
use tracing_subscriber::EnvFilter;

#[pymodule]
#[pyo3(name = "_native")]
fn ophyd_epicsrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_tracing();
    m.add_class::<context::EpicsRsContext>()?;
    m.add_class::<pv::EpicsRsPV>()?;
    m.add_class::<pva::EpicsRsPvaContext>()?;
    m.add_class::<pva::EpicsRsPvaPV>()?;
    Ok(())
}

/// Register a process-wide tracing subscriber so library-emitted
/// `tracing::warn!` / `tracing::debug!` messages actually surface.
///
/// Without this the global default subscriber is a no-op and every
/// log line is silently dropped — a regression versus the earlier
/// `eprintln!` calls. Honors the `RUST_LOG` env var (e.g.
/// `RUST_LOG=ophyd_epicsrs=debug`); defaults to `warn` otherwise.
///
/// Uses `try_init` so a downstream user that has already configured
/// their own subscriber (or that imports a sibling pyo3 crate that
/// did) is not clobbered — the failure is benign.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}
