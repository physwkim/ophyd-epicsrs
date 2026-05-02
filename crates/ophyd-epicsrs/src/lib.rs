pub mod context;
pub mod convert;
pub mod pv;
pub mod pva;
pub mod pva_convert;
pub mod pva_put;
pub mod runtime;

use pyo3::prelude::*;

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
    let _ = pyo3_log::try_init();
    m.add_class::<context::EpicsRsContext>()?;
    m.add_class::<pv::EpicsRsPV>()?;
    m.add_class::<pva::EpicsRsPvaContext>()?;
    m.add_class::<pva::EpicsRsPvaPV>()?;
    Ok(())
}
