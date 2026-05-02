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
    m.add_class::<context::EpicsRsContext>()?;
    m.add_class::<pv::EpicsRsPV>()?;
    m.add_class::<pva::EpicsRsPvaContext>()?;
    m.add_class::<pva::EpicsRsPvaPV>()?;
    Ok(())
}
