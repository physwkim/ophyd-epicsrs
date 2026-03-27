pub mod context;
pub mod convert;
pub mod pv;

use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "_native")]
fn ophyd_epicsrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<context::EpicsRsContext>()?;
    m.add_class::<pv::EpicsRsPV>()?;
    Ok(())
}
