use std::time::{SystemTime, UNIX_EPOCH};

use epics_rs::base::server::snapshot::Snapshot;
use epics_rs::base::types::EpicsValue;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

/// Convert an EpicsValue to a Python object.
pub fn epics_value_to_py(py: Python<'_>, val: &EpicsValue) -> PyObject {
    match val {
        EpicsValue::Double(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Float(v) => (*v as f64).into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Long(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Short(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Char(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Enum(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::String(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::DoubleArray(v) => {
            PyList::new(py, v.iter()).unwrap().into_any().unbind()
        }
        EpicsValue::FloatArray(v) => {
            PyList::new(py, v.iter().map(|x| *x as f64)).unwrap().into_any().unbind()
        }
        EpicsValue::LongArray(v) => {
            PyList::new(py, v.iter()).unwrap().into_any().unbind()
        }
        EpicsValue::ShortArray(v) => {
            PyList::new(py, v.iter()).unwrap().into_any().unbind()
        }
        EpicsValue::CharArray(v) => {
            PyList::new(py, v.iter()).unwrap().into_any().unbind()
        }
        EpicsValue::EnumArray(v) => {
            PyList::new(py, v.iter()).unwrap().into_any().unbind()
        }
    }
}

/// Convert a Python value to an EpicsValue, given the native DbFieldType.
pub fn py_to_epics_value(
    obj: &Bound<'_, pyo3::PyAny>,
    native_type: epics_rs::base::types::DbFieldType,
) -> PyResult<EpicsValue> {
    use epics_rs::base::types::DbFieldType;
    match native_type {
        DbFieldType::Double => {
            let v: f64 = obj.extract()?;
            Ok(EpicsValue::Double(v))
        }
        DbFieldType::Float => {
            let v: f32 = obj.extract()?;
            Ok(EpicsValue::Float(v))
        }
        DbFieldType::Long => {
            let v: i32 = obj.extract()?;
            Ok(EpicsValue::Long(v))
        }
        DbFieldType::Short => {
            let v: i16 = obj.extract()?;
            Ok(EpicsValue::Short(v))
        }
        DbFieldType::Char => {
            let v: u8 = obj.extract()?;
            Ok(EpicsValue::Char(v))
        }
        DbFieldType::Enum => {
            // Try integer first, then parse string as integer.
            // Named enum strings (e.g. "Enable") are resolved in the Python shim
            // using cached enum_strs before reaching here.
            if let Ok(v) = obj.extract::<u16>() {
                Ok(EpicsValue::Enum(v))
            } else if let Ok(s) = obj.extract::<String>() {
                s.parse::<u16>()
                    .map(EpicsValue::Enum)
                    .map_err(|_| pyo3::exceptions::PyTypeError::new_err(
                        format!("cannot convert '{}' to enum index", s),
                    ))
            } else {
                Err(pyo3::exceptions::PyTypeError::new_err(
                    "enum value must be an integer or string",
                ))
            }
        }
        DbFieldType::String => {
            let v: String = obj.extract()?;
            Ok(EpicsValue::String(v))
        }
    }
}

fn system_time_to_epoch(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

/// Convert a Snapshot to a Python dict with ophyd-compatible metadata keys.
///
/// Keys: value, char_value, status, severity, timestamp, precision, units,
///       lower_ctrl_limit, upper_ctrl_limit, enum_strs
pub fn snapshot_to_pydict(py: Python<'_>, snapshot: &Snapshot) -> PyObject {
    let dict = PyDict::new(py);
    dict.set_item("value", epics_value_to_py(py, &snapshot.value)).unwrap();
    dict.set_item("status", snapshot.alarm.status).unwrap();
    dict.set_item("severity", snapshot.alarm.severity).unwrap();
    dict.set_item("timestamp", system_time_to_epoch(snapshot.timestamp)).unwrap();

    // char_value: string representation matching pyepics behavior.
    // For enums, resolve to label via enum_strs; for others, format the value.
    let char_value = match &snapshot.value {
        EpicsValue::Enum(idx) => {
            if let Some(ref ei) = snapshot.enums {
                ei.strings.get(*idx as usize).cloned().unwrap_or_else(|| idx.to_string())
            } else {
                idx.to_string()
            }
        }
        other => format!("{other}"),
    };
    dict.set_item("char_value", char_value).unwrap();

    if let Some(ref disp) = snapshot.display {
        dict.set_item("precision", disp.precision).unwrap();
        dict.set_item("units", &disp.units).unwrap();
        dict.set_item("upper_disp_limit", disp.upper_disp_limit).unwrap();
        dict.set_item("lower_disp_limit", disp.lower_disp_limit).unwrap();
        dict.set_item("upper_alarm_limit", disp.upper_alarm_limit).unwrap();
        dict.set_item("lower_alarm_limit", disp.lower_alarm_limit).unwrap();
        dict.set_item("upper_warning_limit", disp.upper_warning_limit).unwrap();
        dict.set_item("lower_warning_limit", disp.lower_warning_limit).unwrap();
    }

    if let Some(ref ctrl) = snapshot.control {
        dict.set_item("upper_ctrl_limit", ctrl.upper_ctrl_limit).unwrap();
        dict.set_item("lower_ctrl_limit", ctrl.lower_ctrl_limit).unwrap();
    }

    if let Some(ref enums) = snapshot.enums {
        let tuple = PyTuple::new(py, enums.strings.iter()).unwrap();
        dict.set_item("enum_strs", tuple).unwrap();
    }

    dict.into_any().unbind()
}
