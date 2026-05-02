//! PVA value conversion: PvField/ScalarValue ↔ Python.
//!
//! v1 scope: NTScalar / NTScalarArray / NTEnum. NTNDArray is out of scope —
//! image data wrapping requires a separate codec layer.
//!
//! NTScalar shape:
//!   structure NTScalar {
//!       value: <scalar>
//!       alarm: structure { severity: int, status: int, message: string }
//!       timeStamp: structure { secondsPastEpoch: long, nanoseconds: int, userTag: int }
//!       display?: structure { limitLow, limitHigh, description, units, precision }
//!       control?: structure { limitLow, limitHigh, minStep }
//!   }

use epics_rs::pva::pvdata::{PvField, PvStructure, ScalarValue, TypedScalarArray};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

/// Convert a ScalarValue to a Python object.
pub fn scalar_to_py(py: Python<'_>, val: &ScalarValue) -> PyObject {
    match val {
        // PyBool returns a Borrowed (singleton); convert to owned Bound first.
        ScalarValue::Boolean(v) => v.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        ScalarValue::Byte(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::Short(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::Int(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::Long(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::UByte(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::UShort(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::UInt(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::ULong(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::Float(v) => (*v as f64).into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::Double(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::String(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
    }
}

/// Convert a TypedScalarArray to a Python list.
pub fn typed_array_to_py(py: Python<'_>, arr: &TypedScalarArray) -> PyObject {
    match arr {
        TypedScalarArray::Boolean(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::Byte(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::UByte(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::Short(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::UShort(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::Int(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::UInt(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::Long(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::ULong(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::Float(a) => PyList::new(py, a.iter().map(|x| *x as f64))
            .unwrap()
            .into_any()
            .unbind(),
        TypedScalarArray::Double(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
        TypedScalarArray::String(a) => PyList::new(py, a.iter()).unwrap().into_any().unbind(),
    }
}

/// Convert a generic PvField value to a Python object — best-effort.
/// Scalars and scalar arrays produce native types; structures produce dicts.
pub fn pvfield_to_py(py: Python<'_>, field: &PvField) -> PyObject {
    match field {
        PvField::Scalar(s) => scalar_to_py(py, s),
        PvField::ScalarArray(arr) => PyList::new(py, arr.iter().map(|s| scalar_to_py(py, s)))
            .unwrap()
            .into_any()
            .unbind(),
        PvField::ScalarArrayTyped(arr) => typed_array_to_py(py, arr),
        PvField::Structure(s) => structure_to_py(py, s),
        PvField::StructureArray(arr) => PyList::new(py, arr.iter().map(|s| structure_to_py(py, s)))
            .unwrap()
            .into_any()
            .unbind(),
        PvField::Union { value, .. } => pvfield_to_py(py, value),
        PvField::UnionArray(items) => {
            PyList::new(py, items.iter().map(|it| pvfield_to_py(py, &it.value)))
                .unwrap()
                .into_any()
                .unbind()
        }
        PvField::Variant(v) => pvfield_to_py(py, &v.value),
        PvField::VariantArray(items) => {
            PyList::new(py, items.iter().map(|it| pvfield_to_py(py, &it.value)))
                .unwrap()
                .into_any()
                .unbind()
        }
        PvField::Null => py.None(),
    }
}

/// Convert a PvStructure to a Python dict (recursive).
fn structure_to_py(py: Python<'_>, s: &PvStructure) -> PyObject {
    let dict = PyDict::new(py);
    for (name, field) in &s.fields {
        let _ = dict.set_item(name, pvfield_to_py(py, field));
    }
    dict.into_any().unbind()
}

/// Extract scalar from a ScalarValue as f64 (best effort, for alarm/timestamp fields).
fn scalar_as_f64(s: &ScalarValue) -> Option<f64> {
    match s {
        ScalarValue::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
        ScalarValue::Byte(v) => Some(*v as f64),
        ScalarValue::Short(v) => Some(*v as f64),
        ScalarValue::Int(v) => Some(*v as f64),
        ScalarValue::Long(v) => Some(*v as f64),
        ScalarValue::UByte(v) => Some(*v as f64),
        ScalarValue::UShort(v) => Some(*v as f64),
        ScalarValue::UInt(v) => Some(*v as f64),
        ScalarValue::ULong(v) => Some(*v as f64),
        ScalarValue::Float(v) => Some(*v as f64),
        ScalarValue::Double(v) => Some(*v),
        ScalarValue::String(_) => None,
    }
}

fn scalar_as_i64(s: &ScalarValue) -> Option<i64> {
    match s {
        ScalarValue::Boolean(v) => Some(if *v { 1 } else { 0 }),
        ScalarValue::Byte(v) => Some(*v as i64),
        ScalarValue::Short(v) => Some(*v as i64),
        ScalarValue::Int(v) => Some(*v as i64),
        ScalarValue::Long(v) => Some(*v),
        ScalarValue::UByte(v) => Some(*v as i64),
        ScalarValue::UShort(v) => Some(*v as i64),
        ScalarValue::UInt(v) => Some(*v as i64),
        ScalarValue::ULong(v) => Some(*v as i64),
        _ => None,
    }
}

fn struct_field_scalar<'a>(s: &'a PvStructure, name: &str) -> Option<&'a ScalarValue> {
    match s.get_field(name)? {
        PvField::Scalar(v) => Some(v),
        _ => None,
    }
}

fn struct_field_string(s: &PvStructure, name: &str) -> Option<String> {
    match s.get_field(name)? {
        PvField::Scalar(ScalarValue::String(v)) => Some(v.clone()),
        _ => None,
    }
}

/// Public alias used by pva.rs to give get_value_async / monitor parity
/// for NTEnum PVs (both surface as int index, not {index, choices} dict).
pub(crate) fn try_extract_ntenum(s: &PvStructure) -> Option<(i32, Vec<String>)> {
    extract_ntenum(s)
}

/// Return the NTEnum value field as (index, choices) if `value` is an
/// `enum_t` substructure, or None otherwise.
fn extract_ntenum(s: &PvStructure) -> Option<(i32, Vec<String>)> {
    let enum_struct = match s.get_field("value")? {
        PvField::Structure(es) => es,
        _ => return None,
    };
    let index = struct_field_scalar(enum_struct, "index").and_then(scalar_as_i64)? as i32;
    let choices = match enum_struct.get_field("choices")? {
        PvField::ScalarArrayTyped(TypedScalarArray::String(arr)) => arr.iter().cloned().collect(),
        PvField::ScalarArray(arr) => arr
            .iter()
            .filter_map(|v| match v {
                ScalarValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Some((index, choices))
}

/// Build an ophyd-compatible metadata dict from a top-level PvField.
/// Handles NTScalar, NTScalarArray, NTEnum shapes; falls back to raw value
/// for non-NT structures.
pub fn pvfield_to_metadata(py: Python<'_>, field: &PvField) -> PyObject {
    let dict = PyDict::new(py);

    match field {
        PvField::Structure(s) => {
            // NTEnum: value is itself a structure with index + choices.
            if let Some((idx, choices)) = extract_ntenum(s) {
                let _ = dict.set_item("value", idx);
                let _ = dict.set_item(
                    "char_value",
                    choices
                        .get(idx as usize)
                        .cloned()
                        .unwrap_or_else(|| idx.to_string()),
                );
                if !choices.is_empty() {
                    let tuple = PyTuple::new(py, choices.iter()).unwrap();
                    let _ = dict.set_item("enum_strs", tuple);
                }
            } else if let Some(value_field) = s.get_field("value") {
                // NTScalar / NTScalarArray
                let value_py = pvfield_to_py(py, value_field);
                let _ = dict.set_item("value", &value_py);
                let cv = match value_field {
                    PvField::Scalar(sv) => format!("{sv}"),
                    other => format!("{other}"),
                };
                let _ = dict.set_item("char_value", cv);
            } else {
                // Bare structure — return whole thing as dict
                let _ = dict.set_item("value", structure_to_py(py, s));
            }

            // alarm
            if let Some(alarm) = s.get_alarm() {
                let severity = struct_field_scalar(alarm, "severity")
                    .and_then(scalar_as_i64)
                    .unwrap_or(0);
                let status = struct_field_scalar(alarm, "status")
                    .and_then(scalar_as_i64)
                    .unwrap_or(0);
                let _ = dict.set_item("severity", severity);
                let _ = dict.set_item("status", status);
                if let Some(msg) = struct_field_string(alarm, "message") {
                    let _ = dict.set_item("alarm_message", msg);
                }
            } else {
                let _ = dict.set_item("severity", 0i64);
                let _ = dict.set_item("status", 0i64);
            }

            // timeStamp
            if let Some(ts) = s.get_timestamp() {
                let secs = struct_field_scalar(ts, "secondsPastEpoch")
                    .and_then(scalar_as_i64)
                    .unwrap_or(0);
                let nanos = struct_field_scalar(ts, "nanoseconds")
                    .and_then(scalar_as_i64)
                    .unwrap_or(0) as u32;
                let timestamp = secs as f64 + (nanos as f64) * 1e-9;
                let _ = dict.set_item("timestamp", timestamp);
                let _ = dict.set_item("posixseconds", secs);
                let _ = dict.set_item("nanoseconds", nanos);
            } else {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                let _ = dict.set_item("timestamp", now.as_secs_f64());
                let _ = dict.set_item("posixseconds", now.as_secs() as i64);
                let _ = dict.set_item("nanoseconds", now.subsec_nanos());
            }

            // display
            if let Some(PvField::Structure(disp)) = s.get_field("display") {
                if let Some(p) = struct_field_scalar(disp, "precision").and_then(scalar_as_i64) {
                    let _ = dict.set_item("precision", p);
                }
                if let Some(units) = struct_field_string(disp, "units") {
                    let _ = dict.set_item("units", units);
                }
                if let Some(lo) = struct_field_scalar(disp, "limitLow").and_then(scalar_as_f64) {
                    let _ = dict.set_item("lower_disp_limit", lo);
                }
                if let Some(hi) = struct_field_scalar(disp, "limitHigh").and_then(scalar_as_f64) {
                    let _ = dict.set_item("upper_disp_limit", hi);
                }
            }

            // control
            if let Some(PvField::Structure(ctrl)) = s.get_field("control") {
                if let Some(lo) = struct_field_scalar(ctrl, "limitLow").and_then(scalar_as_f64) {
                    let _ = dict.set_item("lower_ctrl_limit", lo);
                }
                if let Some(hi) = struct_field_scalar(ctrl, "limitHigh").and_then(scalar_as_f64) {
                    let _ = dict.set_item("upper_ctrl_limit", hi);
                }
            }

            // valueAlarm — limits only (skip hysteresis etc.)
            if let Some(PvField::Structure(va)) = s.get_field("valueAlarm") {
                if let Some(v) = struct_field_scalar(va, "highAlarmLimit").and_then(scalar_as_f64) {
                    let _ = dict.set_item("upper_alarm_limit", v);
                }
                if let Some(v) = struct_field_scalar(va, "lowAlarmLimit").and_then(scalar_as_f64) {
                    let _ = dict.set_item("lower_alarm_limit", v);
                }
                if let Some(v) = struct_field_scalar(va, "highWarningLimit").and_then(scalar_as_f64)
                {
                    let _ = dict.set_item("upper_warning_limit", v);
                }
                if let Some(v) = struct_field_scalar(va, "lowWarningLimit").and_then(scalar_as_f64)
                {
                    let _ = dict.set_item("lower_warning_limit", v);
                }
            }
        }
        // Non-NT top-level value — just wrap as value
        other => {
            let _ = dict.set_item("value", pvfield_to_py(py, other));
            let _ = dict.set_item("char_value", format!("{other}"));
            let _ = dict.set_item("severity", 0i64);
            let _ = dict.set_item("status", 0i64);
        }
    }

    dict.into_any().unbind()
}
