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
///
/// Hot-path micro-optimization: the three most common NTScalar value types
/// (Double, Long, Int) use direct CPython FFI to skip PyO3's trait dispatch
/// chain (`into_pyobject → BoundObject → into_any → unbind`).
pub fn scalar_to_py(py: Python<'_>, val: &ScalarValue) -> PyObject {
    match val {
        // Fast path: direct FFI for the most common types.
        ScalarValue::Double(v) => unsafe {
            PyObject::from_owned_ptr(py, pyo3::ffi::PyFloat_FromDouble(*v))
        },
        ScalarValue::Long(v) => unsafe {
            PyObject::from_owned_ptr(py, pyo3::ffi::PyLong_FromLongLong(*v))
        },
        ScalarValue::Int(v) => unsafe {
            PyObject::from_owned_ptr(py, pyo3::ffi::PyLong_FromLong(*v as std::ffi::c_long))
        },
        // Standard path for less common types.
        ScalarValue::Boolean(v) => v.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        ScalarValue::Byte(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::Short(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::UByte(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::UShort(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::UInt(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::ULong(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        ScalarValue::Float(v) => unsafe {
            PyObject::from_owned_ptr(py, pyo3::ffi::PyFloat_FromDouble(*v as f64))
        },
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
///
/// Hard-gated by struct_id: top-level `epics:nt/NTEnum:1.0` OR the
/// inner `value` substructure tagged `enum_t`. Without this guard an
/// NTTable whose value substructure happens to have columns named
/// `index` (int) and `choices` (string array) would be misclassified
/// — get_value_async would surface an int instead of the row dict.
fn extract_ntenum(s: &PvStructure) -> Option<(i32, Vec<String>)> {
    let enum_struct = match s.get_field("value")? {
        PvField::Structure(es) => es,
        _ => return None,
    };
    // NT identification: outer struct_id is "epics:nt/NTEnum:1.0" per
    // the spec, with inner `value` carrying struct_id "enum_t". Either
    // marker is sufficient — gate on both being unset, then bail.
    let outer_is_ntenum = s.struct_id == "epics:nt/NTEnum:1.0";
    let inner_is_enum_t = enum_struct.struct_id == "enum_t";
    if !(outer_is_ntenum || inner_is_enum_t) {
        return None;
    }
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
pub fn pvfield_to_metadata(py: Python<'_>, field: &PvField, client_time: Option<(f64, i64, u32)>) -> PyObject {
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
                set_client_timestamp(&dict, client_time);
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
        // Non-NT top-level value (Variant, ScalarArray, etc.) — wrap
        // the raw value and stamp a client-side timestamp so callers
        // never see a 0.0 epoch as a "valid" Reading.
        other => {
            let _ = dict.set_item("value", pvfield_to_py(py, other));
            let _ = dict.set_item("char_value", format!("{other}"));
            let _ = dict.set_item("severity", 0i64);
            let _ = dict.set_item("status", 0i64);
            set_client_timestamp(&dict, client_time);
        }
    }

    dict.into_any().unbind()
}

/// Stamp a client-side timestamp on the metadata dict and mark it as
/// such. `client_timestamp = True` so downstream code can distinguish
/// IOC-sourced from synthesized timestamps (and choose to ignore the
/// latter for time-critical metrics).
fn set_client_timestamp(dict: &Bound<'_, PyDict>, client_time: Option<(f64, i64, u32)>) {
    let (ts, secs, nanos) = client_time.unwrap_or_else(|| {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (now.as_secs_f64(), now.as_secs() as i64, now.subsec_nanos())
    });
    let _ = dict.set_item("timestamp", ts);
    let _ = dict.set_item("posixseconds", secs);
    let _ = dict.set_item("nanoseconds", nanos);
    let _ = dict.set_item("client_timestamp", true);
}

#[pyclass(mapping, name = "EpicsRsPvaMetadata")]
#[derive(Clone)]
pub struct EpicsRsPvaMetadata {
    pub field: PvField,
    pub client_time: Option<(f64, i64, u32)>,
    cached_dict: std::sync::Arc<std::sync::OnceLock<PyObject>>,
}

impl EpicsRsPvaMetadata {
    pub fn new(field: PvField) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let client_time = match &field {
            PvField::Structure(s) => {
                if s.get_timestamp().is_none() {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
                    Some((now.as_secs_f64(), now.as_secs() as i64, now.subsec_nanos()))
                } else {
                    None
                }
            }
            _ => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
                Some((now.as_secs_f64(), now.as_secs() as i64, now.subsec_nanos()))
            }
        };

        Self {
            field,
            client_time,
            cached_dict: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }
}

#[pymethods]
impl EpicsRsPvaMetadata {
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<PyObject> {
        if let Some(dict) = self.cached_dict.get() {
            let val = dict.bind(py).get_item(key)?;
            return Ok(val.into_any().unbind());
        }

        match key {
            "value" => {
                match &self.field {
                    PvField::Structure(s) => {
                        if let Some((idx, _)) = extract_ntenum(s) {
                            return Ok(idx.into_pyobject(py).unwrap().into_any().unbind());
                        } else if let Some(value_field) = s.get_field("value") {
                            return Ok(pvfield_to_py(py, value_field));
                        } else {
                            return Ok(structure_to_py(py, s));
                        }
                    }
                    other => {
                        return Ok(pvfield_to_py(py, other));
                    }
                }
            }
            "timestamp" => {
                match &self.field {
                    PvField::Structure(s) => {
                        if let Some(ts) = s.get_timestamp() {
                            let secs = struct_field_scalar(ts, "secondsPastEpoch")
                                .and_then(scalar_as_i64)
                                .unwrap_or(0);
                            let nanos = struct_field_scalar(ts, "nanoseconds")
                                .and_then(scalar_as_i64)
                                .unwrap_or(0) as u32;
                            let timestamp = secs as f64 + (nanos as f64) * 1e-9;
                            return Ok(timestamp.into_pyobject(py).unwrap().into_any().unbind());
                        }
                    }
                    _ => {}
                }
                if let Some((ts, _, _)) = self.client_time {
                    return Ok(ts.into_pyobject(py).unwrap().into_any().unbind());
                }
            }
            "severity" => {
                match &self.field {
                    PvField::Structure(s) => {
                        if let Some(alarm) = s.get_alarm() {
                            let severity = struct_field_scalar(alarm, "severity")
                                .and_then(scalar_as_i64)
                                .unwrap_or(0);
                            return Ok(severity.into_pyobject(py).unwrap().into_any().unbind());
                        }
                    }
                    _ => {}
                }
                return Ok(0i64.into_pyobject(py).unwrap().into_any().unbind());
            }
            "status" => {
                match &self.field {
                    PvField::Structure(s) => {
                        if let Some(alarm) = s.get_alarm() {
                            let status = struct_field_scalar(alarm, "status")
                                .and_then(scalar_as_i64)
                                .unwrap_or(0);
                            return Ok(status.into_pyobject(py).unwrap().into_any().unbind());
                        }
                    }
                    _ => {}
                }
                return Ok(0i64.into_pyobject(py).unwrap().into_any().unbind());
            }
            _ => {}
        }

        let dict = pvfield_to_metadata(py, &self.field, self.client_time);
        let val = dict.bind(py).get_item(key)?;
        let _ = self.cached_dict.set(dict);
        Ok(val.into_any().unbind())
    }

    fn keys(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = self.get_or_build_dict(py);
        dict.bind(py).call_method0("keys").map(|v| v.into_any().unbind())
    }

    fn values(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = self.get_or_build_dict(py);
        dict.bind(py).call_method0("values").map(|v| v.into_any().unbind())
    }

    fn items(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = self.get_or_build_dict(py);
        dict.bind(py).call_method0("items").map(|v| v.into_any().unbind())
    }

    fn __contains__(&self, py: Python<'_>, key: &str) -> PyResult<bool> {
        let dict = self.get_or_build_dict(py);
        dict.bind(py).contains(key)
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = self.get_or_build_dict(py);
        dict.bind(py).call_method0("__iter__").map(|v| v.into_any().unbind())
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        let dict = self.get_or_build_dict(py);
        dict.bind(py).len()
    }

    /// Mapping-style ``get(key, default=None)`` — non-mutating. Routes
    /// through ``__getitem__`` so well-known keys (value, timestamp,
    /// severity, status) stay on the lazy fast path; missing keys fall
    /// through to the materialised dict and return ``default``.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyObject {
        match self.__getitem__(py, key) {
            Ok(v) => v,
            Err(_) => default.unwrap_or_else(|| py.None()),
        }
    }

    /// Mutating ``pop`` — ophyd's ``Signal.get`` calls
    /// ``info.pop("value")`` and then iterates the rest as alarm /
    /// timestamp metadata. Materialise the underlying dict so the pop
    /// actually removes the entry; subsequent ``__getitem__`` /
    /// ``keys`` see the modified view via the cache.
    #[pyo3(signature = (key, default=None))]
    fn pop(
        &self,
        py: Python<'_>,
        key: &str,
        default: Option<PyObject>,
    ) -> PyResult<PyObject> {
        let dict = self.get_or_build_dict(py).clone_ref(py);
        let bound = dict.bind(py);
        match bound.call_method1("pop", (key, default.unwrap_or_else(|| py.None()))) {
            Ok(v) => Ok(v.into_any().unbind()),
            Err(e) => Err(e),
        }
    }
}

impl EpicsRsPvaMetadata {
    fn get_or_build_dict(&self, py: Python<'_>) -> &PyObject {
        self.cached_dict.get_or_init(|| {
            pvfield_to_metadata(py, &self.field, self.client_time)
        })
    }
}
