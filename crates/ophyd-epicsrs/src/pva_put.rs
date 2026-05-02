//! Python value → epics-rs PvField conversion for PVA typed put
//! (e.g. NTTable column structures).
//!
//! The PvaClient string-form pvput is fine for scalars but cannot
//! express column-structured types like NTTable: the IOC parses the
//! string and rejects anything that isn't a plain scalar literal. To
//! write a Table the wire payload must be a properly-typed
//! `PvStructure` whose fields are `ScalarArrayTyped::<dtype>` columns.
//!
//! ## dtype resolution
//!
//! For every column the converter picks one ScalarArrayTyped variant
//! by checking, in order:
//! 1. **dtype_hints[col]** — numpy dtype string ("<i1", "<f8", ...)
//!    or the literal "string". Comes from
//!    `_TableConverter.__annotations__` so an empty list still gets
//!    the right ScalarArrayTyped variant.
//! 2. **numpy.ndarray.dtype** — the array carries its own dtype.
//! 3. **Element-type sniffing** — first element of a non-empty list.
//! 4. **Double fallback** — empty list + no hint => Double (safe
//!    default; Double is widely accepted across PVA servers).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use epics_rs::pva::pvdata::{PvField, PvStructure, ScalarValue, TypedScalarArray};

/// Convert a Python value to a `PvField`, optionally guided by dtype
/// hints (column → numpy dtype string / "string").
///
/// `struct_id` is used when the top-level value is a dict — it's
/// stamped onto the resulting `PvStructure` so the wire structure
/// carries e.g. `epics:nt/NTTable:1.0`. Pass an empty string to leave
/// it unset.
pub(crate) fn py_to_pvfield(
    value: &Bound<'_, PyAny>,
    dtype_hints: &HashMap<String, String>,
    struct_id: &str,
) -> PyResult<PvField> {
    // dict → PvStructure
    if let Ok(d) = value.downcast::<PyDict>() {
        return py_dict_to_structure(d, dtype_hints, struct_id);
    }

    // numpy array → ScalarArrayTyped using its own dtype
    if value.hasattr("dtype").unwrap_or(false) && value.hasattr("ndim").unwrap_or(false) {
        let ndim: i32 = value.getattr("ndim").and_then(|v| v.extract()).unwrap_or(0);
        if ndim > 0 {
            let dtype_str: String = value.getattr("dtype")?.getattr("str")?.extract()?;
            let lst = value.call_method0("tolist")?;
            let lst = lst.downcast::<PyList>()?;
            return Ok(PvField::ScalarArrayTyped(list_to_typed_array(
                lst,
                Some(dtype_str.as_str()),
            )?));
        }
        // ndim == 0 numpy scalar — fall through after coercion via .item()
        if let Ok(native) = value.call_method0("item") {
            return py_to_pvfield(&native, dtype_hints, struct_id);
        }
    }

    // list / tuple → ScalarArrayTyped (no hint at top level)
    if let Ok(l) = value.downcast::<PyList>() {
        return Ok(PvField::ScalarArrayTyped(list_to_typed_array(l, None)?));
    }
    if let Ok(t) = value.downcast::<PyTuple>() {
        // Cast tuple → list for convenience.
        let lst = PyList::new(value.py(), t.iter())?;
        return Ok(PvField::ScalarArrayTyped(list_to_typed_array(&lst, None)?));
    }

    // bytes → ScalarArrayTyped::UByte (treat as raw bytes)
    if let Ok(b) = value.downcast::<PyBytes>() {
        let bytes: Vec<u8> = b.as_bytes().to_vec();
        return Ok(PvField::ScalarArrayTyped(TypedScalarArray::UByte(
            Arc::from(bytes),
        )));
    }

    // Scalar fallback
    Ok(PvField::Scalar(py_to_scalar(value)?))
}

fn py_dict_to_structure(
    d: &Bound<'_, PyDict>,
    dtype_hints: &HashMap<String, String>,
    struct_id: &str,
) -> PyResult<PvField> {
    let mut fields: Vec<(String, PvField)> = Vec::with_capacity(d.len());
    for (k, v) in d.iter() {
        let key: String = k.extract()?;
        // Per-column dtype hint by name; nested dicts pass the same map down.
        let field = if let Ok(inner) = v.downcast::<PyDict>() {
            py_dict_to_structure(inner, dtype_hints, "")?
        } else if let Some(hint) = dtype_hints.get(&key) {
            value_with_hint(&v, hint)?
        } else {
            py_to_pvfield(&v, dtype_hints, "")?
        };
        fields.push((key, field));
    }
    Ok(PvField::Structure(PvStructure {
        struct_id: struct_id.to_string(),
        fields,
    }))
}

fn value_with_hint(value: &Bound<'_, PyAny>, hint: &str) -> PyResult<PvField> {
    // Scalar / sequence → coerce via hint.
    let lst = if let Ok(l) = value.downcast::<PyList>() {
        Some(l.clone())
    } else if let Ok(t) = value.downcast::<PyTuple>() {
        Some(PyList::new(value.py(), t.iter())?)
    } else if value.hasattr("dtype").unwrap_or(false) && value.hasattr("ndim").unwrap_or(false) {
        let ndim: i32 = value.getattr("ndim").and_then(|v| v.extract()).unwrap_or(0);
        if ndim > 0 {
            let lst = value.call_method0("tolist")?;
            Some(lst.downcast::<PyList>()?.clone())
        } else {
            None
        }
    } else {
        None
    };
    if let Some(lst) = lst {
        Ok(PvField::ScalarArrayTyped(list_to_typed_array(
            &lst,
            Some(hint),
        )?))
    } else {
        // Scalar with hint — coerce
        Ok(PvField::Scalar(py_to_scalar_with_hint(value, hint)?))
    }
}

fn list_to_typed_array(lst: &Bound<'_, PyList>, hint: Option<&str>) -> PyResult<TypedScalarArray> {
    let len = lst.len();
    // Resolve dtype: hint > inference > Double fallback.
    let resolved = match hint {
        Some(h) => h.to_string(),
        None => infer_dtype_from_list(lst)?,
    };
    match resolved.as_str() {
        "string" | "|S" | "|U" | "<U" | ">U" => {
            let mut v: Vec<String> = Vec::with_capacity(len);
            for item in lst.iter() {
                v.push(item.extract::<String>().or_else(|_| {
                    // Allow non-str via str() coercion
                    item.str().map(|s| s.to_string())
                })?);
            }
            Ok(TypedScalarArray::String(Arc::from(v)))
        }
        "|b1" => {
            let mut v: Vec<bool> = Vec::with_capacity(len);
            for item in lst.iter() {
                v.push(item.extract()?);
            }
            Ok(TypedScalarArray::Boolean(Arc::from(v)))
        }
        "|i1" => extract_into_typed_array::<i8>(lst, TypedScalarArray::Byte),
        "|u1" => extract_into_typed_array::<u8>(lst, TypedScalarArray::UByte),
        "<i2" | ">i2" => extract_into_typed_array::<i16>(lst, TypedScalarArray::Short),
        "<u2" | ">u2" => extract_into_typed_array::<u16>(lst, TypedScalarArray::UShort),
        "<i4" | ">i4" => extract_into_typed_array::<i32>(lst, TypedScalarArray::Int),
        "<u4" | ">u4" => extract_into_typed_array::<u32>(lst, TypedScalarArray::UInt),
        "<i8" | ">i8" => extract_into_typed_array::<i64>(lst, TypedScalarArray::Long),
        "<u8" | ">u8" => extract_into_typed_array::<u64>(lst, TypedScalarArray::ULong),
        "<f4" | ">f4" => extract_into_typed_array::<f32>(lst, TypedScalarArray::Float),
        "<f8" | ">f8" => extract_into_typed_array::<f64>(lst, TypedScalarArray::Double),
        other => Err(PyValueError::new_err(format!(
            "unsupported dtype hint {other:?} for typed array (supported: \
            string, |b1, |i1, |u1, <i2, <u2, <i4, <u4, <i8, <u8, <f4, <f8)"
        ))),
    }
}

fn extract_into_typed_array<T>(
    lst: &Bound<'_, PyList>,
    ctor: fn(Arc<[T]>) -> TypedScalarArray,
) -> PyResult<TypedScalarArray>
where
    T: for<'a> FromPyObject<'a>,
{
    let mut v: Vec<T> = Vec::with_capacity(lst.len());
    for item in lst.iter() {
        v.push(item.extract::<T>()?);
    }
    Ok(ctor(Arc::from(v)))
}

/// Sniff the dtype of a non-empty Python list. Empty list → "<f8"
/// (Double) as a safe default — caller is expected to provide a hint
/// for empty columns when the type matters.
fn infer_dtype_from_list(lst: &Bound<'_, PyList>) -> PyResult<String> {
    if lst.is_empty() {
        return Ok("<f8".to_string());
    }
    let first = lst.get_item(0)?;
    if first.is_instance_of::<PyBool>() {
        Ok("|b1".to_string())
    } else if first.is_instance_of::<PyInt>() {
        Ok("<i8".to_string())
    } else if first.is_instance_of::<PyFloat>() {
        Ok("<f8".to_string())
    } else if first.is_instance_of::<PyString>() {
        Ok("string".to_string())
    } else {
        Err(PyTypeError::new_err(format!(
            "cannot infer PVA scalar type from element {first:?}"
        )))
    }
}

fn py_to_scalar(value: &Bound<'_, PyAny>) -> PyResult<ScalarValue> {
    if let Ok(b) = value.extract::<bool>() {
        return Ok(ScalarValue::Boolean(b));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(ScalarValue::Long(i));
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(ScalarValue::Double(f));
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(ScalarValue::String(s));
    }
    Err(PyTypeError::new_err(format!(
        "cannot convert {value:?} to PVA scalar"
    )))
}

fn py_to_scalar_with_hint(value: &Bound<'_, PyAny>, hint: &str) -> PyResult<ScalarValue> {
    match hint {
        "string" | "|S" | "|U" | "<U" | ">U" => Ok(ScalarValue::String(
            value
                .extract::<String>()
                .or_else(|_| value.str().map(|s| s.to_string()))?,
        )),
        "|b1" => Ok(ScalarValue::Boolean(value.extract()?)),
        "|i1" => Ok(ScalarValue::Byte(value.extract()?)),
        "|u1" => Ok(ScalarValue::UByte(value.extract()?)),
        "<i2" | ">i2" => Ok(ScalarValue::Short(value.extract()?)),
        "<u2" | ">u2" => Ok(ScalarValue::UShort(value.extract()?)),
        "<i4" | ">i4" => Ok(ScalarValue::Int(value.extract()?)),
        "<u4" | ">u4" => Ok(ScalarValue::UInt(value.extract()?)),
        "<i8" | ">i8" => Ok(ScalarValue::Long(value.extract()?)),
        "<u8" | ">u8" => Ok(ScalarValue::ULong(value.extract()?)),
        "<f4" | ">f4" => Ok(ScalarValue::Float(value.extract()?)),
        "<f8" | ">f8" => Ok(ScalarValue::Double(value.extract()?)),
        other => Err(PyValueError::new_err(format!(
            "unsupported dtype hint {other:?} for scalar"
        ))),
    }
}

// Note: Python-touching code can't be unit-tested via `cargo test`
// because the abi3 cdylib has no Python symbols at link time. Coverage
// for py_to_pvfield comes from Python-level smoke tests that exercise
// EpicsRsPvaPV.put_pv_field_async directly.
