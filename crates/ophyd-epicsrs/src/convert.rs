use epics_rs::base::server::snapshot::Snapshot;
use epics_rs::base::types::{EpicsValue, WallTime};
use numpy::PyArray1;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

/// Convert an EpicsValue to a Python object.
///
/// Numeric arrays (Double / Float / Long / Int64 / Short / Char / Enum)
/// return as ``numpy.ndarray`` (zero-copy single allocation, ~10 µs for
/// 10000 doubles) instead of ``list`` (which costs N × PyFloat alloc and
/// dominates 10k-element waveform reads — measured at ~150 µs/PV in
/// the bench, vs ~5 µs/PV with ndarray). String arrays stay as ``list``
/// since numpy object-arrays gain nothing here.
pub fn epics_value_to_py(py: Python<'_>, val: &EpicsValue) -> PyObject {
    match val {
        EpicsValue::Double(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Float(v) => (*v as f64).into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Long(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Int64(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Short(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Char(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::Enum(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        // Transient pvalink-server carrier; behaves exactly like Enum
        // everywhere else (labels are for the IOC's put_field only).
        EpicsValue::EnumWithChoices { index, .. } => {
            index.into_pyobject(py).unwrap().into_any().unbind()
        }
        EpicsValue::UChar(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::UShort(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::ULong(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        EpicsValue::UInt64(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        // PvString carries raw wire bytes with no UTF-8 guarantee; the
        // lossy view (U+FFFD for invalid bytes) matches pyepics'
        // `errors='replace'` decode policy at the Python boundary.
        EpicsValue::String(v) => v
            .as_str_lossy()
            .into_pyobject(py)
            .unwrap()
            .into_any()
            .unbind(),
        EpicsValue::DoubleArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::FloatArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::LongArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::Int64Array(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::ShortArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::CharArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::EnumArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::UCharArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::UShortArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::ULongArray(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::UInt64Array(v) => PyArray1::from_slice(py, v).into_any().unbind(),
        EpicsValue::StringArray(v) => PyList::new(py, v.iter().map(|s| s.as_str_lossy()))
            .unwrap()
            .into_any()
            .unbind(),
    }
}

/// Convert a Python value to an EpicsValue, given the native DbFieldType.
/// Handles both scalar and array (list/numpy) inputs.
pub fn py_to_epics_value(
    obj: &Bound<'_, pyo3::PyAny>,
    native_type: epics_rs::base::types::DbFieldType,
) -> PyResult<EpicsValue> {
    use epics_rs::base::types::DbFieldType;

    // Try array extraction first: list, tuple, or numpy array
    if let Ok(seq) = obj.downcast::<pyo3::types::PyList>() {
        return py_sequence_to_epics_array(seq.as_any(), native_type);
    }
    if let Ok(seq) = obj.downcast::<pyo3::types::PyTuple>() {
        return py_sequence_to_epics_array(seq.as_any(), native_type);
    }
    // numpy arrays (not scalars): have dtype AND ndim > 0
    if obj.hasattr("dtype").unwrap_or(false) && obj.hasattr("ndim").unwrap_or(false) {
        let ndim: i32 = obj.getattr("ndim").and_then(|v| v.extract()).unwrap_or(0);
        if ndim > 0 {
            return py_sequence_to_epics_array(obj, native_type);
        }
        // ndim == 0: numpy scalar — fall through to scalar path
        // .item() converts np.float64(6.5) → Python float 6.5
        if let Ok(native) = obj.call_method0("item") {
            return py_to_epics_value(&native, native_type);
        }
    }

    // Scalar path
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
        DbFieldType::Int64 => {
            let v: i64 = obj.extract()?;
            Ok(EpicsValue::Int64(v))
        }
        DbFieldType::Short => {
            let v: i16 = obj.extract()?;
            Ok(EpicsValue::Short(v))
        }
        DbFieldType::Char => {
            // String → CharArray (for waveform FTVL=CHAR path PVs)
            // Must include null terminator so IOC doesn't read stale bytes.
            if let Ok(s) = obj.extract::<String>() {
                let mut bytes = s.into_bytes();
                bytes.push(0);
                return Ok(EpicsValue::CharArray(bytes));
            }
            let v: u8 = obj.extract()?;
            Ok(EpicsValue::Char(v))
        }
        // Unsigned internal types (epics-rs 0.24): no CA wire code exists
        // for these — the IOC promotes them to signed/double DBR types —
        // so a CA client channel never reports them as native_type. Kept
        // total so the conversion is usable if a future wire path (PVA
        // ioc-side) routes through here.
        DbFieldType::UChar => {
            if let Ok(s) = obj.extract::<String>() {
                let mut bytes = s.into_bytes();
                bytes.push(0);
                return Ok(EpicsValue::UCharArray(bytes));
            }
            let v: u8 = obj.extract()?;
            Ok(EpicsValue::UChar(v))
        }
        DbFieldType::UShort => {
            let v: u16 = obj.extract()?;
            Ok(EpicsValue::UShort(v))
        }
        DbFieldType::ULong => {
            let v: u32 = obj.extract()?;
            Ok(EpicsValue::ULong(v))
        }
        DbFieldType::UInt64 => {
            let v: u64 = obj.extract()?;
            Ok(EpicsValue::UInt64(v))
        }
        DbFieldType::Enum => {
            // Try integer first, then parse string as integer.
            // Named enum strings (e.g. "Enable") are resolved in the Python shim
            // using cached enum_strs before reaching here.
            if let Ok(v) = obj.extract::<u16>() {
                Ok(EpicsValue::Enum(v))
            } else if let Ok(s) = obj.extract::<String>() {
                s.parse::<u16>().map(EpicsValue::Enum).map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(format!(
                        "cannot convert '{}' to enum index",
                        s
                    ))
                })
            } else {
                Err(pyo3::exceptions::PyTypeError::new_err(
                    "enum value must be an integer or string",
                ))
            }
        }
        DbFieldType::String => {
            let v: String = obj.extract()?;
            Ok(EpicsValue::String(v.into()))
        }
    }
}

/// Convert a Python sequence (list/tuple/ndarray) to an EpicsValue array.
fn py_sequence_to_epics_array(
    obj: &Bound<'_, pyo3::PyAny>,
    native_type: epics_rs::base::types::DbFieldType,
) -> PyResult<EpicsValue> {
    use epics_rs::base::types::DbFieldType;
    match native_type {
        DbFieldType::Double => {
            let v: Vec<f64> = obj.extract()?;
            Ok(EpicsValue::DoubleArray(v))
        }
        DbFieldType::Float => {
            let v: Vec<f32> = obj.extract()?;
            Ok(EpicsValue::FloatArray(v))
        }
        DbFieldType::Long => {
            let v: Vec<i32> = obj.extract()?;
            Ok(EpicsValue::LongArray(v))
        }
        DbFieldType::Int64 => {
            let v: Vec<i64> = obj.extract()?;
            Ok(EpicsValue::Int64Array(v))
        }
        DbFieldType::Short => {
            let v: Vec<i16> = obj.extract()?;
            Ok(EpicsValue::ShortArray(v))
        }
        DbFieldType::Char => {
            // Accept string → bytes for char waveforms (null terminated)
            if let Ok(s) = obj.extract::<String>() {
                let mut bytes = s.into_bytes();
                bytes.push(0);
                return Ok(EpicsValue::CharArray(bytes));
            }
            let v: Vec<u8> = obj.extract()?;
            Ok(EpicsValue::CharArray(v))
        }
        // Unsigned internal types — unreachable from a CA channel's
        // native_type (see the scalar match above), kept total.
        DbFieldType::UChar => {
            if let Ok(s) = obj.extract::<String>() {
                let mut bytes = s.into_bytes();
                bytes.push(0);
                return Ok(EpicsValue::UCharArray(bytes));
            }
            let v: Vec<u8> = obj.extract()?;
            Ok(EpicsValue::UCharArray(v))
        }
        DbFieldType::UShort => {
            let v: Vec<u16> = obj.extract()?;
            Ok(EpicsValue::UShortArray(v))
        }
        DbFieldType::ULong => {
            let v: Vec<u32> = obj.extract()?;
            Ok(EpicsValue::ULongArray(v))
        }
        DbFieldType::UInt64 => {
            let v: Vec<u64> = obj.extract()?;
            Ok(EpicsValue::UInt64Array(v))
        }
        DbFieldType::Enum => {
            let v: Vec<u16> = obj.extract()?;
            Ok(EpicsValue::EnumArray(v))
        }
        DbFieldType::String => {
            let v: Vec<String> = obj.extract()?;
            Ok(EpicsValue::StringArray(
                v.into_iter().map(Into::into).collect(),
            ))
        }
    }
}

fn wall_time_to_epoch(t: WallTime) -> f64 {
    t.since_unix_epoch().as_secs_f64()
}

/// Convert a Snapshot to a Python dict with ophyd-compatible metadata keys.
///
/// Keys: value, char_value, status, severity, timestamp, precision, units,
///       lower_ctrl_limit, upper_ctrl_limit, enum_strs
pub fn snapshot_to_pydict(py: Python<'_>, snapshot: &Snapshot) -> PyObject {
    let dict = PyDict::new(py);
    dict.set_item("value", epics_value_to_py(py, &snapshot.value))
        .unwrap();
    dict.set_item("status", snapshot.alarm.status).unwrap();
    dict.set_item("severity", snapshot.alarm.severity).unwrap();
    let ts = wall_time_to_epoch(snapshot.timestamp);
    dict.set_item("timestamp", ts).unwrap();
    // posixseconds: clamp the f64→u64 cast for pre-epoch / clock-skew
    // timestamps. Without the clamp, `ts < 0` wraps to a huge positive
    // u64 (cast saturation behaviour in Rust returns 0 for negative,
    // but historically older toolchains differed — pin the semantics
    // explicitly so the value is monotonic and downstream code can
    // safely treat it as "seconds since epoch").
    let posix_secs: u64 = if ts >= 0.0 { ts as u64 } else { 0 };
    dict.set_item("posixseconds", posix_secs).unwrap();
    let nanos = snapshot.timestamp.subsec_nanos();
    dict.set_item("nanoseconds", nanos).unwrap();

    // char_value: string representation matching pyepics behavior.
    // For enums, resolve to label via enum_strs; for others, format the value.
    let char_value = match &snapshot.value {
        EpicsValue::Enum(idx) => {
            if let Some(ref ei) = snapshot.enums {
                ei.strings
                    .get(*idx as usize)
                    .map(|s| s.as_str_lossy().into_owned())
                    .unwrap_or_else(|| idx.to_string())
            } else {
                idx.to_string()
            }
        }
        EpicsValue::CharArray(v) => {
            let end = v.iter().position(|&b| b == 0).unwrap_or(v.len());
            String::from_utf8_lossy(&v[..end]).into_owned()
        }
        other => format!("{other}"),
    };
    dict.set_item("char_value", char_value).unwrap();

    if let Some(ref disp) = snapshot.display {
        dict.set_item("precision", disp.precision).unwrap();
        dict.set_item("units", disp.units.as_str_lossy()).unwrap();
        dict.set_item("upper_disp_limit", disp.upper_disp_limit)
            .unwrap();
        dict.set_item("lower_disp_limit", disp.lower_disp_limit)
            .unwrap();
        dict.set_item("upper_alarm_limit", disp.upper_alarm_limit)
            .unwrap();
        dict.set_item("lower_alarm_limit", disp.lower_alarm_limit)
            .unwrap();
        dict.set_item("upper_warning_limit", disp.upper_warning_limit)
            .unwrap();
        dict.set_item("lower_warning_limit", disp.lower_warning_limit)
            .unwrap();
    }

    if let Some(ref ctrl) = snapshot.control {
        dict.set_item("upper_ctrl_limit", ctrl.upper_ctrl_limit)
            .unwrap();
        dict.set_item("lower_ctrl_limit", ctrl.lower_ctrl_limit)
            .unwrap();
    }

    if let Some(ref enums) = snapshot.enums {
        let tuple = PyTuple::new(py, enums.strings.iter().map(|s| s.as_str_lossy())).unwrap();
        dict.set_item("enum_strs", tuple).unwrap();
    }

    dict.into_any().unbind()
}
