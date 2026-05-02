"""Unit tests for ophyd_epicsrs.detector._converter.

Offline (no IOC required). Covers the three converter dimensions that
have caused the most past regressions: enum cache state machine,
typed array dispatch, Table dtype-hint extraction + schema validation.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pytest
from ophyd_async.core import Array1D, StrictEnum, SubsetEnum, SupersetEnum, Table

from ophyd_epicsrs.detector._converter import (
    Converter,
    _BoolConverter,
    _decode_char_array,
    _EnumConverter,
    _FloatConverter,
    _IntConverter,
    _NumpyArrayConverter,
    _SequenceConverter,
    _StrConverter,
    _TableConverter,
    _is_typed_pvfield_payload,
    make_converter,
)


# ---------- Dispatch ----------


class _DispatchEnum(StrictEnum):
    A = "A"
    B = "B"


class _DispatchTable(Table):
    a: Array1D[np.int8]
    b: Sequence[str]


@pytest.mark.parametrize(
    "datatype,expected",
    [
        (None, Converter),
        (bool, _BoolConverter),
        (int, _IntConverter),
        (float, _FloatConverter),
        (str, _StrConverter),
        (_DispatchEnum, _EnumConverter),
        (Array1D[np.float64], _NumpyArrayConverter),
        (np.ndarray, _NumpyArrayConverter),
        (Sequence[str], _SequenceConverter),
        (_DispatchTable, _TableConverter),
    ],
)
def test_make_converter_dispatch(datatype, expected):
    assert isinstance(make_converter(datatype), expected)


# ---------- Bool / Int / Float / Str ----------


@pytest.mark.parametrize(
    "raw,expected",
    [(0, False), (1, True), ("true", True), ("FALSE", False), ("on", True), ("0", False)],
)
def test_bool_to_python(raw, expected):
    assert _BoolConverter().to_python(raw) is expected


def test_int_coerces_numpy_and_str():
    c = _IntConverter()
    assert c.to_python(3.7) == 3
    assert c.to_python(np.int64(5)) == 5
    assert c.to_python("42") == 42
    assert c.to_wire("9") == 9


def test_float_coerces_numpy_and_int():
    c = _FloatConverter()
    assert c.to_python(5) == 5.0
    assert isinstance(c.to_python(5), float)
    assert c.to_python(np.float32(2.5)) == 2.5


def test_str_decodes_bytes_and_long_string():
    c = _StrConverter()
    assert c.to_python(b"hello") == "hello"
    assert c.to_python("plain") == "plain"
    # CA char waveform: list[u8] with null terminator
    assert c.to_python([0x4F, 0x4E, 0x00]) == "ON"
    assert c.to_python([0x61, 0x62, 0x63]) == "abc"  # no null → use full length


def test_decode_char_array_rejects_non_bytes():
    assert _decode_char_array([1000, 2000]) is None  # out of byte range
    assert _decode_char_array([0x61, "x"]) is None    # mixed types
    assert _decode_char_array([]) is None             # empty


# ---------- Enum ----------


def test_enum_strict_match():
    class Mode(StrictEnum):
        ON = "On"
        OFF = "Off"

    c = _EnumConverter(Mode)
    c.update_metadata({"enum_strs": ("On", "Off")}, source="ca://X")
    assert isinstance(c.to_python(0), Mode)
    assert c.to_python(0) == Mode.ON
    assert c.to_python(1) == Mode.OFF


def test_enum_strict_mismatch_raises():
    class Mode(StrictEnum):
        ON = "On"
        OFF = "Off"

    c = _EnumConverter(Mode)
    with pytest.raises(TypeError, match="strictly equal"):
        c.update_metadata({"enum_strs": ("On", "Off", "Standby")}, source="ca://X")


def test_enum_subset_passthrough_for_extra_choices():
    class Mode(SubsetEnum):
        ON = "On"

    c = _EnumConverter(Mode)
    c.update_metadata({"enum_strs": ("On", "Off", "Standby")}, source="ca://X")
    # SubsetEnum returns plain strings (StrEnum equality works)
    assert c.to_python("On") == "On" == Mode.ON
    assert c.to_python("Off") == "Off"  # PV-only value


def test_enum_superset_violation_raises():
    class Mode(SupersetEnum):
        ON = "On"
        OFF = "Off"

    c = _EnumConverter(Mode)
    with pytest.raises(TypeError, match="superset"):
        c.update_metadata({"enum_strs": ("Unknown",)}, source="ca://X")


def test_enum_to_wire_label_to_index():
    class Mode(StrictEnum):
        ON = "On"
        OFF = "Off"

    c = _EnumConverter(Mode)
    c.update_metadata({"enum_strs": ("On", "Off")}, source="ca://X")
    assert c.to_wire(Mode.ON) == 0
    assert c.to_wire(Mode.OFF) == 1
    assert c.to_wire("On") == 0
    assert c.to_wire(1) == 1  # int passthrough


def test_enum_to_wire_empty_cache_raises():
    """Critical: silent label→wire would surface as cryptic CA TypeError."""

    class Mode(StrictEnum):
        ON = "On"
        OFF = "Off"

    c = _EnumConverter(Mode)
    # cache empty (no update_metadata)
    with pytest.raises(RuntimeError, match="enum_strs cache empty"):
        c.to_wire(Mode.ON)
    # int is fine without cache
    assert c.to_wire(5) == 5


def test_enum_to_wire_unknown_label_raises():
    class Mode(StrictEnum):
        ON = "On"
        OFF = "Off"

    c = _EnumConverter(Mode)
    c.update_metadata({"enum_strs": ("On", "Off")}, source="ca://X")
    with pytest.raises(ValueError, match="not a valid choice"):
        c.to_wire("Unknown")


def test_enum_ntenum_dict_to_python():
    """PVA NTEnum surfaces as {index, choices} dict from get_value_async."""

    class Mode(StrictEnum):
        ON = "On"
        OFF = "Off"

    c = _EnumConverter(Mode)
    assert c.to_python({"index": 0, "choices": ["On", "Off"]}) == Mode.ON
    assert c.to_python({"index": 1, "choices": ["On", "Off"]}) == Mode.OFF


# ---------- Numpy array ----------


def test_numpy_array_typed_dtype():
    c = _NumpyArrayConverter(np.int16)
    out = c.to_python([1, 2, 3])
    assert isinstance(out, np.ndarray)
    assert out.dtype == np.int16
    assert (out == np.array([1, 2, 3], dtype=np.int16)).all()


def test_numpy_array_untyped_inference():
    c = _NumpyArrayConverter(None)
    out = c.to_python([1.0, 2.0])
    assert out.dtype == np.float64


# ---------- Sequence ----------


def test_sequence_str():
    c = _SequenceConverter(_StrConverter())
    assert c.to_python([1, 2, 3]) == ["1", "2", "3"]


def test_sequence_update_metadata_accepts_source_kwarg():
    """Regression: previously the override didn't accept `source=...`."""
    c = make_converter(Sequence[str])
    c.update_metadata({"enum_strs": ()}, source="ca://X")  # must not raise


# ---------- Table dtype hints ----------


def test_table_dtype_hints_from_annotations():
    class T(Table):
        a: Array1D[np.int8]
        b: Array1D[np.float64]
        c: Sequence[str]

    c = make_converter(T)
    assert c._column_dtypes == {
        "a": "|i1",
        "b": "<f8",
        "c": "string",
    }


def test_table_to_wire_marker_payload():
    class T(Table):
        a: Array1D[np.int8]
        b: Sequence[str]

    c: _TableConverter = make_converter(T)
    table = T(a=[1, 2], b=["x", "y"])
    wire = c.to_wire(table)
    assert _is_typed_pvfield_payload(wire)
    assert wire["struct_id"] == "epics:nt/NTTable:1.0"
    assert wire["dtypes"] == {"a": "|i1", "b": "string"}
    assert wire["data"]["b"] == ["x", "y"]


def test_table_empty_preserves_dtype_hints():
    class T(Table):
        a: Array1D[np.int8]
        b: Sequence[str]

    c: _TableConverter = make_converter(T)
    wire = c.to_wire(T.empty())
    # Empty columns + dtype hints → typed PvField construction can still
    # pick the right ScalarArrayTyped variant.
    assert wire["dtypes"]["a"] == "|i1"
    assert wire["dtypes"]["b"] == "string"


# ---------- Table schema validation ----------


def _matching_schema():
    return {
        "kind": "structure",
        "struct_id": "epics:nt/NTTable:1.0",
        "fields": [
            ("labels", {"kind": "scalar_array", "scalar_type": "string"}),
            ("value", {
                "kind": "structure", "struct_id": "",
                "fields": [
                    ("a", {"kind": "scalar_array", "scalar_type": "byte"}),
                    ("b", {"kind": "scalar_array", "scalar_type": "string"}),
                ],
            }),
        ],
    }


def test_schema_validation_match():
    class T(Table):
        a: Array1D[np.int8]
        b: Sequence[str]

    make_converter(T).validate_against_schema(_matching_schema(), source="pva://X")


def test_schema_validation_missing_column():
    class T(Table):
        a: Array1D[np.int8]
        b: Sequence[str]
        extra: Array1D[np.float64]  # not in IOC

    with pytest.raises(TypeError, match="extra"):
        make_converter(T).validate_against_schema(
            _matching_schema(), source="pva://MISSING"
        )


def test_schema_validation_dtype_mismatch():
    class T(Table):
        a: Array1D[np.int32]   # IOC has byte (|i1)
        b: Sequence[str]

    with pytest.raises(TypeError, match="does not match IOC scalar_type"):
        make_converter(T).validate_against_schema(
            _matching_schema(), source="pva://WRONG"
        )


def test_schema_validation_bare_structured_pv():
    class T(Table):
        a: Array1D[np.int8]
        b: Sequence[str]

    bare = {
        "kind": "structure",
        "struct_id": "",
        "fields": [
            ("a", {"kind": "scalar_array", "scalar_type": "byte"}),
            ("b", {"kind": "scalar_array", "scalar_type": "string"}),
        ],
    }
    # Bare PV (no NTTable wrapper) — top-level fields are columns.
    make_converter(T).validate_against_schema(bare, source="pva://BARE")


def test_schema_validation_skips_non_structured():
    class T(Table):
        a: Array1D[np.int8]

    scalar = {"kind": "scalar", "scalar_type": "double"}
    # Non-structured target → silent skip rather than false alarm
    make_converter(T).validate_against_schema(scalar, source="pva://SCALAR")
