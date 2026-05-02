"""Datatype-aware value converters for :class:`EpicsRsSignalBackend`.

A converter is selected from the ``datatype`` hint passed to
``epicsrs_signal_*`` factories. It transforms values in three directions:

1. ``to_python(raw, metadata)`` — wire value (already a Python primitive
   or numpy array thanks to the Rust layer) → typed Python value the
   user requested (e.g. force ``int`` even when the IOC returns
   ``np.int64``, resolve enum index to ``Enum`` instance).
2. ``to_wire(value)`` — typed Python value the caller passed in →
   serialisable form epics-rs can put on the wire (mostly identity, but
   ``Enum.value`` is unwrapped, numpy dtypes are normalised).
3. ``datakey_dtype(value)`` — produce ``(dtype, shape, dtype_numpy)``
   for the bluesky :class:`event_model.DataKey`.

Enum converters cache the PV's ``enum_strs`` from the connect-time
metadata read so subsequent ``get_value`` calls do not need a separate
metadata fetch.
"""

from __future__ import annotations

from collections.abc import Sequence as AbcSequence
from enum import Enum
from typing import Any, Mapping, get_args, get_origin

import numpy as np


class Converter:
    """Identity converter — used when no datatype hint is given."""

    def update_metadata(self, metadata: Mapping[str, Any]) -> None:  # noqa: D401
        """Update internal state from connect-time metadata."""

    def to_python(self, raw: Any, _metadata: Mapping[str, Any] | None = None) -> Any:
        return raw

    def to_wire(self, value: Any) -> Any:
        # Unwrap Enum values so the underlying string/int hits the wire.
        if isinstance(value, Enum):
            return value.value
        return value

    def datakey_dtype(self, value: Any) -> tuple[str, list[int], str]:
        return _datakey_dtype_for_value(value)


class _BoolConverter(Converter):
    def to_python(self, raw, _metadata=None):
        if raw is None:
            return None
        if isinstance(raw, str):
            return raw.strip().lower() in ("true", "1", "on", "yes")
        return bool(raw)

    def to_wire(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip().lower() in ("true", "1", "on", "yes")
        return bool(value)

    def datakey_dtype(self, _value):
        return "boolean", [], "|b1"


class _IntConverter(Converter):
    def to_python(self, raw, _metadata=None):
        if raw is None:
            return None
        return int(raw)

    def to_wire(self, value):
        if value is None:
            return None
        if isinstance(value, Enum):
            value = value.value
        return int(value)

    def datakey_dtype(self, _value):
        return "integer", [], np.dtype(np.int64).str


class _FloatConverter(Converter):
    def to_python(self, raw, _metadata=None):
        if raw is None:
            return None
        return float(raw)

    def to_wire(self, value):
        if value is None:
            return None
        if isinstance(value, Enum):
            value = value.value
        return float(value)

    def datakey_dtype(self, _value):
        return "number", [], np.dtype(np.float64).str


class _StrConverter(Converter):
    def to_python(self, raw, _metadata=None):
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return str(raw)

    def to_wire(self, value):
        if value is None:
            return None
        if isinstance(value, Enum):
            return str(value.value)
        return str(value)

    def datakey_dtype(self, _value):
        return "string", [], "|S40"


class _EnumConverter(Converter):
    """Map integer index ↔ enum-string ↔ ``EnumCls`` instance.

    ``enum_strs`` is sourced from metadata when available, with a fallback
    to whatever was cached at connect time.
    """

    def __init__(self, enum_cls: type[Enum]):
        self.enum_cls = enum_cls
        self._cached_strs: list[str] = []

    def update_metadata(self, metadata: Mapping[str, Any]) -> None:
        choices = metadata.get("enum_strs")
        if choices:
            self._cached_strs = list(choices)

    def _strs(self, metadata: Mapping[str, Any] | None) -> list[str]:
        if metadata:
            choices = metadata.get("enum_strs")
            if choices:
                return list(choices)
        return self._cached_strs

    def to_python(self, raw, metadata=None):
        if raw is None:
            return None
        if isinstance(raw, self.enum_cls):
            return raw
        if isinstance(raw, (int, np.integer)):
            strs = self._strs(metadata)
            if 0 <= int(raw) < len(strs):
                raw = strs[int(raw)]
            else:
                # Out-of-range index — return raw int
                return int(raw)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            try:
                return self.enum_cls(raw)
            except ValueError:
                # SubsetEnum: PV may return a value outside our enum.
                return raw
        return raw

    def to_wire(self, value):
        if value is None:
            return None
        if isinstance(value, self.enum_cls):
            return value.value
        if isinstance(value, Enum):
            return value.value
        return str(value)

    def datakey_dtype(self, _value):
        return "string", [], "|S40"


class _NumpyArrayConverter(Converter):
    def __init__(self, dtype: np.dtype | type | None):
        self.dtype: np.dtype | None = np.dtype(dtype) if dtype is not None else None

    def to_python(self, raw, _metadata=None):
        if raw is None:
            return None
        if self.dtype is None:
            return np.asarray(raw)
        return np.asarray(raw, dtype=self.dtype)

    def to_wire(self, value):
        if value is None:
            return None
        if self.dtype is None:
            return np.asarray(value)
        return np.asarray(value, dtype=self.dtype)

    def datakey_dtype(self, value):
        if self.dtype is not None:
            arr = self.to_python(value) if value is not None else None
            shape = list(arr.shape) if arr is not None else [0]
            return "array", shape, self.dtype.str
        if value is None:
            return "array", [0], np.dtype(np.float64).str
        arr = np.asarray(value)
        return "array", list(arr.shape), arr.dtype.str


class _SequenceConverter(Converter):
    def __init__(self, elem: Converter):
        self.elem = elem

    def update_metadata(self, metadata: Mapping[str, Any]) -> None:
        self.elem.update_metadata(metadata)

    def to_python(self, raw, metadata=None):
        if raw is None:
            return None
        return [self.elem.to_python(x, metadata) for x in raw]

    def to_wire(self, value):
        if value is None:
            return None
        return [self.elem.to_wire(x) for x in value]

    def datakey_dtype(self, value):
        # Strings are most common; for numeric Sequence[T] the user
        # should prefer Array1D anyway.
        if value is None:
            return "array", [0], "|S40"
        return "array", [len(value)], "|S40"


def _datakey_dtype_for_value(value: Any) -> tuple[str, list[int], str]:
    """Fallback dtype inference when no datatype hint is given."""
    if isinstance(value, np.ndarray):
        return "array", list(value.shape), value.dtype.str
    if isinstance(value, (list, tuple)):
        arr = np.asarray(value)
        return "array", list(arr.shape), arr.dtype.str
    if isinstance(value, bool):
        return "boolean", [], "|b1"
    if isinstance(value, (int, np.integer)):
        return "integer", [], np.dtype(np.int64).str
    if isinstance(value, (float, np.floating)):
        return "number", [], np.dtype(np.float64).str
    return "string", [], "|S40"


def _array_dtype_from_hint(datatype: Any) -> np.dtype | None:
    """Extract a numpy dtype from ``Array1D[np.float64]`` / similar hints.

    Returns ``None`` if the hint is plain ``np.ndarray`` (no dtype constraint).
    """
    args = get_args(datatype)
    for arg in args:
        # Array1D[np.float64] expands to np.ndarray[Any, np.dtype[np.float64]]
        inner = get_args(arg)
        for sub in inner:
            if isinstance(sub, type) and issubclass(sub, np.generic):
                return np.dtype(sub)
    return None


def make_converter(datatype: Any) -> Converter:
    """Pick the right converter for the given datatype hint."""
    if datatype is None:
        return Converter()
    if datatype is bool:
        return _BoolConverter()
    if datatype is int:
        return _IntConverter()
    if datatype is float:
        return _FloatConverter()
    if datatype is str:
        return _StrConverter()

    # Enum subclasses (StrictEnum / SubsetEnum / SupersetEnum / plain Enum)
    if isinstance(datatype, type) and issubclass(datatype, Enum):
        return _EnumConverter(datatype)

    # numpy.ndarray (with or without dtype argument)
    origin = get_origin(datatype)
    if datatype is np.ndarray or origin is np.ndarray:
        return _NumpyArrayConverter(_array_dtype_from_hint(datatype))

    # Sequence[T] (typing.Sequence / collections.abc.Sequence)
    if origin in (AbcSequence,):
        args = get_args(datatype)
        elem_type = args[0] if args else str
        return _SequenceConverter(make_converter(elem_type))

    # Anything else (Table, custom classes, etc.) — passthrough.
    return Converter()
