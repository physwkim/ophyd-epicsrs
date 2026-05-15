"""Unit tests for ophyd_epicsrs.ophyd_async._signal_backend.

Offline (no IOC required). Uses unittest.mock to stub the native PV
async methods so connect/put/get_datakey paths are exercised end-to-end
without any CA/PVA traffic.

Covers the behavioural fixes:
- put(None) reads from `_initial_write_value` cache populated at connect
- set_callback raises RuntimeError when invoked without a running loop
- get_datakey skips units/precision/limits per datatype
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from ophyd_epicsrs.ophyd_async._signal_backend import (
    EpicsRsProtocol,
    EpicsRsSignalBackend,
)


def _make_backend(
    datatype,
    *,
    read_pv: str = "TEST:READ",
    write_pv: str | None = None,
    wait=True,
    protocol: EpicsRsProtocol = EpicsRsProtocol.CA,
):
    """Construct a backend with both native PVs replaced by mocks.

    The mocks accept every method the backend uses; tests configure
    individual return values per scenario.
    """
    from ophyd_async.epics.core._util import EpicsOptions

    backend = EpicsRsSignalBackend(
        datatype,
        read_pv=read_pv,
        write_pv=write_pv or read_pv,
        options=EpicsOptions(wait=wait),
        protocol=protocol,
    )
    def _native_mock():
        m = MagicMock()
        m.connect_async = AsyncMock(return_value=True)
        m.cache_native_type_async = AsyncMock(return_value=True)
        m.get_field_desc_async = AsyncMock(return_value=None)
        m.get_reading_async = AsyncMock(return_value={"value": 0.0})
        m.get_value_async = AsyncMock(return_value=0.0)
        m.put_async = AsyncMock(return_value=None)
        m.put_nowait_async = AsyncMock(return_value=None)
        m.clear_monitors = MagicMock()
        m.set_monitor_callback = MagicMock()
        return m

    if write_pv:
        backend._read_pv_native = _native_mock()
        backend._write_pv_native = _native_mock()
    else:
        # Default: read_pv == write_pv. The backend's __init__ aliases
        # both slots to the same object; mirror that here so a put_async
        # configured on one is visible from the other (matches real
        # singleton-context behaviour).
        shared = _native_mock()
        backend._read_pv_native = shared
        backend._write_pv_native = shared
    return backend


# ---------- #1: initial_write_value cache ----------


async def test_connect_caches_initial_write_value():
    backend = _make_backend(float)
    backend._write_pv_native.get_value_async = AsyncMock(return_value=3.14)
    await backend.connect(timeout=1.0)
    assert backend._initial_write_value == 3.14


async def test_put_none_uses_cached_initial_value():
    backend = _make_backend(float)
    backend._write_pv_native.get_value_async = AsyncMock(return_value=2.71)
    await backend.connect(timeout=1.0)
    # Replace with a fresh AsyncMock to prove put(None) does NOT call it.
    fresh_get = AsyncMock(return_value=999.0)
    backend._write_pv_native.get_value_async = fresh_get
    await backend.put(None)
    backend._write_pv_native.put_async.assert_awaited_once_with(2.71)
    fresh_get.assert_not_called()


async def test_put_none_falls_back_to_fetch_when_cache_missing():
    """If the connect-time snapshot failed, put(None) issues a fresh get."""
    backend = _make_backend(float)
    # Simulate connect-time fetch failure → cache stays None.
    backend._write_pv_native.get_value_async = AsyncMock(side_effect=RuntimeError("boom"))
    await backend.connect(timeout=1.0)
    assert backend._initial_write_value is None
    # Subsequent put(None): the fallback get_value_async must succeed
    # (configure a fresh mock that returns a value).
    backend._write_pv_native.get_value_async = AsyncMock(return_value=99.0)
    await backend.put(None)
    backend._write_pv_native.put_async.assert_awaited_once_with(99.0)


# ---------- #3: set_callback loop=None guard ----------


def test_set_callback_without_running_loop_raises():
    backend = _make_backend(float)
    # No `async def` wrapper here → no running loop. The backend's
    # set_callback used to silently route through the Rust dispatch
    # thread (UB on asyncio Event mutation); it must now raise.
    with pytest.raises(RuntimeError, match="running asyncio loop"):
        backend.set_callback(lambda r: None)


async def test_set_callback_inside_loop_registers():
    backend = _make_backend(float)
    backend.set_callback(lambda r: None)
    # The Rust side received the wrapped callback. We just verify the
    # registration path didn't raise — actual delivery requires an IOC.
    backend._read_pv_native.set_monitor_callback.assert_called_once()


async def test_set_callback_none_clears():
    backend = _make_backend(float)
    backend.set_callback(None)
    backend._read_pv_native.clear_monitors.assert_called_once()
    assert backend._monitor_callback is None


async def test_set_callback_raises_when_already_set():
    backend = _make_backend(float)
    backend.set_callback(lambda r: None)
    with pytest.raises(RuntimeError, match="already set"):
        backend.set_callback(lambda r: None)


# ---------- #4: get_datakey skip rules ----------


async def _connect_with_md(backend, md):
    backend._read_pv_native.get_reading_async = AsyncMock(return_value=md)
    await backend.connect(timeout=1.0)


async def test_get_datakey_skips_units_for_str():
    backend = _make_backend(str)
    md = {"value": "hello", "units": "mm", "precision": 3}
    await _connect_with_md(backend, md)
    backend._read_pv_native.get_reading_async = AsyncMock(return_value=md)
    dk = await backend.get_datakey("ca://X")
    assert "units" not in dk


async def test_get_datakey_skips_units_for_bool():
    backend = _make_backend(bool)
    md = {"value": True, "units": "on/off"}
    await _connect_with_md(backend, md)
    backend._read_pv_native.get_reading_async = AsyncMock(return_value=md)
    dk = await backend.get_datakey("ca://X")
    assert "units" not in dk


async def test_get_datakey_skips_precision_for_int():
    backend = _make_backend(int)
    md = {"value": 5, "precision": 4, "units": "counts"}
    await _connect_with_md(backend, md)
    backend._read_pv_native.get_reading_async = AsyncMock(return_value=md)
    dk = await backend.get_datakey("ca://X")
    assert "precision" not in dk
    # Units still emitted for int.
    assert dk.get("units") == "counts"


async def test_get_datakey_skips_limits_for_bool():
    backend = _make_backend(bool)
    md = {
        "value": False,
        "lower_disp_limit": 0,
        "upper_disp_limit": 1,
        "lower_ctrl_limit": 0,
        "upper_ctrl_limit": 1,
    }
    await _connect_with_md(backend, md)
    backend._read_pv_native.get_reading_async = AsyncMock(return_value=md)
    dk = await backend.get_datakey("ca://X")
    assert "limits" not in dk


async def test_get_datakey_emits_units_for_float():
    backend = _make_backend(float)
    md = {"value": 1.0, "units": "mm", "precision": 3}
    await _connect_with_md(backend, md)
    backend._read_pv_native.get_reading_async = AsyncMock(return_value=md)
    dk = await backend.get_datakey("ca://X")
    assert dk["units"] == "mm"
    assert dk["precision"] == 3


async def test_get_datakey_emits_choices_for_enum():
    backend = _make_backend(int)
    md = {"value": 0, "enum_strs": ["A", "B", "C"]}
    await _connect_with_md(backend, md)
    backend._read_pv_native.get_reading_async = AsyncMock(return_value=md)
    dk = await backend.get_datakey("ca://X")
    assert dk["choices"] == ["A", "B", "C"]
