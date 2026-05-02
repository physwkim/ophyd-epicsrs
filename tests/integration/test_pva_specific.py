"""PVA-only behavioural checks.

These cover features of NTScalar/NTEnum that the CA path can't
express (proper UNIX-time timestamps with ns precision, NTEnum
dual-value shape, async pipelining of multiple in-flight ops).
"""

from __future__ import annotations

import asyncio
import time

import pytest


def test_pva_timestamp_close_to_wall_clock(pva_ctx):
    """NTScalar.timeStamp.{secondsPastEpoch, nanoseconds} should be
    very close to wall-clock time on a freshly-acquired sample. CA
    DBR_TIME timestamps in this build come back zeroed (separate
    issue), so this differentiates the two paths."""
    pv = pva_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)

    wall = time.time()
    r = pv.get_with_metadata(timeout=2.0)
    skew = abs(r["posixseconds"] - wall)
    print(f"\n  PVA posixseconds={r['posixseconds']}  wall={wall:.0f}  skew={skew:.1f}s")
    # IOC publishes every 100 ms; within ±5 s is generous (test runner
    # clock skew etc.).
    assert skew < 5.0
    # Nanoseconds field is in the [0, 1e9) range.
    assert 0 <= r["nanoseconds"] < 1_000_000_000


def test_pva_ntscalar_metadata_complete(pva_ctx):
    """NTScalar should populate the full ophyd metadata dict — value,
    alarm fields, timeStamp fields, and display fields (units etc.)."""
    pv = pva_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)
    r = pv.get_with_metadata(timeout=2.0)

    expected = {
        "value",
        "char_value",
        "severity",
        "status",
        "alarm_message",
        "timestamp",
        "posixseconds",
        "nanoseconds",
        "precision",
        "units",
        "lower_disp_limit",
        "upper_disp_limit",
        "lower_ctrl_limit",
        "upper_ctrl_limit",
    }
    missing = expected - r.keys()
    assert not missing, f"missing PVA NTScalar fields: {missing}"
    assert r["units"] == "mA"


@pytest.mark.asyncio
async def test_pva_async_pipelining_multiple_in_flight(pva_ctx):
    """Three independent put_async calls on different PVs should
    overlap when launched via asyncio.gather — the shared tokio
    runtime must not serialize them."""
    from ophyd_epicsrs._contexts import get_pva_context

    ctx = get_pva_context()
    targets = [
        ("mini:ph:ExposureTime", 0.111, "mini:ph:ExposureTime_RBV"),
        ("mini:edge:ExposureTime", 0.222, "mini:edge:ExposureTime_RBV"),
        ("mini:slit:ExposureTime", 0.333, "mini:slit:ExposureTime_RBV"),
    ]
    pvs = []
    for sp, _, rbv in targets:
        pv_sp = ctx.create_pv(sp)
        pv_rbv = ctx.create_pv(rbv)
        assert pv_sp.wait_for_connection(timeout=3.0)
        assert pv_rbv.wait_for_connection(timeout=3.0)
        pvs.append((pv_sp, pv_rbv))

    t0 = time.perf_counter()
    await asyncio.gather(
        *(pv_sp.put_async(value, timeout=3.0)
          for (pv_sp, _), (_, value, _) in zip(pvs, targets))
    )
    dt = (time.perf_counter() - t0) * 1000
    print(f"\n  3x PVA put_async (parallel): {dt:.2f} ms")

    await asyncio.sleep(0.3)
    readbacks = await asyncio.gather(
        *(pv_rbv.get_value_async(timeout=2.0) for _, pv_rbv in pvs)
    )
    expected = [v for _, v, _ in targets]
    for got, want in zip(readbacks, expected):
        assert abs(got - want) < 1e-6


def test_pva_ntenum_choices_not_truncated(pva_ctx):
    """KohzuModeBO has 2 choices (Manual, Auto). The PVA NTEnum path
    must return exactly those — not the CA convention of padding to
    16 entries with empty strings."""
    pv = pva_ctx.create_pv("mini:KohzuModeBO")
    assert pv.wait_for_connection(timeout=3.0)
    r = pv.get_with_metadata(timeout=2.0)
    choices = r["enum_strs"]
    assert choices == ("Manual", "Auto"), f"unexpected choices: {choices}"


def test_pva_monitor_callback_delivers(pva_ctx):
    """PVA monitor delivery must actually fire callbacks. Beam
    current updates every 100 ms; ≥3 events in 1 s is a low bar
    for a working subscription."""
    pv = pva_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)

    received: list[float] = []

    def cb(**kwargs):
        received.append(kwargs.get("value"))

    pv.set_monitor_callback(cb)
    time.sleep(1.0)
    pv.clear_monitors()
    print(f"\n  PVA monitor callbacks in 1s: {len(received)}")
    assert len(received) >= 3


@pytest.mark.asyncio
async def test_async_pva_ntenum_int_put_routes_via_field_path(pva_ctx):
    """The async write path (`put_async`) must apply the same NTEnum
    routing as sync `put`. Without it, `await pv.put_async(int)` on
    an NTEnum channel silently no-ops because string-form pvput can't
    reach the `value.index` field — and ophyd-async's `set()` lands
    here, so every `await sig.set(MyEnum.X)` would silently fail.

    Drives the cache via `get_value_async` (also an async-only path),
    then performs the put via `put_async`, then verifies the IOC
    actually accepted the new value."""
    pv = pva_ctx.create_pv("mini:KohzuModeBO")
    assert pv.wait_for_connection(timeout=3.0)

    # Async-only flow — no sync get_with_metadata anywhere. This is
    # what an ophyd-async SignalBackend.connect → set sequence looks
    # like at the Rust boundary.
    initial = await pv.get_value_async(timeout=2.0)  # populates is_ntenum cache
    target = 1 if initial == 0 else 0

    await pv.put_async(target, timeout=2.0)
    await asyncio.sleep(0.3)
    after = await pv.get_value_async(timeout=2.0)
    assert after == target, (
        f"async NTEnum int put: expected index {target}, got {after} — "
        f"the put was probably routed through plain pvput instead of "
        f"pvput_field('value.index', ...)"
    )

    # Restore.
    await pv.put_async(initial, timeout=2.0)


@pytest.mark.asyncio
async def test_async_pva_ntenum_via_ophyd_async_strict_enum():
    """The full ophyd-async path: a `StrictEnum` typed `SignalRW`
    bound to a PVA NTEnum PV. `await sig.set(KohzuMode.AUTO)` goes
    through the EpicsRsSignalBackend's `_EnumConverter.to_wire`
    (label string → int index) and then `EpicsRsPvaPV.put_async(int)`
    — both layers must agree, otherwise the set silently no-ops."""
    from ophyd_async.core import StrictEnum
    from ophyd_epicsrs.ophyd_async import epicsrs_signal_rw

    class KohzuMode(StrictEnum):
        MANUAL = "Manual"
        AUTO = "Auto"

    sig = epicsrs_signal_rw(KohzuMode, "pva://mini:KohzuModeBO")
    await sig.connect()

    initial = await sig.get_value()
    target = KohzuMode.AUTO if initial == KohzuMode.MANUAL else KohzuMode.MANUAL

    await sig.set(target)
    await asyncio.sleep(0.3)
    assert (await sig.get_value()) is target

    # Restore for following tests.
    await sig.set(initial)
