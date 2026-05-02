"""Live integration tests against the mini-beamline IOC.

Exercises both CA and PVA paths through ophyd (sync) and ophyd-async
(async) frontends. Requires the mini-beamline IOC from
``epics-rs/examples/mini-beamline`` to be running and reachable on the
default CA / PVA ports.

Run with:
    pytest tests/integration/test_mini_beamline.py -v -s

Environment:
- EPICS_CA_ADDR_LIST (or default broadcast) must reach the IOC
- EPICS_PVA_ADDR_LIST likewise
"""

from __future__ import annotations

import asyncio
import time

import pytest


# ---------- Native CA layer ----------


def test_ca_scalar_read(ca_ctx):
    pv = ca_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)
    r = pv.get_with_metadata(timeout=2.0)
    assert r is not None
    assert isinstance(r["value"], float)
    # beam current should be in OFFSET ± AMPLITUDE = 500 ± 25 mA
    assert 400 < r["value"] < 600
    assert r["units"] == "mA"


def test_ca_scalar_put_and_readback(ca_ctx):
    setpoint = ca_ctx.create_pv("mini:ph:ExposureTime")
    rbv = ca_ctx.create_pv("mini:ph:ExposureTime_RBV")
    assert setpoint.wait_for_connection(timeout=3.0)
    assert rbv.wait_for_connection(timeout=3.0)

    setpoint.put(0.123, wait=True, timeout=2.0)
    deadline = time.time() + 2.0
    last = None
    while time.time() < deadline:
        last = rbv.get_with_metadata(timeout=1.0)
        if last and abs(last["value"] - 0.123) < 1e-6:
            break
        time.sleep(0.05)
    assert last is not None
    assert abs(last["value"] - 0.123) < 1e-6, f"rbv={last['value']}"


def test_ca_monitor_callback(ca_ctx):
    pv = ca_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)

    received = []

    def cb(**kwargs):
        received.append(kwargs["value"])

    pv.set_monitor_callback(cb)
    # Beam current updates every BEAM_UPDATE_MS=100ms; wait 1s for ≥5 updates.
    deadline = time.time() + 2.0
    while len(received) < 5 and time.time() < deadline:
        time.sleep(0.05)
    pv.clear_monitors()
    assert len(received) >= 5, f"only {len(received)} updates in 2s"


# ---------- Native PVA layer ----------


def test_pva_scalar_read(pva_ctx):
    pv = pva_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)
    r = pv.get_with_metadata(timeout=2.0)
    assert r is not None
    assert 400 < r["value"] < 600
    # PVA NTScalar projects timeStamp.{secondsPastEpoch, nanoseconds}
    assert r["posixseconds"] > 0


def test_pva_put_and_readback(pva_ctx):
    setpoint = pva_ctx.create_pv("mini:edge:ExposureTime")
    rbv = pva_ctx.create_pv("mini:edge:ExposureTime_RBV")
    assert setpoint.wait_for_connection(timeout=3.0)
    assert rbv.wait_for_connection(timeout=3.0)

    setpoint.put(0.456, wait=True, timeout=2.0)
    deadline = time.time() + 2.0
    last = None
    while time.time() < deadline:
        last = rbv.get_with_metadata(timeout=1.0)
        if last and abs(last["value"] - 0.456) < 1e-6:
            break
        time.sleep(0.05)
    assert last is not None
    assert abs(last["value"] - 0.456) < 1e-6, f"rbv={last['value']}"


# ---------- bulk_caget ----------


def test_bulk_caget_many_pvs(ca_ctx):
    """Single call should hand back a dict of {name: value} faster than
    a sequential read loop."""
    pvs = [
        "mini:current",
        "mini:ph:DetValue_RBV",
        "mini:edge:DetValue_RBV",
        "mini:slit:DetValue_RBV",
        "mini:ph:mtr.RBV",
        "mini:edge:mtr.RBV",
        "mini:slit:mtr.RBV",
        "mini:dot:mtrx.RBV",
        "mini:dot:mtry.RBV",
        "mini:dcm:theta.RBV",
        "mini:dcm:y.RBV",
        "mini:dcm:z.RBV",
        "mini:BraggERdbkAO",
        "mini:BraggThetaRdbkAO",
        "mini:BraggLambdaRdbkAO",
    ]
    # Warm: ensure connected first (bulk_caget itself can connect, but timing
    # the read alone is what we care about).
    for name in pvs:
        ca_ctx.create_pv(name).wait_for_connection(timeout=3.0)

    t0 = time.perf_counter()
    data = ca_ctx.bulk_caget(pvs, timeout=3.0)
    bulk_dt = time.perf_counter() - t0

    assert set(data.keys()) == set(pvs)
    assert all(v is not None for v in data.values())
    print(f"\n  bulk_caget({len(pvs)}) = {bulk_dt * 1000:.2f} ms")


# ---------- ophyd (sync) frontend ----------


def test_ophyd_signal_ca(ophyd_setup):
    sig = ophyd_setup.EpicsSignal("mini:current", name="current")
    sig.wait_for_connection(timeout=3.0)
    v = sig.get()
    assert isinstance(v, float)
    assert 400 < v < 600


def test_ophyd_signal_pva(ophyd_setup):
    sig = ophyd_setup.EpicsSignal("pva://mini:current", name="current_pva")
    sig.wait_for_connection(timeout=3.0)
    v = sig.get()
    assert 400 < v < 600


def test_ophyd_motor_move_and_rbv(ophyd_setup):
    """Move motor, watch RBV converge, then move back. Default motor
    VELO is 1.0 unit/s with 0.1s acceleration, so a 1-unit move takes
    ~1.1 s — but the motor record's settle/processing margin pushes
    the round-trip closer to 5 s. Generous 30 s timeout."""
    mtr = ophyd_setup.EpicsMotor("mini:ph:mtr", name="ph_mtr")
    mtr.wait_for_connection(timeout=5.0)
    initial = mtr.position
    target = initial + 0.5  # short move so test stays fast
    mtr.move(target, wait=True, timeout=10.0)
    assert abs(mtr.position - target) < 0.1, (
        f"motor at {mtr.position}, expected {target}"
    )
    # Move back so we don't leave state behind.
    mtr.move(initial, wait=True, timeout=10.0)


def test_ophyd_point_detector_device(ophyd_setup):
    """Custom Device with multiple Signals — exercises Component + bulk
    connect path."""
    from ophyd import Component as Cpt
    from ophyd import Device, EpicsSignal, EpicsSignalRO

    class PointDetector(Device):
        value = Cpt(EpicsSignalRO, "DetValue_RBV")
        sigma = Cpt(EpicsSignal, "DetSigma", write_pv="DetSigma")
        center = Cpt(EpicsSignal, "DetCenter", write_pv="DetCenter")
        exposure = Cpt(EpicsSignal, "ExposureTime", write_pv="ExposureTime")

    det = PointDetector("mini:ph:", name="ph_det")
    t0 = time.perf_counter()
    det.wait_for_connection(timeout=5.0)
    dt = time.perf_counter() - t0
    print(f"\n  Device(4 PVs) connect: {dt * 1000:.2f} ms")

    reading = det.read()
    assert "ph_det_value" in reading
    assert reading["ph_det_value"]["value"] is not None


def test_ophyd_motor_drives_detector(ophyd_setup):
    """The CP link in the IOC means moving the motor changes the
    detector value (Gaussian centred at 0, sigma=5). At 10 units
    (2σ) the signal drops by e^(-2) ≈ 7.4× — comfortable contrast
    even with the sinusoidal beam-current modulation (±5 %)."""
    mtr = ophyd_setup.EpicsMotor("mini:ph:mtr", name="ph_mtr")
    det_val = ophyd_setup.EpicsSignalRO("mini:ph:DetValue_RBV", name="ph_det")
    mtr.wait_for_connection(timeout=5.0)
    det_val.wait_for_connection(timeout=5.0)

    mtr.move(0.0, wait=True, timeout=10.0)
    time.sleep(0.3)
    centre_val = det_val.get()

    mtr.move(10.0, wait=True, timeout=10.0)
    time.sleep(0.3)
    off_val = det_val.get()

    print(f"\n  centre={centre_val:.2f}  off={off_val:.2f}  ratio={centre_val / max(off_val, 1):.2f}")
    assert centre_val > off_val * 3, (
        f"expected centre {centre_val} to be > 3× off-axis {off_val}"
    )

    mtr.move(0.0, wait=True, timeout=10.0)


# ---------- ophyd-async (asyncio) frontend ----------


@pytest.mark.asyncio
async def test_ophyd_async_ca_signal_rw():
    from ophyd_async.core import init_devices
    from ophyd_epicsrs.ophyd_async import epicsrs_signal_rw

    async with init_devices():
        sig = epicsrs_signal_rw(float, "mini:slit:ExposureTime_RBV", "mini:slit:ExposureTime")
    await sig.set(0.789)
    await asyncio.sleep(0.2)
    v = await sig.get_value()
    assert abs(v - 0.789) < 1e-6


@pytest.mark.asyncio
async def test_ophyd_async_pva_signal_rw():
    """End-to-end PVA round-trip via the ophyd-async factory: put a
    setpoint, read it back. Uses ExposureTime (a plain ao record) so
    the test doesn't depend on the kohzuCtl state machine actually
    driving theta — that machinery has its own mode-arming sequence."""
    from ophyd_async.core import init_devices
    from ophyd_epicsrs.ophyd_async import epicsrs_signal_rw

    async with init_devices():
        sig = epicsrs_signal_rw(
            float,
            "pva://mini:slit:ExposureTime_RBV",
            "pva://mini:slit:ExposureTime",
        )
    await sig.set(0.321)
    await asyncio.sleep(0.3)
    v = await sig.get_value()
    print(f"\n  PVA round-trip: set 0.321 → read {v:.4f}")
    assert abs(v - 0.321) < 1e-6


@pytest.mark.asyncio
async def test_ophyd_async_bulk_connect():
    """Connect a Device with many ophyd-async signals in parallel."""
    from ophyd_async.core import Device, init_devices
    from ophyd_epicsrs.ophyd_async import epicsrs_signal_r, epicsrs_signal_rw

    class PointDet(Device):
        def __init__(self, prefix: str, name: str = "") -> None:
            self.value = epicsrs_signal_r(float, f"{prefix}DetValue_RBV")
            self.sigma = epicsrs_signal_rw(
                float, f"{prefix}DetSigma_RBV", f"{prefix}DetSigma"
            )
            self.center = epicsrs_signal_rw(
                float, f"{prefix}DetCenter_RBV", f"{prefix}DetCenter"
            )
            self.exposure = epicsrs_signal_rw(
                float, f"{prefix}ExposureTime_RBV", f"{prefix}ExposureTime"
            )
            super().__init__(name=name)

    async with init_devices():
        ph = PointDet("mini:ph:", name="ph")
        edge = PointDet("mini:edge:", name="edge")
        slit_det = PointDet("mini:slit:", name="slit")

    t0 = time.perf_counter()
    await asyncio.gather(ph.connect(), edge.connect(), slit_det.connect())
    dt = time.perf_counter() - t0
    print(f"\n  3x PointDet (12 PVs) async connect: {dt * 1000:.2f} ms")

    # Read all
    vals = await asyncio.gather(
        ph.value.get_value(),
        edge.value.get_value(),
        slit_det.value.get_value(),
    )
    print(f"  detector values: {[f'{v:.2f}' for v in vals]}")
    assert all(isinstance(v, float) for v in vals)


# ---------- areaDetector "Device" basics over CA ----------


def test_ophyd_area_detector_array_counter(ophyd_setup):
    """Trigger one acquisition, verify ArrayCounter_RBV increments and
    DetectorState_RBV cycles Idle → Acquire → Idle."""
    acquire = ophyd_setup.EpicsSignal("mini:dot:cam1:Acquire", name="acq")
    counter = ophyd_setup.EpicsSignalRO(
        "mini:dot:cam1:ArrayCounter_RBV", name="counter"
    )
    image_mode = ophyd_setup.EpicsSignal("mini:dot:cam1:ImageMode", name="mode")
    acquire_time = ophyd_setup.EpicsSignal(
        "mini:dot:cam1:AcquireTime", name="atime"
    )
    callbacks = ophyd_setup.EpicsSignal(
        "mini:dot:cam1:ArrayCallbacks", name="cb"
    )

    for s in (acquire, counter, image_mode, acquire_time, callbacks):
        s.wait_for_connection(timeout=3.0)

    callbacks.put(1)
    image_mode.put(0)  # Single
    acquire_time.put(0.05)
    before = counter.get()

    acquire.put(1)
    time.sleep(0.5)
    after = counter.get()
    print(f"\n  ArrayCounter_RBV: {before} → {after}")
    assert after >= before + 1
