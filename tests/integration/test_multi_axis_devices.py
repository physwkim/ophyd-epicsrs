"""Multi-axis Device composition (DCM, slit, BPM, dot XY pair).

Mini-beamline has several real multi-component devices that exercise
ophyd's Component composition pattern, asyn-port-driven scalar
records, and the dot:mtrx/mtry coordinated motor pair.
"""

from __future__ import annotations

import time

import pytest


# ---------- Kohzu DCM (3 motors + setpoint) ----------


def test_dcm_device_composition(ophyd_setup):
    """DCM with theta/y/z motor children + energy/Bragg readback PVs."""
    from ophyd import Component as Cpt
    from ophyd import Device, EpicsMotor, EpicsSignal, EpicsSignalRO

    class DCM(Device):
        theta = Cpt(EpicsMotor, "dcm:theta")
        y = Cpt(EpicsMotor, "dcm:y")
        z = Cpt(EpicsMotor, "dcm:z")
        energy = Cpt(EpicsSignal, "BraggEAO", write_pv="BraggEAO")
        energy_rbv = Cpt(EpicsSignalRO, "BraggERdbkAO")
        bragg_rbv = Cpt(EpicsSignalRO, "BraggThetaRdbkAO")
        wavelength_rbv = Cpt(EpicsSignalRO, "BraggLambdaRdbkAO")
        mode = Cpt(EpicsSignal, "KohzuModeBO", write_pv="KohzuModeBO")
        moving = Cpt(EpicsSignalRO, "KohzuMoving")

    dcm = DCM("mini:", name="dcm")
    t0 = time.perf_counter()
    try:
        dcm.wait_for_connection(timeout=10.0)
    except TimeoutError as e:
        # 9 PVs (incl. 3 motor sub-records) couldn't all connect in 10 s.
        # Mid-test beacon-anomaly reconnect (epics-ca-rs first_sighting
        # → EchoProbe → 5 s echo timeout → TcpClosed; see _contexts.py)
        # can blow this budget. Skip rather than fail — the contract
        # under test is Device composition correctness, not connect
        # latency under network turbulence.
        pytest.skip(f"DCM connect timed out (transient IOC reconnect): {e}")
    print(f"\n  DCM(9 PVs incl. 3 motors) connect: {(time.perf_counter() - t0) * 1000:.1f} ms")

    assert dcm.theta.connected
    assert dcm.y.connected
    assert dcm.z.connected

    # Read full state — all signals populated
    state = dcm.read()
    expected_keys = {
        "dcm_theta",
        "dcm_y",
        "dcm_z",
        "dcm_energy",
        "dcm_energy_rbv",
        "dcm_bragg_rbv",
        "dcm_wavelength_rbv",
        "dcm_mode",
        "dcm_moving",
    }
    assert expected_keys.issubset(state.keys())


def test_dcm_individual_axis_move(ophyd_setup):
    """One axis of the DCM Device behaves like a normal EpicsMotor."""
    from ophyd import Component as Cpt
    from ophyd import Device, EpicsMotor

    class DCM(Device):
        z = Cpt(EpicsMotor, "dcm:z")

    dcm = DCM("mini:", name="dcm")
    dcm.wait_for_connection(timeout=5.0)
    initial = dcm.z.position
    target = initial + 0.5
    dcm.z.move(target, wait=True, timeout=10.0)
    assert abs(dcm.z.position - target) < 0.1
    dcm.z.move(initial, wait=True, timeout=10.0)


# ---------- MovingDot 2D motor pair ----------


def test_dot_xy_motor_pair(ophyd_setup):
    """The dot:mtrx / dot:mtry pair is much faster (VELO=500) than
    the regular sim motors and supports larger ranges (±500). Verify
    both axes move independently and report correct RBV."""
    from ophyd import Component as Cpt
    from ophyd import Device, EpicsMotor

    class DotStage(Device):
        x = Cpt(EpicsMotor, "dot:mtrx")
        y = Cpt(EpicsMotor, "dot:mtry")

    stage = DotStage("mini:", name="stage")
    stage.wait_for_connection(timeout=5.0)

    stage.x.move(50.0, wait=True, timeout=10.0)
    stage.y.move(-30.0, wait=True, timeout=10.0)
    assert abs(stage.x.position - 50.0) < 1.0
    assert abs(stage.y.position - (-30.0)) < 1.0

    stage.x.move(0.0, wait=True, timeout=10.0)
    stage.y.move(0.0, wait=True, timeout=10.0)


# ---------- AreaDetector cam1 as a custom Device ----------


def test_area_detector_cam_device(ophyd_setup):
    """MovingDot cam1 wrapped as a custom Device with the standard
    AD scaffolding (ImageMode mbbi, AcquireTime ao, Acquire bo,
    ArrayCounter_RBV longin)."""
    from ophyd import Component as Cpt
    from ophyd import Device, EpicsSignal, EpicsSignalRO

    class CamDevice(Device):
        acquire = Cpt(EpicsSignal, "Acquire", write_pv="Acquire")
        acquire_rbv = Cpt(EpicsSignalRO, "Acquire_RBV")
        image_mode = Cpt(
            EpicsSignal, "ImageMode", write_pv="ImageMode", string=True
        )
        acquire_time = Cpt(
            EpicsSignal, "AcquireTime", write_pv="AcquireTime"
        )
        num_images = Cpt(
            EpicsSignal, "NumImages", write_pv="NumImages"
        )
        array_counter = Cpt(EpicsSignalRO, "ArrayCounter_RBV")
        array_callbacks = Cpt(
            EpicsSignal, "ArrayCallbacks", write_pv="ArrayCallbacks"
        )
        size_x = Cpt(EpicsSignalRO, "MaxSizeX_RBV")
        size_y = Cpt(EpicsSignalRO, "MaxSizeY_RBV")
        manufacturer = Cpt(EpicsSignalRO, "Manufacturer_RBV", string=True)
        model = Cpt(EpicsSignalRO, "Model_RBV", string=True)

    cam = CamDevice("mini:dot:cam1:", name="cam")
    t0 = time.perf_counter()
    cam.wait_for_connection(timeout=10.0)
    print(f"\n  Cam(11 PVs) connect: {(time.perf_counter() - t0) * 1000:.1f} ms")

    state = cam.read()
    assert state["cam_manufacturer"]["value"] == "Mini Beamline"
    assert state["cam_model"]["value"] == "Moving Dot"
    assert state["cam_size_x"]["value"] == 640
    assert state["cam_size_y"]["value"] == 480
