"""End-to-end bluesky example against the mini-beamline IOC.

Demonstrates the full ophyd-epicsrs stack:
    install backend → declare Devices → spin up RunEngine → run plans.

Prerequisite: the epics-rs mini-beamline IOC must be reachable on the
network with PV prefix ``mini:``. From the epics-rs checkout:

    cargo run --release -p mini-beamline

Verify with:

    caget mini:current

Run this script directly:

    python examples/mini_beamline.py

It prints every emitted document inline so you can watch the run-engine
state flow without standing up a databroker / mongo instance.
"""

from __future__ import annotations

import logging
import time

from bluesky import RunEngine
from bluesky.callbacks import LiveTable
from bluesky.plans import count, grid_scan, rel_scan, scan

# ---------------------------------------------------------------------------
# 1. Install the Rust-backed control layer.
#    Must happen BEFORE any ophyd Signal / Device is constructed,
#    because ophyd binds ``ophyd.cl.get_pv`` at __init__ time.
# ---------------------------------------------------------------------------
from ophyd_epicsrs import use_epicsrs

use_epicsrs(logger=logging.getLogger("ophyd_epicsrs"))

from ophyd import Component as Cpt  # noqa: E402  (must follow use_epicsrs)
from ophyd import Device, EpicsMotor, EpicsSignal, EpicsSignalRO


# ---------------------------------------------------------------------------
# 2. Device declarations
# ---------------------------------------------------------------------------


class PointDetector(Device):
    """Simulated point detector: a motor scans through an analytical
    response (Gaussian for ``ph``, error-function for ``edge``) and the
    IOC's CP-link updates DetValue_RBV every motor step."""

    motor = Cpt(EpicsMotor, "mtr")
    value = Cpt(EpicsSignalRO, "DetValue_RBV", kind="hinted")
    sigma = Cpt(EpicsSignalRO, "DetSigma_RBV", kind="config")
    centre = Cpt(EpicsSignalRO, "DetCenter_RBV", kind="config")
    exposure = Cpt(EpicsSignal, "ExposureTime", kind="config")


class DCM(Device):
    """Double-crystal monochromator — three motor axes only.

    Bragg-energy / wavelength readbacks live at the top level
    (``mini:BraggE…``) not under ``mini:dcm:``, so they're declared
    separately below as top-level signals rather than as Cpts of DCM.
    """

    theta = Cpt(EpicsMotor, "theta")
    y = Cpt(EpicsMotor, "y")
    z = Cpt(EpicsMotor, "z")


class XYStage(Device):
    """Two-axis sample stage. The ``dot`` motor pair is bumped to
    VELO=500 in the IOC's st.cmd, so a 5 × 5 grid_scan finishes in
    seconds rather than minutes."""

    x = Cpt(EpicsMotor, "mtrx")
    y = Cpt(EpicsMotor, "mtry")


# Top-level signals
beam_current = EpicsSignalRO("mini:current", name="beam_current")
bragg_energy = EpicsSignalRO("mini:BraggERdbkAO", name="bragg_energy")
bragg_theta = EpicsSignalRO("mini:BraggThetaRdbkAO", name="bragg_theta")
bragg_lambda = EpicsSignalRO("mini:BraggLambdaRdbkAO", name="bragg_lambda")
kohzu_mode = EpicsSignal("mini:KohzuModeBO", name="kohzu_mode")

# Composite devices
pinhole = PointDetector("mini:ph:", name="pinhole")
edge = PointDetector("mini:edge:", name="edge")
slit = PointDetector("mini:slit:", name="slit")
dcm = DCM("mini:dcm:", name="dcm")
xy_stage = XYStage("mini:dot:", name="xy_stage")


def wait_all_connected(timeout: float = 10.0) -> None:
    """Connect every device in parallel — fail-fast if the IOC isn't up."""
    devs = [
        beam_current, bragg_energy, bragg_theta, bragg_lambda, kohzu_mode,
        pinhole, edge, slit, dcm, xy_stage,
    ]
    print(f"connecting {len(devs)} devices …")
    t0 = time.perf_counter()
    for d in devs:
        d.wait_for_connection(timeout=timeout)
    print(f"  connected in {(time.perf_counter() - t0) * 1000:.0f} ms")


def speed_up_sim_motors(velo: float = 500.0) -> None:
    """Bump every SimMotor's VELO so plans complete in seconds, not minutes.

    Default ``motor.template`` ships VELO=1, so a 20-unit pinhole scan
    spends ~22 s per step physically slewing. The dot stage already has
    VELO=500 hard-coded in the IOC's st.cmd; matching that for the
    other axes brings the whole script under a minute.

    Skipped silently for any motor whose VELO PV doesn't connect (lets
    you point at a real beamline IOC where you'd never want this
    side-effect).
    """
    motors = (pinhole.motor, edge.motor, slit.motor, dcm.theta, dcm.y, dcm.z)
    for m in motors:
        try:
            m.velocity.put(velo)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 3. RunEngine + document subscribers
# ---------------------------------------------------------------------------
RE = RunEngine({})


def document_printer(name, doc):
    """Compact inline printer — one line per document so the script
    output stays readable without a databroker."""
    if name == "start":
        print(f"\n[start]  uid={doc['uid'][:8]}…  plan={doc.get('plan_name', '?')}")
    elif name == "descriptor":
        keys = ", ".join(doc["data_keys"].keys())
        print(f"[descr]  stream={doc['name']}  data_keys=[{keys}]")
    elif name == "event":
        items = ", ".join(f"{k}={v:.4g}" if isinstance(v, (int, float)) else f"{k}={v}"
                          for k, v in doc["data"].items())
        print(f"[event]  seq={doc['seq_num']}  {items}")
    elif name == "stop":
        print(f"[stop]   exit={doc.get('exit_status')}  num_events={doc.get('num_events', {})}")


# ---------------------------------------------------------------------------
# 4. Plans
# ---------------------------------------------------------------------------


def demo_count():
    """5 readings of the beam current — sanity check that the
    RunEngine + EpicsSignalRO + document flow works."""
    print("\n=== count(beam_current, num=5) ===")
    RE(count([beam_current], num=5, delay=0.1), document_printer)


def demo_pinhole_scan():
    """Scan the pinhole motor across its Gaussian peak — the detector
    response should rise to a maximum near motor=0 and fall off
    symmetrically. LiveTable subscriber prints a tabular summary."""
    print("\n=== scan(pinhole, -10 → +10, 11 points) ===")
    RE(
        scan([pinhole.value], pinhole.motor, -10.0, 10.0, 11),
        [document_printer, LiveTable([pinhole.motor.user_readback, pinhole.value])],
    )


def demo_rel_scan_edge():
    """Relative scan around the current edge motor position — the
    detector should trace a monotonic erfc-style transition."""
    print("\n=== rel_scan(edge, -5 → +5, 7 points) ===")
    RE(rel_scan([edge.value], edge.motor, -5.0, 5.0, 7), document_printer)


def demo_grid_scan_xy():
    """5 × 5 grid scan with the dot stage. ``snake_axes=True`` reverses
    every other row so the motor doesn't have to fly back to the
    starting X each row."""
    print("\n=== grid_scan(xy_stage, 5x5) ===")
    RE(
        grid_scan(
            [beam_current],
            xy_stage.x, -2.0, 2.0, 5,
            xy_stage.y, -2.0, 2.0, 5,
            snake_axes=True,
        ),
        document_printer,
    )


def demo_dcm_move():
    """Move the DCM theta axis and read back the resulting energy.
    Demonstrates writing a setpoint via a composite Device's child motor."""
    print("\n=== DCM move + energy readback ===")
    target = 12.0
    print(f"  moving dcm.theta → {target}")
    dcm.theta.move(target, timeout=10.0)
    energy = bragg_energy.get()
    theta = bragg_theta.get()
    print(f"  resulting bragg_energy={energy:.4f}  bragg_theta={theta:.4f}")


# ---------------------------------------------------------------------------
# 5. Main
# ---------------------------------------------------------------------------


def main():
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    wait_all_connected()
    speed_up_sim_motors()

    demo_count()
    demo_pinhole_scan()
    demo_rel_scan_edge()
    demo_grid_scan_xy()
    demo_dcm_move()

    print("\n--- all plans complete ---")
