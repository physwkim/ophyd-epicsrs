"""Shared fixtures for the live mini-beamline integration suite.

Every test file in tests/integration/ inherits these fixtures so the
``use_epicsrs()`` install, the CA/PVA contexts, and the IOC reachability
check happen exactly once per session.
"""

from __future__ import annotations

import pytest

from ophyd_epicsrs._contexts import get_ca_context, get_pva_context
from ophyd_epicsrs._native import EpicsRsContext, EpicsRsPvaContext


@pytest.fixture(scope="session")
def ca_ctx() -> EpicsRsContext:
    # Share the process-wide singleton so the test's CaClient is the
    # same one ophyd / ophyd-async use — see python/ophyd_epicsrs/_contexts.py.
    return get_ca_context()


@pytest.fixture(scope="session")
def pva_ctx() -> EpicsRsPvaContext:
    return get_pva_context()


@pytest.fixture(scope="session", autouse=True)
def _verify_ioc(ca_ctx):
    """Skip every test under tests/integration/ if the IOC isn't up."""
    pv = ca_ctx.create_pv("mini:current")
    if not pv.wait_for_connection(timeout=3.0):
        pytest.skip("mini-beamline IOC not reachable on CA")


@pytest.fixture(scope="session", autouse=True)
def _speed_up_sim_motors(_verify_ioc, ca_ctx):
    """Bump every SimMotor's VELO to 500 unit/s for the test session,
    snapshot + restore the original VELOs at teardown.

    Default ``motor.template`` ships VELO=1, which means a 10-unit move
    is a full 10-second wall clock. With dozens of motor moves across
    the suite, that adds minutes of dead time. Bumping all motors to
    the same VELO as the dot:mtrx/y pair (already 500 in st.cmd) cuts
    suite wall time roughly 4×.

    All motor RBVs still observe the move physically — the detector
    Gaussian / erfc still tracks correctly because the IOC's CP links
    fire on every RBV step regardless of speed.

    Teardown restores each motor's original VELO so a long-running
    local IOC isn't left in a non-default state after the suite —
    otherwise other ophyd code in the same process / IOC session
    would see surprisingly fast motors and wonder why.
    """
    motors = (
        "mini:ph:mtr",
        "mini:edge:mtr",
        "mini:slit:mtr",
        "mini:dcm:theta",
        "mini:dcm:y",
        "mini:dcm:z",
    )
    saved: dict[str, float] = {}
    for m in motors:
        velo = ca_ctx.create_pv(f"{m}.VELO")
        if not velo.wait_for_connection(timeout=2.0):
            continue
        snap = velo.get_with_metadata(timeout=1.0)
        if snap is not None:
            saved[m] = snap["value"]
        velo.put(500.0, wait=True, timeout=2.0)
    yield
    for m, original in saved.items():
        velo = ca_ctx.create_pv(f"{m}.VELO")
        if velo.wait_for_connection(timeout=2.0):
            velo.put(original, wait=True, timeout=2.0)


@pytest.fixture(scope="session")
def ophyd_setup():
    """Install the epics-rs control layer once per session and return
    the ophyd module so individual tests can construct Signals/Devices."""
    from ophyd_epicsrs import use_epicsrs

    use_epicsrs()
    import ophyd

    return ophyd
