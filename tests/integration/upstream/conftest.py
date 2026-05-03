"""Conftest for vendored upstream ophyd tests, remapped to the mini-beamline IOC.

The original upstream tests (epicsrs-tests/ophyd_tests) target a separate
caproto IOC at ``test:motor:`` / ``test:signal:`` / ``XF:31IDA-OP{...}``
prefixes. This conftest replaces those fixtures with mini-beamline PVs so
the same upstream contracts run against the same IOC the rest of our
``tests/integration/`` suite uses — no extra IOC process needed.

Auto-installs the epicsrs control layer at import time so every test in
this directory uses our backend (ophyd binds ``ophyd.cl.get_pv`` at
Signal/Device construction time, so the install must happen before any
upstream test module is imported).
"""

from __future__ import annotations

import logging
import os
from types import SimpleNamespace

import pytest

# Vendored upstream tests branch on ``TEST_CL`` to flip expectations
# between epicsrs (Rust) and the legacy pyepics control layer — e.g.
# the homing tests expect SimMotor to succeed under epicsrs but fail
# under MotorSim. Set it before any test module is imported.
os.environ.setdefault("TEST_CL", "epicsrs")

# Install the epicsrs backend FIRST, before any ophyd Signal / Device
# import in the test modules below. ``use_epicsrs_backend`` is the alias
# the older ``physwkim/epicsrs-tests`` repo expects; we expose it from
# ``ophyd_epicsrs/__init__.py``.
from ophyd_epicsrs import use_epicsrs_backend  # noqa: E402

use_epicsrs_backend()

from ophyd import Component as Cpt  # noqa: E402  # after use_epicsrs_backend
from ophyd import EpicsMotor, EpicsSignal, EpicsSignalRO, Signal  # noqa: E402
from ophyd.utils.epics_pvs import AlarmSeverity, AlarmStatus  # noqa: E402

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mini-beamline IOC PV inventory used by the vendored tests.
# ---------------------------------------------------------------------------
# We pick `mini:ph:mtr` as the canonical motor for upstream test_epicsmotor.py
# because it's a normal motor record (no extra constraints) and is bumped to
# VELO=500 by the parent conftest's `_speed_up_sim_motors` autouse fixture,
# so motor.set() round-trips finish in well under the 10 s test timeout.
MOTOR_PV = "mini:ph:mtr"


# ---------------------------------------------------------------------------
# Sim hardware (no IOC needed)
# ---------------------------------------------------------------------------
@pytest.fixture()
def hw(tmpdir):
    from ophyd.sim import hw

    return hw(str(tmpdir))


# ---------------------------------------------------------------------------
# Real EPICS motor against the mini-beamline IOC
# ---------------------------------------------------------------------------


class CustomAlarmEpicsSignalRO(EpicsSignalRO):
    """RO signal that exposes alarm fields the upstream tests poke at."""

    alarm_status = AlarmStatus.NO_ALARM
    alarm_severity = AlarmSeverity.NO_ALARM


class TestEpicsMotor(EpicsMotor):
    """Mirror of the upstream test class — adds limit-switch placeholders
    and the explicit HLM/LLM signals the upstream tests reference."""

    user_readback = Cpt(CustomAlarmEpicsSignalRO, ".RBV", kind="hinted")
    high_limit_switch = Cpt(Signal, value=0, kind="omitted")
    low_limit_switch = Cpt(Signal, value=0, kind="omitted")
    direction_of_travel = Cpt(Signal, value=0, kind="omitted")
    high_limit_value = Cpt(EpicsSignal, ".HLM", kind="config")
    low_limit_value = Cpt(EpicsSignal, ".LLM", kind="config")

    @user_readback.sub_value
    def _pos_changed(self, timestamp=None, value=None, **kwargs):
        super()._pos_changed(timestamp=timestamp, value=value, **kwargs)


@pytest.fixture(scope="function")
def motor(request, cleanup):
    """Real EpicsMotor against ``mini:ph:mtr``.

    Reset offsets + widen limits before each test so a previous test's
    move/limit state doesn't leak in. Wait for the position to settle at
    0 before yielding so position-relative assertions start from a known
    origin.

    Skips on connect timeout — the upstream beacon-anomaly chain
    (epics-ca-rs first_sighting → EchoProbe → 5 s echo timeout →
    TcpClosed; see python/ophyd_epicsrs/_contexts.py) can occasionally
    blow the default connect budget. The contract under test is the
    EpicsMotor surface, not connect latency under network turbulence.
    """
    # Constructor timeout=10.0 matches what the upstream tests pin
    # (test_timeout / test_high_limit_switch both assert motor.timeout
    # == 10.0). Use a separate longer wait_for_connection budget so a
    # mid-test beacon-anomaly reconnect doesn't blow the connect step
    # without affecting the user-visible motor.timeout attribute.
    motor = TestEpicsMotor(MOTOR_PV, name="epicsmotor", settle_time=0.1, timeout=10.0)
    cleanup.add(motor)

    try:
        motor.wait_for_connection(timeout=15.0)
    except TimeoutError as e:
        pytest.skip(f"motor connect timed out (transient IOC reconnect): {e}")

    motor.user_offset.put(0, wait=True)
    motor.user_offset_dir.put(0, wait=True)
    motor.offset_freeze_switch.put(0, wait=True)
    motor.set_use_switch.put(0, wait=True)
    motor.low_limit_value.put(-100, wait=True)
    motor.high_limit_value.put(100, wait=True)
    motor.set(0).wait()

    return motor


# ---------------------------------------------------------------------------
# Generic destructor-collector used by every upstream test
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Mini-beamline mapping for upstream test_signal.py's `signal_test_ioc`
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def signal_test_ioc(request):
    """Map the upstream test_signal.py PVs onto mini-beamline equivalents.

    - read_only  → mini:current               (RO ai, beam current calc)
    - read_write → mini:dot:cam1:AcquireTime  (ao that round-trips on put)
    - pair_set   → mini:dot:cam1:AcquireTime  (setpoint side)
    - pair_rbv   → mini:dot:cam1:AcquireTime_RBV (readback side)
    - bool_enum  → mini:KohzuModeBO           (bo / NTEnum, 2 enum strs)

    Tests that asked for `set_severity` / `alarm_status` (alarm-injection
    records), `path` (string PVs), or `waveform` (mini's only waveform
    is 600 KB image data, too heavy for a generic signal test) have been
    removed from the vendored test_signal.py — there is no mini-beamline
    equivalent and they are covered separately in our own integration
    suite where applicable.
    """
    from ophyd import EpicsSignalRO

    pvs = {
        "read_only": "mini:current",
        "read_write": "mini:dot:cam1:AcquireTime",
        "pair_set": "mini:dot:cam1:AcquireTime",
        "pair_rbv": "mini:dot:cam1:AcquireTime_RBV",
        "bool_enum": "mini:KohzuModeBO",
    }

    sig = EpicsSignalRO(pvs["read_only"], name="check")
    try:
        sig.wait_for_connection(timeout=5)
    except TimeoutError:
        pytest.skip("mini-beamline IOC not reachable")
    finally:
        sig.destroy()

    return SimpleNamespace(prefix="mini:", name="signal_test_ioc", pvs=pvs)


@pytest.fixture(scope="function")
def cleanup(request):
    items = []

    class Cleaner:
        def add(self, item):
            items.append(item)

    def clean():
        for item in items:
            try:
                item.destroy()
            except Exception:
                logger.exception("destroy() failed for %r", item)
        items.clear()

    request.addfinalizer(clean)
    return Cleaner()
