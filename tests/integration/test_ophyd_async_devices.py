"""ophyd-async StandardReadable / Device patterns.

Wraps the mini-beamline point detectors as proper ophyd-async
StandardReadables and drives them through bluesky's RunEngine. The
RunEngine handles the asyncio loop internally, so this is the same
front-door an end-user would touch.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.fixture
def RE():
    """Fresh RunEngine — also sets up the bluesky asyncio loop that
    ``init_devices()`` requires."""
    from bluesky import RunEngine

    return RunEngine({})


def _call_in_RE_loop(coro):
    """Drive a coroutine on the bluesky-managed event loop. The loop
    is in the "running" state from RE's perspective so plain
    ``loop.run_until_complete`` raises — this helper threads through
    bluesky's published entrypoint instead."""
    from bluesky.run_engine import call_in_bluesky_event_loop

    return call_in_bluesky_event_loop(coro)


def _PointDetector_cls():
    """Build the StandardReadable subclass at call time so it picks up
    whatever ophyd-async version the env has installed."""
    from ophyd_async.core import StandardReadable, StandardReadableFormat
    from ophyd_epicsrs.ophyd_async import epicsrs_signal_r, epicsrs_signal_rw

    class PointDetector(StandardReadable):
        def __init__(self, prefix: str, name: str = "") -> None:
            with self.add_children_as_readables():
                self.value = epicsrs_signal_r(float, f"{prefix}DetValue_RBV")
            with self.add_children_as_readables(StandardReadableFormat.CONFIG_SIGNAL):
                self.sigma = epicsrs_signal_rw(
                    float, f"{prefix}DetSigma_RBV", f"{prefix}DetSigma"
                )
                self.center = epicsrs_signal_rw(
                    float, f"{prefix}DetCenter_RBV", f"{prefix}DetCenter"
                )
            super().__init__(name=name)

    return PointDetector


# ---------- Sync RE-driven tests (need bluesky event loop) ----------


def test_standard_readable_describe(RE):
    """describe() returns event_model-shaped DataKeys for the
    readable signals only (sigma/center are config, not read-stream)."""
    from ophyd_async.core import init_devices

    with init_devices():
        det = _PointDetector_cls()("mini:ph:", name="ph_det")

    desc = _call_in_RE_loop(det.describe())
    assert "ph_det-value" in desc
    key = desc["ph_det-value"]
    assert key["dtype"] == "number"
    assert key["source"].endswith("mini:ph:DetValue_RBV")


def test_standard_readable_read_returns_value(RE):
    from ophyd_async.core import init_devices

    with init_devices():
        det = _PointDetector_cls()("mini:ph:", name="ph_det")
    reading = _call_in_RE_loop(det.read())
    assert "ph_det-value" in reading
    assert isinstance(reading["ph_det-value"]["value"], float)


def test_standard_readable_read_configuration(RE):
    """Configuration stream contains sigma + center."""
    from ophyd_async.core import init_devices

    with init_devices():
        det = _PointDetector_cls()("mini:ph:", name="ph_det")
    cfg = _call_in_RE_loop(det.read_configuration())
    assert "ph_det-sigma" in cfg
    assert "ph_det-center" in cfg


def test_standard_readable_in_count_plan(RE):
    """RunEngine drives the async device through bluesky.plans.count
    end-to-end."""
    from bluesky.plans import count
    from ophyd_async.core import init_devices

    with init_devices():
        det = _PointDetector_cls()("mini:ph:", name="ph_det")

    docs = []
    RE(count([det], num=3), lambda n, d: docs.append((n, d)))

    events = [d for n, d in docs if n == "event"]
    assert len(events) == 3
    for e in events:
        assert "ph_det-value" in e["data"]
        assert isinstance(e["data"]["ph_det-value"], float)


def test_standard_readable_value_changes_with_motor(RE, ophyd_setup):
    """End-to-end: move the legacy ophyd motor and observe the async
    StandardReadable detector value change."""
    import time

    from ophyd_async.core import init_devices

    with init_devices():
        det = _PointDetector_cls()("mini:ph:", name="ph_det")

    mtr = ophyd_setup.EpicsMotor("mini:ph:mtr", name="ph_mtr")
    mtr.wait_for_connection(timeout=5.0)

    mtr.move(0.0, wait=True, timeout=10.0)
    time.sleep(0.3)
    centre = _call_in_RE_loop(det.read())["ph_det-value"]["value"]

    mtr.move(8.0, wait=True, timeout=10.0)
    time.sleep(0.3)
    off = _call_in_RE_loop(det.read())["ph_det-value"]["value"]

    print(f"\n  centre={centre:.1f}  off={off:.1f}")
    assert centre > off * 2
    mtr.move(0.0, wait=True, timeout=10.0)


# ---------- Pure async tests (manual connect, no init_devices) ----------


@pytest.mark.asyncio
async def test_async_device_parallel_connect_three_detectors():
    """Three independent StandardReadables connect concurrently;
    asyncio.gather should overlap the network I/O. Builds devices
    without init_devices so this test doesn't require a RE-managed
    loop."""
    import time

    cls = _PointDetector_cls()
    ph = cls("mini:ph:", name="ph")
    edge = cls("mini:edge:", name="edge")
    slit = cls("mini:slit:", name="slit")

    t0 = time.perf_counter()
    await asyncio.gather(ph.connect(), edge.connect(), slit.connect())
    dt = time.perf_counter() - t0
    print(f"\n  3x StandardReadable connect: {dt * 1000:.2f} ms")
    # Each detector has 3 PVs (1 read + 2 config) = 9 PV connects total.
    # Localhost steady state: ~100–500 ms. Ceiling 5 s absorbs one
    # mid-test reconnect from the upstream beacon-anomaly chain
    # (epics-ca-rs first_sighting → EchoProbe → 5 s echo timeout →
    # TcpClosed; see _contexts.py). The 1 s print warning surfaces
    # the "in-between" zone — under 5 s so it doesn't fail, but
    # slow enough to investigate if it shows up on every run.
    if dt > 1.0:
        print(f"  WARN: 9 PV connect took {dt:.2f}s — investigate if persistent")
    assert dt < 5.0
