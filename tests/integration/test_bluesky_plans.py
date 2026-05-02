"""Bluesky plan integration against mini-beamline.

Exercises the full data-acquisition path: ophyd Device → bluesky
RunEngine → DocumentRouter. The plans drive the IOC's CP-linked
detector + motor combination so the documents we collect contain
recognisable beamline physics (Gaussian peak, monotonic Edge response,
beam-current time series).
"""

from __future__ import annotations

import time

import numpy as np
import pytest


@pytest.fixture
def RE():
    """Fresh RunEngine per test (no shared subscribers between tests)."""
    from bluesky import RunEngine

    return RunEngine({})


@pytest.fixture
def collector():
    """Append-only document collector for inspection."""
    docs: list[tuple[str, dict]] = []

    def cb(name, doc):
        docs.append((name, doc))

    return docs, cb


def test_count_beam_current(ophyd_setup, RE, collector):
    """count(beam_current, num=5) should produce 1 start + 1 descriptor
    + 5 events + 1 stop, with monotonically increasing seq_num."""
    from bluesky.plans import count

    sig = ophyd_setup.EpicsSignalRO("mini:current", name="beam")
    sig.wait_for_connection(timeout=5.0)

    docs, cb = collector
    RE(count([sig], num=5, delay=0.05), cb)

    names = [n for n, _ in docs]
    assert names.count("start") == 1
    assert names.count("descriptor") == 1
    assert names.count("event") == 5
    assert names.count("stop") == 1

    events = [d for n, d in docs if n == "event"]
    seq = [e["seq_num"] for e in events]
    assert seq == [1, 2, 3, 4, 5]

    values = [e["data"]["beam"] for e in events]
    # All readings should be in the OFFSET ± AMPLITUDE band (500 ± 25 mA).
    assert all(450 < v < 550 for v in values)


def test_scan_pinhole_gaussian(ophyd_setup, RE, collector):
    """Scan the PinHole motor across the Gaussian centre and verify
    the detector trace peaks near 0 and falls off symmetrically."""
    from bluesky.plans import scan

    mtr = ophyd_setup.EpicsMotor("mini:ph:mtr", name="ph_mtr")
    det = ophyd_setup.EpicsSignalRO("mini:ph:DetValue_RBV", name="ph_det")
    for s in (mtr, det):
        s.wait_for_connection(timeout=5.0)

    # 5-point scan from -8 to +8 (~1.6σ each side, centre at idx 2)
    docs, cb = collector
    RE(scan([det], mtr, -8.0, 8.0, 5), cb)

    events = [d for n, d in docs if n == "event"]
    assert len(events) == 5

    intensities = [e["data"]["ph_det"] for e in events]

    # Peak should be at index 2 (motor=0); allow ±1 because beam
    # current modulates absolute intensity within ±5 %.
    peak_idx = int(np.argmax(intensities))
    assert abs(peak_idx - 2) <= 1, f"peak at idx {peak_idx}, expected ~2"

    # Symmetric decay: edges much smaller than the centre.
    centre = intensities[peak_idx]
    edges = (intensities[0], intensities[-1])
    assert all(e < centre * 0.5 for e in edges), (
        f"centre {centre:.1f} not >> edges {edges}"
    )

    mtr.move(0.0, wait=True, timeout=10.0)


def test_scan_edge_monotonic(ophyd_setup, RE, collector):
    """Edge detector formula is `erfc((center - mtr) / sigma) / 2`
    with center=5, sigma=2.5 — so the response *increases* with
    motor position (erfc of a large positive arg → 0; erfc of a
    large negative arg → 2). Check that the average over the upper
    half of the scan is much greater than the lower half."""
    from bluesky.plans import scan

    mtr = ophyd_setup.EpicsMotor("mini:edge:mtr", name="edge_mtr")
    det = ophyd_setup.EpicsSignalRO("mini:edge:DetValue_RBV", name="edge_det")
    for s in (mtr, det):
        s.wait_for_connection(timeout=5.0)

    docs, cb = collector
    # Scan well below to well above the edge centre (5.0).
    RE(scan([det], mtr, -2.0, 12.0, 5), cb)

    events = [d for n, d in docs if n == "event"]
    intensities = [e["data"]["edge_det"] for e in events]

    first_half = np.mean(intensities[:2])
    second_half = np.mean(intensities[3:])
    print(f"\n  edge response first half={first_half:.1f}  second half={second_half:.1f}")
    assert second_half > first_half * 5, (
        f"edge response {first_half:.1f} → {second_half:.1f} not increasing as expected"
    )

    mtr.move(5.0, wait=True, timeout=10.0)


def test_baseline_reading_with_supplemental_data(ophyd_setup, RE, collector):
    """SupplementalData baseline: add beam current as a baseline
    stream so each scan event also has a 'baseline' descriptor stream
    captured at start and stop."""
    from bluesky.plans import count
    from bluesky.preprocessors import baseline_decorator

    beam = ophyd_setup.EpicsSignalRO("mini:current", name="beam")
    beam.wait_for_connection(timeout=5.0)
    sigma = ophyd_setup.EpicsSignal("mini:ph:DetSigma", write_pv="mini:ph:DetSigma", name="sigma")
    sigma.wait_for_connection(timeout=5.0)

    @baseline_decorator([sigma])
    def plan():
        return (yield from count([beam], num=2))

    docs, cb = collector
    RE(plan(), cb)

    desc_names = [d.get("name") for n, d in docs if n == "descriptor"]
    assert "baseline" in desc_names, f"no baseline descriptor; got {desc_names}"
    # Two baseline events: one at start, one at stop.
    baseline_events = [
        d for n, d in docs
        if n == "event" and d["descriptor"] in [
            doc["uid"] for nm, doc in docs
            if nm == "descriptor" and doc.get("name") == "baseline"
        ]
    ]
    assert len(baseline_events) == 2


def test_scan_event_timestamps_monotonic(ophyd_setup, RE, collector):
    """Successive event times should march forward — sanity-check that
    monitor delivery isn't reordering things behind RunEngine."""
    from bluesky.plans import count

    beam = ophyd_setup.EpicsSignalRO("mini:current", name="beam")
    beam.wait_for_connection(timeout=5.0)

    docs, cb = collector
    t0 = time.time()
    RE(count([beam], num=4, delay=0.1), cb)
    times = [d["time"] for n, d in docs if n == "event"]
    assert all(t1 < t2 for t1, t2 in zip(times, times[1:])), (
        f"event times not monotonic: {times}"
    )
    # Total span at least 0.3s (3 inter-event gaps × 0.1s).
    assert times[-1] - times[0] >= 0.25
    assert times[0] >= t0 - 0.1  # event time roughly contemporaneous
