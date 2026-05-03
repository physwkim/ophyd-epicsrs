"""ophyd-async + PVA example against the mini-beamline IOC.

Counterpart to ``mini_beamline.py``, which used the synchronous ophyd
control layer over CA. This script:

    * builds devices via :mod:`ophyd_async`'s asyncio-native API
    * routes signal traffic through PVA (``pva://`` prefix) — beam
      current, the camera knobs, the Kohzu mode (NTEnum), and the
      per-station detector readbacks
    * exercises the typed-enum SignalRW path round-trip
      (``await sig.set(KohzuMode.AUTO)`` → ``pvput_field('value.index')``)
    * uses bluesky's ``RunEngine`` for a ``count`` plan against the
      PVA beam-current readback

Motor scans are NOT in this script: ophyd-async's upstream
``Motor`` class hard-imports ``aioca`` for CA, and mini-beamline's
motor records aren't published as NT structures. For motor-driven
scans see ``mini_beamline.py`` (CA / sync ophyd path).

Prerequisite: the epics-rs mini-beamline IOC must be reachable on
the network with PV prefix ``mini:``. Both CA and PVA are served by
the same IOC process (see ``examples/mini-beamline`` in the
``epics-rs`` checkout):

    cargo run --release -p mini-beamline

Run this script directly:

    python examples/mini_beamline_async.py
"""

from __future__ import annotations

import asyncio
import logging
import time

from bluesky import RunEngine
from bluesky.plans import count
from ophyd_async.core import (
    StandardReadable,
    StandardReadableFormat,
    StrictEnum,
    init_devices,
)

from ophyd_epicsrs.ophyd_async import (
    epicsrs_signal_r,
    epicsrs_signal_rw,
    epicsrs_signal_rw_rbv,
)


# ---------------------------------------------------------------------------
# 1. Datatypes
# ---------------------------------------------------------------------------


class KohzuMode(StrictEnum):
    """NTEnum on ``mini:KohzuModeBO`` — IOC defines two states."""

    MANUAL = "Manual"
    AUTO = "Auto"


# ---------------------------------------------------------------------------
# 2. Device declarations
# ---------------------------------------------------------------------------


class PointDetector(StandardReadable):
    """PVA-backed detector for one mini-beamline station (ph / edge / slit).

    ``DetValue_RBV`` is the analytical detector response (Gaussian for
    pinhole, error-function for edge). ``DetSigma`` / ``DetCenter`` are
    the response-shape parameters and behave as configuration signals
    — they appear in ``read_configuration`` but not in the readable
    stream that bluesky's ``count`` / ``scan`` collect.
    """

    def __init__(self, prefix: str, name: str = "") -> None:
        with self.add_children_as_readables():
            self.value = epicsrs_signal_r(float, f"pva://{prefix}DetValue_RBV")
        with self.add_children_as_readables(StandardReadableFormat.CONFIG_SIGNAL):
            self.sigma = epicsrs_signal_rw_rbv(
                float, f"pva://{prefix}DetSigma", "_RBV"
            )
            self.center = epicsrs_signal_rw_rbv(
                float, f"pva://{prefix}DetCenter", "_RBV"
            )
            self.exposure = epicsrs_signal_rw_rbv(
                float, f"pva://{prefix}ExposureTime", "_RBV"
            )
        super().__init__(name=name)


class SimCamera(StandardReadable):
    """PVA wrapper around the mini-beamline AreaDetector cam1 knobs.

    Demonstrates split read/write PVs (``X_RBV`` for reads, ``X`` for
    writes) — the canonical AreaDetector pattern. ``epicsrs_signal_rw_rbv``
    builds the SignalRW from ``"pva://prefix:NAME"`` (write) +
    ``"pva://prefix:NAME_RBV"`` (read) automatically.
    """

    def __init__(self, prefix: str, name: str = "") -> None:
        with self.add_children_as_readables():
            self.array_counter = epicsrs_signal_r(
                int, f"pva://{prefix}ArrayCounter_RBV"
            )
        with self.add_children_as_readables(StandardReadableFormat.CONFIG_SIGNAL):
            self.acquire_time = epicsrs_signal_rw_rbv(
                float, f"pva://{prefix}AcquireTime", "_RBV"
            )
            self.num_images = epicsrs_signal_rw_rbv(
                int, f"pva://{prefix}NumImages", "_RBV"
            )
        super().__init__(name=name)


# ---------------------------------------------------------------------------
# 3. Build all devices inside DeviceCollector so connect() is called
#    automatically when we exit the context manager.
# ---------------------------------------------------------------------------


def build_devices():
    """Construct + connect every device in a single asyncio.gather.

    ``init_devices`` tracks every Device instantiated inside its block
    and runs ``connect()`` on all of them in parallel when the block
    exits. ``mock=False`` (default) means real IOC traffic, not
    synthetic backends.
    """
    print("connecting ophyd-async devices via PVA …")
    t0 = time.perf_counter()
    with init_devices():
        # Top-level signals
        beam_current = epicsrs_signal_r(float, "pva://mini:current")
        bragg_energy = epicsrs_signal_r(float, "pva://mini:BraggERdbkAO")
        kohzu_mode = epicsrs_signal_rw(KohzuMode, "pva://mini:KohzuModeBO")

        # Composite devices
        pinhole = PointDetector("mini:ph:", name="pinhole")
        edge = PointDetector("mini:edge:", name="edge")
        slit = PointDetector("mini:slit:", name="slit")
        cam1 = SimCamera("mini:dot:cam1:", name="cam1")

    print(f"  connected in {(time.perf_counter() - t0) * 1000:.0f} ms")

    return {
        "beam_current": beam_current,
        "bragg_energy": bragg_energy,
        "kohzu_mode": kohzu_mode,
        "pinhole": pinhole,
        "edge": edge,
        "slit": slit,
        "cam1": cam1,
    }


# ---------------------------------------------------------------------------
# 4. RunEngine + inline document subscriber
# ---------------------------------------------------------------------------
RE = RunEngine({})


def document_printer(name, doc):
    """Compact inline printer — same shape as the CA example so the
    two scripts can be diffed visually."""
    if name == "start":
        print(f"\n[start]  uid={doc['uid'][:8]}…  plan={doc.get('plan_name', '?')}")
    elif name == "descriptor":
        keys = ", ".join(doc["data_keys"].keys())
        print(f"[descr]  stream={doc['name']}  data_keys=[{keys}]")
    elif name == "event":
        items = ", ".join(
            f"{k}={v:.4g}" if isinstance(v, (int, float)) else f"{k}={v}"
            for k, v in doc["data"].items()
        )
        print(f"[event]  seq={doc['seq_num']}  {items}")
    elif name == "stop":
        print(
            f"[stop]   exit={doc.get('exit_status')}  "
            f"num_events={doc.get('num_events', {})}"
        )


# ---------------------------------------------------------------------------
# 5. Plans + direct-API demos
# ---------------------------------------------------------------------------


def demo_count(devs):
    """5 readings of the PVA beam current."""
    print("\n=== count(beam_current via PVA, num=5) ===")
    RE(count([devs["beam_current"]], num=5, delay=0.1), document_printer)


def demo_count_pinhole(devs):
    """3 readings of the StandardReadable pinhole detector — emits
    the readable value plus the configuration_dictionary at the start
    of the descriptor."""
    print("\n=== count(pinhole as StandardReadable, num=3) ===")
    RE(count([devs["pinhole"]], num=3, delay=0.2), document_printer)


async def demo_kohzu_mode_round_trip(devs):
    """Direct ophyd-async API (no RunEngine) showing PVA NTEnum
    round-trip: read the current mode, flip it, read it back, restore.

    Exercises the typed-enum SignalRW path:
      * ``get_value`` → PVA pvget → NTEnum index/choices → KohzuMode enum
      * ``set`` → KohzuMode label → NTEnum int index → pvput_field
        (``value.index``) — string-form pvput would be silently rejected
        by the IOC for NTEnum, so the wrapper routes it through field
        addressing.
    """
    print("\n=== KohzuMode NTEnum round-trip (direct ophyd-async API) ===")
    sig = devs["kohzu_mode"]
    initial = await sig.get_value()
    target = KohzuMode.AUTO if initial == KohzuMode.MANUAL else KohzuMode.MANUAL
    print(f"  current={initial.value}  →  target={target.value}")
    await sig.set(target)
    await asyncio.sleep(0.3)
    after = await sig.get_value()
    print(f"  readback after set: {after.value}")
    await sig.set(initial)  # restore
    print(f"  restored to {initial.value}")


def demo_camera_config(devs):
    """Read the camera configuration (read_configuration) — every
    config signal flows through PVA. Each value comes back annotated
    with timestamp + alarm severity from the NTScalar metadata."""
    print("\n=== cam1.read_configuration() via PVA ===")
    cfg = asyncio.run(devs["cam1"].read_configuration())
    for k, v in cfg.items():
        print(f"  {k}: value={v['value']!r}  timestamp={v['timestamp']:.3f}")


# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------


def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    devs = build_devices()

    demo_count(devs)
    demo_count_pinhole(devs)
    demo_camera_config(devs)
    asyncio.run(demo_kohzu_mode_round_trip(devs))

    print("\n--- all plans complete ---")
