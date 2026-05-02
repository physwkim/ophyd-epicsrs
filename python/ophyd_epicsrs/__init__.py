"""ophyd-epicsrs: Rust EPICS backend for ophyd (Channel Access + pvAccess).

The CA / PVA contexts are process-wide singletons. Construct PVs via
``get_ca_context().create_pv(name)`` / ``get_pva_context().create_pv(name)``
— do NOT instantiate ``EpicsRsContext`` / ``EpicsRsPvaContext`` directly.
Each independent context spins up its own beacon monitor / search engine
and trips spurious ``first_sighting`` anomalies in epics-ca-rs that drop
healthy TCP circuits. ``EpicsRsPV`` / ``EpicsRsPvaPV`` are exposed for
``isinstance`` checks and type annotations on the values returned by
``create_pv``; users should not construct them directly either.
"""

import logging
import types

from ophyd_epicsrs._contexts import get_ca_context, get_pva_context, shutdown_all
from ophyd_epicsrs._native import (
    EpicsRsPV,
    EpicsRsPvaPV,
    caught_panic_count,
    reset_log_cache,
)

__all__ = [
    "EpicsRsPV",
    "EpicsRsPvaPV",
    "caught_panic_count",
    "get_ca_context",
    "get_pva_context",
    "reset_log_cache",
    "shutdown_all",
    "use_epicsrs",
]


def use_epicsrs(*, logger=None):
    """Install the epics-rs control layer into ophyd.

    Replaces ``ophyd.cl`` with the Rust-backed shim. Must be called
    before importing or constructing any ophyd Signals/Devices, since
    they bind ``ophyd.cl.get_pv`` at construction time.
    """
    import ophyd

    from . import _shim

    _shim.setup(logger or logging.getLogger("ophyd_epicsrs"))

    exports = (
        "setup",
        "caput",
        "caget",
        "get_pv",
        "thread_class",
        "name",
        "release_pvs",
        "get_dispatcher",
    )
    ophyd.cl = types.SimpleNamespace(**{k: getattr(_shim, k) for k in exports})
