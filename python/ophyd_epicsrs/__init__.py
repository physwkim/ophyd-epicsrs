"""ophyd-epicsrs: Rust EPICS Channel Access backend for ophyd."""

import logging
import types

from ophyd_epicsrs._native import (
    EpicsRsContext,
    EpicsRsPV,
    caught_panic_count,
    reset_log_cache,
)

__all__ = [
    "EpicsRsContext",
    "EpicsRsPV",
    "caught_panic_count",
    "reset_log_cache",
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
