"""ophyd-epicsrs: Rust EPICS Channel Access backend for ophyd."""

from ophyd_epicsrs._native import EpicsRsContext, EpicsRsPV

__all__ = ["EpicsRsContext", "EpicsRsPV", "use_epicsrs_backend"]


def use_epicsrs_backend():
    """Switch ophyd's control layer to use the epics-rs Rust backend."""
    import ophyd

    ophyd.set_cl("epicsrs")
