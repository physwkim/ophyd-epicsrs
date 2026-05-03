"""Process-wide singleton CA / PVA contexts.

Each `EpicsRsContext` / `EpicsRsPvaContext` constructs a Rust
`CaClient` / `PvaClient`, which spawns its own beacon monitor (CA),
search engine, transport coordinator, and registers a fresh UDP
socket with the local CA repeater. Two pieces of this package — the
pyepics-compat shim (`_shim.py`) and the ophyd-async backend
(`_signal_backend.py`) — used to construct their own independent
contexts, and tests added a third via fixtures. With three CA clients
in one process, every IOC's first beacon hit each client's empty
`servers` map and tripped the `first_sighting=true` anomaly path in
`epics-ca-rs/src/client/beacon_monitor.rs:327`. That fired an
`EchoProbe` against every operational channel; under load (test
suites, many concurrent puts/gets), the IOC could miss the 5 s echo
deadline → `TcpClosed` → `handle_disconnect` → "restored N
subscriptions" reconnect storm and timed-out gets/puts.

Sharing one context per protocol per process collapses N anomaly
storms into 1 (still fires once, harmless) and removes the redundant
beacon/search/transport tasks. Construction is lazy — code paths that
only touch CA never spin up the PVA runtime, and vice versa.
"""

from __future__ import annotations

import threading

from ophyd_epicsrs._native import EpicsRsContext, EpicsRsPvaContext

_lock = threading.Lock()
_ca_context: EpicsRsContext | None = None
_pva_context: EpicsRsPvaContext | None = None


def get_ca_context() -> EpicsRsContext:
    """Return the process-wide shared CA context, creating it on first call."""
    global _ca_context
    if _ca_context is None:
        with _lock:
            if _ca_context is None:
                _ca_context = EpicsRsContext()
    return _ca_context


def get_pva_context() -> EpicsRsPvaContext:
    """Return the process-wide shared PVA context, creating it on first call."""
    global _pva_context
    if _pva_context is None:
        with _lock:
            if _pva_context is None:
                _pva_context = EpicsRsPvaContext()
    return _pva_context


def shutdown_all() -> None:
    """Drop the cached singleton CA / PVA contexts.

    Intended for long-running daemons that import ``ophyd_epicsrs`` early
    but only use it during a bounded window — calling this after every PV
    has been released frees the Rust runtime + beacon monitor + repeater
    socket. The Rust ``Drop`` impls handle background-task teardown.

    **Refuses while active PVs exist**. Each context exposes
    ``is_unused()``; if either still holds live ``EpicsRsPV`` /
    ``EpicsRsPvaPV`` wrappers, ``RuntimeError`` is raised. This guards
    against the silent multi-client regression that otherwise lurks:
    after ``shutdown_all`` the singleton slot is empty, but Rust-side
    the old ``CaClient`` stays alive (PVs strongly reference it). The
    next ``get_ca_context()`` would construct a *new* ``CaClient``,
    re-triggering the ``first_sighting`` beacon-anomaly chain that
    sharing singletons (commit 001c605) was meant to prevent.

    Drop order: clear the singleton slots inside the lock, then drop
    the contexts *outside* it so a slow Rust ``Drop`` doesn't block
    other threads' ``get_*_context()`` calls.
    """
    global _ca_context, _pva_context
    with _lock:
        ca, pva = _ca_context, _pva_context
        if ca is not None and not ca.is_unused():
            raise RuntimeError(
                "shutdown_all: CA context still has active EpicsRsPV "
                "wrappers — release every PV first, otherwise the next "
                "get_ca_context() would silently construct a second "
                "CaClient and re-trigger the multi-client beacon-anomaly bug."
            )
        if pva is not None and not pva.is_unused():
            raise RuntimeError(
                "shutdown_all: PVA context still has active EpicsRsPvaPV "
                "wrappers — release every PV first."
            )
        _ca_context = None
        _pva_context = None
    # Drop outside the lock so any lengthy Rust teardown (background
    # task drain, socket close) doesn't block concurrent get_*_context().
    del ca
    del pva
