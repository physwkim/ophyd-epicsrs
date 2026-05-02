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
