"""ophyd control layer shim backed by epics-rs (Rust CA client via PyO3).

This replaces pyepics with a pure-Rust Channel Access implementation.
All CA network I/O runs in a tokio runtime with the GIL released.

Loaded into ophyd by calling ``ophyd_epicsrs.install()``.
"""

import atexit
import logging
import threading

from ophyd._dispatch import EventDispatcher, _CallbackThread, wrap_callback

from ophyd_epicsrs._native import EpicsRsContext, EpicsRsPvaContext

name = "epicsrs"
thread_class = threading.Thread

module_logger = logging.getLogger(__name__)

_context = None
_pva_context = None
_dispatcher = None


# Protocol prefixes follow pvxs / ophyd-async convention:
#   "pva://NAME" → PVA backend
#   "ca://NAME"  → CA backend (prefix stripped)
#   "NAME"       → CA backend (default — preserves ophyd backward compat)
_PVA_PREFIX = "pva://"
_CA_PREFIX = "ca://"


def _split_protocol(pvname):
    """Return (protocol, bare_name) for a possibly-prefixed PV name."""
    if pvname.startswith(_PVA_PREFIX):
        return "pva", pvname[len(_PVA_PREFIX):]
    if pvname.startswith(_CA_PREFIX):
        return "ca", pvname[len(_CA_PREFIX):]
    return "ca", pvname


def _cleanup():
    global _context, _pva_context, _dispatcher
    if _dispatcher is not None and _dispatcher.is_alive():
        _dispatcher.stop()
    _dispatcher = None
    _context = None
    _pva_context = None


def get_dispatcher():
    return _dispatcher


def setup(logger):
    global _context, _dispatcher

    if _dispatcher is not None:
        if logger:
            logger.debug("epicsrs already setup")
        return _dispatcher

    _context = EpicsRsContext()
    # PVA context is created lazily on first use to avoid spawning a
    # second runtime when the user only uses CA.

    if logger:
        logger.debug("Installing epicsrs event dispatcher")

    _dispatcher = EventDispatcher(
        thread_class=_CallbackThread,
        context=None,
        logger=logger or module_logger,
    )
    atexit.register(_cleanup)
    return _dispatcher


def _get_pva_context():
    """Lazily construct the PVA context on first use."""
    global _pva_context
    if _pva_context is None:
        _pva_context = EpicsRsPvaContext()
    return _pva_context


def _process_pending(dispatcher, timeout=2.0):
    """Block until all dispatcher queues drain.

    Mirrors EventDispatcher.process_pending from forks that have it; falls
    back to a direct sentinel insertion using the documented `_threads` /
    `_utility_threads` layout when vanilla ophyd is in use.
    """
    fn = getattr(dispatcher, "process_pending", None)
    if fn is not None:
        return fn(timeout=timeout)
    events = []
    util = set(getattr(dispatcher, "_utility_threads", ()))
    for tname, thread in dispatcher._threads.items():
        if tname in util:
            continue
        ev = threading.Event()
        thread.queue.put((ev.set, (), {}))
        events.append(ev)
    for ev in events:
        ev.wait(timeout=timeout)


class EpicsRsShimPV:
    """ophyd PV wrapper backed by Rust EpicsRsPV. Mirrors the pyepics
    PV surface that ophyd Signals depend on.
    """

    def __init__(
        self,
        pvname,
        form="time",
        auto_monitor=None,
        connection_callback=None,
        callback=None,
        count=None,
        connection_timeout=None,
        access_callback=None,
        verbose=False,
        monitor_delta=None,
        _native_pv=None,
    ):
        # _native_pv lets get_pv() inject a pre-created PVA channel so this
        # class can wrap either CA or PVA backends with the same surface.
        self._pv = _native_pv if _native_pv is not None else _context.create_pv(pvname)
        self.pvname = pvname
        self.form = form
        self.auto_monitor = auto_monitor
        self.connected = False
        self._reference_count = 0
        self._cache_key = None
        self._args = {}
        self._callbacks = {}
        self._next_cb_index = 0
        self._conn_callbacks = []
        self.access_callbacks = []
        self.connection_callbacks = []
        self.chid = None
        self.context = None

        if connection_callback:
            self._conn_callbacks.append(connection_callback)
        if access_callback:
            self.access_callbacks.append(access_callback)
        if callback:
            self.add_callback(callback)

        self._pv.set_connection_callback(self._on_connection_change)
        self._pv.set_access_callback(self._on_access_change)

    def _on_connection_change(self, connected):
        # Deduplicate: the Rust layer fires this from both
        # emit_current_connection_state (one-shot probe) and the
        # connection_events() stream, plus wait_for_connection's
        # explicit invocation when self.connected is False. Without
        # this guard the user's connection_callback could be called
        # 2-3x for a single physical connect.
        if self.connected == connected:
            return
        self.connected = connected
        if connected and self.auto_monitor:
            self._pv.add_monitor_callback(self._on_monitor_update)
        for cb in self._conn_callbacks:
            cb(pvname=self.pvname, conn=connected, pv=self)

    def _on_access_change(self, read_access, write_access):
        for cb in self.access_callbacks:
            cb(read_access=read_access, write_access=write_access, pv=self)

    def wait_for_connection(self, timeout=None):
        result = self._pv.wait_for_connection(timeout=timeout or 5.0)
        if result and not self.connected:
            self._on_connection_change(True)
        if result:
            if self._args.get("value") is None:
                md = self._pv.get_with_metadata(timeout=timeout or 2.0, form=self.form)
                if md:
                    self._args.update(md)
            if _dispatcher is not None:
                _process_pending(_dispatcher)
        return result

    def get(
        self,
        as_string=False,
        count=None,
        as_numpy=True,
        timeout=None,
        use_monitor=True,
    ):
        if use_monitor and self._args.get("value") is not None:
            return self._args["value"]
        md = self._pv.get_with_metadata(timeout=timeout or 2.0, form=self.form)
        if md is not None:
            self._args.update(md)
            return md["value"]
        return None

    def get_with_metadata(
        self,
        as_string=False,
        count=None,
        form=None,
        timeout=None,
        use_monitor=False,
    ):
        if use_monitor and self._args.get("value") is not None:
            return self._args.copy()
        form = form or self.form
        md = self._pv.get_with_metadata(timeout=timeout or 2.0, form=form)
        if md is not None:
            self._args.update(md)
        return md

    def put(
        self,
        value,
        wait=False,
        timeout=None,
        use_complete=False,
        callback=None,
        callback_data=None,
    ):
        if callback:
            use_complete = True
            callback = wrap_callback(_dispatcher, "get_put", callback)
        if timeout is None:
            timeout = 315569520
        effective_wait = wait or use_complete
        self._pv.put(value, wait=effective_wait, timeout=timeout)
        if callback:
            if callback_data is not None:
                callback(pvname=self.pvname, data=callback_data)
            else:
                callback(pvname=self.pvname)

    def add_callback(
        self,
        callback=None,
        index=None,
        run_now=False,
        with_ctrlvars=True,
        **kw,
    ):
        if not self.auto_monitor:
            self.auto_monitor = True
        if index is None:
            index = self._next_cb_index
            self._next_cb_index += 1
        self._callbacks[index] = callback
        self._pv.add_monitor_callback(self._on_monitor_update)
        if run_now and self.connected and self._args:
            callback(pvname=self.pvname, **self._args)
        return index

    def remove_callback(self, index):
        self._callbacks.pop(index, None)

    def _on_monitor_update(self, **kwargs):
        self._args.update({k: v for k, v in kwargs.items() if k != "pvname"})
        for cb in self._callbacks.values():
            try:
                cb(**kwargs)
            except Exception:
                module_logger.exception(
                    "Exception in monitor callback for %s", self.pvname
                )

    def get_timevars(self, timeout=None, warn=True):
        md = self._pv.get_timevars(timeout=timeout or 1.0)
        if md:
            self._args.update(md)
        return md

    def get_ctrlvars(self, timeout=None, warn=True):
        md = self._pv.get_ctrlvars(timeout=timeout or 1.0)
        if md:
            self._args.update(md)
        return md

    def get_all_metadata_blocking(self, timeout):
        self.get_timevars(timeout=timeout)
        self.get_ctrlvars(timeout=timeout)
        md = self._args.copy()
        md.pop("value", None)
        return md

    def get_all_metadata_callback(self, callback, *, timeout):
        def _task(pvname):
            md = self.get_all_metadata_blocking(timeout=timeout)
            callback(pvname, md)

        _dispatcher.schedule_utility_task(_task, pvname=self.pvname)

    def clear_callbacks(self):
        self._callbacks.clear()
        self._conn_callbacks.clear()
        self.access_callbacks.clear()
        self.connection_callbacks.clear()
        self._pv.clear_monitors()

    def clear_auto_monitor(self):
        self.auto_monitor = None
        self._pv.clear_monitors()

    def disconnect(self):
        self._pv.disconnect()

    def _getarg(self, arg):
        if self._args.get(arg) is None:
            if arg in (
                "status",
                "severity",
                "timestamp",
                "posixseconds",
                "nanoseconds",
            ):
                self.get_timevars(timeout=1)
            else:
                self.get_ctrlvars(timeout=1)
        return self._args.get(arg, None)

    def __repr__(self):
        return f"EpicsRsShimPV('{self.pvname}', connected={self.connected})"


def get_pv(
    pvname,
    form="time",
    connect=False,
    context=None,
    timeout=5.0,
    auto_monitor=None,
    connection_callback=None,
    access_callback=None,
    callback=None,
    count=None,
    connection_timeout=None,
    verbose=False,
    monitor_delta=None,
    **kwargs,
):
    """Create a fresh EpicsRsShimPV.

    The protocol is selected by PV-name prefix:
      * ``pva://NAME`` → PVA backend
      * ``ca://NAME``  → CA backend (prefix stripped)
      * ``NAME``       → CA backend (default — preserves backward compat)

    Unlike pyepics we do NOT cache PV objects, because the Rust runtime
    already shares one transport circuit per IOC. Skipping the cache
    avoids subscription-resolution bugs when multiple ophyd Devices
    reference the same PV name.
    """
    connection_callback = wrap_callback(_dispatcher, "metadata", connection_callback)
    callback = wrap_callback(_dispatcher, "monitor", callback)
    access_callback = wrap_callback(_dispatcher, "metadata", access_callback)

    protocol, bare_name = _split_protocol(pvname)
    native_pv = None
    if protocol == "pva":
        native_pv = _get_pva_context().create_pv(bare_name)

    # Keep the ORIGINAL pvname (with `pva://` / `ca://` prefix) on the
    # shim wrapper. ophyd indexes per-pv state (``_received_first_metadata``,
    # ``_signals``) by the pvname string ophyd was originally handed; if
    # the shim reported the stripped name, those lookups would KeyError
    # the moment the connection callback fires. The native PV underneath
    # is created from ``bare_name`` so the actual EPICS request goes to
    # the right place.
    pv = EpicsRsShimPV(
        pvname,
        form=form,
        auto_monitor=auto_monitor,
        connection_callback=connection_callback,
        callback=callback,
        count=count,
        connection_timeout=connection_timeout,
        access_callback=access_callback,
        verbose=verbose,
        monitor_delta=monitor_delta,
        _native_pv=native_pv,
    )
    if connect:
        pv.wait_for_connection(timeout=timeout)
    return pv


def caget(pvname, **kwargs):
    pv = get_pv(pvname)
    pv.wait_for_connection(timeout=kwargs.get("timeout", 5.0))
    md = pv.get_with_metadata(timeout=kwargs.get("timeout", 5.0))
    return md["value"] if md else None


def caput(pvname, value, **kwargs):
    pv = get_pv(pvname)
    pv.wait_for_connection(timeout=kwargs.get("timeout", 5.0))
    pv.put(value, wait=True, timeout=kwargs.get("timeout", 300))


def release_pvs(*pvs):
    for pv in pvs:
        pv.clear_callbacks()
        pv.clear_auto_monitor()
        pv.disconnect()


__all__ = (
    "setup",
    "caput",
    "caget",
    "get_pv",
    "thread_class",
    "name",
    "release_pvs",
    "get_dispatcher",
)
