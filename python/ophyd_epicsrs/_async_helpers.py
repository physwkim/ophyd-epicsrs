"""Light-weight async helpers used by the Rust binding.

`pyo3_async_runtimes::tokio::future_into_py` is convenient but adds
~25-30µs per call (Python coroutine creation, asyncio dispatch,
cross-thread GIL re-acquire on the Tokio worker thread). For
single-shot ops where the work itself is sub-100µs of network +
decode, that overhead is half the wall time.

When an op already runs synchronously to completion before
returning to Python (the Rust side blocked on a sync mpsc::recv via
``py.allow_threads``), we don't need a real Future — we just need
something the caller can ``await``. A vanilla ``async def`` whose
body is a single ``return value`` does exactly that: ``await
_resolved(v)`` runs the coroutine, hits the ``return``, raises
``StopIteration(v)``, and the await unwraps to ``v``. No yield to
the event loop, no Future scheduling.

Net: matches the sync path's ~80µs vs the future_into_py path's
~110µs for PVA single get on localhost.
"""


async def _resolved(value):
    """Return ``value`` from an immediately-completed coroutine."""
    return value


async def _raise(exc):
    """Raise ``exc`` from an immediately-completed coroutine."""
    raise exc
