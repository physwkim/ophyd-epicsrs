# PVA single `get_value_async` performance

Measured on the mini-beamline IOC over loopback (warm channel,
500 sequential calls, p50). PV: `mini:current` (NTScalar / float64).

| Path | p50 | vs CA sync (71µs) | gather() concurrency |
|---|---|---|---|
| pyo3-async `future_into_py` (default) | **107µs** | +36µs | true async ✓ |
| pyo3-async `future_into_py_fast` (our patch) | **97µs** | +26µs | true async ✓ |
| Resolved-coroutine helper bypass | **87µs** | +16µs | broken (sequential) ✗ |
| p4p sync `ctx.get(pv)` | 87µs | +16µs | n/a (sync API) |
| CA sync `pv.get_with_metadata()` | 71µs | baseline | n/a (sync API) |

## What we kept

`future_into_py_fast` (committed to
[physwkim/pyo3-async-runtimes:perf-opt](https://github.com/physwkim/pyo3-async-runtimes/tree/perf-opt))
strips the bridging machinery that doesn't matter for short-lived,
non-cancellable futures:

- **`add_done_callback`** for Python-side cancel propagation
- **`Cancellable` wrapper** + oneshot cancel channel
- **Outer `R::spawn`** panic-detection wrapper (panic now aborts)
- **`R::scope`** for contextvars propagation across the await boundary

10 µs faster than the default, real async semantics preserved —
`asyncio.gather(*[pv.get_value_async() for pv in pvs])` still issues
all calls concurrently.

## What we rejected

A second prototype (`_async_helpers._resolved`) ran the pvget
synchronously inside the call, then returned an immediately-completed
coroutine. That hit **87µs** — closing the gap to p4p — but turned
`gather()` into sequential blocking iteration:

```
gather(100)  before:  3690µs   true parallel
gather(100)  helper:  6733µs   sequential block (each call blocks loop)
gather(100)  fast:    2445µs   true parallel + lighter wrapper
```

ophyd-async commonly fans signals out via `gather()` (per-Device
parallel `connect()` / `set()`); regressing that path 1.8× to save
10µs on the single-call hot path is the wrong trade. We keep the
helper file (`python/ophyd_epicsrs/_async_helpers.py`) on disk but
do not call it from the binding — left as a reference for future
"fully sync but awaitable" experiments.

## Where the remaining 26 µs comes from

Even with `future_into_py_fast`, the bridge still pays:

1. **`asyncio.create_future()`** + **`Future.set_result()`** — two
   Python method calls (~6 µs each).
2. **`call_soon_threadsafe(CheckedCompletor, future, complete, val)`**
   — set_result is delivered as a scheduled event-loop callback so
   it can cross threads safely. The dispatch tick adds ~5-8 µs.
3. **`CheckedCompletor.__call__(future, complete, value)`** — Python
   pyclass call that re-checks `future.cancelled()` then forwards to
   `Future.set_result(value)`.
4. **`Python::with_gil` from the Tokio worker thread** to hand the
   value back to Python — cross-thread GIL acquire (~3-5 µs).

Eliminating these gets us to p4p's 87 µs. Two paths considered:

- **Direct `Future.set_result(value)` (skip `call_soon_threadsafe`)**.
  Saves ~7 µs but only safe when the Tokio worker is on the same OS
  thread as the asyncio event loop. False with the default
  multi-thread tokio runtime, so unsafe to enable unconditionally.
- **Use a thread-local "asyncio loop" task that drains a lock-free
  queue of completions**. Saves the dispatch tick. Adds setup
  complexity and an extra background task. Not worth it for
  10 µs vs the engineering surface.

The 10 µs gap to p4p is acceptable: p4p is sync (each call blocks
its caller, no gather concurrency at the Python layer either) and
backed by hand-tuned C++. Our 97 µs path is a true asyncio-native
future that scales to thousands of concurrent gets.

## Bulk path is still the answer

For workloads that read more than a handful of PVs, the
`bulk_get` / `bulk_get_async` API on `EpicsRsPvaContext` beats both
single-call paths by an order of magnitude (~72 µs total for 100
PVs vs ~2.5 ms via gather of 100 single calls). Document the
recommended pattern:

```python
# slow — N async calls (per-PV await)
values = await asyncio.gather(*[pv.get_value_async() for pv in pvs])

# fast — single batched call (sync)
values = ctx.bulk_get([pv.pvname for pv in pvs])      # ~0.7 µs/PV at 100 PVs

# fast — single batched call (asyncio / ophyd-async)
values = await ctx.bulk_get_async([pv.pvname for pv in pvs])
```
