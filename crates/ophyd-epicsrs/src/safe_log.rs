//! Panic-guarded primitives for spawned tokio tasks.
//!
//! Background: any path that calls into Python (tracing → pyo3-log →
//! `Python::with_gil`, OR direct `Python::with_gil` for callback
//! invocation) panics if the interpreter has been finalised. This
//! happens routinely during pytest fixture teardown and on regular
//! process exit while tokio background tasks (resubscribe loops,
//! monitor dispatch threads, connection-event watchers) are still
//! running.
//!
//! Sync code that's holding the GIL is safe (the GIL itself proves
//! the interpreter is alive). The danger is only for code paths
//! reachable from spawned tokio tasks / std::thread workers.
//!
//! Three macros:
//! - `safe_warn!` / `safe_debug!`: panic-guard a single tracing call.
//! - `safe_call!`: panic-guard an arbitrary block (typically wraps
//!   `Python::with_gil(...)`).  All callback fires from monitor
//!   dispatch threads and connection-event spawns use this.
//!
//! Each guarded call increments a process-wide counter; the *first*
//! caught panic also writes a one-line notice to stderr (raw fd write,
//! no GIL) so a regression in pyo3-log or our finalize handling
//! doesn't go unnoticed in production.

use std::sync::atomic::{AtomicU64, Ordering};

static PANIC_COUNT: AtomicU64 = AtomicU64::new(0);

/// Number of caught panics since process start. Useful for tests and
/// for an introspection function exposed to Python.
pub fn caught_panic_count() -> u64 {
    PANIC_COUNT.load(Ordering::Relaxed)
}

/// Called from the panic-guard macros. The first panic writes a one-
/// line notice to stderr (no GIL needed); subsequent panics only
/// increment the counter so we don't spam during a finalize storm.
pub fn record_panic() {
    let prev = PANIC_COUNT.fetch_add(1, Ordering::Relaxed);
    if prev == 0 {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr(),
            "[ophyd-epicsrs] caught panic in spawned task — \
             likely Python interpreter finalize. Subsequent panics \
             counted via _native.caught_panic_count() but not logged."
        );
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! safe_warn {
    ($($arg:tt)*) => {{
        let __r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::warn!($($arg)*);
        }));
        if __r.is_err() {
            $crate::safe_log::record_panic();
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! safe_debug {
    ($($arg:tt)*) => {{
        let __r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::debug!($($arg)*);
        }));
        if __r.is_err() {
            $crate::safe_log::record_panic();
        }
    }};
}

/// Panic-guard an arbitrary block — typically `Python::with_gil(...)`
/// inside a spawned task. The block's value is discarded; this macro
/// is only for fire-and-forget side-effecting calls (callback
/// invocation, observer notification).
#[doc(hidden)]
#[macro_export]
macro_rules! safe_call {
    ($($body:tt)+) => {{
        let __r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            $($body)+
        }));
        if __r.is_err() {
            $crate::safe_log::record_panic();
        }
    }};
}
