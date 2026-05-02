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
//! Macros:
//! - `safe_warn!` / `safe_debug!`: panic-guard a single tracing call.
//! - `safe_call!`: panic-guard a fire-and-forget block (value
//!   discarded). For monitor dispatch and connection-event spawns.
//! - `safe_call_or!(default, body)`: panic-guard a block whose value
//!   is needed (e.g. `future_into_py` resolving to a `PyObject`); on
//!   panic, returns `default` instead of unwinding the future.
//!
//! Each guarded call increments a process-wide counter; the *first*
//! caught panic also writes a one-line notice to stderr (raw fd
//! write, no GIL) including the captured panic payload so a real bug
//! (not a finalize race) is visible.
//!
//! ## Compile-time enforcement
//!
//! These macros are no-ops if the binary is built with
//! `panic = "abort"`, since `catch_unwind` cannot intercept aborts.
//! The `#[cfg(panic = "abort")] compile_error!` below trips the build
//! immediately so a downstream `Cargo.toml` cannot silently disarm
//! the protections.

#[cfg(panic = "abort")]
compile_error!(
    "ophyd-epicsrs requires panic = \"unwind\". A downstream Cargo.toml \
    has set panic = \"abort\", which would render every catch_unwind in \
    safe_log.rs inert and re-introduce the Python-finalize crash class. \
    See the workspace Cargo.toml comment for details."
);

use std::sync::atomic::{AtomicU64, Ordering};

static PANIC_COUNT: AtomicU64 = AtomicU64::new(0);

/// Number of caught panics since process start. Useful for tests and
/// for an introspection function exposed to Python.
pub fn caught_panic_count() -> u64 {
    PANIC_COUNT.load(Ordering::Relaxed)
}

/// Called from the panic-guard macros. The first panic writes a one-
/// line notice to stderr including the panic payload (no GIL needed);
/// subsequent panics only increment the counter so we don't spam
/// during a finalize storm.
///
/// `payload` is the Box returned by `catch_unwind`'s Err arm. We
/// downcast to the common types (`&'static str`, `String`); other
/// payload types print as `<unknown panic payload>`.
pub fn record_panic(payload: Box<dyn std::any::Any + Send>) {
    let prev = PANIC_COUNT.fetch_add(1, Ordering::Relaxed);
    if prev == 0 {
        use std::io::Write;
        let msg = payload
            .downcast_ref::<&'static str>()
            .copied()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<unknown panic payload>".to_string());
        let _ = writeln!(
            std::io::stderr(),
            "[ophyd-epicsrs] caught panic in spawned task: {msg}\n\
             (typically a Python interpreter finalize race; if you see \
             this during normal operation it may be a real bug — call \
             `ophyd_epicsrs.caught_panic_count()` to track totals)"
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
        if let Err(__payload) = __r {
            $crate::safe_log::record_panic(__payload);
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
        if let Err(__payload) = __r {
            $crate::safe_log::record_panic(__payload);
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
        if let Err(__payload) = __r {
            $crate::safe_log::record_panic(__payload);
        }
    }};
}

/// Panic-guard a block whose value is needed downstream — e.g. a
/// `Python::with_gil` inside `future_into_py` that produces the
/// `PyObject` resolution value. On panic, returns `$default` and
/// records the panic; otherwise returns the block's value.
#[doc(hidden)]
#[macro_export]
macro_rules! safe_call_or {
    ($default:expr, $body:expr) => {{
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $body)) {
            Ok(v) => v,
            Err(__payload) => {
                $crate::safe_log::record_panic(__payload);
                $default
            }
        }
    }};
}
