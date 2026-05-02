//! Panic-guarded `tracing` macros for use inside spawned tokio tasks.
//!
//! Background: pyo3-log forwards every `log::Record` (and via tracing's
//! "log" feature, every tracing event) into Python's logging module
//! through `Python::with_gil`. That panics if the Python interpreter
//! has been finalised — which happens routinely during pytest fixture
//! teardown and on regular process exit while tokio background tasks
//! (resubscribe loops, monitor streams, connection-event watchers) are
//! still running.
//!
//! Sync code that's holding the GIL is safe (the GIL itself proves the
//! interpreter is alive). The danger is only for code paths reachable
//! from spawned tokio tasks. Those sites use `safe_warn!` /
//! `safe_debug!` instead of the raw `tracing::*!` macros — the panic
//! is caught and discarded, so a finalising interpreter doesn't take
//! down the runtime.
//!
//! Use the raw `tracing::*!` macros everywhere else.

#[macro_export]
macro_rules! safe_warn {
    ($($arg:tt)*) => {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::warn!($($arg)*);
        }));
    };
}

#[macro_export]
macro_rules! safe_debug {
    ($($arg:tt)*) => {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::debug!($($arg)*);
        }));
    };
}
