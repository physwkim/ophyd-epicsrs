//! One monitor dispatcher per client context.
//!
//! libca runs a circuit's callbacks on one thread in wire order and pvxs
//! runs a context's on one loop, so a reply reaches its caller after the
//! monitor callbacks for the updates read before it on every PV, not only
//! its own. epics-rs calls a context's subscription callbacks in wire order
//! and settles a reply after the callbacks for the updates read before it;
//! each callback queues its update here, so this thread runs them in that
//! order, and a reply passes the queue once before reaching Python.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use tokio::sync::oneshot;

thread_local! {
    /// Set on a dispatch thread: a reply awaited from inside a monitor
    /// callback cannot wait for that thread's queue to drain.
    pub(crate) static DISPATCHING: Cell<bool> = const { Cell::new(false) };
}

enum Job {
    Run(Box<dyn FnOnce() + Send>),
    Barrier(oneshot::Sender<()>),
}

pub(crate) struct MonitorHub {
    jobs: mpsc::Sender<Job>,
    /// Run jobs sent and not yet finished.
    queued: Arc<AtomicUsize>,
}

impl MonitorHub {
    pub(crate) fn new(thread_name: &str) -> std::io::Result<Arc<Self>> {
        let (jobs, rx) = mpsc::channel();
        let queued = Arc::new(AtomicUsize::new(0));
        let finished = queued.clone();
        std::thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                DISPATCHING.set(true);
                while let Ok(job) = rx.recv() {
                    match job {
                        Job::Run(run) => {
                            crate::safe_call!(run());
                            finished.fetch_sub(1, Ordering::AcqRel);
                        }
                        Job::Barrier(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            })?;
        Ok(Arc::new(Self { jobs, queued }))
    }

    /// Run `job` on the dispatch thread after every job queued before it.
    pub(crate) fn run(&self, job: impl FnOnce() + Send + 'static) {
        self.queued.fetch_add(1, Ordering::AcqRel);
        let _ = self.jobs.send(Job::Run(Box::new(job)));
    }

    /// The barrier a reply read now passes before reaching Python.
    pub(crate) fn barrier(self: &Arc<Self>, timeout: Duration) -> MonitorBarrier {
        MonitorBarrier {
            hub: (!DISPATCHING.get()).then(|| self.clone()),
            deadline: tokio::time::Instant::now().checked_add(timeout),
        }
    }

    async fn settle(&self) {
        if self.queued.load(Ordering::Acquire) == 0 {
            return;
        }
        let (ack, acked) = oneshot::channel();
        if self.jobs.send(Job::Barrier(ack)).is_ok() {
            let _ = acked.await;
        }
    }
}

/// What a reply waits on before reaching Python: the monitor callbacks for
/// the updates read before it, which libca runs first (`tcpiiu.cpp:1263`).
pub(crate) struct MonitorBarrier {
    hub: Option<Arc<MonitorHub>>,
    /// A callback that never returns holds the reply no longer than the
    /// call's own timeout, where libca's caller gives up too.
    deadline: Option<tokio::time::Instant>,
}

impl MonitorBarrier {
    pub(crate) async fn pass(self) {
        let Some(hub) = self.hub else {
            return;
        };
        match self.deadline {
            Some(deadline) => {
                let _ = tokio::time::timeout_at(deadline, hub.settle()).await;
            }
            None => hub.settle().await,
        }
    }
}
