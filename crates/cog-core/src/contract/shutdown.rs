use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// A shutdown signal that can be awaited by multiple concurrent tasks.
/// When the shutdown process is triggered (e.g. by SIGTERM), all tasks
/// awaiting the signal will be woken so they can perform graceful cleanup.
#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    inner: Arc<Notify>,
    triggered: Arc<AtomicBool>,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Notify::new()),
            triggered: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Trigger the shutdown signal, waking all waiters.
    pub fn trigger(&self) {
        if self
            .triggered
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.inner.notify_waiters();
        }
    }

    /// Returns true if shutdown has been triggered.
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }

    /// Wait for the shutdown signal.
    pub async fn wait(&self) {
        if self.is_triggered() {
            return;
        }
        self.inner.notified().await;
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Holder so `broadcast::Sender<()>` can be stored in [`PluginContext`].
#[derive(Clone)]
pub struct ShutdownBroadcastTx(pub tokio::sync::broadcast::Sender<()>);
