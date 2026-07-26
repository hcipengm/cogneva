use cog_core::ShutdownSignal;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub type ShutdownHook = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Coordinates graceful shutdown across multiple components.
/// 1. Listen for OS signals (SIGTERM / SIGINT) or manual trigger.
/// 2. Notify all waiters via [`ShutdownSignal`].
/// 3. Run registered cleanup hooks with a timeout.
/// 4. Exit.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    signal: ShutdownSignal,
    hooks: Arc<Mutex<Vec<ShutdownHook>>>,
    timeout_ms: u64,
}

impl std::fmt::Debug for ShutdownCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownCoordinator")
            .field("signal", &self.signal)
            .field(
                "hooks_count",
                &self.hooks.lock().map(|g| g.len()).unwrap_or(0),
            )
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

impl ShutdownCoordinator {
    pub fn new(timeout_ms: u64) -> Self {
        Self {
            signal: ShutdownSignal::new(),
            hooks: Arc::new(Mutex::new(Vec::new())),
            timeout_ms,
        }
    }

    /// Register an async cleanup hook.
    pub fn register_hook<F, Fut>(&self, hook: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let boxed: ShutdownHook = Box::new(move || Box::pin(hook()));
        let mut hooks = self.hooks.lock().unwrap_or_else(|e| e.into_inner());
        hooks.push(boxed);
    }

    /// Get a clone of the shutdown signal to pass to sub-tasks.
    pub fn signal(&self) -> ShutdownSignal {
        self.signal.clone()
    }

    /// Start a background task that listens for OS signals and triggers shutdown.
    pub fn spawn_signal_listener(&self) -> JoinHandle<()> {
        let signal = self.signal.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = match signal(SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("Failed to register SIGTERM handler: {}", e);
                        return;
                    }
                };
                let mut sigint = match signal(SignalKind::interrupt()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("Failed to register SIGINT handler: {}", e);
                        return;
                    }
                };
                tokio::select! {
                    _ = sigterm.recv() => {},
                    _ = sigint.recv() => {},
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            signal.trigger();
        })
    }

    /// Trigger shutdown manually (e.g. from a health/admin endpoint).
    #[allow(dead_code)]
    pub fn trigger(&self) {
        self.signal.trigger();
    }

    /// Wait for shutdown to be triggered, then run all hooks with timeout.
    pub async fn wait_for_shutdown(&self) {
        self.signal.wait().await;

        let hooks = {
            let mut h = self.hooks.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *h)
        };

        if hooks.is_empty() {
            return;
        }

        let timeout = tokio::time::Duration::from_millis(self.timeout_ms);
        let futures: Vec<_> = hooks.into_iter().map(|h| h()).collect();

        let result = tokio::time::timeout(timeout, futures::future::join_all(futures)).await;
        if result.is_err() {
            // Hooks timed out; we exit anyway to avoid hanging.
        }
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new(10_000) // 10s default timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_shutdown_signal_trigger() {
        let sig = ShutdownSignal::new();
        assert!(!sig.is_triggered());

        sig.trigger();
        assert!(sig.is_triggered());

        // Wait should return immediately since already triggered.
        sig.wait().await;
    }

    #[tokio::test]
    async fn test_shutdown_signal_multiple_waiters() {
        let sig = ShutdownSignal::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..3 {
            let s = sig.clone();
            let c = counter.clone();
            handles.push(tokio::spawn(async move {
                s.wait().await;
                c.fetch_add(1, Ordering::SeqCst);
            }));
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        sig.trigger();
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_hooks_run() {
        let coord = ShutdownCoordinator::new(5000);
        let counter = Arc::new(AtomicUsize::new(0));

        let c = counter.clone();
        coord.register_hook(move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        coord.trigger();
        coord.wait_for_shutdown().await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_hook_timeout() {
        let coord = ShutdownCoordinator::new(50); // very short timeout
        coord.register_hook(|| async {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        });

        coord.trigger();
        // Should not hang; returns after timeout.
        tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            coord.wait_for_shutdown(),
        )
        .await
        .expect("wait_for_shutdown should not hang");
    }
}
