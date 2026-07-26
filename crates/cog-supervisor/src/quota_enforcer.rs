use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use cog_core::WorkspaceQuotaSource;

use crate::error::SupervisorResult;
use crate::scheduler_gate::SchedulerGate;

/// Snapshot of a workspace's quota at a moment in time.
#[derive(Debug, Clone)]
pub struct QuotaSnapshot {
    pub workspace_id: String,
    pub remaining: u64,
    pub threshold: u64,
    pub breached: bool,
    pub recovered: bool,
    pub captured_at: DateTime<Utc>,
}

/// Outcome of a single QuotaEnforcer pass.
#[derive(Debug, Clone, Default)]
pub struct QuotaEnforcementReport {
    pub snapshots: Vec<QuotaSnapshot>,
    pub paused: bool,
    pub resumed: bool,
}

/// Per-workspace breach state — used to emit recovery events.
#[derive(Debug, Default, Clone)]
struct WorkspaceState {
    breached: bool,
}

/// Enforces per-workspace token quotas.
/// On each tick the QuotaEnforcer queries every monitored workspace,
/// compares its remaining quota to the configured breach threshold,
/// and toggles the [`SchedulerGate`] accordingly.
/// The scheduler gate is **cooperative**: the autonomous executor in
/// `cogneva` checks `is_paused()` at the top of each tick and
/// short-circuits when set.  This avoids killing in-flight LLM calls
/// while still preventing new tasks from being scheduled.
pub struct QuotaEnforcer {
    quota: Arc<dyn WorkspaceQuotaSource>,
    workspaces: RwLock<Vec<String>>,
    workspace_state: RwLock<HashMap<String, WorkspaceState>>,
    threshold: u64,
    gate: Arc<SchedulerGate>,
}

impl QuotaEnforcer {
    pub fn new(
        quota: Arc<dyn WorkspaceQuotaSource>,
        threshold: u64,
        gate: Arc<SchedulerGate>,
    ) -> Self {
        Self {
            quota,
            workspaces: RwLock::new(Vec::new()),
            workspace_state: RwLock::new(HashMap::new()),
            threshold,
            gate,
        }
    }

    /// Track a new workspace.  Idempotent.
    pub fn track_workspace(&self, workspace_id: impl Into<String>) {
        let workspace_id = workspace_id.into();
        let mut list = self.workspaces.write().expect("workspace list poisoned");
        if !list.iter().any(|id| id == &workspace_id) {
            list.push(workspace_id.clone());
            self.workspace_state
                .write()
                .expect("workspace state poisoned")
                .entry(workspace_id)
                .or_default();
        }
    }

    /// Stop tracking a workspace.
    pub fn untrack_workspace(&self, workspace_id: &str) {
        let mut list = self.workspaces.write().expect("workspace list poisoned");
        list.retain(|id| id != workspace_id);
        self.workspace_state
            .write()
            .expect("workspace state poisoned")
            .remove(workspace_id);
    }

    pub fn tracked_workspaces(&self) -> Vec<String> {
        self.workspaces
            .read()
            .expect("workspace list poisoned")
            .clone()
    }

    /// Run one enforcement pass over the tracked workspaces.
    pub async fn enforce(&self) -> SupervisorResult<QuotaEnforcementReport> {
        let mut report = QuotaEnforcementReport::default();
        let workspaces = {
            self.workspaces
                .read()
                .expect("workspace list poisoned")
                .clone()
        };

        let mut any_breach = false;
        for ws_id in &workspaces {
            let remaining = self.quota.workspace_remaining(ws_id).await;
            let breach = remaining < self.threshold;

            let prev_breached = {
                let state = self
                    .workspace_state
                    .read()
                    .expect("workspace state poisoned");
                state.get(ws_id).map(|s| s.breached).unwrap_or(false)
            };

            // Update state.
            {
                let mut state = self
                    .workspace_state
                    .write()
                    .expect("workspace state poisoned");
                state.entry(ws_id.clone()).or_default().breached = breach;
            }

            let snap = QuotaSnapshot {
                workspace_id: ws_id.clone(),
                remaining,
                threshold: self.threshold,
                breached: breach,
                recovered: prev_breached && !breach,
                captured_at: Utc::now(),
            };
            report.snapshots.push(snap);

            if breach {
                any_breach = true;
            }
        }

        if any_breach {
            let was = self.gate.pause();
            report.paused = !was;
        } else if self.gate.is_paused() {
            self.gate.resume();
            report.resumed = true;
        }

        Ok(report)
    }

    /// Returns `true` if at least one tracked workspace is currently
    /// flagged as breached.
    pub fn any_breach(&self) -> bool {
        let state = self
            .workspace_state
            .read()
            .expect("workspace state poisoned");
        state.values().any(|s| s.breached)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// In-memory quota source backed by a single atomic counter.
    struct MockQuota {
        remaining: AtomicU64,
    }

    impl MockQuota {
        fn new(initial: u64) -> Self {
            Self {
                remaining: AtomicU64::new(initial),
            }
        }

        fn set(&self, value: u64) {
            self.remaining.store(value, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl WorkspaceQuotaSource for MockQuota {
        async fn workspace_remaining(&self, _ws: &str) -> u64 {
            self.remaining.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn track_and_untrack_workspace() {
        let mock = Arc::new(MockQuota::new(100_000));
        let gate = Arc::new(SchedulerGate::new());
        let enforcer = QuotaEnforcer::new(mock, 1_000, gate);
        enforcer.track_workspace("a");
        enforcer.track_workspace("b");
        enforcer.track_workspace("a"); // idempotent
        assert_eq!(enforcer.tracked_workspaces().len(), 2);
        enforcer.untrack_workspace("a");
        assert_eq!(enforcer.tracked_workspaces(), vec!["b".to_string()]);
    }

    #[tokio::test]
    async fn breach_pauses_scheduler() {
        let mock = Arc::new(MockQuota::new(500));
        let gate = Arc::new(SchedulerGate::new());
        let enforcer = QuotaEnforcer::new(mock.clone(), 1_000, gate.clone());
        enforcer.track_workspace("ws-a");

        let report = enforcer.enforce().await.unwrap();
        assert!(report.snapshots[0].breached);
        assert!(report.paused);
        assert!(gate.is_paused());
    }

    #[tokio::test]
    async fn recovery_resumes_scheduler() {
        let mock = Arc::new(MockQuota::new(500));
        let gate = Arc::new(SchedulerGate::new());
        let enforcer = QuotaEnforcer::new(mock.clone(), 1_000, gate.clone());
        enforcer.track_workspace("ws-a");
        enforcer.enforce().await.unwrap();
        assert!(gate.is_paused());

        // Recharge above threshold and run enforce again.
        mock.set(2_000);
        let report = enforcer.enforce().await.unwrap();
        assert!(!report.snapshots[0].breached);
        assert!(report.snapshots[0].recovered);
        assert!(report.resumed);
        assert!(!gate.is_paused());
    }

    #[tokio::test]
    async fn multiple_workspaces_any_breach_pauses() {
        let mock_a = Arc::new(MockQuota::new(2_000));
        // Use a closure-style aggregate: combine sources via single mock
        // by carrying state per ws_id.  Simpler: use one mock and
        // dynamic responses.
        struct PerWs {
            map: std::sync::RwLock<std::collections::HashMap<String, u64>>,
        }

        #[async_trait::async_trait]
        impl WorkspaceQuotaSource for PerWs {
            async fn workspace_remaining(&self, ws: &str) -> u64 {
                self.map
                    .read()
                    .unwrap()
                    .get(ws)
                    .copied()
                    .unwrap_or(u64::MAX)
            }
        }

        let _ = mock_a;
        let mock = Arc::new(PerWs {
            map: std::sync::RwLock::new(
                [
                    ("ws-a".to_string(), 2_000_u64),
                    ("ws-b".to_string(), 500_u64),
                ]
                .into_iter()
                .collect(),
            ),
        });

        let gate = Arc::new(SchedulerGate::new());
        let enforcer = QuotaEnforcer::new(mock.clone(), 1_000, gate.clone());
        enforcer.track_workspace("ws-a");
        enforcer.track_workspace("ws-b");
        let report = enforcer.enforce().await.unwrap();
        assert_eq!(report.snapshots.len(), 2);
        assert!(report.paused);
        assert!(enforcer.any_breach());

        // Lift the breach on ws-b
        mock.map.write().unwrap().insert("ws-b".to_string(), 5_000);
        let report = enforcer.enforce().await.unwrap();
        assert!(report.resumed);
    }
}
