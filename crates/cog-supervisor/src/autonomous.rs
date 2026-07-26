//! Autonomous collaboration engine for the Supervisor.
//! Watches AgentEvents and automatically triggers downstream actions:
//! - Task hand-off (Agent A completes → Agent B starts)
//! - Failure recovery (retry / dead-letter / migration)
//! - Load-based squad scaling

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use cog_core::{AgentEvent, OrchestratorControl, StateBackend};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::scheduler_gate::SchedulerGate;
use cog_core::SupervisorEvent;

/// Threshold for pending-handoffs / active-agents ratio that triggers a
/// scale-out recommendation in [`AutonomousCollaborator::run_decision_pass`].
const LOAD_RATIO_THRESHOLD: f64 = 0.8;

/// Configuration for autonomous collaboration decisions.
#[derive(Debug, Clone)]
pub struct AutonomousConfig {
    /// Max retries before a task is sent to the dead-letter queue.
    pub max_task_retries: u32,
    /// Interval between autonomous decision passes.
    pub decision_interval_secs: u64,
}

impl Default for AutonomousConfig {
    fn default() -> Self {
        Self {
            max_task_retries: 3,
            decision_interval_secs: 5,
        }
    }
}

/// AutonomousCollaborator makes编排 decisions without human intervention.
pub struct AutonomousCollaborator {
    config: AutonomousConfig,
    _gate: Arc<SchedulerGate>,
    _state_backend: Arc<dyn StateBackend>,
    orchestrator: Arc<dyn OrchestratorControl>,
    event_tx: broadcast::Sender<SupervisorEvent>,
    /// Pending downstream tasks waiting for a predecessor to complete.
    pending_handoffs: tokio::sync::Mutex<HashMap<String, Vec<cog_core::Task>>>,
    /// Retry counter per task_id.
    retry_counts: tokio::sync::Mutex<HashMap<String, u32>>,
    /// Optional meta-learning engine for strategy optimisation.
    meta_learning: Option<Arc<dyn cog_core::MetaLearning>>,
}

/// Async event consumer for the autonomous collaborator.
pub async fn run_autonomous_event_loop(
    collaborator: Arc<AutonomousCollaborator>,
    mut rx: broadcast::Receiver<cog_core::AgentEvent>,
) {
    while let Ok(event) = rx.recv().await {
        collaborator.handle_event(&event).await;
    }
}

impl AutonomousCollaborator {
    pub fn new(
        config: AutonomousConfig,
        gate: Arc<SchedulerGate>,
        state_backend: Arc<dyn StateBackend>,
        orchestrator: Arc<dyn OrchestratorControl>,
        event_tx: broadcast::Sender<SupervisorEvent>,
    ) -> Self {
        Self {
            config,
            _gate: gate,
            _state_backend: state_backend,
            orchestrator,
            event_tx,
            pending_handoffs: tokio::sync::Mutex::new(HashMap::new()),
            retry_counts: tokio::sync::Mutex::new(HashMap::new()),
            meta_learning: None,
        }
    }

    pub fn with_meta_learning(mut self, meta: Arc<dyn cog_core::MetaLearning>) -> Self {
        self.meta_learning = Some(meta);
        self
    }

    /// Register a task hand-off: when `predecessor_task_id` completes,
    /// `successor_task` should be scheduled.
    pub async fn register_handoff(
        &self,
        predecessor_task_id: impl Into<String>,
        successor_task: cog_core::Task,
    ) {
        let mut map = self.pending_handoffs.lock().await;
        map.entry(predecessor_task_id.into())
            .or_default()
            .push(successor_task);
    }

    /// Process a single AgentEvent and make autonomous decisions.
    pub async fn handle_event(&self, event: &AgentEvent) {
        match event {
            AgentEvent::TaskStatusChange {
                task_id,
                status,
                agent_id,
                crew_id,
                ..
            } => {
                let status_lower = status.to_lowercase();
                if status_lower == "completed" || status_lower == "success" {
                    self.on_task_complete(task_id, agent_id.as_deref(), crew_id.as_deref())
                        .await;
                } else if status_lower == "failed" || status_lower == "error" {
                    self.on_task_fail(task_id, agent_id.as_deref(), crew_id.as_deref())
                        .await;
                }
            }
            AgentEvent::AgentStart {
                agent_id,
                crew_id,
                squad_id,
                ..
            } => {
                self.on_agent_start(agent_id, crew_id.as_deref(), squad_id.as_deref())
                    .await;
            }
            AgentEvent::AgentEnd {
                agent_id,
                crew_id,
                squad_id,
                ..
            } => {
                self.on_agent_end(agent_id, crew_id.as_deref(), squad_id.as_deref())
                    .await;
            }
            _ => {}
        }
    }

    async fn on_task_complete(&self, task_id: &str, agent_id: Option<&str>, crew_id: Option<&str>) {
        info!(
            "Autonomous: task {} completed by agent {:?}. Checking hand-offs...",
            task_id, agent_id
        );

        // Trigger downstream hand-offs.
        let successors = {
            let mut map = self.pending_handoffs.lock().await;
            map.remove(task_id).unwrap_or_default()
        };

        for successor_task in successors {
            let successor_id = successor_task.id.clone();
            info!(
                "Autonomous: handing off {} → {} (direct execution)",
                task_id, successor_id
            );

            // Direct execution: add successor task to orchestrator
            if let Err(e) = self.orchestrator.add_task(successor_task).await {
                warn!(
                    "Autonomous: failed to add successor task {}: {}",
                    successor_id, e
                );
            } else {
                info!(
                    "Autonomous: successor task {} added to orchestrator",
                    successor_id
                );
            }

            let _ = self.event_tx.send(SupervisorEvent::TaskHandOff {
                predecessor_task_id: task_id.into(),
                successor_task_id: successor_id.clone(),
                agent_id: agent_id.map(|s| s.into()),
                crew_id: crew_id.map(|s| s.into()),
                timestamp: Utc::now(),
            });
        }

        // Record success to meta-learning if the task was previously retried.
        let had_retries = {
            let retries = self.retry_counts.lock().await;
            retries.contains_key(task_id)
        };
        if had_retries {
            if let Some(ref meta) = self.meta_learning {
                if let Some(features) = self.task_features_for(task_id).await {
                    let _ = meta
                        .record(
                            cog_core::DecisionCategory::RetryPolicy,
                            &features,
                            "identical_retry",
                            cog_core::DecisionOutcome::Success,
                        )
                        .await;
                }
            }
        }

        // Clear retry count on success.
        let mut retries = self.retry_counts.lock().await;
        retries.remove(task_id);
    }

    async fn on_task_fail(&self, task_id: &str, agent_id: Option<&str>, crew_id: Option<&str>) {
        let mut retries = self.retry_counts.lock().await;
        let count = retries.entry(task_id.into()).or_insert(0);
        *count += 1;

        // Build task features and query meta-learning for retry strategy.
        let ml_recommendation = if let Some(ref meta) = self.meta_learning {
            if let Some(features) = self.task_features_for(task_id).await {
                let decision = meta
                    .recommend(cog_core::DecisionCategory::RetryPolicy, &features)
                    .await;
                if let Some(ref d) = decision {
                    info!(
                        task_id = %task_id,
                        recommendation = %d,
                        "Meta-learning retry recommendation"
                    );
                }
                decision
            } else {
                None
            }
        } else {
            None
        };

        let strategy = ml_recommendation.as_deref().unwrap_or("identical_retry");

        if *count > self.config.max_task_retries {
            warn!(
                "Autonomous: task {} failed {} times, sending to dead-letter queue",
                task_id, count
            );

            // Direct execution: push to dead-letter queue in orchestrator
            if let Err(e) = self
                .orchestrator
                .push_to_dlq(
                    task_id,
                    format!(
                        "exceeded max retries ({}) after {} attempts",
                        self.config.max_task_retries, count
                    ),
                )
                .await
            {
                warn!("Autonomous: failed to push task {} to DLQ: {}", task_id, e);
            }

            let _ = self.event_tx.send(SupervisorEvent::TaskDeadLetter {
                task_id: task_id.into(),
                agent_id: agent_id.map(|s| s.into()),
                crew_id: crew_id.map(|s| s.into()),
                retry_count: *count,
                timestamp: Utc::now(),
            });

            // Record failed outcome to meta-learning.
            if let Some(ref meta) = self.meta_learning {
                if let Some(features) = self.task_features_for(task_id).await {
                    let _ = meta
                        .record(
                            cog_core::DecisionCategory::RetryPolicy,
                            &features,
                            strategy,
                            cog_core::DecisionOutcome::Failed,
                        )
                        .await;
                }
            }

            retries.remove(task_id);
        } else {
            info!(
                "Autonomous: task {} failed, retry {}/{} (strategy={})",
                task_id, count, self.config.max_task_retries, strategy
            );

            // Direct execution: retry task in orchestrator
            if let Err(e) = self.orchestrator.retry_task(task_id).await {
                warn!("Autonomous: failed to retry task {}: {}", task_id, e);
            } else {
                info!("Autonomous: task {} retried in orchestrator", task_id);
            }

            let _ = self.event_tx.send(SupervisorEvent::TaskRetry {
                task_id: task_id.into(),
                agent_id: agent_id.map(|s| s.into()),
                crew_id: crew_id.map(|s| s.into()),
                retry_count: *count,
                timestamp: Utc::now(),
            });

            // Record retry decision to meta-learning.
            if let Some(ref meta) = self.meta_learning {
                if let Some(features) = self.task_features_for(task_id).await {
                    let _ = meta
                        .record(
                            cog_core::DecisionCategory::RetryPolicy,
                            &features,
                            strategy,
                            cog_core::DecisionOutcome::Escalated,
                        )
                        .await;
                }
            }
        }
    }

    async fn on_agent_start(&self, agent_id: &str, crew_id: Option<&str>, squad_id: Option<&str>) {
        debug!(
            "Autonomous: agent {} started (crew={:?}, squad={:?})",
            agent_id, crew_id, squad_id
        );
        // Future: evaluate load and spawn additional agents if needed.
    }

    async fn on_agent_end(&self, agent_id: &str, crew_id: Option<&str>, squad_id: Option<&str>) {
        debug!(
            "Autonomous: agent {} ended (crew={:?}, squad={:?})",
            agent_id, crew_id, squad_id
        );
        // Future: release resources and re-evaluate squad composition.
    }

    /// Return the total number of pending handoff tasks.
    pub async fn pending_count(&self) -> usize {
        let map = self.pending_handoffs.lock().await;
        map.values().map(|v| v.len()).sum()
    }

    /// Return the total number of tracked retry entries.
    pub async fn retry_count(&self) -> usize {
        let map = self.retry_counts.lock().await;
        map.len()
    }

    /// Run a periodic decision pass (independent of events).
    /// Evaluates the pending-handoffs / ready-tasks ratio and emits a
    /// scale-out event when the load exceeds [`LOAD_RATIO_THRESHOLD`].
    pub async fn run_decision_pass(&self) {
        let pending = self.pending_count().await;
        let ready = self.orchestrator.get_ready_tasks().await.len();

        if ready == 0 {
            debug!("Autonomous: decision pass — no ready tasks");
            return;
        }

        let ratio = pending as f64 / ready as f64;
        if ratio > LOAD_RATIO_THRESHOLD {
            info!(
                pending = pending,
                ready = ready,
                ratio = %format!("{:.2}", ratio),
                "Autonomous: load ratio exceeds threshold — recommending scale-out"
            );
            let _ = self.event_tx.send(SupervisorEvent::ScaleRecommendation {
                reason: format!(
                    "pending_handoffs={} / ready_tasks={} ratio {:.2} > threshold {:.2}",
                    pending, ready, ratio, LOAD_RATIO_THRESHOLD
                ),
                recommended_agents: (pending as u32).saturating_sub(ready as u32) + 1,
                timestamp: Utc::now(),
            });
        } else {
            debug!(
                pending = pending,
                ready = ready,
                ratio = %format!("{:.2}", ratio),
                "Autonomous: decision pass — load within bounds"
            );
        }
    }

    /// Look up a task in the orchestrator and build simplified [`TaskFeatures`]
    /// for the meta-learning engine.
    async fn task_features_for(&self, task_id: &str) -> Option<cog_core::TaskFeatures> {
        let tasks = self.orchestrator.get_all_tasks().await;
        let task = tasks.into_iter().find(|t| t.id == task_id)?;
        let goal = task
            .input
            .get("goal")
            .and_then(|v| v.as_str())
            .unwrap_or(&task.id)
            .to_string();
        Some(cog_core::TaskFeatures {
            task_type: format!("{:?}", task.task_type),
            domain_tags: vec![goal],
            estimated_complexity: 5.0,
            has_external_dependencies: !task.blocked_by.is_empty(),
            historical_success_rate: 0.5,
            required_skills: Vec::new(),
        })
    }
}
