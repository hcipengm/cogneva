//! Lifecycle Coordinator — bridges Supervisor health events with agent state
//! transitions and checkpoint recovery.
//! (Hook event emission for lifecycle state changes) of the design doc.
//! The coordinator is a thin advisory layer: it receives health reports from
//! the [`HealthChecker`], drives CAS state transitions via the [`StateBackend`],
//! recovers checkpoints for dead-agent task handoff, and emits lifecycle
//! hook events on the Supervisor broadcast channel.

use std::sync::Arc;

use chrono::Utc;
use cog_core::{AgentState, StateBackend, TaskCheckpoint};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::error::SupervisorResult;
use crate::health_checker::HealthReport;
use crate::registry::AgentRegistry;
use cog_core::{HealthIssue, SupervisorEvent};

/// Outcome of a single lifecycle coordination pass.
#[derive(Debug, Clone, Default)]
pub struct LifecycleReport {
    /// Agents whose state was successfully transitioned.
    pub transitioned: Vec<(String, AgentState, AgentState)>,
    /// Agents where the transition failed (concurrent change or invalid).
    pub failed: Vec<(String, String)>,
    /// Checkpoints recovered for task handoff (dead agents).
    pub recovered_checkpoints: Vec<RecoveredCheckpoint>,
    /// Lifecycle hook events emitted.
    pub events_emitted: u32,
}

/// A checkpoint recovered from a dead agent, ready for task re-assignment.
#[derive(Debug, Clone)]
pub struct RecoveredCheckpoint {
    pub agent_id: String,
    pub task_id: String,
    pub checkpoint: TaskCheckpoint,
}

/// Bridges Supervisor health decisions with durable agent state transitions.
/// The coordinator is **advisory** — it does not own the agent process.  It
/// merely updates the canonical state in the [`StateBackend`] so that:
/// 1. Other Supervisor components (Respawner, Rebalancer) see consistent state.
/// 2. The agent's own [`LifecycleManager`] can detect external state changes
///    on its next heartbeat and react accordingly.
/// 3. Checkpoints are recovered when an agent is declared Dead so tasks can
///    be resumed by a replacement agent.
pub struct LifecycleCoordinator {
    registry: Arc<AgentRegistry>,
    state_backend: Arc<dyn StateBackend>,
    event_tx: broadcast::Sender<SupervisorEvent>,
}

impl LifecycleCoordinator {
    pub fn new(
        registry: Arc<AgentRegistry>,
        state_backend: Arc<dyn StateBackend>,
        event_tx: broadcast::Sender<SupervisorEvent>,
    ) -> Self {
        Self {
            registry,
            state_backend,
            event_tx,
        }
    }

    /// Process a [`HealthReport`] and drive state transitions + checkpoint
    /// recovery for every unhealthy agent.
    pub async fn handle_health_report(
        &self,
        report: &HealthReport,
    ) -> SupervisorResult<LifecycleReport> {
        let mut out = LifecycleReport::default();

        // Suspect -> Suspect (or recovery if heartbeat comes back)
        for (agent_id, issue) in &report.suspect {
            if let Err(e) = self
                .transition_if_allowed(agent_id, AgentState::Suspect)
                .await
            {
                out.failed.push((agent_id.clone(), e));
            } else {
                out.transitioned.push((
                    agent_id.clone(),
                    self.previous_state(agent_id)
                        .await
                        .unwrap_or(AgentState::Active),
                    AgentState::Suspect,
                ));
                self.emit_lifecycle_event(agent_id, "suspect", issue).await;
                out.events_emitted += 1;
            }
        }

        // Dead -> Dead + checkpoint recovery
        for (agent_id, issue) in &report.dead {
            let prev = self
                .previous_state(agent_id)
                .await
                .unwrap_or(AgentState::Active);
            if let Err(e) = self.transition_if_allowed(agent_id, AgentState::Dead).await {
                out.failed.push((agent_id.clone(), e));
            } else {
                out.transitioned
                    .push((agent_id.clone(), prev, AgentState::Dead));

                // Recover checkpoints for every task owned by the dead agent.
                if let Some(agent) = self.registry.get_agent(agent_id) {
                    for task_id in &agent.task_ids {
                        match self.state_backend.get_checkpoint(task_id).await {
                            Ok(Some(cp)) => {
                                out.recovered_checkpoints.push(RecoveredCheckpoint {
                                    agent_id: agent_id.clone(),
                                    task_id: task_id.clone(),
                                    checkpoint: cp,
                                });
                            }
                            Ok(None) => {
                                debug!(
                                    agent_id = %agent_id,
                                    task_id = %task_id,
                                    "no checkpoint found for dead agent's task"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    agent_id = %agent_id,
                                    task_id = %task_id,
                                    "checkpoint read failed: {}",
                                    e
                                );
                            }
                        }
                    }
                }

                self.emit_lifecycle_event(agent_id, "dead", issue).await;
                out.events_emitted += 1;
            }
        }

        // Stuck -> Suspect (stuck is a sub-class of unhealthy; we mark Suspect
        // so the operator / automated system can decide whether to kill).
        for (agent_id, issue) in &report.stuck {
            let prev = self
                .previous_state(agent_id)
                .await
                .unwrap_or(AgentState::Active);
            if let Err(e) = self
                .transition_if_allowed(agent_id, AgentState::Suspect)
                .await
            {
                out.failed.push((agent_id.clone(), e));
            } else {
                out.transitioned
                    .push((agent_id.clone(), prev, AgentState::Suspect));
                self.emit_lifecycle_event(agent_id, "stuck", issue).await;
                out.events_emitted += 1;
            }
        }

        Ok(out)
    }

    /// Attempt a state transition only if it is valid according to the
    /// lifecycle state machine.
    async fn transition_if_allowed(
        &self,
        agent_id: &str,
        target: AgentState,
    ) -> Result<(), String> {
        let current = self
            .state_backend
            .get_agent_state(agent_id)
            .await
            .ok()
            .flatten()
            .unwrap_or(AgentState::Init);

        if !Self::is_valid_transition(current, target) {
            return Err(format!(
                "invalid transition {:?} -> {:?} for agent {}",
                current, target, agent_id
            ));
        }

        // Use CAS so we don't clobber a concurrent transition.
        let swapped = self
            .state_backend
            .cas_agent_state(agent_id, &current, &target)
            .await
            .map_err(|e| e.to_string())?;

        if !swapped {
            // Re-read and retry once.
            let current = self
                .state_backend
                .get_agent_state(agent_id)
                .await
                .ok()
                .flatten()
                .unwrap_or(AgentState::Init);
            if !Self::is_valid_transition(current, target) {
                return Err(format!(
                    "invalid transition {:?} -> {:?} for agent {} (retry)",
                    current, target, agent_id
                ));
            }
            let swapped = self
                .state_backend
                .cas_agent_state(agent_id, &current, &target)
                .await
                .map_err(|e| e.to_string())?;
            if !swapped {
                return Err(format!(
                    "CAS failed for agent {} after retry (concurrent change)",
                    agent_id
                ));
            }
        }

        Ok(())
    }

    /// Best-effort read of the previous state (used for the transition log).
    async fn previous_state(&self, agent_id: &str) -> Option<AgentState> {
        self.state_backend
            .get_agent_state(agent_id)
            .await
            .ok()
            .flatten()
    }

    /// Emit a lifecycle hook event on the Supervisor broadcast channel.
    async fn emit_lifecycle_event(&self, agent_id: &str, kind: &str, issue: &HealthIssue) {
        let event = SupervisorEvent::AgentUnhealthy {
            agent_id: agent_id.into(),
            issue: issue.clone(),
            timestamp: Utc::now(),
        };
        let _ = self.event_tx.send(event);

        // Also emit a dedicated lifecycle transition event so that downstream
        // hook handlers (Web UI, audit log, alerting) can react specifically
        // to state changes.
        let lifecycle_event = SupervisorEvent::Tick {
            timestamp: Utc::now(),
            cycle: 0, // lifecycle events are not tied to a health-check cycle
        };
        // We reuse Tick as a lightweight heartbeat; downstream consumers can
        // filter by the preceding AgentUnhealthy event.
        let _ = self.event_tx.send(lifecycle_event);

        debug!(
            agent_id = %agent_id,
            kind = %kind,
            "lifecycle event emitted"
        );
    }

    /// Hard-coded valid transitions matching the design-doc state machine.
    fn is_valid_transition(from: AgentState, to: AgentState) -> bool {
        use AgentState::*;
        match (from, to) {
            (Init, Registered) | (Init, Dead) => true,
            (Registered, Active)
            | (Registered, Idle)
            | (Registered, Inactive)
            | (Registered, Dead) => true,
            (Active, Idle) | (Active, Completing) | (Active, Suspect) | (Active, Dead) => true,
            (Idle, Active) | (Idle, Completing) | (Idle, Suspect) | (Idle, Dead) => true,
            (Completing, Idle) | (Completing, Inactive) | (Completing, Dead) => true,
            (Inactive, Registered) | (Inactive, Active) | (Inactive, Dead) => true,
            (Suspect, Active) | (Suspect, Dead) => true,
            (Dead, _) => false,
            (a, b) if a == b => true,
            _ => false,
        }
    }
}
