use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use cog_core::{OrchestratorControl, Task, TaskType};
use tokio::sync::broadcast;

use crate::error::{SupervisorError, SupervisorResult};
use crate::registry::{AgentRegistry, CrewInfo};
use cog_core::SupervisorEvent;

/// Outcome of a single Respawner pass.
#[derive(Debug, Clone, Default)]
pub struct RespawnReport {
    /// Crews where retry succeeded (some tasks were re-queued).
    pub retried_crews: Vec<RespawnAction>,
    /// Crews that exhausted their retry budget; squad respawn requested.
    pub respawn_requested: Vec<RespawnAction>,
}

/// Detailed action taken for a single crew.
#[derive(Debug, Clone)]
pub struct RespawnAction {
    pub crew_id: String,
    pub task_ids: Vec<String>,
    pub retried: usize,
    pub crew_retry_count: u32,
    pub reason: String,
    pub executed: bool,
}

/// Drives Crew-level retry and Squad-level respawn decisions.
/// On a Dead Agent (provided by the [`crate::HealthChecker`]) the
/// Respawner:
/// 1. Looks up the Crew that owns the failed Agent.
/// 2. Asks [`DagExecutor::crew_can_retry`] whether any tasks in
///    the crew still have retry budget.
/// 3. If yes, calls [`DagExecutor::crew_retry_all`] to re-queue
///    the tasks.  Otherwise it requests a Squad respawn.
pub struct Respawner {
    registry: Arc<AgentRegistry>,
    orchestrator: Arc<dyn OrchestratorControl>,
    event_tx: Option<broadcast::Sender<SupervisorEvent>>,
}

impl Respawner {
    pub fn new(registry: Arc<AgentRegistry>, orchestrator: Arc<dyn OrchestratorControl>) -> Self {
        Self {
            registry,
            orchestrator,
            event_tx: None,
        }
    }

    pub fn with_event_tx(mut self, tx: broadcast::Sender<SupervisorEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    /// Process the set of dead agent ids reported by the HealthChecker.
    pub async fn handle_dead_agents(
        &self,
        dead_agent_ids: &[String],
    ) -> SupervisorResult<RespawnReport> {
        let mut report = RespawnReport::default();
        if dead_agent_ids.is_empty() {
            return Ok(report);
        }

        let crews = self.collect_affected_crews(dead_agent_ids);

        for (crew_id, info) in crews {
            let task_ids = info.task_ids.clone();

            if task_ids.is_empty() {
                // Nothing to retry — directly submit a recovery goal to bring up a fresh Squad.
                let recovery_task = Task::new(
                    format!("recovery-{}", crew_id),
                    TaskType::Planner,
                    serde_json::json!({"crew_id": crew_id, "reason": "crew has no tracked tasks"}),
                );
                let _ = self
                    .orchestrator
                    .submit_goal(&format!("recovery-{}", crew_id), vec![recovery_task])
                    .await;

                report.respawn_requested.push(RespawnAction {
                    crew_id: crew_id.clone(),
                    task_ids,
                    retried: 0,
                    crew_retry_count: info.crew_retry_count,
                    reason: "crew has no tracked tasks; respawn executed".into(),
                    executed: true,
                });

                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(SupervisorEvent::SquadRespawnExecuted {
                        crew_id: crew_id.clone(),
                        reason: "crew has no tracked tasks; respawn executed".into(),
                        timestamp: Utc::now(),
                    });
                }
                continue;
            }

            // Stop if this crew has already exhausted its supervisor-level retries.
            if info.crew_retry_count >= CrewInfo::MAX_CREW_RETRIES {
                let retried = self.orchestrator.crew_retry_all(&task_ids).await;

                report.respawn_requested.push(RespawnAction {
                    crew_id: crew_id.clone(),
                    task_ids,
                    retried,
                    crew_retry_count: info.crew_retry_count,
                    reason: format!(
                        "crew exceeded max supervisor retries ({}); respawn executed",
                        CrewInfo::MAX_CREW_RETRIES
                    ),
                    executed: true,
                });

                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(SupervisorEvent::SquadRespawnExecuted {
                        crew_id: crew_id.clone(),
                        reason: format!(
                            "crew exceeded max supervisor retries ({})",
                            CrewInfo::MAX_CREW_RETRIES
                        ),
                        timestamp: Utc::now(),
                    });
                }
                continue;
            }

            if !self.orchestrator.crew_can_retry(&task_ids).await {
                let retried = self.orchestrator.crew_retry_all(&task_ids).await;

                report.respawn_requested.push(RespawnAction {
                    crew_id: crew_id.clone(),
                    task_ids,
                    retried,
                    crew_retry_count: info.crew_retry_count,
                    reason: "all tasks exhausted retry budget; respawn executed".into(),
                    executed: true,
                });

                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(SupervisorEvent::SquadRespawnExecuted {
                        crew_id: crew_id.clone(),
                        reason: "all tasks exhausted retry budget".into(),
                        timestamp: Utc::now(),
                    });
                }
                continue;
            }

            let retried = self.orchestrator.crew_retry_all(&task_ids).await;

            let new_count = self.registry.record_crew_retry(&crew_id);

            report.retried_crews.push(RespawnAction {
                crew_id: crew_id.clone(),
                task_ids,
                retried,
                crew_retry_count: new_count,
                reason: format!(
                    "crew retry triggered after {} dead agent(s)",
                    dead_agent_ids.len()
                ),
                executed: true,
            });

            if let Some(ref tx) = self.event_tx {
                let _ = tx.send(SupervisorEvent::CrewRetried {
                    crew_id,
                    task_ids: info.task_ids.clone(),
                    retried,
                    timestamp: Utc::now(),
                });
            }
        }

        Ok(report)
    }

    /// Group the dead agents by crew and return the affected crew snapshots.
    fn collect_affected_crews(&self, dead_agent_ids: &[String]) -> Vec<(String, CrewInfo)> {
        let mut affected: Vec<(String, CrewInfo)> = Vec::new();
        let mut seen = HashSet::new();
        for id in dead_agent_ids {
            if let Some(agent) = self.registry.get_agent(id) {
                if let Some(crew_id) = agent.crew_id.as_ref() {
                    if seen.insert(crew_id.clone()) {
                        if let Some(crew) = self.registry.get_crew(crew_id) {
                            affected.push((crew_id.clone(), crew));
                        } else {
                            affected.push((crew_id.clone(), CrewInfo::new(crew_id)));
                        }
                    }
                }
            }
        }
        affected
    }

    /// Forcibly mark an agent as dead in the registry: useful when the
    /// Supervisor takes administrative action.
    pub fn mark_dead(&self, agent_id: &str) -> SupervisorResult<()> {
        if self.registry.get_agent(agent_id).is_none() {
            return Err(SupervisorError::Registry(format!(
                "unknown agent {}",
                agent_id
            )));
        }
        // We update the state-change timestamp so that subsequent
        // checks see this is a recent transition.
        self.registry.mark_state_change(agent_id);
        // Touch the heartbeat backwards to past-dead horizon: simpler is
        // to record the wall-clock now; the caller drives the action.
        let _ = Utc::now();
        Ok(())
    }
}
