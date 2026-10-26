//! Lifecycle Coordinator — bridges Supervisor health events with agent state
//! transitions and checkpoint recovery.
//! (Hook event emission for lifecycle state changes) of the design doc.
//! The coordinator is a thin advisory layer: it receives health reports from
//! the [`crate::HealthChecker`], drives CAS state transitions via the [`StateBackend`],
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
use cog_core::{AgentEvent, HealthIssue, SupervisorEvent};

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
/// 2. The agent's own `LifecycleManager` can detect external state changes
///    on its next heartbeat and react accordingly.
/// 3. Checkpoints are recovered when an agent is declared Dead so tasks can
///    be resumed by a replacement agent.
pub struct LifecycleCoordinator {
    registry: Arc<AgentRegistry>,
    state_backend: Arc<dyn StateBackend>,
    event_tx: broadcast::Sender<SupervisorEvent>,
    /// The agent-facing bus, once someone hands it over. `None` keeps the
    /// coordinator usable in a test that has no bus at all: a transition that
    /// nobody can hear about is still a transition, it just has no echo.
    agent_events: Option<broadcast::Sender<AgentEvent>>,
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
            agent_events: None,
        }
    }

    /// Attach the agent-event bus so state transitions are announced on it.
    ///
    /// Not a `new()` argument: every existing caller of `new()` builds a
    /// coordinator from parts it already has, and only the supervisor holds
    /// the agent bus.
    pub fn with_agent_events(mut self, tx: broadcast::Sender<AgentEvent>) -> Self {
        self.agent_events = Some(tx);
        self
    }

    /// Process a [`HealthReport`] and drive state transitions + checkpoint
    /// recovery for every unhealthy agent.
    ///
    /// The events emitted here share the supervisor's channel, so this is a
    /// second writer of `AgentUnhealthy` and it runs *before* the supervisor
    /// announces its own: an announcement filter that lives only at the
    /// supervisor's tail is no filter at all. Both read
    /// [`HealthReport::announces`] instead of each keeping their own copy.
    ///
    /// Only the announcement is gated. The state transitions and the checkpoint
    /// recovery below stay unconditional -- an agent that has left the fleet
    /// still needs its tasks picked up, it just is not unhealthy news.
    pub async fn handle_health_report(
        &self,
        report: &HealthReport,
    ) -> SupervisorResult<LifecycleReport> {
        let mut out = LifecycleReport::default();

        // Suspect -> Suspect (or recovery if heartbeat comes back)
        for (agent_id, issue) in &report.suspect {
            // Read "from" *before* the CAS, like the other two branches do:
            // read afterwards and the stored state is already the target, so
            // this row would always say Suspect -> Suspect and the frame would
            // report a transition from a state the agent was never in.
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
                self.announce_transition(agent_id, prev, AgentState::Suspect);
                if report.announces(agent_id) {
                    self.emit_lifecycle_event(agent_id, "suspect", issue).await;
                    out.events_emitted += 1;
                }
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
                self.announce_transition(agent_id, prev, AgentState::Dead);

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

                if report.announces(agent_id) {
                    self.emit_lifecycle_event(agent_id, "dead", issue).await;
                    out.events_emitted += 1;
                }
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
                self.announce_transition(agent_id, prev, AgentState::Suspect);
                if report.announces(agent_id) {
                    self.emit_lifecycle_event(agent_id, "stuck", issue).await;
                    out.events_emitted += 1;
                }
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

    /// Announce a state transition that actually landed in the store.
    ///
    /// This is the only producer of `StateChange` in the tree. The roster's
    /// state cell has no other source: `AgentInfo` carries no state field, so
    /// the cell is written by frames or stays empty forever -- and with no
    /// producer anywhere, every face folding this frame drew a column that
    /// could not move. Not "slow to refresh": permanently still, saying
    /// nothing about being still.
    ///
    /// Announced only after the transition was written, and never when it was
    /// refused: a frame that says an agent went Suspect while the store still
    /// holds Active would put the screen and the state machine in disagreement,
    /// with the screen as the optimistic one.
    fn announce_transition(&self, agent_id: &str, from: AgentState, to: AgentState) {
        let Some(tx) = self.agent_events.as_ref() else {
            return;
        };
        // Crew and squad come from the roster rather than from the target
        // state: they are who the agent is, not what it just became.
        let agent = self.registry.get_agent(agent_id);
        let event = AgentEvent::StateChange {
            agent_id: agent_id.to_string(),
            from: wire_state(from).to_string(),
            to: wire_state(to).to_string(),
            crew_id: agent.as_ref().and_then(|a| a.crew_id.clone()),
            squad_id: agent.as_ref().and_then(|a| a.squad_id.clone()),
            timestamp: Utc::now(),
        };
        if tx.send(event).is_err() {
            // No receiver is a normal reading in a process without a bus, not
            // a fault: the transition is already stored, and the next reader to
            // subscribe gets it from there.
            debug!(agent_id = %agent_id, "state change announced with no subscriber");
        }
    }

    /// Emit a lifecycle hook event on the Supervisor broadcast channel.
    async fn emit_lifecycle_event(&self, agent_id: &str, kind: &str, issue: &HealthIssue) {
        let event = SupervisorEvent::AgentUnhealthy {
            agent_id: agent_id.into(),
            issue: issue.clone(),
            timestamp: Utc::now(),
        };
        let _ = self.event_tx.send(event);
        // No second, dedicated event: `AgentUnhealthy` is already the lifecycle
        // transition, and `kind` above names which one it was. A previous
        // version also sent a `Tick { cycle: 0 }` here as a "heartbeat", which
        // was wrong twice over -- it carried no information the event above did
        // not, and `Tick.cycle` means the health-check cycle number (the
        // health-check loop's own tick), so a lifecycle transition writing 0
        // there read as "no health-check has run yet".

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

/// How an [`AgentState`] is spelled on the bus.
///
/// Spelled out here rather than derived at runtime so the compiler guards
/// completeness: a new variant fails to build instead of being published as
/// whatever `Debug` prints (PascalCase, which no reader on the other side
/// compares against). The spelling itself is checked against `AgentState`'s
/// serde rendering in the tests below -- two readers downstream (the gateway's
/// severity filter, the roster's label) compare against this exact string.
fn wire_state(state: AgentState) -> &'static str {
    match state {
        AgentState::Init => "init",
        AgentState::Registered => "registered",
        AgentState::Active => "active",
        AgentState::Idle => "idle",
        AgentState::Completing => "completing",
        AgentState::Inactive => "inactive",
        AgentState::Suspect => "suspect",
        AgentState::Dead => "dead",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every state on the wire spells the way serde spells it. Two spellings
    /// for one enum is how a filter downstream stops matching without anyone
    /// editing it -- the gateway compares `to` to a literal, and a silent
    /// mismatch there does not break a build, it just stops marking the level.
    #[test]
    fn wire_state_matches_serde() {
        for state in [
            AgentState::Init,
            AgentState::Registered,
            AgentState::Active,
            AgentState::Idle,
            AgentState::Completing,
            AgentState::Inactive,
            AgentState::Suspect,
            AgentState::Dead,
        ] {
            let serde_spelling = serde_json::to_value(state)
                .expect("AgentState is a plain enum")
                .as_str()
                .expect("a unit variant serialises to its name")
                .to_string();
            assert_eq!(wire_state(state), serde_spelling, "state {state:?}");
        }
    }
}
