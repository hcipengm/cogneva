use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::{AgentState, StateBackend};

use crate::error::SupervisorResult;
use crate::registry::{AgentInfo, AgentRegistry};
use cog_core::HealthIssue;

/// Aggregate result of a single health-check pass.
#[derive(Debug, Clone, Default)]
pub struct HealthReport {
    pub healthy: Vec<String>,
    pub suspect: Vec<(String, HealthIssue)>,
    pub dead: Vec<(String, HealthIssue)>,
    pub stuck: Vec<(String, HealthIssue)>,
}

impl HealthReport {
    pub fn is_clean(&self) -> bool {
        self.suspect.is_empty() && self.dead.is_empty() && self.stuck.is_empty()
    }

    pub fn unhealthy_total(&self) -> usize {
        self.suspect.len() + self.dead.len() + self.stuck.len()
    }
}

/// Configuration for the [`HealthChecker`] component.
#[derive(Debug, Clone)]
pub struct HealthCheckerConfig {
    /// How long without a heartbeat before marking an Agent as Suspect.
    pub suspect_after: Duration,
    /// How long without a heartbeat before marking an Agent as Dead.
    pub dead_after: Duration,
    /// How long an Agent may stay in `Active` without progress before
    /// being flagged as Stuck.
    pub stuck_after: Duration,
}

impl Default for HealthCheckerConfig {
    fn default() -> Self {
        Self {
            suspect_after: Duration::from_secs(15),
            dead_after: Duration::from_secs(60),
            stuck_after: Duration::from_secs(600),
        }
    }
}

impl From<cog_core::HealthCheckerConfig> for HealthCheckerConfig {
    fn from(c: cog_core::HealthCheckerConfig) -> Self {
        Self {
            suspect_after: Duration::from_secs(c.suspect_after_secs),
            dead_after: Duration::from_secs(c.dead_after_secs),
            stuck_after: Duration::from_secs(c.stuck_after_secs),
        }
    }
}

/// Polls the agent registry + StateBackend to identify unhealthy
/// workers.  Pure observation; no side-effects beyond returning a
/// [`HealthReport`].
pub struct HealthChecker {
    registry: Arc<AgentRegistry>,
    state_backend: Arc<dyn StateBackend>,
    cfg: HealthCheckerConfig,
}

impl HealthChecker {
    pub fn new(
        registry: Arc<AgentRegistry>,
        state_backend: Arc<dyn StateBackend>,
        cfg: HealthCheckerConfig,
    ) -> Self {
        Self {
            registry,
            state_backend,
            cfg,
        }
    }

    /// Run a single health check pass over the registry.
    pub async fn check(&self) -> SupervisorResult<HealthReport> {
        let mut report = HealthReport::default();
        let now = Utc::now();
        let agents = self.registry.agents();

        for agent in agents {
            // 1. Check the durable state first — explicit Dead beats heartbeat.
            let backend_state = self
                .state_backend
                .get_agent_state(&agent.agent_id)
                .await
                .ok()
                .flatten();

            if let Some(AgentState::Dead) = backend_state {
                report.dead.push((
                    agent.agent_id.clone(),
                    HealthIssue::Dead {
                        last_seen: agent.last_heartbeat,
                    },
                ));
                continue;
            }

            // 2. Heartbeat liveness based on registry-recorded timestamps.
            let elapsed = (now - agent.last_heartbeat)
                .to_std()
                .unwrap_or(Duration::from_secs(0));

            if elapsed >= self.cfg.dead_after {
                report.dead.push((
                    agent.agent_id.clone(),
                    HealthIssue::Dead {
                        last_seen: agent.last_heartbeat,
                    },
                ));
                continue;
            }

            if elapsed >= self.cfg.suspect_after {
                let missed_beats = (elapsed.as_secs() / self.cfg.suspect_after.as_secs().max(1))
                    .min(u32::MAX as u64) as u32;
                report.suspect.push((
                    agent.agent_id.clone(),
                    HealthIssue::Suspect { missed_beats },
                ));
                continue;
            }

            // 3. Stuck detection — Agent has been in Active too long.
            if matches!(backend_state, Some(AgentState::Active)) {
                let active_for = (now - agent.state_since)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0));
                if active_for >= self.cfg.stuck_after {
                    report.stuck.push((
                        agent.agent_id.clone(),
                        HealthIssue::Stuck {
                            stuck_seconds: active_for.as_secs(),
                        },
                    ));
                    continue;
                }
            }

            report.healthy.push(agent.agent_id);
        }

        Ok(report)
    }

    /// Reset the heartbeat timestamp for an Agent that was healthy
    /// during this pass — the caller is responsible for invoking this
    /// after observing live activity (e.g. fresh broadcast events).
    pub fn record_recovery(&self, agent: &AgentInfo) {
        self.registry.touch(&agent.agent_id);
    }

    pub fn config(&self) -> &HealthCheckerConfig {
        &self.cfg
    }
}
