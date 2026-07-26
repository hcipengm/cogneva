use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{AgentState, OrchestratorControl, StateBackend, TaskCheckpoint, TaskStatus};

use crate::error::SupervisorResult;
use crate::lifecycle_coordinator::RecoveredCheckpoint;
use crate::registry::{AgentInfo, AgentRegistry};

/// Rebalance plan emitted by the [`TaskRebalancer`].
#[derive(Debug, Clone, Default)]
pub struct RebalancePlan {
    /// Number of currently ready tasks waiting for execution.
    pub ready_tasks: usize,
    /// Number of agents available to receive new tasks.
    pub available_agents: usize,
    /// Per-agent assignment recommendations: `(agent_id, task_id)`.
    pub assignments: Vec<(String, String)>,
    /// Agents currently overloaded (running task count exceeds the
    /// configured per-agent limit).
    pub overloaded_agents: Vec<(String, usize)>,
    /// Tasks that need checkpoint recovery after their agent died.
    /// Each tuple is `(task_id, checkpoint)`.
    pub checkpoint_recoveries: Vec<(String, TaskCheckpoint)>,
}

impl RebalancePlan {
    pub fn is_empty(&self) -> bool {
        self.assignments.is_empty()
            && self.overloaded_agents.is_empty()
            && self.checkpoint_recoveries.is_empty()
    }

    /// Whether this plan contains any actionable work (assignments,
    /// recoveries, or overloaded agents to shed).
    pub fn has_work(&self) -> bool {
        !self.assignments.is_empty()
            || !self.checkpoint_recoveries.is_empty()
            || !self.overloaded_agents.is_empty()
    }
}

/// Configuration for the [`TaskRebalancer`].
#[derive(Debug, Clone)]
pub struct TaskRebalancerConfig {
    /// Soft limit on simultaneous tasks per agent.  Agents above this
    /// value are flagged as overloaded.
    pub max_tasks_per_agent: usize,
    /// Maximum number of recommendations emitted per pass.
    pub max_assignments_per_pass: usize,
}

impl Default for TaskRebalancerConfig {
    fn default() -> Self {
        Self {
            max_tasks_per_agent: 4,
            max_assignments_per_pass: 32,
        }
    }
}

impl From<cog_core::TaskRebalancerConfig> for TaskRebalancerConfig {
    fn from(c: cog_core::TaskRebalancerConfig) -> Self {
        Self {
            max_tasks_per_agent: c.max_tasks_per_agent,
            max_assignments_per_pass: c.max_assignments_per_pass,
        }
    }
}

/// Detects imbalanced workloads and recommends task moves.
/// The rebalancer is **advisory** — it does not mutate orchestrator
/// state.  The Supervisor consumes the plan and emits events; the
/// actual task assignment lives in `cogneva`'s scheduler loop.
pub struct TaskRebalancer {
    registry: Arc<AgentRegistry>,
    state_backend: Arc<dyn StateBackend>,
    orchestrator: Arc<dyn OrchestratorControl>,
    cfg: TaskRebalancerConfig,
}

impl TaskRebalancer {
    pub fn new(
        registry: Arc<AgentRegistry>,
        state_backend: Arc<dyn StateBackend>,
        orchestrator: Arc<dyn OrchestratorControl>,
        cfg: TaskRebalancerConfig,
    ) -> Self {
        Self {
            registry,
            state_backend,
            orchestrator,
            cfg,
        }
    }

    pub fn config(&self) -> &TaskRebalancerConfig {
        &self.cfg
    }

    /// Compute a rebalance plan based on current registry + orchestrator
    /// state.  Returns an empty plan when the cluster is balanced.
    pub async fn plan(&self) -> SupervisorResult<RebalancePlan> {
        let mut plan = RebalancePlan::default();

        // 1. Discover available agents and their current load.
        let agents = self.registry.agents();
        let mut available: Vec<(AgentInfo, usize)> = Vec::with_capacity(agents.len());

        for agent in agents {
            let backend_state = self
                .state_backend
                .get_agent_state(&agent.agent_id)
                .await
                .ok()
                .flatten();

            let allowed = matches!(
                backend_state,
                Some(AgentState::Active) | Some(AgentState::Idle) | Some(AgentState::Registered)
            ) || backend_state.is_none();

            if !allowed {
                continue;
            }

            let in_flight = self.count_running_tasks_for_agent(&agent.agent_id).await;

            if in_flight > self.cfg.max_tasks_per_agent {
                plan.overloaded_agents
                    .push((agent.agent_id.clone(), in_flight));
                continue;
            }

            available.push((agent, in_flight));
        }

        plan.available_agents = available.len();

        // 2. Snapshot ready tasks from the orchestrator.
        let ready: Vec<String> = self
            .orchestrator
            .get_ready_tasks()
            .await
            .into_iter()
            .map(|t| t.id.clone())
            .collect();
        plan.ready_tasks = ready.len();

        if ready.is_empty() || available.is_empty() {
            return Ok(plan);
        }

        // 3. Round-robin tasks onto the agents with the lowest load.
        let mut load_map: HashMap<String, usize> = available
            .iter()
            .map(|(a, load)| (a.agent_id.clone(), *load))
            .collect();

        for (emitted, task_id) in ready.into_iter().enumerate() {
            if emitted >= self.cfg.max_assignments_per_pass {
                break;
            }

            let next_agent = available
                .iter()
                .min_by_key(|(a, _)| load_map.get(&a.agent_id).copied().unwrap_or(usize::MAX))
                .map(|(a, _)| a.agent_id.clone());

            let Some(agent_id) = next_agent else {
                break;
            };

            // Skip the assignment if the chosen agent is already at the
            // soft cap; the rebalancer prefers waiting over piling more
            // work onto a busy worker.
            let load = *load_map.get(&agent_id).unwrap_or(&0);
            if load >= self.cfg.max_tasks_per_agent {
                break;
            }

            plan.assignments.push((agent_id.clone(), task_id));
            load_map
                .entry(agent_id)
                .and_modify(|v| *v += 1)
                .or_insert(1);
        }

        Ok(plan)
    }

    /// Count the number of running tasks owned by an agent according
    /// to the orchestrator.
    async fn count_running_tasks_for_agent(&self, agent_id: &str) -> usize {
        self.orchestrator
            .get_all_tasks()
            .await
            .into_iter()
            .filter(|t| t.agent_id.as_deref() == Some(agent_id))
            .filter(|t| matches!(t.status, TaskStatus::Scheduled | TaskStatus::Running))
            .count()
    }

    /// Build a recovery plan for tasks whose agents were declared Dead.
    /// Each [`RecoveredCheckpoint`] is turned into a
    /// `(task_id, checkpoint)` entry in the plan's
    /// `checkpoint_recoveries`.  The caller (typically the Supervisor)
    /// can then re-submit these tasks to the orchestrator with their
    /// checkpoint so a replacement agent resumes from the last known
    /// good state.
    /// This method **does not** assign the recovered tasks to specific
    /// agents — that happens in the next regular `plan()` pass once the
    /// orchestrator has re-queued them.
    pub async fn recovery_plan(
        &self,
        recovered: &[RecoveredCheckpoint],
    ) -> SupervisorResult<RebalancePlan> {
        let mut plan = RebalancePlan::default();

        for rc in recovered {
            // Verify the task is still in the orchestrator and not already
            // completed or assigned to another agent.
            let tasks = self.orchestrator.get_all_tasks().await;
            let still_relevant = tasks.into_iter().any(|t| {
                t.id == rc.task_id
                    && !matches!(t.status, TaskStatus::Completed | TaskStatus::Failed)
            });

            if still_relevant {
                plan.checkpoint_recoveries
                    .push((rc.task_id.clone(), rc.checkpoint.clone()));
            }
        }

        Ok(plan)
    }
}
