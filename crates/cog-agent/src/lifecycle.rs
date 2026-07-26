use cog_core::{AgentState, SFError, SFResult, StateBackend};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Hook called on every valid state transition.
/// The hook receives `(agent_id, old_state, new_state)`.
/// Errors are logged but do not block the transition.
use AgentState::*;

pub type StateTransitionHook = Arc<dyn Fn(&str, AgentState, AgentState) + Send + Sync>;

/// Configuration for the heartbeat subsystem.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// Interval between heartbeats in milliseconds.
    pub interval_ms: u64,
    /// Number of missed heartbeats before marking an agent as Suspect.
    pub suspect_threshold: u32,
    /// Number of missed heartbeats before marking an agent as Dead.
    pub dead_threshold: u32,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5000,
            suspect_threshold: 2,
            dead_threshold: 5,
        }
    }
}

/// Manages agent lifecycle state transitions, persistence, and heartbeat.
/// Uses a `StateBackend` for durable state storage and supports optional
/// async hooks on every transition.  State transitions are validated
/// against a hard-coded state machine so that illegal transitions are
/// rejected at the type level.
pub struct LifecycleManager {
    backend: Arc<dyn StateBackend>,
    transition_hook: Option<StateTransitionHook>,
    heartbeat_cfg: HeartbeatConfig,
    heartbeat_handles: RwLock<std::collections::HashMap<String, JoinHandle<()>>>,
}

impl LifecycleManager {
    pub fn new(backend: Arc<dyn StateBackend>) -> Self {
        Self {
            backend,
            transition_hook: None,
            heartbeat_cfg: HeartbeatConfig::default(),
            heartbeat_handles: RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn with_transition_hook(mut self, hook: StateTransitionHook) -> Self {
        self.transition_hook = Some(hook);
        self
    }

    pub fn with_heartbeat_config(mut self, cfg: HeartbeatConfig) -> Self {
        self.heartbeat_cfg = cfg;
        self
    }

    /// Register a new agent in the `Init` state.
    /// Idempotent: if the agent is already registered this returns `Ok(())`
    /// without modifying the existing state.
    pub async fn register(&self, agent_id: &str) -> SFResult<()> {
        let current = self.backend.get_agent_state(agent_id).await?;
        if current.is_some() {
            return Ok(());
        }
        self.backend
            .set_agent_state(agent_id, &AgentState::Init)
            .await?;
        Ok(())
    }

    /// Transition an agent to a new state, validating the transition first.
    /// Uses compare-and-swap (CAS) to prevent concurrent overwrites.
    /// If the backend state has changed since we read it, the operation
    /// is retried once with the fresh current state.
    pub async fn transition(&self, agent_id: &str, to: AgentState) -> SFResult<()> {
        let mut from = self
            .backend
            .get_agent_state(agent_id)
            .await?
            .unwrap_or(AgentState::Init);

        if !Self::is_valid_transition(from, to) {
            return Err(SFError::Agent(format!(
                "invalid transition: {:?} -> {:?} for agent {}",
                from, to, agent_id
            )));
        }

        // Attempt CAS; if it fails, re-read and retry once.
        let mut swapped = self.backend.cas_agent_state(agent_id, &from, &to).await?;
        if !swapped {
            from = self
                .backend
                .get_agent_state(agent_id)
                .await?
                .unwrap_or(AgentState::Init);
            if !Self::is_valid_transition(from, to) {
                return Err(SFError::Agent(format!(
                    "invalid transition: {:?} -> {:?} for agent {}",
                    from, to, agent_id
                )));
            }
            swapped = self.backend.cas_agent_state(agent_id, &from, &to).await?;
        }

        if !swapped {
            return Err(SFError::Agent(format!(
                "CAS failed for agent {}: concurrent state change detected",
                agent_id
            )));
        }

        if let Some(ref hook) = self.transition_hook {
            hook(agent_id, from, to);
        }

        Ok(())
    }

    /// Get the current lifecycle state of an agent.
    pub async fn get_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        self.backend.get_agent_state(agent_id).await
    }

    /// Start a background heartbeat task for the given agent.
    /// The task periodically calls `touch(agent_id)` to update the agent's
    /// heartbeat timestamp.  If the agent misses too many beats it is
    /// automatically moved to `Suspect` and then `Dead`.
    pub async fn start_heartbeat(&self, agent_id: impl Into<String>) {
        let agent_id = agent_id.into();
        let backend = self.backend.clone();
        let cfg = self.heartbeat_cfg.clone();
        let agent_id_clone = agent_id.clone();
        let handle = tokio::spawn(async move {
            let mut missed = 0u32;
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(cfg.interval_ms)).await;

                let state = match backend.get_agent_state(&agent_id_clone).await {
                    Ok(Some(s)) => s,
                    _ => {
                        missed += 1;
                        continue;
                    }
                };

                // Only track heartbeats for agents that should be alive
                if !matches!(
                    state,
                    AgentState::Active | AgentState::Idle | AgentState::Suspect
                ) {
                    missed = 0;
                    continue;
                }

                missed += 1;
                if missed >= cfg.dead_threshold {
                    let _ = backend
                        .set_agent_state(&agent_id_clone, &AgentState::Dead)
                        .await;
                    break;
                } else if missed >= cfg.suspect_threshold {
                    let _ = backend
                        .set_agent_state(&agent_id_clone, &AgentState::Suspect)
                        .await;
                }
            }
        });

        let mut handles = self.heartbeat_handles.write().await;
        handles.insert(agent_id, handle);
    }

    /// Stop the heartbeat task for an agent.
    pub async fn stop_heartbeat(&self, agent_id: &str) {
        let mut handles = self.heartbeat_handles.write().await;
        if let Some(handle) = handles.remove(agent_id) {
            handle.abort();
        }
    }

    /// Stop all heartbeat tasks.
    pub async fn shutdown(&self) {
        let mut handles = self.heartbeat_handles.write().await;
        for (_, handle) in handles.drain() {
            handle.abort();
        }
    }

    fn is_valid_transition(from: AgentState, to: AgentState) -> bool {
        match (from, to) {
            // Init can go to Registered or Dead
            (Init, Registered) | (Init, Dead) => true,
            // Registered can go to Active, Idle, Inactive, or Dead
            (Registered, Active)
            | (Registered, Idle)
            | (Registered, Inactive)
            | (Registered, Dead) => true,
            // Active can go to Idle, Completing, Suspect, or Dead
            (Active, Idle) | (Active, Completing) | (Active, Suspect) | (Active, Dead) => true,
            // Idle can go to Active, Completing, Suspect, or Dead
            (Idle, Active) | (Idle, Completing) | (Idle, Suspect) | (Idle, Dead) => true,
            // Completing can go to Idle, Inactive, or Dead
            (Completing, Idle) | (Completing, Inactive) | (Completing, Dead) => true,
            // Inactive can go to Registered, Active, or Dead
            (Inactive, Registered) | (Inactive, Active) | (Inactive, Dead) => true,
            // Suspect can recover to Active or go to Dead
            (Suspect, Active) | (Suspect, Dead) => true,
            // Dead is terminal
            (Dead, _) => false,
            // Same-state transitions are idempotent
            (a, b) if a == b => true,
            _ => false,
        }
    }
}
