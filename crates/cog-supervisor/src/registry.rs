use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use cog_core::{AgentState, StateBackend};

use crate::error::{SupervisorError, SupervisorResult};

/// Default TTL applied to heartbeat records, in seconds.
/// Agents whose heartbeat is older than this are considered expired by
/// [`AgentRegistry::check_expired`]. Aligns with the default TTL used by
/// Redis and etcd agent registries (30 seconds) so the Supervisor's view
/// of liveness matches the Agent's self-registered TTL.
pub const DEFAULT_HEARTBEAT_TTL_SECONDS: u64 = 30;
pub const HEARTBEAT_HISTORY_MAX: usize = 1000;

/// Self-reported health status for a heartbeat.
/// This is independent of the durable [`AgentState`] which tracks the
/// lifecycle of the Agent (`Init` -> `Active` -> `Dead`).
/// `HeartbeatStatus` captures the Agent's instantaneous self-assessment
/// so the Supervisor can react before the state machine catches up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HeartbeatStatus {
    /// Agent is processing tasks normally.
    #[default]
    Healthy,
    /// Agent is alive but reporting elevated load or partial errors.
    Degraded,
    /// Agent is alive but unable to make progress.
    Unhealthy,
}

/// One Agent's most recent heartbeat snapshot.
/// Stored in [`AgentRegistry`] and (optionally) mirrored to the durable
/// [`StateBackend`] via [`AgentRegistry::record_heartbeat`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatRecord {
    pub agent_id: String,
    pub timestamp: DateTime<Utc>,
    pub status: HeartbeatStatus,
    /// Self-reported load, normalised to `0.0` (idle) – `1.0` (saturated).
    pub load_score: f32,
    /// Number of tasks currently owned by the Agent.
    pub task_count: u32,
}

impl HeartbeatRecord {
    /// Construct a default `Healthy` record at the current instant.
    pub fn now(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            timestamp: Utc::now(),
            status: HeartbeatStatus::Healthy,
            load_score: 0.0,
            task_count: 0,
        }
    }

    /// Map the heartbeat status onto an [`AgentState`] for mirroring
    /// into the durable backend.  `Healthy` heartbeats map to `Active`,
    /// `Degraded` to `Idle`, and `Unhealthy` to `Suspect` -- the
    /// Supervisor's [`HealthChecker`] can still escalate to `Dead` once
    /// the heartbeat itself goes stale.
    pub fn to_agent_state(&self) -> AgentState {
        match self.status {
            HeartbeatStatus::Healthy => AgentState::Active,
            HeartbeatStatus::Degraded => AgentState::Idle,
            HeartbeatStatus::Unhealthy => AgentState::Suspect,
        }
    }
}

/// Snapshot of an Agent known to the Supervisor.
/// The Supervisor keeps its own lightweight registry to track liveness
/// independent of the durable [`cog_core::StateBackend`].  This allows
/// detection of stuck or dead Agents even if the persisted state is stale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub agent_id: String,
    pub role: Option<String>,
    pub crew_id: Option<String>,
    pub squad_id: Option<String>,
    /// Tasks currently owned by this Agent.
    pub task_ids: Vec<String>,
    /// Last observed heartbeat (server-side timestamp).
    pub last_heartbeat: DateTime<Utc>,
    /// Time the agent entered its current state.
    pub state_since: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
}

/// Snapshot of a Crew known to the Supervisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrewInfo {
    pub crew_id: String,
    pub agent_ids: Vec<String>,
    pub task_ids: Vec<String>,
    /// Number of completed task retries already attempted at the crew
    /// level.  Capped at [`Self::MAX_CREW_RETRIES`].
    pub crew_retry_count: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CrewInfo {
    pub const MAX_CREW_RETRIES: u32 = 3;

    pub fn new(crew_id: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            crew_id: crew_id.into(),
            agent_ids: Vec::new(),
            task_ids: Vec::new(),
            crew_retry_count: 0,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Thread-safe Supervisor-owned registry.
pub struct AgentRegistry {
    agents: RwLock<HashMap<String, AgentInfo>>,
    crews: RwLock<HashMap<String, CrewInfo>>,
    heartbeats: RwLock<HashMap<String, HeartbeatRecord>>,
    /// Ringbuffer of recent heartbeat records per agent.
    heartbeat_history: RwLock<HashMap<String, VecDeque<HeartbeatRecord>>>,
    /// Optional durable backend for mirroring heartbeat-derived state.
    state_backend: Option<Arc<dyn StateBackend>>,
    /// Optional global [`cog_core::AgentRegistry`] so the Supervisor reads
    /// from the same source of truth as the rest of the system (Redis / etcd).
    agent_registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    heartbeat_history_max: usize,
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self {
            agents: RwLock::new(HashMap::new()),
            crews: RwLock::new(HashMap::new()),
            heartbeats: RwLock::new(HashMap::new()),
            heartbeat_history: RwLock::new(HashMap::new()),
            state_backend: None,
            agent_registry: None,
            heartbeat_history_max: HEARTBEAT_HISTORY_MAX,
        }
    }
}

impl std::fmt::Debug for AgentRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentRegistry")
            .field("agents", &self.agents)
            .field("crews", &self.crews)
            .field("heartbeats", &self.heartbeats)
            .field("heartbeat_history", &self.heartbeat_history)
            .field("state_backend", &self.state_backend.is_some())
            .finish()
    }
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a [`StateBackend`] for heartbeat-derived state mirroring.
    /// When set, [`AgentRegistry::record_heartbeat`] additionally calls
    /// [`StateBackend::set_agent_state`] with the [`AgentState`] derived
    /// from [`HeartbeatRecord::to_agent_state`].  Heartbeat records
    /// themselves remain in-memory -- `StateBackend` is the durable
    /// fallback for state, not the heartbeat ringbuffer.
    pub fn with_state_backend(mut self, backend: Arc<dyn StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// Attach a global [`cog_core::AgentRegistry`] for unified reads.
    pub fn with_agent_registry(mut self, registry: Arc<dyn cog_core::AgentRegistry>) -> Self {
        self.agent_registry = Some(registry);
        self
    }

    /// Set the maximum number of heartbeat records retained per agent.
    pub fn with_heartbeat_history_max(mut self, max: usize) -> Self {
        self.heartbeat_history_max = max;
        self
    }

    /// Register a new agent or update its registration timestamp.
    pub fn register_agent(&self, info: AgentInfo) {
        let mut agents = self.agents.write().expect("agent registry poisoned");
        agents.insert(info.agent_id.clone(), info);
    }

    /// Remove an agent from the registry (graceful unregister).
    pub fn unregister_agent(&self, agent_id: &str) {
        let mut agents = self.agents.write().expect("agent registry poisoned");
        agents.remove(agent_id);
        // Also remove agent from any crews referencing it.
        let mut crews = self.crews.write().expect("crew registry poisoned");
        for crew in crews.values_mut() {
            crew.agent_ids.retain(|id| id != agent_id);
            crew.updated_at = Utc::now();
        }
        // Drop the agent's last heartbeat so check_expired stops yielding it.
        let mut heartbeats = self
            .heartbeats
            .write()
            .expect("heartbeat registry poisoned");
        heartbeats.remove(agent_id);
        // Drop heartbeat history too.
        let mut history = self
            .heartbeat_history
            .write()
            .expect("heartbeat history poisoned");
        history.remove(agent_id);
    }

    /// Mark a heartbeat for the given agent.  Inserts a placeholder if
    /// the agent is unknown so that the Supervisor still tracks the
    /// liveness signal.
    pub fn touch(&self, agent_id: &str) {
        let now = Utc::now();
        {
            let mut agents = self.agents.write().expect("agent registry poisoned");
            let entry = agents.entry(agent_id.to_string()).or_insert(AgentInfo {
                agent_id: agent_id.to_string(),
                role: None,
                crew_id: None,
                squad_id: None,
                task_ids: Vec::new(),
                last_heartbeat: now,
                state_since: now,
                registered_at: now,
            });
            entry.last_heartbeat = now;
        }
        // Mirror into the heartbeat map so health_score / check_expired
        // see the same liveness signal as last_heartbeat.
        let mut heartbeats = self
            .heartbeats
            .write()
            .expect("heartbeat registry poisoned");
        let record = heartbeats
            .entry(agent_id.to_string())
            .or_insert_with(|| HeartbeatRecord::now(agent_id));
        record.timestamp = now;
    }

    /// Periodic heartbeat update keyed by `agent_id`.
    /// Equivalent to [`AgentRegistry::touch`] but `async` and able to
    /// mirror the resulting `Healthy` state into the optional
    /// [`StateBackend`].  Inserts an unknown agent so the heartbeat
    /// signal is preserved even before [`Self::register_agent`] runs.
    pub async fn heartbeat(&self, agent_id: &str) -> SupervisorResult<()> {
        self.touch(agent_id);
        if let Some(backend) = self.state_backend.as_ref() {
            backend
                .set_agent_state(agent_id, &AgentState::Active)
                .await
                .map_err(|e| {
                    SupervisorError::Registry(format!(
                        "heartbeat: state backend update failed for {agent_id}: {e}"
                    ))
                })?;
        }
        Ok(())
    }

    /// Record a full [`HeartbeatRecord`] including status, load, and
    /// task count.  Updates the in-memory map, bumps the agent's
    /// `last_heartbeat` timestamp, and (when a backend is attached)
    /// mirrors the derived [`AgentState`] into [`StateBackend`].
    pub async fn record_heartbeat(&self, record: HeartbeatRecord) -> SupervisorResult<()> {
        let agent_id = record.agent_id.clone();
        let timestamp = record.timestamp;
        let derived_state = record.to_agent_state();

        // Bump the registry's internal `last_heartbeat` so the
        // HealthChecker sees the same liveness signal.
        {
            let mut agents = self.agents.write().expect("agent registry poisoned");
            let entry = agents.entry(agent_id.clone()).or_insert(AgentInfo {
                agent_id: agent_id.clone(),
                role: None,
                crew_id: None,
                squad_id: None,
                task_ids: Vec::new(),
                last_heartbeat: timestamp,
                state_since: timestamp,
                registered_at: timestamp,
            });
            entry.last_heartbeat = timestamp;
            // Truncate the task-id listing to match the reported count.
            // Callers can use set_agent_tasks() to populate the actual ids.
            entry.task_ids.truncate(record.task_count as usize);
        }

        {
            let mut heartbeats = self
                .heartbeats
                .write()
                .expect("heartbeat registry poisoned");
            heartbeats.insert(agent_id.clone(), record.clone());
        }
        {
            let mut history = self
                .heartbeat_history
                .write()
                .expect("heartbeat history poisoned");
            let buf = history.entry(agent_id.clone()).or_default();
            buf.push_back(record);
            while buf.len() > self.heartbeat_history_max {
                buf.pop_front();
            }
        }

        if let Some(backend) = self.state_backend.as_ref() {
            backend
                .set_agent_state(&agent_id, &derived_state)
                .await
                .map_err(|e| {
                    SupervisorError::Registry(format!(
                        "record_heartbeat: state backend update failed for {agent_id}: {e}"
                    ))
                })?;
        }

        Ok(())
    }

    /// Fetch the most recent heartbeat record for `agent_id`.
    pub fn get_heartbeat(&self, agent_id: &str) -> Option<HeartbeatRecord> {
        let heartbeats = self.heartbeats.read().expect("heartbeat registry poisoned");
        heartbeats.get(agent_id).cloned()
    }

    /// Fetch the heartbeat history ringbuffer for `agent_id`.
    pub fn get_heartbeat_history(&self, agent_id: &str) -> Vec<HeartbeatRecord> {
        let history = self
            .heartbeat_history
            .read()
            .expect("heartbeat history poisoned");
        history
            .get(agent_id)
            .cloned()
            .map(|buf| buf.into_iter().collect())
            .unwrap_or_default()
    }

    /// Return the number of stored heartbeat history entries for `agent_id`.
    pub fn heartbeat_history_len(&self, agent_id: &str) -> usize {
        let history = self
            .heartbeat_history
            .read()
            .expect("heartbeat history poisoned");
        history.get(agent_id).map(|buf| buf.len()).unwrap_or(0)
    }

    /// Snapshot of every known heartbeat record.
    pub fn heartbeats(&self) -> Vec<HeartbeatRecord> {
        let heartbeats = self.heartbeats.read().expect("heartbeat registry poisoned");
        heartbeats.values().cloned().collect()
    }

    /// Return the agent_ids whose heartbeat (or `last_heartbeat`, when
    /// no record exists) is older than `ttl_seconds`.
    /// Use [`DEFAULT_HEARTBEAT_TTL_SECONDS`] for the canonical 30-second
    /// expiration window.  Pass `0` to treat every entry as expired
    /// (used by tests).
    pub fn check_expired(&self, ttl_seconds: u64) -> Vec<String> {
        let now = Utc::now();
        let ttl = chrono::Duration::seconds(ttl_seconds as i64);
        let cutoff = now - ttl;

        let mut expired: HashSet<String> = HashSet::new();

        // Heartbeat-record-driven expiry.
        {
            let heartbeats = self.heartbeats.read().expect("heartbeat registry poisoned");
            for (agent_id, record) in heartbeats.iter() {
                if record.timestamp < cutoff {
                    expired.insert(agent_id.clone());
                }
            }
        }

        // Fall back to AgentInfo.last_heartbeat for agents without a
        // dedicated HeartbeatRecord (e.g. registered but never beat).
        {
            let heartbeats = self.heartbeats.read().expect("heartbeat registry poisoned");
            let agents = self.agents.read().expect("agent registry poisoned");
            for (agent_id, info) in agents.iter() {
                if heartbeats.contains_key(agent_id) {
                    continue;
                }
                if info.last_heartbeat < cutoff {
                    expired.insert(agent_id.clone());
                }
            }
        }

        let mut out: Vec<String> = expired.into_iter().collect();
        out.sort();
        out
    }

    /// Compute a normalised health score for `agent_id`.
    /// Returns a value in `[0.0, 1.0]`:
    /// - `1.0` immediately after a heartbeat,
    /// - decays linearly toward `0.0` as the elapsed time approaches
    ///   [`DEFAULT_HEARTBEAT_TTL_SECONDS`],
    /// - `0.0` once the heartbeat has expired.
    ///
    /// Returns `None` for agents with no heartbeat or registration on
    ///
    /// record so callers can distinguish "never seen" from "expired".
    pub fn health_score(&self, agent_id: &str) -> Option<f32> {
        self.health_score_with_ttl(agent_id, DEFAULT_HEARTBEAT_TTL_SECONDS)
    }

    /// Like [`Self::health_score`] but parameterised by `ttl_seconds`.
    pub fn health_score_with_ttl(&self, agent_id: &str, ttl_seconds: u64) -> Option<f32> {
        let last_seen = self
            .get_heartbeat(agent_id)
            .map(|r| r.timestamp)
            .or_else(|| self.get_agent(agent_id).map(|a| a.last_heartbeat))?;

        let elapsed = (Utc::now() - last_seen)
            .to_std()
            .unwrap_or(Duration::from_secs(0));
        let ttl = Duration::from_secs(ttl_seconds.max(1));

        if elapsed >= ttl {
            return Some(0.0);
        }
        let elapsed_f = elapsed.as_secs_f64();
        let ttl_f = ttl.as_secs_f64();
        let score = (1.0 - (elapsed_f / ttl_f)).clamp(0.0, 1.0) as f32;
        Some(score)
    }

    /// Fetch a snapshot of all currently registered agents.
    pub fn agents(&self) -> Vec<AgentInfo> {
        let agents = self.agents.read().expect("agent registry poisoned");
        agents.values().cloned().collect()
    }

    /// Get a single agent snapshot.
    pub fn get_agent(&self, agent_id: &str) -> Option<AgentInfo> {
        let agents = self.agents.read().expect("agent registry poisoned");
        agents.get(agent_id).cloned()
    }

    /// Number of registered agents.
    pub fn agent_count(&self) -> usize {
        self.agents.read().expect("agent registry poisoned").len()
    }

    /// Update the task-ownership listing for an agent.
    pub fn set_agent_tasks(&self, agent_id: &str, task_ids: Vec<String>) {
        let mut agents = self.agents.write().expect("agent registry poisoned");
        if let Some(agent) = agents.get_mut(agent_id) {
            agent.task_ids = task_ids;
        }
    }

    /// Refresh the timestamp tracking when an Agent's state last changed.
    pub fn mark_state_change(&self, agent_id: &str) {
        let mut agents = self.agents.write().expect("agent registry poisoned");
        if let Some(agent) = agents.get_mut(agent_id) {
            agent.state_since = Utc::now();
        }
    }

    /// Register or update a Crew.
    pub fn register_crew(&self, info: CrewInfo) {
        let mut crews = self.crews.write().expect("crew registry poisoned");
        crews.insert(info.crew_id.clone(), info);
    }

    /// Remove a Crew from tracking.
    pub fn unregister_crew(&self, crew_id: &str) {
        let mut crews = self.crews.write().expect("crew registry poisoned");
        crews.remove(crew_id);
    }

    /// Fetch a snapshot of all currently registered crews.
    pub fn crews(&self) -> Vec<CrewInfo> {
        let crews = self.crews.read().expect("crew registry poisoned");
        crews.values().cloned().collect()
    }

    /// Get a single crew snapshot.
    pub fn get_crew(&self, crew_id: &str) -> Option<CrewInfo> {
        let crews = self.crews.read().expect("crew registry poisoned");
        crews.get(crew_id).cloned()
    }

    /// Increment the crew retry counter and return the new value.
    pub fn record_crew_retry(&self, crew_id: &str) -> u32 {
        let mut crews = self.crews.write().expect("crew registry poisoned");
        let crew = crews
            .entry(crew_id.to_string())
            .or_insert_with(|| CrewInfo::new(crew_id));
        crew.crew_retry_count = crew.crew_retry_count.saturating_add(1);
        crew.updated_at = Utc::now();
        crew.crew_retry_count
    }

    /// Discover the set of agent ids known to the registry.
    pub fn known_agent_ids(&self) -> HashSet<String> {
        let agents = self.agents.read().expect("agent registry poisoned");
        agents.keys().cloned().collect()
    }

    // ─── Delegation to global cog_core::AgentRegistry ───

    /// Fetch a single agent registration from the global registry, if attached.
    pub async fn get_registered_agent(
        &self,
        agent_id: &str,
    ) -> SupervisorResult<Option<cog_core::AgentRegistration>> {
        match self.agent_registry.as_ref() {
            Some(reg) => reg
                .get(agent_id)
                .await
                .map_err(|e| SupervisorError::Registry(format!("global registry get failed: {e}"))),
            None => Ok(None),
        }
    }

    /// List all agents from the global registry, if attached.
    pub async fn list_registered_agents(
        &self,
    ) -> SupervisorResult<Vec<cog_core::AgentRegistration>> {
        match self.agent_registry.as_ref() {
            Some(reg) => reg.list().await.map_err(|e| {
                SupervisorError::Registry(format!("global registry list failed: {e}"))
            }),
            None => Ok(Vec::new()),
        }
    }

    /// List agents by role from the global registry, if attached.
    pub async fn list_registered_agents_by_role(
        &self,
        role: &str,
    ) -> SupervisorResult<Vec<cog_core::AgentRegistration>> {
        match self.agent_registry.as_ref() {
            Some(reg) => reg.list_by_role(role).await.map_err(|e| {
                SupervisorError::Registry(format!("global registry list_by_role failed: {e}"))
            }),
            None => Ok(Vec::new()),
        }
    }

    /// List agents by capability from the global registry, if attached.
    pub async fn list_registered_agents_by_capability(
        &self,
        capability: &str,
    ) -> SupervisorResult<Vec<cog_core::AgentRegistration>> {
        match self.agent_registry.as_ref() {
            Some(reg) => reg.list_by_capability(capability).await.map_err(|e| {
                SupervisorError::Registry(format!("global registry list_by_capability failed: {e}"))
            }),
            None => Ok(Vec::new()),
        }
    }
}

impl cog_core::HeartbeatRegistry for AgentRegistry {
    fn get_heartbeat_history(&self, agent_id: &str) -> Vec<cog_core::HeartbeatRecord> {
        let history = self
            .heartbeat_history
            .read()
            .expect("heartbeat history poisoned");
        history
            .get(agent_id)
            .map(|buf| {
                buf.iter()
                    .map(|r| cog_core::HeartbeatRecord {
                        agent_id: r.agent_id.clone(),
                        timestamp: r.timestamp,
                        status: match r.status {
                            HeartbeatStatus::Healthy => cog_core::HeartbeatStatus::Healthy,
                            HeartbeatStatus::Degraded => cog_core::HeartbeatStatus::Degraded,
                            HeartbeatStatus::Unhealthy => cog_core::HeartbeatStatus::Unhealthy,
                        },
                        load_score: r.load_score,
                        task_count: r.task_count,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn list_crews(&self) -> Vec<cog_core::CrewSummary> {
        self.crews()
            .into_iter()
            .map(|c| cog_core::CrewSummary {
                crew_id: c.crew_id,
                agent_ids: c.agent_ids,
                task_ids: c.task_ids,
                crew_retry_count: c.crew_retry_count,
                created_at: c.created_at,
                updated_at: c.updated_at,
            })
            .collect()
    }
}
