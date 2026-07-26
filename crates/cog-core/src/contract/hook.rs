use crate::SFResult;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Hook trigger — one of the lifecycle events that can fire a hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookTrigger {
    OnAgentStart,
    OnAgentEnd,
    OnTaskComplete,
    OnTaskFail,
    OnCrewComplete,
    OnRalphPass,
    OnRalphUnrecoverable,
    OnSquadRetry,
}

/// Runtime hook event — what is dispatched when a trigger fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookEvent {
    pub trigger: HookTrigger,
    /// Stable event identifier used for deduplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup_key: Option<String>,
    /// Optional agent identifier associated with the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// Optional task identifier associated with the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Optional crew identifier — used for Crew-scoped hook routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crew_id: Option<String>,
    /// Optional squad identifier — used for Squad-scoped hook routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub squad_id: Option<String>,
    /// Free-form payload (forwarded into the action context as JSON).
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
}

impl HookEvent {
    pub fn new(trigger: HookTrigger) -> Self {
        Self {
            trigger,
            dedup_key: None,
            agent_id: None,
            task_id: None,
            crew_id: None,
            squad_id: None,
            payload: serde_json::Value::Null,
            timestamp: Utc::now(),
        }
    }

    pub fn with_agent_id(mut self, agent_id: impl Into<String>) -> Self {
        self.agent_id = Some(agent_id.into());
        self
    }

    pub fn with_task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    pub fn with_crew_id(mut self, crew_id: impl Into<String>) -> Self {
        self.crew_id = Some(crew_id.into());
        self
    }

    pub fn with_squad_id(mut self, squad_id: impl Into<String>) -> Self {
        self.squad_id = Some(squad_id.into());
        self
    }

    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn with_dedup_key(mut self, key: impl Into<String>) -> Self {
        self.dedup_key = Some(key.into());
        self
    }

    /// Compute the deduplication key.
    pub fn effective_dedup_key(&self) -> String {
        if let Some(ref k) = self.dedup_key {
            return k.clone();
        }
        format!(
            "{}|{}|{:?}",
            self.agent_id.as_deref().unwrap_or("-"),
            self.task_id.as_deref().unwrap_or("-"),
            self.trigger
        )
    }
}

/// Outcome of a single hook execution — surfaced for observability and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookOutcome {
    /// Action ran to completion.
    Success,
    /// Action returned an error string.
    Failed(String),
    /// Action was skipped because the rate-limit token bucket was empty.
    RateLimited,
    /// Action exceeded its per-hook timeout.
    TimedOut,
    /// Event was deduplicated against a recent identical event.
    Deduplicated,
}

/// Per-hook execution record — emitted for every dispatch attempt.
#[derive(Debug, Clone)]
pub struct HookExecution {
    pub hook_id: String,
    pub trigger: HookTrigger,
    pub outcome: HookOutcome,
    pub timestamp: DateTime<Utc>,
}

/// Severity for [`HookAction::Log`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

/// Broadcast scope for a hook.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookScope {
    #[default]
    Global,
    Crew,
    Squad,
}

/// Action to perform when a hook fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HookAction {
    Webhook {
        url: String,
        #[serde(default)]
        headers: std::collections::HashMap<String, String>,
    },
    RedisStream {
        channel: String,
    },
    Log {
        #[serde(default)]
        level: LogLevel,
    },
    Notify {
        user_id: String,
    },
}

/// Static hook definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookDef {
    pub id: String,
    pub trigger: HookTrigger,
    #[serde(default)]
    pub scope: HookScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crew_id_filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub squad_id_filter: Option<String>,
    pub action: HookAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<crate::RateLimitConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl HookDef {
    pub fn timeout(&self) -> std::time::Duration {
        self.timeout_ms
            .map(std::time::Duration::from_millis)
            .unwrap_or_else(|| std::time::Duration::from_secs(30))
    }
}

/// Trait abstraction for the hook execution engine.
#[async_trait::async_trait]
pub trait HookEngine: Send + Sync {
    /// Dispatch a hook event to all matched hooks and await results.
    async fn emit(&self, event: HookEvent) -> Vec<HookExecution>;

    /// Subscribe to hook events broadcast by this engine.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<HookEvent>;

    /// List all currently registered hooks.
    async fn list_hooks(&self) -> Vec<HookDef>;

    /// Register a single hook, replacing any existing hook with the same id.
    async fn register(&self, def: HookDef);

    /// Replace the entire hook registry.
    async fn replace_hooks(&self, defs: Vec<HookDef>);

    /// Emit a hook event without awaiting the results.
    fn emit_detached(&self, event: HookEvent);
}

/// A single entry from the hook event archive.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HookArchiveEntry {
    pub id: i32,
    pub trigger_type: String,
    pub dedup_key: Option<String>,
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub crew_id: Option<String>,
    pub squad_id: Option<String>,
    pub payload: Value,
    pub timestamp: DateTime<Utc>,
}

/// Archive backend for durable hook event storage.
/// Implementations (e.g. PostgreSQL, file, noop) live in downstream crates.
/// Agent code holds `Arc<dyn HookArchive>` so it never depends on a concrete
/// database crate.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait HookArchive: Send + Sync {
    /// Persist a single hook event.
    async fn archive(
        &self,
        trigger_type: &str,
        dedup_key: Option<&str>,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        crew_id: Option<&str>,
        squad_id: Option<&str>,
        payload: &Value,
    ) -> SFResult<()>;

    /// Query archived hook events.
    async fn query(
        &self,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        trigger_type: Option<&str>,
        since: Option<DateTime<Utc>>,
        limit: usize,
    ) -> SFResult<Vec<HookArchiveEntry>>;
}
