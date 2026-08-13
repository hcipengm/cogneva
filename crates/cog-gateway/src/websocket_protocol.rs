use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::RwLock;

/// Channel name for hook events broadcast via WebSocket.
use cog_core::AgentEvent;

pub const HOOKS_CHANNEL: &str = "hooks";

/// Client → Server WebSocket messages (type-based protocol).
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "ping")]
    Ping {
        #[serde(default)]
        timestamp: Option<String>,
        seq: u64,
    },
    #[serde(rename = "subscribe")]
    Subscribe {
        channels: Vec<String>,
        #[serde(default)]
        timestamp: Option<String>,
    },
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        channels: Vec<String>,
        #[serde(default)]
        timestamp: Option<String>,
    },
    #[serde(rename = "ack")]
    Ack {
        event_ids: Vec<String>,
        #[serde(default)]
        timestamp: Option<String>,
    },
    #[serde(rename = "typing")]
    Typing {
        session_id: String,
        is_typing: bool,
        #[serde(default)]
        timestamp: Option<String>,
    },
    /// Web UI chat input — routed to the LLM; the reply is streamed back to
    /// the requesting connection as `agent_event` envelopes
    /// (message.start / message.text_delta / message.end).
    #[serde(rename = "chat_message")]
    ChatMessage {
        #[serde(default)]
        session_id: Option<String>,
        content: String,
        #[serde(default)]
        timestamp: Option<String>,
    },
}

/// Server → Client WebSocket messages.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "connected")]
    Connected {
        connection_id: String,
        server_time: String,
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        missed_events: Vec<String>,
    },
    #[serde(rename = "pong")]
    Pong {
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<String>,
        seq: u64,
        server_time: String,
    },
    #[serde(rename = "subscribed")]
    Subscribed {
        channels: Vec<String>,
        server_time: String,
    },
    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        channels: Vec<String>,
        server_time: String,
    },
    #[serde(rename = "acknowledged")]
    Acknowledged {
        event_ids: Vec<String>,
        server_time: String,
    },
    #[serde(rename = "agent_event")]
    AgentEvent {
        event_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
        payload: serde_json::Value,
        server_time: String,
    },
    #[serde(rename = "task_update")]
    TaskUpdate {
        event_id: String,
        task_id: String,
        payload: serde_json::Value,
        server_time: String,
    },
    #[serde(rename = "notification")]
    Notification {
        event_id: String,
        payload: serde_json::Value,
        server_time: String,
    },
    #[serde(rename = "quota_warning")]
    QuotaWarning {
        event_id: String,
        payload: serde_json::Value,
        server_time: String,
    },
    #[serde(rename = "kick")]
    Kick {
        event_id: String,
        payload: serde_json::Value,
        server_time: String,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        event_id: Option<String>,
        code: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after: Option<u64>,
        server_time: String,
    },
}

/// Per-connection state.
#[derive(Clone)]
pub struct ConnectionState {
    pub connection_id: String,
    pub user_id: String,
    pub subscribed_channels: HashSet<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub device_id: Option<String>,
    pub platform: Option<String>,
    pub app_version: Option<String>,
}

/// A cached event entry for missed-event recovery.
struct CachedEvent {
    event_id: String,
    timestamp: chrono::DateTime<chrono::Utc>,
    _payload: serde_json::Value,
}

/// Manages active WebSocket connections and a rolling cache of recent events.
pub struct ConnectionManager {
    connections: RwLock<HashMap<String, ConnectionState>>,
    event_cache: RwLock<VecDeque<CachedEvent>>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self::with_event_cache_capacity(1000)
    }

    pub fn with_event_cache_capacity(capacity: usize) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            event_cache: RwLock::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub async fn register(
        &self,
        connection_id: String,
        user_id: String,
        device_id: Option<String>,
        platform: Option<String>,
        app_version: Option<String>,
    ) {
        let mut conns = self.connections.write().await;
        let now = chrono::Utc::now();
        conns.insert(
            connection_id.clone(),
            ConnectionState {
                connection_id,
                user_id,
                subscribed_channels: HashSet::new(),
                created_at: now,
                last_activity: now,
                device_id,
                platform,
                app_version,
            },
        );
    }

    pub async fn unregister(&self, connection_id: &str) {
        let mut conns = self.connections.write().await;
        conns.remove(connection_id);
    }

    pub async fn subscribe(&self, connection_id: &str, channels: &[String]) -> Vec<String> {
        let mut conns = self.connections.write().await;
        let mut subscribed = Vec::new();
        if let Some(conn) = conns.get_mut(connection_id) {
            for ch in channels {
                if conn.subscribed_channels.insert(ch.clone()) {
                    subscribed.push(ch.clone());
                }
            }
        }
        subscribed
    }

    pub async fn unsubscribe(&self, connection_id: &str, channels: &[String]) -> Vec<String> {
        let mut conns = self.connections.write().await;
        let mut unsubscribed = Vec::new();
        if let Some(conn) = conns.get_mut(connection_id) {
            for ch in channels {
                if conn.subscribed_channels.remove(ch) {
                    unsubscribed.push(ch.clone());
                }
            }
        }
        unsubscribed
    }

    /// Update the last_activity timestamp for a connection.
    pub async fn touch(&self, connection_id: &str) {
        let mut conns = self.connections.write().await;
        if let Some(conn) = conns.get_mut(connection_id) {
            conn.last_activity = chrono::Utc::now();
        }
    }

    /// Return the number of active connections.
    pub async fn connection_count(&self) -> usize {
        let conns = self.connections.read().await;
        conns.len()
    }

    /// Return a list of all active connection IDs.
    pub async fn list_connections(&self) -> Vec<String> {
        let conns = self.connections.read().await;
        conns.keys().cloned().collect()
    }

    /// Return all connections for a given user.
    pub async fn connections_for_user(&self, user_id: &str) -> Vec<ConnectionState> {
        let conns = self.connections.read().await;
        conns
            .values()
            .filter(|c| c.user_id == user_id)
            .cloned()
            .collect()
    }

    /// Kick all connections for a given user, returning the kicked connection IDs.
    pub async fn kick_user(&self, user_id: &str) -> Vec<String> {
        let mut conns = self.connections.write().await;
        let to_kick: Vec<String> = conns
            .values()
            .filter(|c| c.user_id == user_id)
            .map(|c| c.connection_id.clone())
            .collect();
        for cid in &to_kick {
            conns.remove(cid);
        }
        to_kick
    }

    /// Kick a specific connection by ID.
    pub async fn kick_connection(&self, connection_id: &str) -> bool {
        let mut conns = self.connections.write().await;
        conns.remove(connection_id).is_some()
    }

    /// Remove connections that have been inactive longer than the given duration.
    /// Returns the removed connection IDs.
    pub async fn prune_inactive(&self, max_inactive: chrono::Duration) -> Vec<(String, String)> {
        let mut conns = self.connections.write().await;
        let cutoff = chrono::Utc::now() - max_inactive;
        let to_remove: Vec<String> = conns
            .values()
            .filter(|c| c.last_activity < cutoff)
            .map(|c| c.connection_id.clone())
            .collect();
        let mut removed = Vec::new();
        for cid in to_remove {
            if let Some(state) = conns.remove(&cid) {
                removed.push((cid, state.user_id));
            }
        }
        removed
    }

    /// Return event IDs from the last 5 minutes as "missed events".
    pub async fn get_missed_event_ids(&self) -> Vec<String> {
        let cache = self.event_cache.read().await;
        let cutoff = chrono::Utc::now() - chrono::Duration::minutes(5);
        cache
            .iter()
            .filter(|e| e.timestamp > cutoff)
            .map(|e| e.event_id.clone())
            .collect()
    }

    /// Record an event in the global cache (evicts entries older than 5 minutes
    /// and keeps at most 1000 entries).
    pub async fn record_event(&self, event_id: String, payload: serde_json::Value) {
        let mut cache = self.event_cache.write().await;
        let cutoff = chrono::Utc::now() - chrono::Duration::minutes(5);

        while cache
            .front()
            .map(|e| e.timestamp <= cutoff)
            .unwrap_or(false)
        {
            cache.pop_front();
        }

        cache.push_back(CachedEvent {
            event_id,
            timestamp: chrono::Utc::now(),
            _payload: payload,
        });

        while cache.len() > 1000 {
            cache.pop_front();
        }
    }

    /// Check whether an event should be delivered to a given connection
    /// based on its subscribed channels. If the connection has no subscriptions,
    /// all events are delivered (backward-compatible behaviour).
    /// A subscription ending in `*` matches by prefix (e.g. `agent:*` receives
    /// every `agent:<id>` channel).
    pub async fn should_deliver(&self, connection_id: &str, event_channels: &[String]) -> bool {
        let conns = self.connections.read().await;
        let Some(conn) = conns.get(connection_id) else {
            return false;
        };
        if conn.subscribed_channels.is_empty() {
            return true;
        }
        event_channels.iter().any(|ch| {
            conn.subscribed_channels.iter().any(|sub| {
                sub == ch
                    || sub
                        .strip_suffix('*')
                        .is_some_and(|prefix| ch.starts_with(prefix))
            })
        })
    }
}

/// Derive the logical channel names for an `AgentEvent`.
pub fn event_channels(event: &cog_core::AgentEvent) -> Vec<String> {
    match event {
        AgentEvent::TaskStatusChange {
            task_id, status, ..
        } => {
            // Hook events forwarded by HookToWsForwarder use status="hook_event".
            if status == "hook_event" {
                vec![HOOKS_CHANNEL.to_string()]
            } else {
                vec![format!("task:{}", task_id)]
            }
        }
        AgentEvent::AgentStart { agent_id, .. }
        | AgentEvent::AgentEnd { agent_id, .. }
        | AgentEvent::TurnStart { agent_id, .. }
        | AgentEvent::TurnEnd { agent_id, .. }
        | AgentEvent::MessageStart { agent_id, .. }
        | AgentEvent::MessageUpdate { agent_id, .. }
        | AgentEvent::MessageEnd { agent_id, .. }
        | AgentEvent::ToolExecutionStart { agent_id, .. }
        | AgentEvent::ToolExecutionUpdate { agent_id, .. }
        | AgentEvent::ToolExecutionEnd { agent_id, .. }
        | AgentEvent::StateChange { agent_id, .. }
        | AgentEvent::SelfReview { agent_id, .. }
        | AgentEvent::ReActStepStart { agent_id, .. }
        | AgentEvent::ReActStepEnd { agent_id, .. }
        | AgentEvent::AgentError { agent_id, .. }
        | AgentEvent::ResourceAlert { agent_id, .. }
        | AgentEvent::Heartbeat { agent_id, .. }
        | AgentEvent::CheckpointSaved { agent_id, .. } => {
            vec![format!("agent:{}", agent_id)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wildcard_subscription_matches_prefixed_channels() {
        let mgr = ConnectionManager::new();
        mgr.register("c1".into(), "u1".into(), None, None, None).await;
        mgr.subscribe("c1", &["agent:*".to_string(), "hooks".to_string()])
            .await;

        assert!(mgr
            .should_deliver("c1", &["agent:worker-1".to_string()])
            .await);
        assert!(mgr.should_deliver("c1", &["hooks".to_string()]).await);
        assert!(!mgr
            .should_deliver("c1", &["task:t1".to_string()])
            .await);
        assert!(!mgr.should_deliver("unknown", &["hooks".to_string()]).await);
    }

    #[tokio::test]
    async fn empty_subscription_receives_everything() {
        let mgr = ConnectionManager::new();
        mgr.register("c1".into(), "u1".into(), None, None, None).await;
        assert!(mgr
            .should_deliver("c1", &["agent:worker-1".to_string()])
            .await);
    }
}
