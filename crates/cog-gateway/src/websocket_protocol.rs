use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::RwLock;

/// Channel name for hook events broadcast via WebSocket.
use cog_core::AgentEvent;

pub const HOOKS_CHANNEL: &str = "hooks";

/// 前缀通道：一次成功的写落在哪个 REST 前缀下，就发到 `resource:<前缀>`。
pub const RESOURCE_CHANNEL_PREFIX: &str = "resource:";

/// Client → Server WebSocket messages (type-based protocol).
///
/// 每个变体都要有客户端真发过，否则解析它的那条路和它带来的回执都是为一条不存在
/// 的消息写的。曾经这里还有 `unsubscribe` 与 `ack` 两条：两个客户端都没有发过，
/// `ack` 的处理器也只是把 id 原样回显，服务端从不使用。加新消息时把发送点在同一个
/// 改动里写上。
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
    /// 停止这一段对话里正在跑的轮。
    ///
    /// 寻址按会话，不按某一轮：客户端拿着的是会话号，一轮的身份（`message_id`）
    /// 是服务端生出来的，用户按下停止时屏幕上并没有它。同一段对话里可以有好几轮
    /// 同时在跑（用户连发两句），所以一次停止停的是这一段上全部在跑的那些。
    ///
    /// 停的那一轮会收到 `message.cancelled` 再收到 `message.end`：界面靠前一帧
    /// 把这一轮标成「被停掉」，靠后一帧收掉光标；只发后者的话，一屏被停掉的回答
    /// 与一屏「模型没有返回任何内容」长得一模一样。
    #[serde(rename = "chat_cancel")]
    ChatCancel {
        session_id: String,
        #[serde(default)]
        timestamp: Option<String>,
    },
    /// 人对一次执行前询问的回答。
    ///
    /// 按请求号寻址，不按会话号：同一段对话里可能有两问同时在等（用户连发两句），
    /// 按会话答会把答案交给错的那一个。
    ///
    /// `remember` 是答案的另一半：人要求把这一次的结论留下来（「总是允许」「总是拒绝」），
    /// 下次同类调用不再问。它只管「要不要记」，记成什么样的规则由装配层决定——网关
    /// 不认识工具的参数，也就不该替它定义「同类」。
    #[serde(rename = "permission_response")]
    PermissionResponse {
        request_id: String,
        allow: bool,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        remember: bool,
        #[serde(default)]
        timestamp: Option<String>,
    },
    /// 这一段对话处哪种权限模式。闭集由内核定义，这里只搬名字。
    #[serde(rename = "permission_mode")]
    PermissionMode {
        session_id: String,
        mode: String,
        #[serde(default)]
        timestamp: Option<String>,
    },
    /// 要一份「总是允许 / 总是拒绝」留下来的规则名单。
    ///
    /// 名单属于人，但会话号一起带着：会话范围的规则只在这一段对话里看得见，
    /// 而界面开着的就是这一段。
    #[serde(rename = "permission_rules")]
    PermissionRules {
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        timestamp: Option<String>,
    },
    /// 撤掉一条留下来的规则。回执是撤完之后的整份名单（见服务端的 `permission_rules`）。
    #[serde(rename = "permission_rule_revoke")]
    PermissionRuleRevoke {
        #[serde(default)]
        session_id: Option<String>,
        rule_id: String,
        #[serde(default)]
        timestamp: Option<String>,
    },
}

/// Server → Client WebSocket messages.
///
/// 每个变体都要有一处 `send_server_message` 走到它，否则客户端那边对应的分支就是
/// 在等一个永远不会来的帧。曾经这里还有 `task_update` / `quota_warning` / `kick` /
/// `error` 四个变体，全树没有一处构造它们，而客户端一直在等 `task_update`——那一支
/// 从写下来那天起就没有进过。要发新的帧，把构造点在同一个改动里写上。
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
    #[serde(rename = "notification")]
    Notification {
        event_id: String,
        payload: serde_json::Value,
        server_time: String,
    },
    /// 一次执行前询问：内核要动这个文件、跑这条命令，先问这条连接上的人。
    ///
    /// 按请求号寻址，不按会话号：同一段对话里可能有两问同时在等，答案带着这个
    /// 号回来才不会交到错的那一个手上。`summary` 是一句人话——工具名加它要碰
    /// 的东西，不是整份参数。
    #[serde(rename = "permission_request")]
    PermissionRequest {
        request_id: String,
        session_id: String,
        tool: String,
        summary: String,
        mode: String,
        /// 待批准的计划正文，只有 plan 模式的那一问带它。
        #[serde(skip_serializing_if = "Option::is_none")]
        plan: Option<String>,
    },
    /// 一段对话上留下来的规则：人点过「总是」的那些，带各自的撤销号。
    ///
    /// 名单里没有一条规则与「一条都没列出来」是同一件事的两面：前者是这个人手上
    /// 真的没有规则，后者是这个平台这一侧还没接上。所以这个帧只在读到了的时候发。
    #[serde(rename = "permission_rules")]
    PermissionRules {
        session_id: String,
        rules: Vec<crate::chat::PermissionRuleView>,
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

/// Translate a `TaskEvent` into the `AgentEvent::TaskStatusChange` wire shape
/// so subscribers of `task:<id>` channels see goal progress. Pure protocol
/// mapping: the task lifecycle bus stays internal, the WebSocket edge renders
/// it in the event vocabulary clients already understand.
pub fn task_event_as_status_change(event: &cog_core::TaskEvent) -> cog_core::AgentEvent {
    use cog_core::TaskEvent as TE;
    let (task_id, status, timestamp) = match event {
        TE::TaskCreated { task_id, timestamp } => (task_id, "pending", timestamp),
        TE::TaskScheduled { task_id, timestamp } => (task_id, "scheduled", timestamp),
        TE::TaskStarted { task_id, timestamp } => (task_id, "running", timestamp),
        TE::TaskCompleted {
            task_id, timestamp, ..
        } => (task_id, "completed", timestamp),
        TE::TaskFailed {
            task_id, timestamp, ..
        } => (task_id, "failed", timestamp),
        TE::TaskCancelled {
            task_id, timestamp, ..
        } => (task_id, "cancelled", timestamp),
        TE::TaskRetried {
            task_id, timestamp, ..
        } => (task_id, "retried", timestamp),
        TE::TaskTimeout {
            task_id, timestamp, ..
        } => (task_id, "timeout", timestamp),
        TE::TaskStalled {
            task_id, timestamp, ..
        } => (task_id, "stalled", timestamp),
        TE::TaskLeaseExpired {
            task_id, timestamp, ..
        } => (task_id, "lease_expired", timestamp),
    };
    cog_core::AgentEvent::TaskStatusChange {
        task_id: task_id.clone(),
        status: status.to_string(),
        agent_id: None,
        crew_id: None,
        squad_id: None,
        timestamp: *timestamp,
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
        // 一次写落在哪个前缀下。挂在它自己的通道上：只订了 `task:*` 的连接不该
        // 被设置页的保存吵醒，而订了 `resource:*` 的那一面正是为它订的。
        AgentEvent::ResourceChanged { domain, .. } => {
            vec![format!("{}{}", RESOURCE_CHANNEL_PREFIX, domain)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wildcard_subscription_matches_prefixed_channels() {
        let mgr = ConnectionManager::new();
        mgr.register("c1".into(), "u1".into(), None, None, None)
            .await;
        mgr.subscribe("c1", &["agent:*".to_string(), "hooks".to_string()])
            .await;

        assert!(
            mgr.should_deliver("c1", &["agent:worker-1".to_string()])
                .await
        );
        assert!(mgr.should_deliver("c1", &["hooks".to_string()]).await);
        assert!(!mgr.should_deliver("c1", &["task:t1".to_string()]).await);
        assert!(!mgr.should_deliver("unknown", &["hooks".to_string()]).await);
    }

    #[tokio::test]
    async fn empty_subscription_receives_everything() {
        let mgr = ConnectionManager::new();
        mgr.register("c1".into(), "u1".into(), None, None, None)
            .await;
        assert!(
            mgr.should_deliver("c1", &["agent:worker-1".to_string()])
                .await
        );
    }
}
