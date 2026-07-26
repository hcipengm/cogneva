//! 护栏决策审计日志。

use chrono::Utc;
use cog_core::{CheckType, GuardAuditLog, GuardAuditRecorder, GuardResult, Message, ToolCall};
use std::sync::Arc;
use tokio::sync::Mutex;

/// 内存审计记录器。
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct InMemoryAuditRecorder {
    logs: Arc<Mutex<Vec<GuardAuditLog>>>,
}

impl Default for InMemoryAuditRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryAuditRecorder {
    pub fn new() -> Self {
        Self {
            logs: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn logs(&self) -> Vec<GuardAuditLog> {
        self.logs.lock().await.clone()
    }

    fn hash_input(messages: &[Message]) -> String {
        let text: String = messages.iter().map(|m| m.content()).collect();
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    fn hash_text(text: &str) -> String {
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    async fn push(
        &self,
        guard_type: &str,
        check_type: CheckType,
        result: &GuardResult,
        input_hash: String,
    ) {
        let log = GuardAuditLog {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            guard_type: guard_type.into(),
            check_type,
            result: result.into(),
            input_hash,
        };
        self.logs.lock().await.push(log);
    }
}

#[async_trait::async_trait]
impl GuardAuditRecorder for InMemoryAuditRecorder {
    async fn record_input_check(&self, messages: &[Message], result: &GuardResult) {
        self.push(
            "composite",
            CheckType::Input,
            result,
            Self::hash_input(messages),
        )
        .await;
    }

    async fn record_output_check(&self, response: &str, result: &GuardResult) {
        self.push(
            "composite",
            CheckType::Output,
            result,
            Self::hash_text(response),
        )
        .await;
    }

    async fn record_tool_check(&self, _tool: &ToolCall, result: &GuardResult) {
        self.push("composite", CheckType::ToolCall, result, String::new())
            .await;
    }
}
