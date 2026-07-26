use crate::{Message, ToolCall};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Guardrail check result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardResult {
    Pass,
    Block { reason: String, rule: String },
    Warn { reason: String, rule: String },
}

/// Type of guardrail check.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckType {
    Input,
    Output,
    ToolCall,
}

/// Snapshot of a guardrail result for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardResultSnapshot {
    pub verdict: String,
    pub reason: Option<String>,
    pub rule: Option<String>,
}

impl From<&GuardResult> for GuardResultSnapshot {
    fn from(r: &GuardResult) -> Self {
        match r {
            GuardResult::Pass => Self {
                verdict: "pass".into(),
                reason: None,
                rule: None,
            },
            GuardResult::Block { reason, rule } => Self {
                verdict: "block".into(),
                reason: Some(reason.clone()),
                rule: Some(rule.clone()),
            },
            GuardResult::Warn { reason, rule } => Self {
                verdict: "warn".into(),
                reason: Some(reason.clone()),
                rule: Some(rule.clone()),
            },
        }
    }
}

/// Audit log entry for a guardrail decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardAuditLog {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub guard_type: String,
    pub check_type: CheckType,
    pub result: GuardResultSnapshot,
    pub input_hash: String,
}

/// Unified guardrail trait.
#[async_trait::async_trait]
pub trait Guardrail: Send + Sync {
    /// Input guard: check user input before LLM call.
    async fn check_input(&self, messages: &[Message]) -> GuardResult;
    /// Output guard: check LLM response.
    async fn check_output(&self, response: &str) -> GuardResult;
    /// Tool guard: check before tool execution.
    async fn check_tool_call(&self, tool: &ToolCall) -> GuardResult;
}

/// Audit recorder trait for guardrail decisions.
#[async_trait::async_trait]
pub trait GuardAuditRecorder: Send + Sync {
    async fn record_input_check(&self, messages: &[Message], result: &GuardResult);
    async fn record_output_check(&self, response: &str, result: &GuardResult);
    async fn record_tool_check(&self, tool: &ToolCall, result: &GuardResult);
}

#[async_trait::async_trait]
impl GuardAuditRecorder for Arc<dyn GuardAuditRecorder> {
    async fn record_input_check(&self, messages: &[Message], result: &GuardResult) {
        self.as_ref().record_input_check(messages, result).await
    }
    async fn record_output_check(&self, response: &str, result: &GuardResult) {
        self.as_ref().record_output_check(response, result).await
    }
    async fn record_tool_check(&self, tool: &ToolCall, result: &GuardResult) {
        self.as_ref().record_tool_check(tool, result).await
    }
}
