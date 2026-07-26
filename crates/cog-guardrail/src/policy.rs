//! 策略引擎 — 规则组合 + 优先级。

use cog_core::GuardResult;
use serde::{Deserialize, Serialize};

/// 单条策略规则。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub guard_type: GuardType,
    pub action: Action,
    pub priority: i32, // 越高越先执行
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardType {
    ContentFilter,
    PromptInjection,
    PiiDetection,
    ToolGuard,
    Custom { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Pass,
    Block,
    Warn,
    RequireApproval,
}

/// 策略引擎。
pub struct GuardPolicyEngine {
    rules: Vec<PolicyRule>,
}

impl Default for GuardPolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl GuardPolicyEngine {
    pub fn new() -> Self {
        Self { rules: vec![] }
    }

    pub fn add_rule(&mut self, rule: PolicyRule) {
        self.rules.push(rule);
        self.rules.sort_by_key(|r| -r.priority);
    }

    pub fn evaluate(&self, result: &GuardResult) -> GuardResult {
        match result {
            GuardResult::Pass => GuardResult::Pass,
            GuardResult::Block { reason, rule } => {
                // 检查是否有高优先级规则覆盖
                GuardResult::Block {
                    reason: reason.clone(),
                    rule: rule.clone(),
                }
            }
            GuardResult::Warn { reason, rule } => GuardResult::Warn {
                reason: reason.clone(),
                rule: rule.clone(),
            },
        }
    }
}

/// 默认策略。
pub struct GuardPolicy;

impl GuardPolicy {
    pub fn default_rules() -> Vec<PolicyRule> {
        vec![
            PolicyRule {
                id: "block-prompt-injection".into(),
                name: "Block Prompt Injection".into(),
                description: "Immediately block prompt injection attempts".into(),
                guard_type: GuardType::PromptInjection,
                action: Action::Block,
                priority: 100,
                enabled: true,
            },
            PolicyRule {
                id: "block-nsfw".into(),
                name: "Block NSFW".into(),
                description: "Block NSFW content".into(),
                guard_type: GuardType::ContentFilter,
                action: Action::Block,
                priority: 90,
                enabled: true,
            },
            PolicyRule {
                id: "redact-pii".into(),
                name: "Redact PII".into(),
                description: "Warn and redact PII in outputs".into(),
                guard_type: GuardType::PiiDetection,
                action: Action::Warn,
                priority: 80,
                enabled: true,
            },
            PolicyRule {
                id: "guard-dangerous-tools".into(),
                name: "Guard Dangerous Tools".into(),
                description: "Require approval for dangerous tool calls".into(),
                guard_type: GuardType::ToolGuard,
                action: Action::RequireApproval,
                priority: 70,
                enabled: true,
            },
        ]
    }
}
