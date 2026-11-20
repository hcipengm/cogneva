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

/// 默认策略的优先级：只有先后，没有量级。
///
/// 判据只看顺序（引擎按 `-priority` 排），所以一条规则的优先级就是「它在列表里
/// 排第几」——第一条最高。写死 100/90/80/70 会让「调顺序」和「调档位」看起来是
/// 两件事，而这里实际只有顺序这一件事；四个数还会各自被人单独改，改出一个
/// 顺序和肉眼读到的顺序不一致的列表。
fn priority_for(position: usize, count: usize) -> i32 {
    (count - position) as i32
}

/// 默认策略。
pub struct GuardPolicy;

impl GuardPolicy {
    /// 一条规则，优先级留空（0 = 还没排），由 [`GuardPolicy::default_rules`]
    /// 按它在列表里的位置补上。
    fn rule(
        id: &str,
        name: &str,
        description: &str,
        guard_type: GuardType,
        action: Action,
    ) -> PolicyRule {
        PolicyRule {
            id: id.into(),
            name: name.into(),
            description: description.into(),
            guard_type,
            action,
            priority: 0,
            enabled: true,
        }
    }

    pub fn default_rules() -> Vec<PolicyRule> {
        let mut rules = vec![
            Self::rule(
                "block-prompt-injection",
                "Block Prompt Injection",
                "Immediately block prompt injection attempts",
                GuardType::PromptInjection,
                Action::Block,
            ),
            Self::rule(
                "block-nsfw",
                "Block NSFW",
                "Block NSFW content",
                GuardType::ContentFilter,
                Action::Block,
            ),
            Self::rule(
                "redact-pii",
                "Redact PII",
                "Warn and redact PII in outputs",
                GuardType::PiiDetection,
                Action::Warn,
            ),
            Self::rule(
                "guard-dangerous-tools",
                "Guard Dangerous Tools",
                "Require approval for dangerous tool calls",
                GuardType::ToolGuard,
                Action::RequireApproval,
            ),
        ];
        let count = rules.len();
        for (position, rule) in rules.iter_mut().enumerate() {
            rule.priority = priority_for(position, count);
        }
        rules
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 优先级就是位置：改列表的顺序就是改执行顺序，没有第二处要跟着改。
    #[test]
    fn the_default_ladder_is_its_position() {
        let rules = GuardPolicy::default_rules();
        let count = rules.len();
        for (position, rule) in rules.iter().enumerate() {
            assert_eq!(rule.priority, (count - position) as i32, "{}", rule.id);
        }
        assert!(
            rules
                .windows(2)
                .all(|pair| pair[0].priority > pair[1].priority),
            "相邻两条的先后必须严格分开"
        );
    }

    /// 换输入 ⇒ 值跟着变：同一个位置在更长的列表里拿到更高的优先级，倒过来也一样。
    #[test]
    fn the_ladder_moves_with_the_list() {
        assert_eq!(priority_for(0, 4), 4);
        assert_eq!(priority_for(3, 4), 1);
        assert_eq!(priority_for(0, 5), 5);
        assert_eq!(priority_for(4, 5), 1);
    }
}
