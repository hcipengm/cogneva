//! 工具调用前置检查 — 危险操作拦截。

use cog_core::GuardResult;
use cog_core::ToolCall;

/// 工具护栏配置。
#[derive(Debug, Clone)]
pub struct ToolGuardConfig {
    pub blocked_tools: Vec<String>,
    pub dangerous_tools: Vec<String>,
    pub require_approval_tools: Vec<String>,
    // 没有「一次最多删几个文件」这一项：护栏里没有任何一处读它，而一个没人读的界不是
    // 界，是配置面在声称一件代码没做的事。真要做「一次删太多」的判定，判定点
    // 得先存在，界才轮到从某条读数导出。
    pub allowed_domains: Vec<String>,
}

impl Default for ToolGuardConfig {
    fn default() -> Self {
        Self {
            blocked_tools: vec!["execute_shell".into(), "exec_code".into()],
            dangerous_tools: vec![
                "delete_file".into(),
                "write_file".into(),
                "send_email".into(),
            ],
            require_approval_tools: vec!["database_migration".into(), "deploy_production".into()],
            allowed_domains: vec![],
        }
    }
}

/// 工具调用护栏。
pub struct ToolGuard {
    config: ToolGuardConfig,
}

impl ToolGuard {
    pub fn new(config: ToolGuardConfig) -> Self {
        Self { config }
    }

    pub fn check(&self, tool: &ToolCall) -> GuardResult {
        let tool_name = &tool.name;

        if self.config.blocked_tools.contains(tool_name) {
            return GuardResult::Block {
                reason: format!("Tool '{}' is permanently blocked", tool_name),
                rule: "tool_guard:blocked".into(),
            };
        }

        if self.config.require_approval_tools.contains(tool_name) {
            return GuardResult::Warn {
                reason: format!("Tool '{}' requires human approval", tool_name),
                rule: "tool_guard:approval_required".into(),
            };
        }

        if self.config.dangerous_tools.contains(tool_name) {
            return GuardResult::Warn {
                reason: format!("Tool '{}' is flagged as dangerous", tool_name),
                rule: "tool_guard:dangerous".into(),
            };
        }

        // Domain whitelist check for URL-related tools
        if !self.config.allowed_domains.is_empty() {
            if let Some(url) = tool.arguments.get("url").and_then(|u| u.as_str()) {
                let allowed = self.config.allowed_domains.iter().any(|d| url.contains(d));
                if !allowed {
                    return GuardResult::Block {
                        reason: format!("URL '{}' not in allowed domains", url),
                        rule: "tool_guard:domain".into(),
                    };
                }
            }
        }

        GuardResult::Pass
    }
}

#[async_trait::async_trait]
impl cog_core::Guardrail for ToolGuard {
    async fn check_input(&self, _messages: &[cog_core::Message]) -> GuardResult {
        GuardResult::Pass
    }

    async fn check_output(&self, _response: &str) -> GuardResult {
        GuardResult::Pass
    }

    async fn check_tool_call(&self, tool: &ToolCall) -> GuardResult {
        self.check(tool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 配置面上不许留没人读的界。
    ///
    /// 一项「一次最多删几个文件」的界曾经在这里，护栏里没有任何一处读它——删掉而不是
    /// 给它命名：一个没人读的界不是界，是配置面在声称一件代码没做的事。
    #[test]
    fn the_guard_config_claims_no_bound_it_never_reads() {
        // 针不在这一行里原样出现：否则这条判据读到的就是它自己。
        let needle = concat!("max_file_delete_", "count");
        assert!(
            !include_str!("tool_guard.rs").contains(needle),
            "护栏不读的界不许留在配置面上"
        );
        // 删除之后结构仍然建得出来：剩下的三个工具清单与域名清单才是它的全部。
        let config = ToolGuardConfig::default();
        assert!(config.blocked_tools.contains(&"execute_shell".to_string()));
        assert!(config.allowed_domains.is_empty());
    }
}
