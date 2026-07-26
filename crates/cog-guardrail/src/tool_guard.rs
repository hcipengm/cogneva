//! 工具调用前置检查 — 危险操作拦截。

use cog_core::GuardResult;
use cog_core::ToolCall;

/// 工具护栏配置。
#[derive(Debug, Clone)]
pub struct ToolGuardConfig {
    pub blocked_tools: Vec<String>,
    pub dangerous_tools: Vec<String>,
    pub require_approval_tools: Vec<String>,
    pub max_file_delete_count: u32,
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
            max_file_delete_count: 10,
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
