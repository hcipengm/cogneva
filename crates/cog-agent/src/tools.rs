use cog_core::{SFResult, SandboxBackend, SandboxPayload, SandboxRequest};
use cog_core::{Tool, ToolImplementation};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ToolRegistry {
    tools: Arc<std::sync::RwLock<HashMap<String, Tool>>>,
    sandbox_backend: Option<Arc<dyn SandboxBackend>>,
    guardrail: Option<Arc<dyn cog_core::Guardrail>>,
    plugin_registry: Option<Arc<dyn cog_core::PluginRegistry>>,
    wasm_timeout: Duration,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: Arc::new(std::sync::RwLock::new(HashMap::new())),
            sandbox_backend: None,
            guardrail: None,
            plugin_registry: None,
            wasm_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_wasm_timeout(mut self, secs: u64) -> Self {
        self.wasm_timeout = Duration::from_secs(secs);
        self
    }

    pub fn with_sandbox_backend(mut self, backend: Arc<dyn SandboxBackend>) -> Self {
        self.sandbox_backend = Some(backend);
        self
    }

    pub fn set_sandbox_backend(&mut self, backend: Arc<dyn SandboxBackend>) {
        self.sandbox_backend = Some(backend);
    }

    pub fn with_guardrail(mut self, guardrail: Arc<dyn cog_core::Guardrail>) -> Self {
        self.guardrail = Some(guardrail);
        self
    }

    pub fn set_guardrail(&mut self, guardrail: Arc<dyn cog_core::Guardrail>) {
        self.guardrail = Some(guardrail);
    }

    pub fn with_plugin_registry(mut self, registry: Arc<dyn cog_core::PluginRegistry>) -> Self {
        self.plugin_registry = Some(registry);
        self
    }

    pub fn set_plugin_registry(&mut self, registry: Arc<dyn cog_core::PluginRegistry>) {
        self.plugin_registry = Some(registry);
    }

    pub fn get(&self, name: &str) -> Option<Tool> {
        self.tools.read().unwrap().get(name).cloned()
    }

    pub fn list(&self) -> Vec<Tool> {
        self.tools.read().unwrap().values().cloned().collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.tools.read().unwrap().keys().cloned().collect()
    }

    pub async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        let tool = self
            .tools
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| cog_core::SFError::Agent(format!("Tool not found: {}", name)))?;

        // Guardrail check before tool execution
        if let Some(ref guardrail) = self.guardrail {
            let tool_call = cog_core::ToolCall {
                id: uuid::Uuid::new_v4().to_string(),
                name: name.into(),
                arguments: arguments.clone(),
            };
            match guardrail.check_tool_call(&tool_call).await {
                cog_core::GuardResult::Pass => {}
                cog_core::GuardResult::Block { reason, rule } => {
                    return Err(cog_core::SFError::Agent(format!(
                        "Guardrail blocked tool '{}': {} (rule: {})",
                        name, reason, rule
                    )));
                }
                cog_core::GuardResult::Warn { reason, rule } => {
                    tracing::warn!(
                        "Guardrail warned on tool '{}': {} (rule: {})",
                        name,
                        reason,
                        rule
                    );
                }
            }
        }

        match &tool.implementation {
            ToolImplementation::Native(handler) => (handler)(arguments).await,
            ToolImplementation::Wasm {
                plugin_id,
                export_name,
            } => {
                let backend = self.sandbox_backend.as_ref().ok_or_else(|| {
                    cog_core::SFError::Agent("SandboxBackend not configured for WASM tool".into())
                })?;
                let bytes = if let Some(ref registry) = self.plugin_registry {
                    match registry.fetch_by_id(plugin_id).await {
                        Ok(b) => b,
                        Err(e) => {
                            return Err(cog_core::SFError::Agent(format!(
                                "Failed to fetch plugin '{}': {}",
                                plugin_id, e
                            )));
                        }
                    }
                } else {
                    return Err(cog_core::SFError::Agent(
                        "PluginRegistry not configured for WASM tool".into(),
                    ));
                };
                let req = SandboxRequest {
                    task_id: format!("tool-{}", name),
                    agent_id: plugin_id.clone(),
                    payload: SandboxPayload::Wasm {
                        bytes,
                        entry: export_name.clone(),
                    },
                    input: arguments,
                    timeout: self.wasm_timeout,
                    limits: Default::default(),
                };
                let result = backend.execute(&req).await?;
                Ok(result.into_json())
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tools.read().unwrap().is_empty()
    }
}

#[async_trait::async_trait]
impl cog_core::ToolExecutor for ToolRegistry {
    async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> cog_core::SFResult<serde_json::Value> {
        self.execute(name, arguments).await
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl cog_core::ToolRegistry for ToolRegistry {
    fn register(&self, tool: cog_core::Tool) {
        self.tools.write().unwrap().insert(tool.name.clone(), tool);
    }
}

/// 内置工具工厂。
pub mod builtins {
    use super::*;

    pub fn read_file() -> Tool {
        Tool {
            name: "read_file".into(),
            description: "Read the contents of a file".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path" }
                },
                "required": ["path"]
            }),
            implementation: ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move {
                    let path = args["path"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("path required".into()))?;
                    let content = tokio::fs::read_to_string(path)
                        .await
                        .map_err(|e| cog_core::SFError::IO(e.to_string()))?;
                    Ok(serde_json::json!({ "content": content }))
                })
            })),
        }
    }

    pub fn write_file() -> Tool {
        Tool {
            name: "write_file".into(),
            description: "Write content to a file".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"]
            }),
            implementation: ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move {
                    let path = args["path"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("path required".into()))?;
                    let content = args["content"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("content required".into()))?;
                    tokio::fs::write(path, content)
                        .await
                        .map_err(|e| cog_core::SFError::IO(e.to_string()))?;
                    Ok(serde_json::json!({ "success": true }))
                })
            })),
        }
    }

    pub fn run_command() -> Tool {
        Tool {
            name: "run_command".into(),
            description: "Run a shell command".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "Command to run" }
                },
                "required": ["command"]
            }),
            implementation: ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move {
                    let command = args["command"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("command required".into()))?;
                    // Security: reject shell metacharacters to prevent command injection.
                    // Only simple single-command execution is allowed.
                    const SHELL_META: &[char] = &[';', '&', '|', '`', '$', '<', '>', '(', ')'];
                    if command.chars().any(|c| SHELL_META.contains(&c)) {
                        return Err(cog_core::SFError::Validation(
                            "Command contains shell metacharacters. Only simple commands are allowed.".into()
                        ));
                    }
                    tracing::warn!(command = %command, "run_command executing");
                    let parts: Vec<&str> = command.split_whitespace().collect();
                    if parts.is_empty() {
                        return Err(cog_core::SFError::Validation("empty command".into()));
                    }
                    let mut cmd = tokio::process::Command::new(parts[0]);
                    for part in &parts[1..] {
                        cmd.arg(part);
                    }
                    let output = cmd
                        .output()
                        .await
                        .map_err(|e| cog_core::SFError::IO(e.to_string()))?;
                    Ok(serde_json::json!({
                        "stdout": String::from_utf8_lossy(&output.stdout),
                        "stderr": String::from_utf8_lossy(&output.stderr),
                        "code": output.status.code()
                    }))
                })
            })),
        }
    }

    pub fn search_code() -> Tool {
        Tool {
            name: "search_code".into(),
            description: "Search for code patterns in the codebase".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query" }
                },
                "required": ["query"]
            }),
            implementation: ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move {
                    let query = args["query"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("query required".into()))?;
                    // Placeholder - actual implementation would use ripgrep or similar
                    Ok(serde_json::json!({ "results": [], "query": query }))
                })
            })),
        }
    }
}
