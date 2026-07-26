//! Unified executor — dispatches [`SandboxRequest`] to the correct runtime.

use async_trait::async_trait;
use cog_core::{
    SFResult, SandboxBackend, SandboxPayload, SandboxRequest, SandboxResult, Task, TaskExecutor,
    TaskResult, TaskResultMetadata, TaskType,
};
use std::sync::Arc;

/// Dispatcher that holds references to all enabled runtimes and routes
/// requests based on [`SandboxPayload`] variant.
pub struct ExtensionExecutor {
    wasm: Option<Arc<dyn SandboxBackend>>,
}

impl ExtensionExecutor {
    pub fn new(wasm: Option<Arc<dyn SandboxBackend>>) -> Self {
        Self { wasm }
    }

    pub async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        match &req.payload {
            SandboxPayload::Wasm { .. } => {
                let backend = self
                    .wasm
                    .as_ref()
                    .ok_or_else(|| cog_core::SFError::Agent("WASM runtime not available".into()))?;
                backend.execute(req).await
            }
        }
    }
}

/// [`cog_core::TaskExecutor`] implementation for extension workloads (`WasmSkill`).
pub struct TaskSandboxExecutor {
    backend: Arc<dyn SandboxBackend>,
    plugin_registry: Option<Arc<dyn cog_core::PluginRegistry>>,
}

impl TaskSandboxExecutor {
    pub fn new(backend: Arc<dyn SandboxBackend>) -> Self {
        Self {
            backend,
            plugin_registry: None,
        }
    }

    pub fn with_plugin_registry(mut self, registry: Arc<dyn cog_core::PluginRegistry>) -> Self {
        self.plugin_registry = Some(registry);
        self
    }
}

#[async_trait]
impl TaskExecutor for TaskSandboxExecutor {
    fn supports(&self, task_type: &TaskType) -> bool {
        matches!(task_type, TaskType::WasmSkill)
    }

    async fn execute(&self, task: &Task) -> SFResult<TaskResult> {
        let registry = self.plugin_registry.as_ref().map(|r| r.as_ref());
        let output = crate::execute_task(self.backend.as_ref(), task, registry).await?;
        let success = output
            .get("exit_code")
            .and_then(|v| v.as_i64())
            .map(|c| c == 0)
            .unwrap_or(true);
        let metadata = TaskResultMetadata::new("sandbox");
        Ok(TaskResult {
            success,
            output,
            metadata,
        })
    }
}
