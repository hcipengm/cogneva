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
            SandboxPayload::Command { .. }
            | SandboxPayload::ReadFile { .. }
            | SandboxPayload::WriteFile { .. }
            | SandboxPayload::EditFile { .. }
            | SandboxPayload::CaptureCheckpoint { .. }
            | SandboxPayload::PlanCheckpoint { .. }
            | SandboxPayload::RestoreCheckpoint { .. } => Err(cog_core::SFError::Agent(
                "ExtensionExecutor only dispatches WASM payloads".into(),
            )),
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
        // 没有 `exit_code` 不等于成功。`SandboxResult::into_json` 把后端自带的
        // `output` 原样交出来，那份对象里可能一个成功信号都没有；缺了读数就得
        // 读作「没有证据说它跑成了」，而不是按默认值记成跑成了。
        let success = output
            .get("exit_code")
            .and_then(|v| v.as_i64())
            .map(|c| c == 0)
            .unwrap_or(false);
        let metadata = TaskResultMetadata::new("sandbox");
        Ok(TaskResult {
            success,
            output,
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{PluginManifest, PluginRegistry, SandboxRequest};

    /// A backend that answers with its own `output` object and no exit code in
    /// it. `SandboxResult::into_json` hands a supplied `output` back verbatim,
    /// so the JSON the executor reads carries no success signal at all.
    struct AnswersWithOutputOnly;

    #[async_trait::async_trait]
    impl SandboxBackend for AnswersWithOutputOnly {
        async fn execute(&self, _req: &SandboxRequest) -> SFResult<SandboxResult> {
            Ok(SandboxResult {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 0,
                output: Some(serde_json::json!({ "stdout": "the module's own answer" })),
                duration_ms: 0,
                resource_usage: Default::default(),
                workdir: None,
            })
        }

        async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
            Ok("compiled".into())
        }
    }

    /// Supplies the bytes the task asks for; what they contain never matters
    /// here, because the backend above runs none of them.
    struct HandsOutModule;

    #[async_trait::async_trait]
    impl PluginRegistry for HandsOutModule {
        async fn discover(&self, _source: &str) -> SFResult<Vec<PluginManifest>> {
            Ok(vec![])
        }

        async fn fetch(&self, _manifest: &PluginManifest) -> SFResult<Vec<u8>> {
            Ok(b"\0asm\x01\0\0\0".to_vec())
        }

        async fn fetch_by_id(&self, _plugin_id: &str) -> SFResult<Vec<u8>> {
            Ok(b"\0asm\x01\0\0\0".to_vec())
        }
    }

    /// 没有 `exit_code` 的答复只是「没有结局可读」，不是成功。把缺读数读成
    /// 成功，任何一个自带 output 的后端都能让每一条任务看起来跑成了。
    #[tokio::test]
    async fn a_run_that_reported_no_exit_code_is_not_read_as_a_success() {
        let executor = TaskSandboxExecutor::new(Arc::new(AnswersWithOutputOnly))
            .with_plugin_registry(Arc::new(HandsOutModule));
        let task = Task::new(
            "a-task",
            TaskType::WasmSkill,
            serde_json::json!({ "plugin_id": "demo" }),
        );

        let result = executor
            .execute(&task)
            .await
            .expect("the backend answered, so the executor has an outcome to report");

        assert!(
            !result.success,
            "nothing said this run succeeded, so it may not be reported as a success: {:?}",
            result.output
        );
    }
}
