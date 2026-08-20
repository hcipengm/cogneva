//! Composite sandbox backend: dispatches on payload variant. WASM modules go
//! to the local wasmtime runtime; shell commands go to the command backend
//! (remote executor pod in production, in-process in embedded mode).

use async_trait::async_trait;
use cog_core::{
    CommandEventStream, SFResult, SandboxBackend, SandboxPayload, SandboxRequest, SandboxResult,
};
use std::sync::Arc;

pub struct CompositeSandbox {
    wasm: Arc<dyn SandboxBackend>,
    executor: Arc<dyn SandboxBackend>,
}

impl CompositeSandbox {
    pub fn new(wasm: Arc<dyn SandboxBackend>, executor: Arc<dyn SandboxBackend>) -> Self {
        Self { wasm, executor }
    }

    fn route(&self, req: &SandboxRequest) -> &Arc<dyn SandboxBackend> {
        match &req.payload {
            SandboxPayload::Wasm { .. } => &self.wasm,
            SandboxPayload::Command { .. }
            | SandboxPayload::ReadFile { .. }
            | SandboxPayload::WriteFile { .. } => &self.executor,
        }
    }
}

#[async_trait]
impl SandboxBackend for CompositeSandbox {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        self.route(req).execute(req).await
    }

    async fn precompile(&self, bytes: &[u8]) -> SFResult<String> {
        self.wasm.precompile(bytes).await
    }

    async fn execute_stream(&self, req: &SandboxRequest) -> SFResult<CommandEventStream> {
        self.route(req).execute_stream(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{CommandEvent, SandboxResult};

    struct RecordingBackend {
        name: &'static str,
        hits: std::sync::Mutex<usize>,
    }

    impl RecordingBackend {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                hits: std::sync::Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl SandboxBackend for RecordingBackend {
        async fn execute(&self, _req: &SandboxRequest) -> SFResult<SandboxResult> {
            *self.hits.lock().unwrap() += 1;
            Ok(SandboxResult {
                stdout: self.name.into(),
                stderr: String::new(),
                exit_code: 0,
                output: None,
                duration_ms: 0,
                resource_usage: Default::default(),
            })
        }

        async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
            Ok(format!("{}-module", self.name))
        }
    }

    fn req(payload: SandboxPayload) -> SandboxRequest {
        SandboxRequest {
            task_id: "t".into(),
            agent_id: "a".into(),
            payload,
            input: serde_json::json!({}),
            timeout: std::time::Duration::from_secs(5),
            limits: Default::default(),
        }
    }

    #[tokio::test]
    async fn composite_dispatches_by_payload_variant() {
        let wasm = Arc::new(RecordingBackend::new("wasm"));
        let command = Arc::new(RecordingBackend::new("command"));
        let composite = CompositeSandbox::new(wasm.clone(), command.clone());

        let r = composite
            .execute(&req(SandboxPayload::Wasm {
                bytes: vec![],
                entry: "main".into(),
            }))
            .await
            .unwrap();
        assert_eq!(r.stdout, "wasm");

        let r = composite
            .execute(&req(SandboxPayload::Command {
                command: "echo hi".into(),
            }))
            .await
            .unwrap();
        assert_eq!(r.stdout, "command");

        assert_eq!(*wasm.hits.lock().unwrap(), 1);
        assert_eq!(*command.hits.lock().unwrap(), 1);
        assert_eq!(composite.precompile(b"x").await.unwrap(), "wasm-module");
    }

    #[tokio::test]
    async fn composite_stream_routes_to_executor_backend() {
        let wasm = Arc::new(RecordingBackend::new("wasm"));
        let executor = Arc::new(crate::runtime::local::LocalExecutor::new());
        let composite = CompositeSandbox::new(wasm, executor);
        let mut stream = composite
            .execute_stream(&req(SandboxPayload::Command {
                command: "echo streamed".into(),
            }))
            .await
            .unwrap();
        use futures::StreamExt;
        let mut saw_stdout = false;
        while let Some(event) = stream.next().await {
            if let CommandEvent::Stdout { data } = event {
                assert!(data.contains("streamed"));
                saw_stdout = true;
            }
        }
        assert!(saw_stdout);
    }
}
