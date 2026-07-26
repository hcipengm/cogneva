use cog_agent::hooks::{HookEngine, HookEngineConfig, HookExecution, HookPublisher, HookTrigger};
use cog_collaboration::{PgeMode, SquadConfig, SquadExecutor, SquadResult};
use cog_core::HookEngine as HookEngineTrait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::mocks::{
    MockLLMProvider, MockMetricsBackend, MockObjectBackend, MockStateBackend,
};

// ─── Mock Agent & AgentManager for smoke tests ─────────────────────────────

#[allow(dead_code)]
struct MockSquadAgent {
    response: serde_json::Value,
}

#[async_trait::async_trait]
impl cog_core::Agent for MockSquadAgent {
    async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }

    async fn start(&self) {}

    async fn snapshot(&self, _task_id: String) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
        Ok(cog_core::AgentCheckpoint {
            checkpoint_id: String::new(),
            task_id: String::new(),
            agent_state: serde_json::Value::Null,
            context_window: Vec::new(),
            event_offset: 0,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn continue_(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }

    async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn abort(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn reset(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
        Ok(cog_core::AgentState::Idle)
    }

    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
        Ok(())
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }

    async fn chat_stream(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
        producer.end(cog_core::ChatResponse::default());
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.chat_stream(&[], &cog_core::ChatOptions::default())
            .await
    }

    async fn read_board(&self, _task_id: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        Ok(None)
    }

    async fn write_board(
        &self,
        _task_id: &str,
        _field: &str,
        _value: &str,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn review_output(
        &self,
        _output: &str,
        _config: &cog_core::SelfReviewConfig,
    ) -> cog_core::SFResult<cog_core::SelfReviewResult> {
        Ok(cog_core::SelfReviewResult::Pass {
            score: 1.0,
            summary: "mock pass".into(),
        })
    }
}

#[allow(dead_code)]
struct MockAgentManager;

#[async_trait::async_trait]
impl cog_core::AgentManager for MockAgentManager {
    async fn create_agent(
        &self,
        _agent_id: &str,
        role: &str,
        _llm: Arc<dyn cog_core::LlmClient>,
    ) -> cog_core::SFResult<Arc<dyn cog_core::Agent>> {
        let response = match role {
            "planner" => serde_json::json!({
                "summary": "test analysis",
                "plan": {"specification": "test spec", "design": "test design"},
                "sub_tasks": [{"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}],
            }),
            "generator" => serde_json::json!({
                "content": {"code": "fn main() {}", "tests": "", "documentation": ""},
                "artifacts": [],
            }),
            "evaluator" => serde_json::json!({
                "verdict": "pass",
                "score": 92,
                "feedback": "good",
                "criteria": [],
            }),
            _ => serde_json::json!({}),
        };
        Ok(Arc::new(MockSquadAgent { response }))
    }

    async fn dispatch(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn list_workers(&self) -> cog_core::SFResult<Vec<cog_core::WorkerInfo>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn get_agent(
        &self,
        _agent_id: &str,
    ) -> cog_core::SFResult<Option<Arc<dyn cog_core::Agent>>> {
        Ok(None)
    }
}

/// Test harness that wires together all mock backends for end-to-end Squad testing.
#[allow(dead_code)]
pub struct TestHarness {
    pub llm: Arc<MockLLMProvider>,
    pub state: Arc<MockStateBackend>,
    pub object: Arc<MockObjectBackend>,
    pub metrics: Arc<MockMetricsBackend>,
    pub hook_engine: Arc<dyn HookEngineTrait>,
    pub squad_executor: tokio::sync::Mutex<SquadExecutor>,
    hook_recorder: Arc<HookRecorder>,
}

/// Records hook executions for assertion.
struct HookRecorder {
    calls: Mutex<Vec<(HookTrigger, HookExecution)>>,
}

#[allow(dead_code)]
impl HookRecorder {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, trigger: HookTrigger, exec: HookExecution) {
        self.calls.lock().unwrap().push((trigger, exec));
    }

    fn count_for(&self, trigger: HookTrigger) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| *t == trigger)
            .count()
    }
}

/// Builder for [`TestHarness`].
#[allow(dead_code)]
pub struct TestHarnessBuilder {
    with_llm: bool,
    with_state: bool,
    with_object: bool,
    with_metrics: bool,
    with_hooks: bool,
    with_squad: bool,
}

#[allow(dead_code)]
impl TestHarnessBuilder {
    pub fn new() -> Self {
        Self {
            with_llm: false,
            with_state: false,
            with_object: false,
            with_metrics: false,
            with_hooks: false,
            with_squad: false,
        }
    }

    pub fn with_mock_llm(mut self) -> Self {
        self.with_llm = true;
        self
    }

    pub fn with_mock_state_backend(mut self) -> Self {
        self.with_state = true;
        self
    }

    #[allow(dead_code)]
    pub fn with_mock_object_backend(mut self) -> Self {
        self.with_object = true;
        self
    }

    #[allow(dead_code)]
    pub fn with_mock_metrics_backend(mut self) -> Self {
        self.with_metrics = true;
        self
    }

    pub fn with_hook_engine(mut self) -> Self {
        self.with_hooks = true;
        self
    }

    pub fn with_squad_executor(mut self) -> Self {
        self.with_squad = true;
        self
    }

    pub fn build(self) -> TestHarness {
        let llm = Arc::new(MockLLMProvider::new());
        let state = Arc::new(MockStateBackend::new());
        let object = Arc::new(MockObjectBackend::new());
        let metrics = Arc::new(MockMetricsBackend::new());

        let hook_recorder = Arc::new(HookRecorder::new());
        let publisher = Arc::new(RecordingPublisher {
            recorder: Arc::clone(&hook_recorder),
        });
        let hook_engine: Arc<dyn HookEngineTrait> = Arc::new(HookEngine::with_config(
            publisher,
            HookEngineConfig {
                dedup_window: Duration::from_millis(50),
                ..Default::default()
            },
        ));

        let mut executor = SquadExecutor::new();
        if self.with_hooks {
            executor = executor.with_hook_engine(Arc::clone(&hook_engine));
        }
        if self.with_object {
            let obj: Arc<dyn cog_core::ObjectBackend> = object.clone();
            executor = executor.with_object_backend(obj);
        }
        if self.with_squad {
            let llm: Arc<dyn cog_core::LlmClient> = llm.clone();
            executor = executor.with_llm_provider(llm);
            executor = executor.with_agent_manager(Arc::new(MockAgentManager));
        }

        TestHarness {
            llm,
            state,
            object,
            metrics,
            hook_engine,
            squad_executor: tokio::sync::Mutex::new(executor),
            hook_recorder,
        }
    }
}

impl Default for TestHarnessBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A [`HookPublisher`] that records every execution.
#[allow(dead_code)]
struct RecordingPublisher {
    recorder: Arc<HookRecorder>,
}

#[async_trait::async_trait]
impl HookPublisher for RecordingPublisher {
    async fn publish_webhook(
        &self,
        _url: &str,
        _headers: &HashMap<String, String>,
        _payload: &serde_json::Value,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn publish_redis_stream(
        &self,
        _channel: &str,
        _payload: &serde_json::Value,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn notify_user(
        &self,
        _user_id: &str,
        _payload: &serde_json::Value,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }
}

#[allow(dead_code)]
impl TestHarness {
    /// Create a new builder.
    pub fn builder() -> TestHarnessBuilder {
        TestHarnessBuilder::new()
    }

    /// Build a default [`SquadConfig`] for the given goal.
    pub fn squad_config_for_goal(goal: &str) -> SquadConfig {
        SquadConfig {
            goal: goal.into(),
            context: serde_json::json!({}),
            pge_mode: PgeMode::Pipeline,
            max_retries: 1,
            profile: None,
            context_window_size: None,
            boundary_config: None,
            execution_mode: false,
            is_self_evolution: false,
            planner_skill_id: None,
            generator_skill_id: None,
            evaluator_skill_id: None,
        }
    }

    /// Execute a squad for the given task_id and config with a timeout.
    pub async fn execute_squad(
        &self,
        task_id: &str,
        config: SquadConfig,
        timeout: Duration,
    ) -> Option<SquadResult> {
        tokio::time::timeout(timeout, async {
            let executor = self.squad_executor.lock().await;
            executor.execute_squad(task_id.to_string(), config).await
        })
        .await
        .ok()
    }

    /// Assert that a hook trigger fired at least `min_count` times.
    #[allow(dead_code)]
    pub fn assert_hook_fired(&self, trigger: HookTrigger, min_count: usize) {
        let count = self.hook_recorder.count_for(trigger);
        assert!(
            count >= min_count,
            "expected hook {:?} to fire at least {} times, but got {}",
            trigger,
            min_count,
            count
        );
    }

    /// Access the mock LLM provider.
    #[allow(dead_code)]
    pub fn mock_llm(&self) -> &MockLLMProvider {
        &self.llm
    }

    /// Access the mock state backend.
    #[allow(dead_code)]
    pub fn mock_state(&self) -> &MockStateBackend {
        &self.state
    }

    /// Access the mock object backend.
    #[allow(dead_code)]
    pub fn mock_object(&self) -> &MockObjectBackend {
        &self.object
    }

    /// Access the mock metrics backend.
    #[allow(dead_code)]
    pub fn mock_metrics(&self) -> &MockMetricsBackend {
        &self.metrics
    }
}
