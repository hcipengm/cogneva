//! End-to-end tests for self-review output revision and execution archiving.
//!
//! Covers:
//! 1. `SelfReviewConfig` enabled → Generator/Planner output is replaced by the
//!    revised text returned from the self-review loop.
//! 2. `CollaborationExecutor` archives successful executions into the
//!    `KnowledgeBackend` in the background.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cog_collaboration::actors::{GeneratorActor, PlannerActor};
use cog_collaboration::CollaborationExecutor;
use cog_core::{
    Agent, AgentManager, AgentState, FailurePattern, ImplementationExample, InboxMessage,
    KnowledgeBackend, KnowledgeEntry, LlmClient, SelfReviewConfig, SelfReviewResult, Task,
    TaskDecompositionPattern, TaskExecutionRecord, TaskResult, WorkerInfo,
};

// ---------------------------------------------------------------------------
// MockAgent
// ---------------------------------------------------------------------------

/// Mock agent returning a canned `prompt` response and, when configured, a
/// revised output from `review_and_revise`.
struct MockAgent {
    response: serde_json::Value,
    revised: Option<String>,
    review_calls: Arc<Mutex<u32>>,
}

impl MockAgent {
    fn new(response: serde_json::Value) -> Self {
        Self {
            response,
            revised: None,
            review_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn with_revision(response: serde_json::Value, revised: String) -> Self {
        Self {
            response,
            revised: Some(revised),
            review_calls: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Agent for MockAgent {
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

    async fn state(&self) -> cog_core::SFResult<AgentState> {
        Ok(AgentState::Idle)
    }

    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
        Ok(())
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

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }

    async fn receive_message(&self, _msg: InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn review_and_revise(
        &self,
        output: &str,
        _config: &SelfReviewConfig,
    ) -> cog_core::SFResult<(String, SelfReviewResult)> {
        *self.review_calls.lock().unwrap() += 1;
        let text = self.revised.clone().unwrap_or_else(|| output.to_string());
        Ok((
            text,
            SelfReviewResult::Pass {
                score: 0.9,
                summary: "mock review".into(),
            },
        ))
    }
}

// ---------------------------------------------------------------------------
// MockAgentManager / MockLlm / MockKnowledgeBackend
// ---------------------------------------------------------------------------

/// Creates role-based mock agents so a full squad can run end-to-end.
struct MockAgentManager;

#[async_trait]
impl AgentManager for MockAgentManager {
    async fn create_agent(
        &self,
        _agent_id: &str,
        role: &str,
        _llm: Arc<dyn LlmClient>,
    ) -> cog_core::SFResult<Arc<dyn Agent>> {
        let response = match role {
            "generator" => serde_json::json!({
                "content": {"code": "fn main() { println!(\"hello\"); }"},
                "artifacts": [],
            }),
            "evaluator" => serde_json::json!({
                "verdict": "pass",
                "score": 92,
                "feedback": "looks good",
                "criteria": [],
            }),
            // planner / moderator / mode_selector / anything else
            _ => serde_json::json!({
                "summary": "mock plan",
                "plan": {"approach": "mock"},
                "sub_tasks": [
                    {"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}
                ],
            }),
        };
        Ok(Arc::new(MockAgent::new(response)))
    }

    async fn dispatch(&self, _msg: InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn list_workers(&self) -> cog_core::SFResult<Vec<WorkerInfo>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn get_agent(&self, _agent_id: &str) -> cog_core::SFResult<Option<Arc<dyn Agent>>> {
        Ok(None)
    }
}

struct MockLlm;

#[async_trait]
impl LlmClient for MockLlm {
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

    async fn chat(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::ChatResponse> {
        Ok(cog_core::ChatResponse::default())
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// Records `archive_execution` calls.
struct MockKnowledgeBackend {
    archived: Mutex<Vec<String>>,
}

impl MockKnowledgeBackend {
    fn new() -> Self {
        Self {
            archived: Mutex::new(Vec::new()),
        }
    }

    fn archived_count(&self) -> usize {
        self.archived.lock().unwrap().len()
    }
}

#[async_trait]
impl KnowledgeBackend for MockKnowledgeBackend {
    async fn retrieve_relevant(
        &self,
        _task: &Task,
        _query: &str,
        _top_k: usize,
    ) -> cog_core::SFResult<Vec<KnowledgeEntry>> {
        Ok(Vec::new())
    }

    async fn retrieve_similar_decompositions(
        &self,
        _goal: &str,
        _top_k: usize,
    ) -> cog_core::SFResult<Vec<TaskDecompositionPattern>> {
        Ok(Vec::new())
    }

    async fn retrieve_similar_implementations(
        &self,
        _task_type: &str,
        _input_summary: &str,
        _top_k: usize,
    ) -> cog_core::SFResult<Vec<ImplementationExample>> {
        Ok(Vec::new())
    }

    async fn retrieve_failure_patterns(
        &self,
        _task_type: &str,
        _top_k: usize,
    ) -> cog_core::SFResult<Vec<FailurePattern>> {
        Ok(Vec::new())
    }

    async fn retrieve_task_history(
        &self,
        _task_id: &str,
    ) -> cog_core::SFResult<Vec<TaskExecutionRecord>> {
        Ok(Vec::new())
    }

    async fn archive_execution(&self, task: &Task, result: &TaskResult) -> cog_core::SFResult<()> {
        assert!(result.success, "only successful results should be archived");
        self.archived.lock().unwrap().push(task.id.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_task(goal: &str) -> Task {
    Task::new(
        format!("test-{}", uuid::Uuid::new_v4()),
        cog_core::TaskType::Custom("test".into()),
        serde_json::json!({"goal": goal}),
    )
}

fn self_review_config() -> SelfReviewConfig {
    SelfReviewConfig {
        max_iterations: 1,
        quality_threshold: 0.8,
        spec: None,
        best_practices: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn generator_applies_self_review_revision() {
    let original = serde_json::json!({
        "content": {"code": "fn main() {}"},
        "artifacts": [],
    });
    let revised = serde_json::json!({
        "content": {"code": "fn main() { println!(\"revised\"); }"},
        "artifacts": [],
    })
    .to_string();

    let agent = MockAgent::with_revision(original, revised);
    let review_calls = agent.review_calls.clone();
    let actor = GeneratorActor::new(Arc::new(agent)).with_self_review(self_review_config());

    let task = test_task("write a simple main function");
    let output = actor
        .generate(
            &task,
            &serde_json::json!({}),
            0,
            cog_collaboration::actors::PreviousAttempt::default(),
            None,
        )
        .await;

    assert_eq!(
        *review_calls.lock().unwrap(),
        1,
        "self-review should run once"
    );
    assert_eq!(
        output.content["code"], "fn main() { println!(\"revised\"); }",
        "generator output should be replaced by the revised text"
    );
}

#[tokio::test]
async fn planner_applies_self_review_revision() {
    let original = serde_json::json!({
        "summary": "original plan",
        "plan": {},
        "sub_tasks": [],
    });
    let revised = serde_json::json!({
        "summary": "revised plan",
        "plan": {"approach": "better"},
        "sub_tasks": [
            {"id": "t1", "name": "Revised Task", "task_type": "generate", "input": {}, "blocked_by": []}
        ],
    })
    .to_string();

    let agent = MockAgent::with_revision(original, revised);
    let actor = PlannerActor::new(Arc::new(agent)).with_self_review(self_review_config());

    let task = test_task("plan a simple hello world");
    let output = actor.plan(&task, 0, None, None, None, None).await;

    assert_eq!(output.summary, "revised plan");
    assert_eq!(
        output.sub_tasks.len(),
        1,
        "revised sub_tasks should be parsed"
    );
}

#[tokio::test]
async fn no_self_review_config_skips_review() {
    let agent = MockAgent::with_revision(
        serde_json::json!({"content": {"code": "x"}, "artifacts": []}),
        serde_json::json!({"content": {"code": "y"}, "artifacts": []}).to_string(),
    );
    let review_calls = agent.review_calls.clone();
    // No with_self_review — default behavior must be unchanged.
    let actor = GeneratorActor::new(Arc::new(agent));

    let task = test_task("write a simple main function");
    let output = actor
        .generate(
            &task,
            &serde_json::json!({}),
            0,
            cog_collaboration::actors::PreviousAttempt::default(),
            None,
        )
        .await;

    assert_eq!(
        *review_calls.lock().unwrap(),
        0,
        "review must not run without config"
    );
    assert_eq!(output.content["code"], "x");
}

#[tokio::test]
async fn collaboration_executor_archives_successful_execution() {
    let knowledge = Arc::new(MockKnowledgeBackend::new());
    let executor = CollaborationExecutor::new()
        .with_llm_provider(Arc::new(MockLlm))
        .with_agent_manager(Arc::new(MockAgentManager))
        .with_knowledge_backend(knowledge.clone());

    let mut task = test_task("implement a simple hello world function");
    task.is_executable = false;

    let result = cog_core::TaskExecutor::execute(&executor, &task)
        .await
        .expect("decomposition should succeed with stub agents");
    assert!(result.success);

    // archive_execution runs in a spawned background task — poll briefly.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while knowledge.archived_count() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "archive_execution was not called"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let archived = knowledge.archived.lock().unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0], task.id);
}
