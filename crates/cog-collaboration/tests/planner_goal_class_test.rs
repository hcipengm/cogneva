//! What the Planner asks the decomposition namespace with.
//!
//! The namespace keys its rows on a goal's class, and the store matches a query
//! as a substring of an entry's key. A query built from anything else — the goal
//! text, or the type of whatever task happens to hold the goal — retrieves
//! nothing and retrieves it the same way a namespace nothing was ever written to
//! does, so the failure is silent from both ends. These tests pin the string the
//! Planner sends, because that string is the only thing tying the read side to
//! the write side.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cog_collaboration::actors::PlannerActor;
use cog_core::{
    Agent, FailurePattern, GoalClass, ImplementationExample, KnowledgeBackend, KnowledgeEntry,
    Task, TaskDecompositionPattern, TaskExecutionRecord, TaskResult, TaskType,
};

/// Records the class every decomposition query was made with.
struct ClassRecordingBackend {
    queried: Mutex<Vec<String>>,
}

impl ClassRecordingBackend {
    fn new() -> Self {
        Self {
            queried: Mutex::new(Vec::new()),
        }
    }

    fn classes(&self) -> Vec<String> {
        self.queried.lock().unwrap().clone()
    }
}

#[async_trait]
impl KnowledgeBackend for ClassRecordingBackend {
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
        goal_class: &str,
        _goal: &str,
        _top_k: usize,
    ) -> cog_core::SFResult<Vec<TaskDecompositionPattern>> {
        self.queried.lock().unwrap().push(goal_class.to_string());
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

    async fn archive_execution(
        &self,
        _task: &Task,
        _result: &TaskResult,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn archive_decomposition(
        &self,
        _task: &Task,
        _sub_task_types: &[String],
    ) -> cog_core::SFResult<()> {
        Ok(())
    }
}

struct MockAgent {
    response: serde_json::Value,
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

    async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
        Ok(cog_core::AgentState::Idle)
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

    async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }
}

fn planner_with(backend: Arc<ClassRecordingBackend>) -> PlannerActor {
    let agent: Arc<dyn Agent> = Arc::new(MockAgent {
        response: serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": [{
                "id": "t1",
                "name": "n",
                "task_type": "generate",
                "input": {"query": "q"},
                "blocked_by": []
            }]
        }),
    });
    PlannerActor::new(agent).with_knowledge(backend)
}

/// The class a goal was submitted under, carried with the goal onto the task the
/// decomposition loop plans it on.
#[tokio::test]
async fn a_goal_carries_its_class_onto_the_task_that_plans_it() {
    let backend = Arc::new(ClassRecordingBackend::new());
    let planner = planner_with(backend.clone());
    let carried = GoalClass::of(&Task::new(
        "outer",
        TaskType::Custom("platform_ci_fix".into()),
        serde_json::json!({"goal": "fix the red build"}),
    ))
    .value;

    // The decomposition loop re-hosts the goal on a task of its own making,
    // whose type names the loop rather than the work.
    let planning_task = Task::new(
        "ralph-plan-1",
        TaskType::Custom("ralph_plan_goal".into()),
        serde_json::json!({
            "goal": "fix the red build",
            (GoalClass::INPUT_FIELD): carried,
        }),
    );

    planner
        .plan(&planning_task, 1, None, None, None, None)
        .await;

    assert_eq!(
        backend.classes(),
        vec![carried],
        "the planner must query with the class the goal carried, not with the type of the host task"
    );
}

/// The control: without a carried class the query falls back to the host's own
/// type. That is a coarser class, not an empty namespace — which is exactly why
/// the fallback has to be visible on its own face rather than inferred from the
/// rows a query returns.
#[tokio::test]
async fn a_goal_that_carries_no_class_is_queried_under_its_host_type() {
    let backend = Arc::new(ClassRecordingBackend::new());
    let planner = planner_with(backend.clone());
    let task = Task::new(
        "t1",
        TaskType::Generator,
        serde_json::json!({"goal": "summarise the builds"}),
    );

    planner.plan(&task, 1, None, None, None, None).await;

    assert_eq!(
        backend.classes(),
        vec![TaskType::Generator.retrieval_class()]
    );
    assert_eq!(
        GoalClass::of(&task).source,
        cog_core::GoalClassSource::HostType
    );
}
