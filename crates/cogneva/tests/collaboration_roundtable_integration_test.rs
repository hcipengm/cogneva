use cog_collaboration::actors::{EvaluatorActor, GeneratorActor, PlannerActor};
use cog_collaboration::{PgeRoundtable, PgeRoundtableConfig};
use cog_core::Task;
use std::sync::Arc;

fn test_task(goal: &str) -> Task {
    Task::new(
        format!("test-{}", uuid::Uuid::new_v4()),
        cog_core::TaskType::Custom("test".into()),
        serde_json::json!({"goal": goal}),
    )
}

/// Test-only mock implementing the object-level [`cog_core::Agent`] trait.
struct MockAgent {
    response: serde_json::Value,
}

#[async_trait::async_trait]
impl cog_core::Agent for MockAgent {
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

fn planner_with_mock() -> MockAgent {
    MockAgent {
        response: serde_json::json!({
            "summary": "test analysis",
            "plan": {"specification": "spec", "design": "design"},
            "sub_tasks": [{"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}]
        }),
    }
}

fn generator_with_mock() -> MockAgent {
    MockAgent {
        response: serde_json::json!({
            "content": {"code": "fn main() {}", "tests": "", "documentation": ""},
            "artifacts": []
        }),
    }
}

fn evaluator_with_mock() -> MockAgent {
    MockAgent {
        response: serde_json::json!({
            "verdict": "pass",
            "score": 85,
            "feedback": "good work",
            "criteria": []
        }),
    }
}

fn make_roundtable(config: PgeRoundtableConfig) -> PgeRoundtable {
    PgeRoundtable::new(
        config,
        PlannerActor::new(Arc::new(planner_with_mock())),
        GeneratorActor::new(Arc::new(generator_with_mock())),
        EvaluatorActor::new(Arc::new(evaluator_with_mock())),
    )
}

#[tokio::test]
async fn test_roundtable_debate_reaches_consensus() {
    let config = PgeRoundtableConfig {
        max_iterations: 3,
        consensus_threshold: 0.5,
        ..Default::default()
    };
    let roundtable = make_roundtable(config);

    let result = roundtable
        .debate(
            &test_task("implement a hello world function"),
            serde_json::json!({}),
        )
        .await;

    assert!(
        result.iterations > 0,
        "Should complete at least one iteration"
    );
    assert!(
        !result.final_plan.sub_tasks.is_empty(),
        "Should produce a plan with tasks"
    );
    assert!(
        result.final_evaluation.score.unwrap_or(0) > 0,
        "Should produce a non-zero score"
    );
}

#[tokio::test]
async fn test_roundtable_debate_with_low_threshold_passes() {
    let config = PgeRoundtableConfig {
        max_iterations: 5,
        consensus_threshold: 0.1,
        ..Default::default()
    };
    let roundtable = make_roundtable(config);

    let result = roundtable
        .debate(&test_task("review code quality"), serde_json::json!({}))
        .await;

    assert!(
        result.iterations >= 1,
        "Should complete at least one iteration"
    );
    assert!(
        result.consensus_reached,
        "Should reach consensus with low threshold"
    );
}

#[tokio::test]
async fn test_roundtable_history_recorded() {
    let config = PgeRoundtableConfig {
        max_iterations: 2,
        consensus_threshold: 1.0,
        ..Default::default()
    };
    let roundtable = make_roundtable(config);

    let result = roundtable
        .debate(&test_task("generate tests"), serde_json::json!({}))
        .await;

    assert_eq!(
        result.history.len() as u32,
        result.iterations,
        "History should match iteration count"
    );

    for (i, iter) in result.history.iter().enumerate() {
        assert_eq!(iter.iteration as usize, i + 1);
        assert!(!iter.plan.sub_tasks.is_empty());
    }
}

#[tokio::test]
async fn test_roundtable_evaluator_criteria_present() {
    let config = PgeRoundtableConfig {
        max_iterations: 1,
        consensus_threshold: 1.0,
        ..Default::default()
    };
    let roundtable = make_roundtable(config);

    let result = roundtable
        .debate(&test_task("any goal"), serde_json::json!({}))
        .await;

    assert!(
        !result.final_evaluation.feedback.is_empty(),
        "Evaluator should produce feedback"
    );
}

#[tokio::test]
async fn test_roundtable_context_board_populated() {
    let config = PgeRoundtableConfig {
        max_iterations: 2,
        consensus_threshold: 1.0,
        context_board: Some(serde_json::json!({"topic": "testing"})),
        ..Default::default()
    };
    let roundtable = make_roundtable(config);

    let result = roundtable
        .debate(&test_task("generate tests"), serde_json::json!({}))
        .await;

    let board = result
        .context_board
        .expect("Context board should be present");

    assert!(
        board.get("latest_plan").is_some(),
        "Board should contain planner output"
    );
    assert!(
        board.get("latest_generation").is_some(),
        "Board should contain generator output"
    );
    assert!(
        board.get("latest_evaluation").is_some(),
        "Board should contain evaluator output"
    );
    assert!(
        board.get("round").is_some(),
        "Board should contain round counter"
    );

    assert_eq!(board.get("topic").and_then(|v| v.as_str()), Some("testing"));
}

#[tokio::test]
async fn test_roundtable_evaluator_veto_blocks_consensus() {
    let config = PgeRoundtableConfig {
        max_iterations: 3,
        consensus_threshold: 0.8,
        ..Default::default()
    };
    let roundtable = make_roundtable(config);

    let result = roundtable
        .debate(&test_task("any goal"), serde_json::json!({}))
        .await;

    assert!(
        result.consensus_reached,
        "Stub evaluator (passed=true, score=85) should allow consensus"
    );
}

#[tokio::test]
async fn test_roundtable_without_context_board_still_works() {
    let config = PgeRoundtableConfig {
        max_iterations: 2,
        consensus_threshold: 1.0,
        ..Default::default()
    };
    let roundtable = make_roundtable(config);

    let result = roundtable
        .debate(&test_task("simple task"), serde_json::json!({}))
        .await;

    assert!(
        result.context_board.is_some(),
        "Empty board should still be created"
    );
    assert!(
        result.iterations > 0,
        "Should complete iterations without board"
    );
}
