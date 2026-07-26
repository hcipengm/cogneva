use async_trait::async_trait;
use cog_collaboration::actors::{EvaluatorActor, GeneratorActor, PlannerActor};
use cog_collaboration::{
    parse_evaluation_result, parse_generator_output, parse_planner_output, PgeRoundtable,
    PgeRoundtableConfig, Verdict,
};
use std::sync::Arc;

fn test_task(goal: &str) -> cog_core::Task {
    cog_core::Task::new(
        format!("test-{}", uuid::Uuid::new_v4()),
        cog_core::TaskType::Custom("test".into()),
        serde_json::json!({"goal": goal}),
    )
}

/// Test-only mock implementing the object-level [`cog_core::Agent`] trait.
struct MockAgent {
    response: serde_json::Value,
}

#[async_trait]
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

fn pass_planner() -> MockAgent {
    MockAgent {
        response: serde_json::json!({
            "summary": "test analysis",
            "plan": {"specification": "test spec", "design": "test design"},
            "sub_tasks": [{"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}],
        }),
    }
}

fn pass_generator() -> MockAgent {
    MockAgent {
        response: serde_json::json!({
            "content": {"code": "fn main() {}", "tests": "", "documentation": ""},
            "artifacts": [],
        }),
    }
}

fn pass_evaluator() -> MockAgent {
    MockAgent {
        response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
    }
}

#[test]
fn roundtable_config_with_skill_ids() {
    let config = PgeRoundtableConfig {
        max_iterations: 3,
        consensus_threshold: 0.5,
        skill_ids: vec!["custom-planner".into(), "custom-generator".into()],
        ..Default::default()
    };
    assert_eq!(config.skill_ids.len(), 2);
    assert_eq!(config.max_iterations, 3);
}

#[tokio::test]
async fn roundtable_debate_runs_with_external_agents() {
    let planner = PlannerActor::new(Arc::new(pass_planner()));
    let generator = GeneratorActor::new(Arc::new(pass_generator()));
    let evaluator = EvaluatorActor::new(Arc::new(pass_evaluator()));
    let roundtable = PgeRoundtable::new(
        PgeRoundtableConfig::default(),
        planner,
        generator,
        evaluator,
    );
    let result = roundtable
        .debate(&test_task("test goal"), serde_json::json!({}))
        .await;
    assert!(!result.history.is_empty());
}

#[test]
fn parse_planner_output_from_json() {
    let json = serde_json::json!({
        "summary": "Test analysis",
        "plan": {"specification": "Test spec", "design": "Test design"},
        "sub_tasks": [
            {"id": "t1", "name": "Task 1", "task_type": "plan", "input": {}, "blocked_by": []}
        ]
    });
    let output = parse_planner_output(&json, "goal");
    assert_eq!(output.summary, "Test analysis");
    assert_eq!(output.sub_tasks.len(), 1);
    assert_eq!(output.sub_tasks[0].id, "t1");
}

#[test]
fn parse_generator_output_from_json() {
    let json = serde_json::json!({
        "content": {"code": "fn main() {}", "tests": "test_main", "documentation": "Docs"},
        "artifacts": [{"name": "main.rs", "content": "fn main() {}", "artifact_type": "code"}]
    });
    let output = parse_generator_output(&json);
    assert!(output.content.get("code").is_some());
    assert_eq!(output.artifacts.len(), 1);
}

#[test]
fn parse_evaluation_result_from_json() {
    let json = serde_json::json!({
        "score": 85,
        "passed": true,
        "feedback": "Good work",
        "criteria": [{"name": "quality", "score": 85, "comment": "Nice"}]
    });
    let result = parse_evaluation_result(&json);
    assert_eq!(result.score, Some(85));
    assert!(matches!(result.verdict, Verdict::Pass));
    assert_eq!(result.criteria.len(), 1);
}
