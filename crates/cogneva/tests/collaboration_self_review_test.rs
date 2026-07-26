use async_trait::async_trait;
use cog_agent::self_review::{Comparison, Critique};
use cog_agent::AgentRuntime;
use cog_agent::SelfReviewLoop;
use cog_core::RuntimeConfig;
use cog_core::{
    AssistantMessageEvent, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions,
    ContentBlock, Message, SFResult, StopReason, Usage,
};
use cog_core::{SelfReviewConfig, SelfReviewResult};
/// DummyProvider that returns configurable JSON/text responses for self-review tests.
struct DummyProvider {
    /// Responses returned in order for each `chat` call.
    responses: std::sync::Mutex<Vec<String>>,
}

impl DummyProvider {
    fn new(responses: Vec<String>) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses),
        }
    }

    fn pop_response(&self) -> String {
        let mut guard = self.responses.lock().unwrap();
        if guard.is_empty() {
            return "{\"issues\":[],\"missing\":[],\"strengths\":[],\"score\":1.0,\"gaps\":[],\"aligned\":[]}".into();
        }
        guard.remove(0)
    }
}

#[async_trait]
impl cog_core::LlmClient for DummyProvider {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        let text = self.pop_response();
        Ok(ChatResponse {
            content: vec![ContentBlock::text(text)],
            api: "dummy".into(),
            provider: "dummy".into(),
            model: "dummy".into(),
            response_id: None,
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_core::DEFAULT_STREAM_CAPACITY);
        let text = self.pop_response();
        tokio::spawn(async move {
            let response = ChatResponse {
                content: vec![ContentBlock::text(text.clone())],
                api: "dummy".into(),
                provider: "dummy".into(),
                model: "dummy".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };

            let _ = producer
                .push(AssistantMessageEvent::Done {
                    reason: StopReason::Stop,
                    message: Message::assistant_text(text),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(response);
        });
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, _) = AssistantMessageEventStream::with_capacity(1);
        Ok(stream)
    }

    async fn health_check(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// SelfReviewLoop unit tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_observe_wraps_output() {
    let obs = SelfReviewLoop::observe("hello world");
    assert_eq!(obs.output, "hello world");
}

#[tokio::test]
async fn test_critique_parses_llm_json() {
    let provider = DummyProvider::new(vec![
        r#"{"issues":["bug1"],"missing":["test"],"strengths":["clean"]}"#.into(),
    ]);

    let _loop = SelfReviewLoop::new(SelfReviewConfig::default());
    let obs = SelfReviewLoop::observe("some output");
    let critique = SelfReviewLoop::critique(&obs, "", &provider).await.unwrap();

    assert_eq!(critique.issues, vec!["bug1"]);
    assert_eq!(critique.missing, vec!["test"]);
    assert_eq!(critique.strengths, vec!["clean"]);
}

#[tokio::test]
async fn test_compare_parses_llm_json() {
    let provider = DummyProvider::new(vec![
        r#"{"gaps":["gap1"],"aligned":["aligned1"],"score":0.75}"#.into(),
    ]);

    let _loop = SelfReviewLoop::new(SelfReviewConfig::default());
    let critique = Critique {
        issues: vec!["issue1".into()],
        missing: vec![],
        strengths: vec!["strength1".into()],
        raw: "raw".into(),
    };
    let comparison = SelfReviewLoop::compare(&critique, &[], &provider)
        .await
        .unwrap();

    assert_eq!(comparison.gaps, vec!["gap1"]);
    assert_eq!(comparison.aligned, vec!["aligned1"]);
    assert!((comparison.score - 0.75).abs() < f32::EPSILON);
}

#[tokio::test]
async fn test_decide_pass_when_above_threshold() {
    let comparison = Comparison {
        gaps: vec![],
        aligned: vec!["good".into()],
        score: 0.85,
        raw: "raw".into(),
    };
    let result = SelfReviewLoop::decide(&comparison, 0.8);

    assert!(
        matches!(result, SelfReviewResult::Pass { score, .. } if (score - 0.85).abs() < f32::EPSILON)
    );
}

#[tokio::test]
async fn test_decide_need_revision_when_below_threshold() {
    let comparison = Comparison {
        gaps: vec!["missing docs".into()],
        aligned: vec![],
        score: 0.5,
        raw: "raw".into(),
    };
    let result = SelfReviewLoop::decide(&comparison, 0.8);

    assert!(
        matches!(result, SelfReviewResult::NeedRevision { critique, score, .. } if critique == "missing docs" && (score - 0.5).abs() < f32::EPSILON)
    );
}

#[tokio::test]
async fn test_revise_returns_revised_text() {
    let provider = DummyProvider::new(vec!["revised output".into()]);

    let result = SelfReviewResult::NeedRevision {
        critique: "bad".into(),
        suggestions: vec!["fix it".into()],
        score: 0.3,
    };
    let revised = SelfReviewLoop::revise(&result, "original", &provider)
        .await
        .unwrap();

    assert_eq!(revised, "revised output");
}

#[tokio::test]
async fn test_revise_returns_original_on_pass() {
    let provider = DummyProvider::new(vec![]);

    let result = SelfReviewResult::Pass {
        score: 1.0,
        summary: "great".into(),
    };
    let revised = SelfReviewLoop::revise(&result, "original", &provider)
        .await
        .unwrap();

    assert_eq!(revised, "original");
}

#[tokio::test]
async fn test_full_review_passes_immediately() {
    let provider = DummyProvider::new(vec![
        // critique
        r#"{"issues":[],"missing":[],"strengths":["perfect"]}"#.into(),
        // comparison
        r#"{"gaps":[],"aligned":["perfect"],"score":0.95}"#.into(),
    ]);

    let config = SelfReviewConfig {
        max_iterations: 2,
        quality_threshold: 0.8,
        spec: None,
        best_practices: vec![],
    };
    let loop_ = SelfReviewLoop::new(config);
    let (output, result) = loop_.review("my output", &provider).await.unwrap();

    assert_eq!(output, "my output");
    assert!(
        matches!(result, SelfReviewResult::Pass { score, .. } if (score - 0.95).abs() < f32::EPSILON)
    );
}

#[tokio::test]
async fn test_full_review_revises_once_then_passes() {
    let provider = DummyProvider::new(vec![
        // critique (first cycle)
        r#"{"issues":["typo"],"missing":[],"strengths":["good"]}"#.into(),
        // comparison (first cycle — below threshold)
        r#"{"gaps":["typo"],"aligned":["good"],"score":0.5}"#.into(),
        // revise
        "my output fixed".into(),
        // critique (second cycle)
        r#"{"issues":[],"missing":[],"strengths":["fixed"]}"#.into(),
        // comparison (second cycle — above threshold)
        r#"{"gaps":[],"aligned":["fixed"],"score":0.95}"#.into(),
    ]);

    let config = SelfReviewConfig {
        max_iterations: 3,
        quality_threshold: 0.8,
        spec: None,
        best_practices: vec![],
    };
    let loop_ = SelfReviewLoop::new(config);
    let (output, result) = loop_.review("my output", &provider).await.unwrap();

    assert_eq!(output, "my output fixed");
    assert!(
        matches!(result, SelfReviewResult::Pass { score, .. } if (score - 0.95).abs() < f32::EPSILON)
    );
}

#[tokio::test]
async fn test_full_review_max_iterations_exceeded() {
    let provider = DummyProvider::new(vec![
        // critique (cycle 1)
        r#"{"issues":["bug"],"missing":[],"strengths":[]}"#.into(),
        // comparison (cycle 1 — below threshold)
        r#"{"gaps":["bug"],"aligned":[],"score":0.3}"#.into(),
        // revise
        "revision 1".into(),
        // critique (cycle 2)
        r#"{"issues":["bug2"],"missing":[],"strengths":[]}"#.into(),
        // comparison (cycle 2 — below threshold)
        r#"{"gaps":["bug2"],"aligned":[],"score":0.3}"#.into(),
        // revise
        "revision 2".into(),
    ]);

    let config = SelfReviewConfig {
        max_iterations: 2,
        quality_threshold: 0.8,
        spec: None,
        best_practices: vec![],
    };
    let loop_ = SelfReviewLoop::new(config);
    let (output, result) = loop_.review("my output", &provider).await.unwrap();

    assert_eq!(output, "revision 2");
    assert!(
        matches!(result, SelfReviewResult::NeedRevision { score, .. } if (score - 0.3).abs() < f32::EPSILON)
    );
}

// ---------------------------------------------------------------------------
// AgentRuntime integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_agent_loop_runs_correctly() {
    let provider = DummyProvider::new(vec![
        // AgentRuntime think_stream → Done with "hello"
        "hello".into(),
        // build_result chat → JSON result
        r#"{"status":"ok"}"#.into(),
    ]);

    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(16);
    let config = RuntimeConfig {
        agent_id: "test-agent".into(),
        role: "planner".to_string(),
        max_iterations: 1,
        context_window_size: 4000,
        skill_config: None,
        crew_id: None,
        squad_id: None,
        skill_cache_ttl_secs: 30,
    };

    let mut agent_loop = AgentRuntime::new(config, event_tx);

    let result = agent_loop
        .run(serde_json::json!({"test": true}), &provider)
        .await;

    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val.get("status"), Some(&serde_json::json!("ok")));
}
