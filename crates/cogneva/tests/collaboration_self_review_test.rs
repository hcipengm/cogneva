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
    /// Actor header of every call, in order, so a test can assert which
    /// component a review's token spend was charged to.
    actors: std::sync::Mutex<Vec<String>>,
    /// The user message of every call, in order. The payload is the only place
    /// the standard a review was held to is visible from outside: the score it
    /// returns is the same number whether it was judged against a specification
    /// or against nothing at all.
    payloads: std::sync::Mutex<Vec<String>>,
}

impl DummyProvider {
    fn new(responses: Vec<String>) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses),
            actors: std::sync::Mutex::new(Vec::new()),
            payloads: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn actors(&self) -> Vec<String> {
        self.actors.lock().unwrap().clone()
    }

    fn payloads(&self) -> Vec<String> {
        self.payloads.lock().unwrap().clone()
    }

    fn note_call(&self, messages: &[Message], options: &ChatOptions) {
        self.actors.lock().unwrap().push(
            options
                .headers
                .get(cog_core::LLM_ACTOR_HEADER)
                .cloned()
                .unwrap_or_else(|| "none".into()),
        );
        let text = messages
            .iter()
            .map(|m| m.content())
            .collect::<Vec<_>>()
            .join("\n");
        self.payloads.lock().unwrap().push(text);
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
    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        self.note_call(messages, options);
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
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.note_call(messages, options);
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
                upstream_failure: None,
                retry_after_secs: None,
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
    let critique = SelfReviewLoop::critique(&obs, "", "self_review", &provider)
        .await
        .unwrap();

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
    let comparison = SelfReviewLoop::compare(&critique, "", &[], "self_review", &provider)
        .await
        .unwrap();

    assert_eq!(comparison.gaps, vec!["gap1"]);
    assert_eq!(comparison.aligned, vec!["aligned1"]);
    assert!((comparison.score - 0.75).abs() < f32::EPSILON);
}

/// The step that produces the score is held to the same standard as the step
/// that finds the defects.
///
/// A specification handed to the critique step and withheld from the comparison
/// step leaves the score resting on the reviewer's own findings: the number is
/// what the pass/fail decision reads, and nothing the caller declared can
/// contradict it. Both payloads are asserted, because the two steps are only
/// distinguishable from the outside by what they were sent.
#[tokio::test]
async fn test_the_comparison_step_is_held_to_the_specification() {
    let provider = DummyProvider::new(vec![
        r#"{"issues":[],"missing":[],"strengths":[]}"#.into(),
        r#"{"gaps":[],"aligned":[],"score":1.0}"#.into(),
    ]);
    let config = SelfReviewConfig {
        spec: Some("acceptance criteria: no rollback step, no test".into()),
        ..SelfReviewConfig::default()
    };

    let _ = SelfReviewLoop::new(config).review("draft", &provider).await;

    let payloads = provider.payloads();
    assert_eq!(payloads.len(), 2, "critique and compare, one call each");
    assert!(
        payloads[0].contains("no rollback step"),
        "the critique step is held to the specification: {}",
        payloads[0]
    );
    assert!(
        payloads[1].contains("no rollback step"),
        "and so is the step that scores against it: {}",
        payloads[1]
    );
}

/// Self-review runs on behalf of every agent, so its spend has to be charged
/// to the agent under review. A single constant label would merge all of them
/// into one bucket and leave nothing to hold a per-agent budget against.
#[tokio::test]
async fn test_review_charges_its_token_spend_to_the_calling_agent() {
    let provider = DummyProvider::new(vec![
        r#"{"issues":[],"missing":[],"strengths":[]}"#.into(),
        r#"{"gaps":[],"aligned":[],"score":1.0}"#.into(),
    ]);
    let loop_ = SelfReviewLoop::new(SelfReviewConfig::default())
        .with_actor(cog_agent::self_review::self_review_actor("generator"));

    let _ = loop_.review("draft", &provider).await.unwrap();

    assert_eq!(
        provider.actors(),
        vec![
            "self_review:generator".to_string(),
            "self_review:generator".to_string()
        ],
        "critique and compare must both carry the reviewed agent's identity"
    );
}

/// A caller that does not identify itself still gets a bounded label rather
/// than an empty header.
#[tokio::test]
async fn test_review_without_a_caller_names_the_component_alone() {
    let provider = DummyProvider::new(vec![
        r#"{"issues":[],"missing":[],"strengths":[]}"#.into(),
        r#"{"gaps":[],"aligned":[],"score":1.0}"#.into(),
    ]);
    let loop_ = SelfReviewLoop::new(SelfReviewConfig::default());

    let _ = loop_.review("draft", &provider).await.unwrap();

    assert_eq!(
        provider.actors(),
        vec!["self_review".to_string(), "self_review".to_string()]
    );
}

/// A revision that rewrote nothing ends the review instead of buying another
/// round of the identical question.
///
/// The loop is driven to `NeedRevision` and the revision step hands back no
/// text, which the revise step reads as "no revision" and answers with the
/// original. The next round would build its critique and comparison from that
/// same text, the same specification and the same best practices, so it asks a
/// question already answered — and an unchanged text cannot be improved by
/// asking twice. The call count is the assertion: critique, comparison and
/// revision, and nothing after them.
#[tokio::test]
async fn test_a_revision_that_rewrote_nothing_ends_the_review() {
    let provider = DummyProvider::new(vec![
        r#"{"issues":["no test covers the change"],"missing":[],"strengths":[]}"#.into(),
        r#"{"gaps":["no test"],"aligned":[],"score":0.4}"#.into(),
        // Empty text: the revision step treats it as "nothing was revised" and
        // answers with the original output.
        "".into(),
    ]);
    let config = SelfReviewConfig {
        max_iterations: 3,
        ..SelfReviewConfig::default()
    };

    let (text, result) = SelfReviewLoop::new(config)
        .review("draft", &provider)
        .await
        .unwrap();

    assert_eq!(text, "draft", "no revision is the original text");
    assert!(
        matches!(result, SelfReviewResult::NeedRevision { .. }),
        "the judgement already reached is what the caller is handed"
    );
    assert_eq!(
        provider.payloads().len(),
        3,
        "critique, comparison, revision — the second round must not be paid for"
    );
}

/// A revision that rewrote the text keeps the loop going: the next round has a
/// different text to judge, so it is the review working rather than repeating
/// itself.
#[tokio::test]
async fn test_a_revision_that_rewrote_the_text_continues_the_review() {
    let provider = DummyProvider::new(vec![
        r#"{"issues":["no test covers the change"],"missing":[],"strengths":[]}"#.into(),
        r#"{"gaps":["no test"],"aligned":[],"score":0.4}"#.into(),
        "draft with a test".into(),
        r#"{"issues":[],"missing":[],"strengths":[]}"#.into(),
        r#"{"gaps":[],"aligned":[],"score":0.95}"#.into(),
    ]);
    let config = SelfReviewConfig {
        max_iterations: 3,
        ..SelfReviewConfig::default()
    };

    let (text, result) = SelfReviewLoop::new(config)
        .review("draft", &provider)
        .await
        .unwrap();

    assert_eq!(text, "draft with a test");
    assert!(matches!(result, SelfReviewResult::Pass { .. }));
    assert_eq!(
        provider.payloads().len(),
        5,
        "the rewritten text is judged again, and that round passes"
    );
}

/// A critique with the given findings and nothing else, for the decide tests.
fn critique_of(issues: &[&str], missing: &[&str]) -> Critique {
    Critique {
        issues: issues.iter().map(|s| s.to_string()).collect(),
        missing: missing.iter().map(|s| s.to_string()).collect(),
        strengths: Vec::new(),
        raw: "raw".into(),
    }
}

#[tokio::test]
async fn test_decide_pass_when_above_threshold() {
    let comparison = Comparison {
        gaps: vec![],
        aligned: vec!["good".into()],
        score: 0.85,
        raw: "raw".into(),
    };
    let result = SelfReviewLoop::decide(&critique_of(&[], &[]), &comparison, 0.8);

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
    let result = SelfReviewLoop::decide(&critique_of(&[], &[]), &comparison, 0.8);

    assert!(
        matches!(result, SelfReviewResult::NeedRevision { critique, score, .. } if critique == "missing docs" && (score - 0.5).abs() < f32::EPSILON),
        "a critique that found nothing leaves the gap restatement as the instruction"
    );
}

/// The revision is told what the critique step found, not a coarser restatement
/// of it: the issues and missing items name the defect in more detail than the
/// gaps derived from them, and a revision that is told less than the call before
/// it is a revision that misses on the first round and buys another one.
#[tokio::test]
async fn a_revision_is_told_the_findings_the_critique_step_named() {
    let comparison = Comparison {
        gaps: vec!["the plan is thin".into()],
        aligned: vec![],
        score: 0.4,
        raw: "raw".into(),
    };
    let result = SelfReviewLoop::decide(
        &critique_of(&["no rollback step"], &["no test"]),
        &comparison,
        0.8,
    );

    match result {
        SelfReviewResult::NeedRevision {
            critique,
            suggestions,
            ..
        } => {
            assert_eq!(critique, "no rollback step; no test");
            assert_eq!(suggestions, vec!["the plan is thin".to_string()]);
        }
        other => panic!("a score below the threshold asks for a revision, got {other:?}"),
    }
}

#[tokio::test]
async fn test_revise_returns_revised_text() {
    let provider = DummyProvider::new(vec!["revised output".into()]);

    let result = SelfReviewResult::NeedRevision {
        critique: "bad".into(),
        suggestions: vec!["fix it".into()],
        score: 0.3,
    };
    let revised = SelfReviewLoop::revise(&result, "original", "self_review", &provider)
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
    let revised = SelfReviewLoop::revise(&result, "original", "self_review", &provider)
        .await
        .unwrap();

    assert_eq!(revised, "original");
}

/// A provider standing in for a backend that answered without usable content:
/// either it never reached a model, in which case the reason rides in
/// `error_message`, or it finished having produced nothing at all.
struct SilentProvider {
    error_message: Option<String>,
}

#[async_trait]
impl cog_core::LlmClient for SilentProvider {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        Ok(ChatResponse {
            content: Vec::new(),
            api: "silent".into(),
            provider: "silent".into(),
            model: "silent".into(),
            response_id: None,
            usage: Usage::default(),
            stop_reason: if self.error_message.is_some() {
                StopReason::Error
            } else {
                StopReason::Stop
            },
            error_message: self.error_message.clone(),
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, _) =
            AssistantMessageEventStream::with_capacity(cog_core::DEFAULT_STREAM_CAPACITY);
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, _) =
            AssistantMessageEventStream::with_capacity(cog_core::DEFAULT_STREAM_CAPACITY);
        Ok(stream)
    }

    async fn health_check(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn test_revise_keeps_the_original_when_the_upstream_never_answered() {
    let provider = SilentProvider {
        error_message: Some(r#"API error: {"error":"所有 LLM 上游当前不可用"}"#.into()),
    };
    let result = SelfReviewResult::NeedRevision {
        critique: "bad".into(),
        suggestions: vec!["fix it".into()],
        score: 0.3,
    };

    let err = SelfReviewLoop::revise(&result, "original", "self_review", &provider)
        .await
        .expect_err("a backend that never answered has no revision to give");

    assert!(
        err.to_string().contains("所有 LLM 上游当前不可用"),
        "the provider's reason must survive: {err}"
    );
}

#[tokio::test]
async fn test_revise_keeps_the_original_when_nothing_was_produced() {
    let provider = SilentProvider {
        error_message: None,
    };
    let result = SelfReviewResult::NeedRevision {
        critique: "bad".into(),
        suggestions: vec!["fix it".into()],
        score: 0.3,
    };

    let revised = SelfReviewLoop::revise(&result, "original", "self_review", &provider)
        .await
        .unwrap();

    assert_eq!(
        revised, "original",
        "no revision is not an empty revision: applying it would erase the deliverable"
    );
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
        think_stall_timeout_secs: 240,
    };

    let mut agent_loop = AgentRuntime::new(config, event_tx);

    let result = agent_loop
        .run(serde_json::json!({"test": true}), &provider)
        .await;

    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val.get("status"), Some(&serde_json::json!("ok")));
}
