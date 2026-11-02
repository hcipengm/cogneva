//! 自检的花销记在哪件活上。
//!
//! 账本按两格取行：`actor` 答「哪个角色在花」，`task_id` 答「哪件活花的」。
//! 自检这条路上两人都是它自己——它既不是另一个角色，也不是另一件活，所以两格
//! 都得从调用方带进来：只带 `actor` 时，2026-11-02 实测 354 次成功自检调用在
//! 按任务读成本的那一面全是空 id，一件变更的「成本」里不含审它花掉的钱。
//!
//! 这条测试拿的是请求上真发出去的那两格头，不是循环自己记下的字段：贴错了的
//! 字段在账本上仍然是空的，只有请求上的头会走到账本。

use async_trait::async_trait;
use cog_agent::SelfReviewLoop;
use cog_core::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, ContentBlock,
    LlmClient, Message, SFResult, SelfReviewConfig, StopReason, LLM_ACTOR_HEADER, LLM_TASK_HEADER,
};
use std::sync::{Arc, Mutex};

/// 记下每一次调用请求上的两格头，并回答一份能通过的评审。
struct RecordingLlm {
    seen: Arc<Mutex<Vec<(Option<String>, Option<String>)>>>,
}

#[async_trait]
impl LlmClient for RecordingLlm {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        unimplemented!("the review loop asks non-streaming")
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        unimplemented!("the review loop asks non-streaming")
    }

    async fn chat(&self, _messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        self.seen.lock().unwrap().push((
            options.headers.get(LLM_ACTOR_HEADER).cloned(),
            options.headers.get(LLM_TASK_HEADER).cloned(),
        ));
        // 两步都问、两份答案同一段文字：评审与对照都从它解析，分数给满，循环
        // 走一轮就结束——这条测试量的是头上的两格，不是循环走几轮。
        let body =
            r#"{"issues":[],"missing":[],"strengths":[],"gaps":[],"aligned":[],"score":1.0}"#;
        Ok(ChatResponse {
            content: vec![ContentBlock::text(body)],
            api: "test".into(),
            provider: "test".into(),
            model: "test".into(),
            response_id: None,
            usage: Default::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// 一个带任务的自检，每一次调用都同时带「谁在花」与「哪件活」。
#[tokio::test]
async fn a_review_on_behalf_of_a_task_carries_both_identities_on_every_call() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let llm = RecordingLlm { seen: seen.clone() };

    let review = SelfReviewLoop::new(SelfReviewConfig {
        max_iterations: 1,
        quality_threshold: 0.5,
        spec: Some("the plan names the file".into()),
        best_practices: Vec::new(),
    })
    .with_actor("self_review:planner")
    .with_task("change-ledger-abc");

    let _ = review.review("draft", &llm).await.unwrap();

    let calls = seen.lock().unwrap().clone();
    assert!(!calls.is_empty(), "the loop asked something");
    for (index, (actor, task)) in calls.iter().enumerate() {
        assert_eq!(
            actor.as_deref(),
            Some("self_review:planner"),
            "call {index} lost the role: the review's spend would fall back to the role-less bucket"
        );
        assert_eq!(
            task.as_deref(),
            Some("change-ledger-abc"),
            "call {index} lost the task: this review's cost would not be part of its change's cost"
        );
    }
}

/// 没有任务可报的那些调用不编一个 id：编出来的会读成一件真花过钱的任务。
#[tokio::test]
async fn a_review_with_no_task_carries_no_task_header() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let llm = RecordingLlm { seen: seen.clone() };

    let review = SelfReviewLoop::new(SelfReviewConfig {
        max_iterations: 1,
        quality_threshold: 0.5,
        spec: Some("anything".into()),
        best_practices: Vec::new(),
    })
    .with_actor("self_review")
    .with_task("   ");

    let _ = review.review("draft", &llm).await.unwrap();

    let calls = seen.lock().unwrap().clone();
    assert!(!calls.is_empty(), "the loop asked something");
    for (index, (_, task)) in calls.iter().enumerate() {
        assert_eq!(
            task.as_deref(),
            None,
            "call {index} invented a task id out of a blank one"
        );
    }
}
