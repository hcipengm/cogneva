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
use cog_agent::{Agent, SelfReviewLoop};
use cog_core::{
    Agent as AgentContract, AssistantMessageEventStream, ChatOptions, ChatResponse,
    CompleteOptions, ContentBlock, LlmClient, Message, RuntimeConfig, SFResult, SelfReviewConfig,
    StopReason, LLM_ACTOR_HEADER, LLM_TASK_HEADER,
};
use std::sync::{Arc, Mutex};

/// 一次调用请求上的两格头，按 `(actor, task)` 的先后记。
type StampedHeaders = Vec<(Option<String>, Option<String>)>;

/// 记下每一次调用请求上的两格头，并回答一份能通过的评审。
struct RecordingLlm {
    seen: Arc<Mutex<StampedHeaders>>,
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
        revise: true,
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
        revise: true,
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

/// 协作层拿到的是 trait 上的那个入口，不是循环本身——任务 id 要在外面包了
/// 一层之后仍然到得了请求上。
///
/// 这一层丢参数是编译得过的：`review_and_revise_for_task` 在契约里带一份
/// 默认实现，它把 `task_id` 原样丢掉、转调不带任务的那个方法。一个有真实
/// 评审的实现在那里忘了重写，调度侧照常拿到评审结果，只是这一族的花销从此
/// 归不到任何一件活——账本上读作「这活没花过自检的钱」，而不是「有人丢了参数」。
#[tokio::test]
async fn the_agent_entry_point_forwards_the_task_to_every_review_call() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let agent = Agent::new(
        RuntimeConfig::default(),
        Arc::new(RecordingLlm { seen: seen.clone() }),
    );
    let config = SelfReviewConfig {
        max_iterations: 1,
        quality_threshold: 0.5,
        revise: true,
        spec: Some("the plan names the file".into()),
        best_practices: Vec::new(),
    };

    let _ =
        AgentContract::review_and_revise_for_task(&agent, "draft", &config, "change-ledger-abc")
            .await
            .unwrap();

    let calls = seen.lock().unwrap().clone();
    assert!(!calls.is_empty(), "the agent asked something");
    for (index, (actor, task)) in calls.iter().enumerate() {
        assert_eq!(
            actor.as_deref(),
            Some("self_review:planner"),
            "call {index} lost the role on its way through the agent"
        );
        assert_eq!(
            task.as_deref(),
            Some("change-ledger-abc"),
            "call {index} lost the task on its way through the agent: the wrapper took the \
             contract's default, which drops the argument"
        );
    }
}

/// 记下每次请求的正文，并回答一张能通过的要求表。
///
/// 要求表的每一行都要有 `spec_quote` 且能在规格里找到，否则它会被判成没判过——
/// 这条测试量的是请求里带着什么，替身的回答必须能过，才轮得到量。
struct TableRecordingLlm {
    seen: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl LlmClient for TableRecordingLlm {
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

    async fn chat(&self, messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        self.seen
            .lock()
            .unwrap()
            .push(messages.last().map(|m| m.content()).unwrap_or_default());
        let body = r#"{"rows":[
             {"requirement":"names the file","spec_quote":"name the file","status":"met",
              "evidence":"it says docs/quickstart.md"}]}"#;
        Ok(ChatResponse {
            content: vec![ContentBlock::text(body)],
            ..Default::default()
        })
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// 运行时就带着「有人就这件活说过的方向」，判它的时候那份方向要到请求上。
///
/// 这一层同样是编译得过的丢参数：运行时里有一格，`self_review_loop` 不抄给循环，
/// 判活的尺子上就没有它——人说了话，而读数上「说过了」与「没人说」同形。
#[tokio::test]
async fn an_agent_carries_the_direction_it_was_given_into_its_review() {
    const DIRECTION: &str = "prefers \"quoting the failing line\" over \"a prose verdict\": \
                             prose verdicts could not be checked";
    let seen = Arc::new(Mutex::new(Vec::new()));
    let agent = Agent::new(
        RuntimeConfig {
            role: "generator".into(),
            stated_directions: vec![DIRECTION.into()],
            ..Default::default()
        },
        Arc::new(TableRecordingLlm { seen: seen.clone() }),
    );
    let config = SelfReviewConfig {
        max_iterations: 1,
        quality_threshold: 0.5,
        revise: false,
        spec: Some("name the file".into()),
        best_practices: Vec::new(),
    };

    let _ = AgentContract::review_and_revise(&agent, "draft", &config)
        .await
        .unwrap();

    let payloads = seen.lock().unwrap().clone();
    let payload = payloads.last().expect("the agent asked something");
    let request: serde_json::Value = serde_json::from_str(payload).expect("请求是 JSON");
    let rubric = request["rubric"].as_str().unwrap_or_default();
    assert!(
        rubric.contains(DIRECTION),
        "运行时带着方向，判活时却没到请求上：这一层把它丢了\n{rubric}"
    );
}

/// 不带任务的那个入口不编一个 id 出来：同一层包装，少一格就必须真的少一格。
#[tokio::test]
async fn the_agent_entry_point_without_a_task_carries_no_task_header() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let agent = Agent::new(
        RuntimeConfig::default(),
        Arc::new(RecordingLlm { seen: seen.clone() }),
    );
    let config = SelfReviewConfig {
        max_iterations: 1,
        quality_threshold: 0.5,
        revise: true,
        spec: Some("anything".into()),
        best_practices: Vec::new(),
    };

    let _ = AgentContract::review_and_revise(&agent, "draft", &config)
        .await
        .unwrap();

    let calls = seen.lock().unwrap().clone();
    assert!(!calls.is_empty(), "the agent asked something");
    for (index, (_, task)) in calls.iter().enumerate() {
        assert_eq!(
            task.as_deref(),
            None,
            "call {index} invented a task id for a review nobody made it for"
        );
    }
}
