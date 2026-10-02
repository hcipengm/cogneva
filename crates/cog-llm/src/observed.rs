use async_trait::async_trait;
use cog_core::{
    AssistantMessageEvent, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions,
    LlmClient, Message, SFResult,
};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;

use crate::observable::{global_observable, LlmObservable};

/// 观测那一跳的容量。
///
/// 这层是旁路：它要把事件原样交给下游，又只有把流读到底才知道这一跳的结果，
/// 所以只能自己再起一跳。容量取与路由层相同的大小——观测不该成为新的瓶颈，
/// 下游读得慢时压力先落在路由那一跳上，与没有这层时一致。
const FORWARD_CAPACITY: usize = 64;

pub struct ObservedLlmClient {
    inner: Arc<dyn LlmClient>,
    obs: Arc<LlmObservable>,
}

impl ObservedLlmClient {
    pub fn new(inner: Arc<dyn LlmClient>) -> Self {
        Self::with_observable(inner, global_observable())
    }

    /// 用指定的计数器装配。进程里只有插件建的那一个实例、拿的也是全局那份；
    /// 允许注入是为了让断言不必去读全进程共享的计数器。
    pub fn with_observable(inner: Arc<dyn LlmClient>, obs: Arc<LlmObservable>) -> Self {
        Self { inner, obs }
    }

    pub fn inner(&self) -> Arc<dyn LlmClient> {
        self.inner.clone()
    }

    /// 一次**调用**的收尾读数。
    ///
    /// 按最终响应记账：成功记 usage，失败只记一次错误。**不按上游换过几次
    /// 后端记**——路由为了绕开限流会在一条流里换后端，那是路由的事；在这里
    /// 重数一遍会把同一次调用报成多次，按用量算的成本就随换手次数涨价了。
    fn record_response(&self, response: &ChatResponse, latency_ms: u64) {
        if response.error_message.is_some() {
            self.obs.record_error();
        } else {
            self.obs.record_call(
                response.usage.input as u64,
                response.usage.output as u64,
                latency_ms,
            );
        }
    }
}

/// TTFT 记在**第一个内容增量**上。
///
/// `Start` / `TextStart` 这类标记在首个 token 之前就到了，用它们计时量的是
/// 「连上了」，不是「出字了」。
fn is_content_delta(event: &AssistantMessageEvent) -> bool {
    matches!(
        event,
        AssistantMessageEvent::TextDelta { .. }
            | AssistantMessageEvent::ThinkingDelta { .. }
            | AssistantMessageEvent::ToolCallDelta { .. }
    )
}

/// 把内部流原样转发出去，途中记 TTFT，流结束时记这一跳的结果。
///
/// 流式调用的结果（含 usage）只有读到底才拿得到，而事件流是具体类型、没法在
/// 外面套一层返回同一类型的适配器，所以观测只能挂在转发路径上。下游把这层流
/// 丢掉时那一跳不记数：没有最终响应就分不出成功还是失败，少记一笔好过把
/// 「被放弃」报成一次错误。
fn observed_stream(
    stream: AssistantMessageEventStream,
    obs: Arc<LlmObservable>,
    start: Instant,
) -> AssistantMessageEventStream {
    let (out, mut producer) = AssistantMessageEventStream::with_capacity(FORWARD_CAPACITY);
    tokio::spawn(async move {
        let mut stream = stream;
        let mut first_content_seen = false;
        while let Some(event) = stream.next().await {
            if !first_content_seen && is_content_delta(&event) {
                obs.record_ttft(start.elapsed().as_millis() as u64);
                first_content_seen = true;
            }
            if producer.push(event).await.is_err() {
                return;
            }
        }
        let response = stream.result().await;
        let latency_ms = start.elapsed().as_millis() as u64;
        if response.error_message.is_some() {
            obs.record_error();
        } else {
            obs.record_call(
                response.usage.input as u64,
                response.usage.output as u64,
                latency_ms,
            );
        }
        producer.end(response);
    });
    out
}

#[async_trait]
impl LlmClient for ObservedLlmClient {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let start = Instant::now();
        match self.inner.chat_stream(messages, options).await {
            Ok(stream) => Ok(observed_stream(stream, self.obs.clone(), start)),
            Err(e) => {
                // 连流都没拿到——上游在首字节之前就拒了，agent 的流量正是
                // 这个形态失败的。这一跳的失败面就在这条路上，不记在这里
                // 就等于「上游全拒」在 D9 上留下一个 0。
                self.obs.record_error();
                Err(e)
            }
        }
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let start = Instant::now();
        match self.inner.complete_stream(prompt, options).await {
            Ok(stream) => Ok(observed_stream(stream, self.obs.clone(), start)),
            Err(e) => {
                self.obs.record_error();
                Err(e)
            }
        }
    }

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        let start = Instant::now();
        match self.inner.chat(messages, options).await {
            Ok(response) => {
                self.record_response(&response, start.elapsed().as_millis() as u64);
                Ok(response)
            }
            Err(e) => {
                self.obs.record_error();
                Err(e)
            }
        }
    }

    async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Observable, SFError, StopReason, Usage};

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Outcome {
        /// 正常出字并给出 usage。
        Success,
        /// 流在首字节前以错误响应收尾。
        Refused,
        /// 连流都没拿到。
        ConnectFailure,
    }

    struct FakeClient {
        outcome: Outcome,
    }

    fn ok_response() -> ChatResponse {
        ChatResponse {
            usage: Usage {
                input: 5,
                output: 7,
                ..Usage::default()
            },
            stop_reason: StopReason::Stop,
            ..ChatResponse::default()
        }
    }

    fn refused_response() -> ChatResponse {
        ChatResponse {
            stop_reason: StopReason::Error,
            error_message: Some("API error (HTTP 403)".into()),
            ..ChatResponse::default()
        }
    }

    #[async_trait]
    impl LlmClient for FakeClient {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            if self.outcome == Outcome::ConnectFailure {
                return Err(SFError::LLM(
                    "upstream refused before the first byte".into(),
                ));
            }
            let outcome = self.outcome;
            let (stream, mut producer) = AssistantMessageEventStream::with_capacity(4);
            tokio::spawn(async move {
                let _ = producer
                    .push(AssistantMessageEvent::Start {
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                let response = if outcome == Outcome::Success {
                    let _ = producer
                        .push(AssistantMessageEvent::TextDelta {
                            content_index: 0,
                            delta: "hi".into(),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                    ok_response()
                } else {
                    refused_response()
                };
                producer.end(response);
            });
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &CompleteOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            self.chat_stream(&[], &ChatOptions::default()).await
        }

        async fn chat(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            if self.outcome == Outcome::ConnectFailure {
                return Err(SFError::LLM("upstream refused".into()));
            }
            Ok(if self.outcome == Outcome::Success {
                ok_response()
            } else {
                refused_response()
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn client(outcome: Outcome) -> (ObservedLlmClient, Arc<LlmObservable>) {
        let obs = Arc::new(LlmObservable::new());
        (
            ObservedLlmClient::with_observable(Arc::new(FakeClient { outcome }), obs.clone()),
            obs,
        )
    }

    async fn reading(obs: &LlmObservable, name: &str) -> Option<f64> {
        obs.collect_metrics("D9")
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.name == name)
            .map(|m| m.value)
    }

    /// 把流读到底，等观测那一跳收尾。
    async fn drain(stream: &mut AssistantMessageEventStream) {
        while stream.next().await.is_some() {}
    }

    #[tokio::test]
    async fn a_streamed_call_is_recorded_once_with_its_usage() {
        let (client, obs) = client(Outcome::Success);
        let mut stream = client
            .chat_stream(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        drain(&mut stream).await;

        assert_eq!(reading(&obs, "llm_call_count").await, Some(1.0));
        assert_eq!(reading(&obs, "llm_token_in").await, Some(5.0));
        assert_eq!(reading(&obs, "llm_token_out").await, Some(7.0));
        assert_eq!(reading(&obs, "llm_error_count").await, Some(0.0));
        // 有内容增量才有 TTFT，且这两个读数只在 calls > 0 时发布。
        assert!(reading(&obs, "llm_avg_ttft_ms").await.is_some());
        assert!(reading(&obs, "llm_avg_latency_ms").await.is_some());
    }

    #[tokio::test]
    async fn a_refused_stream_counts_an_error_and_no_call() {
        let (client, obs) = client(Outcome::Refused);
        let mut stream = client
            .chat_stream(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        drain(&mut stream).await;

        assert_eq!(reading(&obs, "llm_error_count").await, Some(1.0));
        assert_eq!(reading(&obs, "llm_call_count").await, Some(0.0));
        assert_eq!(reading(&obs, "llm_token_in").await, Some(0.0));
        assert!(
            reading(&obs, "llm_avg_ttft_ms").await.is_none(),
            "没有内容增量就不该发布 TTFT"
        );
    }

    #[tokio::test]
    async fn a_connect_failure_before_the_first_byte_is_recorded() {
        let (client, obs) = client(Outcome::ConnectFailure);
        assert!(client
            .chat_stream(&[Message::user("hi")], &ChatOptions::default())
            .await
            .is_err());

        assert_eq!(reading(&obs, "llm_error_count").await, Some(1.0));
        assert_eq!(reading(&obs, "llm_call_count").await, Some(0.0));
    }

    #[tokio::test]
    async fn a_non_streaming_call_is_recorded_once() {
        let (client, obs) = client(Outcome::Success);
        client
            .chat(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();

        assert_eq!(reading(&obs, "llm_call_count").await, Some(1.0));
        assert_eq!(reading(&obs, "llm_token_in").await, Some(5.0));
        assert_eq!(reading(&obs, "llm_token_out").await, Some(7.0));
    }

    /// 这一条钉的是**装配**：进程里那个客户端是 `ObservedLlmClient` 套在
    /// `RoutingProvider` 外面，四个读数必须在这层套法下动。分开测两层会漏掉
    /// 「每层各自都对、合起来没人记流式」——那正是线上读 0 的形状。
    #[tokio::test]
    async fn the_composed_client_records_a_streamed_call() {
        let backend = Arc::new(FakeClient {
            outcome: Outcome::Success,
        });
        let router = crate::RoutingProvider::new(vec![backend], 1, true, true);
        let obs = Arc::new(LlmObservable::new());
        let client = ObservedLlmClient::with_observable(Arc::new(router), obs.clone());

        let mut stream = client
            .chat_stream(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        drain(&mut stream).await;

        assert_eq!(reading(&obs, "llm_call_count").await, Some(1.0));
        assert_eq!(reading(&obs, "llm_token_in").await, Some(5.0));
        assert_eq!(reading(&obs, "llm_token_out").await, Some(7.0));
    }

    #[tokio::test]
    async fn a_failed_non_streaming_call_counts_an_error() {
        let (client, obs) = client(Outcome::ConnectFailure);
        assert!(client
            .chat(&[Message::user("hi")], &ChatOptions::default())
            .await
            .is_err());

        assert_eq!(reading(&obs, "llm_error_count").await, Some(1.0));
        assert_eq!(reading(&obs, "llm_call_count").await, Some(0.0));
    }
}
