//! LLM 智能路由 — 多后端故障转移。
//! 当主 backend 返回 429（Rate Limit）或 402（Quota / Payment Required）时，
//! 自动切换到下一个配置的 backend。每次请求都优先尝试最高优先级（索引 0）的
//! backend，自然实现"主 API 恢复后切回"。

use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, Message, SFError, SFResult, UpstreamFailure};
use futures::StreamExt;
use std::sync::Arc;
use tracing::warn;

use crate::{AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions};
use cog_core::LlmClient as LLMProvider;

/// 从我们自己写出的错误文本里取回状态码。
///
/// 首字节之前从事件流里冒出来的 `Error` 事件不携带类型化原因——事件本身没有
/// 放状态码的地方——而这类事件常见于"响应不是 2xx"。provider 在那种情况下写的
/// 是固定格式 `API error (HTTP <code>)`，这里取回的是这个自有的代号，不是上游
/// 的用词。取不到就返回 `None`，不猜。
fn status_from_own_message(msg: &str) -> Option<u16> {
    const MARKER: &str = "(HTTP ";
    let start = msg.find(MARKER)? + MARKER.len();
    let rest = &msg[start..];
    let end = rest.find(')')?;
    rest[..end].trim().parse().ok()
}

/// 既没有类型化原因、也取不回我们自己的代号时，退到上游惯用的措辞上认。
///
/// 这一条是**降级路径**，不是判据的常态：有状态码的地方一律走
/// [`UpstreamFailure`]。认词的做法只在拿不到更硬信号时才启用，因为各家上游的
/// 措辞既不相同也不稳定——网关返回的体里曾出现 `retry_after_seconds` 恰好含
/// 数字而真实原因另有其物，凭文本判会把这类噪声当信号。返回的两个标志分别
/// 对应"像限流"和"像配额"，由调用方与各自的开关配对。
fn text_failover_flags(msg: &str) -> (bool, bool) {
    let lower = msg.to_lowercase();
    let rate = lower.contains("rate limit")
        || lower.contains("too many requests")
        || lower.contains("429");
    let quota = lower.contains("quota exceeded")
        || lower.contains("payment required")
        || lower.contains("insufficient_quota")
        || lower.contains("402");
    (rate, quota)
}

/// 多后端故障转移 Provider。
/// - 每次请求按 backends 数组顺序尝试。
/// - `chat` 方法覆盖默认实现，在获取 `ChatResponse` 后检查 `error_message`，
///   匹配 429/402 时切换到下一个 backend。
/// - `chat_stream` 对初始连接 `Err` 做 failover；连接成功后再包一层"首内容
///   delta 之前的 Error 事件也可切换后端"（见 `wrap_with_failover`），内容
///   一旦开始下发即锁定后端，避免用户看到两个模型拼出的半份答案。
/// - `health_check` 只要任一 backend 健康即返回 `true`。
pub struct RoutingProvider {
    backends: Vec<Arc<dyn LLMProvider>>,
    max_attempts: usize,
    retry_on_429: bool,
    retry_on_402: bool,
}

impl RoutingProvider {
    pub fn new(
        backends: Vec<Arc<dyn LLMProvider>>,
        max_attempts: u32,
        retry_on_429: bool,
        retry_on_402: bool,
    ) -> Self {
        Self {
            backends,
            max_attempts: max_attempts as usize,
            retry_on_429,
            retry_on_402,
        }
    }

    /// 这个原因是否值得换下一个后端。
    ///
    /// 只有"换一个上游可能就成了"的两档才换：限流是分钟级信号，配额耗尽在
    /// 别的端点上未必同样耗尽。鉴权被拒、请求不合法换后端没用——那是我们这一
    /// 侧或凭证的问题，多试几个只会把同一个错误多报几遍。
    fn allows_failover(&self, cause: UpstreamFailure) -> bool {
        match cause {
            UpstreamFailure::RateLimited => self.retry_on_429,
            UpstreamFailure::QuotaExhausted => self.retry_on_402,
            _ => false,
        }
    }

    /// 是否切换到下一个后端。
    ///
    /// 判据的优先序是定的：有类型化原因就只认它；没有类型、但我们自己的错误
    /// 文本里带着状态码代号，就从代号翻出原因再判；两者都没有才退到认词。
    fn should_failover(&self, failure: Option<UpstreamFailure>, message: Option<&str>) -> bool {
        if let Some(cause) = failure {
            return self.allows_failover(cause);
        }
        let Some(msg) = message else {
            return false;
        };
        if let Some(status) = status_from_own_message(msg) {
            return self.allows_failover(UpstreamFailure::from_status(status));
        }
        let (rate, quota) = text_failover_flags(msg);
        (rate && self.retry_on_429) || (quota && self.retry_on_402)
    }

    /// Wrap a backend stream so a rate-limit/quota `Error` event that arrives
    /// before the first content delta transparently fails over to the next
    /// backend. Non-content marker events (Start/TextStart/...) seen before the
    /// switch are discarded, so consumers observe one clean, well-formed stream.
    /// Once any content delta has been forwarded the backend is committed —
    /// switching then would show the user a half-answer from two models.
    fn wrap_with_failover(
        &self,
        first: AssistantMessageEventStream,
        first_idx: usize,
        messages: &[Message],
        options: &ChatOptions,
    ) -> AssistantMessageEventStream {
        let attempts = self.backends.len().min(self.max_attempts).max(1);
        let backends = self.backends.clone();
        let retry_on_429 = self.retry_on_429;
        let retry_on_402 = self.retry_on_402;
        let messages = messages.to_vec();
        let options = options.clone();

        let (out_stream, mut producer) = AssistantMessageEventStream::with_capacity(64);
        tokio::spawn(async move {
            let probe = RoutingProvider::new(backends, attempts as u32, retry_on_429, retry_on_402);
            let mut current = first;
            let mut idx = first_idx;

            'backend: loop {
                let result_fut = current.result();
                let mut pending: Vec<AssistantMessageEvent> = Vec::new();
                let mut committed = false;

                while let Some(ev) = current.next().await {
                    if committed {
                        if producer.push(ev).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    match &ev {
                        AssistantMessageEvent::Error { error, .. } => {
                            let text = error.content();
                            if probe.should_failover(None, Some(&text)) && idx + 1 < attempts {
                                warn!(
                                    "Backend {} stream failed pre-content ({}), failing over to backend {}",
                                    idx, text, idx + 1
                                );
                                idx += 1;
                                match probe.backends[idx].chat_stream(&messages, &options).await {
                                    Ok(s) => {
                                        current = s;
                                        continue 'backend;
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Backend {} connect failed during failover: {e}",
                                            idx
                                        );
                                    }
                                }
                            }
                            committed = true;
                            for p in pending.drain(..) {
                                if producer.push(p).await.is_err() {
                                    return;
                                }
                            }
                            if producer.push(ev).await.is_err() {
                                return;
                            }
                        }
                        AssistantMessageEvent::TextDelta { .. }
                        | AssistantMessageEvent::ThinkingDelta { .. }
                        | AssistantMessageEvent::ToolCallDelta { .. } => {
                            committed = true;
                            for p in pending.drain(..) {
                                if producer.push(p).await.is_err() {
                                    return;
                                }
                            }
                            if producer.push(ev).await.is_err() {
                                return;
                            }
                        }
                        _ => pending.push(ev),
                    }
                }

                // Stream ended: flush any markers of a delta-less reply, then
                // propagate the final response.
                for p in pending.drain(..) {
                    if producer.push(p).await.is_err() {
                        return;
                    }
                }
                producer.end(result_fut.await);
                return;
            }
        });

        out_stream
    }
}

#[async_trait]
impl LLMProvider for RoutingProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let attempts = self.backends.len().min(self.max_attempts).max(1);

        for i in 0..attempts {
            let backend = &self.backends[i];
            match backend.chat_stream(messages, options).await {
                Ok(stream) => {
                    return Ok(self.wrap_with_failover(stream, i, messages, options));
                }
                Err(e) => {
                    let err_str = format!("{e}");
                    warn!("Backend {} chat_stream failed: {}", i, err_str);
                    if self.should_failover(e.upstream_failure(), Some(&err_str))
                        && i + 1 < attempts
                    {
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Err(SFError::LLM("All LLM backends failed".into()))
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let chat_options = ChatOptions {
            model: options.model.clone(),
            temperature: options.temperature,
            max_tokens: options.max_tokens,
            api_key: options.api_key.clone(),
            ..Default::default()
        };
        self.chat_stream(&[Message::user(prompt)], &chat_options)
            .await
    }

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        let attempts = self.backends.len().min(self.max_attempts).max(1);

        for i in 0..attempts {
            let backend = &self.backends[i];
            let start = std::time::Instant::now();
            let mut stream = match backend.chat_stream(messages, options).await {
                Ok(s) => s,
                Err(e) => {
                    let err_str = format!("{e}");
                    warn!("Backend {} chat_stream failed: {}", i, err_str);
                    if self.should_failover(e.upstream_failure(), Some(&err_str))
                        && i + 1 < attempts
                    {
                        continue;
                    }
                    return Err(e);
                }
            };

            // Drain the bounded event stream to completion before awaiting the
            // result. The producer task pushes events with backpressure and only
            // calls `end()` — which resolves `result()` — after its last push
            // succeeds. Once the channel fills, `push().await` blocks, so a
            // consumer that awaits `result()` without ever reading events
            // deadlocks both itself and the producer: the producer stalls on a
            // full channel and never reaches `end()`, and `result()` waits on a
            // oneshot that is never sent. A large reply (e.g. a structured JSON
            // extraction emitting more deltas than the stream capacity) hangs the
            // caller forever with no error and no timeout. Reading every event
            // lets the producer finish. This mirrors the drain each backend
            // provider already does in its own `chat()`.
            while stream.next().await.is_some() {}

            let response = stream.result().await;
            let latency_ms = start.elapsed().as_millis() as u64;

            let obs = crate::observable::global_observable();
            if response.error_message.is_some() {
                obs.record_error();
            } else {
                let tokens_in = response.usage.input as u64;
                let tokens_out = response.usage.output as u64;
                obs.record_call(tokens_in, tokens_out, latency_ms);
            }

            if self.should_failover(response.upstream_failure, response.error_message.as_deref()) {
                if i + 1 < attempts {
                    warn!(
                        backend = i,
                        cause = ?response.upstream_failure,
                        error = response.error_message.as_deref().unwrap_or("unknown"),
                        "Backend refused this call; failing over to the next backend"
                    );
                    continue;
                }
                // 换无可换：把**这次失败的原始原因**带出去，而不是另造一句
                // 笼统的话。下游据此区分该等窗口复位还是该重试，丢在这里就
                // 只能再猜一遍文本。
                return Err(match response.upstream_failure {
                    Some(cause) => SFError::Upstream {
                        cause,
                        reason: response.error_message.unwrap_or_default(),
                    },
                    None => SFError::LLM(
                        response
                            .error_message
                            .unwrap_or_else(|| "All LLM backends refused this call".into()),
                    ),
                });
            }

            return Ok(response);
        }

        Err(SFError::LLM(
            "All LLM backends exhausted without a typed refusal".into(),
        ))
    }

    async fn health_check(&self) -> bool {
        for backend in &self.backends {
            if backend.health_check().await {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{AssistantMessageEvent, ContentBlock, Message, StopReason};

    struct MockProvider {
        response_text: String,
        error_msg: Option<String>,
        /// When true, emit a TextDelta before the Error event (post-content
        /// failure — must NOT trigger failover).
        mid_stream_error: bool,
    }

    #[async_trait]
    impl LLMProvider for MockProvider {
        async fn chat(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            Ok(ChatResponse {
                content: vec![ContentBlock::text(self.response_text.clone())],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: crate::Usage::default(),
                stop_reason: if self.error_msg.is_some() {
                    StopReason::Error
                } else {
                    StopReason::Stop
                },
                error_message: self.error_msg.clone(),
                upstream_failure: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            let content = vec![ContentBlock::Text {
                text: self.response_text.clone(),
                text_signature: None,
            }];
            let response = ChatResponse {
                content: content.clone(),
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: crate::Usage::default(),
                stop_reason: if self.error_msg.is_some() {
                    StopReason::Error
                } else {
                    StopReason::Stop
                },
                error_message: self.error_msg.clone(),
                upstream_failure: None,
                timestamp: chrono::Utc::now(),
            };
            let (stream, mut producer) = AssistantMessageEventStream::with_capacity(10);
            let _ = producer
                .push(AssistantMessageEvent::Start {
                    timestamp: chrono::Utc::now(),
                })
                .await;
            if self.error_msg.is_some() {
                if self.mid_stream_error {
                    let _ = producer
                        .push(AssistantMessageEvent::TextDelta {
                            content_index: 0,
                            delta: "partial".into(),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }
                let _ = producer
                    .push(AssistantMessageEvent::Error {
                        reason: StopReason::Error,
                        error: Message::assistant_text(self.error_msg.clone().unwrap_or_default()),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            } else {
                let _ = producer
                    .push(AssistantMessageEvent::TextEnd {
                        content_index: 0,
                        content: self.response_text.clone(),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                let _ = producer
                    .push(AssistantMessageEvent::Done {
                        reason: StopReason::Stop,
                        message: Message::assistant_text(self.response_text.clone()),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            }
            producer.end(response);
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &CompleteOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            self.chat_stream(&[], &ChatOptions::default()).await
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_failover_on_429() {
        let primary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 429 rate limit exceeded".into()),
            mid_stream_error: false,
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
            mid_stream_error: false,
        });

        let router = RoutingProvider::new(vec![primary, secondary], 3, true, true);
        let response = router
            .chat(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect();
        assert_eq!(text, "hello from secondary");
    }

    #[tokio::test]
    async fn test_failover_on_402() {
        let primary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 402 payment required".into()),
            mid_stream_error: false,
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
            mid_stream_error: false,
        });

        let router = RoutingProvider::new(vec![primary, secondary], 3, true, true);
        let response = router
            .chat(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect();
        assert_eq!(text, "hello from secondary");
    }

    #[tokio::test]
    async fn test_no_failover_when_disabled() {
        let primary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 429 rate limit exceeded".into()),
            mid_stream_error: false,
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
            mid_stream_error: false,
        });

        let router = RoutingProvider::new(vec![primary, secondary], 3, false, false);
        let response = router
            .chat(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        assert!(response.error_message.is_some());
    }

    #[tokio::test]
    async fn test_all_backends_exhausted() {
        let primary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 429".into()),
            mid_stream_error: false,
        });
        let secondary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 402".into()),
            mid_stream_error: false,
        });

        let router = RoutingProvider::new(vec![primary, secondary], 3, true, true);
        let result = router
            .chat(&[Message::user("hi")], &ChatOptions::default())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_primary_success_no_failover() {
        let primary = Arc::new(MockProvider {
            response_text: "hello from primary".into(),
            error_msg: None,
            mid_stream_error: false,
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
            mid_stream_error: false,
        });

        let router = RoutingProvider::new(vec![primary, secondary], 3, true, true);
        let response = router
            .chat(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect();
        assert_eq!(text, "hello from primary");
    }

    /// 判据的优先序：类型化原因在场时，文本说什么都不改变结论。上游把配额写成
    /// 不带惯用词的措辞时，靠认词认不出，靠状态码才认得出。
    #[test]
    fn typed_cause_decides_even_when_the_text_says_nothing() {
        let router = RoutingProvider::new(Vec::new(), 3, true, true);

        assert!(router.should_failover(Some(UpstreamFailure::QuotaExhausted), None));
        assert!(router.should_failover(
            Some(UpstreamFailure::RateLimited),
            Some("temporarily out of capacity")
        ));

        // 类型说"不该换"，文本再怎么像限流也不换。
        assert!(
            !router.should_failover(Some(UpstreamFailure::Auth), Some("429 rate limit exceeded"))
        );
        assert!(!router.should_failover(Some(UpstreamFailure::ServerError), Some("quota exceeded")));
    }

    /// 没有类型时先认我们自己的状态码代号，再退到认词。
    #[test]
    fn own_status_marker_beats_vendor_wording() {
        let router = RoutingProvider::new(Vec::new(), 3, true, true);

        // 代号是 503（服务端故障）→ 不换，哪怕体里写着 "rate limit"。
        assert!(
            !router.should_failover(None, Some("API error (HTTP 503): upstream said rate limit"))
        );
        assert!(router.should_failover(None, Some("API error (HTTP 429): slow down")));
        assert!(router.should_failover(None, Some("API error (HTTP 402): pay up")));
    }

    /// 两个开关各自管一档；关掉的那档不换后端。
    #[test]
    fn each_switch_gates_its_own_cause() {
        let only_429 = RoutingProvider::new(Vec::new(), 3, true, false);
        assert!(only_429.should_failover(Some(UpstreamFailure::RateLimited), None));
        assert!(!only_429.should_failover(Some(UpstreamFailure::QuotaExhausted), None));

        let only_402 = RoutingProvider::new(Vec::new(), 3, false, true);
        assert!(!only_402.should_failover(Some(UpstreamFailure::RateLimited), None));
        assert!(only_402.should_failover(Some(UpstreamFailure::QuotaExhausted), None));
    }

    /// 状态码代号的解析只看我们自己写的那种固定形状，取不到就不认。
    #[test]
    fn status_marker_parsing_is_exact() {
        assert_eq!(
            status_from_own_message("API error (HTTP 429): x"),
            Some(429)
        );
        assert_eq!(
            status_from_own_message("API error (HTTP 503): x"),
            Some(503)
        );
        assert_eq!(status_from_own_message("429 rate limit"), None);
        assert_eq!(
            status_from_own_message("HTTP error: connection reset"),
            None
        );
    }

    #[tokio::test]
    async fn test_stream_failover_on_pre_content_error() {
        let primary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 429 rate limit exceeded".into()),
            mid_stream_error: false,
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
            mid_stream_error: false,
        });

        let router = RoutingProvider::new(vec![primary, secondary], 3, true, true);
        let mut stream = router
            .chat_stream(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        let mut result_fut = stream.result();
        let mut saw_error = false;
        while let Some(ev) = stream.next().await {
            if matches!(ev, AssistantMessageEvent::Error { .. }) {
                saw_error = true;
            }
        }
        let response = (&mut result_fut).await;
        assert!(!saw_error, "pre-content error must be hidden by failover");
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect();
        assert_eq!(text, "hello from secondary");
    }

    #[tokio::test]
    async fn test_stream_no_failover_after_content() {
        let primary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 429 rate limit exceeded".into()),
            mid_stream_error: true,
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
            mid_stream_error: false,
        });

        let router = RoutingProvider::new(vec![primary, secondary], 3, true, true);
        let mut stream = router
            .chat_stream(&[Message::user("hi")], &ChatOptions::default())
            .await
            .unwrap();
        let mut result_fut = stream.result();
        let mut deltas = String::new();
        let mut saw_error = false;
        while let Some(ev) = stream.next().await {
            match ev {
                AssistantMessageEvent::TextDelta { delta, .. } => deltas.push_str(&delta),
                AssistantMessageEvent::Error { .. } => saw_error = true,
                _ => {}
            }
        }
        let response = (&mut result_fut).await;
        assert_eq!(deltas, "partial");
        assert!(saw_error, "post-content error must reach the consumer");
        assert_eq!(
            response.error_message.as_deref(),
            Some("API error: 429 rate limit exceeded")
        );
    }

    /// A backend whose `chat_stream` emits far more events than the stream
    /// capacity from a spawned producer task, exactly like the real OpenAI
    /// provider streaming a large reply. The producer can only reach `end()`
    /// (which resolves `result()`) if the consumer keeps reading; once the
    /// bounded channel fills, `push().await` blocks.
    struct HighVolumeProvider {
        events: usize,
        capacity: usize,
    }

    #[async_trait]
    impl LLMProvider for HighVolumeProvider {
        async fn chat(
            &self,
            messages: &[Message],
            options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            let mut stream = self.chat_stream(messages, options).await?;
            while stream.next().await.is_some() {}
            Ok(stream.result().await)
        }

        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            let (stream, mut producer) = AssistantMessageEventStream::with_capacity(self.capacity);
            let events = self.events;
            tokio::spawn(async move {
                let content = vec![ContentBlock::text("done")];
                for i in 0..events {
                    // Blocks on backpressure once the channel is full and no
                    // consumer is draining — the deadlock trigger.
                    if producer
                        .push(AssistantMessageEvent::TextDelta {
                            content_index: 0,
                            delta: format!("chunk{i}"),
                            timestamp: chrono::Utc::now(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let response = ChatResponse {
                    content,
                    api: "mock".into(),
                    provider: "mock".into(),
                    model: "mock".into(),
                    response_id: None,
                    usage: crate::Usage::default(),
                    stop_reason: StopReason::Stop,
                    error_message: None,
                    upstream_failure: None,
                    timestamp: chrono::Utc::now(),
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

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// `RoutingProvider::chat()` must drain the event stream before awaiting
    /// `result()`. Without the drain, a reply emitting more events than the
    /// stream capacity deadlocks: the producer blocks on a full channel and
    /// never calls `end()`, so `result()` waits forever with no error and no
    /// timeout. This hung every non-streaming `chat()` caller routed through
    /// `RoutingProvider` (notably the memory ingestor's structured extraction).
    /// The timeout turns a would-be hang into a test failure.
    #[tokio::test]
    async fn test_chat_drains_high_volume_stream_without_deadlock() {
        let backend = Arc::new(HighVolumeProvider {
            events: 500,
            capacity: 8,
        });
        let router = RoutingProvider::new(vec![backend], 1, true, true);

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            router.chat(&[Message::user("hi")], &ChatOptions::default()),
        )
        .await
        .expect("RoutingProvider::chat() deadlocked: it must drain the event stream before awaiting result()")
        .unwrap();

        assert_eq!(response.stop_reason, StopReason::Stop);
    }
}
