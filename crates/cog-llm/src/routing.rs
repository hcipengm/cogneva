//! LLM 智能路由 — 多后端故障转移。
//! 当主 backend 返回 429（Rate Limit）或 402（Quota / Payment Required）时，
//! 自动切换到下一个配置的 backend。每次请求都优先尝试最高优先级（索引 0）的
//! backend，自然实现"主 API 恢复后切回"。

use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, Message, SFError, SFResult};
use futures::StreamExt;
use std::sync::Arc;
use tracing::warn;

use crate::{AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions};
use cog_core::LlmClient as LLMProvider;

/// 检测错误消息是否表示 429 或 402。
/// 基于字符串匹配，零改动现有 provider 实现。
pub fn is_rate_limit_or_quota_error(error_message: Option<&str>) -> bool {
    let Some(msg) = error_message else {
        return false;
    };
    let lower = msg.to_lowercase();
    lower.contains("429")
        || lower.contains("402")
        || lower.contains("rate limit")
        || lower.contains("quota exceeded")
        || lower.contains("payment required")
        || lower.contains("too many requests")
        || lower.contains("insufficient_quota")
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

    fn should_failover(&self, error_message: Option<&str>) -> bool {
        if !self.retry_on_429 && !self.retry_on_402 {
            return false;
        }
        let Some(msg) = error_message else {
            return false;
        };
        let lower = msg.to_lowercase();
        if self.retry_on_429
            && (lower.contains("429")
                || lower.contains("rate limit")
                || lower.contains("too many requests"))
        {
            return true;
        }
        if self.retry_on_402
            && (lower.contains("402")
                || lower.contains("quota exceeded")
                || lower.contains("payment required")
                || lower.contains("insufficient_quota"))
        {
            return true;
        }
        false
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
                            if probe.should_failover(Some(&text)) && idx + 1 < attempts {
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
                    if self.should_failover(Some(&err_str)) && i + 1 < attempts {
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
                    if self.should_failover(Some(&err_str)) && i + 1 < attempts {
                        continue;
                    }
                    return Err(e);
                }
            };

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

            if self.should_failover(response.error_message.as_deref()) {
                if i + 1 < attempts {
                    warn!(
                        "Backend {} returned rate-limit/quota error ({}), failing over to backend {}",
                        i,
                        response.error_message.as_deref().unwrap_or("unknown"),
                        i + 1
                    );
                    continue;
                }
                return Err(SFError::LLM(response.error_message.unwrap_or_else(|| {
                    "All LLM backends exhausted due to rate limits or quota errors".into()
                })));
            }

            return Ok(response);
        }

        Err(SFError::LLM(
            "All LLM backends exhausted due to rate limits or quota errors".into(),
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
                timestamp: chrono::Utc::now(),
            };
            let (stream, mut producer) = AssistantMessageEventStream::with_capacity(10);
            let _ = producer
                .push(AssistantMessageEvent::Start {
                    partial: Message::assistant(content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            if self.error_msg.is_some() {
                if self.mid_stream_error {
                    let _ = producer
                        .push(AssistantMessageEvent::TextDelta {
                            content_index: 0,
                            delta: "partial".into(),
                            partial: Message::assistant(content.clone()),
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
                        partial: Message::assistant(content),
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

    #[test]
    fn test_is_rate_limit_or_quota_error() {
        assert!(is_rate_limit_or_quota_error(Some("429 rate limit")));
        assert!(is_rate_limit_or_quota_error(Some("402 payment required")));
        assert!(is_rate_limit_or_quota_error(Some("quota exceeded")));
        assert!(!is_rate_limit_or_quota_error(Some(
            "500 internal server error"
        )));
        assert!(!is_rate_limit_or_quota_error(None));
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
}
