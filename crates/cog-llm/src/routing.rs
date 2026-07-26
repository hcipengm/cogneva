//! LLM 智能路由 — 多后端故障转移。
//! 当主 backend 返回 429（Rate Limit）或 402（Quota / Payment Required）时，
//! 自动切换到下一个配置的 backend。每次请求都优先尝试最高优先级（索引 0）的
//! backend，自然实现"主 API 恢复后切回"。

use async_trait::async_trait;
use cog_core::{Message, SFError, SFResult};
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
/// - `chat_stream` 仅对初始连接返回的 `Err` 做 failover；流内错误暂不支持自动切换。
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
                Ok(stream) => return Ok(stream),
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
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
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
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
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
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
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
        });
        let secondary = Arc::new(MockProvider {
            response_text: "".into(),
            error_msg: Some("API error: 402".into()),
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
        });
        let secondary = Arc::new(MockProvider {
            response_text: "hello from secondary".into(),
            error_msg: None,
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
}
