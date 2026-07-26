use async_trait::async_trait;
use cog_core::{Message, SFError, SFResult};
use std::sync::Arc;
use std::time::Duration;

// Re-export circuit breaker from cog-core.
pub use cog_core::resilience::{CircuitBreaker, CircuitBreakerConfig, CircuitState};

// ─── Retry Policy ───

/// Backoff strategy for retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffStrategy {
    /// Fixed delay between retries.
    Fixed,
    /// Exponential backoff: delay = base * 2^attempt.
    Exponential,
    /// Linear backoff: delay = base * attempt.
    Linear,
}

/// Retry policy for transient LLM failures.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub strategy: BackoffStrategy,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 500,
            max_delay_ms: 8000,
            strategy: BackoffStrategy::Exponential,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Compute the delay for a given retry attempt (0-indexed).
    pub fn delay(&self, attempt: u32) -> Duration {
        let base = self.base_delay_ms;
        let raw = match self.strategy {
            BackoffStrategy::Fixed => base,
            BackoffStrategy::Exponential => base.saturating_mul(2u64.saturating_pow(attempt)),
            BackoffStrategy::Linear => base.saturating_mul(attempt as u64 + 1),
        };
        let capped = raw.min(self.max_delay_ms);
        let jittered = if self.jitter {
            let frac = (capped as f64 * 0.75) as u64;
            capped - ((capped - frac) / 2)
        } else {
            capped
        };
        Duration::from_millis(jittered)
    }
}

// ─── Resilient Provider ───

/// Wraps any [`LLMProvider`] with retry and circuit-breaker logic.
/// Streaming methods (`chat_stream`, `complete_stream`) apply retry to the
/// initial connection only; mid-stream failures are not retried (they are
/// surfaced as `AssistantMessageEvent::Error` inside the stream).
/// The non-streaming `chat` method retries the full operation.
pub struct ResilientProvider {
    inner: Arc<dyn LLMProvider>,
    retry: RetryPolicy,
    breaker: Arc<CircuitBreaker>,
}

impl ResilientProvider {
    pub fn new(
        inner: Arc<dyn LLMProvider>,
        retry: RetryPolicy,
        breaker_cfg: CircuitBreakerConfig,
    ) -> Self {
        Self {
            inner,
            retry,
            breaker: Arc::new(CircuitBreaker::new(breaker_cfg)),
        }
    }

    /// Access the underlying provider.
    pub fn inner(&self) -> &Arc<dyn LLMProvider> {
        &self.inner
    }

    /// Access the circuit breaker for introspection.
    pub fn circuit_breaker(&self) -> &CircuitBreaker {
        &self.breaker
    }

    async fn with_retry<T, F, Fut>(&self, operation: F) -> SFResult<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = SFResult<T>>,
    {
        let mut last_err = None;
        for attempt in 0..=self.retry.max_retries {
            self.breaker.allow()?;
            match operation().await {
                Ok(v) => {
                    self.breaker.record_success();
                    return Ok(v);
                }
                Err(e) => {
                    self.breaker.record_failure();
                    last_err = Some(e);
                    if attempt < self.retry.max_retries {
                        tokio::time::sleep(self.retry.delay(attempt)).await;
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| SFError::Agent("all retries exhausted".into())))
    }
}

#[async_trait]
impl LLMProvider for ResilientProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let inner = self.inner.clone();
        let messages = messages.to_vec();
        let options = options.clone();
        self.with_retry(move || {
            let inner = inner.clone();
            let messages = messages.clone();
            let options = options.clone();
            async move { inner.chat_stream(&messages, &options).await }
        })
        .await
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let inner = self.inner.clone();
        let prompt = prompt.to_string();
        let options = options.clone();
        self.with_retry(move || {
            let inner = inner.clone();
            let prompt = prompt.clone();
            let options = options.clone();
            async move { inner.complete_stream(&prompt, &options).await }
        })
        .await
    }

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        let inner = self.inner.clone();
        let messages = messages.to_vec();
        let options = options.clone();
        self.with_retry(move || {
            let inner = inner.clone();
            let messages = messages.clone();
            let options = options.clone();
            async move { inner.chat(&messages, &options).await }
        })
        .await
    }

    async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }
}

use crate::{AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions};
use cog_core::LlmClient as LLMProvider;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_ms: 1000,
        });

        assert!(cb.allow().is_ok());
        cb.record_failure();
        assert!(cb.allow().is_ok());
        cb.record_failure();
        assert!(cb.allow().is_ok());
        cb.record_failure();

        assert!(cb.allow().is_err());
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_half_open_recovery() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 1,
            timeout_ms: 10,
        });

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(50));
        assert!(cb.allow().is_ok());
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_fails_again() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            timeout_ms: 10,
        });

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(50));
        assert!(cb.allow().is_ok());
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_retry_policy_exponential() {
        let policy = RetryPolicy {
            strategy: BackoffStrategy::Exponential,
            base_delay_ms: 100,
            max_delay_ms: 1000,
            jitter: false,
            ..Default::default()
        };
        assert_eq!(policy.delay(0).as_millis(), 100);
        assert_eq!(policy.delay(1).as_millis(), 200);
        assert_eq!(policy.delay(2).as_millis(), 400);
        assert_eq!(policy.delay(3).as_millis(), 800);
        assert_eq!(policy.delay(10).as_millis(), 1000);
    }

    #[test]
    fn test_retry_policy_linear() {
        let policy = RetryPolicy {
            strategy: BackoffStrategy::Linear,
            base_delay_ms: 100,
            max_delay_ms: 500,
            jitter: false,
            ..Default::default()
        };
        assert_eq!(policy.delay(0).as_millis(), 100);
        assert_eq!(policy.delay(1).as_millis(), 200);
        assert_eq!(policy.delay(2).as_millis(), 300);
        assert_eq!(policy.delay(5).as_millis(), 500);
    }

    #[test]
    fn test_retry_policy_fixed() {
        let policy = RetryPolicy {
            strategy: BackoffStrategy::Fixed,
            base_delay_ms: 250,
            max_delay_ms: 1000,
            jitter: false,
            ..Default::default()
        };
        assert_eq!(policy.delay(0).as_millis(), 250);
        assert_eq!(policy.delay(5).as_millis(), 250);
    }
}
