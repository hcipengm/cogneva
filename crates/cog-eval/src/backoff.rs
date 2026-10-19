//! 调用方侧的退避：上游说了等多久就等多久，没说就等一个有界的值。
//!
//! 为什么需要：网关在池级熔断与窗口配速两处都给了「下次几点再来」
//! （`retry_after_seconds`），可调用侧一直是拿回错误就立刻换下一题。
//! 2026-10-19T01:23Z 那一刻，一个窗口翻页造成的四分钟黑障被这种紧循环放大成
//! 一分钟 455 条拒绝——**请求没有变多，是重试没有间隔**。
//!
//! 三条界都是**调用方自己**的，不改网关语义：
//!   * 读到提示就睡到那个时刻。提示可能只以文本形式剩下（错误在层与层之间被压平成
//!     字符串），所以既读类型化字段也读渲染，读法与产生它的那一处共用同一个记号。
//!   * 读不到提示也**不许立即重试**：失败之后至少隔 [`DEFAULT_REFUSAL_BACKOFF`]。
//!   * 同一个调用方一分钟内的尝试次数有上界 [`ATTEMPTS_PER_MINUTE`]：这一条防的不是
//!     单次退避，而是「很多短促的尝试接连发生」——每一次都合规，合起来仍是风暴。
//!
//! 界是**每一次调用前**问的（`before_call`），所以并发跑的那些任务会一起等：
//! 一个任务撞上的拒绝，也是其余任务下一次上游调用的时刻。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cog_core::contract::llm::retry_after_hint_in;
use cog_core::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, LlmClient, Message,
    SFResult,
};

/// 没读到提示时，两次上游尝试之间的最短间隔。
///
/// 一秒不是拍出来的：它比一次快速失败（实测 0.7 ms）长三个数量级，足以把
/// 「一秒钟灌进几百条」压回「一秒钟一条」。比它更长的值会把一次正常的重跑也拖慢，
/// 而拒绝本来就是少数时刻。
pub const DEFAULT_REFUSAL_BACKOFF: Duration = Duration::from_secs(1);

/// 同一个调用方一分钟内最多发起多少次尝试。
///
/// 单次退避管不住「很多任务同时失败」：每一个都遵守自己那一秒的间隔，合起来仍是
/// 一分钟上千次。这个数是**每一次调用都要过的一道闸**，与单次退避叠加。
/// 10 次/秒 比正常评测的调用密度（现网一小时几十到几百次）高两个数量级，所以它
/// 不是正常路径的节流阀；它只在拒绝风暴里咬合。
pub const ATTEMPTS_PER_MINUTE: usize = 600;

/// 一跑里调用侧退避的界，以及它咬合了多少次。随表一起出（`Table::backoff`）。
///
/// 只印界不印次数，「装了退避」与「退避从没生效过」在产物上同形；只印次数不印界，
/// 读的人没法判断这个次数是撞上了多长的一次等待。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BackoffReading {
    pub attempts_per_minute: usize,
    pub default_wait_secs: u64,
    pub refusals: u64,
    pub waited_secs: u64,
}

/// 调用侧共享的退避状态。
#[derive(Debug)]
pub struct RefusalBackoff {
    /// 早于这一刻的调用不许发生。`note_refusal` 把它往后推，多个任务读到同一个值。
    next_allowed: Mutex<Option<Instant>>,
    /// 最近一分钟内的尝试时刻，用于每分钟上界。
    attempts: Mutex<VecDeque<Instant>>,
    per_minute: usize,
    window: Duration,
    /// 被推迟过多少次、累计推迟了多少秒。读数用：没有它，「装了退避」与「退避从没
    /// 生效过」在表上同形。
    refusals: AtomicU64,
    waited_secs: AtomicU64,
}

impl RefusalBackoff {
    pub fn new(per_minute: usize) -> Self {
        Self::with_window(per_minute, Duration::from_secs(60))
    }

    /// 同 [`Self::new`]，但窗口可换。只给测试用：真实调用的窗口就是一分钟，
    /// 换窗口等于把「每分钟」这三个字悄悄改成别的单位。
    pub fn with_window(per_minute: usize, window: Duration) -> Self {
        Self {
            next_allowed: Mutex::new(None),
            attempts: Mutex::new(VecDeque::new()),
            per_minute,
            window,
            refusals: AtomicU64::new(0),
            waited_secs: AtomicU64::new(0),
        }
    }

    /// 记下一次「上游说现在不行」，返回被推迟到多久之后。
    ///
    /// `stated` 是类型化字段（错误对象或响应体自带的秒数）；没有就回到文本里找。
    /// 两处都读不到，用 [`DEFAULT_REFUSAL_BACKOFF`]——**不是零**：
    /// 「没说要等」不等于「可以立刻再试」，这两件事只有一个能做。
    /// 提示可能很大（熔断器说的几千秒），这里不截断：那是网关自己的封顶已经算过的数，
    /// 再截一次等于替上游改口。
    pub fn note_refusal(&self, stated: Option<u64>, detail: &str) -> Duration {
        let wait = stated
            .or_else(|| retry_after_hint_in(detail))
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_REFUSAL_BACKOFF);
        let mut next = self.next_allowed.lock().unwrap();
        let candidate = Instant::now() + wait;
        if next.is_none_or(|current| current < candidate) {
            *next = Some(candidate);
        }
        self.refusals.fetch_add(1, Ordering::Relaxed);
        self.waited_secs
            .fetch_add(wait.as_secs(), Ordering::Relaxed);
        wait
    }

    /// 等到允许发下一次调用，并把这一次记进窗口。
    ///
    /// 等待在**循环里重算**：睡着的这段时间里可能有别的任务把 `next_allowed` 又往后
    /// 推了，醒来不复核就等于按一个已经过期的时刻发请求。
    pub async fn before_call(&self) {
        loop {
            match self.wait_now() {
                Some(d) if !d.is_zero() => tokio::time::sleep(d).await,
                _ => {
                    self.record_attempt();
                    return;
                }
            }
        }
    }

    /// 这一跑装的是哪几条界、它们咬合了多少次。
    pub fn reading(&self) -> BackoffReading {
        let (refusals, waited_secs) = self.refusals_seen();
        BackoffReading {
            attempts_per_minute: self.per_minute,
            default_wait_secs: DEFAULT_REFUSAL_BACKOFF.as_secs(),
            refusals,
            waited_secs,
        }
    }

    /// 被推迟过多少次、累计推迟了多少秒（读数：表里那一行据此判断这条界有没有真咬合）。
    pub fn refusals_seen(&self) -> (u64, u64) {
        (
            self.refusals.load(Ordering::Relaxed),
            self.waited_secs.load(Ordering::Relaxed),
        )
    }

    /// 此刻还要等多久才能发下一次调用。`None` ＝立刻可以。
    fn wait_now(&self) -> Option<Duration> {
        let now = Instant::now();
        let by_hint = self
            .next_allowed
            .lock()
            .unwrap()
            .and_then(|deadline| deadline.checked_duration_since(now));
        let mut attempts = self.attempts.lock().unwrap();
        while let Some(oldest) = attempts.front() {
            if now.duration_since(*oldest) >= self.window {
                attempts.pop_front();
            } else {
                break;
            }
        }
        let by_rate = if attempts.len() >= self.per_minute {
            attempts
                .front()
                .map(|oldest| (*oldest + self.window).saturating_duration_since(now))
                .filter(|d| !d.is_zero())
        } else {
            None
        };
        match (by_hint, by_rate) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    fn record_attempt(&self) {
        let now = Instant::now();
        let mut attempts = self.attempts.lock().unwrap();
        while let Some(oldest) = attempts.front() {
            if now.duration_since(*oldest) >= self.window {
                attempts.pop_front();
            } else {
                break;
            }
        }
        attempts.push_back(now);
    }
}

/// 给任何 `LlmClient` 套上 [`RefusalBackoff`]。
///
/// 为什么包在客户端而不是包在调用点：一次评测里真正在打上游的是外壳内部的循环
/// （一道题可能问很多次），调用点在外面只能看到「这一题失败了」，看不到中间每一次。
/// 包在最里层，则每一次真正的调用都过同一道闸——包括它自己的重试。
pub struct BackoffLlm {
    inner: Arc<dyn LlmClient>,
    backoff: Arc<RefusalBackoff>,
}

impl BackoffLlm {
    pub fn new(inner: Arc<dyn LlmClient>, backoff: Arc<RefusalBackoff>) -> Self {
        Self { inner, backoff }
    }

    /// 一次已经回来的响应里是不是带着拒绝。
    ///
    /// 后端不可服务时**仍会以 `Ok` 回来**（原因在 `error_message` 里、正文为空），
    /// 所以只看 `Err` 会漏掉整整一半的拒绝：漏掉的那一半不会让退避不生效，
    /// 但会让它退到一个不知道要等多久的默认值上。
    fn note_if_refused(&self, response: &ChatResponse) {
        if response.error_message.is_none() && response.retry_after_secs.is_none() {
            return;
        }
        let stated = response.retry_after_secs.or_else(|| {
            response
                .error_message
                .as_deref()
                .and_then(retry_after_hint_in)
        });
        let detail = response.error_message.as_deref().unwrap_or_default();
        self.backoff.note_refusal(stated, detail);
    }
}

#[async_trait::async_trait]
impl LlmClient for BackoffLlm {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.backoff.before_call().await;
        let stream = self.inner.chat_stream(messages, options).await;
        if let Err(e) = &stream {
            self.backoff
                .note_refusal(e.retry_after_secs(), &e.to_string());
        }
        // 流本身里的失败事件读不到（那是消费方的事），这一条只保证「发出去之前等够了」。
        stream
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.backoff.before_call().await;
        let stream = self.inner.complete_stream(prompt, options).await;
        if let Err(e) = &stream {
            self.backoff
                .note_refusal(e.retry_after_secs(), &e.to_string());
        }
        stream
    }

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        self.backoff.before_call().await;
        match self.inner.chat(messages, options).await {
            Ok(response) => {
                self.note_if_refused(&response);
                Ok(response)
            }
            Err(e) => {
                self.backoff
                    .note_refusal(e.retry_after_secs(), &e.to_string());
                Err(e)
            }
        }
    }

    async fn health_check(&self) -> bool {
        // 健康检查不是「调用」，不进每分钟配额：它存在的理由就是在不打扰上游的前提下
        // 问一句「在不在」。
        self.inner.health_check().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{SFError, UpstreamFailure};
    use std::sync::Mutex;

    /// 提示要**真的**被等到：断言的是实际发生的时刻差，不是「记了一个数」。
    #[tokio::test]
    async fn a_stated_wait_is_waited_out_before_the_next_call() {
        let backoff = RefusalBackoff::new(ATTEMPTS_PER_MINUTE);
        let applied = backoff.note_refusal(Some(1), "");
        assert_eq!(applied, Duration::from_secs(1));
        let before = Instant::now();
        backoff.before_call().await;
        assert!(
            before.elapsed() >= Duration::from_secs(1),
            "the next call must not happen before the wait the upstream stated, waited {:?}",
            before.elapsed()
        );
    }

    /// 读不到提示时也**不许立即重试**：这是有界退避，不是「没有退避」。
    #[tokio::test]
    async fn a_refusal_with_no_stated_wait_still_waits() {
        let backoff = RefusalBackoff::new(ATTEMPTS_PER_MINUTE);
        let applied = backoff.note_refusal(None, "API error (HTTP 503): no hint here");
        assert_eq!(applied, DEFAULT_REFUSAL_BACKOFF);
        let before = Instant::now();
        backoff.before_call().await;
        assert!(
            before.elapsed() >= DEFAULT_REFUSAL_BACKOFF,
            "a refusal without a hint still needs a floor, waited {:?}",
            before.elapsed()
        );
    }

    /// 每分钟上界对**很多次短促的成功尝试**也生效：单次退避对它们各自都没话说。
    /// 窗口缩到 200ms 以便这个用例能在一次抖动里跑完——被测的是算术，不是秒数。
    #[tokio::test]
    async fn the_attempt_window_bounds_a_burst() {
        let window = Duration::from_millis(200);
        let backoff = RefusalBackoff::with_window(2, window);
        backoff.before_call().await;
        backoff.before_call().await;
        let before = Instant::now();
        backoff.before_call().await;
        assert!(
            before.elapsed() >= window.mul_f32(0.8),
            "the third attempt inside the window must wait for the first to age out, waited {:?}",
            before.elapsed()
        );
    }

    /// 后来的短提示不能撤掉先前的长提示：界取大的那个。
    #[tokio::test]
    async fn a_later_short_hint_does_not_reset_a_long_one() {
        let backoff = RefusalBackoff::new(ATTEMPTS_PER_MINUTE);
        backoff.note_refusal(Some(1), "");
        backoff.note_refusal(Some(0), "");
        let before = Instant::now();
        backoff.before_call().await;
        assert!(
            before.elapsed() >= Duration::from_secs(1),
            "a later zero must not undo the deadline an earlier refusal set, waited {:?}",
            before.elapsed()
        );
    }

    /// 夹具：第一次调用是 429＋`retry_after_secs=1`，第二次必须发生在它之后。
    /// 断言的是两次 `chat` 的真实时刻差——中间隔着一个包装层，这一层要是把等待丢了，
    /// 这个用例当场红。
    struct RefusingOnce {
        calls: Mutex<Vec<Instant>>,
    }

    #[async_trait::async_trait]
    impl LlmClient for RefusingOnce {
        async fn chat(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            let mut calls = self.calls.lock().unwrap();
            calls.push(Instant::now());
            if calls.len() == 1 {
                return Err(SFError::upstream_refused_after(
                    UpstreamFailure::RateLimited,
                    "429 too many requests",
                    Some(1),
                ));
            }
            Ok(ChatResponse::default())
        }

        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            unimplemented!("this fixture is only called through `chat`")
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &CompleteOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            unimplemented!("this fixture is only called through `chat`")
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn the_caller_waits_the_stated_retry_after_before_trying_again() {
        let inner = Arc::new(RefusingOnce {
            calls: Mutex::new(Vec::new()),
        });
        let backoff = Arc::new(RefusalBackoff::new(ATTEMPTS_PER_MINUTE));
        let llm = BackoffLlm::new(inner.clone(), backoff.clone());

        let first = llm.chat(&[], &ChatOptions::default()).await;
        assert!(first.is_err(), "the fixture's first call must be refused");
        let second = llm.chat(&[], &ChatOptions::default()).await;
        assert!(second.is_ok(), "the fixture's second call must succeed");

        let calls = inner.calls.lock().unwrap();
        let gap = calls[1].duration_since(calls[0]);
        assert!(
            gap >= Duration::from_secs(1),
            "the second call must happen after the stated retry_after, gap was {gap:?}"
        );
        assert_eq!(
            backoff.refusals_seen(),
            (1, 1),
            "one refusal and one second of wait must be counted for the reading"
        );
    }

    /// 一个**只有 prose** 的拒绝也要被读到：中间层把错误压成字符串之后，
    /// 提示只剩渲染文本，包装层只读类型化字段就会退到默认值。
    #[tokio::test]
    async fn a_hint_that_survives_only_as_text_is_still_read() {
        struct TextOnly {
            calls: Mutex<Vec<Instant>>,
        }

        #[async_trait::async_trait]
        impl LlmClient for TextOnly {
            async fn chat(
                &self,
                _messages: &[Message],
                _options: &ChatOptions,
            ) -> SFResult<ChatResponse> {
                let mut calls = self.calls.lock().unwrap();
                calls.push(Instant::now());
                if calls.len() == 1 {
                    return Err(SFError::LLM(format!(
                        "wrapper flattened this: refused {}",
                        cog_core::contract::llm::render_retry_after(1)
                    )));
                }
                Ok(ChatResponse::default())
            }

            async fn chat_stream(
                &self,
                _messages: &[Message],
                _options: &ChatOptions,
            ) -> SFResult<AssistantMessageEventStream> {
                unimplemented!("this fixture is only called through `chat`")
            }

            async fn complete_stream(
                &self,
                _prompt: &str,
                _options: &CompleteOptions,
            ) -> SFResult<AssistantMessageEventStream> {
                unimplemented!("this fixture is only called through `chat`")
            }

            async fn health_check(&self) -> bool {
                true
            }
        }

        let inner = Arc::new(TextOnly {
            calls: Mutex::new(Vec::new()),
        });
        let backoff = Arc::new(RefusalBackoff::new(ATTEMPTS_PER_MINUTE));
        let llm = BackoffLlm::new(inner.clone(), backoff.clone());

        let _ = llm.chat(&[], &ChatOptions::default()).await;
        let _ = llm.chat(&[], &ChatOptions::default()).await;

        let calls = inner.calls.lock().unwrap();
        assert!(
            calls[1].duration_since(calls[0]) >= Duration::from_secs(1),
            "a hint that only survives as text must still be honoured"
        );
    }
}
