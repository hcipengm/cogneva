//! What happens when the token ledger cannot be written.
//!
//! The insert is spawned rather than awaited: a caller whose stream has already
//! finished must not wait on PostgreSQL. The price of that is the one failure in
//! this pipeline that leaves no reading of its own. The hourly fold is computed
//! from these rows, so a row that never landed is not a late number — it is
//! spend that no window will ever count, and the hour reads exactly like an hour
//! with that many fewer calls. Nothing downstream can tell the difference, which
//! is why the loss has to be answered for at the write itself.
//!
//! Two things do that. The write is retried a few times with backoff, because
//! what fails here in practice is a restart or a connection reset, and those
//! clear within a second; a spawn that gives up on the first error turns a
//! sub-second PG hiccup into permanently missing spend. And a streak of failures
//! drives a durable alert through the same sink the pool alert uses, resolved by
//! the first write that lands — a metering outage is a thing someone has to be
//! told about while it is happening, not something to be inferred later from an
//! hour that looks quiet.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cog_core::alerts::{ALERT_FACE_LABEL, ALERT_LABEL_VALUE_MAX_CHARS};
use cog_core::PersistentAlertDraft;

/// The rule a metering write outage fires under. One rule, one row: the
/// condition is "the ledger cannot be written", not one row per lost write.
pub(crate) const WRITE_FAILED_RULE: &str = "llm_usage_ledger_write_failed";

/// Who is called when metering stops landing. The pool alert is filed under the
/// same face and for the same reason: the thing to change is the metering
/// path's own configuration or its database, not a reader.
const WRITE_FAILED_FACE: &str = "token 管理";

/// How many times one row is offered to the store before the loss is declared.
///
/// Three, not more: this is a detached task, so extra attempts cost nothing on
/// the request path, but a store that is down for a minute fails every attempt
/// of every caller anyway — the retries are for the transient case, and the
/// alert is for the other one.
// cap: bound — 落库失败的重试次数，界的是写丢失的概率与请求耗时的折中
pub(crate) const WRITE_ATTEMPTS: u32 = 3;

/// Whether a row has started failing to land, so that only the edges are acted
/// on.
///
/// The alert sink is durable and idempotent, but driving it on every write
/// would put a PG round trip behind every successful call. The streak is the
/// latch: the first failure in a run fires, the first success after it
/// resolves, and every write in between costs one atomic swap.
#[derive(Debug, Default)]
pub struct WriteHealth {
    failing: AtomicBool,
}

impl WriteHealth {
    /// Record a write that landed. `true` means it is the one that ends a
    /// failing streak — the only success the alert has to be resolved on.
    pub fn note_landed(&self) -> bool {
        self.failing.swap(false, Ordering::SeqCst)
    }

    /// Record a write that did not land after every attempt. `true` means the
    /// streak just started, which is when the alert fires.
    pub fn note_lost(&self) -> bool {
        !self.failing.swap(true, Ordering::SeqCst)
    }
}

/// The alert row this module drives, for whichever direction the edge goes.
///
/// The dedup key is the rule itself: there is one condition, and re-deriving a
/// per-event identity would open a new row per lost write while the resolve
/// closed only the last one.
pub(crate) fn write_failed_draft(message: String) -> PersistentAlertDraft {
    PersistentAlertDraft {
        rule: WRITE_FAILED_RULE.into(),
        dedup_key: WRITE_FAILED_RULE.into(),
        severity: "critical".into(),
        message,
        labels: serde_json::json!({ ALERT_FACE_LABEL: WRITE_FAILED_FACE }),
    }
}

/// The message a loss streak carries. It names the consequence, because the
/// person reading it has to know that an hour which looks quiet is not quiet.
pub(crate) fn write_failed_message(attempts: u32, last_error: &str) -> String {
    let tail: String = last_error
        .chars()
        .take(ALERT_LABEL_VALUE_MAX_CHARS)
        .collect();
    format!(
        "LLM 计量明细写不进账本：每行试了 {attempts} 次仍失败，这一段的花销不会出现在任何窗口里，而它读起来与「没人调用」完全一样。最后一次错误：{tail}"
    )
}

/// How long the first wait before a retry is.
///
/// A fifth of a second: long enough to ride out a connection reset, short
/// enough that a store which is really down is declared in the same second
/// rather than holding a task open behind it.
// cap: bound — 指数退避的基数，第 n 次重试等 base*2^n
const BACKOFF_BASE_MS: u64 = 200;

/// How long to wait before the next attempt, by the attempt that just failed.
///
/// Doubling from [`BACKOFF_BASE_MS`], so three attempts span about a second.
/// The shift is capped so a caller that asks for many attempts spends its
/// budget on a store that might come back rather than on one that will not.
fn backoff_before(attempt: u32) -> Duration {
    Duration::from_millis(BACKOFF_BASE_MS << attempt.min(4))
}

/// Offer one write to the store until it lands or the attempts run out.
///
/// Returns the number of attempts the success took, or the last error together
/// with the attempts spent.
pub(crate) async fn persist_with_retry<T, E, F, Fut>(
    attempts: u32,
    mut write: F,
) -> Result<(T, u32), (E, u32)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let attempts = attempts.max(1);
    let mut last = None;
    for attempt in 0..attempts {
        match write().await {
            Ok(value) => return Ok((value, attempt + 1)),
            Err(error) => {
                last = Some(error);
                // No sleep after the last attempt: the wait would be spent
                // after the decision to give up was already made.
                if attempt + 1 < attempts {
                    tokio::time::sleep(backoff_before(attempt)).await;
                }
            }
        }
    }
    Err((last.expect("at least one attempt ran"), attempts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test(start_paused = true)]
    async fn a_write_that_fails_twice_and_lands_third_is_not_a_loss() {
        let calls = Cell::new(0);
        let result = persist_with_retry(WRITE_ATTEMPTS, || {
            calls.set(calls.get() + 1);
            let n = calls.get();
            async move {
                if n < 3 {
                    Err("connection reset")
                } else {
                    Ok("landed")
                }
            }
        })
        .await;
        assert_eq!(result, Ok(("landed", 3)), "三次里的第三次落地");
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_that_never_lands_reports_the_attempts_and_the_last_error() {
        let result =
            persist_with_retry::<(), &str, _, _>(WRITE_ATTEMPTS, || async { Err("store is down") })
                .await;
        assert_eq!(
            result,
            Err(("store is down", WRITE_ATTEMPTS)),
            "每一次都试过，报告的是最后一次的错误"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_attempt_is_still_one_attempt() {
        let result = persist_with_retry::<(), &str, _, _>(0, || async { Err("boom") }).await;
        assert_eq!(result, Err(("boom", 1)), "零次尝试读起来像「没量过」");
    }

    #[test]
    fn the_backoff_grows_and_stops_growing_at_the_cap() {
        assert_eq!(backoff_before(0), Duration::from_millis(200));
        assert_eq!(backoff_before(1), Duration::from_millis(400));
        assert_eq!(
            backoff_before(9),
            Duration::from_millis(3200),
            "指数项封顶之后不再翻倍"
        );
    }

    #[test]
    fn only_the_edge_of_a_streak_is_a_transition() {
        let health = WriteHealth::default();
        assert!(health.note_lost(), "第一笔丢失要开一条告警");
        assert!(!health.note_lost(), "同一段故障里第二笔不再开第二条");
        assert!(!health.note_lost());
        assert!(health.note_landed(), "第一笔落地要把它关掉");
        assert!(
            !health.note_landed(),
            "恢复之后每一笔都去写一次告警就是噪声"
        );
        assert!(health.note_lost(), "再次丢失是新的那一段");
    }

    #[test]
    fn the_message_says_what_the_loss_looks_like_from_outside() {
        let message =
            write_failed_message(WRITE_ATTEMPTS, "pq: the database system is starting up");
        assert!(
            message.contains("与「没人调用」完全一样"),
            "读者要能从这句话知道安静的小时不是安静的：{message}"
        );
        assert!(message.contains("starting up"), "最后一次错误要带出去");
    }
}
