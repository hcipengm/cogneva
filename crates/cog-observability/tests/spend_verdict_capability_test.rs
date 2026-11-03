//! "The spend projection keeps up with the clock", judged from its watermark.
//!
//! The loop's own liveness reading cannot answer this: a loop that ticks and
//! cannot write beats on time forever while the projection stands still. These
//! cases pin the four states a watermark can be in and the code each one
//! exports, because the difference between them is the difference between
//! "restart the loop" and "fix what it cannot write".

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use cog_observability::capability::{Capability, Verdict};
use cog_observability::spend_verdict::{
    SpendVerdictFreshness, WindowWatermark, FRESHNESS_CAPABILITY_ID, FRESHNESS_MAX_LAG,
};

struct Watermark {
    answer: anyhow::Result<Option<DateTime<Utc>>>,
}

#[async_trait::async_trait]
impl WindowWatermark for Watermark {
    async fn newest_window_end(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        match &self.answer {
            Ok(v) => Ok(*v),
            Err(e) => Err(anyhow::anyhow!("{e}")),
        }
    }
}

fn at(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).single().expect("timestamp")
}

fn freshness(
    answer: anyhow::Result<Option<DateTime<Utc>>>,
    now: i64,
) -> SpendVerdictFreshness<Watermark> {
    SpendVerdictFreshness::with_clock(
        Some(Arc::new(Watermark { answer })),
        Arc::new(move || at(now)),
    )
}

/// A process that built no store still declares the claim: the deployment is
/// told the projection is not current rather than being told nothing. Deleting
/// the claim when the input is missing is what makes a face disappear exactly
/// when it matters.
#[tokio::test]
async fn a_process_without_a_store_still_declares_and_says_unmeasured() {
    let c: SpendVerdictFreshness<Watermark> =
        SpendVerdictFreshness::with_clock(None, Arc::new(|| at(1_000_000)));
    assert_eq!(c.id(), FRESHNESS_CAPABILITY_ID);
    match c.judge().await {
        Verdict::Unmeasured(reason) => {
            assert!(reason.contains("no spend verdict store"), "{reason}")
        }
        other => panic!("expected unmeasured, got {other:?}"),
    }
}

/// Steady state: the newest judged window ended one window ago, which is one
/// full window inside the bound.
#[tokio::test]
async fn a_window_judged_one_window_ago_is_ok() {
    let now = 1_000_000;
    let c = freshness(Ok(Some(at(now - 3600))), now);
    assert_eq!(c.id(), FRESHNESS_CAPABILITY_ID);
    assert!(matches!(c.judge().await, Verdict::Ok));
}

/// The failure this capability exists for: the loop is up, the projection is
/// hours behind, and the verdict says so with both numbers.
#[tokio::test]
async fn a_projection_three_windows_behind_is_red_with_its_numbers() {
    let now = 1_000_000;
    let c = freshness(Ok(Some(at(now - 3 * 3600))), now);
    match c.judge().await {
        Verdict::Red(reason) => {
            assert!(reason.contains("10800"), "{reason}");
            assert!(reason.contains("7200"), "{reason}");
        }
        other => panic!("expected red, got {other:?}"),
    }
}

/// Exactly at the bound is inside it. A bound that reddens on its own value
/// reddens on a busy-but-working system, and an alert nobody can clear is one
/// nobody reads.
#[tokio::test]
async fn exactly_at_the_bound_is_ok() {
    let now = 1_000_000;
    let c = freshness(Ok(Some(at(now - FRESHNESS_MAX_LAG.as_secs() as i64))), now);
    assert!(matches!(c.judge().await, Verdict::Ok));
}

/// A projection that has never judged a window is not a pass — and not a red
/// either: a deployment that just came up has nothing to be behind. Unmeasured
/// is the only code that says both.
#[tokio::test]
async fn a_projection_that_never_judged_is_unmeasured() {
    let c = freshness(Ok(None), 1_000_000);
    match c.judge().await {
        Verdict::Unmeasured(reason) => assert!(reason.contains("no window"), "{reason}"),
        other => panic!("expected unmeasured, got {other:?}"),
    }
}

/// "I could not read the watermark" is unmeasured, never ok. This is the
/// database being down: the projection is not current, and the operator must
/// not read silence as a pass.
#[tokio::test]
async fn a_watermark_that_cannot_be_read_is_unmeasured() {
    let c = freshness(Err(anyhow::anyhow!("connection refused")), 1_000_000);
    match c.judge().await {
        Verdict::Unmeasured(reason) => assert!(reason.contains("connection refused"), "{reason}"),
        other => panic!("expected unmeasured, got {other:?}"),
    }
}

/// A watermark in the future is a clock or writer disagreement. Folding it
/// into ok would make a skewed writer the quietest possible configuration.
#[tokio::test]
async fn a_watermark_ahead_of_the_clock_is_not_ok() {
    let now = 1_000_000;
    let c = freshness(Ok(Some(at(now + 600))), now);
    match c.judge().await {
        Verdict::Unmeasured(reason) => assert!(reason.contains("future"), "{reason}"),
        other => panic!("expected unmeasured, got {other:?}"),
    }
}

/// The cadence is the window, not something faster: asking twice inside one
/// window cannot produce a new answer.
#[tokio::test]
async fn the_judgement_runs_once_per_window() {
    let c = freshness(Ok(None), 1_000_000);
    match c.cadence() {
        cog_core::loop_health::Cadence::Periodic(d) => {
            assert_eq!(d, Duration::from_secs(3600))
        }
        other => panic!("expected a periodic cadence, got {other:?}"),
    }
}
