//! The usage fold's claim: it keeps up with the clock.
//!
//! The loop has its own liveness reading under `observability_llm_usage_rollup`,
//! and every case here is one the beat cannot distinguish: a beating loop whose
//! newest folded window is hours old. The judgement is written against the
//! watermark rather than the beat, so these cases drive it with a fake clock
//! and a fake store — no database, no tokio timer, no waiting an hour.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use cog_observability::capability::{Capability, Verdict};
use cog_observability::llm_usage_rollup::{
    UsageRollupFreshness, FRESHNESS_CAPABILITY_ID, FRESHNESS_MAX_LAG, WINDOW_SECS,
};

struct Watermark {
    answer: Result<Option<DateTime<Utc>>, String>,
}

#[async_trait::async_trait]
impl cog_observability::capability::WindowWatermark for Watermark {
    async fn newest_window_end(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        match &self.answer {
            Ok(at) => Ok(*at),
            Err(reason) => Err(anyhow::anyhow!(reason.clone())),
        }
    }
}

fn at(hours_ago: i64, now: DateTime<Utc>) -> DateTime<Utc> {
    now - chrono::Duration::hours(hours_ago)
}

fn freshness(
    answer: Result<Option<DateTime<Utc>>, String>,
    now: DateTime<Utc>,
) -> UsageRollupFreshness<Watermark> {
    UsageRollupFreshness::with_clock(Some(Arc::new(Watermark { answer })), Arc::new(move || now))
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 3, 12, 0, 0).unwrap()
}

/// The steady state: the newest folded window closed at most a pass ago.
#[tokio::test]
async fn a_fold_current_within_one_window_is_ok() {
    let judge = freshness(Ok(Some(at(1, now()))), now());
    assert_eq!(judge.judge().await, Verdict::Ok);
}

/// Three windows behind is the failure the beat cannot see: the loop is alive,
/// the fold is not landing. The reason carries both numbers so an operator can
/// tell "late" from "off by a factor" without querying anything else.
#[tokio::test]
async fn a_fold_three_windows_behind_is_red_with_its_numbers() {
    let judge = freshness(Ok(Some(at(3, now()))), now());
    match judge.judge().await {
        Verdict::Red(reason) => {
            assert!(reason.contains("10800"), "{reason}");
            assert!(reason.contains("7200"), "{reason}");
        }
        other => panic!("three windows behind must be red, got {other:?}"),
    }
}

/// The bound itself is inside the bound: a judgement that fires exactly at the
/// bound would make the documented number mean something one second different
/// from what it says.
#[tokio::test]
async fn exactly_at_the_bound_is_ok() {
    let bound = now() - chrono::Duration::seconds(FRESHNESS_MAX_LAG.as_secs() as i64);
    let judge = freshness(Ok(Some(bound)), now());
    assert_eq!(judge.judge().await, Verdict::Ok);
}

/// A fold that never landed a window is not a pass — a deployment that just
/// came up has nothing folded, and saying `Ok` there is the false green this
/// layer exists to remove.
#[tokio::test]
async fn a_fold_that_never_landed_a_window_is_unmeasured() {
    let judge = freshness(Ok(None), now());
    match judge.judge().await {
        Verdict::Unmeasured(reason) => assert!(reason.contains("no window"), "{reason}"),
        other => panic!("no folded window must be unmeasured, got {other:?}"),
    }
}

/// An unreadable watermark is "could not judge", not "stale": the two have
/// different repairs, and firing an age alert during a database outage would
/// send someone to the wrong one.
#[tokio::test]
async fn a_watermark_that_cannot_be_read_is_unmeasured() {
    let judge = freshness(Err("connection refused".into()), now());
    match judge.judge().await {
        Verdict::Unmeasured(reason) => assert!(reason.contains("connection refused"), "{reason}"),
        other => panic!("an unreadable watermark must be unmeasured, got {other:?}"),
    }
}

/// A watermark ahead of the clock is a clock or writer disagreement. Reporting
/// it as fresh would make a skewed writer the quietest configuration.
#[tokio::test]
async fn a_watermark_ahead_of_the_clock_is_not_ok() {
    let judge = freshness(Ok(Some(now() + chrono::Duration::hours(1))), now());
    match judge.judge().await {
        Verdict::Unmeasured(reason) => assert!(reason.contains("future"), "{reason}"),
        other => panic!("a future watermark must not read as ok, got {other:?}"),
    }
}

/// The judgement is asked once a window: a faster cadence cannot produce a new
/// answer, because a window is the unit the answer exists in.
#[tokio::test]
async fn the_judgement_runs_once_per_window() {
    let judge = freshness(Ok(Some(at(1, now()))), now());
    assert_eq!(
        judge.cadence().period_secs(),
        WINDOW_SECS as u64,
        "the cadence published is the window the answer changes on"
    );
    assert_eq!(judge.id(), FRESHNESS_CAPABILITY_ID);
}

/// A process with no ledger still declares the claim. A claim that disappears
/// with its input is a claim nobody can read — and this is exactly the
/// deployment whose fold is not current.
#[tokio::test]
async fn a_process_without_a_ledger_still_declares_and_says_unmeasured() {
    let judge: UsageRollupFreshness<Watermark> = UsageRollupFreshness::with_clock(
        None,
        Arc::new(|| Utc.with_ymd_and_hms(2026, 11, 3, 12, 0, 0).unwrap()),
    );
    assert_eq!(judge.id(), FRESHNESS_CAPABILITY_ID);
    match judge.judge().await {
        Verdict::Unmeasured(reason) => assert!(reason.contains("no usage ledger"), "{reason}"),
        other => panic!("no store must be unmeasured rather than a pass, got {other:?}"),
    }
}

/// The bound is derived from the window, not written twice: a fold that changed
/// its window would otherwise be judged against a number nobody re-derived.
#[test]
fn the_bound_is_two_windows() {
    assert_eq!(
        FRESHNESS_MAX_LAG,
        Duration::from_secs(2 * WINDOW_SECS as u64)
    );
}
