//! What to do when work outlives the wall someone declared for it.
//!
//! The old answer was to kill it. That answer is wrong whenever the thing that
//! overran is still doing the work: a `buildah push` cut at 1800 seconds throws
//! away a push that was about to finish, a `cargo build` cut at its budget
//! throws away every object it had already compiled, a `cargo test` cut at its
//! budget throws away the whole run and reports nothing. The host paid for all
//! of that work; killing it at a round number refunds none of it and costs the
//! next round the whole price again.
//!
//! So a declared wall is an **expectation**, not a knife: passing it publishes
//! one line saying the work is slower than expected, and the work is waited on
//! for as long as it lives. What actually ends it is the thing itself exiting,
//! or the machine losing the process — never a number we picked in advance.
//!
//! This lives in one place because the same control law copied into a dozen
//! files drifts: one copy keeps the warning, another silently drops it, and the
//! next reader cannot tell which behaviour this system has.

use std::future::Future;
use std::time::Duration;

/// How often the waiter wakes. It only exists so the completion of the inner
/// future can be raced against a clock; it is not a wall.
// cap: bound — 只排期、从不取消任何东西：进度由内层 future 决定，这个数只定回来看一眼的节拍
pub const POLL_EVERY: Duration = Duration::from_secs(15);

/// 轮询间隔的测试覆盖，0 表示用 [`POLL_EVERY`]。
///
/// 与节拍本身同一个理由：一个测试要观察「等待期间心跳有没有动」，就得让轮询快过
/// 测试自己的寿命，否则它只能在十秒级的节拍上干等。生产路径一字不改。
static POLL_OVERRIDE_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn poll_every() -> Duration {
    let ms = POLL_OVERRIDE_MS.load(std::sync::atomic::Ordering::Relaxed);
    if ms == 0 {
        POLL_EVERY
    } else {
        Duration::from_millis(ms)
    }
}

/// 测试钩子：把轮询间隔改短。传 [`Duration::ZERO`] 恢复生产值。
#[doc(hidden)]
pub fn set_poll_every_for_tests(poll: Duration) {
    POLL_OVERRIDE_MS.store(
        poll.as_millis() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// Once the declared wall is behind us, how often to say so again. A build that
/// has been running for three hours is a different fact from one that ran over
/// by a minute, and the first warning alone would read as stale — the same
/// reason any measured figure needs a freshness rule.
// cap: bound — 只决定超墙之后重复说一句的间隔，从不取消任何东西
pub const REPEAT_EVERY: Duration = Duration::from_secs(300);

/// Await `fut` to completion, warning when it outlives `declared`.
///
/// `declared` of zero means the caller declared nothing: the future is simply
/// awaited and no line is published.
pub async fn wait_past_wall<F>(declared: Duration, what: &str, fut: F) -> F::Output
where
    F: Future,
{
    wait_past_wall_beating(declared, what, || {}, fut).await
}

/// The same, with a beat callback run on every poll.
///
/// Long waits are observed by loops that treat silence as a stall (the build
/// gate's claim renewal, the deployer's round beat). A wait that stops calling
/// back reads as a dead process to every one of them, so the caller passes the
/// same beat it would have made had it not been waiting.
pub async fn wait_past_wall_beating<F, B>(
    declared: Duration,
    what: &str,
    beat: B,
    fut: F,
) -> F::Output
where
    F: Future,
    B: FnMut(),
{
    wait_with_poll(poll_every(), declared, what, beat, fut).await
}

/// The loop itself, with the poll period injected so tests do not have to sit
/// through a production cadence to observe it.
async fn wait_with_poll<F, B>(
    poll: Duration,
    declared: Duration,
    what: &str,
    mut beat: B,
    fut: F,
) -> F::Output
where
    F: Future,
    B: FnMut(),
{
    tokio::pin!(fut);
    let started = std::time::Instant::now();
    let mut said_so = false;
    let mut next_say = REPEAT_EVERY;
    loop {
        tokio::select! {
            out = &mut fut => return out,
            _ = tokio::time::sleep(poll) => {
                beat();
                if declared.is_zero() {
                    continue;
                }
                let elapsed = started.elapsed();
                if !said_so || elapsed >= next_say {
                    said_so = true;
                    while elapsed >= next_say {
                        next_say += REPEAT_EVERY;
                    }
                    tracing::warn!(
                        what,
                        declared_secs = declared.as_secs(),
                        elapsed_secs = elapsed.as_secs(),
                        "work has outlived the wall declared for it and is still running; waiting for it instead of killing work in progress"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape the whole module exists for: work that runs past its declared
    /// wall finishes, and its own output comes back. A killer would have
    /// returned neither.
    #[tokio::test]
    async fn work_that_outlives_its_wall_is_waited_on_not_killed() {
        let started = std::time::Instant::now();
        let got = wait_past_wall(Duration::from_millis(10), "test work", async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            "finished on its own"
        })
        .await;
        assert_eq!(got, "finished on its own");
        assert!(
            started.elapsed() >= Duration::from_millis(120),
            "returned after {:?}: the work was not waited on",
            started.elapsed()
        );
    }

    /// A caller that declared nothing gets no warning and the same answer.
    #[tokio::test]
    async fn no_declared_wall_just_means_wait() {
        let got = wait_past_wall(Duration::ZERO, "no wall", async { 7 }).await;
        assert_eq!(got, 7);
    }

    /// The beat runs while waiting, which is what keeps a long wait from
    /// reading as a stalled process.
    #[tokio::test]
    async fn the_beat_runs_while_we_wait() {
        let beats = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let b = beats.clone();
        let _ = wait_with_poll(
            Duration::from_millis(10),
            Duration::ZERO,
            "beating work",
            move || {
                b.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            async {
                tokio::time::sleep(Duration::from_millis(45)).await;
            },
        )
        .await;
        assert!(
            beats.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "the beat never ran during a wait longer than two polls"
        );
    }
}
