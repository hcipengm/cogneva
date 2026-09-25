//! How many generation rounds one CI failure is worth.
//!
//! A landing that breaks CI is reverted and answered by re-driving generation:
//! a fix task built from the failure log. That path had no budget of any kind,
//! and the cost of a round is a full generation plus a full CI run. What made
//! it unbounded is that the per-change flag only stops the *second* re-drive of
//! the *same* change: the re-drive's own change is a new change with its own
//! flag, so a fix that also breaks CI opens a fresh round, and the chain grows
//! as long as the generator keeps producing a revision that lands and then
//! fails.
//!
//! The identity that bounds it is the **cause**, not the change. Two rounds
//! are the same round when they are chasing the same CI failure, and the only
//! durable description of a CI failure available here is its log — the record
//! of the change that failed is removed once its landing is settled, so the
//! signature has to be taken while the failure is in hand.
//!
//! Rounding the ledger on the cause also states what is *not* bounded: a round
//! chasing a cause that has not been seen before is new information about the
//! code, and refusing it would be refusing to learn. That is the same reason
//! the deployer advances past a failure on a new revision. The total burn is
//! therefore bounded per cause and per window, not in total.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

/// Re-drives that were refused, by reason. A refused re-drive is the system
/// deciding to stop generating against a failure, which is a decision the
/// reading surface has to carry: without this, "nothing is being generated"
/// and "generation was switched off for this cause" look the same from
/// outside.
pub use cog_core::metric_names::REDRIVE_REFUSALS_TOTAL as REDRIVE_REFUSALS_METRIC;

/// Why a re-drive was refused.
///
/// Closed set: the metric labels it, and a reason nobody named would be
/// counted under a label that lies about what it means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedriveRefusal {
    /// The failure did not come with a log to generate against. A fix task
    /// built on "(logs unavailable)" is a guess that costs a full round, so
    /// the absence of evidence is answered by not spending one.
    NoEvidence,
    /// This cause has already used up its rounds for the window. Another round
    /// would be the same attempt at the same failure.
    CauseExhausted,
}

impl RedriveRefusal {
    /// Every reason, so a gate can check that each one reaches a rule.
    pub const ALL: [Self; 2] = [Self::NoEvidence, Self::CauseExhausted];

    /// The metric label. Stable: alert rules read these.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoEvidence => "no_evidence",
            Self::CauseExhausted => "cause_exhausted",
        }
    }
}

/// Round charge lost, by which side of the ledger lost it.
///
/// The ledger is what makes the budget mean the same thing twice, so losing it
/// is losing the bound: a read that failed grants the rounds a fresh ledger
/// would, and a write that failed leaves a round uncharged so the same cause
/// can buy another one after a restart. Both directions end in a budget that
/// quietly stopped applying, which is why they are counted, and they are kept
/// apart because the two repairs are different.
pub use cog_core::metric_names::REDRIVE_BUDGET_LOSSES_TOTAL as REDRIVE_BUDGET_LOSSES_METRIC;

/// Which half of the ledger lost a charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetSide {
    /// The ledger could not be read, so its rounds were forgotten.
    Read,
    /// The ledger could not be written, so this round was not charged.
    Write,
}

impl BudgetSide {
    /// Every side, so a gate can check that each one reaches a rule.
    pub const ALL: [Self; 2] = [Self::Read, Self::Write];

    /// The metric label. Stable: alert rules read these.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// What to do about a red CI verdict on a landed change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedriveDecision {
    /// Generate a fix from this failure, and charge it to this cause. The
    /// signature is what the round is charged to, so the caller does not have
    /// to read the log a second time to find out what it paid for.
    Spend {
        /// The cause the round is charged to.
        signature: String,
    },
    /// Do not generate; the reason is what gets counted.
    Refuse(RedriveRefusal),
}

/// The signature of a CI failure: what broke, as a stable set.
///
/// Taken from the three things a CI log names that survive a re-run with
/// different timings and paths: which jobs failed, which tests failed, and
/// which compiler errors were raised. Everything else in the log (durations,
/// request ids, absolute paths, line numbers) changes between runs of the same
/// failure, and a signature built from it would never match itself.
///
/// `None` when the log names none of the three. That is not the same as an
/// empty signature: it means no cause could be read, and the caller answers it
/// with the `NoEvidence` refusal rather than by inventing a cause that matches
/// nothing.
pub fn failure_signature(log: &str) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();

    for line in log.lines() {
        let line = line.trim();
        // Job headers: the log is a concatenation of `## <job> (job <id>)`
        // blocks, so every line here is a job that ran and therefore failed.
        if let Some(rest) = line.strip_prefix("## ") {
            if let Some((name, _)) = rest.split_once(" (job ") {
                if !name.trim().is_empty() {
                    parts.push(format!("job:{}", name.trim()));
                }
            }
            continue;
        }
        // `test some::name ... FAILED`
        if let Some(rest) = line.strip_prefix("test ") {
            if let Some((name, _)) = rest.split_once(" ... ") {
                if line.ends_with("FAILED") && !name.trim().is_empty() {
                    parts.push(format!("test:{}", name.trim()));
                }
            }
            continue;
        }
        // `error[E0308]: mismatched types`
        if let Some(rest) = line.strip_prefix("error[") {
            if let Some((code, _)) = rest.split_once(']') {
                if !code.trim().is_empty() {
                    parts.push(format!("code:{}", code.trim()));
                }
            }
            continue;
        }
        // A plain `error: ...` line. Its message is the content that carries
        // the cause, so it is kept whole rather than reduced to a count: two
        // rounds that fail with different messages are different causes.
        if let Some(rest) = line.strip_prefix("error: ") {
            let msg = rest.trim();
            if !msg.is_empty() {
                parts.push(format!("msg:{msg}"));
            }
        }
    }

    if parts.is_empty() {
        return None;
    }
    parts.sort();
    parts.dedup();
    Some(parts.join("|"))
}

/// One cause's rounds, as the times they were spent.
///
/// Times rather than a count: the budget is "how many rounds in the window",
/// and a count alone could not forget an old round without also forgetting
/// whether the cause had been seen at all.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CauseEntry {
    /// When each round was spent, oldest first.
    #[serde(default)]
    pub spent: Vec<DateTime<Utc>>,
}

/// Which causes have already been given their rounds.
///
/// Persisted, because the budget it enforces has to mean the same thing across
/// a restart: a process that forgot its rounds would hand out a fresh one every
/// time it rolled, which is exactly the deployment where a chain burns most.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CauseLedger {
    /// Keyed by signature. Ordered so the file is diffable.
    #[serde(default)]
    pub causes: BTreeMap<String, CauseEntry>,
}

impl CauseLedger {
    /// How many rounds this cause has been given inside the window.
    ///
    /// Reads through the window rather than trusting the stored list, so a
    /// ledger written by an older configuration still answers correctly.
    pub fn rounds_in_window(&self, signature: &str, window: Duration, now: DateTime<Utc>) -> u32 {
        let Some(entry) = self.causes.get(signature) else {
            return 0;
        };
        let fresh = entry.spent.iter().filter(|t| now - **t < window).count();
        fresh as u32
    }

    /// Charge one round to this cause and drop everything that has aged out.
    pub fn spend(&mut self, signature: &str, window: Duration, now: DateTime<Utc>) {
        let entry = self.causes.entry(signature.to_string()).or_default();
        entry.spent.push(now);
        let keep = now - window;
        entry.spent.retain(|t| *t >= keep);
    }

    /// Drop causes whose rounds have all aged out.
    pub fn prune(&mut self, window: Duration, now: DateTime<Utc>) {
        let keep = now - window;
        for entry in self.causes.values_mut() {
            entry.spent.retain(|t| *t >= keep);
        }
        self.causes.retain(|_, entry| !entry.spent.is_empty());
    }
}

/// Whether to spend a round on this failure. Pure: the caller owns the clock
/// and the ledger, so the decision can be asserted without a filesystem.
pub fn decide(
    log: &str,
    ledger: &CauseLedger,
    max_rounds: u32,
    window: Duration,
    now: DateTime<Utc>,
) -> RedriveDecision {
    let Some(signature) = failure_signature(log) else {
        return RedriveDecision::Refuse(RedriveRefusal::NoEvidence);
    };
    if ledger.rounds_in_window(&signature, window, now) >= max_rounds {
        return RedriveDecision::Refuse(RedriveRefusal::CauseExhausted);
    }
    RedriveDecision::Spend { signature }
}

/// Where the ledger lives. Not inside `landing/`: that directory is read as
/// one record per file, and a file that is not a record would be loaded and
/// discarded on every pass.
fn ledger_path() -> PathBuf {
    crate::landing::data_dir().join("redrive-budget.json")
}

/// Load the ledger.
///
/// A file that is not there yet is an empty ledger and not an error: that is
/// the normal state before the first round is charged. Anything else (an
/// unreadable file, a ledger that no longer parses) is returned to the caller
/// as an error so the loss can be counted — the caller still carries on with
/// an empty ledger, because refusing every re-drive over a filesystem fault
/// would turn the fault into a halt in generation.
pub async fn load_ledger() -> SFResult<CauseLedger> {
    load_ledger_at(&ledger_path()).await
}

/// Persist the ledger after spending a round.
pub async fn save_ledger(ledger: &CauseLedger) -> SFResult<()> {
    save_ledger_at(&ledger_path(), ledger).await
}

async fn load_ledger_at(path: &Path) -> SFResult<CauseLedger> {
    let text = match tokio::fs::read_to_string(path).await {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(CauseLedger::default()),
        Err(e) => return Err(SFError::IO(format!("read redrive budget: {e}"))),
    };
    parse_ledger(&text)
}

/// What a ledger file's text says. Pure, so the reader's two failure
/// directions can be asserted without a filesystem: valid text loads, text that
/// is not a ledger does not.
fn parse_ledger(text: &str) -> SFResult<CauseLedger> {
    serde_json::from_str(text).map_err(|e| SFError::Internal(format!("parse redrive budget: {e}")))
}

async fn save_ledger_at(path: &Path, ledger: &CauseLedger) -> SFResult<()> {
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| SFError::IO(format!("create data dir: {e}")))?;
    }
    let json = serde_json::to_string_pretty(ledger)
        .map_err(|e| SFError::Internal(format!("serialize redrive budget: {e}")))?;
    tokio::fs::write(path, json)
        .await
        .map_err(|e| SFError::IO(format!("write redrive budget: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(minutes: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 25, 0, 0, 0).unwrap() + Duration::minutes(minutes)
    }

    fn window() -> Duration {
        Duration::hours(24)
    }

    const LOG_A: &str = "## CI (job 1)\nrunning tests\ntest cog_core::task::x ... FAILED\n";

    /// The signature has to be the same for two runs of the same failure, and
    /// different for a different one. Anything in the log that changes between
    /// runs of the same failure would break the first half.
    #[test]
    fn the_same_failure_signs_the_same_way_and_a_different_one_does_not() {
        let noisy = "## CI (job 1)\n   Compiling cog-core v0.1.0\n    Finished in 41.2s\n\
                     test cog_core::task::x ... FAILED\n        elapsed: 0.03s\n";
        assert_eq!(failure_signature(LOG_A), failure_signature(noisy));
        assert_ne!(
            failure_signature(LOG_A),
            failure_signature("## CI (job 1)\ntest cog_core::task::y ... FAILED\n")
        );
        // A different job that fails is a different cause even when the test
        // is the same: the failure moved somewhere else.
        assert_ne!(
            failure_signature(LOG_A),
            failure_signature("## Lint (job 2)\ntest cog_core::task::x ... FAILED\n")
        );
    }

    /// A log that names no job, test or error has no cause to read, and saying
    /// so is different from signing it as empty — the empty signature would
    /// match every other unreadable log and merge unrelated failures.
    #[test]
    fn a_log_that_names_no_cause_has_no_signature() {
        assert_eq!(failure_signature(""), None);
        assert_eq!(failure_signature("   \n\n"), None);
        assert_eq!(failure_signature("Build finished with 0 warnings\n"), None);
        assert!(failure_signature("error[E0308]: mismatched types\n").is_some());
    }

    /// A refusal has to be the answer when there is nothing to generate
    /// against: the round it would spend is a full generation, and a guess is
    /// not what a round is worth.
    #[test]
    fn no_log_means_no_round() {
        let ledger = CauseLedger::default();
        assert_eq!(
            decide("", &ledger, 1, window(), at(0)),
            RedriveDecision::Refuse(RedriveRefusal::NoEvidence)
        );
    }

    /// The budget is per cause, so the round a first failure gets is still
    /// available, and the round a repeat of it asks for is not.
    #[test]
    fn a_cause_gets_its_rounds_and_then_stops() {
        let mut ledger = CauseLedger::default();
        assert_eq!(
            decide(LOG_A, &ledger, 1, window(), at(0)),
            RedriveDecision::Spend {
                signature: failure_signature(LOG_A).unwrap()
            }
        );
        ledger.spend(&failure_signature(LOG_A).unwrap(), window(), at(0));
        assert_eq!(
            decide(LOG_A, &ledger, 1, window(), at(10)),
            RedriveDecision::Refuse(RedriveRefusal::CauseExhausted)
        );
    }

    /// A different cause is new information about the code, and the budget
    /// must not refuse it: generating against a failure nobody has tried is
    /// the whole point of the channel.
    #[test]
    fn a_cause_nobody_has_tried_is_never_refused() {
        let mut ledger = CauseLedger::default();
        ledger.spend(&failure_signature(LOG_A).unwrap(), window(), at(0));
        let other = "## CI (job 1)\ntest cog_core::task::y ... FAILED\n";
        assert!(matches!(
            decide(other, &ledger, 1, window(), at(1)),
            RedriveDecision::Spend { .. }
        ));
    }

    /// The window is what keeps the budget from being a permanent verdict: a
    /// cause that comes back after the branch has moved is a new situation.
    #[test]
    fn a_cause_forgets_its_rounds_once_the_window_passes() {
        let mut ledger = CauseLedger::default();
        let sig = failure_signature(LOG_A).unwrap();
        ledger.spend(&sig, window(), at(0));
        assert_eq!(ledger.rounds_in_window(&sig, window(), at(60)), 1);
        assert_eq!(ledger.rounds_in_window(&sig, window(), at(24 * 60 + 1)), 0);
        assert!(matches!(
            decide(LOG_A, &ledger, 1, window(), at(24 * 60 + 1)),
            RedriveDecision::Spend { .. }
        ));
    }

    /// A spent round is charged even when the same cause is recorded again in
    /// a way that would age out: the stored list must stay bounded and must
    /// keep only what the window still counts.
    #[test]
    fn spending_drops_rounds_that_the_window_no_longer_counts() {
        let mut ledger = CauseLedger::default();
        let sig = failure_signature(LOG_A).unwrap();
        ledger.spend(&sig, window(), at(0));
        ledger.spend(&sig, window(), at(24 * 60 + 30));
        assert_eq!(ledger.causes[&sig].spent.len(), 1);
        // The round that is still inside its window survives a prune, and the
        // cause is forgotten only once nothing in the ledger counts it.
        ledger.prune(window(), at(48 * 60));
        assert_eq!(ledger.causes[&sig].spent.len(), 1);
        ledger.prune(window(), at(49 * 60));
        assert!(ledger.causes.is_empty());
    }

    /// A round is charged before the generation it pays for runs, so the count
    /// has to survive the process that charged it.
    #[test]
    fn the_ledger_survives_a_round_trip_through_its_file() {
        let mut ledger = CauseLedger::default();
        let sig = failure_signature(LOG_A).unwrap();
        ledger.spend(&sig, window(), at(0));
        let text = serde_json::to_string(&ledger).unwrap();
        let back: CauseLedger = serde_json::from_str(&text).unwrap();
        assert_eq!(back.rounds_in_window(&sig, window(), at(5)), 1);
        // A ledger written before the field existed still loads.
        let older: CauseLedger = serde_json::from_str("{}").unwrap();
        assert_eq!(older.rounds_in_window(&sig, window(), at(5)), 0);
    }

    fn ledger_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cogneva-redrive-{}-{name}.json",
            std::process::id()
        ))
    }

    /// A file that was never written is the state before the first round, not a
    /// lost budget. If this read reported an error, every fresh install would
    /// look like a budget that stopped applying.
    #[tokio::test]
    async fn a_ledger_that_was_never_written_is_empty_and_not_a_loss() {
        let path = ledger_file("missing");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = load_ledger_at(&path)
            .await
            .expect("an absent ledger is not an error");
        assert!(ledger.causes.is_empty());
    }

    /// Text that is not a ledger has to reach the caller as an error: read as
    /// "no rounds spent" it would grant a fresh round per cause, and the round
    /// is a full generation plus a full CI run.
    #[tokio::test]
    async fn a_ledger_that_does_not_parse_is_a_loss_the_caller_sees() {
        let path = ledger_file("corrupt");
        tokio::fs::write(&path, "{\"causes\": ").await.unwrap();
        assert!(load_ledger_at(&path).await.is_err());
        let _ = tokio::fs::remove_file(&path).await;
    }

    /// The charge has to be in the file the next process reads, which is the
    /// only thing that stops a restart from re-funding a spend cause.
    #[tokio::test]
    async fn a_charge_written_is_a_charge_read_back() {
        let path = ledger_file("roundtrip");
        let sig = failure_signature(LOG_A).unwrap();
        let mut ledger = CauseLedger::default();
        ledger.spend(&sig, window(), at(0));
        save_ledger_at(&path, &ledger).await.unwrap();
        let back = load_ledger_at(&path).await.unwrap();
        assert_eq!(back.rounds_in_window(&sig, window(), at(5)), 1);
        assert_eq!(
            decide(LOG_A, &back, 1, window(), at(5)),
            RedriveDecision::Refuse(RedriveRefusal::CauseExhausted)
        );
        let _ = tokio::fs::remove_file(&path).await;
    }
}
