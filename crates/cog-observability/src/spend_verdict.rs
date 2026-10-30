//! What each registered call site's token spend in a closed window amounts to.
//!
//! The ledger answers "how much was burned, by whom". It cannot answer the two
//! questions that decide whether that number is right: **was this call site
//! supposed to spend here**, and **where nothing was spent, was that a decision
//! or a hole**. A usage row only ever describes calls that happened, so a site
//! that ran and a site that stopped are the same reading on that table (none),
//! and a saving is invisible unless the saving itself is written down (which is
//! what [`crate::skip_store`] is for).
//!
//! So the unit here is not a ledger row but a cell: **(registered call site ×
//! closed window)**. Each cell lands exactly one of four words.
//!
//! - `necessary` — it spent. The tokens are the account's, and no further
//!   judgement is needed to know the call was made.
//! - `documented_skip` — it did not spend and at least one saving was recorded
//!   for it in this window, with the reason. This is the half a spend ledger
//!   structurally cannot hold, and the reason is carried rather than folded in,
//!   because a saving whose reason is unreadable cannot be told from one nobody
//!   decided.
//! - `unexpected_absent` — the run was live, this site had been spending, and
//!   it stopped with nothing on the record to explain it. This is the cell the
//!   whole face exists for: the calls that *should* have been made and were not,
//!   which is the direction where missing spend makes the system more expensive
//!   rather than cheaper (a review that silently stopped being asked is a saving
//!   on the review and a cost everywhere downstream of it).
//! - `unsettled` — the saving face could not be read, so a window with no spend
//!   cannot be told from a window whose savings are merely unreadable. **It
//!   never degrades to `unexpected_absent`**: a face that is down would
//!   otherwise report every deliberate saving in the platform as a defect, and
//!   the reading would be loudest exactly when it knows least.
//!
//! **Why "it had been spending" and not "it is registered".** The literal rule —
//! a registered site with no spend and no recorded saving in a window — was
//! measured against the ledger before this was written, over the 30 days ending
//! 2026-10-16: it emits a cell for a *healthy* site in most of the hours the
//! platform is up (the stages of one run land in different hours, so every site
//! is idle in the hours its siblings are working). At hourly resolution that
//! rule is a constant alarm and carries no information. Adding the predecessor
//! condition — the site spent in the window immediately before, and the run is
//! still live — turns it into a *stop*, and the same measurement puts it at 0
//! cells for the planner over the 51 hours it spent, 2 for `memory` over 50
//! hours, and 9 for the moderator over 36. A rate that low is what makes a cell
//! worth looking at (the counts are re-measurable at any time with
//! `scripts/host/cogneva-token-account-reading.sh verdict`; they are not
//! repeated here as constants, because a number in a comment is a number that
//! goes stale).
//!
//! **What a missing cell means.** A window where the site had not been spending
//! either (its run had already ended, or never started) gets no cell: there is
//! no spend to judge, and inventing a word for it would put four different
//! situations in one bucket. A reader that needs to tell "no cell because
//! nothing was expected" from "no cell because the fold did not run" reads the
//! watermark ([`last_verdicted_window_end`]) and the loop's own liveness series,
//! not this table — the same division the usage rollup uses.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tracing::{info, warn};

use crate::skip_store::LlmSkipStore;
use crate::usage_store::{windows_to_roll, LlmUsageStore};

/// The loop, as the liveness readings name it. A loop whose whole job is to run
/// on a period has no other way to say it is still doing so.
pub const LOOP: &str = "observability_llm_spend_verdict";

/// The cells' table. A projection of three readings — the ledger, this roster,
/// and the saved-call ledger — and never a fourth place a spend is decided.
pub const VERDICT_TABLE: &str = "cog_llm_spend_verdict";

/// Window length. The same closed hour the usage rollup folds, so a verdict
/// cell and the spend it judges are keyed identically and can be read together
/// without a mapping.
// cap: bound
pub const WINDOW_SECS: i64 = 3600;

/// How often the loop looks for windows that have closed.
// cap: bound
pub const PERIOD: Duration = Duration::from_secs(600);

/// How far back a first run reaches: nowhere.
///
/// The usage rollup reaches back because its input is the ledger, which is
/// recomputable from history — a fold that starts late can still fill the hours
/// it missed. This fold has a second input that is *not* recomputable: the
/// saving record only exists from the moment its writer does, so a window
/// judged from before that would read a deliberate saving as an unexplained
/// silence. That is exactly the false red the `unsettled` arm exists to avoid,
/// and the way not to mint it is not to reach back. A first run therefore
/// judges the newest closed window and follows the watermark from there; a run
/// that *has* a watermark still fills every window after it, so a gap while
/// this loop was down is caught up rather than skipped.
// cap: bound
pub const LOOKBACK_SECS: i64 = 0;

/// This store's schema, and the one a restore replays.
pub const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS cog_llm_spend_verdict (
                window_start  TIMESTAMPTZ NOT NULL,
                window_end    TIMESTAMPTZ NOT NULL,
                actor         TEXT NOT NULL,
                verdict       TEXT NOT NULL,
                skip_reason   TEXT NOT NULL DEFAULT '',
                skips   BIGINT NOT NULL DEFAULT 0,
                calls         BIGINT NOT NULL DEFAULT 0,
                tokens_input  BIGINT NOT NULL DEFAULT 0,
                tokens_output BIGINT NOT NULL DEFAULT 0,
                decided_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (window_start, window_end, actor, verdict, skip_reason)
            )
;
CREATE INDEX IF NOT EXISTS idx_cog_llm_spend_verdict_window ON cog_llm_spend_verdict (window_start, window_end);
"#;

/// What one call site's window amounts to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpendVerdict {
    /// It spent. Nothing more needs to be said about the window.
    Necessary,
    /// It did not spend, and the saving is on the record with its reason.
    DocumentedSkip,
    /// The run was live, it had been spending, and it stopped unexplained.
    UnexpectedAbsent,
    /// The saving face could not be read, so this window cannot be judged.
    /// Deliberately not folded into [`Self::UnexpectedAbsent`].
    Unsettled,
}

impl SpendVerdict {
    /// Every value, in the order the table reads them. Closed: the column is
    /// what a reader selects rows by, so a value outside this list is not a new
    /// verdict, it is a bug that would silently become its own series.
    pub const ALL: [SpendVerdict; 4] = [
        SpendVerdict::Necessary,
        SpendVerdict::DocumentedSkip,
        SpendVerdict::UnexpectedAbsent,
        SpendVerdict::Unsettled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SpendVerdict::Necessary => "necessary",
            SpendVerdict::DocumentedSkip => "documented_skip",
            SpendVerdict::UnexpectedAbsent => "unexpected_absent",
            SpendVerdict::Unsettled => "unsettled",
        }
    }
}

/// A call site the platform declares it spends tokens from, and where its
/// savings are recorded.
///
/// Declared rather than derived from the ledger, because the declaration is the
/// decision: a site read off its own history can only ever be judged against
/// what it already did, and a site that *stopped being reached* has no history
/// left to be read off. Declaring it is what makes its absence readable — and
/// the census over this list (`crates/cogneva/tests/llm_spend_verdict_contract_test.rs`)
/// is what keeps a declaration from outliving the call site it names, and a new
/// call site from arriving without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredSite {
    /// The actor label the gateway records this site's calls under. The same
    /// closed vocabulary `cog_gateway::actor_labels` admits, so a verdict cell
    /// and the spend it judges are keyed by one value rather than two that have
    /// to be matched up by hand.
    pub actor: &'static str,
    /// The `stage` this site's savings are recorded under, when a skipping call
    /// site states them at all. `None` for a site with no skip face yet: its
    /// silence can only read as [`SpendVerdict::UnexpectedAbsent`], which is
    /// honest — nothing today can document a saving it does not have.
    pub skip_stage: Option<&'static str>,
    /// Why this site is registered, and by what reading its expectation is
    /// grounded. Written down because the next person to change it has to know
    /// what the entry rests on.
    pub why: &'static str,
}

/// A call site the platform declares, whose silence carries no verdict, and the
/// reason it carries none.
///
/// In the open, with a reason, rather than simply absent: an entry missing from
/// the roster reads exactly like a site nobody thought about, and the difference
/// between "we decided this" and "we forgot this" is the whole value of the
/// list. An on-demand site (a human chat, a CLI eval run) is idle in almost
/// every window, so judging its silence would make the face loud and
/// information-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnregisteredSite {
    pub actor: &'static str,
    pub why: &'static str,
}

/// The sites whose silence is a defect, with the reading each rests on.
///
/// All five have been observed spending by the ledger, and the expectation is
/// "a stretch of runs does not end on its own": each of these is reached on
/// every pass of the loop that reaches its upstream, so a window where the run
/// is live, the site had been spending, and it stopped is a change in behaviour
/// rather than a quiet hour.
pub const REGISTERED_SITES: &[RegisteredSite] = &[
    RegisteredSite {
        actor: "agent:planner",
        skip_stage: None,
        why: "the stage every run starts from; over the 30 days ending 2026-10-16 it spent in 51 hours and stopped mid-run in none of them, which is what makes a stop here a signal rather than a quiet hour",
    },
    RegisteredSite {
        actor: "agent:generator",
        skip_stage: None,
        why: "reached on every iteration of a run that produced a plan; 2 stops over 48 spending hours in the same window",
    },
    RegisteredSite {
        actor: "agent:moderator",
        skip_stage: None,
        why: "reached at the end of every round that did not reach consensus; 9 stops over 36 spending hours — the largest of the five, and the reason its cells are worth reading rather than dismissing",
    },
    RegisteredSite {
        actor: "self_review:planner",
        skip_stage: Some(cog_core::roles::PLANNER),
        why: "the review of the plan; the site most likely to be skipped deliberately (a self-evolution plan is judged by measurement instead), so with the saved-call ledger in place most of its absent windows should read `documented_skip` — a window that reads `unexpected_absent` here is the review having gone away, which is the expensive direction",
    },
    RegisteredSite {
        actor: "self_review:moderator",
        skip_stage: Some(cog_core::roles::MODERATOR),
        why: "the review of the moderator's verdict, skipped under the same declared reasons as the planner's; 5 stops over 13 spending hours",
    },
];

/// Declared call sites whose silence carries no verdict, and why.
///
/// Kept complete against the actor vocabulary by the census test: every value
/// `cog_gateway::actor_labels` admits is either registered above or listed
/// here, so a new call site forces a decision at this file.
pub const UNREGISTERED_SITES: &[UnregisteredSite] = &[
    UnregisteredSite {
        actor: "agent:*",
        why: "the role family is configuration: a configured role that is not the planner, generator or moderator is reached by the workflow its role names, and the platform does not know when that workflow is due",
    },
    UnregisteredSite {
        actor: "self_review:*",
        why: "same family, same reason: only the planner's and the moderator's reviews have been observed running often enough to say a missing window is a stop; every other configured self-review role is judged by the census over the writer, not by a window",
    },
    UnregisteredSite {
        actor: "memory",
        why: "driven by writes to the knowledge base rather than by the run loop: over the 30 days ending 2026-10-16 it spent in 50 hours but 2 of its stops followed a window it had spent in, so its silence is a quiet input rather than a stopped call site. Worth registering once the trigger it waits on has a reading of its own — today it does not, and inventing one would be a second opinion about when extraction is due",
    },
    UnregisteredSite {
        actor: "intent_assess",
        why: "one call per submitted intent, not one per pass: 7 of its 12 spending hours were followed by a silent one, which is its normal shape rather than a stop",
    },
    UnregisteredSite {
        actor: "evolution",
        why: "driven by the change pipeline rather than by the run loop; its silence and the pipeline's idle state are the same reading today",
    },
    UnregisteredSite {
        actor: "ralph",
        why: "driven by an explicitly launched squad loop; a window it does not run in is a window nobody launched it",
    },
    UnregisteredSite {
        actor: "eval",
        why: "a CLI experiment run, launched by hand: idle in every window no one runs it, and no reading says when one is due",
    },
    UnregisteredSite {
        actor: "intent_gateway",
        why: "answers an operator's chat message; absence is the absence of a message",
    },
    UnregisteredSite {
        actor: "web_chat",
        why: "answers an operator's chat message; same as `intent_gateway`",
    },
    UnregisteredSite {
        actor: "wiki",
        why: "maintenance pass over the wiki, launched on demand; no reading says when one is due",
    },
    UnregisteredSite {
        actor: "skill_extractor",
        why: "runs when a reflection produces a candidate skill; its silence and the reflection chain's idle state are the same reading",
    },
    UnregisteredSite {
        actor: "sandbox-document-organizer",
        why: "reached from the sandbox documenter, which runs when a run produces documents rather than on a cadence",
    },
    UnregisteredSite {
        actor: "self_review",
        why: "the default a review loop starts with when its caller names no actor; it names no call site of its own, so it has no window to be judged in",
    },
    UnregisteredSite {
        actor: "unknown",
        why: "not a call site: it is the cell a caller that declared no actor lands in, and the verdict on it would be a verdict on the declaration, which is what `llm_call_without_declared_actor` already reads",
    },
];

/// A stage whose savings are recorded and read by no cell, and why.
///
/// Declared rather than left to the runtime warning, so that "no cell reads
/// this" is a decision on the record and the warning stays reserved for a stage
/// nobody has thought about. Each of these is a stage of a self-review that the
/// platform has never been observed spending on: its savings are real and land
/// on the realtime series, but there is no spend window to judge them against,
/// so no cell will ever carry them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnreadSkipStage {
    pub stage: &'static str,
    pub why: &'static str,
}

/// Stages that are recorded but read by no registered site.
///
/// Kept complete against the call sites by
/// `crates/cogneva/tests/llm_spend_verdict_contract_test.rs`: the literals the
/// skipping call sites pass are exactly the registered stages plus these, so a
/// new call site forces a decision at this file rather than arriving as a
/// saving nothing can see.
pub const UNREAD_SKIP_STAGES: &[UnreadSkipStage] = &[
    UnreadSkipStage {
        stage: cog_core::roles::GENERATOR,
        why: "the generator's review has been skipped in every window observed; no spend has ever been recorded under `self_review:generator`, so there is no window in which its silence could be told from its absence",
    },
    UnreadSkipStage {
        stage: cog_core::roles::EVALUATOR,
        why: "the evaluator's review is skipped by construction when the change-validation path already answers the question, and its own review has never been observed spending",
    },
    UnreadSkipStage {
        stage: cog_core::roles::MERGER,
        why: "same shape as the generator's: savings are recorded, no spend under `self_review:merger` has ever been observed to judge them against",
    },
];

/// Whether some registered site reads this stage's savings.
pub fn skip_stage_is_read(stage: &str) -> bool {
    REGISTERED_SITES
        .iter()
        .any(|site| site.skip_stage == Some(stage))
}

/// Whether this stage is either read by a registered site or declared unread.
/// A stage outside both is one no decision covers, which is what the loop warns
/// about.
pub fn skip_stage_is_declared(stage: &str) -> bool {
    skip_stage_is_read(stage) || UNREAD_SKIP_STAGES.iter().any(|u| u.stage == stage)
}

/// Everything the rule needs about one cell, gathered so the rule itself has no
/// clocks, no store, and nothing to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteWindowFacts {
    /// Where this site's savings are recorded, if it has a skip face.
    pub skip_stage: Option<&'static str>,
    /// Tokens this site spent in the window, both directions.
    pub tokens: i64,
    /// Calls this site made in the window.
    pub calls: i64,
    /// Whether the site spent in the window immediately before this one. A stop
    /// is only a stop if the stretch it belongs to was alive one window ago.
    pub spent_before: bool,
    /// Whether a run was live at all in this window — whether any registered
    /// site spent. Without this, the single window after the whole platform
    /// shuts down would report every site as having stopped.
    pub run_live: bool,
    /// The savings recorded for this site's stage inside the window.
    pub skips: Vec<SkippedCall>,
}

/// One recorded saving: the reason, and how many calls it saved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedCall {
    pub reason: String,
    pub skips: i64,
}

/// The verdict for one cell, or `None` when there is nothing to judge.
///
/// The order of the arms is the rule. Spend first — a window that spent needs
/// no explanation. Then the recorded saving. Then the unreadable saving face,
/// *before* the absence: a saving that could not be read must never be reported
/// as a call site that stopped, or a face that is down would name every
/// deliberate saving in the platform as a defect. Only then can an absence be
/// told from a quiet hour, which needs both the predecessor window and a live
/// run.
///
/// **`unsettled` does not ask for a predecessor window, and that is the
/// deliberate difference from the absence below.** The predecessor condition
/// exists to keep an *idle* hour from reading as a *stop*; it says nothing
/// about whether this window's reading depends on the saving face. Asking for
/// it here would mean the face going down reports nothing at all for the sites
/// that lean on it most — a review that is skipped in most windows never has a
/// predecessor, so its windows would come back uncelled, and "the saving face
/// is unreadable" would look exactly like "no window needed a verdict". The
/// only other condition is that a run was live, because with nothing running
/// there is no window to judge at all.
pub fn verdict_for(facts: &SiteWindowFacts, savings_readable: bool) -> Option<SpendVerdict> {
    if facts.tokens > 0 {
        return Some(SpendVerdict::Necessary);
    }
    if !facts.skips.is_empty() {
        return Some(SpendVerdict::DocumentedSkip);
    }
    if !savings_readable && facts.skip_stage.is_some() && facts.run_live {
        // Only a site that has a saving face can be made unjudgeable by it: a
        // site with none has nothing that could document its silence either
        // way, so it falls through to the absence, which is the honest reading
        // of it whether the face is up or down.
        return Some(SpendVerdict::Unsettled);
    }
    if facts.spent_before && facts.run_live {
        return Some(SpendVerdict::UnexpectedAbsent);
    }
    None
}

/// One cell as it is written to [`VERDICT_TABLE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictCell {
    pub actor: String,
    pub verdict: SpendVerdict,
    /// The reason for a `documented_skip` cell, empty for every other verdict.
    /// Part of the key rather than a folded field, so a window with two
    /// different reasons reports both instead of hiding one behind the other.
    pub skip_reason: String,
    pub skips: i64,
    pub calls: i64,
    pub tokens_input: i64,
    pub tokens_output: i64,
}

/// Decide every cell of one window.
///
/// `spend` is the ledger's per-actor totals for the window, `before` the same
/// for the window immediately before it, `saves` the savings recorded inside
/// it, and `savings_readable` whether the saving face could be read at all.
/// Kept free of I/O so the rule can be judged on constructed facts.
pub fn cells_for_window(
    spend: &BTreeMap<String, (i64, i64, i64)>,
    before: &BTreeMap<String, (i64, i64, i64)>,
    saves: &BTreeMap<String, Vec<SkippedCall>>,
    savings_readable: bool,
) -> Vec<VerdictCell> {
    let run_live = REGISTERED_SITES.iter().any(|site| {
        spend
            .get(site.actor)
            .is_some_and(|(_, _, tokens)| *tokens > 0)
    });
    let mut cells = Vec::new();
    for site in REGISTERED_SITES {
        let (calls, tokens_input, tokens_output) =
            spend.get(site.actor).copied().unwrap_or((0, 0, 0));
        let tokens = tokens_input + tokens_output;
        let spent_before = before
            .get(site.actor)
            .is_some_and(|(_, _, tokens)| *tokens > 0);
        let skips = site
            .skip_stage
            .and_then(|stage| saves.get(stage))
            .cloned()
            .unwrap_or_default();
        let facts = SiteWindowFacts {
            skip_stage: site.skip_stage,
            tokens,
            calls,
            spent_before,
            run_live,
            skips,
        };
        let Some(verdict) = verdict_for(&facts, savings_readable) else {
            continue;
        };
        if verdict == SpendVerdict::DocumentedSkip {
            for skip in &facts.skips {
                cells.push(VerdictCell {
                    actor: site.actor.to_string(),
                    verdict,
                    skip_reason: skip.reason.clone(),
                    skips: skip.skips,
                    calls,
                    tokens_input,
                    tokens_output,
                });
            }
            continue;
        }
        cells.push(VerdictCell {
            actor: site.actor.to_string(),
            verdict,
            skip_reason: String::new(),
            skips: 0,
            calls,
            tokens_input,
            tokens_output,
        });
    }
    cells
}

/// The verdict cells over `[since, until)`, one line per
/// `(actor, verdict, skip_reason)` — the reading a panel, an alert rule or an
/// operator reaches for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictLine {
    pub actor: String,
    pub verdict: String,
    pub skip_reason: String,
    pub cells: i64,
    pub skips: i64,
    pub tokens_input: i64,
    pub tokens_output: i64,
}

/// PostgreSQL store for the verdict projection.
#[derive(Clone)]
pub struct SpendVerdictStore {
    pool: PgPool,
    ledger: LlmUsageStore,
    /// Absent when this deployment has no saved-call ledger. Kept as an option
    /// rather than assumed, because "the face is missing" and "the face says
    /// nothing was saved" must not read alike — the first is `unsettled`.
    skips: Option<LlmSkipStore>,
}

impl SpendVerdictStore {
    pub async fn connect(
        database_url: &str,
        ledger: LlmUsageStore,
        skips: Option<LlmSkipStore>,
    ) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self {
            pool,
            ledger,
            skips,
        })
    }

    pub async fn init_schema(&self, schema: &dyn cog_core::SchemaRunner) -> anyhow::Result<()> {
        schema
            .apply_schema(DDL)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    /// End of the newest window already judged, or `None` on a first run. The
    /// loop resumes here rather than replaying history — and a process that
    /// could not read a window leaves the watermark where it is, so the window
    /// is judged on the next pass instead of being written from a reading that
    /// failed.
    pub async fn last_verdicted_window_end(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        let row = sqlx::query(&format!(
            "SELECT MAX(window_end) AS last FROM {VERDICT_TABLE}"
        ))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.get::<Option<DateTime<Utc>>, _>("last")))
    }

    /// Judge every window in `windows`, oldest first, and land the cells.
    ///
    /// Returns how many cells were written. Idempotent by primary key: rerunning
    /// a window replaces the same cells, so the loop may run from more than one
    /// replica and a window it missed while down is judged on the next pass.
    pub async fn judge_windows(
        &self,
        windows: &[crate::usage_store::RollupWindow],
    ) -> anyhow::Result<u64> {
        let mut written = 0u64;
        for w in windows {
            let span = chrono::Duration::seconds(w.end.timestamp() - w.start.timestamp());
            let spend = actor_totals(&self.ledger, w.start, w.end).await?;
            let before = actor_totals(&self.ledger, w.start - span, w.start).await?;
            // A saving face that errors must not take the window down with it:
            // the spend cells are still true, and the zero-spend ones become
            // `unsettled`, which is the reading that says "this window could not
            // be judged" rather than one that invents a verdict.
            let (saves, savings_readable) = match self.skips.as_ref() {
                Some(store) => match store.skips_between(w.start, w.end).await {
                    Ok(rows) => {
                        let mut map: BTreeMap<String, Vec<SkippedCall>> = BTreeMap::new();
                        for row in rows {
                            map.entry(row.stage).or_default().push(SkippedCall {
                                reason: row.reason,
                                skips: row.skips,
                            });
                        }
                        (map, true)
                    }
                    Err(e) => {
                        warn!(error = %e, window = %w.start, "saved-call read failed; this window reads unsettled");
                        (BTreeMap::new(), false)
                    }
                },
                None => (BTreeMap::new(), false),
            };
            for cell in cells_for_window(&spend, &before, &saves, savings_readable) {
                sqlx::query(&format!(
                    r#"
                    INSERT INTO {VERDICT_TABLE}
                        (window_start, window_end, actor, verdict, skip_reason,
                         skips, calls, tokens_input, tokens_output)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (window_start, window_end, actor, verdict, skip_reason)
                    DO UPDATE SET
                        skips   = EXCLUDED.skips,
                        calls         = EXCLUDED.calls,
                        tokens_input  = EXCLUDED.tokens_input,
                        tokens_output = EXCLUDED.tokens_output
                    "#
                ))
                .bind(w.start)
                .bind(w.end)
                .bind(&cell.actor)
                .bind(cell.verdict.as_str())
                .bind(&cell.skip_reason)
                .bind(cell.skips)
                .bind(cell.calls)
                .bind(cell.tokens_input)
                .bind(cell.tokens_output)
                .execute(&self.pool)
                .await?;
                written += 1;
            }
        }
        Ok(written)
    }

    /// The cells over `[since, until)`, summed per `(actor, verdict, reason)`
    /// and ordered so two reads of the same range compare line for line.
    pub async fn verdicts_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<VerdictLine>> {
        verdict_lines(&self.pool, since, until).await
    }
}

/// The cells whose windows lie wholly inside `[since, until)`.
///
/// A window has to *end* inside the range rather than merely start in it: an
/// hour still running has not been judged, and counting it would report a
/// silence that the loop has not had the chance to look at. Both readers — the
/// loop's own check and the spend screen — go through here so they cannot
/// disagree about which windows are in a range.
pub async fn verdict_lines(
    pool: &PgPool,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
) -> anyhow::Result<Vec<VerdictLine>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT actor, verdict, skip_reason,
               COUNT(*)::BIGINT AS cells,
               COALESCE(SUM(skips), 0)::BIGINT   AS skips,
               COALESCE(SUM(tokens_input), 0)::BIGINT  AS tokens_input,
               COALESCE(SUM(tokens_output), 0)::BIGINT AS tokens_output
        FROM {VERDICT_TABLE}
        WHERE window_start >= $1 AND window_end <= $2
        GROUP BY actor, verdict, skip_reason
        ORDER BY actor, verdict, skip_reason
        "#
    ))
    .bind(since)
    .bind(until)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| VerdictLine {
            actor: r.get("actor"),
            verdict: r.get("verdict"),
            skip_reason: r.get("skip_reason"),
            cells: r.get("cells"),
            skips: r.get("skips"),
            tokens_input: r.get("tokens_input"),
            tokens_output: r.get("tokens_output"),
        })
        .collect())
}

/// Per-actor `(calls, tokens_in, tokens_out)` over the window, summed across
/// every upstream and protocol the ledger grouped them by.
async fn actor_totals(
    ledger: &LlmUsageStore,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
) -> anyhow::Result<BTreeMap<String, (i64, i64, i64)>> {
    let spend = ledger.usage_by_actor(since, until).await?;
    let mut totals: BTreeMap<String, (i64, i64, i64)> = BTreeMap::new();
    for row in spend {
        let entry = totals.entry(row.actor).or_insert((0, 0, 0));
        entry.0 += row.calls;
        entry.1 += row.tokens_input;
        entry.2 += row.tokens_output;
    }
    Ok(totals)
}

/// Judge every window that has closed since the last one judged. Returns the
/// number of cells written, 0 when there was nothing to do.
pub async fn run_once(store: &SpendVerdictStore) -> anyhow::Result<u64> {
    let last = store.last_verdicted_window_end().await?;
    let windows = windows_to_roll(last, Utc::now(), WINDOW_SECS, LOOKBACK_SECS);
    let written = store.judge_windows(&windows).await?;
    report_unmatched_skip_stages(store).await;
    Ok(written)
}

/// Say, once per pass, when a recorded saving names a stage no decision covers.
///
/// A saving nothing joins to is a saving that never appears in the verdict
/// face at all — it would be counted on the realtime series and silently absent
/// from the account, which is the failure this whole face exists to prevent.
/// Always a warning and never a cell: there is no window and no site to file it
/// under. A stage that is *declared* unread ([`UNREAD_SKIP_STAGES`]) is not
/// reported here; only one that no list has ever considered is, which is what
/// makes this reading worth acting on rather than something to filter out.
async fn report_unmatched_skip_stages(store: &SpendVerdictStore) {
    let Some(skips) = store.skips.as_ref() else {
        return;
    };
    let since = Utc::now() - chrono::Duration::seconds(LOOKBACK_SECS.max(WINDOW_SECS));
    match skips.skips_between(since, Utc::now()).await {
        Ok(rows) => {
            let undeclared: BTreeSet<&str> = rows
                .iter()
                .map(|row| row.stage.as_str())
                .filter(|stage| !skip_stage_is_declared(stage))
                .collect();
            for stage in undeclared {
                warn!(
                    stage,
                    "a saved call is recorded under a stage no list covers; \
                     it is on the realtime series and absent from the verdict face"
                );
            }
        }
        Err(e) => warn!(error = %e, "saved-call stages could not be read"),
    }
}

/// Spawn the verdict loop under this file's own name.
///
/// No lease, for the reason the usage rollup needs none: every cell is a pure
/// function of the two ledgers and the roster, so two processes judging the
/// same window upsert the same rows with the same words.
pub fn spawn(
    store: Arc<SpendVerdictStore>,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    cog_core::loop_health::spawn(
        LOOP,
        cog_core::loop_health::Cadence::Periodic(PERIOD),
        shutdown.clone(),
        move |beat| {
            let store = store.clone();
            let shutdown = shutdown.clone();
            async move {
                let mut interval = tokio::time::interval(PERIOD);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = interval.tick() => match run_once(&store).await {
                            Ok(0) => {}
                            Ok(cells) => {
                                info!(cells, "spend verdict judged closed windows");
                            }
                            Err(e) => {
                                warn!(error = %e, "spend verdict pass failed; the watermark is unmoved and the window is judged on the next pass");
                            }
                        },
                        _ = shutdown.wait() => break,
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(tokens: i64, spent_before: bool) -> SiteWindowFacts {
        SiteWindowFacts {
            skip_stage: Some("planner"),
            tokens,
            calls: if tokens > 0 { 1 } else { 0 },
            spent_before,
            run_live: true,
            skips: Vec::new(),
        }
    }

    /// A window that spent is `necessary` whatever else is true: a recorded
    /// saving beside a call that was anyway made does not make the spend a
    /// saving.
    #[test]
    fn spending_makes_the_window_necessary() {
        let mut f = facts(120, true);
        f.skips = vec![SkippedCall {
            reason: "disabled".into(),
            skips: 3,
        }];
        assert_eq!(verdict_for(&f, true), Some(SpendVerdict::Necessary));
        // ...even when the saving face could not be read.
        assert_eq!(verdict_for(&f, false), Some(SpendVerdict::Necessary));
    }

    /// A recorded saving is what an unexplained absence is not: the same
    /// window, judged with the record and without it, is two different words.
    #[test]
    fn a_recorded_saving_is_what_makes_an_absence_documented() {
        let mut f = facts(0, true);
        assert_eq!(verdict_for(&f, true), Some(SpendVerdict::UnexpectedAbsent));
        f.skips = vec![SkippedCall {
            reason: "self_evolution".into(),
            skips: 2,
        }];
        assert_eq!(verdict_for(&f, true), Some(SpendVerdict::DocumentedSkip));
    }

    /// The rule the third value exists for: a saving face that could not be
    /// read leaves a zero-spend window unjudged, and never reports it as a
    /// call site that stopped.
    #[test]
    fn an_unreadable_saving_face_is_unsettled_and_never_an_absence() {
        let f = facts(0, true);
        assert_eq!(verdict_for(&f, false), Some(SpendVerdict::Unsettled));
        assert_ne!(verdict_for(&f, false), Some(SpendVerdict::UnexpectedAbsent));
        // The site does not have to have been spending: the sites that lean on
        // the saving face most are the ones skipped in most windows, so asking
        // for a predecessor here would leave exactly those windows uncelled
        // while the face was down.
        assert_eq!(
            verdict_for(&facts(0, false), false),
            Some(SpendVerdict::Unsettled)
        );
        // With no run live there is no window to judge, either way.
        let mut no_run = facts(0, false);
        no_run.run_live = false;
        assert_eq!(verdict_for(&no_run, false), None);
    }

    /// A site with no skip face is not unsettled by that face being down: there
    /// is nothing that could document its silence either way, so the honest
    /// reading is the absence.
    #[test]
    fn a_site_with_no_saving_face_reads_the_same_either_way() {
        let mut f = facts(0, true);
        f.skip_stage = None;
        assert_eq!(verdict_for(&f, false), Some(SpendVerdict::UnexpectedAbsent));
        assert_eq!(verdict_for(&f, true), Some(SpendVerdict::UnexpectedAbsent));
    }

    /// A quiet hour is not a stop: without the predecessor window there is
    /// nothing to judge, and the cell is not written at all.
    #[test]
    fn a_site_that_had_not_been_spending_yields_no_cell() {
        assert_eq!(verdict_for(&facts(0, false), true), None);
    }

    /// The single window after the whole platform stops is not a window in
    /// which every call site stopped.
    #[test]
    fn a_window_with_no_run_in_it_judges_nobody() {
        let mut f = facts(0, true);
        f.run_live = false;
        assert_eq!(verdict_for(&f, true), None);
    }

    /// A first run asks for the newest closed window and nothing before it.
    ///
    /// One of this fold's two inputs cannot be recomputed from history: the
    /// saving record only exists from the moment its writer does. A run that
    /// reached back would therefore judge every hour before that against a
    /// silence it has no way to explain, and report each deliberate saving as
    /// an absence — the false red the `unsettled` arm exists to prevent, minted
    /// by the fold itself. The bound is stated as the behaviour a raised
    /// lookback would change rather than as the value of the constant, so the
    /// number is free to move as long as the reach does not.
    #[test]
    fn a_first_run_judges_only_the_newest_closed_window() {
        let asked = windows_to_roll(None, Utc::now(), WINDOW_SECS, LOOKBACK_SECS);
        assert_eq!(
            asked.len(),
            1,
            "a first run asked for {asked:?}: every window before the saving \
             record existed would be judged against silence it cannot explain"
        );
    }

    /// The closed vocabulary, spelled out: a value outside it is not a new
    /// verdict, it is a reader selecting by a column that grew without it.
    #[test]
    fn the_verdict_vocabulary_is_closed() {
        let words: Vec<&str> = SpendVerdict::ALL.iter().map(|v| v.as_str()).collect();
        assert_eq!(
            words,
            vec![
                "necessary",
                "documented_skip",
                "unexpected_absent",
                "unsettled"
            ]
        );
    }

    /// Every registered site names an actor the vocabulary admits, and no site
    /// is declared twice — the roster's own shape, before the census over the
    /// whole actor vocabulary is applied.
    #[test]
    fn no_site_is_registered_twice() {
        let mut seen = BTreeSet::new();
        for site in REGISTERED_SITES {
            assert!(
                seen.insert(site.actor),
                "{} is registered twice",
                site.actor
            );
            assert!(!site.why.trim().is_empty(), "{}: no reason", site.actor);
        }
    }

    /// A cell is written for a `documented_skip` per reason, so a window with
    /// two reasons reports both rather than hiding one behind the other.
    #[test]
    fn each_recorded_reason_gets_its_own_cell() {
        let spend: BTreeMap<String, (i64, i64, i64)> = BTreeMap::new();
        let before: BTreeMap<String, (i64, i64, i64)> =
            [("self_review:planner".to_string(), (1, 10, 0))].into();
        let saves: BTreeMap<String, Vec<SkippedCall>> = [(
            "planner".to_string(),
            vec![
                SkippedCall {
                    reason: "disabled".into(),
                    skips: 1,
                },
                SkippedCall {
                    reason: "self_evolution".into(),
                    skips: 4,
                },
            ],
        )]
        .into();
        let cells = cells_for_window(&spend, &before, &saves, true);
        let planner: Vec<&VerdictCell> = cells
            .iter()
            .filter(|c| c.actor == "self_review:planner")
            .collect();
        assert_eq!(planner.len(), 2);
        assert!(planner
            .iter()
            .all(|c| c.verdict == SpendVerdict::DocumentedSkip));
        assert_eq!(planner[0].skip_reason, "disabled");
        assert_eq!(planner[1].skip_reason, "self_evolution");
        assert_eq!(planner[1].skips, 4);
    }

    /// With no run live and nothing spent, the window writes no cell for
    /// anybody — the case that keeps a platform-wide shutdown from reading as
    /// every call site having stopped at once.
    #[test]
    fn a_dead_platform_writes_no_cells() {
        let spend: BTreeMap<String, (i64, i64, i64)> = BTreeMap::new();
        let before: BTreeMap<String, (i64, i64, i64)> = REGISTERED_SITES
            .iter()
            .map(|site| (site.actor.to_string(), (4i64, 100i64, 10i64)))
            .collect();
        let cells = cells_for_window(&spend, &before, &BTreeMap::new(), true);
        assert!(cells.is_empty(), "{cells:?}");
    }
}
