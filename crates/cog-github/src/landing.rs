//! Landing channel — flows a verified change onto the base branch.
//!
//! A change is committed straight onto the base branch upstream: no branch, no
//! pull request, no merge step. Every landed commit carries a `Change-Id`
//! trailer, which is what makes a landing idempotent across a restart and what
//! ties the revert answering a red CI run back to the change it undoes.
//!
//! The self-evolution mainline replaces its own process with the newly built
//! binary, so nothing scheduled after that switch can run in the old process.
//! A landing is therefore a persisted intent: it is written to disk before the
//! push, and a background watch loop reads those records on every start — that
//! is what answers a red CI run with a revert, minutes later, in a different
//! process than the one that landed the commit.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use cog_core::{GeneratedChange, LandedSource, SFError, SFResult};

use crate::config::{BotIdentityConfig, GitHubIntegrationConfig, GiteeIntegrationConfig};
use crate::contribution::ContributionController;
use crate::error::{CogGitHubError, Result};
use crate::provider::CodePlatformProvider;

/// Trailer stamped on every landed commit, carrying the change id.
///
/// It is the landing's identity: an existing commit with this trailer means
/// the change is already on the branch, and a push that races another landing
/// can be retried without producing a duplicate.
const CHANGE_ID_TRAILER: &str = "Change-Id";

/// Landings that failed, one increment per failed call, labeled with the
/// category.
///
/// A landing failure is otherwise one log line inside a loop that then moves
/// on, so "the channel is empty" and "the channel is being refused" look the
/// same from outside. The category is the only part of the failure that
/// aggregates: the message names a file and a hunk, the category says whether
/// the branch moved under the change or the change itself is the problem.
///
/// This is also where "refused once" and "refused for good" are told apart: a
/// `path` refusal is terminal, so the caller takes the change out of the queue
/// on the first one, while every other category is retried and shows up here
/// once per attempt.
pub use cog_core::metric_names::LANDING_FAILURES_TOTAL as LANDING_FAILURES_METRIC;

/// Red CI verdicts on a landed revision that were traced to the commit it was
/// replayed onto rather than to the change, one increment per landing.
///
/// This is the counter for a landing that was **not** answered, and it exists
/// because that non-action looks exactly like a landing whose CI never came
/// back: the record stays, no revert appears, and the branch is red either way.
/// What it separates is the branch being red *because of this change* from the
/// branch having been red *before* it — the difference between a change that
/// owes the fix and one that inherited the fault.
pub use cog_core::metric_names::LANDING_CI_FAILURE_INHERITED_TOTAL as LANDING_CI_FAILURE_INHERITED_METRIC;

/// Pushes to a mirror that were refused, one increment per refusal, labeled
/// with the mirror.
///
/// The base branch takes the commit whether or not a mirror did, so a mirror
/// that keeps refusing looks exactly like a mirror that is keeping up: the
/// landing succeeds either way, and the difference lives only here. What it
/// counts is a state nobody would otherwise see — the same repository on
/// another host, behind by every commit since the refusal.
pub use cog_core::metric_names::MIRROR_PUSH_FAILURES_TOTAL as MIRROR_PUSH_FAILURES_METRIC;

/// The path segment the gateway's git routes use for GitHub.
///
/// Same word the deployer builds its upstream refs from, and the same one the
/// gateway's route table matches on: a mirror's URL and its label both come
/// from here, so the two cannot name different hosts.
const GITHUB_SLUG: &str = "github";

/// The path segment the gateway's git routes use for Gitee.
const GITEE_SLUG: &str = "gitee";

/// Why a landing call failed, as a closed set the metric labels.
///
/// Decided where the failure happens — which step failed names the category —
/// and never recovered from the error text afterwards. The categories ask for
/// different answers: a conflict is the base branch having moved under the
/// change, which re-driving generation can address; a path or size refusal is
/// a property of the change that re-driving would only repeat; an environment
/// failure says nothing about the change at all; and a diff that could not be
/// read says nothing about the change either, only about the attempt to read
/// it, which is why it is not filed with the path refusals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LandingCategory {
    /// The verified delta no longer replays onto the base tip. The ordinary
    /// cause is another commit reaching the branch first and moving the code
    /// the change was generated against.
    Conflict,
    /// Every attempt lost the push race for the branch.
    Raced,
    /// The push was refused for a reason re-applying cannot fix.
    Rejected,
    /// The path policy refuses the change: it touches paths outside the
    /// contribution whitelist, or a forbidden path. Both name paths that were
    /// read and are not acceptable, which is what makes this a verdict on the
    /// change and not on the machinery around it.
    Path,
    /// Which paths the change touches could not be read from its diff.
    ///
    /// The gate is the same one and it still fails closed — nothing is pushed
    /// on a diff it cannot read — but the answer is not a verdict on the
    /// change. Keeping this apart from [`Self::Path`] is what lets the caller
    /// retry a change whose diff came out malformed instead of retiring it as a
    /// whitelist violation it never committed.
    UnreadableDiff,
    /// The change exceeds the landing policy's size cap.
    Oversized,
    /// The landing machinery could not run: git, fetch, worktree, commit.
    Environment,
}

impl LandingCategory {
    /// The metric label. Stable: alert rules and dashboards read these, so a
    /// value keeps its spelling once published. `path` kept its name as its
    /// meaning narrowed to the refusals that name paths, because renaming a
    /// value that a live rule matches would break the series rather than
    /// re-describe it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conflict => "conflict",
            Self::Raced => "raced",
            Self::Rejected => "rejected",
            Self::Path => "path",
            Self::UnreadableDiff => "unreadable_diff",
            Self::Oversized => "oversized",
            Self::Environment => "environment",
        }
    }
}

/// A landing failure carrying its category up to whoever records it.
///
/// Anything raised by the crate's own error type converts to `Environment` by
/// construction, so an unclassified failure is never silently labeled as one
/// of the named ones; the steps that do know their category override that
/// default at their own call site.
#[derive(Debug)]
struct LandingError {
    category: LandingCategory,
    error: CogGitHubError,
}

impl LandingError {
    fn of(category: LandingCategory, error: CogGitHubError) -> Self {
        Self { category, error }
    }
}

impl std::fmt::Display for LandingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl From<CogGitHubError> for LandingError {
    fn from(error: CogGitHubError) -> Self {
        Self {
            category: LandingCategory::Environment,
            error,
        }
    }
}

impl From<std::io::Error> for LandingError {
    fn from(error: std::io::Error) -> Self {
        Self::from(CogGitHubError::from(error))
    }
}

/// Which of the two answers the contribution gate gave.
///
/// The gate is asked one question — may this change reach the public branch? —
/// and refuses for two different reasons: it read the paths and they are not
/// acceptable, or it could not read the paths at all. The first is the change's
/// own property and the second is not, so they leave as different categories
/// even though one function produced both errors.
fn contribution_refusal_category(error: &CogGitHubError) -> LandingCategory {
    match error {
        CogGitHubError::DiffUnreadable(_) => LandingCategory::UnreadableDiff,
        _ => LandingCategory::Path,
    }
}

/// Carry a refusal the caller may act on across the trait boundary.
///
/// It leaves as [`SFError::Validation`], the type for an input that cannot
/// pass, and every other category stays the internal error an unclassified
/// failure has always been. One flattened variant leaves the caller unable to
/// tell "re-driving repeats this refusal" from "try again once the host is
/// free" -- on 2026-09-27 that flattening cost a change the whitelist had
/// already refused seven release builds in just under two hours.
fn refusal_error(error: LandingError) -> SFError {
    if redriving_repeats_the_refusal(error.category) {
        SFError::Validation(error.to_string())
    } else {
        SFError::Internal(error.to_string())
    }
}

/// Whether re-driving a change the channel refused would reach the same answer.
///
/// The question is not whether the refusal is final but whether re-driving the
/// change would reach it again, because re-driving is not free. The queue keeps
/// no verdicts: a `.diff` left in it is read back next cycle as a change nobody
/// has looked at yet and rebuilt from scratch -- apply, the whole-workspace
/// test, the release build -- and each of those holds the single build slot the
/// deployer needs to advance.
///
/// An unreadable diff stays retryable: nothing was refused about the change,
/// because nothing about it could be read. Filing it as a path refusal would
/// buy a malformed diff the same terminal treatment as a whitelist violation,
/// and the queue is where a re-serialised diff would be offered again from.
///
/// A size refusal is on that side, and for the same reason a path refusal is:
/// the cap is computed from the change's own content, which does not change
/// between cycles, so the verdict cannot move. It used to be kept retryable
/// because owner approval waives the cap, and an entry left in the queue was
/// meant to be where that approval would find it -- but approval does not read
/// the queue, nothing carries the entry to it, and all the entry bought was
/// another rebuild. On 2026-10-04 a change at 211 lines against the 200-line
/// cap was refused at 22:17 and again at 22:30, off two rounds of test plus
/// release build, with the deployer and every other pending change behind it.
///
/// Terminal and retry-worthy are two questions, not one. The cap is still the
/// one gate approval lifts, and the record keeps the change after it leaves the
/// queue, so the owner's door can still be built on the record; what the queue
/// must not do is re-drive a change it has already answered for.
///
/// An exhaustive match, so a category added to [`LandingCategory`] cannot reach
/// a caller without being answered for here first.
fn redriving_repeats_the_refusal(category: LandingCategory) -> bool {
    match category {
        // The rules owner approval does not lift, and the cap, which approval
        // lifts but which nothing carries back to the queue.
        LandingCategory::Path | LandingCategory::Oversized => true,
        // The verdict says nothing about the change, so a later attempt at the
        // same content could still be read, applied against a base that has
        // moved back, or pushed into a race it wins.
        LandingCategory::UnreadableDiff
        | LandingCategory::Conflict
        | LandingCategory::Raced
        | LandingCategory::Rejected
        // git, fetch, worktree, commit: the host, not the change.
        | LandingCategory::Environment => false,
    }
}

/// Where the channel keeps everything it has to remember across a restart.
pub fn data_dir() -> PathBuf {
    PathBuf::from(
        std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into()),
    )
}

/// Directory holding one `<change_id>.json` file per landing.
pub fn landing_dir() -> PathBuf {
    data_dir().join("landing")
}

/// Where a landing's scratch working tree is built.
///
/// A separate worktree rather than the shared clone: landings check out,
/// rewrite, and commit, while the shared clone is being fetched concurrently
/// by PR diffing.
fn landing_worktree() -> PathBuf {
    landing_dir().join("worktree")
}

fn record_path(change_id: &str) -> PathBuf {
    landing_dir().join(format!("{}.json", crate::pending_changes::slug(change_id)))
}

/// Lifecycle of one change in the landing channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LandingState {
    /// Generated but not yet verified by a sandbox, so not landable. Recorded
    /// so it is visible and can be flushed once a mainline verifies it (or the
    /// owner approves it explicitly).
    Unverified,
    /// Committed to the base branch; CI on that commit is being watched.
    Landed,
    /// Verification rejected it, so it will never land. Terminal: the record
    /// stays as the audit trail, but it is out of the verification queue and
    /// out of the owner's approval list.
    Retired,
}

/// One change's landing record, persisted so an intent survives a restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LandingRecord {
    /// The change itself, so a flush or a re-drive needs no second source.
    pub change: GeneratedChange,
    /// Base branch the commit went to.
    pub base: String,
    /// Commit that landed on the base branch (empty while unverified).
    #[serde(default)]
    pub landed_rev: String,
    /// Where the change sits in the landing lifecycle.
    pub state: LandingState,
    /// Whether the CI failure for this landing has already been reported.
    #[serde(default)]
    pub failure_recorded: bool,
    /// Whether generation has already been re-driven from this landing's CI
    /// failure. One attempt only.
    #[serde(default)]
    pub redriven: bool,
    /// Whether it has already been reported that this record never reached the
    /// land step. The report is a latch rather than a level so a change that is
    /// simply old does not re-announce itself every pass.
    #[serde(default)]
    pub unlanded_reported: bool,
    /// Whether this landing's red CI has already been traced to the commit it
    /// was replayed onto. A latch for the same reason as `unlanded_reported`:
    /// the record stays watched for as long as the window lasts, and the
    /// reading counts landings, not passes over one.
    #[serde(default)]
    pub inherited_ci_reported: bool,
    /// Why verification settled this change as one that must not land. Set
    /// together with `Retired`, and the only record of a verdict that leaves
    /// no commit behind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_reason: Option<String>,
    /// When the change first entered the channel.
    pub created_at: DateTime<Utc>,
    /// Last time this record changed; the CI watch window is measured from it.
    pub updated_at: DateTime<Utc>,
}

/// Save a record (idempotent per change id).
pub async fn save_record(record: &LandingRecord) -> SFResult<()> {
    let dir = landing_dir();
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| SFError::IO(format!("create landing dir: {e}")))?;
    let json = serde_json::to_string_pretty(record)
        .map_err(|e| SFError::Internal(format!("serialize landing record: {e}")))?;
    tokio::fs::write(record_path(&record.change.change_id), json)
        .await
        .map_err(|e| SFError::IO(format!("write landing record: {e}")))
}

/// Every landing record, oldest first. Unreadable entries are skipped.
pub async fn load_records() -> Vec<LandingRecord> {
    let mut entries: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    let Ok(mut it) = tokio::fs::read_dir(landing_dir()).await else {
        return Vec::new();
    };
    while let Ok(Some(file)) = it.next_entry().await {
        let path = file.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let mtime = file
            .metadata()
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        entries.push((mtime, path));
    }
    entries.sort_by_key(|(mtime, _)| *mtime);

    let mut records = Vec::new();
    for (_, path) in entries {
        if let Ok(text) = tokio::fs::read_to_string(&path).await {
            if let Ok(record) = serde_json::from_str::<LandingRecord>(&text) {
                records.push(record);
            }
        }
    }
    records
}

/// Drop a record once its landing is settled (green, reverted, or abandoned).
pub async fn remove_record(change_id: &str) {
    let _ = tokio::fs::remove_file(record_path(change_id)).await;
}

/// Load the record for one change, if any.
pub async fn load_record(change_id: &str) -> Option<LandingRecord> {
    let text = tokio::fs::read_to_string(record_path(change_id))
        .await
        .ok()?;
    serde_json::from_str(&text).ok()
}

/// One upstream the channel lands onto.
///
/// The same repository is mirrored on more than one host so that either can
/// answer for the other, and a commit that reached only one of them is not that
/// mirror — it is one host holding a history the other never saw. So every
/// target is pushed, and what each of them said is kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTarget {
    /// Remote name inside the working copy. The first target is `origin`: the
    /// one the worktree tracks and the one the deployer advances the bare main
    /// from.
    pub remote: String,
    /// Platform slug of the gateway route, and the directory the host is
    /// mirrored under: `github` or `gitee`.
    pub slug: String,
    /// Repository on that host, `owner/repo`.
    pub repo: String,
    /// Branch the commit lands on there.
    pub base: String,
}

impl PushTarget {
    /// The target the GitHub integration names: what the channel has always
    /// landed onto.
    pub fn github(config: &GitHubIntegrationConfig) -> Self {
        Self {
            remote: "origin".into(),
            slug: GITHUB_SLUG.into(),
            repo: config.repo.clone(),
            base: config.base_branch.clone(),
        }
    }

    /// The same repository on Gitee.
    pub fn gitee(config: &GiteeIntegrationConfig) -> Self {
        Self {
            remote: GITEE_SLUG.into(),
            slug: GITEE_SLUG.into(),
            repo: config.repo.clone(),
            base: config.base_branch.clone(),
        }
    }

    /// The metric label. The remote name, because that is the word the working
    /// copy and the push errors use.
    fn label(&self) -> &str {
        &self.remote
    }
}

/// The mirrors a landing must also reach, derived from the two integrations.
///
/// Empty when the Gitee integration is off, or when it names a different
/// repository: what is being mirrored is one repository onto two hosts, and a
/// second repository would take the commit as a copy rather than as the same
/// history. A configured mirror that cannot be used is said out loud — the
/// alternative is a deployment that believes it lands onto two hosts while it
/// lands onto one.
pub fn mirror_targets(
    github: &GitHubIntegrationConfig,
    gitee: &GiteeIntegrationConfig,
) -> Vec<PushTarget> {
    if !gitee.enabled {
        return Vec::new();
    }
    if gitee.repo != github.repo {
        warn!(
            github_repo = %github.repo,
            gitee_repo = %gitee.repo,
            "gitee_integration names another repository; not mirroring landings onto it"
        );
        return Vec::new();
    }
    vec![PushTarget::gitee(gitee)]
}

/// The landing channel: commits verified changes to the base branch and
/// records what it did so a red CI run can be answered later.
pub struct MainChannel {
    workdir: PathBuf,
    config: GitHubIntegrationConfig,
    /// Everything a landed commit has to reach, in push order: the primary
    /// first, the mirrors after it. The order is not cosmetic — a commit that
    /// loses the race for the base branch must not be on a mirror either, or
    /// the two hosts hold different histories instead of the same one.
    mirrors: Vec<PushTarget>,
    provider: Arc<dyn CodePlatformProvider>,
    controller: Arc<ContributionController>,
    /// Serializes landings: each one rebuilds a scratch worktree, so two
    /// concurrent landings (mainline + owner flush) would clobber each other.
    gate: tokio::sync::Mutex<()>,
    /// Where landing failures are counted. Set once, from `start`, because the
    /// storage plugin that publishes the backend inits in a later layer than
    /// this plugin and the channel is built during `init`; none until then, in
    /// which case a failure stays a log line.
    metrics: OnceLock<Arc<dyn cog_core::MetricsBackend>>,
    /// Whether this process has already published the fate counter's empty
    /// classes. The seed has to happen once per process and not once per tick:
    /// recording a zero still appends a sample row, and the fate domain is 18
    /// series wide, so a per-tick seed costs tens of thousands of rows a day
    /// in a table that is capped and shared with every other reading.
    ///
    /// Read and set by the publisher, which lives in the funnel module because
    /// the domain it walks is defined there.
    pub(crate) fate_domain_seeded: std::sync::atomic::AtomicBool,
    /// The census count this process last wrote for each cell, keyed by
    /// (entry point, stage), with the time it got through.
    ///
    /// A gauge's value is its newest sample, so a count the store already holds
    /// is a row that says nothing about the count. It still says something
    /// about the writer, though: the companion the store renders is the only
    /// reading that separates a quiet series from an abandoned one, and it can
    /// only say "still there" for a cell its writer keeps stamping. So the last
    /// count and the time it was written are both kept — the count turns the
    /// 36-rows-per-tick flood into a write when a count moves, and the time
    /// turns a cell that never moves into one row per heartbeat. A failed write
    /// is not recorded, so the next tick retries it — the cell would otherwise
    /// sit at a value this process never got through.
    ///
    /// Written by the publisher, which lives in the funnel module because the
    /// cells it walks are defined there.
    pub(crate) census_written:
        std::sync::Mutex<std::collections::HashMap<(String, String), (u64, std::time::Instant)>>,
}

impl std::fmt::Debug for MainChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MainChannel")
            .field("workdir", &self.workdir)
            .field("base", &self.config.base_branch)
            .field("mirrors", &self.mirrors)
            .finish_non_exhaustive()
    }
}

impl MainChannel {
    /// Create a channel landing onto `config.base_branch` from `workdir`.
    pub fn new(
        workdir: impl Into<PathBuf>,
        config: GitHubIntegrationConfig,
        provider: Arc<dyn CodePlatformProvider>,
        controller: Arc<ContributionController>,
    ) -> Self {
        Self {
            workdir: workdir.into(),
            config,
            mirrors: Vec::new(),
            provider,
            controller,
            gate: tokio::sync::Mutex::new(()),
            metrics: OnceLock::new(),
            fate_domain_seeded: std::sync::atomic::AtomicBool::new(false),
            census_written: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Land onto these mirrors as well: every commit the primary takes is
    /// pushed there too, and a mirror that refuses is counted rather than
    /// silently dropped.
    pub fn with_mirrors(mut self, mirrors: impl IntoIterator<Item = PushTarget>) -> Self {
        self.mirrors = mirrors.into_iter().collect();
        self
    }

    /// Count landing failures into `metrics`. Called once, from the plugin's
    /// `start`, after every plugin has initialised and the backend exists.
    pub fn attach_metrics(&self, metrics: Arc<dyn cog_core::MetricsBackend>) {
        let _ = self.metrics.set(metrics);
    }

    /// The metrics backend, once `start` has attached it. `None` before that,
    /// when the channel is built during `init`: a reading published from a
    /// layer above has to tolerate the window rather than panic in it.
    pub(crate) fn metrics_handle(&self) -> Option<Arc<dyn cog_core::MetricsBackend>> {
        self.metrics.get().cloned()
    }

    /// The base branch this channel commits to.
    pub fn base_branch(&self) -> &str {
        &self.config.base_branch
    }

    /// How often the CI watch loop re-reads landed commits' CI state.
    pub fn ci_poll_interval_secs(&self) -> u64 {
        self.config.landing_policy.ci_poll_interval_secs
    }

    /// Commit `change` onto the base branch and return the landed commit.
    ///
    /// Idempotent per change: a commit already carrying the change's
    /// `Change-Id` trailer on the base branch is returned instead of a second
    /// commit being made. When the push loses a race against a newer base tip
    /// the change is re-applied onto the fresh tip, up to
    /// `landing_policy.max_land_attempts` times.
    pub async fn land(
        &self,
        change: &GeneratedChange,
        source: Option<&LandedSource>,
    ) -> Result<String> {
        // The category is dropped here rather than at `land_inner`: this is a
        // public signature, and `LandingError` is crate-private. Whoever needs
        // the category calls `land_inner` -- the trait boundary below does.
        self.land_inner(change, source, false)
            .await
            .map_err(|failure| failure.error)
    }

    /// Land a change the owner approved explicitly.
    ///
    /// The click is a human decision, so the quality gates (self-review score,
    /// size cap) do not apply — the owner already took responsibility for
    /// them. The safety gates (whitelist, forbidden paths) still do: approving
    /// a contribution is not the same as approving writing to `deploy/`.
    pub async fn land_approved(&self, change: &GeneratedChange) -> Result<String> {
        self.land_inner(change, None, true)
            .await
            .map_err(|failure| failure.error)
    }

    async fn land_inner(
        &self,
        change: &GeneratedChange,
        source: Option<&LandedSource>,
        owner_approved: bool,
    ) -> std::result::Result<String, LandingError> {
        match self.land_attempt(change, source, owner_approved).await {
            Ok(rev) => Ok(rev),
            Err(failure) => {
                // Counted here, once, and the pair travels on: the caller that
                // can act on the category is a layer above, and it can only do
                // so if this does not flatten the failure on its way out.
                self.note_landing_failure(failure.category).await;
                Err(failure)
            }
        }
    }

    /// One landing call, with the category of a failure kept alongside it.
    async fn land_attempt(
        &self,
        change: &GeneratedChange,
        source: Option<&LandedSource>,
        owner_approved: bool,
    ) -> std::result::Result<String, LandingError> {
        self.check_policy(change, owner_approved)?;
        let base = self.config.base_branch.clone();
        let attempts = self.config.landing_policy.max_land_attempts.max(1);
        let _guard = self.gate.lock().await;

        let mut last_race = None;
        for attempt in 0..attempts {
            match self.attempt_land(change, source, &base).await? {
                LandOutcome::Landed(rev) => {
                    self.record_landed(change, &base, &rev).await?;
                    tracing::info!(
                        change_id = %change.change_id,
                        rev = %rev,
                        base,
                        attempt,
                        "change landed on base branch"
                    );
                    return Ok(rev);
                }
                LandOutcome::AlreadyLanded(rev) => {
                    self.record_landed(change, &base, &rev).await?;
                    tracing::info!(
                        change_id = %change.change_id,
                        rev = %rev,
                        "change already on the base branch; not landing twice"
                    );
                    return Ok(rev);
                }
                LandOutcome::Superseded(e) => {
                    tracing::info!(
                        change_id = %change.change_id,
                        attempt,
                        error = %e,
                        "base branch moved during landing; re-applying on the new tip"
                    );
                    last_race = Some(e);
                }
            }
        }
        Err(LandingError::of(
            LandingCategory::Raced,
            last_race.unwrap_or_else(|| {
                CogGitHubError::Provider(format!(
                    "landing {} lost the race for {base} {attempts} times",
                    change.change_id
                ))
            }),
        ))
    }

    /// Count one failed landing under its category. A failure to record is
    /// logged and dropped: losing the reading must not turn a failed landing
    /// into a different failure.
    async fn note_landing_failure(&self, category: LandingCategory) {
        let Some(metrics) = self.metrics.get().cloned() else {
            return;
        };
        let labels = HashMap::from([("category".to_string(), category.as_str().to_string())]);
        if let Err(e) = metrics
            .record_counter(LANDING_FAILURES_METRIC, 1.0, labels)
            .await
        {
            warn!(
                category = category.as_str(),
                "cannot record landing failure: {e}"
            );
        }
    }

    /// Count one red landing whose failing checks were already failing on the
    /// commit it was replayed onto.
    ///
    /// No labels: the interesting part is that it happened at all and how
    /// often, and the checks that were passed through are named in the warn the
    /// caller writes next to this. Logged and dropped on failure, like the
    /// counters around it.
    async fn note_inherited_ci_failure(&self) {
        let Some(metrics) = self.metrics_handle() else {
            return;
        };
        if let Err(e) = metrics
            .record_counter(LANDING_CI_FAILURE_INHERITED_METRIC, 1.0, HashMap::new())
            .await
        {
            warn!("cannot record an inherited CI failure: {e}");
        }
    }

    /// Count one re-drive that was not submitted, under the reason it was not.
    ///
    /// A refused re-drive is generation being switched off for a cause, and
    /// the only reason it is ever noticed is this counter: the alternative
    /// reading of the same quiet is "nothing needed a fix". Logged and dropped
    /// on failure, like the landing failure counter, and for the same reason.
    pub(crate) async fn note_redrive_refusal(&self, reason: crate::redrive_budget::RedriveRefusal) {
        let Some(metrics) = self.metrics.get().cloned() else {
            return;
        };
        let labels = HashMap::from([("reason".to_string(), reason.as_str().to_string())]);
        if let Err(e) = metrics
            .record_counter(crate::redrive_budget::REDRIVE_REFUSALS_METRIC, 1.0, labels)
            .await
        {
            warn!(
                reason = reason.as_str(),
                "cannot record a refused re-drive: {e}"
            );
        }
    }

    /// Count one round charge the ledger lost, under the side that lost it.
    ///
    /// This is the counter that keeps the budget honest about itself: a read
    /// that failed grants the rounds a fresh ledger would, and a write that
    /// failed leaves a round uncharged, and neither one shows up anywhere else.
    /// Logged and dropped on failure, like the counters above.
    pub(crate) async fn note_budget_loss(&self, side: crate::redrive_budget::BudgetSide) {
        let Some(metrics) = self.metrics.get().cloned() else {
            return;
        };
        let labels = HashMap::from([("side".to_string(), side.as_str().to_string())]);
        if let Err(e) = metrics
            .record_counter(
                crate::redrive_budget::REDRIVE_BUDGET_LOSSES_METRIC,
                1.0,
                labels,
            )
            .await
        {
            warn!(
                side = side.as_str(),
                "cannot record a lost budget charge: {e}"
            );
        }
    }

    async fn record_landed(&self, change: &GeneratedChange, base: &str, rev: &str) -> Result<()> {
        let now = Utc::now();
        let existing = load_record(&change.change_id).await;
        // The landing is counted on its edge: a second call for a change
        // already on the branch is the idempotency check working, not a second
        // landing.
        let first_landing = crate::change_funnel::is_first_landing(existing.as_ref());
        let created = existing.map(|r| r.created_at).unwrap_or(now);
        save_record(&LandingRecord {
            change: change.clone(),
            base: base.to_string(),
            landed_rev: rev.to_string(),
            state: LandingState::Landed,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            inherited_ci_reported: false,
            retired_reason: None,
            created_at: created,
            updated_at: now,
        })
        .await
        .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
        // A change that was staged awaiting a decision is no longer pending:
        // the owner's click (or the policy) put it on the branch.
        crate::pending_changes::remove_staged(&change.change_id).await;
        if first_landing {
            self.note_change_fate(change, crate::change_funnel::FunnelFate::Landed)
                .await;
        }
        Ok(())
    }

    /// Reject a change that must not reach the public branch, before any git
    /// operation runs. Fail closed: an unparseable diff or an unmeasured
    /// change is held back rather than trusted.
    fn check_policy(
        &self,
        change: &GeneratedChange,
        owner_approved: bool,
    ) -> std::result::Result<(), LandingError> {
        let policy = &self.config.landing_policy;
        ensure_contribution_allowed(&change.content)
            .map_err(|e| LandingError::of(contribution_refusal_category(&e), e))?;

        if !owner_approved {
            let changed_lines = count_changed_lines(&change.content);
            if changed_lines > policy.max_changed_lines {
                return Err(LandingError::of(
                    LandingCategory::Oversized,
                    CogGitHubError::PrivacyRejected(format!(
                        "change {} touches {changed_lines} lines, over the {}-line cap",
                        change.change_id, policy.max_changed_lines
                    )),
                ));
            }
        }

        let files = affected_files(&change.content)
            .map_err(|e| LandingError::of(contribution_refusal_category(&e), e))?;
        for file in &files {
            if let Some(pattern) = policy
                .forbidden_paths
                .iter()
                .find(|p| path_forbidden(file, p))
            {
                return Err(LandingError::of(
                    LandingCategory::Path,
                    CogGitHubError::PrivacyRejected(format!(
                        "change {} touches forbidden path {file} (pattern {pattern})",
                        change.change_id
                    )),
                ));
            }
        }
        Ok(())
    }

    /// One landing attempt against the current remote tip.
    async fn attempt_land(
        &self,
        change: &GeneratedChange,
        source: Option<&LandedSource>,
        base: &str,
    ) -> std::result::Result<LandOutcome, LandingError> {
        // Mirror repair happens here rather than only where a push was refused:
        // a change that has landed is never pushed again, so the mirror that
        // refused it would stay behind for good. What this leaves unrepaired is
        // what the two verdicts below read, and neither of them may say "in
        // both hosts" while it is non-empty.
        let unrepaired = self.catch_up_mirrors(base).await?;
        self.note_mirror_failures(&unrepaired).await;

        if let Some(rev) = self.landed_rev_on(base, &change.change_id).await? {
            // Finding the change on the primary is not the same as it having
            // landed: a change half-landed by an earlier round is found here
            // too, and answering `AlreadyLanded` would report a pair that the
            // mirror never completed — for good, since nothing lands it again.
            if !unrepaired.is_empty() {
                return Err(mirror_error(&unrepaired, &rev));
            }
            return Ok(LandOutcome::AlreadyLanded(rev));
        }
        let wt = self.fresh_worktree(base).await?;

        match source {
            // The sandbox built and deployed this commit: it, not the original
            // diff, is the verified truth, so replay its delta onto the base.
            Some(src) => {
                // The sandbox commits on a detached worktree HEAD, so the rev
                // hangs off no advertised ref and `upload-pack` refuses to
                // serve it by SHA. Pin it under a private ref in the source
                // repo first; the ref is left behind on purpose — it also keeps
                // the object alive for the promotion pipeline that publishes
                // the same commit afterwards.
                let repo = src.repo.to_string_lossy().to_string();
                let rev = run_git(
                    &src.repo,
                    &["rev-parse", &format!("{}^{{commit}}", src.rev)],
                )
                .await?
                .trim()
                .to_string();
                let pinned = format!(
                    "refs/cogneva/landing/{}",
                    crate::pending_changes::slug(&change.change_id)
                );
                run_git(&src.repo, &["update-ref", &pinned, &rev]).await?;
                run_git(&wt, &["fetch", "--no-tags", &repo, &pinned]).await?;
                // This is the step the base branch moving under the change
                // breaks: a delta generated against an older tip no longer
                // applies to the new one.
                run_git(&wt, &["cherry-pick", "--no-commit", "FETCH_HEAD"])
                    .await
                    .map_err(|e| LandingError::of(LandingCategory::Conflict, e))?;
            }
            None => {
                // Owner-approved change applied as a patch. Only its own
                // hunks are taken, so a stale diff cannot drag unrelated
                // repository state along.
                let patch = tempfile::NamedTempFile::new()?;
                std::fs::write(patch.path(), &change.content)?;
                run_git(
                    &wt,
                    &[
                        "apply",
                        "--whitespace=nowarn",
                        &patch.path().to_string_lossy(),
                    ],
                )
                .await
                .map_err(|e| LandingError::of(LandingCategory::Conflict, e))?;
            }
        }
        run_git(&wt, &["add", "-A"]).await?;
        self.commit(&wt, &commit_message(&self.config.bot_identity, change))
            .await?;
        let rev = run_git(&wt, &["rev-parse", "HEAD"])
            .await?
            .trim()
            .to_string();
        match self.push(&wt, &rev).await {
            Ok(refusals) if refusals.is_empty() => Ok(LandOutcome::Landed(rev)),
            Ok(refusals) => {
                // The commit is on the base branch, but a mirror did not take
                // it, so the pair is not level and this is not a landing. It is
                // reported as a failure rather than swallowed: the caller
                // deploys on this verdict, and a round that says "landed" while
                // one host is missing the commit is the reading that made the
                // missing mirror invisible in the first place. The next attempt
                // repairs the mirror above and answers `AlreadyLanded`.
                self.note_mirror_failures(&refusals).await;
                Err(mirror_error(&refusals, &rev))
            }
            Err(PushFailure::Raced(msg)) => {
                Ok(LandOutcome::Superseded(CogGitHubError::Provider(msg)))
            }
            Err(PushFailure::Rejected(msg)) => Err(LandingError::of(
                LandingCategory::Rejected,
                CogGitHubError::Provider(msg),
            )),
        }
    }

    /// The commit already on `origin/<base>` carrying this change's trailer.
    async fn landed_rev_on(&self, base: &str, change_id: &str) -> Result<Option<String>> {
        run_git(&self.workdir, &["fetch", "origin", base]).await?;
        let pattern = format!("^{CHANGE_ID_TRAILER}: {}$", escape_regex(change_id));
        let out = run_git(
            &self.workdir,
            &[
                "log",
                &format!("origin/{base}"),
                "--format=%H",
                "--max-count=1",
                "--extended-regexp",
                &format!("--grep={pattern}"),
            ],
        )
        .await?;
        Ok(out.lines().next().map(|s| s.to_string()))
    }

    /// A scratch working tree sitting on the current remote tip.
    ///
    /// Rebuilt from scratch every attempt: a worktree left dirty or stale by a
    /// previous landing is discarded rather than repaired, so an attempt can
    /// never build on state it did not create.
    async fn fresh_worktree(&self, base: &str) -> Result<PathBuf> {
        let wt = landing_worktree();
        let wt_arg = wt.to_string_lossy().to_string();
        let _ = run_git(&self.workdir, &["worktree", "remove", "--force", &wt_arg]).await;
        let _ = tokio::fs::remove_dir_all(&wt).await;
        let _ = run_git(&self.workdir, &["worktree", "prune"]).await;
        run_git(&self.workdir, &["fetch", "origin", base]).await?;
        run_git(
            &self.workdir,
            &[
                "worktree",
                "add",
                "--detach",
                &wt_arg,
                &format!("origin/{base}"),
            ],
        )
        .await?;
        Ok(wt)
    }

    async fn commit(&self, dir: &Path, message: &str) -> Result<()> {
        let identity = &self.config.bot_identity;
        run_git(
            dir,
            &[
                "-c",
                &format!("user.name={}", identity.git_author_name()),
                "-c",
                &format!("user.email={}", identity.git_author_email()),
                "commit",
                "-m",
                message,
            ],
        )
        .await
        .map(|_| ())
    }

    /// Push the checked-out commit to the base branch and to every mirror.
    ///
    /// The base branch is pushed first, so a commit that lost the race there is
    /// nowhere rather than on a mirror alone: a mirror holding a commit the
    /// base branch rejected is a fork, and a fork cannot be repaired by the
    /// fast-forward that repairs every other mirror problem. A mirror that
    /// refuses afterwards leaves the mirror *behind*, which is repairable, and
    /// it comes back named for the caller to decide with.
    async fn push(
        &self,
        dir: &Path,
        rev: &str,
    ) -> std::result::Result<Vec<MirrorFailure>, PushFailure> {
        self.push_to(dir, &PushTarget::github(&self.config), rev)
            .await?;
        Ok(self.push_to_mirrors(dir, rev).await)
    }

    /// Push `rev` to each mirror, collecting the refusals.
    async fn push_to_mirrors(&self, dir: &Path, rev: &str) -> Vec<MirrorFailure> {
        let mut refusals = Vec::new();
        for target in &self.mirrors {
            if let Err(failure) = self.push_to(dir, target, rev).await {
                warn!(
                    mirror = target.label(),
                    rev,
                    reason = %failure,
                    "a mirror refused a landed commit; it stays behind until it takes it"
                );
                refusals.push(MirrorFailure {
                    target: target.clone(),
                    failure,
                });
            }
        }
        refusals
    }

    /// Push `rev` to one target's base branch. A lost race against a newer tip
    /// on that branch is separated from a real rejection: only the former is
    /// worth re-applying onto the fresh tip.
    async fn push_to(
        &self,
        dir: &Path,
        target: &PushTarget,
        rev: &str,
    ) -> std::result::Result<(), PushFailure> {
        let (ok, _stdout, stderr) = match run_git_status(
            dir,
            &[
                "push",
                &target.remote,
                &format!("{rev}:refs/heads/{}", target.base),
            ],
        )
        .await
        {
            Ok(out) => out,
            Err(e) => return Err(PushFailure::Rejected(e.to_string())),
        };
        if ok {
            return Ok(());
        }
        if is_push_race(&stderr) {
            return Err(PushFailure::Raced(format!(
                "push to {} on {} lost the race: {}",
                target.base,
                target.label(),
                stderr.trim()
            )));
        }
        Err(PushFailure::Rejected(format!(
            "push to {} on {} rejected: {}",
            target.base,
            target.label(),
            stderr.trim()
        )))
    }

    /// Bring each mirror's base branch up to the primary's, when it is behind.
    ///
    /// The landing itself already pushes to the mirrors, so this is not the
    /// main path: it answers a mirror that refused a push earlier. That mirror
    /// would otherwise stay behind forever, because the change it refused has
    /// landed and nothing pushes again for a commit in the queue's past.
    ///
    /// Fast-forward only. A mirror whose tip is not an ancestor of the
    /// primary's holds a history the primary does not have — two hosts that
    /// disagree about a branch — and picking one of them, or force-pushing over
    /// the other, is not this channel's call to make.
    async fn catch_up_mirrors(
        &self,
        base: &str,
    ) -> std::result::Result<Vec<MirrorFailure>, LandingError> {
        if self.mirrors.is_empty() {
            return Ok(Vec::new());
        }
        // One read of the base branch for all of them, and its failure is the
        // environment's rather than any mirror's: this read goes to the primary
        // host, so a refusal counted against a mirror here would put the wrong
        // host's name in the alert. Same commit for every mirror, too, so
        // reading it per mirror would be the same answer asked N times.
        let head = self.primary_base(base).await?;
        let mut refusals = Vec::new();
        for target in &self.mirrors {
            match self.catch_up_mirror(target, &head).await {
                Ok(()) => {}
                Err(failure) => {
                    warn!(
                        mirror = target.label(),
                        reason = %failure,
                        "a mirror is not level with the base branch"
                    );
                    refusals.push(MirrorFailure {
                        target: target.clone(),
                        failure,
                    });
                }
            }
        }
        Ok(refusals)
    }

    /// The base tip as the primary host serves it, read into the worktree.
    async fn primary_base(&self, base: &str) -> std::result::Result<String, LandingError> {
        run_git(&self.workdir, &["fetch", "origin", base])
            .await
            .map_err(|e| LandingError::of(LandingCategory::Environment, e))?;
        run_git(&self.workdir, &["rev-parse", &format!("origin/{base}")])
            .await
            .map(|out| out.trim().to_string())
            .map_err(|e| LandingError::of(LandingCategory::Environment, e))
    }

    async fn catch_up_mirror(
        &self,
        target: &PushTarget,
        head: &str,
    ) -> std::result::Result<(), PushFailure> {
        let refused = |reason: String| PushFailure::Rejected(reason);
        run_git(
            &self.workdir,
            &["fetch", "--no-tags", &target.remote, &target.base],
        )
        .await
        .map_err(|e| {
            refused(format!(
                "cannot read {} on {}: {e}",
                target.base,
                target.label()
            ))
        })?;
        let tip = self
            .branch_head(&format!("{}/{}", target.remote, target.base))
            .await?;
        if tip == head {
            return Ok(());
        }
        if !self.is_ancestor(&tip, head).await? {
            return Err(refused(format!(
                "{} is not behind {}: it holds {tip}, which is not an ancestor of {head}",
                target.label(),
                GITHUB_SLUG
            )));
        }
        self.push_to(&self.workdir, target, head).await
    }

    /// The commit a ref points at in the landing working copy.
    async fn branch_head(&self, refname: &str) -> std::result::Result<String, PushFailure> {
        run_git(&self.workdir, &["rev-parse", refname])
            .await
            .map(|out| out.trim().to_string())
            .map_err(|e| PushFailure::Rejected(format!("cannot resolve {refname}: {e}")))
    }

    /// Whether `older` is an ancestor of `newer`.
    ///
    /// `merge-base --is-ancestor` answers in its exit code — 1 means no, and
    /// anything above that is the command failing rather than answering — so
    /// the reading is the code and not the text next to it.
    async fn is_ancestor(
        &self,
        older: &str,
        newer: &str,
    ) -> std::result::Result<bool, PushFailure> {
        let output = tokio::process::Command::new("git")
            .args(["merge-base", "--is-ancestor", older, newer])
            .current_dir(&self.workdir)
            .output()
            .await
            .map_err(|e| PushFailure::Rejected(e.to_string()))?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            code => Err(PushFailure::Rejected(format!(
                "cannot tell whether {older} is an ancestor of {newer} (git exited {code:?}): {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))),
        }
    }

    /// Count the mirrors that refused, so a mirror falling behind is a reading
    /// and not just a log line.
    async fn note_mirror_failures(&self, refusals: &[MirrorFailure]) {
        let Some(metrics) = self.metrics.get().cloned() else {
            return;
        };
        for refusal in refusals {
            let labels =
                HashMap::from([("mirror".to_string(), refusal.target.label().to_string())]);
            if let Err(e) = metrics
                .record_counter(MIRROR_PUSH_FAILURES_METRIC, 1.0, labels)
                .await
            {
                warn!(
                    mirror = refusal.target.label(),
                    "cannot record a refused mirror push: {e}"
                );
            }
        }
    }

    /// Revert a landed commit so the base branch returns to green, and return
    /// the revert commit.
    /// The commit a landed revision was replayed onto, read from the local
    /// repository.
    ///
    /// The record stores the base *branch*, which moves, so it cannot say which
    /// tree this revision was built on. The parent of the landing commit is
    /// exactly that tree — the tip as it stood when the landing was made — and
    /// it is in the local object store because the landing commit was created
    /// there. A repository that no longer holds the object answers `None`,
    /// which the caller reads as "cannot attribute" rather than as an
    /// exoneration.
    async fn landed_parent_rev(&self, landed_rev: &str) -> Option<String> {
        if landed_rev.is_empty() {
            return None;
        }
        let rev = run_git(&self.workdir, &["rev-parse", &format!("{landed_rev}^")])
            .await
            .ok()?;
        let rev = rev.trim().to_string();
        (!rev.is_empty()).then_some(rev)
    }

    /// Whether every check that failed on this landing had already failed on
    /// the commit it was replayed onto.
    ///
    /// A revision's CI is about the whole tree, and a landed revision is the
    /// base tip with one change on top of it — so a check the tip was already
    /// failing fails on the landing too. Answering a landing for those is what
    /// lets one broken tip convict every change built on it, in landing order,
    /// including the change that would have repaired it. It is the rule the
    /// workspace gate already applies to tests, at the one place a conviction
    /// still costs a landed commit.
    ///
    /// `false` whenever either side cannot be named: an unreadable attribution
    /// is not an exoneration, so a change with no baseline keeps answering for
    /// what it inherited. That is what the caller did before this existed, and
    /// it stays the fallback.
    ///
    /// Two reds on the same check name is not a claim that the change broke
    /// nothing inside it — a change can add a failure to a check that was
    /// already failing. The record keeps watching for exactly that case: once
    /// the tip is repaired, the same check failing again is a failure this
    /// change owns, and the next pass answers for it.
    async fn ci_failure_is_inherited(&self, record: &LandingRecord) -> bool {
        let Ok(Some(failed_here)) = self
            .provider
            .ci_failed_checks_for_sha(&record.landed_rev)
            .await
        else {
            return false;
        };
        if failed_here.is_empty() {
            // Nothing named failed, so there is nothing to attribute: the red
            // verdict came from a face that cannot name its checks.
            return false;
        }
        let Some(parent) = self.landed_parent_rev(&record.landed_rev).await else {
            return false;
        };
        let Ok(Some(failed_before)) = self.provider.ci_failed_checks_for_sha(&parent).await else {
            return false;
        };
        failed_here
            .iter()
            .all(|check| failed_before.contains(check))
    }

    async fn revert(&self, record: &LandingRecord) -> Result<String> {
        let _guard = self.gate.lock().await;
        let wt = self.fresh_worktree(&record.base).await?;
        run_git(&wt, &["revert", "--no-commit", &record.landed_rev]).await?;
        let message = format!(
            "revert(cogneva): undo change {}\n\n\
             This reverts commit {}.\n\n\
             {CHANGE_ID_TRAILER}: revert-{}\n",
            record.change.change_id, record.landed_rev, record.change.change_id
        );
        self.commit(&wt, &message).await?;
        let rev = run_git(&wt, &["rev-parse", "HEAD"])
            .await?
            .trim()
            .to_string();
        // The revert is a landing like any other: a mirror that keeps the commit
        // it undoes while the base branch undoes it is exactly the state the
        // mirror is not supposed to hold. Unlike a landing, though, a refused
        // mirror does not fail this call. A landing can be re-driven because
        // `landed_rev_on` finds the commit it already made; a revert has no such
        // record, so re-driving it reverts the revert — a second undo the caller
        // never asked for, in place of the one the mirror is missing. The gap is
        // counted here and closed by `catch_up_mirrors` on the next attempt.
        let refusals = self
            .push(&wt, &rev)
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
        self.note_mirror_failures(&refusals).await;
        Ok(rev)
    }
}

/// Result of one landing attempt.
enum LandOutcome {
    /// The change was committed and pushed by this attempt.
    Landed(String),
    /// The base branch already carries this change.
    AlreadyLanded(String),
    /// The base branch moved first; the change must be re-applied on the tip.
    Superseded(CogGitHubError),
}

/// Why a push did not take.
#[derive(Debug, Clone)]
enum PushFailure {
    /// A newer commit reached the branch first.
    Raced(String),
    /// The push is not allowed, or failed for a reason retrying cannot fix.
    Rejected(String),
}

impl std::fmt::Display for PushFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushFailure::Raced(m) | PushFailure::Rejected(m) => f.write_str(m),
        }
    }
}

/// A mirror that did not take a commit the base branch has.
///
/// The mirror is named because "a mirror refused" is not a state anyone can act
/// on: which host, and why, is what tells a credential apart from a network
/// path apart from a mirror that has moved somewhere the primary has not.
#[derive(Debug, Clone)]
struct MirrorFailure {
    target: PushTarget,
    failure: PushFailure,
}

/// The failure a landing reports when a mirror did not take the commit.
///
/// Every refusal is named, not just the first: the hosts can refuse for
/// different reasons, and a report that stops at one of them sends its reader
/// looking for a cause that only explains half of what they will find.
fn mirror_error(refusals: &[MirrorFailure], rev: &str) -> LandingError {
    let named = refusals
        .iter()
        .map(|r| format!("{} ({})", r.target.label(), r.failure))
        .collect::<Vec<_>>()
        .join(", ");
    LandingError::of(
        LandingCategory::Rejected,
        CogGitHubError::Provider(format!(
            "commit {rev} is on the base branch but not on every mirror: {named}"
        )),
    )
}

#[async_trait::async_trait]
impl cog_core::ChangeSink for MainChannel {
    async fn submit_change(&self, change: GeneratedChange) -> SFResult<String> {
        // Owner policy gate: Ask/Local stage the change for an explicit
        // decision instead of landing it.
        if self.controller.should_stage() {
            let path = crate::pending_changes::stage_change(&change).await?;
            tracing::info!(
                change_id = %change.change_id,
                policy = self.controller.policy().as_str(),
                "change staged by contribution policy"
            );
            return Ok(format!("staged:{}", path.display()));
        }
        cog_core::ChangeLanding::record_unverified(self, &change).await?;
        Ok(format!("unverified:{}", change.change_id))
    }
}

#[async_trait::async_trait]
impl cog_core::ChangeLanding for MainChannel {
    async fn land(
        &self,
        change: &GeneratedChange,
        source: Option<&LandedSource>,
    ) -> SFResult<String> {
        self.land_inner(change, source, false)
            .await
            .map_err(refusal_error)
    }

    fn check_contribution_allowed(&self, diff: &str) -> SFResult<()> {
        // The first thing `check_policy` does, on the same text: one function
        // holds the rule and this is a second caller of it rather than a second
        // copy of it, so the answer here and the answer `land` gives cannot
        // drift apart.
        ensure_contribution_allowed(diff)
            .map_err(|e| refusal_error(LandingError::of(contribution_refusal_category(&e), e)))
    }

    /// The same rule, asked for the paths rather than the verdict, and mapped
    /// across the boundary the identical way: a diff that cannot be read has to
    /// arrive as the same refusal whichever of the two questions was asked, or
    /// asking the second one would turn a refusal the caller can act on into an
    /// internal error it can only retry.
    fn contribution_refusal_paths(&self, diff: &str) -> SFResult<Vec<PathBuf>> {
        non_contributable_paths(diff)
            .map(|paths| paths.into_iter().map(PathBuf::from).collect())
            .map_err(|e| refusal_error(LandingError::of(contribution_refusal_category(&e), e)))
    }

    async fn record_unverified(&self, change: &GeneratedChange) -> SFResult<()> {
        let now = Utc::now();
        let created = load_record(&change.change_id)
            .await
            .map(|r| r.created_at)
            .unwrap_or(now);
        save_record(&LandingRecord {
            change: change.clone(),
            base: self.config.base_branch.clone(),
            landed_rev: String::new(),
            state: LandingState::Unverified,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            inherited_ci_reported: false,
            retired_reason: None,
            created_at: created,
            updated_at: now,
        })
        .await
    }

    async fn unverified_changes(&self) -> SFResult<Vec<GeneratedChange>> {
        Ok(load_records()
            .await
            .into_iter()
            .filter(|r| r.state == LandingState::Unverified)
            .map(|r| r.change)
            .collect())
    }

    async fn retire_unverified(&self, change_id: &str, reason: &str) -> SFResult<()> {
        let Some(mut record) = load_record(change_id).await else {
            // Nothing of this channel's to settle: the change reached the
            // verify loop through the local queue instead.
            return Ok(());
        };
        if record.state != LandingState::Unverified {
            // Already settled — a landing that raced this verdict wins, since
            // the change is on the branch either way.
            return Ok(());
        }
        record.state = LandingState::Retired;
        record.retired_reason = Some(reason.to_string());
        record.updated_at = Utc::now();
        save_record(&record).await?;
        // The guards above already made this an edge: only a record that was
        // still unverified reaches here, and it leaves retired.
        self.note_change_fate(&record.change, crate::change_funnel::FunnelFate::Retired)
            .await;
        Ok(())
    }
}

/// One pass over the landing records: settle every landed revision whose CI
/// has finished.
///
/// Green ends the story. Red is answered in the order the change was landed:
/// the failure goes to reflection so the system learns from it, the commit is
/// reverted so the base branch is green again within seconds, and generation
/// is re-driven once from the failure log — a change that breaks CI twice is
/// out of the generator's reach and is left to a human.
///
/// A revision whose CI never reports within `ci_watch_timeout_secs` stops
/// being watched: `None` means no evidence, and waiting forever on a verdict
/// that is not coming would grow the record set without bound.
pub async fn watch_landed(
    channel: &MainChannel,
    reflection: Option<&dyn cog_core::ReflectionEngine>,
    orchestrator: Option<&dyn cog_core::OrchestratorControl>,
) {
    let policy = &channel.config.landing_policy;
    let now = Utc::now();
    for mut record in load_records().await {
        if record.state == LandingState::Retired {
            // Verification settled this one: it never lands, and that is a
            // verdict rather than a stall, so there is nothing to watch and
            // nothing to report.
            continue;
        }
        if record.state != LandingState::Landed || record.landed_rev.is_empty() {
            // A change that was submitted and then never reached the land step
            // leaves this record behind with nothing to move it. The record is
            // the only trace of that change, so a silent skip makes "submitted
            // and abandoned" indistinguishable from "never submitted" on every
            // surface we have. The window is the same one a landed commit is
            // watched for: either way it is "an outcome was expected by now".
            let age = (now - record.updated_at).num_seconds().max(0) as u64;
            if !record.unlanded_reported && age > policy.ci_watch_timeout_secs {
                tracing::warn!(
                    change_id = %record.change.change_id,
                    age_secs = age,
                    state = ?record.state,
                    "a submitted change never reached the land step; without a land or an \
                     explicit approval it stays here forever"
                );
                record.unlanded_reported = true;
                // `updated_at` is deliberately not refreshed: it is the clock
                // this age is measured from, so touching it would reset the
                // staleness the report exists to surface.
                if let Err(e) = save_record(&record).await {
                    tracing::warn!(change_id = %record.change.change_id, error = %e,
                        "could not latch the unlanded report; it will repeat next pass");
                }
            }
            continue;
        }
        let age = (now - record.updated_at).num_seconds().max(0) as u64;
        if age > policy.ci_watch_timeout_secs {
            tracing::warn!(
                change_id = %record.change.change_id,
                rev = %record.landed_rev,
                age_secs = age,
                "no CI verdict for a landed commit within the watch window; no longer watching"
            );
            remove_record(&record.change.change_id).await;
            continue;
        }

        let verdict = match channel
            .provider
            .ci_verdict_for_sha(&record.landed_rev)
            .await
        {
            Ok(verdict) => verdict,
            Err(e) => {
                tracing::warn!(
                    change_id = %record.change.change_id,
                    rev = %record.landed_rev,
                    error = %e,
                    "CI verdict lookup failed; retrying next round"
                );
                continue;
            }
        };

        match verdict {
            // Still running, or no check has reported yet.
            None => {}
            Some(true) => {
                if let Some(engine) = reflection {
                    if let Err(e) = engine
                        .record_change_outcome(&record.change.change_id, true, "")
                        .await
                    {
                        tracing::warn!(error = %e, "recording a green landing failed");
                    }
                }
                tracing::info!(
                    change_id = %record.change.change_id,
                    rev = %record.landed_rev,
                    "landed change is green on the base branch"
                );
                remove_record(&record.change.change_id).await;
            }
            Some(false) => {
                // Whose failure is this, before anything answers for it. The
                // revert below puts the branch back to the tree this revision
                // was replayed onto, so on a failure the tip already had it
                // restores a tree that fails the same checks: the revert costs
                // the change and returns nothing. Recording the outcome and
                // re-driving generation from it cost more than nothing — both
                // point the next round at a change that did not do this.
                if channel.ci_failure_is_inherited(&record).await {
                    if !record.inherited_ci_reported {
                        tracing::warn!(
                            change_id = %record.change.change_id,
                            rev = %record.landed_rev,
                            "landed change is red only on checks that were already failing on the \
                             commit it was replayed onto; keeping the change and watching, since \
                             reverting it would restore the tree that fails"
                        );
                        record.inherited_ci_reported = true;
                        // `updated_at` is left alone: it is the clock the watch
                        // window is measured from, and this is not a change to
                        // what is being watched.
                        if let Err(e) = save_record(&record).await {
                            tracing::warn!(
                                change_id = %record.change.change_id,
                                error = %e,
                                "could not latch the inherited-failure report; it will repeat \
                                 next pass"
                            );
                        }
                        channel.note_inherited_ci_failure().await;
                    }
                    continue;
                }

                // A fetch that failed is not an empty log. Collapsing the two
                // made a broken log-fetch path indistinguishable from a
                // failure with nothing to say, and the re-drive was then
                // refused for evidence that was never asked for. Keep the
                // record and let the next pass retry; the watch-window expiry
                // above is what bounds a fetch that never comes back.
                let log = match channel
                    .provider
                    .ci_failure_log_for_sha(&record.landed_rev)
                    .await
                {
                    Ok(log) => log,
                    Err(e) => {
                        tracing::warn!(
                            change_id = %record.change.change_id,
                            rev = %record.landed_rev,
                            error = %e,
                            "CI failure log could not be fetched; retrying next round"
                        );
                        continue;
                    }
                };

                // Record once: the revert may take several rounds to succeed,
                // and a failure must not be reported once per round.
                if !record.failure_recorded {
                    if let Some(engine) = reflection {
                        if let Err(e) = engine
                            .record_change_outcome(&record.change.change_id, false, &log)
                            .await
                        {
                            tracing::warn!(error = %e, "recording a red landing failed");
                        }
                    }
                    record.failure_recorded = true;
                    record.updated_at = Utc::now();
                    if let Err(e) = save_record(&record).await {
                        tracing::warn!(error = %e, "persisting the landing failure state failed");
                    }
                }

                if policy.revert_on_ci_failure {
                    match channel.revert(&record).await {
                        Ok(rev) => {
                            tracing::warn!(
                                change_id = %record.change.change_id,
                                reverted = %record.landed_rev,
                                revert_rev = %rev,
                                "landed change failed CI; reverted the base branch"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                change_id = %record.change.change_id,
                                error = %e,
                                "revert failed; retrying next round"
                            );
                            continue;
                        }
                    }
                }

                if policy.redrive_on_ci_failure && !record.redriven {
                    let window = chrono::Duration::seconds(policy.redrive_cause_window_secs as i64);
                    // A ledger that cannot be read is carried on with as an
                    // empty one — a filesystem fault must not turn into a halt
                    // in generation — but it is counted, because the budget
                    // that stopped applying is otherwise invisible.
                    let mut ledger = match crate::redrive_budget::load_ledger().await {
                        Ok(ledger) => ledger,
                        Err(e) => {
                            tracing::warn!(error = %e,
                                "could not read the re-drive budget; continuing with an empty one, \
                                 so this cause's rounds start over");
                            channel
                                .note_budget_loss(crate::redrive_budget::BudgetSide::Read)
                                .await;
                            crate::redrive_budget::CauseLedger::default()
                        }
                    };
                    match crate::redrive_budget::decide(
                        &log,
                        &ledger,
                        policy.redrive_max_rounds_per_cause,
                        window,
                        now,
                    ) {
                        crate::redrive_budget::RedriveDecision::Spend { signature } => {
                            // Charged before the generation it pays for: the
                            // charge is what the round costs, and a charge
                            // written after the work would be skipped by a
                            // restart during it. A charge that cannot be
                            // written is logged and the round still runs —
                            // losing the accounting must not turn a fixable
                            // failure into a halt in generation.
                            ledger.spend(&signature, window, now);
                            if let Err(e) = crate::redrive_budget::save_ledger(&ledger).await {
                                tracing::warn!(error = %e,
                                    "could not persist the re-drive budget; this round will be \
                                     charged again the next time this cause fails");
                                channel
                                    .note_budget_loss(crate::redrive_budget::BudgetSide::Write)
                                    .await;
                            }
                            redrive(orchestrator, &record, &log).await;
                        }
                        crate::redrive_budget::RedriveDecision::Refuse(reason) => {
                            tracing::warn!(
                                change_id = %record.change.change_id,
                                reason = reason.as_str(),
                                "not re-driving generation for this CI failure"
                            );
                            channel.note_redrive_refusal(reason).await;
                        }
                    }
                }
                remove_record(&record.change.change_id).await;
            }
        }
    }
}

/// Submit a fix task for a reverted landing, once per landing: the `redriven`
/// flag is persisted, so a restart between the revert and the re-drive does
/// not produce a second identical fix task.
///
/// That flag is not a budget. It stops the second re-drive of one change, and
/// the fix it submits is a new change carrying its own flag, so a fix that
/// also breaks CI opens a fresh round — the chain is what the cause ledger in
/// `redrive_budget` bounds, and this only submits what that decision paid for.
async fn redrive(
    orchestrator: Option<&dyn cog_core::OrchestratorControl>,
    record: &LandingRecord,
    log: &str,
) {
    let Some(orchestrator) = orchestrator else {
        tracing::warn!(
            change_id = %record.change.change_id,
            "no orchestrator; cannot re-drive generation from the CI failure"
        );
        return;
    };
    let goal = format!(
        "A change landed on {} broke CI and was reverted.\n\n\
         Change `{}` (rev {}, reverted). Its goal was:\n{}\n\n\
         Failed CI output:\n{}",
        record.base,
        record.change.change_id,
        record.landed_rev,
        record.change.goal,
        if log.trim().is_empty() {
            "(logs unavailable)"
        } else {
            log
        }
    );
    let task_id = format!(
        "redrive-{}",
        crate::pending_changes::slug(&record.change.change_id)
    );
    // The id is stable per change, so a resubmission for a change that already
    // produced a re-drive is dropped by the idempotent insert. Read the row
    // first: without it the submission reports success and the change is left
    // with no attempt running and no record of why.
    if let Some(existing) = orchestrator.get_task(&task_id).await {
        tracing::warn!(
            change_id = %record.change.change_id,
            task_id = %task_id,
            status = ?existing.status,
            "a re-drive for this change is already in the graph; nothing queued"
        );
        return;
    }
    let task = cog_core::Task::new(
        task_id,
        cog_core::TaskType::Custom("platform_ci_fix".into()),
        serde_json::json!({
            "goal": goal,
            "change_id": record.change.change_id,
            "reverted_rev": record.landed_rev,
            "base_branch": record.base,
            "failure_log": log,
            "evolution_mode": "generate_change",
        }),
    );
    match orchestrator.submit_goal_auto(&goal, vec![task]).await {
        Ok(ids) => tracing::info!(
            change_id = %record.change.change_id,
            tasks = ?ids,
            "re-drove generation once from the CI failure of a reverted change"
        ),
        Err(e) => tracing::warn!(
            change_id = %record.change.change_id,
            error = %e,
            "re-drive submission failed"
        ),
    }
}

/// Commit message for a landed change: what it does, why, and the trailer the
/// landing idempotency check reads.
fn commit_message(identity: &BotIdentityConfig, change: &GeneratedChange) -> String {
    let title = change
        .goal
        .lines()
        .next()
        .unwrap_or("autonomous change")
        .chars()
        .take(70)
        .collect::<String>();
    let mut body = format!(
        "chore(cogneva): land change {}\n\n{title}\n",
        change.change_id
    );
    if let Some(ref rationale) = change.rationale {
        body.push_str(&format!("\n## Rationale\n\n{rationale}\n"));
    }
    if let Some(score) = change.self_review_score {
        // 行文必须自己说清它不是门：这个值没有任何阈值比较，落地与否由别的
        // 判定决定。写成 "Self-review score" 时，读提交体的人会以为这条变更
        // 通过了一次质量门——而那个门在别处，且不看这个数。
        body.push_str(&format!(
            "\nScore the producing squad reported: {score:.2} (informational; no gate reads it)\n"
        ));
    }
    if !change.affected_files.is_empty() {
        body.push_str(&format!(
            "\nAffected files: {}\n",
            change.affected_files.join(", ")
        ));
    }
    body.push_str(&format!(
        "\n{}: {}\n\n{}\n",
        CHANGE_ID_TRAILER,
        change.change_id,
        identity_signoff(identity)
    ));
    body
}

fn identity_signoff(identity: &BotIdentityConfig) -> String {
    format!(
        "Signed-off-by: {} <{}>",
        identity.git_author_name(),
        identity.git_author_email()
    )
}

/// Whitelist gate for public contributions. Every path the diff touches must
/// fall inside the generic, shareable surface: Rust sources under
/// `crates/**/src/`, files under `prompts/`, or the root README/CHANGELOG.
/// Everything else — deploy manifests, configs, secrets, business data — is
/// hard-rejected before anything is pushed. A diff from which no paths can be
/// parsed is rejected as well (fail closed).
pub fn ensure_contribution_allowed(diff: &str) -> Result<()> {
    let denied = non_contributable_paths(diff)?;
    if denied.is_empty() {
        Ok(())
    } else {
        Err(CogGitHubError::PrivacyRejected(format!(
            "change touches non-contributable paths: {} \
             (whitelist: crates/**/src/**/*.rs, prompts/**, root README/CHANGELOG)",
            denied.join(", ")
        )))
    }
}

/// The paths a diff names that lie outside the surface this instance may
/// contribute at all.
///
/// The verdict and its reason are one rule asked two ways: the caller that
/// only needs to know whether to carry on reads the emptiness of this, and the
/// caller that has to file the refusal under its criterion and its files reads
/// the list. Splitting them any other way — a verdict here and a parser there —
/// would let the two answers disagree about the same patch.
pub fn non_contributable_paths(diff: &str) -> Result<Vec<String>> {
    let files = affected_files(diff)?;
    Ok(files.into_iter().filter(|p| !is_allowed_path(p)).collect())
}

/// Files a unified diff touches, as a landing error when none can be read.
///
/// Its own error variant rather than the privacy gate's: both stop the change
/// before it is pushed, but a caller has to be able to tell "these paths are
/// refused" from "there were no paths to refuse", and the variant is the only
/// thing that survives the trip up.
fn affected_files(diff: &str) -> Result<Vec<String>> {
    cog_core::parse_diff_affected_files(diff)
        .map_err(|e| CogGitHubError::DiffUnreadable(e.to_string()))
}

/// Whether a single repo-relative path is inside the contribution whitelist.
fn is_allowed_path(path: &str) -> bool {
    let p = path.strip_prefix("./").unwrap_or(path);

    if let Some(rest) = p.strip_prefix("crates/") {
        // crates/<crate>/src/<...>.rs — the src segment is mandatory and the
        // final path segment must be a Rust source file.
        let mut segs = rest.split('/');
        let crate_seg = segs.next();
        if crate_seg.is_none_or(|s| s.is_empty()) || segs.next() != Some("src") {
            return false;
        }
        return matches!(segs.next_back(), Some(file) if file.ends_with(".rs"));
    }

    if let Some(rest) = p.strip_prefix("prompts/") {
        // Any file under prompts/, but no traversal/empty segments.
        return !rest.is_empty()
            && rest
                .split('/')
                .all(|seg| !seg.is_empty() && seg != "." && seg != "..");
    }

    // Root-level README*/CHANGELOG* only (no slash => repository root).
    if !p.contains('/') {
        let name = p.to_ascii_lowercase();
        return name.starts_with("readme") || name.starts_with("changelog");
    }

    false
}

/// Whether `path` matches a configured `forbidden_paths` entry.
///
/// A leading `*` matches a filename suffix (`.lock` files), a trailing `/`
/// matches a directory prefix, and anything else matches the path exactly or
/// as a prefix. Deliberately small: these are path guards, not a glob engine.
fn path_forbidden(path: &str, pattern: &str) -> bool {
    let p = path.strip_prefix("./").unwrap_or(path);
    if let Some(suffix) = pattern.strip_prefix('*') {
        return p.ends_with(suffix);
    }
    p == pattern || p.starts_with(pattern)
}

/// Changed lines in a unified diff (additions + deletions, headers excluded).
///
/// The shared counter, not a local one: the same number decides this policy's
/// cap and the routing rule that tiers work by declared size, and two counters
/// would let one change be over the cap here and under it there.
fn count_changed_lines(diff: &str) -> usize {
    cog_core::count_diff_lines(diff)
}

/// Escape regex metacharacters so a change id can be embedded in a `git log`
/// grep pattern literally.
fn escape_regex(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\.+*?()|[]{}^$#&-~".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// True when a rejected push failed because the base branch moved, rather than
/// because the credential may not push. Only the former is worth retrying.
fn is_push_race(stderr: &str) -> bool {
    let s = stderr.to_ascii_lowercase();
    s.contains("non-fast-forward") || s.contains("fetch first") || s.contains("stale info")
}

/// True when `workdir` looks like a usable git working copy.
pub async fn is_git_workdir(workdir: &Path) -> bool {
    workdir.join(".git").exists()
}

/// Ensure the configured git workdir is a usable clone of the target repo.
///
/// Missing directories are created by cloning. Remote selection (first match
/// wins): `COGNEVA_GIT_PROXY_BASE` set → `{base}/github/{repo}.git` via the
/// security gateway, which injects credentials on egress so no secret enters
/// this process or the remote URL; `COGNEVA_GITHUB_USE_SSH` set →
/// `ssh://git@github.com:22/...` (authentication from `GIT_SSH_COMMAND`);
/// otherwise an `x-access-token` HTTPS remote when a platform token is
/// available, falling back to anonymous HTTPS.
///
/// In gateway-proxy mode an existing working copy's `origin` is rewritten to
/// the proxy URL (idempotent), so clones made with SSH/token remotes migrate
/// without a re-clone. Other modes leave existing remotes untouched.
///
/// The mirrors are configured on the way through, for the same reason `origin`
/// is: a working copy cloned before a mirror was declared has only the primary,
/// and landing would push to a remote that is not there.
pub async fn ensure_workdir(
    config: &GitHubIntegrationConfig,
    token: Option<&str>,
    mirrors: &[PushTarget],
) -> Result<PathBuf> {
    let workdir = config.git_workdir_path();
    let url = remote_url(config, token);
    let existing = is_git_workdir(&workdir).await;
    if !existing {
        if let Some(parent) = workdir.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let output = tokio::process::Command::new("git")
            .arg("clone")
            .arg(&url)
            .arg(&workdir)
            .output()
            .await?;
        if !output.status.success() {
            // Never leak the token into logs.
            let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            if let Some(t) = token.filter(|t| !t.is_empty()) {
                stderr = stderr.replace(t, "***");
            }
            return Err(CogGitHubError::Provider(format!(
                "git clone failed: {stderr}"
            )));
        }
    } else if git_proxy_base().is_some() {
        set_remote(&workdir, "origin", &url).await?;
    }
    for target in mirrors {
        // A mirror is a second host for the same repository, so it is set up
        // whether or not the working copy is new. A mirror with no route and no
        // credential is reported rather than pushed with nothing: landing onto
        // one host while believing it is two is the state this replaced.
        let Some(url) = mirror_url(target, token) else {
            warn!(
                mirror = target.label(),
                "no git route or credential for this mirror; it will not be pushed"
            );
            continue;
        };
        set_remote(&workdir, &target.remote, &url).await?;
    }
    Ok(workdir)
}

/// Point `name` at `url`, adding the remote when the working copy does not have
/// it yet: `remote set-url` alone fails on a remote that is not there, which is
/// every mirror in a working copy cloned before the mirror was declared.
async fn set_remote(dir: &Path, name: &str, url: &str) -> Result<()> {
    let exists = run_git_status(dir, &["remote", "get-url", name]).await?.0;
    let verb = if exists { "set-url" } else { "add" };
    run_git(dir, &["remote", verb, name, url]).await?;
    Ok(())
}

/// Where one mirror's git remote points.
///
/// Through the gateway when one is configured, and by token otherwise: the
/// mirror is a second host rather than a second route to the first, so its
/// credentials are the second host's, and its URL is built the same way as
/// `origin`'s. What it does *not* share with `origin` is the fallback — an
/// anonymous URL builds a remote that cannot be pushed to, and a fan-out that
/// counts a host it can never reach is the state this replaced.
fn mirror_url(target: &PushTarget, token: Option<&str>) -> Option<String> {
    if target.slug == GITHUB_SLUG {
        return None;
    }
    if let Some(base) = git_proxy_base() {
        return Some(proxy_route(&base, &target.slug, &target.repo));
    }
    token
        .filter(|t| !t.is_empty())
        .map(|t| format!("https://oauth2:{t}@gitee.com/{}.git", target.repo))
}

/// The gateway's route to one host: the proxy base, the platform's own path
/// segment, then the repository.
///
/// Both remotes are built through here, so the route the channel pushes to and
/// the route it reads a mirror's tip from cannot disagree about their shape —
/// a disagreement that would read as a mirror that is not level.
fn proxy_route(base: &str, slug: &str, repo: &str) -> String {
    format!("{}/{slug}/{repo}.git", base.trim_end_matches('/'))
}

fn git_proxy_base() -> Option<String> {
    std::env::var("COGNEVA_GIT_PROXY_BASE")
        .ok()
        .filter(|s| !s.is_empty())
}

fn remote_url(config: &GitHubIntegrationConfig, token: Option<&str>) -> String {
    let repo = config.repo.clone();
    select_remote_url_for(
        config,
        token,
        git_proxy_base().as_deref(),
        std::env::var_os("COGNEVA_GITHUB_USE_SSH").is_some(),
        &repo,
    )
}

/// Build the git remote URL for `target_repo` (`owner/repo`).
fn select_remote_url_for(
    _config: &GitHubIntegrationConfig,
    token: Option<&str>,
    proxy_base: Option<&str>,
    use_ssh: bool,
    target_repo: &str,
) -> String {
    if let Some(base) = proxy_base {
        return proxy_route(base, GITHUB_SLUG, target_repo);
    }
    if use_ssh {
        return format!("ssh://git@github.com:22/{target_repo}.git");
    }
    match token {
        Some(t) => format!("https://x-access-token:{t}@github.com/{target_repo}.git"),
        None => format!("https://github.com/{target_repo}.git"),
    }
}

async fn run_git(dir: &Path, args: &[&str]) -> Result<String> {
    let (ok, stdout, stderr) = run_git_status(dir, args).await?;
    if !ok {
        return Err(CogGitHubError::Provider(format!(
            "git {:?} failed: {}",
            args,
            stderr.trim()
        )));
    }
    Ok(stdout)
}

async fn run_git_status(dir: &Path, args: &[&str]) -> Result<(bool, String, String)> {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .await?;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::ChangeLanding;
    use cog_core::MetricsBackend as _;

    fn diff_touching(paths: &[&str]) -> String {
        paths
            .iter()
            .map(|p| format!("--- a/{p}\n+++ b/{p}\n@@ -1 +1 @@\n-old\n+new\n"))
            .collect()
    }

    fn change(id: &str, diff: &str) -> GeneratedChange {
        GeneratedChange {
            change_id: id.into(),
            goal: "fix the thing".into(),
            content: diff.into(),
            affected_files: vec!["crates/cog-github/src/lib.rs".into()],
            rationale: None,
            pge_mode: "squad".into(),
            self_review_score: Some(0.9),
            issue_number: None,
            intent: Some(cog_core::EvolutionIntent::CiFix),
        }
    }

    fn channel(policy: crate::config::LandingPolicy) -> MainChannel {
        let config = GitHubIntegrationConfig {
            repo: "o/r".into(),
            landing_policy: policy,
            ..Default::default()
        };
        MainChannel::new(
            "/tmp/nonexistent",
            config,
            Arc::new(NullProvider),
            ContributionController::new_shared(),
        )
    }

    /// Policy checks never reach the network, so a provider that always fails
    /// is enough to prove a rejected change is rejected before any git call.
    #[derive(Debug)]
    struct NullProvider;

    #[async_trait::async_trait]
    impl CodePlatformProvider for NullProvider {
        async fn list_open_issues(&self) -> Result<Vec<crate::provider::PlatformIssue>> {
            Ok(Vec::new())
        }
        async fn create_pull_request(
            &self,
            _req: crate::provider::CreatePullRequest,
        ) -> Result<crate::provider::PlatformPullRequest> {
            Err(CogGitHubError::Provider("unused".into()))
        }
        async fn comment_on_issue(&self, _n: u64, _body: String) -> Result<()> {
            Ok(())
        }
        async fn merge_pull_request(&self, _n: u64, _sha: String) -> Result<()> {
            Ok(())
        }
        async fn get_pull_request(&self, _n: u64) -> Result<crate::provider::PullRequestDetail> {
            Err(CogGitHubError::Provider("unused".into()))
        }
    }

    /// A provider whose CI verdict is red and whose failure log answers with a
    /// fixed result: the watch loop's answer to a red verdict depends on
    /// telling a fetch that failed apart from a log that was fetched and came
    /// back empty.
    #[derive(Debug)]
    struct RedCiProvider {
        log: std::result::Result<String, String>,
    }

    #[async_trait::async_trait]
    impl CodePlatformProvider for RedCiProvider {
        async fn list_open_issues(&self) -> Result<Vec<crate::provider::PlatformIssue>> {
            Ok(Vec::new())
        }
        async fn create_pull_request(
            &self,
            _req: crate::provider::CreatePullRequest,
        ) -> Result<crate::provider::PlatformPullRequest> {
            Err(CogGitHubError::Provider("unused".into()))
        }
        async fn comment_on_issue(&self, _n: u64, _body: String) -> Result<()> {
            Ok(())
        }
        async fn merge_pull_request(&self, _n: u64, _sha: String) -> Result<()> {
            Ok(())
        }
        async fn get_pull_request(&self, _n: u64) -> Result<crate::provider::PullRequestDetail> {
            Err(CogGitHubError::Provider("unused".into()))
        }
        async fn ci_verdict_for_sha(&self, _sha: &str) -> Result<Option<bool>> {
            Ok(Some(false))
        }
        async fn ci_failure_log_for_sha(&self, _sha: &str) -> Result<String> {
            self.log.clone().map_err(CogGitHubError::Provider)
        }
    }

    #[test]
    fn whitelist_allows_generic_code_prompt_and_root_docs() {
        assert!(is_allowed_path("crates/cog-github/src/lib.rs"));
        assert!(is_allowed_path(
            "crates/cog-core/src/contract/reflection.rs"
        ));
        assert!(is_allowed_path("./crates/cog-github/src/webhook.rs"));
        assert!(is_allowed_path("prompts/change-generator.md"));
        assert!(is_allowed_path("README.md"));
        assert!(is_allowed_path("CHANGELOG.md"));
    }

    #[test]
    fn whitelist_rejects_private_infra_config_and_secrets() {
        let denied = [
            "deploy/helm/cogneva/values.yaml",
            ".github/workflows/release.yml",
            "crates/cog-github/tests/it.rs",
            "Cargo.toml",
            "cogneva.json",
            "secrets/id_rsa",
            "crates/cog-github/src",
            "prompts/../etc/passwd",
        ];
        for path in denied {
            assert!(!is_allowed_path(path), "should deny {path}");
        }
    }

    #[test]
    fn privacy_gate_accepts_whitelisted_diff() {
        let diff = diff_touching(&[
            "crates/cog-github/src/landing.rs",
            "crates/cog-core/src/lib.rs",
            "prompts/evaluator.md",
            "README.md",
        ]);
        ensure_contribution_allowed(&diff).unwrap();
    }

    #[test]
    fn privacy_gate_rejects_diff_with_any_non_whitelisted_path() {
        let diff = diff_touching(&[
            "crates/cog-github/src/lib.rs",
            "deploy/helm/cogneva/values.yaml",
        ]);
        let err = ensure_contribution_allowed(&diff).unwrap_err();
        match err {
            CogGitHubError::PrivacyRejected(msg) => {
                assert!(msg.contains("deploy/helm/cogneva/values.yaml"));
                assert!(msg.contains("whitelist"));
            }
            other => panic!("expected PrivacyRejected, got {other:?}"),
        }
    }

    /// 判词与它的理由是同一条规则问两次，所以两者必须逐字对上：谁要是各自从
    /// 一份清单里读，同一份 diff 就会同时得到"过"和"被拒的路径"两个答案，而
    /// 拒绝被归档到哪个判据、点名哪些文件，全看问的是哪一次。
    #[test]
    fn the_refused_paths_and_the_verdict_are_one_rule() {
        let diff = diff_touching(&[
            "crates/cog-github/src/lib.rs",
            "deploy/helm/cogneva/values.yaml",
            "deploy/k3s/evolution-configmap.yaml",
        ]);

        let denied = non_contributable_paths(&diff).unwrap();
        assert_eq!(
            denied,
            vec![
                "deploy/helm/cogneva/values.yaml".to_string(),
                "deploy/k3s/evolution-configmap.yaml".to_string(),
            ],
            "被拒的路径按 diff 里出现的顺序点名，放行的那条不在里面"
        );

        let msg = match ensure_contribution_allowed(&diff).unwrap_err() {
            CogGitHubError::PrivacyRejected(msg) => msg,
            other => panic!("expected PrivacyRejected, got {other:?}"),
        };
        for path in &denied {
            assert!(
                msg.contains(path.as_str()),
                "判词里必须出现被判的每一个路径，{path} 却不在：{msg}"
            );
        }
    }

    /// 空集在这一问里是"放行"，不是"没读到"：一份全在白名单里的 diff 不许在
    /// 这条路上留下任何名字，否则调用方会拿着一份不存在的拒绝去归档。
    #[test]
    fn a_whitelisted_diff_refuses_no_paths() {
        let diff = diff_touching(&["crates/cog-core/src/lib.rs", "README.md"]);
        assert!(non_contributable_paths(&diff).unwrap().is_empty());
    }

    /// 提前问到的答案必须与落地给出的那个**同一个**：同一条被点名的路径、同一个
    /// 出口类型。提前判若落成另一条判据，就等于在沙箱前面拒掉一条本来能落的变更。
    ///
    /// 两句都要：被拒的变更在两份读数里都点名同一条路径（同一个判据的两个调用点，
    /// 不是两次各自判断），而白名单内的变更提前放行——少了后一句，"提前一律拒绝"
    /// 也会让前一句全绿。
    #[tokio::test]
    async fn the_early_verdict_is_the_one_landing_gives() {
        let chan = channel(crate::config::LandingPolicy::default());
        let denied = change(
            "c1",
            &diff_touching(&[
                "crates/cog-github/src/lib.rs",
                "crates/cog-github/tests/it.rs",
            ]),
        );

        let early = chan
            .check_contribution_allowed(&denied.content)
            .unwrap_err();
        assert!(
            matches!(early, SFError::Validation(_)),
            "提前判的拒绝要走终局那一支，否则调用方会把它当成环境问题再跑一遍：{early:?}"
        );
        assert!(
            early.to_string().contains("crates/cog-github/tests/it.rs"),
            "点名要落在被拒的路径上：{early}"
        );

        // 走 trait 那一面，因为调用方读到的就是它：`MainChannel` 自己的 `land`
        // 返回本 crate 的错误类型（类别在那一层被丢掉），两种类型不比为凭。
        let late = ChangeLanding::land(&chan, &denied, None).await.unwrap_err();
        assert!(
            matches!(late, SFError::Validation(_)),
            "提前判与落地判要落在同一个出口上，否则调用方对同一个拒绝会用两种做法：{late:?}"
        );
        assert!(
            late.to_string().contains("crates/cog-github/tests/it.rs"),
            "落地给的读数必须点同一条路径：{late}"
        );

        let allowed = change("c2", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        chan.check_contribution_allowed(&allowed.content)
            .expect("白名单内的变更必须提前放行");
    }

    /// 提前判**只**判贡献面：业主自己那两档（`forbidden_paths` 与改动行数上限）留在
    /// 落地那一侧。上限是业主批准就能豁免的那一档，提前把它算成终局，等于替业主做掉
    /// 那个判决——一条他本可放行的变更会在沙箱之前消失。
    ///
    /// 但落地那一侧的出口是 `Validation`：上限算的是变更自己的正文，换个周期重跑一遍
    /// 得到同一句话，所以调用方该做的是把它拿出队列，而不是留着让下一轮再整仓测一遍。
    /// 业主那一档没有消失——记录里留着这条变更，业主的门要建也是建在记录上。
    #[tokio::test]
    async fn the_early_verdict_leaves_the_owners_limits_to_the_landing() {
        let chan = channel(crate::config::LandingPolicy {
            max_changed_lines: 1,
            ..Default::default()
        });
        let big = change(
            "c1",
            &diff_touching(&[
                "crates/cog-github/src/lib.rs",
                "crates/cog-github/src/webhook.rs",
            ]),
        );

        chan.check_contribution_allowed(&big.content)
            .expect("行数上限不归提前判管");

        let late = ChangeLanding::land(&chan, &big, None).await.unwrap_err();
        assert!(
            matches!(late, SFError::Validation(_)),
            "上限是变更自己正文的属性，重跑一次得到同一句话：出口要让调用方把这条变更拿出队列，\
             否则它每一轮都被整仓测试与 release 构建重跑一遍，占死唯一的构建槽：{late:?}"
        );
    }

    #[test]
    fn privacy_gate_fails_closed_on_unparseable_diff() {
        let err = ensure_contribution_allowed("not a diff at all").unwrap_err();
        assert!(
            matches!(err, CogGitHubError::DiffUnreadable(_)),
            "the gate still refuses it, but as an unreadable diff rather than \
             as a whitelist violation it never committed: {err:?}"
        );
    }

    /// A diff the gate could not read must not cross the boundary looking like
    /// a path refusal. The caller retires on `Validation`, so the two arriving
    /// as one type is what took a malformed diff out of the queue -- and the
    /// queue is the only place a re-serialised diff would have been offered
    /// from again.
    ///
    /// Both halves are needed: the unreadable one has to stay retryable, and a
    /// real whitelist violation has to keep leaving as terminal, or the gate
    /// would have been loosened instead of re-read.
    #[tokio::test]
    async fn an_unreadable_diff_is_not_a_path_refusal() {
        let chan = channel(crate::config::LandingPolicy::default());
        let unreadable = change("c1", "not a diff at all");
        assert_eq!(
            chan.check_policy(&unreadable, false).unwrap_err().category,
            LandingCategory::UnreadableDiff
        );
        let early = chan
            .check_contribution_allowed(&unreadable.content)
            .unwrap_err();
        assert!(
            matches!(early, SFError::Internal(_)),
            "an unreadable diff has to stay retryable: {early:?}"
        );

        let denied = change("c2", &diff_touching(&["deploy/cogneva/pg.yaml"]));
        assert_eq!(
            chan.check_policy(&denied, false).unwrap_err().category,
            LandingCategory::Path
        );
        let refused = ChangeLanding::land(&chan, &denied, None).await.unwrap_err();
        assert!(
            matches!(refused, SFError::Validation(_)),
            "a path the whitelist read and refused is still terminal: {refused:?}"
        );
    }

    #[test]
    fn forbidden_path_patterns() {
        assert!(path_forbidden("deploy/helm/values.yaml", "deploy/"));
        assert!(path_forbidden("Cargo.lock", "*.lock"));
        assert!(path_forbidden(
            ".github/workflows/ci.yml",
            ".github/workflows"
        ));
        assert!(!path_forbidden("crates/cog-github/src/lib.rs", "deploy/"));
        assert!(!path_forbidden("docs/notes.md", "*.lock"));
    }

    #[test]
    fn changed_line_count_ignores_diff_headers() {
        let diff = "--- a/x.rs\n+++ b/x.rs\n@@ -1,2 +1,2 @@\n-old\n+new\n context\n";
        assert_eq!(count_changed_lines(diff), 2);
    }

    #[tokio::test]
    async fn policy_holds_back_an_oversized_change() {
        let policy = crate::config::LandingPolicy {
            max_changed_lines: 1,
            ..Default::default()
        };
        let ch = change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        let err = channel(policy).check_policy(&ch, false).unwrap_err();
        assert!(err.to_string().contains("over the 1-line cap"), "{err}");
    }

    #[tokio::test]
    async fn policy_holds_back_a_forbidden_path() {
        let ch = change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        // The whitelist passes this path, so the forbidden list is what stops
        // it — that is the configurable gate being exercised.
        let policy = crate::config::LandingPolicy {
            forbidden_paths: vec!["crates/".into()],
            ..Default::default()
        };
        let err = channel(policy).check_policy(&ch, false).unwrap_err();
        assert!(err.to_string().contains("forbidden path"), "{err}");
    }

    /// Every category, so the loop below covers the whole set. Kept in step
    /// with `redriving_repeats_the_refusal`, whose match is the half that fails
    /// to compile when the enum grows.
    const ALL_CATEGORIES: [LandingCategory; 7] = [
        LandingCategory::Conflict,
        LandingCategory::Raced,
        LandingCategory::Rejected,
        LandingCategory::Path,
        LandingCategory::UnreadableDiff,
        LandingCategory::Oversized,
        LandingCategory::Environment,
    ];

    /// The categories the landing alert rule deliberately does not read, each
    /// with the reason it does not have to. A category in neither this list nor
    /// the rule's selector fails the test below, so a variant added to the enum
    /// has to be either wired into the rule or written off here on purpose --
    /// it cannot fall through into being counted where nobody is listening.
    ///
    /// Both entries are the same shape: a failure the deployment's own machinery
    /// resolves without anyone being told, where the next attempt is expected to
    /// land. Everything else that is retried waits on something no automatic
    /// step supplies -- a diff that has to be re-read, an owner who has to waive
    /// the cap -- and those are read by the rule for exactly that reason.
    const NOT_ALERTED: [(LandingCategory, &str); 2] = [
        (
            LandingCategory::Conflict,
            "another commit reached the branch first; re-applying is expected to land",
        ),
        (
            LandingCategory::Raced,
            "another landing won the push race; the next attempt is expected to land",
        ),
    ];

    /// The category labels the landing alert rule's selector matches, read from
    /// the chart file the rule is delivered from.
    ///
    /// Scanned as text rather than parsed as JSON: the selector is one literal
    /// inside a PromQL expression, so a reader would have to walk to it either
    /// way. Every way this can go wrong -- the rule renamed, the matcher
    /// changed, the label renamed -- has the same consequence, and returning an
    /// empty set for any of them would make the assertions below pass for the
    /// wrong reason, so each one panics with the text it looked for.
    fn alert_selector_categories() -> Vec<String> {
        const CHART: &str = include_str!("../../../deploy/helm/cogneva/files/cogneva.json");
        const MARKER: &str = "cogneva_landing_failures_total{category=~\\\"";
        const END: &str = "\\\"";

        let start = CHART
            .find(MARKER)
            .unwrap_or_else(|| panic!("the chart's alert rules have no {MARKER:?}"))
            + MARKER.len();
        let rest = &CHART[start..];
        let end = rest
            .find(END)
            .unwrap_or_else(|| panic!("the selector after {MARKER:?} is never closed"));
        rest[..end].split('|').map(str::to_owned).collect()
    }

    /// Every category is either read by the landing alert rule or written off
    /// with a reason, and no category is both.
    ///
    /// The rule's selector is what turns these counters into something that
    /// wakes someone up, and nothing about adding a variant to the enum makes
    /// the selector notice: the counters keep being recorded and keep being
    /// read by nobody, which is indistinguishable from the failure never
    /// happening. Checked in both directions, because a selector naming a
    /// category that no longer exists is the same silence from the other side.
    #[test]
    fn every_category_is_either_alerted_or_excluded_with_a_reason() {
        let selected = alert_selector_categories();

        for category in ALL_CATEGORIES {
            let named = selected.iter().any(|l| l == category.as_str());
            let written_off = NOT_ALERTED.iter().any(|(c, _)| *c == category);
            assert!(
                named != written_off,
                "{category:?} is {}: the rule {} it and the exemption list {} \
                 it, so it has to be exactly one of the two (rule reads \
                 {selected:?})",
                if named {
                    "read twice"
                } else {
                    "read by nobody"
                },
                if named { "names" } else { "does not name" },
                if written_off {
                    "lists"
                } else {
                    "does not list"
                },
            );
        }

        for label in &selected {
            assert!(
                ALL_CATEGORIES.iter().any(|c| c.as_str() == label),
                "the rule matches {label:?}, which is not a category any more: \
                 it is a rule for a value nothing produces"
            );
        }

        let alerted = selected.len();
        assert!(
            alerted < ALL_CATEGORIES.len(),
            "the rule reads every category, so the exemption list above is \
             dead and nothing records which failures are meant to be silent"
        );
    }

    /// A refusal the change's own content caused has to leave the channel as
    /// its own kind. The caller is the only layer that can take the change out
    /// of the queue, and it can only tell "re-driving repeats this" from "the
    /// host was busy" if the kind survives the boundary -- it reads that off
    /// the type alone, so exactly the categories re-driving cannot move may
    /// arrive as `Validation` and everything else has to stay `Internal`.
    ///
    /// The set is pinned rather than derived, so widening it is a decision
    /// somebody makes here and not a side effect of a mapping change.
    #[test]
    fn only_a_refusal_re_driving_cannot_move_crosses_as_validation() {
        let mut stuck = Vec::new();
        for category in ALL_CATEGORIES {
            let e = LandingError::of(category, CogGitHubError::PrivacyRejected("refused".into()));
            let mapped = refusal_error(e);
            if redriving_repeats_the_refusal(category) {
                assert!(matches!(mapped, SFError::Validation(_)), "{category:?}");
                stuck.push(category);
            } else {
                assert!(matches!(mapped, SFError::Internal(_)), "{category:?}");
            }
        }
        assert_eq!(
            stuck,
            vec![LandingCategory::Path, LandingCategory::Oversized],
            "the paths approval does not lift, and the cap approval does -- which \
             the queue cannot carry back to the owner anyway, so re-driving it buys \
             a rebuild and nothing else"
        );
    }

    /// The label is what the counter aggregates by, so two categories sharing
    /// one would be counted into a single series and no reader could tell them
    /// apart afterwards -- the exact shape this split exists to undo. Nothing
    /// else fails when a variant is added with a label that is already taken,
    /// because `as_str` is a plain match and the labels are string literals.
    #[test]
    fn every_category_counts_under_a_label_of_its_own() {
        let labels: Vec<&str> = ALL_CATEGORIES.iter().map(|c| c.as_str()).collect();
        let mut sorted = labels.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            labels.len(),
            sorted.len(),
            "two categories share a label: {labels:?}"
        );
        assert!(
            labels.iter().all(|l| !l.is_empty()),
            "an empty label would be counted under no name at all: {labels:?}"
        );
    }

    #[tokio::test]
    async fn policy_accepts_a_clean_change() {
        let ch = change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        channel(Default::default())
            .check_policy(&ch, false)
            .unwrap();
    }

    /// A channel wired to an in-memory backend, plus that backend, so a test
    /// can read back what a landing counted.
    fn measured_channel(
        policy: crate::config::LandingPolicy,
    ) -> (MainChannel, Arc<cog_storage::MemoryMetricsBackend>) {
        let chan = channel(policy);
        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
        chan.attach_metrics(metrics.clone());
        (chan, metrics)
    }

    /// Recorded count for one failure category, zero when it has no series.
    async fn failure_count(metrics: &cog_storage::MemoryMetricsBackend, category: &str) -> f64 {
        metrics
            .query_counter_totals(LANDING_FAILURES_METRIC.as_str())
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.labels.get("category").map(String::as_str) == Some(category))
            .map(|s| s.value)
            .unwrap_or(0.0)
    }

    async fn series_count(metrics: &cog_storage::MemoryMetricsBackend) -> usize {
        metrics
            .query_counter_totals(LANDING_FAILURES_METRIC.as_str())
            .await
            .unwrap()
            .len()
    }

    /// How many census rows have been written, not what they say. The rule
    /// being judged is about rows, so the count is the reading.
    async fn census_rows(metrics: &cog_storage::MemoryMetricsBackend) -> usize {
        metrics
            .query_gauge_range(
                crate::change_funnel::CHANGE_FUNNEL_METRIC.as_str(),
                chrono::Utc::now() - chrono::Duration::minutes(5),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .len()
    }

    /// How many fate rows have been appended, not their value. The seed is
    /// free to read and not to write, so the cost it has to be judged by is
    /// the row count.
    async fn fate_rows(metrics: &cog_storage::MemoryMetricsBackend) -> usize {
        metrics
            .query_counter_range(
                crate::change_funnel::CHANGE_FATE_METRIC.as_str(),
                chrono::Utc::now() - chrono::Duration::minutes(5),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .len()
    }

    /// The census publishes its empty cells, so the fate counter has to seed
    /// its empty classes too: a counter carries no series until its first
    /// increment, which makes "nothing has ended this way since this process
    /// started" and "this counter was never wired up" the same reading. The
    /// fate is also the one reading the census cannot stand in for, because a
    /// landing takes its record with it.
    #[tokio::test]
    async fn publishing_seeds_every_fate_before_anything_has_ended() {
        let (chan, metrics) = measured_channel(Default::default());

        chan.publish_funnel().await;

        let fate_metric = crate::change_funnel::CHANGE_FATE_METRIC.as_str();
        let totals = metrics.query_counter_totals(fate_metric).await.unwrap();
        assert_eq!(
            totals.len(),
            cog_core::EvolutionIntent::ALL.len() * crate::change_funnel::FunnelFate::ALL.len()
        );
        assert!(
            totals.iter().all(|s| s.value == 0.0),
            "a seed moved a total: {totals:?}"
        );

        // A fate that did happen keeps its count through the next tick: the
        // seed adds zero, it does not reset what is there.
        chan.note_change_fate(
            &change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"])),
            crate::change_funnel::FunnelFate::Landed,
        )
        .await;
        chan.publish_funnel().await;

        let landed = metrics
            .query_counter_totals(fate_metric)
            .await
            .unwrap()
            .into_iter()
            .find(|s| {
                s.labels.get("intent").map(String::as_str) == Some("ci_fix")
                    && s.labels.get("fate").map(String::as_str) == Some("landed")
            })
            .expect("the landed class is seeded and carries its count");
        assert_eq!(landed.value, 1.0);
    }

    /// Seeding the fate domain creates one series per class by recording a
    /// zero, and the backend appends a row for every record whether or not it
    /// carries a value. Done on the tick that is the domain's width in rows
    /// per tick, into a store that is capped and shared with every other
    /// reading, and after the first pass it buys nothing: the series is
    /// already there. What the seed has to guarantee is that the classes exist
    /// from this process's first tick, and the first pass is that tick.
    #[tokio::test]
    async fn the_fate_seed_runs_once_per_process_not_once_per_tick() {
        let (chan, metrics) = measured_channel(Default::default());
        let domain =
            cog_core::EvolutionIntent::ALL.len() * crate::change_funnel::FunnelFate::ALL.len();

        chan.publish_funnel().await;
        assert_eq!(
            fate_rows(&metrics).await,
            domain,
            "the first tick seeds every class"
        );

        chan.publish_funnel().await;
        assert_eq!(
            fate_rows(&metrics).await,
            domain,
            "the second tick re-appended the seed"
        );

        // Only the zeros are skipped: a fate that really happened is an event
        // and appends like any other.
        chan.note_change_fate(
            &change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"])),
            crate::change_funnel::FunnelFate::Landed,
        )
        .await;
        assert_eq!(fate_rows(&metrics).await, domain + 1);
    }

    /// A gauge's value is its newest sample, so a census cell whose count has
    /// not moved already holds its value in the store — and the sweep cannot
    /// take that value away, because it exempts every gauge series' newest row.
    /// Republishing the whole census on every tick therefore bought no reading
    /// of the count and cost a row per cell per tick in a capped log shared
    /// with every other reading: 36 cells, a 30 s tick, about 100k rows a day.
    ///
    /// What those rows do buy is the writer's own presence — the companion the
    /// store renders is the only series that moves when the writer does — so
    /// the pass is not "only when a count moves" but "when a count moves, or
    /// when this process has gone a heartbeat without stamping it" (see
    /// [`crate::change_funnel::CENSUS_HEARTBEAT`]). This test's passes all
    /// happen inside one heartbeat, so it pins the first half of that rule: a
    /// tick that changes nothing and is not yet due a stamp writes nothing.
    #[tokio::test]
    async fn a_census_cell_is_written_when_it_moves_and_not_before() {
        let (chan, metrics) = measured_channel(Default::default());
        let domain = crate::change_funnel::census(&[], &[]);

        chan.publish_census(&domain).await;
        assert_eq!(
            census_rows(&metrics).await,
            domain.len(),
            "the first pass writes every cell, empty ones included"
        );

        chan.publish_census(&domain).await;
        assert_eq!(
            census_rows(&metrics).await,
            domain.len(),
            "a census that did not move was written again"
        );

        let mut moved = domain.clone();
        moved
            .iter_mut()
            .find(|p| {
                p.intent == cog_core::EvolutionIntent::SelfSignal
                    && p.stage == crate::change_funnel::FunnelStage::Landed
            })
            .expect("the census holds every pair")
            .count = 7;
        chan.publish_census(&moved).await;
        assert_eq!(
            census_rows(&metrics).await,
            domain.len() + 1,
            "a moved cell has to go out, and only it"
        );

        let latest = metrics
            .query_gauge_latest(crate::change_funnel::CHANGE_FUNNEL_METRIC.as_str())
            .await
            .unwrap();
        assert_eq!(latest.len(), domain.len());
        assert_eq!(
            latest.iter().filter(|s| s.value == 7.0).count(),
            1,
            "exactly the cell that moved carries its new count"
        );
    }

    #[tokio::test]
    async fn an_oversized_change_is_counted_under_its_own_category() {
        let policy = crate::config::LandingPolicy {
            max_changed_lines: 1,
            ..Default::default()
        };
        let (chan, metrics) = measured_channel(policy);
        let ch = change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"]));

        chan.land(&ch, None).await.unwrap_err();

        assert_eq!(failure_count(&metrics, "oversized").await, 1.0);
        // 只有这一类被记账：类别取自失败的那一步，不是事后从错误文本里猜的。
        assert_eq!(series_count(&metrics).await, 1);
    }

    #[tokio::test]
    async fn a_forbidden_path_is_counted_under_path() {
        let policy = crate::config::LandingPolicy {
            forbidden_paths: vec!["crates/".into()],
            ..Default::default()
        };
        let (chan, metrics) = measured_channel(policy);
        let ch = change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"]));

        chan.land(&ch, None).await.unwrap_err();

        assert_eq!(failure_count(&metrics, "path").await, 1.0);
        assert_eq!(series_count(&metrics).await, 1);
    }

    #[tokio::test]
    async fn a_whitelist_violation_is_counted_under_path() {
        let (chan, metrics) = measured_channel(Default::default());
        let ch = change("c1", &diff_touching(&["deploy/helm/cogneva/values.yaml"]));

        chan.land(&ch, None).await.unwrap_err();

        assert_eq!(failure_count(&metrics, "path").await, 1.0);
        assert_eq!(series_count(&metrics).await, 1);
    }

    /// The two answers the gate can give are counted apart, so a run of
    /// refusals can be read for which of them it was. Under one value the
    /// count said how often the gate stopped a change and nothing about
    /// whether the changes were unacceptable or merely unreadable, which is
    /// the difference between a queue that is working and one that is throwing
    /// work away.
    #[tokio::test]
    async fn an_unreadable_diff_is_counted_under_its_own_category() {
        let (chan, metrics) = measured_channel(Default::default());
        let ch = change("c1", "not a diff at all");

        chan.land(&ch, None).await.unwrap_err();

        assert_eq!(failure_count(&metrics, "unreadable_diff").await, 1.0);
        assert_eq!(failure_count(&metrics, "path").await, 0.0);
        assert_eq!(series_count(&metrics).await, 1);
    }

    /// 没接后端时落地行为一字不变：计数是旁路，不是前置条件。
    #[tokio::test]
    async fn a_landing_without_a_backend_fails_the_same_way() {
        let policy = crate::config::LandingPolicy {
            max_changed_lines: 1,
            ..Default::default()
        };
        let ch = change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"]));

        let err = channel(policy).land(&ch, None).await.unwrap_err();

        assert!(err.to_string().contains("over the 1-line cap"), "{err}");
    }

    #[test]
    fn commit_message_carries_the_change_id_trailer() {
        let msg = commit_message(&BotIdentityConfig::default(), &change("chg-42", "diff"));
        assert!(msg.contains("Change-Id: chg-42"));
        assert!(msg.contains("Signed-off-by:"));
    }

    #[test]
    fn regex_escape_makes_the_id_literal() {
        assert_eq!(escape_regex("chg-1"), "chg\\-1");
        // A prefix id must not be re-matched as a whole id.
        let pattern = format!("^Change-Id: {}$", escape_regex("chg-1"));
        assert_eq!(pattern, "^Change-Id: chg\\-1$");
    }

    #[test]
    fn push_race_detection() {
        assert!(is_push_race(
            "! [rejected]        main -> main (non-fast-forward)"
        ));
        assert!(is_push_race(
            " ! [rejected]        main -> main (fetch first)\n             hint: Updates were rejected because the remote contains work that you do\n             hint: not have locally."
        ));
        assert!(!is_push_race("remote: Permission to o/r.git denied to bot"));
    }

    /// 一个仓库两个宿主，两条路由只该差在平台段上。分开拼写迟早会有一天不一致，
    /// 而不一致读出来正好是「镜像没跟上」——一个查不出病因的读数。
    #[test]
    fn a_proxy_route_differs_only_by_the_platform_segment() {
        assert_eq!(
            proxy_route("http://gw:8081/git/", "github", "o/r"),
            "http://gw:8081/git/github/o/r.git"
        );
        assert_eq!(
            proxy_route("http://gw:8081/git", "gitee", "o/r"),
            "http://gw:8081/git/gitee/o/r.git"
        );
    }

    /// 只有 Gitee 集成开着、并且指向**同一个**仓库时才有第二个宿主。指向另一个
    /// 仓库时没有镜像：那是把变更复制到别处，不是同一份历史存了两份。
    #[test]
    fn the_mirror_set_is_the_same_repository_on_a_second_host() {
        let github = GitHubIntegrationConfig {
            repo: "o/r".into(),
            base_branch: "main".into(),
            ..Default::default()
        };
        let gitee = |enabled: bool, repo: &str| GiteeIntegrationConfig {
            enabled,
            repo: repo.into(),
            base_branch: "main".into(),
            ..Default::default()
        };

        assert!(mirror_targets(&github, &gitee(false, "o/r")).is_empty());

        let targets = mirror_targets(&github, &gitee(true, "o/r"));
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].remote, "gitee");
        assert_eq!(targets[0].slug, "gitee");
        assert_eq!(targets[0].repo, "o/r");
        assert_eq!(targets[0].base, "main");

        assert!(mirror_targets(&github, &gitee(true, "o/other")).is_empty());
    }

    /// 主机的远端名是 `origin`：工作树跟踪它，部署器也从它推进裸仓的主分支。
    #[test]
    fn the_primary_target_is_the_remote_the_worktree_tracks() {
        let github = GitHubIntegrationConfig {
            repo: "o/r".into(),
            base_branch: "trunk".into(),
            ..Default::default()
        };
        let target = PushTarget::github(&github);
        assert_eq!(target.remote, "origin");
        assert_eq!(target.slug, "github");
        assert_eq!(target.base, "trunk");
    }

    /// 半成功的报错要把每一端都点名：两端可以各自拒绝，只报第一端的错会让人
    /// 去找一个只解释一半现场的原因。
    #[test]
    fn a_refused_mirror_is_named_with_its_reason_in_the_failure() {
        let target = |remote: &str| PushTarget {
            remote: remote.into(),
            slug: remote.into(),
            repo: "o/r".into(),
            base: "main".into(),
        };
        let refusals = vec![
            MirrorFailure {
                target: target("gitee"),
                failure: PushFailure::Rejected("permission denied".into()),
            },
            MirrorFailure {
                target: target("gitlab"),
                failure: PushFailure::Rejected("not behind github".into()),
            },
        ];

        let err = mirror_error(&refusals, "abc123");

        assert_eq!(err.category, LandingCategory::Rejected);
        let text = err.error.to_string();
        assert!(text.contains("abc123"), "{text}");
        assert!(
            text.contains("gitee") && text.contains("permission denied"),
            "{text}"
        );
        assert!(
            text.contains("gitlab") && text.contains("not behind github"),
            "{text}"
        );
    }

    /// 祖先判据的答案是退出码：0 是、1 否、其余都是「命令没说」。把第三态读成
    /// 「是祖先」，真分叉就会被当成落后去快进，而那一步是 force push。
    #[tokio::test]
    async fn ancestor_readings_come_from_the_exit_code_not_the_text() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        run_git(repo, &["init", "-q"]).await.unwrap();
        run_git(repo, &["config", "user.email", "t@example.invalid"])
            .await
            .unwrap();
        run_git(repo, &["config", "user.name", "t"]).await.unwrap();
        async fn commit(repo: &std::path::Path, content: &str) -> String {
            std::fs::write(repo.join("f"), content).unwrap();
            run_git(repo, &["add", "-A"]).await.unwrap();
            run_git(repo, &["commit", "-qm", content]).await.unwrap();
            run_git(repo, &["rev-parse", "HEAD"])
                .await
                .unwrap()
                .trim()
                .to_string()
        }
        let one = commit(repo, "one").await;
        let two = commit(repo, "two").await;

        let chan = MainChannel::new(
            repo.to_path_buf(),
            GitHubIntegrationConfig::default(),
            Arc::new(NullProvider),
            ContributionController::new_shared(),
        );
        assert!(chan.is_ancestor(&one, &two).await.unwrap());
        assert!(!chan.is_ancestor(&two, &one).await.unwrap());
        // git exits above 1 when it could not answer at all, which is not "no".
        assert!(chan.is_ancestor(&"0".repeat(40), &two).await.is_err());
    }

    /// 两个宿主、真 git：一次推送两端都到同一个 rev；落后的一端下一轮被快进补齐；
    /// 分叉的一端（tip 不是主分支祖先）被拒并带上两端的 tip，而主分支一步不动。
    ///
    /// 上面的单测读的是判据的形状，这一条读的是它们接上真 git 之后还成不成立：
    /// 「补齐」和「不许动」都是对远端 ref 的操作，用 mock 断言不了。
    #[tokio::test]
    async fn a_landing_reaches_every_host_and_repairs_only_a_behind_mirror() {
        async fn commit(dir: &std::path::Path, content: &str) -> String {
            std::fs::write(dir.join("f"), content).unwrap();
            run_git(dir, &["add", "-A"]).await.unwrap();
            run_git(dir, &["commit", "-qm", content]).await.unwrap();
            run_git(dir, &["rev-parse", "HEAD"])
                .await
                .unwrap()
                .trim()
                .to_string()
        }
        async fn tip(dir: &std::path::Path, refname: &str) -> String {
            run_git(dir, &["rev-parse", refname])
                .await
                .unwrap()
                .trim()
                .to_string()
        }
        async fn bare(path: &std::path::Path) {
            tokio::fs::create_dir_all(path).await.unwrap();
            run_git(path, &["init", "--bare", "-q"]).await.unwrap();
        }

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (origin, gitee, work) = (
            root.join("origin.git"),
            root.join("gitee.git"),
            root.join("work"),
        );
        bare(&origin).await;
        bare(&gitee).await;
        tokio::fs::create_dir_all(&work).await.unwrap();
        run_git(&work, &["init", "-q", "-b", "main"]).await.unwrap();
        run_git(&work, &["config", "user.email", "t@example.invalid"])
            .await
            .unwrap();
        run_git(&work, &["config", "user.name", "t"]).await.unwrap();
        run_git(
            &work,
            &["remote", "add", "origin", origin.to_str().unwrap()],
        )
        .await
        .unwrap();
        run_git(&work, &["remote", "add", "gitee", gitee.to_str().unwrap()])
            .await
            .unwrap();
        let first = commit(&work, "one").await;
        run_git(&work, &["push", "-q", "origin", "main"])
            .await
            .unwrap();
        run_git(&work, &["push", "-q", "gitee", "main"])
            .await
            .unwrap();

        let chan = MainChannel::new(
            work.clone(),
            GitHubIntegrationConfig {
                repo: "o/r".into(),
                base_branch: "main".into(),
                ..Default::default()
            },
            Arc::new(NullProvider),
            ContributionController::new_shared(),
        )
        .with_mirrors(vec![PushTarget::gitee(&GiteeIntegrationConfig {
            enabled: true,
            repo: "o/r".into(),
            base_branch: "main".into(),
            ..Default::default()
        })]);

        // 一次落地：两端的 base 分支都到同一个 rev。
        let second = commit(&work, "two").await;
        let refusals = chan.push(&work, &second).await.unwrap();
        assert!(refusals.is_empty(), "{refusals:?}");
        assert_eq!(tip(&origin, "refs/heads/main").await, second);
        assert_eq!(tip(&gitee, "refs/heads/main").await, second);

        // 镜像被回退一版：这一轮之后由快进补齐，且主分支不动。
        run_git(&gitee, &["update-ref", "refs/heads/main", &first])
            .await
            .unwrap();
        let unrepaired = chan.catch_up_mirrors("main").await.unwrap();
        assert!(unrepaired.is_empty(), "{unrepaired:?}");
        assert_eq!(tip(&gitee, "refs/heads/main").await, second);
        assert_eq!(tip(&origin, "refs/heads/main").await, second);

        // 镜像上一条从第一版分出去的提交：两端历史不同，拒绝并点名两端的 tip。
        run_git(&work, &["checkout", "-q", "-b", "fork", &first])
            .await
            .unwrap();
        let forked = commit(&work, "diverged").await;
        run_git(&work, &["push", "-q", "--force", "gitee", "fork:main"])
            .await
            .unwrap();
        run_git(&work, &["checkout", "-q", "main"]).await.unwrap();

        let unrepaired = chan.catch_up_mirrors("main").await.unwrap();
        assert_eq!(unrepaired.len(), 1, "{unrepaired:?}");
        assert_eq!(unrepaired[0].target.remote, "gitee");
        let said = unrepaired[0].failure.to_string();
        assert!(
            said.contains(&forked) && said.contains(&second),
            "两端的 tip 都要在：{said}"
        );
        assert_eq!(
            tip(&origin, "refs/heads/main").await,
            second,
            "主分支不该动"
        );
        assert_eq!(tip(&gitee, "refs/heads/main").await, forked);

        // 半成功不是成功：主分支前进了，镜像被拒就报出来。
        let third = commit(&work, "three").await;
        let refusals = chan.push(&work, &third).await.unwrap();
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert_eq!(refusals[0].target.remote, "gitee");
        assert_eq!(tip(&origin, "refs/heads/main").await, third);
        assert_eq!(tip(&gitee, "refs/heads/main").await, forked);
    }

    #[tokio::test]
    async fn unverified_records_round_trip() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let ch = change("chg-1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        cog_core::ChangeLanding::record_unverified(&channel(Default::default()), &ch)
            .await
            .unwrap();

        let records = load_records().await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, LandingState::Unverified);
        assert_eq!(records[0].change.change_id, "chg-1");

        remove_record("chg-1").await;
        assert!(load_records().await.is_empty());

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 只存在于记录里的变更必须能被验证循环读到：交接的持久面就是这条记录，
    /// 换一个部署生成、或者进程重启丢掉内存里的描述之后，它是唯一还剩的东西。
    #[tokio::test]
    async fn unverified_changes_reports_what_the_verify_loop_must_pick_up() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let chan = channel(Default::default());
        let mut ch = change("chg-1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        ch.issue_number = Some(4);
        ch.self_review_score = Some(0.85);
        cog_core::ChangeLanding::record_unverified(&chan, &ch)
            .await
            .unwrap();

        let pending = cog_core::ChangeLanding::unverified_changes(&chan)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        // 记录那份变更原样带出来：落地要落它，不是落一份从产物重建的副本。
        assert_eq!(pending[0].change_id, "chg-1");
        assert_eq!(pending[0].goal, ch.goal);
        assert_eq!(pending[0].issue_number, Some(4));
        assert_eq!(pending[0].self_review_score, Some(0.85));

        // 已经落地的记录不再是待验证输入。
        remove_record("chg-1").await;
        assert!(cog_core::ChangeLanding::unverified_changes(&chan)
            .await
            .unwrap()
            .is_empty());

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 验证判定必须终局，且理由要留下：不留理由，"被判定打不上"和"从没提交过"
    /// 在读侧就分不出来，而记录正是这两者之间唯一的区别。
    #[tokio::test]
    async fn retiring_an_unverified_record_settles_it_with_the_reason() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let chan = channel(Default::default());
        let ch = change("chg-1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        cog_core::ChangeLanding::record_unverified(&chan, &ch)
            .await
            .unwrap();

        cog_core::ChangeLanding::retire_unverified(&chan, "chg-1", "patch does not apply")
            .await
            .unwrap();

        assert!(
            cog_core::ChangeLanding::unverified_changes(&chan)
                .await
                .unwrap()
                .is_empty(),
            "判定打不上的变更不得再进验证队列，否则每轮重验一次，永不终局"
        );
        let rec = load_record("chg-1").await.expect("记录留下作审计线索");
        assert_eq!(rec.state, LandingState::Retired);
        assert_eq!(rec.retired_reason.as_deref(), Some("patch does not apply"));
        assert_ne!(
            rec.updated_at, rec.created_at,
            "终局要体现在记录的变化上，不是留在原地无人问"
        );

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 已经落到主分支的记录不能被一个迟到的判定改写成退休。
    #[tokio::test]
    async fn a_landed_record_is_not_overwritten_by_a_late_verdict() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let chan = channel(Default::default());
        let ch = change("chg-1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        let now = Utc::now();
        save_record(&LandingRecord {
            change: ch,
            base: "main".into(),
            landed_rev: "abc1234".into(),
            state: LandingState::Landed,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            inherited_ci_reported: false,
            retired_reason: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();

        cog_core::ChangeLanding::retire_unverified(&chan, "chg-1", "stale verdict")
            .await
            .unwrap();

        let rec = load_record("chg-1").await.unwrap();
        assert_eq!(rec.state, LandingState::Landed);
        assert_eq!(rec.landed_rev, "abc1234");
        assert_eq!(rec.retired_reason, None);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 变更也可能来自本地队列，这条通道上没有它的记录：收口是空操作，不得凭空
    /// 造一条记录出来。
    #[tokio::test]
    async fn settling_a_change_this_channel_never_saw_creates_nothing() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        cog_core::ChangeLanding::retire_unverified(&channel(Default::default()), "chg-nope", "why")
            .await
            .unwrap();

        assert!(load_records().await.is_empty());

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 退休是判定，不是卡住：巡检不得把它报成"提交之后再没走到落地"。
    #[tokio::test]
    async fn a_retired_record_is_not_reported_as_never_landing() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let policy = crate::config::LandingPolicy {
            ci_watch_timeout_secs: 1800,
            ..Default::default()
        };
        let chan = channel(policy);
        let now = Utc::now();
        save_record(&LandingRecord {
            change: change(
                "chg-retired",
                &diff_touching(&["crates/cog-github/src/lib.rs"]),
            ),
            base: "main".into(),
            landed_rev: String::new(),
            state: LandingState::Retired,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            inherited_ci_reported: false,
            retired_reason: Some("patch does not apply".into()),
            created_at: now - chrono::Duration::seconds(7200),
            updated_at: now - chrono::Duration::seconds(7200),
        })
        .await
        .unwrap();

        watch_landed(&chan, None, None).await;

        let records = load_records().await;
        let rec = records
            .iter()
            .find(|r| r.change.change_id == "chg-retired")
            .expect("退休记录不会被巡检清掉");
        assert!(!rec.unlanded_reported, "退休是判定过的终局，不是没人管它");

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[tokio::test]
    async fn a_change_that_never_landed_is_reported_once() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let policy = crate::config::LandingPolicy {
            ci_watch_timeout_secs: 1800,
            ..Default::default()
        };
        let chan = channel(policy.clone());
        let now = Utc::now();

        // One record past the window and one inside it: only the first is news.
        for (id, age_secs) in [("chg-old", 7200i64), ("chg-fresh", 60i64)] {
            save_record(&LandingRecord {
                change: change(id, &diff_touching(&["crates/cog-github/src/lib.rs"])),
                base: "main".into(),
                landed_rev: String::new(),
                state: LandingState::Unverified,
                failure_recorded: false,
                redriven: false,
                unlanded_reported: false,
                inherited_ci_reported: false,
                retired_reason: None,
                created_at: now - chrono::Duration::seconds(age_secs),
                updated_at: now - chrono::Duration::seconds(age_secs),
            })
            .await
            .unwrap();
        }

        watch_landed(&chan, None, None).await;

        let records = load_records().await;
        let old = records
            .iter()
            .find(|r| r.change.change_id == "chg-old")
            .expect("old record survives the pass");
        assert!(
            old.unlanded_reported,
            "a change stale past the window is news"
        );
        // The age clock must survive the report, or the next pass would read a
        // freshly-touched record and never say anything again.
        assert_eq!(old.updated_at, now - chrono::Duration::seconds(7200));
        let fresh = records
            .iter()
            .find(|r| r.change.change_id == "chg-fresh")
            .expect("fresh record survives the pass");
        assert!(
            !fresh.unlanded_reported,
            "a change still inside the window is not news"
        );

        // Idempotent: the latch is what keeps a stuck record from re-announcing
        // itself every pass for as long as it stays stuck.
        watch_landed(&chan, None, None).await;
        let records = load_records().await;
        assert_eq!(records.len(), 2, "neither record is reaped by reporting");
        assert!(
            records
                .iter()
                .find(|r| r.change.change_id == "chg-old")
                .unwrap()
                .unlanded_reported
        );

        remove_record("chg-old").await;
        remove_record("chg-fresh").await;
        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// A channel whose CI provider answers with a fixed result, so the watch
    /// loop's red-verdict path runs without a network.
    fn watch_channel(policy: crate::config::LandingPolicy, provider: RedCiProvider) -> MainChannel {
        MainChannel::new(
            "/tmp/nonexistent",
            GitHubIntegrationConfig {
                repo: "o/r".into(),
                landing_policy: policy,
                ..Default::default()
            },
            Arc::new(provider),
            ContributionController::new_shared(),
        )
    }

    /// A fresh landed record whose CI has come back red.
    async fn save_red_landing(id: &str) {
        let now = Utc::now();
        save_record(&LandingRecord {
            change: change(id, &diff_touching(&["crates/cog-github/src/lib.rs"])),
            base: "main".into(),
            landed_rev: "abc1234".into(),
            state: LandingState::Landed,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            inherited_ci_reported: false,
            retired_reason: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    }

    /// 取不到的日志不是空日志：抓取失败时记录必须留下等下一轮重试，不能记失败、
    /// 不能退役，否则一条坏掉的抓取路径会被读成"失败没有证据"，正是
    /// redrive_refused_without_evidence 这条告警误报的来源。
    #[tokio::test]
    async fn a_failed_log_fetch_keeps_the_record_for_the_next_pass() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let policy = crate::config::LandingPolicy {
            revert_on_ci_failure: false,
            redrive_on_ci_failure: false,
            ..Default::default()
        };
        let chan = watch_channel(
            policy,
            RedCiProvider {
                log: Err("log endpoint returned 500".to_string()),
            },
        );
        save_red_landing("chg-red").await;

        watch_landed(&chan, None, None).await;

        let rec = load_record("chg-red")
            .await
            .expect("a fetch error keeps the record so the next pass retries");
        assert!(
            !rec.failure_recorded,
            "no outcome may be recorded against a log that was never read"
        );
        assert!(!rec.redriven);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 取回来是空的日志才是"没有证据"：失败照常记录、记录照常退役，这条路不因
    /// 区分抓取错误而改变。
    #[tokio::test]
    async fn an_empty_log_still_settles_the_record() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let policy = crate::config::LandingPolicy {
            revert_on_ci_failure: false,
            redrive_on_ci_failure: false,
            ..Default::default()
        };
        let chan = watch_channel(
            policy,
            RedCiProvider {
                log: Ok(String::new()),
            },
        );
        save_red_landing("chg-red").await;

        watch_landed(&chan, None, None).await;

        assert!(
            load_record("chg-red").await.is_none(),
            "an empty log was fetched, so the landing is settled and the record retires"
        );

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// A scratch repository with two empty commits: the second is a landing,
    /// the first is the tree it was replayed onto. A landing's CI measures its
    /// whole tree, so the commit underneath it has to exist for there to be
    /// anything to attribute a failure to.
    async fn repo_with_parent() -> (tempfile::TempDir, String, String) {
        let dir = tempfile::tempdir().unwrap();
        run_git(dir.path(), &["init", "-q"]).await.unwrap();
        for message in ["tip", "landed"] {
            run_git(
                dir.path(),
                &[
                    "-c",
                    "user.email=t@example.com",
                    "-c",
                    "user.name=t",
                    "commit",
                    "-q",
                    "--allow-empty",
                    "-m",
                    message,
                ],
            )
            .await
            .unwrap();
        }
        let landed = run_git(dir.path(), &["rev-parse", "HEAD"])
            .await
            .unwrap()
            .trim()
            .to_string();
        let base = run_git(dir.path(), &["rev-parse", "HEAD^"])
            .await
            .unwrap()
            .trim()
            .to_string();
        (dir, base, landed)
    }

    /// A provider that names the failing checks on a commit. The map is behind
    /// a lock so a test can repair the tip between two passes — which is what
    /// the watch loop's own answer to an inherited failure depends on.
    struct NamedChecks {
        failed: std::sync::Mutex<std::collections::HashMap<String, Vec<String>>>,
    }

    impl NamedChecks {
        fn new(failed: impl IntoIterator<Item = (String, Vec<String>)>) -> Self {
            Self {
                failed: std::sync::Mutex::new(failed.into_iter().collect()),
            }
        }
    }

    #[async_trait::async_trait]
    impl CodePlatformProvider for NamedChecks {
        async fn list_open_issues(&self) -> Result<Vec<crate::provider::PlatformIssue>> {
            Ok(Vec::new())
        }
        async fn create_pull_request(
            &self,
            _req: crate::provider::CreatePullRequest,
        ) -> Result<crate::provider::PlatformPullRequest> {
            Err(CogGitHubError::Provider("unused".into()))
        }
        async fn comment_on_issue(&self, _n: u64, _body: String) -> Result<()> {
            Ok(())
        }
        async fn merge_pull_request(&self, _n: u64, _sha: String) -> Result<()> {
            Ok(())
        }
        async fn get_pull_request(&self, _n: u64) -> Result<crate::provider::PullRequestDetail> {
            Err(CogGitHubError::Provider("unused".into()))
        }
        async fn ci_verdict_for_sha(&self, _sha: &str) -> Result<Option<bool>> {
            Ok(Some(false))
        }
        async fn ci_failure_log_for_sha(&self, _sha: &str) -> Result<String> {
            Ok(String::new())
        }
        async fn ci_failed_checks_for_sha(&self, sha: &str) -> Result<Option<Vec<String>>> {
            Ok(self.failed.lock().unwrap().get(sha).cloned())
        }
    }

    /// A channel watching `workdir`, with its own metrics backend so a test can
    /// read back what the pass counted.
    fn watching_at(
        workdir: &Path,
        provider: Arc<dyn CodePlatformProvider>,
    ) -> (MainChannel, Arc<cog_storage::MemoryMetricsBackend>) {
        let chan = MainChannel::new(
            workdir,
            GitHubIntegrationConfig {
                repo: "o/r".into(),
                // Neither the revert nor the re-drive is what is under test
                // here, and both reach the network. A settled record is the
                // signal for "this landing was answered for", which is how the
                // tests around this one read a conviction too.
                landing_policy: crate::config::LandingPolicy {
                    revert_on_ci_failure: false,
                    redrive_on_ci_failure: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            provider,
            ContributionController::new_shared(),
        );
        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
        chan.attach_metrics(metrics.clone());
        (chan, metrics)
    }

    /// A landed record whose CI came back red on `landed_rev`.
    async fn save_landing_of(id: &str, landed_rev: &str) {
        let now = Utc::now();
        save_record(&LandingRecord {
            change: change(id, &diff_touching(&["crates/cog-github/src/lib.rs"])),
            base: "main".into(),
            landed_rev: landed_rev.into(),
            state: LandingState::Landed,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            inherited_ci_reported: false,
            retired_reason: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    }

    async fn inherited_count(metrics: &cog_storage::MemoryMetricsBackend) -> f64 {
        metrics
            .query_counter_totals(LANDING_CI_FAILURE_INHERITED_METRIC.as_str())
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.value)
            .sum()
    }

    /// 一笔落在已经红的树上的变更，不该替那棵树挨这一笔。
    ///
    /// 撤回会把分支退回它落地时那棵树，而那棵树过的就是同一个检查——撤回花掉
    /// 的是这条变更，换回来的是零。所以这条路既不撤、也不记失败、也不回投，只把
    /// 这件事记成一次读数并继续看着；记录留着，是因为树被修好之后同一个检查再红，
    /// 那就是这条变更自己的账了。
    #[tokio::test]
    async fn a_failure_the_tree_already_had_is_not_charged_to_the_change() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let (repo, base, landed) = repo_with_parent().await;
        let provider = Arc::new(NamedChecks::new([
            (landed.clone(), vec!["Format".to_string()]),
            (base.clone(), vec!["Format".to_string()]),
        ]));
        let (chan, metrics) = watching_at(repo.path(), provider);
        save_landing_of("chg-inherited", &landed).await;

        watch_landed(&chan, None, None).await;

        let rec = load_record("chg-inherited")
            .await
            .expect("an inherited failure is not the change's, so it is not settled");
        assert!(rec.inherited_ci_reported);
        assert_eq!(inherited_count(&metrics).await, 1.0);

        // The report is a latch: the record stays watched for as long as the
        // window lasts, and the counter counts landings, not passes over one.
        watch_landed(&chan, None, None).await;
        assert!(load_record("chg-inherited").await.is_some());
        assert_eq!(inherited_count(&metrics).await, 1.0);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 另一半：这条变更自己带来的失败照旧由它承担，撤回那条路一步不改。
    #[tokio::test]
    async fn a_failure_the_change_introduced_is_still_answered_for() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let (repo, base, landed) = repo_with_parent().await;
        let provider = Arc::new(NamedChecks::new([
            (
                landed.clone(),
                vec!["Format".to_string(), "Test".to_string()],
            ),
            (base.clone(), vec!["Format".to_string()]),
        ]));
        let (chan, metrics) = watching_at(repo.path(), provider);
        save_landing_of("chg-introduced", &landed).await;

        watch_landed(&chan, None, None).await;

        assert!(
            load_record("chg-introduced").await.is_none(),
            "a check the tip was passing and this revision fails is the change's to answer for"
        );
        assert_eq!(inherited_count(&metrics).await, 0.0);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 说不清是谁的失败，就是这条变更的失败——判不出不许读成洗清。
    #[tokio::test]
    async fn an_unnameable_failure_keeps_the_change_answering_for_it() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let (repo, _base, landed) = repo_with_parent().await;
        let (chan, metrics) = watching_at(repo.path(), Arc::new(NamedChecks::new([])));
        save_landing_of("chg-unnameable", &landed).await;

        watch_landed(&chan, None, None).await;

        assert!(
            load_record("chg-unnameable").await.is_none(),
            "a platform that cannot name its checks leaves the change answering for the failure"
        );
        assert_eq!(inherited_count(&metrics).await, 0.0);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 留着记录的理由是可证伪的：树修好之后，同一个检查再红就该由这条变更负责。
    #[tokio::test]
    async fn a_repaired_tip_hands_the_same_check_back_to_the_change() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let (repo, base, landed) = repo_with_parent().await;
        let provider = Arc::new(NamedChecks::new([
            (landed.clone(), vec!["Format".to_string()]),
            (base.clone(), vec!["Format".to_string()]),
        ]));
        let (chan, _metrics) = watching_at(repo.path(), provider.clone());
        save_landing_of("chg-then-fixed", &landed).await;

        watch_landed(&chan, None, None).await;
        assert!(load_record("chg-then-fixed").await.is_some());

        // The tip is repaired and comes back green; this revision still fails
        // the same check, so there is nothing left for it to have inherited.
        provider.failed.lock().unwrap().remove(&base);
        watch_landed(&chan, None, None).await;

        assert!(
            load_record("chg-then-fixed").await.is_none(),
            "once the tip no longer fails the check, that check failing is this change's"
        );

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[tokio::test]
    async fn staged_policy_does_not_record_a_landing() {
        let _guard = crate::identity::ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let controller = ContributionController::new_shared();
        cog_core::ContributionControl::set_policy(
            controller.as_ref(),
            cog_core::ContributionPolicy::Ask,
        );
        let chan = MainChannel::new(
            "/tmp/nonexistent",
            GitHubIntegrationConfig {
                repo: "o/r".into(),
                ..Default::default()
            },
            Arc::new(NullProvider),
            controller,
        );

        let ch = change("chg-2", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        let out = cog_core::ChangeSink::submit_change(&chan, ch)
            .await
            .unwrap();
        assert!(out.starts_with("staged:"), "{out}");
        assert!(load_records().await.is_empty());

        // The staging dir is the same data dir, so clean it up for the next
        // test in this process.
        crate::pending_changes::remove_staged("chg-2").await;
        std::env::remove_var("COGNEVA_DATA_DIR");
    }
}
