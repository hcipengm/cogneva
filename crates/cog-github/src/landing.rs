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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use cog_core::{GeneratedChange, LandedSource, SFError, SFResult};

use crate::config::{BotIdentityConfig, GitHubIntegrationConfig};
use crate::contribution::ContributionController;
use crate::error::{CogGitHubError, Result};
use crate::provider::CodePlatformProvider;

/// Trailer stamped on every landed commit, carrying the change id.
///
/// It is the landing's identity: an existing commit with this trailer means
/// the change is already on the branch, and a push that races another landing
/// can be retried without producing a duplicate.
const CHANGE_ID_TRAILER: &str = "Change-Id";

/// Directory holding one `<change_id>.json` file per landing.
pub fn landing_dir() -> PathBuf {
    let dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
    PathBuf::from(dir).join("landing")
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

/// The landing channel: commits verified changes to the base branch and
/// records what it did so a red CI run can be answered later.
pub struct MainChannel {
    workdir: PathBuf,
    config: GitHubIntegrationConfig,
    provider: Arc<dyn CodePlatformProvider>,
    controller: Arc<ContributionController>,
    /// Serializes landings: each one rebuilds a scratch worktree, so two
    /// concurrent landings (mainline + owner flush) would clobber each other.
    gate: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for MainChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MainChannel")
            .field("workdir", &self.workdir)
            .field("base", &self.config.base_branch)
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
            provider,
            controller,
            gate: tokio::sync::Mutex::new(()),
        }
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
        self.land_inner(change, source, false).await
    }

    /// Land a change the owner approved explicitly.
    ///
    /// The click is a human decision, so the quality gates (self-review score,
    /// size cap) do not apply — the owner already took responsibility for
    /// them. The safety gates (whitelist, forbidden paths) still do: approving
    /// a contribution is not the same as approving writing to `deploy/`.
    pub async fn land_approved(&self, change: &GeneratedChange) -> Result<String> {
        self.land_inner(change, None, true).await
    }

    async fn land_inner(
        &self,
        change: &GeneratedChange,
        source: Option<&LandedSource>,
        owner_approved: bool,
    ) -> Result<String> {
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
        Err(last_race.unwrap_or_else(|| {
            CogGitHubError::Provider(format!(
                "landing {} lost the race for {base} {attempts} times",
                change.change_id
            ))
        }))
    }

    async fn record_landed(&self, change: &GeneratedChange, base: &str, rev: &str) -> Result<()> {
        let now = Utc::now();
        let created = load_record(&change.change_id)
            .await
            .map(|r| r.created_at)
            .unwrap_or(now);
        save_record(&LandingRecord {
            change: change.clone(),
            base: base.to_string(),
            landed_rev: rev.to_string(),
            state: LandingState::Landed,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            created_at: created,
            updated_at: now,
        })
        .await
        .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
        // A change that was staged awaiting a decision is no longer pending:
        // the owner's click (or the policy) put it on the branch.
        crate::pending_changes::remove_staged(&change.change_id).await;
        Ok(())
    }

    /// Reject a change that must not reach the public branch, before any git
    /// operation runs. Fail closed: an unparseable diff or an unmeasured
    /// change is held back rather than trusted.
    fn check_policy(&self, change: &GeneratedChange, owner_approved: bool) -> Result<()> {
        let policy = &self.config.landing_policy;
        ensure_contribution_allowed(&change.content)?;

        if !owner_approved {
            let changed_lines = count_changed_lines(&change.content);
            if changed_lines > policy.max_changed_lines {
                return Err(CogGitHubError::PrivacyRejected(format!(
                    "change {} touches {changed_lines} lines, over the {}-line cap",
                    change.change_id, policy.max_changed_lines
                )));
            }
        }

        let files = affected_files(&change.content)?;
        for file in &files {
            if let Some(pattern) = policy
                .forbidden_paths
                .iter()
                .find(|p| path_forbidden(file, p))
            {
                return Err(CogGitHubError::PrivacyRejected(format!(
                    "change {} touches forbidden path {file} (pattern {pattern})",
                    change.change_id
                )));
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
    ) -> Result<LandOutcome> {
        if let Some(rev) = self.landed_rev_on(base, &change.change_id).await? {
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
                run_git(&wt, &["cherry-pick", "--no-commit", "FETCH_HEAD"]).await?;
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
                .await?;
            }
        }
        run_git(&wt, &["add", "-A"]).await?;
        self.commit(&wt, &commit_message(&self.config.bot_identity, change))
            .await?;
        let rev = run_git(&wt, &["rev-parse", "HEAD"])
            .await?
            .trim()
            .to_string();
        match self.push(&wt, base).await {
            Ok(()) => Ok(LandOutcome::Landed(rev)),
            Err(PushFailure::Raced(msg)) => {
                Ok(LandOutcome::Superseded(CogGitHubError::Provider(msg)))
            }
            Err(PushFailure::Rejected(msg)) => Err(CogGitHubError::Provider(msg)),
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

    /// Push the checked-out commit to the base branch. A lost race against a
    /// newer base tip is separated from a real rejection: only the former is
    /// worth re-applying onto the fresh tip.
    async fn push(&self, dir: &Path, base: &str) -> std::result::Result<(), PushFailure> {
        let (ok, _stdout, stderr) = match run_git_status(
            dir,
            &["push", "origin", &format!("HEAD:refs/heads/{base}")],
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
                "push to {base} lost the race: {}",
                stderr.trim()
            )));
        }
        Err(PushFailure::Rejected(format!(
            "push to {base} rejected: {}",
            stderr.trim()
        )))
    }

    /// Revert a landed commit so the base branch returns to green, and return
    /// the revert commit.
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
        self.push(&wt, &record.base)
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
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
        MainChannel::land(self, change, source)
            .await
            .map_err(|e| SFError::Internal(e.to_string()))
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
            created_at: created,
            updated_at: now,
        })
        .await
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
                let log = channel
                    .provider
                    .ci_failure_log_for_sha(&record.landed_rev)
                    .await
                    .unwrap_or_default();

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
                    redrive(orchestrator, &record, &log).await;
                }
                remove_record(&record.change.change_id).await;
            }
        }
    }
}

/// re-drive pushed the task is submitted exactly once per landing: the
/// `redriven` flag is persisted, so a restart between the revert and the
/// re-drive does not produce a second identical fix task.
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
        body.push_str(&format!("\nSelf-review score: {score:.2}\n"));
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
    let files = affected_files(diff)?;
    let denied: Vec<&str> = files
        .iter()
        .map(String::as_str)
        .filter(|p| !is_allowed_path(p))
        .collect();
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

/// Files a unified diff touches, as a landing error when none can be read.
fn affected_files(diff: &str) -> Result<Vec<String>> {
    cog_core::parse_diff_affected_files(diff)
        .map_err(|e| CogGitHubError::PrivacyRejected(e.to_string()))
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
fn count_changed_lines(diff: &str) -> usize {
    diff.lines()
        .filter(|l| {
            (l.starts_with('+') || l.starts_with('-'))
                && !l.starts_with("+++")
                && !l.starts_with("---")
        })
        .count()
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
pub async fn ensure_workdir(
    config: &GitHubIntegrationConfig,
    token: Option<&str>,
) -> Result<PathBuf> {
    let workdir = config.git_workdir_path();
    let url = remote_url(config, token);
    if is_git_workdir(&workdir).await {
        if git_proxy_base().is_some() {
            let output = tokio::process::Command::new("git")
                .args(["remote", "set-url", "origin", &url])
                .current_dir(&workdir)
                .output()
                .await?;
            if !output.status.success() {
                return Err(CogGitHubError::Provider(format!(
                    "git remote set-url failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                )));
            }
        }
        return Ok(workdir);
    }
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
    Ok(workdir)
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
        return format!("{}/github/{}.git", base.trim_end_matches('/'), target_repo);
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

    #[test]
    fn privacy_gate_fails_closed_on_unparseable_diff() {
        let err = ensure_contribution_allowed("not a diff at all").unwrap_err();
        assert!(matches!(err, CogGitHubError::PrivacyRejected(_)));
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

    #[tokio::test]
    async fn policy_accepts_a_clean_change() {
        let ch = change("c1", &diff_touching(&["crates/cog-github/src/lib.rs"]));
        channel(Default::default())
            .check_policy(&ch, false)
            .unwrap();
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
