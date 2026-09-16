//! Auto-merge executor — the missing executor of the evolution loop.
//!
//! The loop already produced PRs and the policy evaluator already knew when
//! one was mergeable, but nothing connected the two, so generated changes
//! piled up open forever and the loop never closed back into `main`. This
//! module is that connection: once per discovery round it re-checks every
//! self-produced PR against the deterministic gates (CI, self-review score,
//! and the [`AutoMergePolicy`] checks) and merges the ones that pass.
//!
//! Every judgement here is deterministic — no LLM call. A PR that clears the
//! gates is merged and its deployment is triggered; one blocked by something
//! only a human can clear is annotated once and stops being re-judged
//! (unattended operation leaves an explicit marker, not an open tail); one
//! held by a self-clearing gate is retried silently next round.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use cog_core::{OrchestratorControl, Task, TaskType};

use crate::config::{AutoMergePolicy, BotIdentityConfig};
use crate::merge_decider::{MergeDecider, MergeDecision};
use crate::provider::{CodePlatformProvider, PlatformPullRequest, PullRequestDetail};

/// Marker that opens the machine-readable block on every generated PR.
const META_MARKER: &str = "<!-- cogneva-bot-meta -->";
/// Branch prefix every generated PR head uses.
const SELF_BRANCH_PREFIX: &str = "cogneva/";
/// Label applied to a PR that needs a human before it can merge. It is also
/// in the default forbidden-label set, so an annotated PR is structurally
/// ineligible for auto-merge from that point on.
const NEEDS_HUMAN_LABEL: &str = "manual-only";
/// Task type of the deployment intent submitted after a successful merge.
pub const DEPLOY_TASK_TYPE: &str = "merged_change_deploy";

/// Task type [`MergeExecutor`] uses when it asks the orchestrator to carry a
/// merged change into the canary pipeline.
fn deploy_task_id(merge_sha: &str) -> String {
    // Deterministic id: the orchestrator's idempotent skip then collapses
    // repeated submissions for the same merge commit after a restart.
    format!(
        "merged-change-deploy-{}",
        merge_sha.chars().take(12).collect::<String>()
    )
}

/// Outcome tally of one merge round.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MergeRoundStats {
    /// Self-produced PRs examined.
    pub examined: usize,
    /// PRs merged this round.
    pub merged: usize,
    /// PRs held back by a self-clearing gate.
    pub waiting: usize,
    /// PRs newly marked as needing a human.
    pub blocked: usize,
    /// Merge calls that failed (retried next round).
    pub failed: usize,
}

/// Per-PR bookkeeping persisted across restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PrEntry {
    /// Last wait reason, kept for observability.
    #[serde(default)]
    last_reason: String,
    /// Head that was last judged, so a new push reopens judgement.
    #[serde(default)]
    head_sha: String,
    /// Terminal obstacle already annotated (comment + label); such a PR is
    /// not re-judged at the same head.
    #[serde(default)]
    annotated: bool,
    /// change id parsed from the PR body — the index outcome feedback and
    /// deployment triggering need, kept here because the recorder's own map
    /// is in-memory only.
    #[serde(default)]
    change_id: Option<String>,
    /// Merge commit sha once merged.
    #[serde(default)]
    merged_sha: Option<String>,
}

/// Persisted merge bookkeeping (`$COGNEVA_DATA_DIR/merge-state.json`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MergeState {
    #[serde(default)]
    prs: HashMap<u64, PrEntry>,
}

/// Production state path: the shared data dir.
#[cfg(not(test))]
fn merge_state_path() -> PathBuf {
    let dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
    PathBuf::from(dir).join("merge-state.json")
}

/// Test builds resolve a per-instance temp file: a shared path would leak
/// "already annotated/merged" flags across tests and across runs.
#[cfg(test)]
fn merge_state_path() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "merge-state-test-{}-{}.json",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

/// Decides and performs auto-merge of self-produced PRs.
pub struct MergeExecutor {
    policy: AutoMergePolicy,
    identity: BotIdentityConfig,
    state: MergeState,
    path: PathBuf,
    loaded: bool,
}

impl MergeExecutor {
    /// Build an executor from the integration config.
    pub fn new(config: &crate::config::GitHubIntegrationConfig) -> Self {
        Self {
            policy: config.auto_merge_policy.clone(),
            identity: config.bot_identity.clone(),
            state: MergeState::default(),
            path: merge_state_path(),
            loaded: false,
        }
    }

    /// Evaluate and act on every open self-produced PR. Round failures are
    /// logged, never propagated: merging is a background improvement, and a
    /// broken merge round must not stop intent discovery.
    pub async fn run_once(
        &mut self,
        provider: &dyn CodePlatformProvider,
        orchestrator: Option<&dyn OrchestratorControl>,
        reflection: Option<&dyn cog_core::ReflectionEngine>,
    ) -> MergeRoundStats {
        let mut stats = MergeRoundStats::default();
        // Policy off means "behave exactly as before": no listing, no merge,
        // no state churn.
        if !self.policy.enabled {
            return stats;
        }
        // Each platform runs its own discovery loop, so the same executor
        // exists on both. Only the configured target actually merges: the
        // other end receives `main` by mirror, and merging there would fork
        // the two histories.
        if !self.runs_on(provider.platform_kind()) {
            return stats;
        }
        self.ensure_loaded().await;

        let prs = match provider.list_open_pull_requests().await {
            Ok(prs) => prs,
            Err(e) => {
                tracing::warn!(error = %e, "merge executor: failed to list open PRs");
                return stats;
            }
        };
        let open: std::collections::HashSet<u64> = prs.iter().map(|p| p.number).collect();
        // Drop bookkeeping for PRs that left the open set (merged/closed by
        // anyone), so the state file tracks live work only.
        self.state.prs.retain(|n, _| open.contains(n));

        for pr in prs {
            if !self.is_self_pr(&pr) {
                continue;
            }
            stats.examined += 1;
            let detail = match provider.get_pull_request(pr.number).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(pr = pr.number, error = %e, "merge executor: PR detail fetch failed");
                    continue;
                }
            };
            self.consider(provider, &pr, &detail, orchestrator, reflection, &mut stats)
                .await;
        }

        self.persist().await;
        stats
    }

    /// Judge one self-produced PR and act on the verdict.
    async fn consider(
        &mut self,
        provider: &dyn CodePlatformProvider,
        pr: &PlatformPullRequest,
        detail: &PullRequestDetail,
        orchestrator: Option<&dyn OrchestratorControl>,
        reflection: Option<&dyn cog_core::ReflectionEngine>,
        stats: &mut MergeRoundStats,
    ) {
        let change_id = parse_change_id(&pr.body);
        let entry = self.state.prs.entry(detail.number).or_default();
        if entry.head_sha != detail.head_sha {
            // A new push is a new proposal: re-judge from scratch.
            entry.head_sha = detail.head_sha.clone();
            entry.annotated = false;
        }
        if entry.change_id.is_none() {
            entry.change_id = change_id;
        }
        // Already merged (seen by the terminal-state check below but not yet
        // reaped) — nothing left to do.
        if entry.merged_sha.is_some() {
            return;
        }
        // A PR already marked as needing a human at this head is not
        // re-judged; the mark itself (label) is the standing signal.
        if entry.annotated {
            return;
        }

        // Self-review gate. Missing or unparseable evidence is not evidence:
        // it waits rather than merges.
        if self.policy.require_self_review {
            match parse_self_review(&pr.body) {
                Some(score) if score >= self.policy.min_self_review_score => {}
                Some(score) => {
                    let reason = format!(
                        "self-review score {score:.2} below minimum {:.2}",
                        self.policy.min_self_review_score
                    );
                    entry.last_reason = reason.clone();
                    self.annotate(provider, detail, reflection, &reason, stats)
                        .await;
                    return;
                }
                None => {
                    entry.last_reason = "self-review score missing".into();
                    crate::observable::global_merge_observable()
                        .record("waiting", "self_review_missing");
                    stats.waiting += 1;
                    return;
                }
            }
        }

        match MergeDecider::can_auto_merge(detail, &self.policy) {
            MergeDecision::AutoMerge => {
                self.merge(provider, detail, &pr.body, orchestrator, stats)
                    .await;
            }
            MergeDecision::Wait { reason, terminal } => {
                entry.last_reason = reason.clone();
                if terminal {
                    // A gate only a human can clear (red CI, over budget,
                    // forbidden path/label, requested review). Annotate once
                    // and stop burning rounds on it. The unsatisfied fix
                    // intent is itself a signal the main loop will regenerate.
                    self.annotate(provider, detail, reflection, &reason, stats)
                        .await;
                } else {
                    crate::observable::global_merge_observable()
                        .record("waiting", reason_kind(&reason));
                    stats.waiting += 1;
                }
            }
        }
    }

    /// Merge a PR that passed every gate, then trigger the deployment leg.
    async fn merge(
        &mut self,
        provider: &dyn CodePlatformProvider,
        detail: &PullRequestDetail,
        pr_body: &str,
        orchestrator: Option<&dyn OrchestratorControl>,
        stats: &mut MergeRoundStats,
    ) {
        let observable = crate::observable::global_merge_observable();
        // Re-check the frozen head right before acting: the detail was fetched
        // this round, and the sha is what the decision was made on.
        match provider
            .merge_pull_request(detail.number, detail.head_sha.clone())
            .await
        {
            Ok(()) => {
                if let Some(entry) = self.state.prs.get_mut(&detail.number) {
                    entry.merged_sha = Some(detail.head_sha.clone());
                    entry.last_reason = "merged".into();
                }
                stats.merged += 1;
                observable.record("auto_merged", "ok");
                tracing::info!(
                    pr = detail.number,
                    sha = %detail.head_sha,
                    "merge executor: self-produced PR merged"
                );
                self.after_merge(provider, detail, pr_body, orchestrator)
                    .await;
            }
            Err(e) => {
                let text = e.to_string();
                // A conflict is not transient: the branch would have to be
                // rebased, which nothing here can do. Every other failure
                // (network, rate limit, transient API error) is retried next
                // round.
                if is_conflict(&text) {
                    self.annotate(
                        provider,
                        detail,
                        None,
                        &format!("merge conflict: {text}"),
                        stats,
                    )
                    .await;
                } else {
                    stats.failed += 1;
                    observable.record("failed", "merge_call");
                    tracing::warn!(
                        pr = detail.number,
                        error = %text,
                        "merge executor: merge call failed; retrying next round"
                    );
                }
            }
        }
    }

    /// Post-merge actions: trigger deployment and leave a comment with the
    /// evidence trail.
    async fn after_merge(
        &self,
        provider: &dyn CodePlatformProvider,
        detail: &PullRequestDetail,
        pr_body: &str,
        orchestrator: Option<&dyn OrchestratorControl>,
    ) {
        let change_id = parse_change_id(pr_body);
        let mut deploy_note = "deployment trigger disabled".to_string();
        if self.policy.trigger_deploy_after_merge {
            match orchestrator {
                Some(orch) => {
                    // The PR head sha is what was merged, not the merge commit
                    // main now points at; naming it `head_sha` keeps the
                    // payload honest about which of the two it carries.
                    let goal = format!(
                        "Deploy main after merging PR #{} (head {}).",
                        detail.number, detail.head_sha
                    );
                    let task = Task::new(
                        deploy_task_id(&detail.head_sha),
                        TaskType::Custom(DEPLOY_TASK_TYPE.into()),
                        serde_json::json!({
                            "pr_head_sha": detail.head_sha,
                            "pr_number": detail.number,
                            "change_id": change_id,
                            "goal": goal.clone(),
                        }),
                    );
                    match orch.submit_goal_auto(&goal, vec![task]).await {
                        Ok(ids) => deploy_note = format!("deployment task {}", ids.join(", ")),
                        Err(e) => {
                            deploy_note = format!("deployment trigger failed: {e}");
                            tracing::warn!(
                                pr = detail.number,
                                error = %e,
                                "merge executor: deployment intent submission failed"
                            );
                        }
                    }
                }
                None => deploy_note = "no orchestrator available".into(),
            }
        }
        let body = format!(
            "Auto-merged this change.\n\n\
             - merged head: `{}`\n\
             - self-review: {}\n\
             - {}",
            detail.head_sha,
            parse_self_review_note(pr_body),
            deploy_note
        );
        if let Err(e) = provider.comment_on_pull(detail.number, body).await {
            tracing::warn!(pr = detail.number, error = %e, "merge executor: merge comment failed");
        }
    }

    /// Mark a PR as needing a human: one comment with the reason, one label
    /// that structurally disqualifies it from auto-merge, and one outcome
    /// record so the reflection engine learns from the failure.
    async fn annotate(
        &mut self,
        provider: &dyn CodePlatformProvider,
        detail: &PullRequestDetail,
        reflection: Option<&dyn cog_core::ReflectionEngine>,
        reason: &str,
        stats: &mut MergeRoundStats,
    ) {
        self.state.prs.entry(detail.number).or_default().annotated = true;
        stats.blocked += 1;
        crate::observable::global_merge_observable().record("blocked", reason_kind(reason));
        tracing::warn!(
            pr = detail.number,
            reason,
            "merge executor: PR needs a human before it can merge"
        );

        let body = format!(
            "This change cannot be auto-merged and needs a human:\n\n> {reason}\n\n\
             Annotated with `{NEEDS_HUMAN_LABEL}`; automatic merge will not be retried for this revision."
        );
        if let Err(e) = provider.comment_on_pull(detail.number, body).await {
            tracing::warn!(pr = detail.number, error = %e, "merge executor: annotation comment failed");
        }
        if let Err(e) = provider
            .add_labels(detail.number, &[NEEDS_HUMAN_LABEL.to_string()])
            .await
        {
            tracing::warn!(pr = detail.number, error = %e, "merge executor: annotation label failed");
        }
        if let Some(reflection) = reflection {
            let change_id = self
                .state
                .prs
                .get(&detail.number)
                .and_then(|e| e.change_id.clone())
                .unwrap_or_else(|| format!("pr-{}", detail.number));
            if let Err(e) = reflection
                .record_change_outcome(&change_id, false, reason)
                .await
            {
                tracing::warn!(change_id = %change_id, error = %e, "merge executor: outcome record failed");
            }
        }
    }

    /// Whether this executor should act on the given platform. `Primary`
    /// means "wherever this loop's provider points", which is how the loop
    /// behaved before the setting existed.
    fn runs_on(&self, platform: &str) -> bool {
        match self.policy.merge_target_platform {
            crate::config::MergeTargetPlatform::Primary => true,
            crate::config::MergeTargetPlatform::Github => platform == "github",
            crate::config::MergeTargetPlatform::Gitee => platform == "gitee",
        }
    }

    /// Whether a PR was produced by this instance. Three signals, and the
    /// per-instance handle is mandatory: a branch name and a login are both
    /// trivially forgeable, while the handle embeds the instance fingerprint,
    /// so requiring it is what makes a forged `cogneva/` branch useless as a
    /// merge trigger. Missing metadata fails closed.
    fn is_self_pr(&self, pr: &PlatformPullRequest) -> bool {
        let branch_ok = pr.head_branch.starts_with(SELF_BRANCH_PREFIX);
        let author_ok = self.identity.is_own_login(&pr.author);
        let handle_ok = parse_meta_bot(&pr.body)
            .map(|h| h == self.identity.own_handle())
            .unwrap_or(false);
        (branch_ok || author_ok) && handle_ok
    }

    async fn ensure_loaded(&mut self) {
        if self.loaded {
            return;
        }
        self.loaded = true;
        let Ok(text) = tokio::fs::read_to_string(&self.path).await else {
            return; // first boot — nothing merged yet
        };
        match serde_json::from_str::<MergeState>(&text) {
            Ok(state) => self.state = state,
            Err(e) => tracing::warn!(
                path = %self.path.display(),
                error = %e,
                "merge executor: state file corrupt; starting fresh"
            ),
        }
    }

    async fn persist(&self) {
        if let Some(parent) = self.path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Ok(text) = serde_json::to_string(&self.state) {
            if let Err(e) = tokio::fs::write(&self.path, text).await {
                tracing::warn!(error = %e, "merge executor: state file not writable");
            }
        }
    }
}

/// Read `bot:` out of the metadata block.
fn parse_meta_bot(body: &str) -> Option<String> {
    parse_meta_field(body, "bot:")
}

/// Read the `eval:` line out of the metadata block.
fn parse_meta_eval(body: &str) -> Option<String> {
    parse_meta_field(body, "eval:")
}

/// Read a single-valued line from the machine-readable block. The block ends
/// at the first blank line after it starts, so a stray `eval:` later in the
/// human-readable body cannot spoof a score. The blank line that follows the
/// marker itself is part of the marker's own line break, not the end of the
/// block.
fn parse_meta_field(body: &str, key: &str) -> Option<String> {
    let start = body.find(META_MARKER)? + META_MARKER.len();
    let mut in_block = false;
    for line in body[start..].lines() {
        let line = line.trim();
        if line.is_empty() {
            if in_block {
                break;
            }
            continue;
        }
        in_block = true;
        if let Some(value) = line.strip_prefix(key) {
            let value = value.trim();
            if value.is_empty() {
                return None;
            }
            return Some(value.to_string());
        }
    }
    None
}

/// Parse `eval: self-review=<score>`. Anything else — a missing block, a
/// missing `eval` line, `n/a`, a non-numeric value — yields `None`, which the
/// caller treats as "gate not passed" rather than "no opinion".
fn parse_self_review(body: &str) -> Option<f32> {
    let eval = parse_meta_eval(body)?;
    eval.trim()
        .strip_prefix("self-review=")?
        .trim()
        .parse::<f32>()
        .ok()
}

/// Parse the change id the publisher embeds in the body. Used for outcome
/// feedback and deployment correlation; a miss only degrades attribution.
fn parse_change_id(body: &str) -> Option<String> {
    let rest = body.split("Change `").nth(1)?;
    let id = rest.split('`').next()?.trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

/// Human-readable self-review note for the merge comment.
fn parse_self_review_note(body: &str) -> String {
    match parse_self_review(body) {
        Some(score) => format!("{score:.2}"),
        None => "n/a".into(),
    }
}

/// Coarse reason bucket for metrics labels. Free-text reasons must never
/// reach a Prometheus label — the cardinality would grow without bound.
fn reason_kind(reason: &str) -> &'static str {
    let r = reason.to_ascii_lowercase();
    if r.contains("ci is failing") {
        "ci_failed"
    } else if r.contains("ci status unknown") {
        "ci_pending"
    } else if r.contains("cooldown") {
        "cooldown"
    } else if r.contains("changed lines") {
        "over_budget"
    } else if r.contains("forbidden path") {
        "forbidden_path"
    } else if r.contains("forbidden label") {
        "forbidden_label"
    } else if r.contains("self-review") {
        // Checked before the generic "review" bucket: the self-review gate's
        // reason text contains "review" too, and a low score is a different
        // failure from a requested human reviewer.
        "self_review"
    } else if r.contains("review") {
        "review_requested"
    } else if r.contains("conflict") {
        "conflict"
    } else {
        "other"
    }
}

/// Whether a merge API error is a hard conflict. GitHub reports an
/// unmergeable PR as 405 with "Pull request is not mergeable", and a racing
/// merge as 409; anything else is treated as transient and retried.
fn is_conflict(error_text: &str) -> bool {
    let t = error_text.to_ascii_lowercase();
    // The bare status codes are matched on digit boundaries so that an
    // unrelated number (a sha, a byte count) cannot terminally block a PR
    // that would otherwise have merged on the next round.
    t.contains("conflict")
        || t.contains("not mergeable")
        || has_status_code(&t, "405")
        || has_status_code(&t, "409")
}

/// Whether `text` contains `code` as a standalone number.
fn has_status_code(text: &str, code: &str) -> bool {
    text.match_indices(code).any(|(i, _)| {
        let before_ok = !text[..i]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_digit());
        let after_ok = !text[i + code.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit());
        before_ok && after_ok
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MergeTargetPlatform;

    fn body_with(handle: &str, eval: &str) -> String {
        format!(
            "Change `chg-1` generated autonomously by Cogneva (PGE mode: ralph).\n\n\
             {META_MARKER}\nbot: {handle}\nenv: prod, 4c/8G\neval: {eval}\nrelated: n/a"
        )
    }

    #[test]
    fn parses_self_review_score() {
        assert_eq!(
            parse_self_review(&body_with("Alice#a3f9d2c1", "self-review=0.87")),
            Some(0.87)
        );
        assert_eq!(parse_self_review(&body_with("Alice#a3f9d2c1", "n/a")), None);
        assert_eq!(parse_self_review("no metadata block here"), None);
    }

    #[test]
    fn metadata_field_stops_at_block_end() {
        // A later `eval:` in the human-readable body must not be read as the
        // machine-readable one.
        let body = format!(
            "{META_MARKER}\nbot: Alice#a3f9d2c1\neval: n/a\n\n\
             prose\n\neval: self-review=0.99"
        );
        assert_eq!(parse_self_review(&body), None);
        assert_eq!(parse_meta_bot(&body).as_deref(), Some("Alice#a3f9d2c1"));
    }

    #[test]
    fn parses_change_id() {
        assert_eq!(
            parse_change_id(&body_with("Alice#a3f9d2c1", "self-review=0.9")).as_deref(),
            Some("chg-1")
        );
        assert_eq!(parse_change_id("no change here"), None);
    }

    #[test]
    fn self_pr_requires_handle_evidence() {
        let mut config = crate::config::GitHubIntegrationConfig::default();
        config.bot_identity.username = "cogneva-bot".into();
        config.bot_identity.fingerprint = Some("a".repeat(64));
        let executor = MergeExecutor::new(&config);
        let handle = executor.identity.own_handle();

        let pr = |branch: &str, author: &str, body: String| PlatformPullRequest {
            number: 1,
            title: "t".into(),
            url: String::new(),
            state: "open".into(),
            head_branch: branch.into(),
            base_branch: "main".into(),
            body,
            author: author.into(),
            labels: vec![],
        };

        // Own branch + own handle → self-produced.
        assert!(executor.is_self_pr(&pr(
            "cogneva/auto-x",
            "someone-else",
            body_with(&handle, "self-review=0.9")
        )));
        // Forged branch without the instance handle → not ours.
        assert!(!executor.is_self_pr(&pr(
            "cogneva/auto-x",
            "someone-else",
            body_with("Mallory#deadbeef", "self-review=0.99")
        )));
        // No metadata block at all → fails closed.
        assert!(!executor.is_self_pr(&pr("cogneva/auto-x", "someone-else", "hi".into())));
        // Foreign PR by a stranger → not ours.
        assert!(!executor.is_self_pr(&pr(
            "feature/x",
            "someone-else",
            body_with(&handle, "self-review=0.9")
        )));
    }

    #[test]
    fn reason_kinds_are_bounded() {
        assert_eq!(reason_kind("CI is failing"), "ci_failed");
        assert_eq!(reason_kind("CI status unknown"), "ci_pending");
        assert_eq!(reason_kind("cooldown of 24h not elapsed"), "cooldown");
        assert_eq!(
            reason_kind("changed lines 500 exceed budget 200"),
            "over_budget"
        );
        assert_eq!(
            reason_kind("touches forbidden path: deploy/x"),
            "forbidden_path"
        );
        assert_eq!(reason_kind("forbidden label: security"), "forbidden_label");
        assert_eq!(reason_kind("something nobody predicted"), "other");
    }

    #[test]
    fn conflict_detection() {
        assert!(is_conflict("HTTP 409 Conflict"));
        assert!(is_conflict("Pull request is not mergeable"));
        assert!(is_conflict("gitee merge failed: status 405"));
        assert!(!is_conflict("HTTP 500 internal server error"));
        // A bare number that merely contains 405/409 must not terminalize a
        // PR that would merge on the next round.
        assert!(!is_conflict("upstream sha 4059ab12c0 rejected by gateway"));
        assert!(!is_conflict("timed out after 4096ms"));
    }

    #[test]
    fn self_review_failures_are_not_bucketed_as_review_requests() {
        assert_eq!(
            reason_kind("self-review score 0.42 below minimum 0.80"),
            "self_review"
        );
        assert_eq!(reason_kind("human review requested"), "review_requested");
    }

    #[test]
    fn default_policy_requires_self_review_on_github() {
        let policy = AutoMergePolicy::default();
        assert!(policy.require_self_review);
        assert_eq!(policy.min_self_review_score, 0.80);
        assert_eq!(policy.merge_target_platform, MergeTargetPlatform::Github);
        assert!(policy.legacy_pr_reap);
        assert!(policy.trigger_deploy_after_merge);
        assert!(!policy.local_gate_signals);
    }

    #[test]
    fn only_the_target_platform_merges() {
        let mut config = crate::config::GitHubIntegrationConfig::default();
        let executor = MergeExecutor::new(&config);
        assert!(executor.runs_on("github"));
        assert!(!executor.runs_on("gitee"));

        config.auto_merge_policy.merge_target_platform = MergeTargetPlatform::Gitee;
        let executor = MergeExecutor::new(&config);
        assert!(executor.runs_on("gitee"));
        assert!(!executor.runs_on("github"));

        config.auto_merge_policy.merge_target_platform = MergeTargetPlatform::Primary;
        let executor = MergeExecutor::new(&config);
        assert!(executor.runs_on("github"));
        assert!(executor.runs_on("gitee"));
    }

    #[test]
    fn deploy_task_id_is_deterministic() {
        let sha = "abcdef0123456789abcdef0123456789abcdef01";
        assert_eq!(deploy_task_id(sha), deploy_task_id(sha));
        assert!(deploy_task_id(sha).ends_with("abcdef012345"));
    }
}
