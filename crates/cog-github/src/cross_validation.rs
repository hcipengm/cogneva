//! Cross-validation between private instances (A2A): every bot polls the
//! public repo for open PRs carrying the `cogneva-bot` label, applies the
//! change in its own sandbox, runs the workspace tests and eval, and comments
//! the verdict back on the PR. Pure outbound polling — instances behind NAT
//! participate without any inbound webhook.
//!
//! This module holds only the deterministic pieces (self-recognition via the
//! metadata block, verdict extraction from the task result, comment/state
//! rendering, diff fetch). The actual apply/test/eval is an intelligent step:
//! it is packaged as a `pr_cross_validate` orchestrator task and executed by
//! the collaboration main flow; this crate never calls an LLM directly.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::{CogGitHubError, Result};
use crate::provider::{PlatformComment, PlatformPullRequest};

/// Label marking PRs that are open to cross-validation by any instance.
pub const CROSS_VALIDATION_LABEL: &str = "cogneva-bot";

/// Orchestrator task kind for the sandbox validation run.
pub const CROSS_VALIDATE_TASK_KIND: &str = "pr_cross_validate";

/// Cap on a PR diff fed into the task payload; larger PRs are skipped rather
/// than blowing the task input.
pub const MAX_CV_DIFF_BYTES: usize = 512 * 1024;

/// One in-flight cross-validation task for a PR.
#[derive(Debug, Clone)]
pub struct CvInflight {
    /// Orchestrator task id being polled.
    pub task_id: String,
    /// Head commit SHA the task is validating (PRs may get pushed mid-run).
    pub head_sha: String,
}

impl CvInflight {
    /// Bind a validation task id to the PR head SHA it is validating.
    pub fn new(task_id: impl Into<String>, head_sha: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            head_sha: head_sha.into(),
        }
    }
}

/// The structured verdict returned by the validation task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossValidationVerdict {
    /// `pass`, `fail` or `inconclusive`.
    pub verdict: String,
    /// One-paragraph human-readable conclusion.
    pub summary: String,
    /// `cargo test` result summary.
    pub tests: String,
    /// Eval A/B summary, or "not applicable".
    pub eval_note: String,
}

/// Persisted cross-validation record, keyed by PR number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedEntry {
    /// Head SHA validated; a newer push invalidates the record.
    pub head_sha: String,
    /// `pass` / `fail` / `inconclusive` / `error` (task failed to produce a
    /// verdict).
    pub verdict: String,
    /// Whether the verdict comment was posted.
    pub commented: bool,
    /// RFC3339 timestamp.
    pub at: String,
}

/// State file content: which PR head SHAs this instance already validated,
/// keyed by PR number.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossValidationState {
    /// PR number → last validated entry.
    pub validated: HashMap<u64, ValidatedEntry>,
}

impl CrossValidationState {
    /// Load from the state file; missing/corrupt files start empty.
    pub async fn load() -> Self {
        match tokio::fs::read_to_string(cross_validation_state_path()).await {
            Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    /// Best-effort persist; a failure only logs (the comment scan on the PR
    /// itself still prevents double-posting).
    pub async fn save(&self) {
        let path = cross_validation_state_path();
        let Ok(json) = serde_json::to_string_pretty(self) else {
            return;
        };
        let Some(parent) = path.parent() else {
            return;
        };
        if tokio::fs::create_dir_all(parent).await.is_ok() {
            if let Err(e) = tokio::fs::write(&path, json).await {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "cross-validation state file not writable; re-validation may repeat after restart"
                );
            }
        }
    }

    /// Record a validation outcome for `pr` at `head_sha`.
    pub fn record(&mut self, pr: u64, head_sha: String, verdict: String, commented: bool) {
        self.validated.insert(
            pr,
            ValidatedEntry {
                head_sha,
                verdict,
                commented,
                at: chrono::Utc::now().to_rfc3339(),
            },
        );
    }

    /// Whether this exact PR head was already validated.
    pub fn is_validated(&self, pr: u64, head_sha: &str) -> bool {
        self.validated
            .get(&pr)
            .map_or(false, |e| e.head_sha == head_sha)
    }
}

/// State file location: `$COGNEVA_DATA_DIR/cross-validation.json`.
pub fn cross_validation_state_path() -> PathBuf {
    let dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
    PathBuf::from(dir).join("cross-validation.json")
}

/// Extract the authoring instance handle from a PR body's metadata block
/// (`bot: <handle>` after the `cogneva-bot-meta` marker).
pub fn parse_meta_bot_handle(pr_body: &str) -> Option<String> {
    let mut after_marker = false;
    for line in pr_body.lines() {
        if !after_marker {
            if line.contains("cogneva-bot-meta") {
                after_marker = true;
            }
            continue;
        }
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        if let Some(rest) = line.strip_prefix("bot:") {
            let handle = rest.trim();
            if !handle.is_empty() {
                return Some(handle.to_string());
            }
        }
    }
    None
}

/// Whether a PR was produced by this instance. The metadata-block handle is
/// authoritative (it survives shared bot accounts); a PR without a metadata
/// block is treated as self when authored by the configured platform account,
/// a conservative guard for pre-metadata self PRs.
pub fn is_self_pr(pr: &PlatformPullRequest, own_handle: Option<&str>, own_username: &str) -> bool {
    let meta_bot = parse_meta_bot_handle(&pr.body);
    if let (Some(own), Some(bot)) = (own_handle, meta_bot.as_deref()) {
        if own == bot {
            return true;
        }
    }
    if meta_bot.is_none() && !own_username.is_empty() && pr.author == own_username {
        return true;
    }
    false
}

/// Machine-detectable signature embedded in a verdict comment, also used by
/// the future contribution board to attribute verdicts per instance/head.
pub fn verdict_comment_marker(pr: u64, head_sha: &str, handle: &str) -> String {
    let sha8: String = head_sha.chars().take(8).collect();
    format!("<!-- cogneva-cv: pr={pr} head={sha8} bot={handle} -->")
}

/// Whether this instance already posted a verdict comment for this exact PR
/// head. Scanned from the PR comments so it survives state loss/restarts.
pub fn comment_already_posted(
    comments: &[PlatformComment],
    pr: u64,
    head_sha: &str,
    handle: &str,
) -> bool {
    let needle = verdict_comment_marker(pr, head_sha, handle);
    comments.iter().any(|c| c.body.contains(&needle))
}

/// Render the verdict comment posted back onto the PR.
pub fn render_verdict_comment(
    v: &CrossValidationVerdict,
    pr: u64,
    head_sha: &str,
    handle: &str,
) -> String {
    let sha8: String = head_sha.chars().take(8).collect();
    let summary = v.summary.trim();
    let tests = if v.tests.trim().is_empty() {
        "not reported".to_string()
    } else {
        v.tests.trim().to_string()
    };
    let eval = if v.eval_note.trim().is_empty() {
        "not reported".to_string()
    } else {
        v.eval_note.trim().to_string()
    };
    format!(
        "## Cogneva cross-validation\n\n\
         Instance `{handle}` validated this PR in its own sandbox (head `{sha8}`).\n\n\
         **Verdict: {}**\n\n\
         {summary}\n\n\
         - Tests: {tests}\n\
         - Eval: {eval}\n\n\
         {marker}",
        v.verdict.to_uppercase(),
        marker = verdict_comment_marker(pr, head_sha, handle),
    )
}

/// Extract the validation verdict from a completed task result. The squad
/// output shape varies (flat verdict object or a PGE artifact containing the
/// verdict JSON), so every known location is probed; `None` means no
/// well-formed verdict was produced (treated as an infra error, never as a
/// pass).
pub fn extract_verdict(result: &serde_json::Value) -> Option<CrossValidationVerdict> {
    if let Some(v) = verdict_from_object(result) {
        return Some(v);
    }
    if let Some(cv) = result.get("cross_validation") {
        if let Some(v) = verdict_from_object(cv) {
            return Some(v);
        }
    }
    // PGE pipeline/roundtable final generation artifacts.
    if let Some(artifacts) = result
        .pointer("/squad_result/result/final_generation/artifacts")
        .and_then(|a| a.as_array())
    {
        for artifact in artifacts {
            let Some(content) = artifact.get("content").and_then(|c| c.as_str()) else {
                continue;
            };
            if let Some(json) = first_json_object(content) {
                if let Some(v) = verdict_from_object(&json) {
                    return Some(v);
                }
            }
        }
    }
    None
}

/// Parse a verdict from one JSON object with a `verdict` field.
fn verdict_from_object(obj: &serde_json::Value) -> Option<CrossValidationVerdict> {
    let raw = obj
        .get("verdict")
        .and_then(|v| v.as_str())?
        .trim()
        .to_lowercase();
    let verdict = if raw.starts_with("pass") {
        "pass"
    } else if raw.starts_with("fail") {
        "fail"
    } else if raw.contains("inconc") {
        "inconclusive"
    } else {
        return None;
    };
    let field = |name: &str| {
        obj.get(name)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string()
    };
    let mut eval_note = field("eval");
    if eval_note.is_empty() {
        eval_note = field("eval_note");
    }
    Some(CrossValidationVerdict {
        verdict: verdict.to_string(),
        summary: field("summary"),
        tests: field("tests"),
        eval_note,
    })
}

/// Find and parse the first balanced-looking JSON object in a text that may
/// carry a code fence or surrounding prose.
fn first_json_object(text: &str) -> Option<serde_json::Value> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str(&text[start..=end]).ok()
}

/// Fetch a PR's unified diff in the local clone (credentials live in the main
/// process). The diff is the three-dot change from the merge-base with the
/// base branch to the PR head, i.e. exactly what the PR contributes.
pub async fn fetch_pr_diff(workdir: &Path, base_branch: &str, head_branch: &str) -> Result<String> {
    run_git(workdir, &["fetch", "origin", base_branch]).await?;
    run_git(workdir, &["fetch", "origin", head_branch]).await?;
    let range = format!("origin/{base_branch}...FETCH_HEAD");
    let diff = run_git(workdir, &["diff", &range]).await?;
    if diff.trim().is_empty() {
        return Err(CogGitHubError::Provider(
            "PR diff is empty after fetch".into(),
        ));
    }
    if diff.len() > MAX_CV_DIFF_BYTES {
        return Err(CogGitHubError::Provider(format!(
            "PR diff too large for cross-validation payload: {} bytes (cap {MAX_CV_DIFF_BYTES})",
            diff.len()
        )));
    }
    Ok(diff)
}

async fn run_git(workdir: &Path, args: &[&str]) -> Result<String> {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(workdir)
        .output()
        .await?;
    if !output.status.success() {
        return Err(CogGitHubError::Provider(format!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ENV_LOCK;

    fn pr_with(body: &str, author: &str) -> PlatformPullRequest {
        PlatformPullRequest {
            number: 7,
            title: "t".into(),
            url: "u".into(),
            state: "open".into(),
            head_branch: "cogneva/auto-x".into(),
            base_branch: "main".into(),
            body: body.into(),
            author: author.into(),
            labels: vec![CROSS_VALIDATION_LABEL.into()],
        }
    }

    #[test]
    fn parse_meta_bot_handle_reads_block() {
        let body = "Intro\n<!-- cogneva-bot-meta -->\nbot: Alice#a3f9d2c1\nenv: prod\n\ntail";
        assert_eq!(
            parse_meta_bot_handle(body).as_deref(),
            Some("Alice#a3f9d2c1")
        );
        assert!(parse_meta_bot_handle("no block here\nbot: x").is_none());
        assert!(parse_meta_bot_handle("<!-- cogneva-bot-meta -->\nno bot line\n").is_none());
    }

    #[test]
    fn self_pr_detection() {
        let own = "Alice#a3f9d2c1";
        let body = format!("<!-- cogneva-bot-meta -->\nbot: {own}\nenv: prod\n");
        assert!(is_self_pr(
            &pr_with(&body, "shared-bot"),
            Some(own),
            "cogneva-bot"
        ));

        let other = "<!-- cogneva-bot-meta -->\nbot: Bob#b81c0e9f\nenv: prod\n";
        // Different instance, even on the same shared platform account.
        assert!(!is_self_pr(
            &pr_with(other, "shared-bot"),
            Some(own),
            "shared-bot"
        ));

        // No metadata block: same platform account => conservatively self.
        assert!(is_self_pr(
            &pr_with("old style PR", "cogneva-bot"),
            Some(own),
            "cogneva-bot"
        ));
        // No metadata block, different account => another bot, validate it.
        assert!(!is_self_pr(
            &pr_with("old style PR", "someone-else"),
            Some(own),
            "cogneva-bot"
        ));
    }

    #[test]
    fn verdict_extraction_from_flat_object() {
        let v = extract_verdict(&serde_json::json!({
            "verdict": "pass",
            "summary": "all good",
            "tests": "cargo test: 512 passed",
            "eval": "not applicable"
        }))
        .unwrap();
        assert_eq!(v.verdict, "pass");
        assert_eq!(v.tests, "cargo test: 512 passed");
    }

    #[test]
    fn verdict_extraction_from_artifact_with_fenced_json() {
        let result = serde_json::json!({
            "squad_result": {
                "result": {
                    "final_generation": {
                        "artifacts": [
                            {"name": "notes.md", "content": "prose", "artifact_type": "report"},
                            {"name": "verdict.json", "artifact_type": "report",
                             "content": "```json\n{\"verdict\":\"failed\",\"summary\":\"compile error\",\"tests\":\"cargo check failed\",\"eval\":\"n/a\"}\n```"}
                        ]
                    }
                }
            }
        });
        let v = extract_verdict(&result).unwrap();
        assert_eq!(v.verdict, "fail");
        assert_eq!(v.summary, "compile error");
    }

    #[test]
    fn verdict_extraction_returns_none_without_verdict() {
        assert!(extract_verdict(&serde_json::json!({"squad_result": {}})).is_none());
        assert!(extract_verdict(&serde_json::json!({"verdict": "maybe"})).is_none());
    }

    #[test]
    fn comment_marker_dedup_is_per_instance_and_head() {
        let handle = "Alice#a3f9d2c1";
        let body = render_verdict_comment(
            &CrossValidationVerdict {
                verdict: "pass".into(),
                summary: "ok".into(),
                tests: "t".into(),
                eval_note: "n/a".into(),
            },
            42,
            "abcdef1234567890",
            handle,
        );
        assert!(body.contains("Verdict: PASS"));
        assert!(body.contains("Alice#a3f9d2c1"));
        assert!(body.contains("abcdef12"));

        let comments = vec![PlatformComment {
            author: "cogneva-bot".into(),
            body,
            created_at: chrono::Utc::now(),
        }];
        // Same instance + head => already posted.
        assert!(comment_already_posted(
            &comments,
            42,
            "abcdef1234567890",
            handle
        ));
        // Different instance must still post its own verdict.
        assert!(!comment_already_posted(
            &comments,
            42,
            "abcdef1234567890",
            "Bob#b81c0e9f"
        ));
        // Same instance, new head push => validate/comment again.
        assert!(!comment_already_posted(
            &comments,
            42,
            "9988776655443322",
            handle
        ));
    }

    #[test]
    fn state_records_and_invalidates_on_new_head() {
        let mut state = CrossValidationState::default();
        assert!(!state.is_validated(1, "aaa"));
        state.record(1, "aaa".into(), "pass".into(), true);
        assert!(state.is_validated(1, "aaa"));
        assert!(!state.is_validated(1, "bbb"));
        let round_trip: CrossValidationState =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        assert_eq!(round_trip, state);
    }

    #[tokio::test]
    async fn state_persists_to_data_dir() {
        let _guard = ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let mut state = CrossValidationState::default();
        state.record(9, "deadbeef".into(), "fail".into(), false);
        state.save().await;
        assert!(cross_validation_state_path().exists());
        let loaded = CrossValidationState::load().await;
        assert_eq!(loaded, state);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[tokio::test]
    async fn fetch_pr_diff_returns_three_dot_change() {
        // Nest the repo under the tempdir so `../origin.git` resolves *inside*
        // this test's unique tempdir (parallel tests must not share /tmp).
        let workdir = tempfile::tempdir().unwrap();
        let root = workdir.path().join("repo");
        std::fs::create_dir(&root).unwrap();
        let sh = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(args)
                .current_dir(&root)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {:?}: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        };
        sh(&["init", "-q", "-b", "main", "."]);
        sh(&["config", "user.email", "t@t"]);
        sh(&["config", "user.name", "t"]);
        std::fs::write(root.join("lib.txt"), "base\n").unwrap();
        sh(&["add", "."]);
        sh(&["commit", "-q", "-m", "base"]);

        // Serve as its own "origin" so fetch works locally.
        sh(&["clone", "-q", "--bare", ".", "../origin.git"]);
        sh(&["remote", "add", "origin", "../origin.git"]);

        sh(&["checkout", "-q", "-b", "cogneva/auto-1"]);
        std::fs::write(root.join("lib.txt"), "base\nchange\n").unwrap();
        sh(&["commit", "-aq", "-m", "change"]);
        sh(&["push", "-q", "origin", "cogneva/auto-1"]);
        sh(&["checkout", "-q", "main"]);

        let diff = fetch_pr_diff(&root, "main", "cogneva/auto-1")
            .await
            .unwrap();
        assert!(diff.contains("+change"));
        assert!(diff.contains("+++ b/lib.txt"));
    }
}
