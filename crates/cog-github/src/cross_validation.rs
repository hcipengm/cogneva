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

/// Machine-readable marker prefix carrying a validating instance's A/B eval
/// metrics in a verdict comment. The board action parses these to rank
/// competing solutions by measured improvement and statistical significance.
pub const EVAL_MARKER_PREFIX: &str = "<!-- cogneva-eval:";

/// Structured A/B eval metrics for one validated candidate. Field names are
/// shortened for the inline comment marker; the same schema is what the
/// validation task is asked to return.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalMetrics {
    /// Whether an eval A/B applied to this change (false = tests-only verdict).
    #[serde(default)]
    pub applicable: bool,
    /// Success rate before the change (baseline), 0.0–1.0.
    #[serde(default, rename = "rb", skip_serializing_if = "Option::is_none")]
    pub rate_before: Option<f64>,
    /// Success rate after the change (treatment), 0.0–1.0.
    #[serde(default, rename = "ra", skip_serializing_if = "Option::is_none")]
    pub rate_after: Option<f64>,
    /// Baseline sample size.
    #[serde(default, rename = "nb", skip_serializing_if = "Option::is_none")]
    pub n_before: Option<u64>,
    /// Treatment sample size.
    #[serde(default, rename = "na", skip_serializing_if = "Option::is_none")]
    pub n_after: Option<u64>,
    /// Two-proportion z statistic for the success-rate change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub z: Option<f64>,
    /// Whether the success-rate change is statistically significant (|z|>1.96).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub significant: bool,
    /// Mean latency (ms) before the change.
    #[serde(default, rename = "lb", skip_serializing_if = "Option::is_none")]
    pub latency_before_ms: Option<u64>,
    /// Mean latency (ms) after the change.
    #[serde(default, rename = "la", skip_serializing_if = "Option::is_none")]
    pub latency_after_ms: Option<u64>,
}

impl EvalMetrics {
    /// Percentage-point improvement in success rate (after − before), if both
    /// rates are present. Positive means the candidate is better.
    pub fn improvement_pp(&self) -> Option<f64> {
        match (self.rate_before, self.rate_after) {
            (Some(b), Some(a)) => Some((a - b) * 100.0),
            _ => None,
        }
    }
}

/// Render the inline comment marker for a metrics payload.
pub fn eval_marker(m: &EvalMetrics) -> String {
    let json = serde_json::to_string(m).unwrap_or_else(|_| "{}".into());
    format!("{EVAL_MARKER_PREFIX} {json} -->")
}

/// Extract the metrics marker from a verdict comment body, if present.
pub fn parse_eval_marker(body: &str) -> Option<EvalMetrics> {
    let start = body.find(EVAL_MARKER_PREFIX)? + EVAL_MARKER_PREFIX.len();
    let rest = &body[start..];
    let end = rest.find("-->")?;
    serde_json::from_str::<EvalMetrics>(rest[..end].trim()).ok()
}

/// Parse metrics from the validation task's `eval` JSON object. Returns None
/// when eval did not apply or the payload carries no usable rates.
fn metrics_from_object(obj: &serde_json::Value) -> Option<EvalMetrics> {
    let mut m: EvalMetrics = serde_json::from_value(obj.clone()).ok()?;
    // Accept both the short marker keys and the long prompt field names.
    let get_f = |short: &str, long: &str| {
        obj.get(short)
            .or_else(|| obj.get(long))
            .and_then(|v| v.as_f64())
    };
    let get_u = |short: &str, long: &str| {
        obj.get(short)
            .or_else(|| obj.get(long))
            .and_then(|v| v.as_u64())
    };
    if m.rate_before.is_none() {
        m.rate_before = get_f("rb", "rate_before");
    }
    if m.rate_after.is_none() {
        m.rate_after = get_f("ra", "rate_after");
    }
    if m.n_before.is_none() {
        m.n_before = get_u("nb", "n_before");
    }
    if m.n_after.is_none() {
        m.n_after = get_u("na", "n_after");
    }
    if m.latency_before_ms.is_none() {
        m.latency_before_ms = get_u("lb", "latency_before_ms");
    }
    if m.latency_after_ms.is_none() {
        m.latency_after_ms = get_u("la", "latency_after_ms");
    }
    if !m.applicable {
        return None;
    }
    // Require at least one measured rate; an empty/applicable-only payload is
    // treated as "no quantitative data".
    if m.rate_before.is_none() && m.rate_after.is_none() {
        return None;
    }
    Some(m)
}

/// Human-readable one-line summary of metrics for the verdict comment.
fn render_metrics_human(m: &EvalMetrics) -> String {
    let mut parts = Vec::new();
    if let (Some(b), Some(a)) = (m.rate_before, m.rate_after) {
        let pp = (a - b) * 100.0;
        let sig = if m.significant {
            ", significant"
        } else {
            ", not significant"
        };
        let n = match (m.n_before, m.n_after) {
            (Some(nb), Some(na)) => format!(", n={nb}/{na}"),
            _ => String::new(),
        };
        let z = m.z.map(|z| format!(", z={z:.2}")).unwrap_or_default();
        parts.push(format!(
            "success rate {:.0}% → {:.0}% ({:+.1}pp{z}{n}{sig})",
            b * 100.0,
            a * 100.0,
            pp
        ));
    }
    if let (Some(lb), Some(la)) = (m.latency_before_ms, m.latency_after_ms) {
        parts.push(format!("latency {lb}ms → {la}ms"));
    }
    if parts.is_empty() {
        "eval ran but reported no quantitative metrics".to_string()
    } else {
        parts.join("; ")
    }
}

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
#[derive(Debug, Clone, PartialEq)]
pub struct CrossValidationVerdict {
    /// `pass`, `fail` or `inconclusive`.
    pub verdict: String,
    /// One-paragraph human-readable conclusion.
    pub summary: String,
    /// `cargo test` result summary.
    pub tests: String,
    /// Eval A/B summary, or "not applicable".
    pub eval_note: String,
    /// Structured A/B eval metrics when the task ran a quantitative eval;
    /// None for tests-only verdicts or free-text eval reports.
    pub metrics: Option<EvalMetrics>,
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
            .is_some_and(|e| e.head_sha == head_sha)
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
    // Prefer structured metrics for the human line when present; fall back to
    // the free-text eval note, then "not reported".
    let eval = match &v.metrics {
        Some(m) => render_metrics_human(m),
        None if !v.eval_note.trim().is_empty() => v.eval_note.trim().to_string(),
        _ => "not reported".to_string(),
    };
    // Structured metrics ride along as a machine marker for the board action.
    let eval_marker_line = match &v.metrics {
        Some(m) => format!("\n\n{}", eval_marker(m)),
        None => String::new(),
    };
    format!(
        "## Cogneva cross-validation\n\n\
         Instance `{handle}` validated this PR in its own sandbox (head `{sha8}`).\n\n\
         **Verdict: {}**\n\n\
         {summary}\n\n\
         - Tests: {tests}\n\
         - Eval: {eval}\n\n\
         {marker}{eval_marker_line}",
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
    // `eval` is either a structured object (preferred: feeds the consensus
    // ranking) or a free-text note (older/non-quantitative validations).
    let mut metrics = None;
    let mut eval_note = String::new();
    match obj.get("eval") {
        Some(serde_json::Value::Object(_)) => {
            if let Some(m) = obj.get("eval").and_then(metrics_from_object) {
                eval_note = render_metrics_human(&m);
                metrics = Some(m);
            } else {
                // eval object present but not applicable / no rates.
                eval_note = obj["eval"]
                    .get("note")
                    .and_then(|v| v.as_str())
                    .unwrap_or("not applicable")
                    .to_string();
            }
        }
        Some(serde_json::Value::String(s)) => eval_note = s.trim().to_string(),
        _ => {
            let note = field("eval_note");
            if !note.is_empty() {
                eval_note = note;
            }
        }
    }
    Some(CrossValidationVerdict {
        verdict: verdict.to_string(),
        summary: field("summary"),
        tests: field("tests"),
        eval_note,
        metrics,
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
                metrics: None,
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
    fn structured_eval_metrics_round_trip_through_marker() {
        let result = serde_json::json!({
            "verdict": "pass",
            "tests": "all green",
            "summary": "works",
            "eval": {
                "applicable": true,
                "rate_before": 0.70,
                "rate_after": 0.88,
                "n_before": 50,
                "n_after": 50,
                "z": 2.31,
                "significant": true,
                "latency_before_ms": 1200,
                "latency_after_ms": 980
            }
        });
        let v = extract_verdict(&result).expect("verdict parsed");
        let m = v.metrics.as_ref().expect("structured metrics present");
        assert_eq!(m.rate_after, Some(0.88));
        assert!(m.significant);
        assert!((m.improvement_pp().unwrap() - 18.0).abs() < 0.01);

        // The verdict comment carries a machine marker that parses back.
        let body = render_verdict_comment(&v, 12, "deadbeefcafef00d", "Carol#c1");
        assert!(body.contains("success rate 70% → 88%"));
        let parsed = parse_eval_marker(&body).expect("eval marker in comment");
        assert_eq!(parsed.rate_before, Some(0.70));
        assert_eq!(parsed.n_after, Some(50));
        assert!(parsed.significant);
    }

    #[test]
    fn long_field_names_are_accepted_for_eval_metrics() {
        let result = serde_json::json!({
            "verdict": "pass",
            "eval": {
                "applicable": true,
                "rate_before": 0.5,
                "rate_after": 0.75,
                "n_before": 20,
                "n_after": 20
            }
        });
        let v = extract_verdict(&result).unwrap();
        let m = v.metrics.unwrap();
        assert_eq!(m.rate_after, Some(0.75));
        assert_eq!(m.n_before, Some(20));
    }

    #[test]
    fn free_text_eval_falls_back_without_metrics() {
        let result = serde_json::json!({
            "verdict": "inconclusive",
            "eval": "eval harness not available in this sandbox"
        });
        let v = extract_verdict(&result).unwrap();
        assert!(v.metrics.is_none());
        assert!(v.eval_note.contains("harness not available"));
        // No marker is emitted without structured metrics.
        let body = render_verdict_comment(&v, 3, "abcd1234", "Dan#d9");
        assert!(!body.contains(EVAL_MARKER_PREFIX));
    }

    #[test]
    fn non_applicable_eval_yields_no_metrics() {
        let result = serde_json::json!({
            "verdict": "pass",
            "eval": {"applicable": false}
        });
        let v = extract_verdict(&result).unwrap();
        assert!(v.metrics.is_none());
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
