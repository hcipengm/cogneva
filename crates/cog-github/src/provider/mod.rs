//! Code platform provider abstraction.
//!
//! `CodePlatformProvider` defines a generic interface for interacting with code
//! hosting platforms.  `cog-github` ships GitHub and Gitee implementations;
//! Gitee 与 GitHub 地位平等，issue 即外部意图进化入口。

pub mod gitee;
pub mod github;

use async_trait::async_trait;

use crate::error::Result;

/// Generic code platform operations used by Cogneva's self-evolution loop.
#[async_trait]
pub trait CodePlatformProvider: Send + Sync {
    /// List open issues from the configured repository.
    async fn list_open_issues(&self) -> Result<Vec<PlatformIssue>>;

    /// List open pull requests from the configured repository.
    ///
    /// PR 与 issue 同为进化意图入口（PR 未必带解法，可能只是需求描述）。
    /// Default: unsupported — providers return an empty list so PR-intent
    /// discovery degrades gracefully.
    async fn list_open_pull_requests(&self) -> Result<Vec<PlatformPullRequest>> {
        Ok(Vec::new())
    }

    /// Create a pull request from a change description.
    async fn create_pull_request(&self, req: CreatePullRequest) -> Result<PlatformPullRequest>;

    /// Post a comment on an issue.
    async fn comment_on_issue(&self, issue_number: u64, body: String) -> Result<()>;

    /// Merge a pull request if allowed by policy.
    async fn merge_pull_request(&self, pr_number: u64, sha: String) -> Result<()>;

    /// List comments on an issue (oldest first).
    ///
    /// Default: unsupported — providers without comment support return an
    /// empty list so conversation handling degrades gracefully.
    async fn list_issue_comments(&self, _issue_number: u64) -> Result<Vec<PlatformComment>> {
        Ok(Vec::new())
    }

    /// List comments on a pull request (oldest first). PRs and issues are equal
    /// external-intent entry points, so the clarification conversation runs on
    /// both. On GitHub the issues-comments endpoint serves PRs too, so the
    /// default delegates to [`list_issue_comments`](Self::list_issue_comments);
    /// Gitee overrides with its `/pulls/{n}/comments` endpoint.
    async fn list_pull_comments(&self, pr_number: u64) -> Result<Vec<PlatformComment>> {
        self.list_issue_comments(pr_number).await
    }

    /// Post a comment on a pull request. Default delegates to
    /// [`comment_on_issue`](Self::comment_on_issue) (GitHub treats PRs as
    /// issues); Gitee overrides with its `/pulls/{n}/comments` endpoint.
    async fn comment_on_pull(&self, pr_number: u64, body: String) -> Result<()> {
        self.comment_on_issue(pr_number, body).await
    }

    /// Fetch a media attachment (screenshot / recording / video / PDF) as raw
    /// bytes plus its MIME type. In gateway mode the bytes are fetched through
    /// the security gateway's zero-credential `/attach` proxy (which injects
    /// platform credentials on the first hop and validates host/size/MIME); in
    /// direct mode the public media URL is fetched directly.
    ///
    /// Default: unsupported — providers without attachment access return an
    /// error so multimodal triage degrades to text-only.
    async fn fetch_attachment(&self, _url: &str) -> Result<AttachmentData> {
        Err(crate::error::CogGitHubError::Provider(
            "fetch_attachment not supported by this provider".into(),
        ))
    }

    /// Fetch log tails for failed CI jobs of a workflow run.
    ///
    /// Default: unsupported — providers without CI log access return an
    /// empty list so CI-failure handling degrades gracefully.
    async fn fetch_ci_failure_logs(&self, _run_id: u64) -> Result<Vec<CiJobLog>> {
        Ok(Vec::new())
    }

    /// Poll recently completed CI workflow runs that ended in failure.
    ///
    /// Webhook fallback for environments without public inbound access
    /// (e.g. cloud firewall blocking the NodePort).
    ///
    /// Default: unsupported — providers return an empty list.
    async fn list_recent_ci_failures(&self, _max: usize) -> Result<Vec<CiFailureEvent>> {
        Ok(Vec::new())
    }

    /// Fetch the current state of a pull request for merge decisions and
    /// outcome recording.
    async fn get_pull_request(&self, pr_number: u64) -> Result<PullRequestDetail>;

    /// Attach labels to an issue or pull request (GitHub labels PRs through
    /// the issues API; Gitee labels PRs via its pulls endpoint).
    ///
    /// Default: unsupported — providers without label access return an error
    /// so callers can decide whether labeling is mandatory for their flow.
    async fn add_labels(&self, _issue_number: u64, _labels: &[String]) -> Result<()> {
        Err(crate::error::CogGitHubError::Provider(
            "add_labels not supported by this provider".into(),
        ))
    }

    /// Fetch a single issue by number (webhook event handling).
    ///
    /// Default: scans `list_open_issues` — providers with a direct API should
    /// override for efficiency.
    async fn get_issue(&self, issue_number: u64) -> Result<PlatformIssue> {
        let issues = self.list_open_issues().await?;
        issues
            .into_iter()
            .find(|i| i.number == issue_number)
            .ok_or_else(|| {
                crate::error::CogGitHubError::Provider(format!(
                    "issue #{issue_number} not found among open issues"
                ))
            })
    }
}

/// A platform-agnostic issue representation.
#[derive(Debug, Clone, PartialEq)]
pub struct PlatformIssue {
    /// Issue number on the platform.
    pub number: u64,
    /// Issue title.
    pub title: String,
    /// Issue body in markdown/plain text.
    pub body: String,
    /// Issue state string (e.g. `"open"`, `"closed"`).
    pub state: String,
    /// Labels attached to the issue.
    pub labels: Vec<String>,
    /// Author login/username.
    pub author: String,
    /// Creation timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last update timestamp.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Request to create a pull request.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePullRequest {
    /// PR title.
    pub title: String,
    /// PR body in markdown.
    pub body: String,
    /// Head branch (the branch containing the changes).
    pub head_branch: String,
    /// Base branch (the branch to merge into).
    pub base_branch: String,
    /// Whether to create the PR as a draft.
    pub draft: bool,
}

/// A platform-agnostic pull request representation.
#[derive(Debug, Clone, PartialEq)]
pub struct PlatformPullRequest {
    /// PR number on the platform.
    pub number: u64,
    /// PR title.
    pub title: String,
    /// Platform URL for the PR.
    pub url: String,
    /// PR state string (e.g. `"open"`, `"closed"`, `"merged"`).
    pub state: String,
    /// Head branch.
    pub head_branch: String,
    /// Base branch.
    pub base_branch: String,
    /// PR body in markdown (intent description when PR is used as an
    /// evolution input; empty when not fetched).
    pub body: String,
    /// Author login/username (empty when not fetched).
    pub author: String,
    /// Labels attached to the PR.
    pub labels: Vec<String>,
}

/// A comment on an issue.
#[derive(Debug, Clone, PartialEq)]
pub struct PlatformComment {
    /// Author login/username.
    pub author: String,
    /// Comment body.
    pub body: String,
    /// Creation timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// A fetched media attachment: raw bytes and its MIME type. Used to feed
/// screenshots / recordings / video / PDFs to the multimodal actionability
/// judge as base64 content blocks.
#[derive(Debug, Clone, PartialEq)]
pub struct AttachmentData {
    /// Raw media bytes.
    pub bytes: Vec<u8>,
    /// MIME type (e.g. `image/png`, `video/mp4`).
    pub mime_type: String,
}

/// Cap on a single attachment's bytes (mirrors the gateway `/attach` limit;
/// applied in direct mode too so a huge file cannot blow up memory).
pub const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;

/// Derive the security-gateway root (where `/attach` lives) from a platform
/// passthrough `api_base` such as `http://gw:8081/github` or
/// `http://gw:8081/gitee`. Returns `None` for direct (public-platform) bases,
/// in which case attachments are fetched directly from their public URL.
pub fn gateway_attach_root(api_base: &str) -> Option<String> {
    let trimmed = api_base.trim_end_matches('/');
    for suffix in ["/github", "/gitee"] {
        if let Some(root) = trimmed.strip_suffix(suffix) {
            if !root.is_empty() {
                return Some(root.to_string());
            }
        }
    }
    None
}

/// Fetch a media attachment, through the gateway `/attach` proxy when a
/// gateway root is known (zero-credential egress, host/size/MIME validated
/// there), else directly. Returns the bytes and a MIME type (from the
/// response `Content-Type`, inferred from the URL extension when absent).
pub(crate) async fn http_fetch_attachment(
    client: &reqwest::Client,
    gateway_root: Option<&str>,
    url: &str,
) -> Result<AttachmentData> {
    // reqwest percent-encodes the `url` query parameter for us.
    let req = match gateway_root {
        Some(root) => client
            .get(format!("{}/attach", root.trim_end_matches('/')))
            .query(&[("url", url)]),
        None => client.get(url),
    };
    let resp = req
        .send()
        .await
        .map_err(crate::error::CogGitHubError::Http)?;
    let status = resp.status();
    if !status.is_success() {
        return Err(crate::error::CogGitHubError::Provider(format!(
            "attachment fetch returned HTTP {status} for {url}"
        )));
    }
    let header_mime = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_string());
    let bytes = resp
        .bytes()
        .await
        .map_err(crate::error::CogGitHubError::Http)?;
    if bytes.len() > MAX_ATTACHMENT_BYTES {
        return Err(crate::error::CogGitHubError::Provider(format!(
            "attachment too large ({} bytes) for {url}",
            bytes.len()
        )));
    }
    let mime_type = header_mime
        .filter(|m| is_media_mime(m))
        .or_else(|| ext_mime(url))
        .unwrap_or_else(|| "application/octet-stream".to_string());
    Ok(AttachmentData {
        bytes: bytes.to_vec(),
        mime_type,
    })
}

/// True when a MIME type is a media kind we accept as an attachment.
fn is_media_mime(mime: &str) -> bool {
    let m = mime.to_ascii_lowercase();
    m.starts_with("image/")
        || m.starts_with("audio/")
        || m.starts_with("video/")
        || m == "application/pdf"
}

/// Infer a media MIME type from a URL's file extension.
fn ext_mime(url: &str) -> Option<String> {
    let path = url.split('?').next().unwrap_or(url);
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    let mime = match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "pdf" => "application/pdf",
        _ => return None,
    };
    Some(mime.to_string())
}

/// A CI workflow-run failure observed on the platform.
#[derive(Debug, Clone, PartialEq)]
pub struct CiFailureEvent {
    /// Workflow run id.
    pub run_id: u64,
    /// Workflow name (e.g. `"CI"`).
    pub workflow_name: String,
    /// Head commit SHA of the run.
    pub head_sha: String,
    /// Head branch of the run.
    pub head_branch: String,
    /// Platform URL for the run.
    pub html_url: String,
}

/// Log tail of a failed CI job within a workflow run.
#[derive(Debug, Clone, PartialEq)]
pub struct CiJobLog {
    /// Job id on the platform.
    pub job_id: u64,
    /// Job display name (e.g. `"Test"`, `"Clippy"`).
    pub job_name: String,
    /// Tail of the job log (capped per job by the provider).
    pub log_tail: String,
}

/// Detailed pull request state used for merge decisions and outcome
/// recording.
#[derive(Debug, Clone, PartialEq)]
pub struct PullRequestDetail {
    /// PR number on the platform.
    pub number: u64,
    /// PR title.
    pub title: String,
    /// Platform URL for the PR.
    pub url: String,
    /// PR state string (e.g. `"open"`, `"closed"`); `"merged"` when merged.
    pub state: String,
    /// Labels attached to the PR.
    pub labels: Vec<String>,
    /// Total changed lines (additions + deletions).
    pub changed_lines: usize,
    /// Files touched by the PR.
    pub affected_files: Vec<String>,
    /// Combined CI status: `Some(true)` all green, `Some(false)` failing,
    /// `None` when no status is reported yet.
    pub ci_passed: Option<bool>,
    /// Whether a human review has been requested.
    pub review_requested: bool,
    /// Head commit SHA.
    pub head_sha: String,
    /// Creation timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
}
