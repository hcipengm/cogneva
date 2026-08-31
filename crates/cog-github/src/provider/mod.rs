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

    /// Create a pull request from a patch description.
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
