//! GitHub-specific implementation of `CodePlatformProvider`.
//!
//! Uses `octocrab` for GitHub API calls.  The GitHub token is resolved from the
//! configured account and is kept out of the evolution sandbox.

use async_trait::async_trait;
use octocrab::Octocrab;

use cog_core::GitHubAccount;

use crate::error::{CogGitHubError, Result};
use crate::provider::{
    CiFailureEvent, CiJobLog, CodePlatformProvider, CreatePullRequest, PlatformComment,
    PlatformIssue, PlatformPullRequest, PullRequestDetail,
};

/// Max failed jobs whose logs are fetched per run.
const MAX_FAILED_JOBS: usize = 5;
/// Max bytes of log kept per job (tail).
const MAX_JOB_LOG_BYTES: usize = 16 * 1024;
/// Max total bytes of logs returned per run.
const MAX_TOTAL_LOG_BYTES: usize = 80 * 1024;

/// GitHub code platform provider.
#[derive(Clone, Debug)]
pub struct GitHubProvider {
    client: Octocrab,
    owner: String,
    repo: String,
    account: GitHubAccount,
}

impl GitHubProvider {
    /// Build a new GitHub provider from an account and a `owner/repo` string.
    ///
    /// The token is resolved from the environment or the inline config field at
    /// this moment and is held in memory only.  It is not written to disk or
    /// passed to the sandbox.
    pub fn new(account: &GitHubAccount, repo: &str) -> Result<Self> {
        let token = account
            .resolve_token()
            .map_err(|e| CogGitHubError::MissingToken(e.to_string()))?;
        let client = Octocrab::builder()
            .personal_token(token)
            .build()
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
        let (owner, repo_name) = split_repo(repo)?;
        Ok(Self {
            client,
            owner,
            repo: repo_name,
            account: account.clone(),
        })
    }

    /// The account this provider is acting as.
    pub fn account(&self) -> &GitHubAccount {
        &self.account
    }
}

#[async_trait]
impl CodePlatformProvider for GitHubProvider {
    async fn list_open_issues(&self) -> Result<Vec<PlatformIssue>> {
        let page = self
            .client
            .issues(&self.owner, &self.repo)
            .list()
            .state(octocrab::params::State::Open)
            .per_page(100)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        Ok(page
            .items
            .into_iter()
            .map(|issue| PlatformIssue {
                number: issue.number,
                title: issue.title,
                body: issue.body.unwrap_or_default(),
                state: format!("{:?}", issue.state),
                labels: issue.labels.into_iter().map(|l| l.name).collect(),
                author: issue.user.login,
                created_at: issue.created_at,
                updated_at: issue.updated_at,
            })
            .collect())
    }

    async fn create_pull_request(&self, req: CreatePullRequest) -> Result<PlatformPullRequest> {
        let base_branch = req.base_branch.clone();
        let head_branch = req.head_branch.clone();
        let pr = self
            .client
            .pulls(&self.owner, &self.repo)
            .create(req.title, &head_branch, &base_branch)
            .body(req.body)
            .draft(req.draft)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        Ok(PlatformPullRequest {
            number: pr.number as u64,
            title: pr.title.unwrap_or_default(),
            url: pr.html_url.map(|u| u.to_string()).unwrap_or_default(),
            state: pr.state.map(|s| format!("{:?}", s)).unwrap_or_default(),
            head_branch,
            base_branch,
        })
    }

    async fn comment_on_issue(&self, issue_number: u64, body: String) -> Result<()> {
        self.client
            .issues(&self.owner, &self.repo)
            .create_comment(issue_number, body)
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
        Ok(())
    }

    async fn merge_pull_request(&self, pr_number: u64, _sha: String) -> Result<()> {
        self.client
            .pulls(&self.owner, &self.repo)
            .merge(pr_number)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
        Ok(())
    }

    async fn list_issue_comments(&self, issue_number: u64) -> Result<Vec<PlatformComment>> {
        let page = self
            .client
            .issues(&self.owner, &self.repo)
            .list_comments(issue_number)
            .per_page(100)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        Ok(page
            .items
            .into_iter()
            .map(|c| PlatformComment {
                author: c.user.login,
                body: c.body.unwrap_or_default(),
                created_at: c.created_at,
            })
            .collect())
    }

    async fn fetch_ci_failure_logs(&self, run_id: u64) -> Result<Vec<CiJobLog>> {
        let jobs = self
            .client
            .workflows(&self.owner, &self.repo)
            .list_jobs(run_id.into())
            .per_page(100)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        let failed: Vec<_> = jobs
            .items
            .into_iter()
            .filter(|j| {
                matches!(
                    j.conclusion,
                    Some(octocrab::models::workflows::Conclusion::Failure)
                )
            })
            .take(MAX_FAILED_JOBS)
            .collect();

        if failed.is_empty() {
            return Ok(Vec::new());
        }

        let token = self
            .account
            .resolve_token()
            .map_err(|e| CogGitHubError::MissingToken(e.to_string()))?;
        let http = reqwest::Client::new();

        let mut logs = Vec::new();
        let mut total_bytes = 0usize;
        for job in failed {
            if total_bytes >= MAX_TOTAL_LOG_BYTES {
                break;
            }
            let url = format!(
                "https://api.github.com/repos/{}/{}/actions/jobs/{}/logs",
                self.owner, self.repo, job.id
            );
            // The endpoint answers 302 to a signed download URL; reqwest
            // follows it and drops the Authorization header cross-origin.
            match http.get(&url).bearer_auth(&token).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let text = resp.text().await.unwrap_or_default();
                    let remaining = MAX_TOTAL_LOG_BYTES - total_bytes;
                    let cap = MAX_JOB_LOG_BYTES.min(remaining);
                    let tail = log_tail(&text, cap);
                    total_bytes += tail.len();
                    logs.push(CiJobLog {
                        job_id: job.id.into_inner(),
                        job_name: job.name,
                        log_tail: tail,
                    });
                }
                Ok(resp) => {
                    tracing::warn!(
                        job = %job.name,
                        status = %resp.status(),
                        "Failed to fetch CI job log"
                    );
                }
                Err(e) => {
                    tracing::warn!(job = %job.name, error = %e, "Failed to fetch CI job log");
                }
            }
        }
        Ok(logs)
    }

    async fn list_recent_ci_failures(&self, max: usize) -> Result<Vec<CiFailureEvent>> {
        let runs = self
            .client
            .workflows(&self.owner, &self.repo)
            .list_all_runs()
            .status("completed")
            .per_page(max.min(100) as u8)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        Ok(runs
            .items
            .into_iter()
            .filter(|r| r.conclusion.as_deref() == Some("failure"))
            .map(|r| CiFailureEvent {
                run_id: r.id.into_inner(),
                workflow_name: r.name,
                head_sha: r.head_sha,
                head_branch: r.head_branch,
                html_url: r.html_url.to_string(),
            })
            .collect())
    }

    async fn get_pull_request(&self, pr_number: u64) -> Result<PullRequestDetail> {
        let pr = self
            .client
            .pulls(&self.owner, &self.repo)
            .get(pr_number)
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        let files = self
            .client
            .pulls(&self.owner, &self.repo)
            .list_files(pr_number)
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        let ci_passed = match self
            .client
            .repos(&self.owner, &self.repo)
            .combined_status_for_ref(&octocrab::params::repos::Reference::Branch(
                pr.head.ref_field.clone(),
            ))
            .await
        {
            Ok(status) => Some(matches!(
                status.state,
                octocrab::models::StatusState::Success
            )),
            Err(_) => None,
        };

        let state = if pr.merged_at.is_some() {
            "merged".to_string()
        } else {
            pr.state
                .map(|s| format!("{:?}", s).to_lowercase())
                .unwrap_or_default()
        };

        Ok(PullRequestDetail {
            number: pr.number as u64,
            title: pr.title.unwrap_or_default(),
            url: pr.html_url.map(|u| u.to_string()).unwrap_or_default(),
            state,
            labels: pr
                .labels
                .unwrap_or_default()
                .into_iter()
                .map(|l| l.name)
                .collect(),
            changed_lines: (pr.additions.unwrap_or(0) + pr.deletions.unwrap_or(0)) as usize,
            affected_files: files.items.into_iter().map(|f| f.filename).collect(),
            ci_passed,
            review_requested: pr
                .requested_reviewers
                .map(|r| !r.is_empty())
                .unwrap_or(false),
            head_sha: pr.head.sha,
            created_at: pr.created_at.unwrap_or_else(chrono::Utc::now),
        })
    }
}

fn split_repo(repo: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = repo.split('/').collect();
    if parts.len() != 2 {
        return Err(CogGitHubError::InvalidConfig(format!(
            "repo must be in owner/repo format, got: {}",
            repo
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Keep the last `cap` bytes of `text`, starting on a char boundary.
fn log_tail(text: &str, cap: usize) -> String {
    if text.len() <= cap {
        return text.to_string();
    }
    let mut start = text.len() - cap;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_repo() {
        assert_eq!(
            split_repo("cogneva/cogneva").unwrap(),
            ("cogneva".into(), "cogneva".into())
        );
        assert!(split_repo("bad-format").is_err());
    }

    #[test]
    fn test_log_tail() {
        assert_eq!(log_tail("short", 1024), "short");
        let long = "x".repeat(1000);
        let tail = log_tail(&long, 100);
        assert_eq!(tail.len(), 100);
        // Multi-byte chars are never split mid-sequence.
        let utf8 = format!("{}中文日志", "y".repeat(100));
        let tail = log_tail(&utf8, 9);
        assert!(tail.len() <= 9);
        assert!(std::str::from_utf8(tail.as_bytes()).is_ok());
    }
}
