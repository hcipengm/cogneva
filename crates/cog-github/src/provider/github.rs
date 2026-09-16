//! GitHub-specific implementation of `CodePlatformProvider`.
//!
//! Uses `octocrab` for GitHub API calls.  The GitHub token is resolved from the
//! configured account and is kept out of the evolution sandbox.

use async_trait::async_trait;
use octocrab::Octocrab;

use crate::config::GitHubAccount;

use crate::error::{CogGitHubError, Result};
use crate::provider::{
    gateway_attach_root, http_fetch_attachment, AttachmentData, CiFailureEvent, CiJobLog,
    CiRunSummary, CodePlatformProvider, CreatePullRequest, PlatformComment, PlatformIssue,
    PlatformPullRequest, PullRequestDetail,
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
    /// Base branch changes land on and whose CI is watched (e.g. `main`).
    base_branch: String,
    /// API 基址覆盖（安全网关透传端点）。None = 直连 api.github.com。
    api_base: Option<String>,
}

impl GitHubProvider {
    /// Build a new GitHub provider from an account and a `owner/repo` string.
    ///
    /// When `api_base` points at the security gateway passthrough endpoint the
    /// token is not resolved at all — the gateway injects the real credential
    /// on egress and this process stays token-free.  Otherwise the token is
    /// resolved from the environment or the inline config field at this moment
    /// and is held in memory only; it is not written to disk or passed to the
    /// sandbox.
    pub fn new(
        account: &GitHubAccount,
        repo: &str,
        base_branch: &str,
        api_base: Option<&str>,
    ) -> Result<Self> {
        let client = match api_base {
            Some(base) => Octocrab::builder()
                .base_uri(base)
                .map_err(|e| {
                    CogGitHubError::InvalidConfig(format!("invalid github api_base: {e}"))
                })?
                .build()
                .map_err(|e| CogGitHubError::Provider(e.to_string()))?,
            None => {
                let token = account
                    .resolve_token()
                    .map_err(|e| CogGitHubError::MissingToken(e.to_string()))?;
                Octocrab::builder()
                    .personal_token(token)
                    .build()
                    .map_err(|e| CogGitHubError::Provider(e.to_string()))?
            }
        };
        let (owner, repo_name) = split_repo(repo)?;
        Ok(Self {
            client,
            owner,
            repo: repo_name,
            account: account.clone(),
            base_branch: base_branch.to_string(),
            api_base: api_base.map(|s| s.trim_end_matches('/').to_string()),
        })
    }

    /// The account this provider is acting as.
    pub fn account(&self) -> &GitHubAccount {
        &self.account
    }

    /// Base branch this provider lands changes on and watches CI for.
    pub fn base_branch(&self) -> &str {
        &self.base_branch
    }

    /// CI verdict for a PR head, from check runs first and legacy commit
    /// statuses second.
    ///
    /// The base repo is queried for the head **SHA** (Actions reports
    /// `pull_request` workflow runs against the head SHA in the base repo);
    /// a PR whose head lives in a fork also has its fork queried, because
    /// reading only the base repo would show an empty set for exactly the
    /// cross-fork PRs the auto-fork flow produces. Querying the head *branch
    /// name* — what this used to do — finds nothing for fork heads and, worse,
    /// yields a `Pending` combined status with zero statuses for
    /// Actions-only repos, which reads as a hard failure.
    ///
    /// `None` means "no evidence", never "probably fine": an empty signal set
    /// and a still-running run both return `None` so the merge gate waits
    /// instead of guessing. Only a completed non-passing conclusion returns
    /// `Some(false)`, which is what makes the gate terminal.
    async fn ci_verdict(&self, pr: &octocrab::models::pulls::PullRequest) -> Option<bool> {
        let sha = pr.head.sha.clone();
        let mut targets = vec![(self.owner.clone(), self.repo.clone())];
        if let Some(repo) = pr.head.repo.as_ref() {
            if let Some(owner) = repo.owner.as_ref() {
                if owner.login != self.owner || repo.name != self.repo {
                    targets.push((owner.login.clone(), repo.name.clone()));
                }
            }
        }

        let mut conclusions: Vec<String> = Vec::new();
        let mut pending = false;
        let mut saw_signal = false;
        for (owner, repo) in &targets {
            let (saw, is_pending, mut found) = self.check_run_signals(owner, repo, &sha).await;
            saw_signal |= saw;
            pending |= is_pending;
            conclusions.append(&mut found);
        }
        if let Some(verdict) = fold_ci_signals(saw_signal, pending, &conclusions) {
            return Some(verdict);
        }

        for (owner, repo) in &targets {
            if let Some(verdict) = self.combined_status_verdict(owner, repo, &sha).await {
                return Some(verdict);
            }
        }
        None
    }

    /// Conclusions of every check run reported for `sha` on one repository:
    /// whether any signal exists at all, whether some run is still running,
    /// and the completed conclusions. An API error yields no signal — never
    /// evidence either way.
    async fn check_run_signals(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> (bool, bool, Vec<String>) {
        let mut saw_signal = false;
        let mut pending = false;
        let mut conclusions = Vec::new();
        let runs = self
            .client
            .checks(owner, repo)
            .list_check_runs_for_git_ref(octocrab::params::repos::Commitish(sha.to_string()))
            .send()
            .await;
        if let Ok(list) = runs {
            for run in list.check_runs {
                saw_signal = true;
                match run.conclusion {
                    Some(conclusion) => conclusions.push(conclusion),
                    None => pending = true,
                }
            }
        }
        (saw_signal, pending, conclusions)
    }

    /// Commit-status fallback for repositories that report CI through statuses
    /// rather than check runs. `None` when nothing was reported.
    async fn combined_status_verdict(&self, owner: &str, repo: &str, sha: &str) -> Option<bool> {
        let route = format!("/repos/{owner}/{repo}/commits/{sha}/status");
        let status = self
            .client
            .get::<octocrab::models::CombinedStatus, _, _>(route, None::<&()>)
            .await
            .ok()?;
        if status.total_count == 0 {
            return None;
        }
        match status.state {
            octocrab::models::StatusState::Success => Some(true),
            octocrab::models::StatusState::Failure | octocrab::models::StatusState::Error => {
                Some(false)
            }
            _ => None,
        }
    }
}

#[async_trait]
impl CodePlatformProvider for GitHubProvider {
    fn platform_kind(&self) -> &'static str {
        "github"
    }

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
            // GitHub's issues API mixes pull requests into the list; PRs are
            // served by the dedicated PR path, processing them here too would
            // judge every PR twice.
            .filter(|issue| issue.pull_request.is_none())
            .map(|issue| PlatformIssue {
                number: issue.number,
                title: issue.title,
                body: issue.body.unwrap_or_default(),
                state: format!("{:?}", issue.state),
                labels: issue.labels.into_iter().map(|l| l.name).collect(),
                author: issue.user.login,
                url: issue.html_url.to_string(),
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
            title: pr.title.clone().unwrap_or_default(),
            url: pr
                .html_url
                .as_ref()
                .map(|u| u.to_string())
                .unwrap_or_default(),
            state: pr.state.map(|s| format!("{:?}", s)).unwrap_or_default(),
            head_branch,
            base_branch,
            body: pr.body.unwrap_or_default(),
            author: pr.user.map(|u| u.login).unwrap_or_default(),
            labels: pr
                .labels
                .unwrap_or_default()
                .into_iter()
                .map(|l| l.name)
                .collect(),
        })
    }

    async fn add_labels(&self, issue_number: u64, labels: &[String]) -> Result<()> {
        self.client
            .issues(&self.owner, &self.repo)
            .add_labels(issue_number, labels)
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;
        Ok(())
    }

    async fn list_open_pull_requests(&self) -> Result<Vec<PlatformPullRequest>> {
        let page = self
            .client
            .pulls(&self.owner, &self.repo)
            .list()
            .state(octocrab::params::State::Open)
            .per_page(100)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        Ok(page
            .items
            .into_iter()
            .map(|pr| PlatformPullRequest {
                number: pr.number,
                title: pr.title.clone().unwrap_or_default(),
                url: pr
                    .html_url
                    .as_ref()
                    .map(|u| u.to_string())
                    .unwrap_or_default(),
                state: pr.state.map(|s| format!("{:?}", s)).unwrap_or_default(),
                head_branch: pr.head.ref_field,
                base_branch: pr.base.ref_field,
                body: pr.body.unwrap_or_default(),
                author: pr.user.map(|u| u.login).unwrap_or_default(),
                labels: pr
                    .labels
                    .unwrap_or_default()
                    .into_iter()
                    .map(|l| l.name)
                    .collect(),
            })
            .collect())
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

    async fn fetch_attachment(&self, url: &str) -> Result<AttachmentData> {
        // Gateway mode (api_base like http://gw/github): fetch through the
        // zero-credential /attach proxy; direct mode: fetch the public URL.
        let root = self.api_base.as_deref().and_then(gateway_attach_root);
        let http = reqwest::Client::new();
        http_fetch_attachment(&http, root.as_deref(), url).await
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

        // 网关模式本进程零 token：凭证由透传端点出口注入；直连模式才解析。
        let token = self.account.resolve_token().ok();
        if token.is_none() && self.api_base.is_none() {
            tracing::warn!("no github token and no gateway api_base; CI job logs unavailable");
            return Ok(Vec::new());
        }
        let base = self.api_base.as_deref().unwrap_or("https://api.github.com");
        let http = reqwest::Client::new();

        let mut logs = Vec::new();
        let mut total_bytes = 0usize;
        for job in failed {
            if total_bytes >= MAX_TOTAL_LOG_BYTES {
                break;
            }
            let url = format!(
                "{base}/repos/{}/{}/actions/jobs/{}/logs",
                self.owner, self.repo, job.id
            );
            // The endpoint answers 302 to a signed download URL; reqwest
            // follows it and drops the Authorization header cross-origin.
            let mut req = http.get(&url);
            if let Some(t) = &token {
                req = req.bearer_auth(t);
            }
            match req.send().await {
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

    async fn latest_branch_ci_run(&self, branch: &str) -> Result<Option<CiRunSummary>> {
        // No status filter: a newer commit whose CI is still running must also
        // count as "the world moved on", so the newest run of any state is the
        // freshness reference.
        let runs = self
            .client
            .workflows(&self.owner, &self.repo)
            .list_all_runs()
            .branch(branch)
            .per_page(1)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        Ok(runs.items.into_iter().next().map(|r| CiRunSummary {
            run_id: r.id.into_inner(),
            head_sha: r.head_sha,
            conclusion: r.conclusion.unwrap_or_default(),
        }))
    }

    async fn ci_verdict_for_sha(&self, sha: &str) -> Result<Option<bool>> {
        let (saw_signal, pending, conclusions) =
            self.check_run_signals(&self.owner, &self.repo, sha).await;
        if let Some(verdict) = fold_ci_signals(saw_signal, pending, &conclusions) {
            return Ok(Some(verdict));
        }
        Ok(self
            .combined_status_verdict(&self.owner, &self.repo, sha)
            .await)
    }

    async fn ci_failure_log_for_sha(&self, sha: &str) -> Result<String> {
        // The workflow-run list has no head-SHA filter, so narrow by branch
        // (a landed commit is on the base branch) and match client-side.
        let runs = self
            .client
            .workflows(&self.owner, &self.repo)
            .list_all_runs()
            .branch(&self.base_branch)
            .per_page(50)
            .send()
            .await
            .map_err(|e| CogGitHubError::Provider(e.to_string()))?;

        let Some(run) = runs
            .items
            .into_iter()
            .find(|r| r.head_sha == sha && r.conclusion.as_deref() == Some("failure"))
        else {
            return Ok(String::new());
        };

        let logs = self.fetch_ci_failure_logs(run.id.into_inner()).await?;
        Ok(logs
            .into_iter()
            .map(|l| format!("## {} (job {})\n{}", l.job_name, l.job_id, l.log_tail))
            .collect::<Vec<_>>()
            .join("\n\n"))
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

        let ci_passed = self.ci_verdict(&pr).await;

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

pub(crate) fn split_repo(repo: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = repo.split('/').collect();
    if parts.len() != 2 {
        return Err(CogGitHubError::InvalidConfig(format!(
            "repo must be in owner/repo format, got: {}",
            repo
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Whether one completed run's conclusion counts as passing.
///
/// `neutral` and `skipped` pass: they are what a path-filtered or
/// condition-gated job reports, and GitHub's own required-check evaluation
/// treats them as satisfied. Anything unrecognised fails closed.
fn ci_conclusion_passes(conclusion: &str) -> bool {
    matches!(conclusion, "success" | "neutral" | "skipped")
}

/// Fold collected check-run signals into a verdict; `None` means no evidence.
///
/// Split out from the network path so the three-way outcome (`Some(true)` /
/// `Some(false)` / `None`) can be tested without a live platform.
fn fold_ci_signals(saw_signal: bool, pending: bool, conclusions: &[String]) -> Option<bool> {
    if !saw_signal {
        return None;
    }
    if pending {
        return None;
    }
    Some(conclusions.iter().all(|c| ci_conclusion_passes(c)))
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

    #[tokio::test]
    async fn gateway_mode_builds_without_token() {
        // 测试进程不走 main.rs，rustls 无进程级默认 CryptoProvider，
        // 构建 HTTPS client 会 panic——测试内自行安装（幂等）。
        let _ = rustls::crypto::ring::default_provider().install_default();
        let account = GitHubAccount::Bot(crate::config::BotAccount {
            username: "bot".into(),
            token_env: Some("COG_TEST_DEFINITELY_UNSET_TOKEN".into()),
            token: None,
            ..Default::default()
        });
        // 直连模式：无 token 拒绝构建（凭证缺失响亮报错）。
        assert!(GitHubProvider::new(&account, "o/r", "main", None).is_err());
        // 网关模式：本进程零 token，凭证由透传端点出口注入。
        let p =
            GitHubProvider::new(&account, "o/r", "main", Some("http://gw:8081/github/")).unwrap();
        assert_eq!(p.api_base.as_deref(), Some("http://gw:8081/github"));
        assert_eq!(p.base_branch(), "main");
    }

    #[test]
    fn ci_signals_need_evidence_before_they_say_anything() {
        let none: Vec<String> = Vec::new();
        // No runs at all: unknown, not "passed" and not "failed" — the merge
        // gate waits on this rather than blocking or merging blind.
        assert_eq!(fold_ci_signals(false, false, &none), None);
        // Runs exist but none finished: still unknown.
        assert_eq!(fold_ci_signals(true, true, &none), None);
        // A finished run plus one still running: unknown, because the pending
        // one may yet fail.
        assert_eq!(fold_ci_signals(true, true, &["success".to_string()]), None);
    }

    #[test]
    fn ci_signals_verdict_comes_from_completed_runs() {
        let passed = vec!["success".to_string(), "skipped".to_string()];
        assert_eq!(fold_ci_signals(true, false, &passed), Some(true));
        let failed = vec!["success".to_string(), "failure".to_string()];
        assert_eq!(fold_ci_signals(true, false, &failed), Some(false));
        // Unknown conclusions fail closed.
        let unknown = vec!["startup_failure".to_string()];
        assert_eq!(fold_ci_signals(true, false, &unknown), Some(false));
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
