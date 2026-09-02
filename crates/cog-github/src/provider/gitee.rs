//! Gitee implementation of `CodePlatformProvider` (Gitee API v5).
//!
//! Gitee 与 GitHub 地位平等：issue 即外部意图进化入口。凭证约定与
//! GitHub 一致——`api_base` 指向安全网关 `/gitee` 透传端点时本进程零
//! token（网关以 access_token query 参数出口注入）；直连 gitee.com 时
//! 才用 `token_env` 解析的 token。
//!
//! Gitee 没有与 GitHub Actions 对应的开放 API，CI 失败相关方法保持
//! trait 默认（返回空列表，发现循环自动降级）。

use async_trait::async_trait;

use crate::error::{CogGitHubError, Result};
use crate::provider::{
    CiFailureEvent, CiJobLog, CodePlatformProvider, CreatePullRequest, PlatformComment,
    PlatformIssue, PlatformPullRequest, PullRequestDetail,
};

/// Gitee code platform provider (API v5).
#[derive(Clone, Debug)]
pub struct GiteeProvider {
    http: reqwest::Client,
    owner: String,
    repo: String,
    base: String,
    /// 直连模式的 token（网关模式为 None）。
    token: Option<String>,
}

impl GiteeProvider {
    /// Build a Gitee provider for `owner/repo`.
    ///
    /// `api_base` 为空时直连 `https://gitee.com/api/v5`；`token` 仅在直连
    /// 模式需要（以 access_token query 参数附带），网关模式传 None。
    pub fn new(repo: &str, api_base: Option<&str>, token: Option<String>) -> Result<Self> {
        let (owner, repo_name) = super::github::split_repo(repo)?;
        Ok(Self {
            http: reqwest::Client::new(),
            owner,
            repo: repo_name,
            base: api_base
                .unwrap_or("https://gitee.com/api/v5")
                .trim_end_matches('/')
                .to_string(),
            token,
        })
    }

    /// 拼装 API URL；直连模式附带 access_token。
    fn url(&self, path: &str) -> String {
        let raw = format!("{}/repos/{}/{}{}", self.base, self.owner, self.repo, path);
        match &self.token {
            Some(t) => {
                let mut url = reqwest::Url::parse(&raw).expect("provider-built url is valid");
                url.query_pairs_mut().append_pair("access_token", t);
                url.into()
            }
            None => raw,
        }
    }

    /// 发送请求并把非 2xx 统一映射为 Provider 错误（含状态码，便于诊断
    /// 网关未配置 token 的 503 与 Gitee 自身的 401/403）。
    async fn send(&self, req: reqwest::RequestBuilder) -> Result<serde_json::Value> {
        let resp = req.send().await.map_err(CogGitHubError::Http)?;
        let status = resp.status();
        let text = resp.text().await.map_err(CogGitHubError::Http)?;
        if !status.is_success() {
            return Err(CogGitHubError::Provider(format!(
                "gitee api returned HTTP {status}: {}",
                &text[..text.len().min(200)]
            )));
        }
        Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::Null))
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.http.get(self.url(path))
    }

    /// trait 的 issue_number 是 u64，承载 Gitee issue 的数值 `id`；而
    /// Gitee 的 issue 子资源路径用的是字符串编号（形如 I1A2B3）。本函数
    /// 做一次列表扫描完成 id → 字符串编号翻译（发现循环低频调用，可接受）。
    async fn issue_string_number(&self, issue_id: u64) -> Result<String> {
        let v = self
            .send(
                self.get("/issues")
                    .query(&[("state", "all"), ("per_page", "100")]),
            )
            .await?;
        v.as_array()
            .and_then(|a| {
                a.iter()
                    .find(|i| i["id"].as_u64() == Some(issue_id))
                    .and_then(|i| i["number"].as_str().map(String::from))
            })
            .ok_or_else(|| CogGitHubError::Provider(format!("gitee issue id {issue_id} not found")))
    }
}

/// 从 Gitee issue JSON 提取平台无关表示。Gitee 的 `number` 是字符串
/// 编号（I1A2B3），trait 的 u64 承载数值 `id`；字符串编号在需要时经
/// `issue_string_number` 翻译。
fn parse_issue(v: &serde_json::Value) -> PlatformIssue {
    let labels = v["labels"]
        .as_array()
        .map(|ls| {
            ls.iter()
                .filter_map(|l| l["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    PlatformIssue {
        number: v["id"].as_u64().unwrap_or_default(),
        title: v["title"].as_str().unwrap_or_default().to_string(),
        body: v["body"].as_str().unwrap_or_default().to_string(),
        state: v["state"].as_str().unwrap_or_default().to_string(),
        labels,
        author: v["user"]["login"].as_str().unwrap_or_default().to_string(),
        created_at: parse_time(&v["created_at"]),
        updated_at: parse_time(&v["updated_at"]),
    }
}

/// Gitee 时间戳为 ISO8601（+08:00 偏移）；解析失败退化为当前时间，
/// 单个脏字段不应拖垮整个扫描轮次。
fn parse_time(v: &serde_json::Value) -> chrono::DateTime<chrono::Utc> {
    v.as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now)
}

#[async_trait]
impl CodePlatformProvider for GiteeProvider {
    async fn list_open_issues(&self) -> Result<Vec<PlatformIssue>> {
        let v = self
            .send(self.get("/issues").query(&[
                ("state", "open"),
                ("per_page", "100"),
                ("sort", "created"),
                ("direction", "desc"),
            ]))
            .await?;
        Ok(v.as_array()
            .map(|a| a.iter().map(parse_issue).collect())
            .unwrap_or_default())
    }

    async fn create_pull_request(&self, req: CreatePullRequest) -> Result<PlatformPullRequest> {
        // Gitee v5 创建 PR 接受 form 参数。
        let v = self
            .send(self.http.post(self.url("/pulls")).form(&[
                ("title", req.title.as_str()),
                ("head", req.head_branch.as_str()),
                ("base", req.base_branch.as_str()),
                ("body", req.body.as_str()),
            ]))
            .await?;
        Ok(parse_pr(&v, &req))
    }

    async fn list_open_pull_requests(&self) -> Result<Vec<PlatformPullRequest>> {
        let v = self
            .send(self.get("/pulls").query(&[
                ("state", "open"),
                ("per_page", "100"),
                ("sort", "created"),
                ("direction", "desc"),
            ]))
            .await?;
        Ok(v.as_array()
            .map(|a| a.iter().map(parse_pr_list_item).collect())
            .unwrap_or_default())
    }

    async fn comment_on_issue(&self, issue_number: u64, body: String) -> Result<()> {
        let number = self.issue_string_number(issue_number).await?;
        self.send(
            self.http
                .post(self.url(&format!("/issues/{number}/comments")))
                .form(&[("body", body.as_str())]),
        )
        .await?;
        Ok(())
    }

    async fn merge_pull_request(&self, pr_number: u64, _sha: String) -> Result<()> {
        self.send(
            self.http
                .put(self.url(&format!("/pulls/{pr_number}/merge")))
                .form(&[("merge_method", "merge")]),
        )
        .await?;
        Ok(())
    }

    async fn list_issue_comments(&self, issue_number: u64) -> Result<Vec<PlatformComment>> {
        let number = self.issue_string_number(issue_number).await?;
        let v = self
            .send(
                self.get(&format!("/issues/{number}/comments"))
                    .query(&[("per_page", "100")]),
            )
            .await?;
        Ok(v.as_array()
            .map(|a| {
                a.iter()
                    .map(|c| PlatformComment {
                        author: c["user"]["login"].as_str().unwrap_or_default().to_string(),
                        body: c["body"].as_str().unwrap_or_default().to_string(),
                        created_at: parse_time(&c["created_at"]),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn get_pull_request(&self, pr_number: u64) -> Result<PullRequestDetail> {
        let pr = self.send(self.get(&format!("/pulls/{pr_number}"))).await?;
        let files = self
            .send(self.get(&format!("/pulls/{pr_number}/files")))
            .await
            .ok();
        let (affected_files, changed_lines) = files
            .as_ref()
            .and_then(|f| f.as_array())
            .map(|a| {
                let names = a
                    .iter()
                    .filter_map(|f| f["filename"].as_str().map(String::from))
                    .collect::<Vec<_>>();
                let lines = a
                    .iter()
                    .map(|f| {
                        f["additions"].as_u64().unwrap_or(0) + f["deletions"].as_u64().unwrap_or(0)
                    })
                    .sum::<u64>() as usize;
                (names, lines)
            })
            .unwrap_or_default();

        let merged = pr["merged"].as_bool().unwrap_or(false);
        let state = if merged {
            "merged".to_string()
        } else {
            pr["state"].as_str().unwrap_or_default().to_string()
        };
        Ok(PullRequestDetail {
            number: pr["number"].as_u64().unwrap_or(pr_number),
            title: pr["title"].as_str().unwrap_or_default().to_string(),
            url: pr["html_url"].as_str().unwrap_or_default().to_string(),
            state,
            labels: pr["labels"]
                .as_array()
                .map(|ls| {
                    ls.iter()
                        .filter_map(|l| l["name"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            changed_lines,
            affected_files,
            // Gitee 无 commit 组合状态开放 API，CI 判定交给其他信号。
            ci_passed: None,
            review_requested: pr["assignees"]
                .as_array()
                .map(|a| !a.is_empty())
                .unwrap_or(false),
            head_sha: pr["head"]["sha"].as_str().unwrap_or_default().to_string(),
            created_at: parse_time(&pr["created_at"]),
        })
    }

    // Gitee 无 GitHub Actions 对应物：CI 失败发现与日志抓取保持 trait
    // 默认实现（返回空），发现循环自动跳过该信号。
    async fn fetch_ci_failure_logs(&self, _run_id: u64) -> Result<Vec<CiJobLog>> {
        Ok(Vec::new())
    }

    async fn list_recent_ci_failures(&self, _max: usize) -> Result<Vec<CiFailureEvent>> {
        Ok(Vec::new())
    }
}

/// 从创建 PR 响应构造平台无关表示。
fn parse_pr(v: &serde_json::Value, req: &CreatePullRequest) -> PlatformPullRequest {
    PlatformPullRequest {
        number: v["number"].as_u64().unwrap_or_default(),
        title: v["title"].as_str().unwrap_or(&req.title).to_string(),
        url: v["html_url"].as_str().unwrap_or_default().to_string(),
        state: v["state"].as_str().unwrap_or("open").to_string(),
        head_branch: req.head_branch.clone(),
        base_branch: req.base_branch.clone(),
        body: v["body"].as_str().unwrap_or_default().to_string(),
        author: v["user"]["login"].as_str().unwrap_or_default().to_string(),
        labels: parse_labels(v),
    }
}

/// Gitee v5 PR 列表条目（head/base 分支名在响应里而非请求里）。
fn parse_pr_list_item(v: &serde_json::Value) -> PlatformPullRequest {
    PlatformPullRequest {
        number: v["number"].as_u64().unwrap_or_default(),
        title: v["title"].as_str().unwrap_or_default().to_string(),
        url: v["html_url"].as_str().unwrap_or_default().to_string(),
        state: v["state"].as_str().unwrap_or("open").to_string(),
        head_branch: v["head"]["ref"].as_str().unwrap_or_default().to_string(),
        base_branch: v["base"]["ref"].as_str().unwrap_or_default().to_string(),
        body: v["body"].as_str().unwrap_or_default().to_string(),
        author: v["user"]["login"].as_str().unwrap_or_default().to_string(),
        labels: parse_labels(v),
    }
}

fn parse_labels(v: &serde_json::Value) -> Vec<String> {
    v["labels"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|l| l["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_carries_token_only_in_direct_mode() {
        let direct = GiteeProvider::new("o/r", None, Some("tok".into())).unwrap();
        assert_eq!(
            direct.url("/issues"),
            "https://gitee.com/api/v5/repos/o/r/issues?access_token=tok"
        );
        let gateway = GiteeProvider::new("o/r", Some("http://gw:8081/gitee/"), None).unwrap();
        assert_eq!(
            gateway.url("/issues"),
            "http://gw:8081/gitee/repos/o/r/issues"
        );
    }

    #[test]
    fn issue_maps_numeric_id() {
        // trait 的 u64 number 承载 Gitee 数值 id；字符串编号（I1A2B3）
        // 仅用于子资源路径翻译。
        let v = serde_json::json!({
            "id": 123456, "number": "I1A2B3", "title": "t", "body": "b",
            "state": "open", "labels": [{"name": "bug"}],
            "user": {"login": "alice"},
            "created_at": "2026-08-01T10:00:00+08:00",
            "updated_at": "2026-08-02T10:00:00+08:00"
        });
        let issue = parse_issue(&v);
        assert_eq!(issue.number, 123456);
        assert_eq!(issue.labels, vec!["bug"]);
        assert_eq!(issue.author, "alice");
        assert_eq!(issue.created_at.timestamp(), 1785549600);
    }

    #[test]
    fn bad_timestamp_falls_back_to_now() {
        let before = chrono::Utc::now();
        let t = parse_time(&serde_json::json!("not-a-time"));
        assert!(t >= before);
    }
}
