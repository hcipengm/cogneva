//! Gitee implementation of `CodePlatformProvider` (Gitee API v5).
//!
//! Gitee 与 GitHub 地位平等：issue 即外部意图进化入口。凭证约定与
//! GitHub 一致——`api_base` 指向安全网关 `/gitee` 透传端点时本进程零
//! token（网关以 access_token query 参数出口注入）；直连 gitee.com 时
//! 才用 `token_env` 解析的 token。
//!
//! Gitee 没有与 GitHub Actions 对应的开放 API，CI 失败相关方法保持
//! trait 默认（返回空列表，发现循环自动降级）。

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{CogGitHubError, Result};
use crate::provider::{
    form_body, gateway_attach_root, http_fetch_attachment, with_query, AttachmentData,
    CiFailureEvent, CiJobLog, CodePlatformProvider, CreatePullRequest, PlatformComment,
    PlatformIssue, PlatformPullRequest, PullRequestDetail,
};

/// Gitee code platform provider (API v5).
#[derive(Clone, Debug)]
pub struct GiteeProvider {
    /// 由装配层注入的出站客户端：超时/连接池/代理只有一个来源，本 crate
    /// 经 `cog_core::HttpClient` 契约取用，不认具体实现。
    http: Arc<dyn cog_core::HttpClient>,
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
    pub fn new(
        repo: &str,
        api_base: Option<&str>,
        token: Option<String>,
        http: Arc<dyn cog_core::HttpClient>,
    ) -> Result<Self> {
        let (owner, repo_name) = super::github::split_repo(repo)?;
        Ok(Self {
            http,
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
            Some(t) => with_query(&raw, &[("access_token", t)]),
            None => raw,
        }
    }

    /// 发送请求并把非 2xx 统一映射为 Provider 错误（含状态码，便于诊断
    /// 网关未配置 token 的 503 与 Gitee 自身的 401/403）。
    async fn send(&self, req: cog_core::HttpRequest) -> Result<serde_json::Value> {
        let resp = self
            .http
            .execute(req)
            .await
            .map_err(CogGitHubError::Upstream)?;
        let status = resp.status;
        let text = String::from_utf8_lossy(&resp.body).into_owned();
        if !(200..300).contains(&status) {
            let snippet: String = text.chars().take(200).collect();
            return Err(CogGitHubError::Provider(format!(
                "gitee api returned HTTP {status}: {snippet}"
            )));
        }
        // 一段空正文是「这条接口没有东西要读」（合并、评论这类写接口），读成
        // 空对象；而一段**非空却不是 JSON** 的正文（代理挡在前面答的一页 HTML）
        // 是这份读数坏了——折成 `Null`，调用方会把每个字段都读成空串或 0。
        if text.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| {
            let snippet: String = text.chars().take(200).collect();
            CogGitHubError::Provider(format!(
                "gitee api answered {status} with a body that is not JSON ({e}): {snippet}"
            ))
        })
    }

    /// 一个带 query 参数的 GET。参数在这里编码：`url()` 拼出的地址已经可能
    /// 带一个 `access_token`，直接字符串拼接会与它抢 `?`。
    fn get(&self, path: &str, params: &[(&str, &str)]) -> cog_core::HttpRequest {
        cog_core::HttpRequest::get(with_query(&self.url(path), params))
    }

    /// 一个 form 编码的请求；Gitee v5 的写接口（建 PR、评论、合并、打标签）
    /// 收 `application/x-www-form-urlencoded` 而非 JSON。
    fn form_request(
        &self,
        method: &str,
        path: &str,
        fields: &[(&str, &str)],
    ) -> cog_core::HttpRequest {
        cog_core::HttpRequest::new(method, self.url(path))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_body(fields).into_bytes())
    }

    /// 取一整份名单：一页一页地问，直到某一页装不满为止。
    ///
    /// Gitee v5 的列表端点一次最多回 `per_page` 条，超出的部分要靠 `page` 再问
    /// 一次。只问一次的读法把「第 101 条起」与「就这些」读成同一样，而调用方
    /// 拿这份名单当开放集：处理循环按它去重、裁决回收按它判谁还开着、issue 编号
    /// 翻译按它找号——少掉的那些不会以任何形式露头，读面看起来和完整的名单一样。
    ///
    /// 「问完了没有」由服务端自己给：一页不满 `per_page` 条就是最后一页；正好
    /// 装满则可能是后面还有，再问下一页。页长 `PER_PAGE` 是接口的上限，不是
    /// 我们挑的数，所以这里没有第二个要论证的界。
    ///
    /// 正文不是数组时整份读不成：一句错误对象或一页 HTML 不是一份空名单。
    async fn list_all(&self, path: &str, base: &[(&str, &str)]) -> Result<Vec<serde_json::Value>> {
        let per_page = PER_PAGE.to_string();
        let mut all: Vec<serde_json::Value> = Vec::new();
        let mut page: usize = 1;
        loop {
            let page_no = page.to_string();
            let mut params: Vec<(&str, &str)> = base.to_vec();
            params.push(("per_page", per_page.as_str()));
            params.push(("page", page_no.as_str()));
            let v = self.send(self.get(path, &params)).await?;
            let batch = v.as_array().ok_or_else(|| {
                CogGitHubError::Provider(format!("gitee {path} answered no array: {v}"))
            })?;
            let got = batch.len();
            all.extend(batch.iter().cloned());
            if got < PER_PAGE {
                break;
            }
            page += 1;
        }
        Ok(all)
    }

    /// trait 的 issue_number 是 u64，承载 Gitee issue 的数值 `id`；而
    /// Gitee 的 issue 子资源路径用的是字符串编号（形如 I1A2B3）。本函数
    /// 做一次列表扫描完成 id → 字符串编号翻译（发现循环低频调用，可接受）。
    ///
    /// 扫的是**全量**：第 101 个 issue 起不在第一页上，只扫一页的写法会把它们
    /// 说成「没有这个 issue」，评论于是永远发不出去。
    async fn issue_string_number(&self, issue_id: u64) -> Result<String> {
        let listing = self.list_all("/issues", &[("state", "all")]).await?;
        listing
            .iter()
            .find(|i| i["id"].as_u64() == Some(issue_id))
            .and_then(|i| i["number"].as_str().map(String::from))
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
        url: v["html_url"].as_str().unwrap_or_default().to_string(),
        created_at: parse_time(&v["created_at"]),
        updated_at: parse_time(&v["updated_at"]),
    }
}

/// 从 Gitee 评论 JSON 提取平台无关表示（issue 与 PR 两条端点同形）。
fn parse_comment(v: &serde_json::Value) -> PlatformComment {
    PlatformComment {
        author: v["user"]["login"].as_str().unwrap_or_default().to_string(),
        body: v["body"].as_str().unwrap_or_default().to_string(),
        created_at: parse_time(&v["created_at"]),
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

/// 一页最多取多少条。
///
/// Gitee v5 的列表端点都认这个上限。它不只是「一页要多大」，还是**问完了没有**
/// 的判据本身：一页正好这么多条，就说明后面可能还有，得再问一页。
// cap: fixed — Gitee v5 列表端点的 per_page 上限：接口一次最多回 100 条，不是我们挑的数
const PER_PAGE: usize = 100;

/// 列「还开着的那一批」用的三个查询参数。
///
/// issues 与 pulls 是两条端点，可「只要开着的、按创建时间倒序」是同一句话。
/// 分开写时，改一条只让其中一个列表换了次序，而两边都仍然返回一个像样的列表。
const OPEN_LIST_QUERY: [(&str, &str); 3] = [
    ("state", "open"),
    ("sort", "created"),
    ("direction", "desc"),
];

#[async_trait]
impl CodePlatformProvider for GiteeProvider {
    fn platform_kind(&self) -> &'static str {
        crate::GITEE_PLATFORM
    }

    /// 列表都经 `list_all` 翻到不满一页为止，交出去的名单就是全部。
    fn lists_every_page(&self) -> bool {
        true
    }

    async fn list_open_issues(&self) -> Result<Vec<PlatformIssue>> {
        let listing = self.list_all("/issues", &OPEN_LIST_QUERY).await?;
        Ok(listing.iter().map(parse_issue).collect())
    }

    async fn create_pull_request(&self, req: CreatePullRequest) -> Result<PlatformPullRequest> {
        // Gitee v5 创建 PR 接受 form 参数。
        let v = self
            .send(self.form_request(
                "POST",
                "/pulls",
                &[
                    ("title", req.title.as_str()),
                    ("head", req.head_branch.as_str()),
                    ("base", req.base_branch.as_str()),
                    ("body", req.body.as_str()),
                ],
            ))
            .await?;
        Ok(parse_pr(&v, &req))
    }

    async fn list_open_pull_requests(&self) -> Result<Vec<PlatformPullRequest>> {
        let listing = self.list_all("/pulls", &OPEN_LIST_QUERY).await?;
        Ok(listing.iter().map(parse_pr_list_item).collect())
    }

    async fn comment_on_issue(&self, issue_number: u64, body: String) -> Result<()> {
        let number = self.issue_string_number(issue_number).await?;
        self.send(self.form_request(
            "POST",
            &format!("/issues/{number}/comments"),
            &[("body", body.as_str())],
        ))
        .await?;
        Ok(())
    }

    async fn merge_pull_request(&self, pr_number: u64, _sha: String) -> Result<()> {
        self.send(self.form_request(
            "PUT",
            &format!("/pulls/{pr_number}/merge"),
            &[("merge_method", "merge")],
        ))
        .await?;
        Ok(())
    }

    async fn add_labels(&self, issue_number: u64, labels: &[String]) -> Result<()> {
        // Gitee v5 给 PR 打标签走 pulls 端点，labels 为逗号分隔名单。
        let joined = labels.join(",");
        self.send(self.form_request(
            "POST",
            &format!("/pulls/{issue_number}/labels"),
            &[("labels", joined.as_str())],
        ))
        .await?;
        Ok(())
    }

    async fn list_issue_comments(&self, issue_number: u64) -> Result<Vec<PlatformComment>> {
        let number = self.issue_string_number(issue_number).await?;
        let listing = self
            .list_all(&format!("/issues/{number}/comments"), &[])
            .await?;
        Ok(listing.iter().map(parse_comment).collect())
    }

    async fn list_pull_comments(&self, pr_number: u64) -> Result<Vec<PlatformComment>> {
        // Gitee PR 评论端点与 issue 不同（PR 用数值 number，直接进路径）。
        let listing = self
            .list_all(&format!("/pulls/{pr_number}/comments"), &[])
            .await?;
        Ok(listing.iter().map(parse_comment).collect())
    }

    async fn comment_on_pull(&self, pr_number: u64, body: String) -> Result<()> {
        self.send(self.form_request(
            "POST",
            &format!("/pulls/{pr_number}/comments"),
            &[("body", body.as_str())],
        ))
        .await?;
        Ok(())
    }

    async fn fetch_attachment(&self, url: &str) -> Result<AttachmentData> {
        // Gateway mode (base like http://gw/gitee): zero-credential /attach
        // proxy; direct mode: fetch the public URL.
        let root = gateway_attach_root(&self.base);
        http_fetch_attachment(self.http.as_ref(), root.as_deref(), url).await
    }

    async fn get_pull_request(&self, pr_number: u64) -> Result<PullRequestDetail> {
        let pr = self
            .send(self.get(&format!("/pulls/{pr_number}"), &[]))
            .await?;
        // 文件清单是这份详情的两格（改了哪些文件、改了几行），读不成的时候整份
        // 报错：把读不成的清单当空的，交出去的是一句「这个 PR 一行没改」。同一个
        // PR 在 GitHub 那侧走 `?`，两侧不该对同一件事给两个答案。
        //
        // 单条文件少了 `additions` / `deletions` 仍按 0 计：Gitee 对二进制文件
        // 就是这么答的，「这个文件没有行数」是一个读到的数。
        let files = self
            .send(self.get(&format!("/pulls/{pr_number}/files"), &[]))
            .await?;
        let files = files.as_array().ok_or_else(|| {
            CogGitHubError::Provider(format!(
                "gitee /pulls/{pr_number}/files answered no array: {files}"
            ))
        })?;
        let affected_files: Vec<String> = files
            .iter()
            .filter_map(|f| f["filename"].as_str().map(String::from))
            .collect();
        let changed_lines = files
            .iter()
            .map(|f| f["additions"].as_u64().unwrap_or(0) + f["deletions"].as_u64().unwrap_or(0))
            .sum::<u64>() as usize;

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
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn unused_http() -> std::sync::Arc<dyn cog_core::HttpClient> {
        std::sync::Arc::new(crate::provider::UnusedHttpClient)
    }

    #[test]
    fn url_carries_token_only_in_direct_mode() {
        let direct = GiteeProvider::new("o/r", None, Some("tok".into()), unused_http()).unwrap();
        assert_eq!(
            direct.url("/issues"),
            "https://gitee.com/api/v5/repos/o/r/issues?access_token=tok"
        );
        let gateway =
            GiteeProvider::new("o/r", Some("http://gw:8081/gitee/"), None, unused_http()).unwrap();
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

    /// A Gitee that answers per path: the PR object and the file list are two
    /// reads, and the tests below need them to fail independently.
    #[derive(Debug)]
    struct PageByPath {
        pr: (u16, String),
        files: (u16, String),
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for PageByPath {
        async fn execute(
            &self,
            req: cog_core::HttpRequest,
        ) -> cog_core::SFResult<cog_core::HttpResponse> {
            let (status, body) = if req.url.contains("/files") {
                self.files.clone()
            } else {
                self.pr.clone()
            };
            Ok(cog_core::HttpResponse {
                status,
                headers: Default::default(),
                body: body.into_bytes(),
            })
        }
    }

    fn provider(pr: (u16, &str), files: (u16, &str)) -> GiteeProvider {
        GiteeProvider::new(
            "o/r",
            None,
            None,
            Arc::new(PageByPath {
                pr: (pr.0, pr.1.to_string()),
                files: (files.0, files.1.to_string()),
            }),
        )
        .unwrap()
    }

    const PR_JSON: &str = r#"{"number":7,"title":"t","state":"open","merged":false,
        "html_url":"https://gitee.com/o/r/pulls/7","head":{"sha":"abc"}}"#;

    /// 文件清单读不成时，这份详情就是一整份读不成的读数——不许折成「这个 PR
    /// 改动了 0 个文件、0 行」。旧代码 `.ok()` 正是那样：403 之后报的是一个
    /// 什么都没改的 PR。
    #[tokio::test]
    async fn a_pull_request_whose_files_could_not_be_read_is_not_one_that_changed_nothing() {
        let p = provider((200, PR_JSON), (403, r#"{"message":"forbidden"}"#));
        let error = p
            .get_pull_request(7)
            .await
            .expect_err("文件清单读不成，这份详情读不成");
        let said = error.to_string();
        assert!(said.contains("403"), "{said}");
    }

    /// 反例：两条读都成功时，行数是清单里加起来的那个数。
    #[tokio::test]
    async fn a_pull_request_detail_sums_the_files_it_read() {
        let p = provider(
            (200, PR_JSON),
            (
                200,
                r#"[{"filename":"a.rs","additions":3,"deletions":1},
                    {"filename":"b.rs","additions":0,"deletions":4}]"#,
            ),
        );
        let detail = p.get_pull_request(7).await.expect("两条读都成功");
        assert_eq!(detail.changed_lines, 8);
        assert_eq!(detail.affected_files, vec!["a.rs", "b.rs"]);
    }

    /// 2xx 的正文不是 JSON（代理挡在前面答一页 HTML）：这份读数坏了，不是一
    /// 个空对象。旧代码 `unwrap_or(Value::Null)` 把它读成「每个字段都空」。
    #[tokio::test]
    async fn an_answer_that_is_not_json_is_not_an_empty_object() {
        let p = provider((200, "<html>upstream says ok</html>"), (200, "[]"));
        let error = p
            .get_pull_request(7)
            .await
            .expect_err("不是 JSON 的 200 不是一份读到的答案");
        let said = error.to_string();
        assert!(said.contains("not JSON"), "{said}");
    }

    /// 反例：空正文照旧读成「没有东西要读」，写接口（合并、评论）靠它。
    #[tokio::test]
    async fn an_empty_body_is_still_nothing_to_read() {
        let p = provider((200, ""), (200, "[]"));
        // 空正文的 PR 对象是一份空对象：字段全缺，但不再是读失败。
        let detail = p.get_pull_request(7).await.expect("空正文不是坏读数");
        assert_eq!(detail.number, 7, "响应里没有 number 时落回请求里的那个");
        assert_eq!(detail.changed_lines, 0);
    }

    /// 一台按 `page`/`per_page` 分页的假 Gitee：手里有一份名单，问哪页给哪页。
    ///
    /// 真接口的规矩就是这个：不点名 `page` 时给第一页，一页最多 `per_page` 条
    /// （缺省 20，上限 100），超出的部分不问就不给。所以「只问一次」与「问到
    /// 问完」在这台假服务上拿到的是不同长度的名单——测试量的正是这一步。
    #[derive(Debug)]
    struct PagedGitee {
        items: Vec<serde_json::Value>,
        asked: Arc<AtomicUsize>,
    }

    fn paged(items: Vec<serde_json::Value>) -> (GiteeProvider, Arc<AtomicUsize>) {
        let asked = Arc::new(AtomicUsize::new(0));
        let provider = GiteeProvider::new(
            "o/r",
            None,
            None,
            Arc::new(PagedGitee {
                items,
                asked: asked.clone(),
            }),
        )
        .unwrap();
        (provider, asked)
    }

    fn query_of(url: &str, name: &str) -> Option<String> {
        let (_, query) = url.split_once('?')?;
        query.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == name).then(|| v.to_string())
        })
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for PagedGitee {
        async fn execute(
            &self,
            req: cog_core::HttpRequest,
        ) -> cog_core::SFResult<cog_core::HttpResponse> {
            self.asked.fetch_add(1, Ordering::SeqCst);
            let page: usize = query_of(&req.url, "page")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            let per_page: usize = query_of(&req.url, "per_page")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20);
            let start = (page.saturating_sub(1)) * per_page;
            let slice: Vec<serde_json::Value> = if start < self.items.len() {
                self.items[start..(start + per_page).min(self.items.len())].to_vec()
            } else {
                Vec::new()
            };
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: serde_json::to_vec(&slice).unwrap(),
            })
        }
    }

    fn issue_json(id: u64) -> serde_json::Value {
        serde_json::json!({
            "id": id, "number": format!("I{id}"), "title": format!("t{id}"), "body": "b",
            "state": "open", "user": {"login": "alice"},
            "created_at": "2026-08-01T10:00:00+08:00",
            "updated_at": "2026-08-02T10:00:00+08:00"
        })
    }

    /// 还开着的 issue 超过一页时，列出来的必须是全部：只问一次的老读法把
    /// 第 101 条起与「就这些」读成同一样，而发现循环拿这份名单当开放集。
    #[tokio::test]
    async fn a_listing_does_not_stop_at_the_first_page() {
        let (p, asked) = paged((1..=250).map(issue_json).collect());
        let issues = p.list_open_issues().await.expect("250 条都答得出来");
        assert_eq!(issues.len(), 250, "第 101 条起也必须在名单里");
        assert_eq!(issues.last().map(|i| i.number), Some(250));
        assert!(
            asked.load(Ordering::SeqCst) >= 3,
            "250 条按一页 100 要问三次，实际问了 {}",
            asked.load(Ordering::SeqCst)
        );
    }

    /// 正好装满一页不是「问完了」：得再问一页，拿到不满的一页才收手。
    /// 「满了就停」的写法在这里少答一次，下一批名单的第一条会被当成名单的结尾。
    #[tokio::test]
    async fn a_full_page_is_not_the_end_of_the_list() {
        let (p, asked) = paged((1..=100).map(issue_json).collect());
        let issues = p.list_open_issues().await.expect("100 条");
        assert_eq!(issues.len(), 100);
        assert_eq!(
            asked.load(Ordering::SeqCst),
            2,
            "满页之后还要再问一页（空的那页）才敢说没有了"
        );
    }

    /// issue 编号翻译要翻到最后一页：第 150 个 id 不在第一页上，只扫一页的
    /// 写法会说「没有这个 issue」，于是它的评论永远发不出去。
    #[tokio::test]
    async fn translating_an_issue_id_reads_past_the_first_page() {
        let (p, _) = paged((1..=150).map(issue_json).collect());
        let number = p
            .issue_string_number(150)
            .await
            .expect("第 150 个 id 在第二页上");
        assert_eq!(number, "I150");
    }

    /// 2xx 的正文是一句对象（错误信封、代理的一页 HTML）时，这一份名单读不成：
    /// 老写法 `.as_array()...unwrap_or_default()` 把它读成一份空名单——「一条
    /// 还开着的 issue 都没有」，与真的没有是两件事。
    #[tokio::test]
    async fn an_answer_that_is_not_a_list_is_not_an_empty_list() {
        let p = GiteeProvider::new(
            "o/r",
            None,
            None,
            Arc::new(PageByPath {
                pr: (200, r#"{"message":"rate limited"}"#.into()),
                files: (200, "[]".into()),
            }),
        )
        .unwrap();
        let error = p
            .list_open_issues()
            .await
            .expect_err("一句对象不是一份名单");
        assert!(error.to_string().contains("no array"), "{error}");
    }
}
