//! GitHub integration 配置——cog-github 自有配置段（core config.rs 不
//! 聚合单 crate 配置）。自读 cogneva.json
//! `github_integration` 段并叠加 `COGNEVA_GITHUB_*` env 覆盖。
//!
//! Security rule: **tokens never enter the evolution sandbox**. They are
//! resolved from environment variables or K8s secrets by the gateway/main
//! process and are kept out of sandbox containers / MicroVMs.

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

/// A registered GitHub account used by Cogneva to interact with GitHub.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GitHubAccount {
    /// Bot-registered account (machine user / GitHub App installation).
    Bot(BotAccount),
    /// Human-registered account whose credentials are handed to Cogneva.
    Human(HumanAccount),
}

impl GitHubAccount {
    /// The GitHub login/username of this account.
    pub fn username(&self) -> &str {
        match self {
            GitHubAccount::Bot(a) => &a.username,
            GitHubAccount::Human(a) => &a.username,
        }
    }

    /// The display name used for Git commits and PR comments.
    pub fn display_name(&self) -> &str {
        match self {
            GitHubAccount::Bot(a) => &a.name,
            GitHubAccount::Human(a) => &a.name,
        }
    }

    /// The email address used for Git commits.
    pub fn email(&self) -> &str {
        match self {
            GitHubAccount::Bot(a) => &a.email,
            GitHubAccount::Human(a) => &a.email,
        }
    }

    /// Resolve the GitHub token.
    ///
    /// Tries `token_env` first, then falls back to the inline `token`.
    /// The inline token is intended for local testing only.
    pub fn resolve_token(&self) -> SFResult<String> {
        match self {
            GitHubAccount::Bot(a) => a.resolve_token(),
            GitHubAccount::Human(a) => a.resolve_token(),
        }
    }

    /// True if this is a bot-registered account.
    pub fn is_bot(&self) -> bool {
        matches!(self, GitHubAccount::Bot(_))
    }

    /// True if this is a human-registered account.
    pub fn is_human(&self) -> bool {
        matches!(self, GitHubAccount::Human(_))
    }
}

/// Bot-registered account configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BotAccount {
    /// GitHub login/username of the bot account.
    pub username: String,
    /// Display name used in Git commits and PR comments.
    pub name: String,
    /// Email address used in Git commits.
    pub email: String,
    /// Name of the environment variable that holds the GitHub token.
    /// Recommended production mechanism.
    pub token_env: Option<String>,
    /// Inline GitHub token. **Only for local testing.**
    pub token: Option<String>,
    /// Optional GitHub App installation ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_installation_id: Option<u64>,
}

impl BotAccount {
    /// Resolve the effective GitHub token for this bot account.
    pub fn resolve_token(&self) -> SFResult<String> {
        resolve_token(&self.username, &self.token_env, &self.token)
    }
}

impl Default for BotAccount {
    fn default() -> Self {
        Self {
            username: String::new(),
            name: "Cogneva Bot".into(),
            email: "bot@cogneva.ai".into(),
            token_env: Some("COGNEVA_GITHUB_BOT_TOKEN".into()),
            token: None,
            app_installation_id: None,
        }
    }
}

/// Human-registered account configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HumanAccount {
    /// GitHub login/username of the human account.
    pub username: String,
    /// Display name used in Git commits and PR comments.
    pub name: String,
    /// Email address used in Git commits.
    pub email: String,
    /// Name of the environment variable that holds the GitHub token.
    pub token_env: Option<String>,
    /// Inline GitHub token. **Only for local testing.**
    pub token: Option<String>,
}

impl HumanAccount {
    /// Resolve the effective GitHub token for this human account.
    pub fn resolve_token(&self) -> SFResult<String> {
        resolve_token(&self.username, &self.token_env, &self.token)
    }
}

impl Default for HumanAccount {
    fn default() -> Self {
        Self {
            username: String::new(),
            name: String::new(),
            email: String::new(),
            token_env: Some("COGNEVA_GITHUB_HUMAN_TOKEN".into()),
            token: None,
        }
    }
}

fn resolve_token(
    username: &str,
    token_env: &Option<String>,
    token: &Option<String>,
) -> SFResult<String> {
    if let Some(env_name) = token_env {
        if let Ok(value) = std::env::var(env_name) {
            if !value.is_empty() {
                return Ok(value);
            }
        }
    }
    if let Some(value) = token {
        if !value.is_empty() {
            return Ok(value.clone());
        }
    }
    Err(SFError::Validation(format!(
        "github account has no token: account={}",
        username
    )))
}

/// Webhook 事件入口配置（discovery_mode=events/both 时启用）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookConfig {
    /// 监听端口。
    pub port: u16,
    /// Webhook secret 所在环境变量名（HMAC-SHA256 签名验证）。
    pub secret_env: String,
    /// 监听路径。
    pub path: String,
    /// 网关验签模式：true 时平台签名由安全网关完成，本进程只验网关
    /// 转发的内部 HMAC（COGNEVA_WEBHOOK_INTERNAL_SECRET），GitHub 与
    /// Gitee 事件共用同一入口；false 保持 legacy 直连验签（仅 GitHub）。
    pub gateway_verified: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            secret_env: "COGNEVA_GITHUB_WEBHOOK_SECRET".into(),
            path: "/webhooks/github".into(),
            gateway_verified: false,
        }
    }
}

/// Top-level configuration section for the Cogneva GitHub integration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GitHubIntegrationConfig {
    /// Master switch for the GitHub integration.
    pub enabled: bool,
    /// Discovery mode: `polling`, `events`, or `both`.
    pub discovery_mode: String,
    /// Target repository in `owner/repo` format.
    pub repo: String,
    /// Base branch for PRs (e.g. `main`).
    pub base_branch: String,
    /// Registered accounts. The first account with a non-empty username is
    /// used as the primary actor.
    pub accounts: Vec<GitHubAccount>,
    /// Polling interval in seconds when discovery mode includes polling.
    pub poll_interval_secs: u64,
    /// Maximum number of issues to scan per polling round.
    pub max_issues_per_scan: usize,
    /// Whether to automatically create PRs when a change is generated.
    pub auto_create_pr: bool,
    /// Policy for deciding whether a PR can be automatically merged.
    pub auto_merge_policy: AutoMergePolicy,
    /// Labels that force human review before any action is taken.
    pub human_required_labels: Vec<String>,
    /// Labels that cause the issue to be skipped entirely.
    pub forbidden_labels: Vec<String>,
    /// Allowed issue states to scan.
    pub allowed_issue_states: Vec<String>,
    /// Bot identity used to recognize the bot's own comments.
    pub bot_identity: BotIdentityConfig,
    /// Multi-round clarification settings.
    pub conversation: ConversationConfig,
    /// Webhook 事件入口（discovery_mode=events/both 时生效）。
    pub webhook: WebhookConfig,
    /// Local git working copy used by the PR publisher. Empty disables
    /// change-to-PR publishing (the ChangeSink is not registered).
    pub pr_workdir: String,
    /// GitHub API 基址覆盖：指向安全网关透传端点（如
    /// `http://cogneva-security-gateway:8081/github`）。设置后本进程不再
    /// 解析平台 token，凭证由网关出口注入；不设置则直连 api.github.com
    /// 并走 token_env/inline token 解析。
    pub api_base: Option<String>,
}

impl Default for GitHubIntegrationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            discovery_mode: "polling".into(),
            repo: String::new(),
            base_branch: "main".into(),
            accounts: vec![
                GitHubAccount::Bot(BotAccount::default()),
                GitHubAccount::Human(HumanAccount::default()),
            ],
            poll_interval_secs: 300,
            max_issues_per_scan: 50,
            auto_create_pr: true,
            auto_merge_policy: AutoMergePolicy::default(),
            human_required_labels: vec!["security".into(), "breaking-change".into()],
            forbidden_labels: vec!["wontfix".into(), "manual-only".into()],
            allowed_issue_states: vec!["open".into()],
            bot_identity: BotIdentityConfig::default(),
            conversation: ConversationConfig::default(),
            webhook: WebhookConfig::default(),
            pr_workdir: String::new(),
            api_base: None,
        }
    }
}

impl GitHubIntegrationConfig {
    /// Returns the first account with a non-empty username, or an error if
    /// no account is configured.
    pub fn primary_account(&self) -> SFResult<&GitHubAccount> {
        self.accounts
            .iter()
            .find(|a| !a.username().is_empty())
            .ok_or_else(|| SFError::Validation("no GitHub account configured".into()))
    }
}

/// Policy for automatically merging a generated PR.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AutoMergePolicy {
    /// Whether auto-merge is enabled at all.
    pub enabled: bool,
    /// Require CI checks to pass before auto-merge.
    pub require_ci_pass: bool,
    /// Wait if any reviewer has been requested.
    pub require_no_review_requested: bool,
    /// Maximum number of changed lines allowed for auto-merge.
    pub max_changed_lines: usize,
    /// File paths that forbid auto-merge when touched.
    pub forbidden_paths: Vec<String>,
    /// Labels that forbid auto-merge when present.
    pub forbidden_labels: Vec<String>,
    /// Minimum hours to wait after PR creation before merging.
    pub cooldown_hours: u64,
    /// Whether a human can override auto-merge via labels.
    pub require_human_review_override: bool,
}

impl Default for AutoMergePolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            require_ci_pass: true,
            require_no_review_requested: false,
            max_changed_lines: 200,
            forbidden_paths: vec![
                ".github/workflows".into(),
                "deploy/".into(),
                "secrets/".into(),
                "*.lock".into(),
            ],
            forbidden_labels: vec![
                "security".into(),
                "breaking-change".into(),
                "manual-only".into(),
            ],
            cooldown_hours: 24,
            require_human_review_override: true,
        }
    }
}

/// Multi-round issue clarification settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConversationConfig {
    /// Maximum number of clarification rounds.
    pub max_clarification_rounds: u32,
    /// Hours to wait for a reply before timing out.
    pub awaiting_reply_timeout_hours: u64,
    /// Whether to automatically post clarification questions as GitHub comments.
    pub auto_reply: bool,
    /// Signature appended to bot comments.
    pub bot_signature: String,
}

impl Default for ConversationConfig {
    fn default() -> Self {
        Self {
            max_clarification_rounds: 3,
            awaiting_reply_timeout_hours: 72,
            auto_reply: true,
            bot_signature: "— Cogneva Bot".into(),
        }
    }
}

/// Bot identity used to recognize the bot's own comments and sign commits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BotIdentityConfig {
    /// GitHub bot username.
    pub username: String,
    /// Git commit author name (may be overridden by the instance persona once
    /// generated; kept for backward-compatible static configuration).
    pub name: String,
    /// Git commit email.
    pub email: String,
    /// 实例自治身份：人名池分配的名字（Alice/Ralph/…），首次进化时生成并持久化。
    pub persona: Option<String>,
    /// 实例机器指纹（SHA-256 hex）；身份的规范来源，缺失时首次使用自动生成。
    pub fingerprint: Option<String>,
}

impl Default for BotIdentityConfig {
    fn default() -> Self {
        Self {
            username: "cogneva-bot".into(),
            name: "Cogneva Bot".into(),
            email: "bot@cogneva.ai".into(),
            persona: None,
            fingerprint: None,
        }
    }
}

impl BotIdentityConfig {
    /// 已解析的实例自治身份（配置里存在指纹时）；人名由指纹规范重算。
    pub fn instance(&self) -> Option<crate::identity::InstanceIdentity> {
        self.fingerprint
            .as_ref()
            .filter(|fp| !fp.is_empty())
            .map(|fp| crate::identity::InstanceIdentity::from_fingerprint(fp))
    }

    /// 提交作者名：优先实例句柄（如 `Alice#a3f9d2c1`），回退静态配置。
    pub fn git_author_name(&self) -> String {
        self.instance()
            .map(|i| i.git_name)
            .unwrap_or_else(|| self.name.clone())
    }

    /// 提交作者邮箱：优先实例邮箱（per-instance，可归因），回退静态配置。
    pub fn git_author_email(&self) -> String {
        self.instance()
            .map(|i| i.git_email)
            .unwrap_or_else(|| self.email.clone())
    }
}

const GITHUB_ENV: &[(&str, &str)] = &[
    ("COGNEVA_GITHUB_REPO", "repo"),
    ("COGNEVA_GITHUB_BASE_BRANCH", "base_branch"),
    ("COGNEVA_GITHUB_API_BASE", "api_base"),
    (
        "COGNEVA_GITHUB_WEBHOOK_GATEWAY_VERIFIED",
        "webhook.gateway_verified",
    ),
];
impl GitHubIntegrationConfig {
    /// 自读 cogneva.json `github_integration` 段 + env 覆盖。
    /// 文件/段缺失回退默认（enabled=false）；段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(std::path::Path::new(&path))
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        let mut section = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                root.pointer("/github_integration")
                    .cloned()
                    .unwrap_or(serde_json::json!({}))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cog_core::config::apply_env_paths(&mut section, GITHUB_ENV);
        serde_json::from_value(section)
            .map_err(|e| SFError::Config(format!("{} github_integration: {e}", path.display())))
    }
}

/// Gitee 集成配置（cogneva.json `gitee_integration` 段）。
///
/// Gitee 与 GitHub 地位平等：issue 即外部意图进化入口。只承载平台特有
/// 字段（仓库/基址/轮询节奏）；分诊标签、澄清对话、自动合并等策略与
/// `github_integration` 共享同一份，由插件合成循环配置，避免两端漂移。
/// 凭证约定同 GitHub：`api_base` 指向安全网关透传端点（`/gitee`）时
/// 本进程零 token；直连 gitee.com 时才用 `token_env`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GiteeIntegrationConfig {
    /// Master switch for the Gitee integration.
    pub enabled: bool,
    /// Target repository in `owner/repo` format.
    pub repo: String,
    /// Base branch for PRs (e.g. `main`).
    pub base_branch: String,
    /// Gitee API 基址覆盖：指向安全网关 `/gitee` 透传端点；不设置则直连
    /// `https://gitee.com/api/v5`。
    pub api_base: Option<String>,
    /// Polling interval in seconds.
    pub poll_interval_secs: u64,
    /// Maximum number of issues to scan per polling round.
    pub max_issues_per_scan: usize,
    /// 直连模式 token 环境变量名（网关模式留空）。
    pub token_env: Option<String>,
}

impl Default for GiteeIntegrationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            repo: String::new(),
            base_branch: "main".into(),
            api_base: None,
            poll_interval_secs: 300,
            max_issues_per_scan: 50,
            token_env: None,
        }
    }
}

const GITEE_ENV: &[(&str, &str)] = &[
    ("COGNEVA_GITEE_REPO", "repo"),
    ("COGNEVA_GITEE_BASE_BRANCH", "base_branch"),
    ("COGNEVA_GITEE_API_BASE", "api_base"),
];

impl GiteeIntegrationConfig {
    /// 自读 cogneva.json `gitee_integration` 段 + env 覆盖，语义同
    /// [`GitHubIntegrationConfig::load`]。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(std::path::Path::new(&path))
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        let mut section = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                root.pointer("/gitee_integration")
                    .cloned()
                    .unwrap_or(serde_json::json!({}))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cog_core::config::apply_env_paths(&mut section, GITEE_ENV);
        serde_json::from_value(section)
            .map_err(|e| SFError::Config(format!("{} gitee_integration: {e}", path.display())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primary_account() {
        let cfg = GitHubIntegrationConfig {
            accounts: vec![
                GitHubAccount::Bot(BotAccount {
                    username: "cogneva-bot".into(),
                    ..Default::default()
                }),
                GitHubAccount::Human(HumanAccount {
                    username: "human".into(),
                    ..Default::default()
                }),
            ],
            ..Default::default()
        };
        assert_eq!(cfg.primary_account().unwrap().username(), "cogneva-bot");
    }

    #[test]
    fn test_resolve_token_from_env() {
        // 独立 env var 名：测试线程共享进程 env，同名 var 会竞态。
        let account = BotAccount {
            username: "test-bot".into(),
            token_env: Some("COG_GITHUB_TEST_TOKEN_BOT".into()),
            token: None,
            ..Default::default()
        };
        std::env::set_var("COG_GITHUB_TEST_TOKEN_BOT", "ghp_secret_from_env");
        assert_eq!(account.resolve_token().unwrap(), "ghp_secret_from_env");
        std::env::remove_var("COG_GITHUB_TEST_TOKEN_BOT");
    }

    #[test]
    fn test_resolve_token_prefers_env_over_inline() {
        let account = HumanAccount {
            username: "alice".into(),
            token_env: Some("COG_GITHUB_TEST_TOKEN_HUMAN".into()),
            token: Some("inline_token".into()),
            ..Default::default()
        };
        std::env::set_var("COG_GITHUB_TEST_TOKEN_HUMAN", "env_token");
        assert_eq!(account.resolve_token().unwrap(), "env_token");
        std::env::remove_var("COG_GITHUB_TEST_TOKEN_HUMAN");
    }

    #[test]
    fn missing_file_returns_default() {
        let cfg = GitHubIntegrationConfig::load_from(std::path::Path::new("/nonexistent/x.json"))
            .unwrap();
        assert!(!cfg.enabled);
    }

    #[test]
    fn reads_section() {
        let dir = std::env::temp_dir().join(format!("cog-github-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"github_integration": {"enabled": true, "repo": "a/b", "poll_interval_secs": 45}}"#,
        )
        .unwrap();
        let cfg = GitHubIntegrationConfig::load_from(&path).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.repo, "a/b");
        assert_eq!(cfg.poll_interval_secs, 45);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn gitee_section_defaults_and_parse() {
        let dir = std::env::temp_dir().join(format!("cog-gitee-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"gitee_integration": {"enabled": true, "repo": "o/r", "api_base": "http://gw:8081/gitee"}}"#,
        )
        .unwrap();
        let cfg = GiteeIntegrationConfig::load_from(&path).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.repo, "o/r");
        assert_eq!(cfg.base_branch, "main");
        assert_eq!(cfg.api_base.as_deref(), Some("http://gw:8081/gitee"));
        std::fs::remove_dir_all(&dir).ok();

        let missing =
            GiteeIntegrationConfig::load_from(std::path::Path::new("/nonexistent/x.json")).unwrap();
        assert!(!missing.enabled);
    }
}
