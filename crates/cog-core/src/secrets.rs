//! 密钥管理：统一 `SecretProvider` 契约 + 日志脱敏。
//!
//! 支持三种来源：
//! - 环境变量（[`EnvSecretProvider`]）
//! - 文件（[`FileSecretProvider`]，覆盖 K8s Secret 挂载卷场景）
//! - 外部提供者（如 Vault）只需实现 [`SecretProvider`] 并在组合根注册
//!
//! [`redact_secrets`] 用于日志输出前的脱敏，命中常见 API Key 形态的内容
//! 会被替换为 `[redacted]`。

use std::path::PathBuf;

/// 统一密钥提供者契约。
#[async_trait::async_trait]
pub trait SecretProvider: Send + Sync {
    /// 提供者名称（用于诊断日志）。
    fn name(&self) -> &'static str;
    /// 按引用读取密钥；未找到返回 `Ok(None)`。
    async fn get(&self, reference: &str) -> crate::SFResult<Option<String>>;
}

/// 环境变量密钥提供者。
pub struct EnvSecretProvider;

#[async_trait::async_trait]
impl SecretProvider for EnvSecretProvider {
    fn name(&self) -> &'static str {
        "env"
    }

    async fn get(&self, reference: &str) -> crate::SFResult<Option<String>> {
        Ok(std::env::var(reference).ok().filter(|v| !v.is_empty()))
    }
}

/// 文件密钥提供者 —— 覆盖 K8s Secret 挂载卷（如 `/var/run/secrets/...`）。
/// `reference` 相对于根目录；拒绝 `..` 逃逸。
pub struct FileSecretProvider {
    root: PathBuf,
}

impl FileSecretProvider {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait::async_trait]
impl SecretProvider for FileSecretProvider {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn get(&self, reference: &str) -> crate::SFResult<Option<String>> {
        if reference.split('/').any(|seg| seg == "..") {
            return Err(crate::SFError::Validation(format!(
                "secret reference escapes root: {reference}"
            )));
        }
        let path = self.root.join(reference);
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => Ok(Some(content.trim_end().to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(crate::SFError::IO(format!(
                "read secret {}: {e}",
                path.display()
            ))),
        }
    }
}

/// 按优先级链式查询多个提供者。
pub struct ChainedSecretProvider {
    providers: Vec<std::sync::Arc<dyn SecretProvider>>,
}

impl ChainedSecretProvider {
    pub fn new(providers: Vec<std::sync::Arc<dyn SecretProvider>>) -> Self {
        Self { providers }
    }

    /// 依次查询，返回第一个命中的值。
    pub async fn resolve(&self, reference: &str) -> crate::SFResult<Option<String>> {
        for provider in &self.providers {
            if let Some(value) = provider.get(reference).await? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

/// 日志脱敏：将常见 API Key / Token 形态替换为 `[redacted]`。
/// 覆盖：OpenAI `sk-...`、Anthropic `sk-ant-...`、GitHub `ghp_...`/`github_pat_...`、
/// AWS `AKIA...`、JWT、以及 `api_key=<value>` / `token=<value>` 键值形态。
pub fn redact_secrets(input: &str) -> String {
    let mut out = input.to_string();
    for pattern in SECRET_PATTERNS {
        let re = regex::Regex::new(pattern).expect("static secret pattern is valid");
        out = re.replace_all(&out, "[redacted]").into_owned();
    }
    out
}

const SECRET_PATTERNS: &[&str] = &[
    r"sk-ant-[A-Za-z0-9_-]{8,}",
    r"sk-[A-Za-z0-9_-]{16,}",
    r"ghp_[A-Za-z0-9]{16,}",
    r"github_pat_[A-Za-z0-9_]{16,}",
    r"AKIA[0-9A-Z]{16}",
    r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}",
    r"(?i)(api[_-]?key|token|secret|password)=[^\s&]{4,}",
    r"(?i)(api[_-]?key|token|secret|password)\s*:\s*[^\s,}]{4,}",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn env_provider_reads_variable() {
        std::env::set_var("COGNEVA_TEST_SECRET_ENV", "s3cret");
        let value = EnvSecretProvider
            .get("COGNEVA_TEST_SECRET_ENV")
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some("s3cret"));
        std::env::remove_var("COGNEVA_TEST_SECRET_ENV");
    }

    #[tokio::test]
    async fn file_provider_reads_and_blocks_escape() {
        let dir = std::env::temp_dir().join(format!("cog-secret-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("api-key"), "file-secret\n").unwrap();

        let provider = FileSecretProvider::new(&dir);
        let value = provider.get("api-key").await.unwrap();
        assert_eq!(value.as_deref(), Some("file-secret"));

        assert!(provider.get("../outside").await.is_err());
        assert!(provider.get("missing").await.unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn chained_provider_returns_first_hit() {
        std::env::set_var("COGNEVA_TEST_SECRET_CHAIN", "chained");
        let chain = ChainedSecretProvider::new(vec![
            std::sync::Arc::new(FileSecretProvider::new("/nonexistent")),
            std::sync::Arc::new(EnvSecretProvider),
        ]);
        let value = chain.resolve("COGNEVA_TEST_SECRET_CHAIN").await.unwrap();
        assert_eq!(value.as_deref(), Some("chained"));
        std::env::remove_var("COGNEVA_TEST_SECRET_CHAIN");
    }

    #[test]
    fn redact_common_key_shapes() {
        assert_eq!(
            redact_secrets("key=sk-abcdefghijklmnop1234 done"),
            "key=[redacted] done"
        );
        assert_eq!(
            redact_secrets("token ghp_0123456789abcdefZZ"),
            "token [redacted]"
        );
        assert_eq!(redact_secrets("api_key=supersecretvalue"), "[redacted]");
        assert_eq!(redact_secrets("nothing secret here"), "nothing secret here");
    }
}
