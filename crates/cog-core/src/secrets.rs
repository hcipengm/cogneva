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

/// Redact a structured payload: run [`redact_secrets`] over every string, and
/// replace whole members whose key names a credential.
///
/// Serializing the value and redacting the text is not an option: the key-value
/// patterns match across a JSON member's quotes and colon and leave the document
/// unparseable. Walking the nodes keeps the structure while the string shapes
/// (`sk-...`, JWTs, a `token=...` written inside a value) stay visible.
///
/// The key half has no text counterpart: in JSON the key sits outside the value,
/// so `{"token": "..."}` is a shape a string-level redactor can never see. The
/// criterion is the exact key, not a prefix, and the vocabulary is the same one
/// the text patterns use -- a second copy would drift against it.
pub fn redact_json_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => *text = redact_secrets(text),
        serde_json::Value::Array(items) => items.iter_mut().for_each(redact_json_secrets),
        serde_json::Value::Object(members) => {
            let credential_key =
                regex::Regex::new(CREDENTIAL_KEY_PATTERN).expect("static key pattern is valid");
            for (key, member) in members.iter_mut() {
                if credential_key.is_match(key) {
                    *member = serde_json::Value::String("[redacted]".to_string());
                } else {
                    redact_json_secrets(member);
                }
            }
        }
        _ => {}
    }
}

/// The credential key names, written once: both the text patterns and the
/// structured-payload key criterion below are built from this one literal, so a
/// word added here reaches both readers instead of one.
macro_rules! define_secret_patterns {
    ($words:literal) => {
        /// "This member's value is a credential" for a structured payload: the
        /// whole key text, anchored, so `token_budget` is not one.
        const CREDENTIAL_KEY_PATTERN: &str = concat!(r"(?i)^(", $words, r")$");

        const SECRET_PATTERNS: &[&str] = &[
            r"sk-ant-[A-Za-z0-9_-]{8,}",
            r"sk-[A-Za-z0-9_-]{16,}",
            r"ghp_[A-Za-z0-9]{16,}",
            r"github_pat_[A-Za-z0-9_]{16,}",
            r"AKIA[0-9A-Z]{16}",
            r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}",
            concat!(r"(?i)(", $words, r")=[^\s&]{4,}"),
            concat!(r"(?i)(", $words, r")\s*:\s*[^\s,}]{4,}"),
        ];
    };
}

define_secret_patterns!("api[_-]?key|token|secret|password");

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

    /// The structured form has to cover both halves: a member filed under a
    /// credential key, and a credential shape sitting inside a value. The last
    /// two members are what separates an exact key match from a prefix one --
    /// `token_budget` is a number this system counts, not a credential.
    #[test]
    fn redact_json_covers_keys_and_leaves_the_structure_alone() {
        let mut value = serde_json::json!({
            "api_key": "supersecretvalue",
            "token": 12345,
            "nested": { "password": ["hunter2xyz"] },
            "note": "key=sk-abcdefghijklmnop1234 done",
            "list": ["ghp_0123456789abcdefZZ"],
            "count": 3,
            "token_budget": 7,
        });
        redact_json_secrets(&mut value);

        assert_eq!(value["api_key"], serde_json::json!("[redacted]"));
        assert_eq!(value["token"], serde_json::json!("[redacted]"));
        // The member under a credential key goes wholesale, whatever its shape.
        assert_eq!(value["nested"]["password"], serde_json::json!("[redacted]"));
        assert_eq!(value["note"], serde_json::json!("key=[redacted] done"));
        assert_eq!(value["list"][0], serde_json::json!("[redacted]"));
        assert_eq!(value["count"], serde_json::json!(3));
        assert_eq!(value["token_budget"], serde_json::json!(7));
    }
}
