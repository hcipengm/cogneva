//! HTTP client 配置——cog-net 自有配置段（core config.rs 不聚合单 crate
//! 配置，审计文档 §7.3）。自读 cogneva.json `http_client` 段。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

/// Configuration for building an HTTP client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpClientConfig {
    /// Overall request timeout (including connection + transfer).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// TCP connect timeout.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Max idle connections to keep per host.
    #[serde(default = "default_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: usize,
    /// Whether to accept invalid certificates (dev only).
    #[serde(default)]
    pub danger_accept_invalid_certs: bool,
    /// Optional HTTP proxy URL.
    pub proxy_url: Option<String>,
    /// Default User-Agent header.
    #[serde(default = "default_user_agent")]
    pub user_agent: String,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout_secs: default_timeout_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
            pool_max_idle_per_host: default_pool_max_idle_per_host(),
            danger_accept_invalid_certs: false,
            proxy_url: None,
            user_agent: default_user_agent(),
        }
    }
}

impl HttpClientConfig {
    /// 自读 cogneva.json `http_client` 段；文件/段缺失回退默认，
    /// 段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(std::path::Path::new(&path))
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                match root.pointer("/http_client") {
                    Some(section) => serde_json::from_value(section.clone()).map_err(|e| {
                        SFError::Config(format!("{} http_client: {e}", path.display()))
                    }),
                    None => Ok(Self::default()),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(SFError::Config(format!("{}: {e}", path.display()))),
        }
    }
}

fn default_timeout_secs() -> u64 {
    30
}
fn default_connect_timeout_secs() -> u64 {
    10
}
fn default_pool_max_idle_per_host() -> usize {
    32
}
fn default_user_agent() -> String {
    "cogneva-http-client/0.1".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_returns_default() {
        let cfg = HttpClientConfig::load_from(std::path::Path::new("/nonexistent/x.json")).unwrap();
        assert_eq!(cfg.timeout_secs, 30);
    }

    #[test]
    fn reads_section() {
        let dir = std::env::temp_dir().join(format!("cog-net-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(&path, r#"{"http_client": {"timeout_secs": 5}}"#).unwrap();
        let cfg = HttpClientConfig::load_from(&path).unwrap();
        assert_eq!(cfg.timeout_secs, 5);
        assert_eq!(cfg.connect_timeout_secs, 10);
        std::fs::remove_dir_all(&dir).ok();
    }
}
