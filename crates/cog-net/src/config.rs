//! HTTP client 配置——cog-net 自有配置段（core config.rs 不聚合单 crate
//! 配置）。自读 cogneva.json `http_client` 段。

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
        let path = cog_core::paths::config_file();
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

// 十秒：TCP 三次握手在最坏的跨网段往返上的上限——再短，一次正常的网络抖动就被
// 判成「对端不可达」；再长，「对端根本没起来」会一直拖到整体请求超时才暴露。
// cap: bound — 十秒：跨网段握手的最坏往返；再短误判不可达，再长拖成整体超时
// wall: wait=TCP handshake; on-timeout=the connect fails with a network error; resume=yes, the caller may retry
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

// 三十二：同时向同一个提供方在飞的请求数就是每个主机的空闲连接上限该覆盖的峰值，
// 再多只是让提供方替我们占着文件描述符。
// cap: bound — 三十二：同一提供方在飞请求数的峰值；再多只是替提供方占 fd
const DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 32;

fn default_timeout_secs() -> u64 {
    (cog_core::day::SECONDS_PER_MINUTE / 2) as u64
}
fn default_connect_timeout_secs() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECS
}
fn default_pool_max_idle_per_host() -> usize {
    DEFAULT_POOL_MAX_IDLE_PER_HOST
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
        // 文件没写的那一格回落到默认值，而不是回落到某个写在这里的第二份数。
        assert_eq!(
            cfg.connect_timeout_secs,
            HttpClientConfig::default().connect_timeout_secs
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 两个默认值各只有一处声明：`fn default_*` 读具名常量，常量上写着它由什么钉住。
    #[test]
    fn the_http_client_defaults_read_their_names() {
        let cfg = HttpClientConfig::default();
        assert_eq!(cfg.connect_timeout_secs, DEFAULT_CONNECT_TIMEOUT_SECS);
        assert_eq!(cfg.pool_max_idle_per_host, DEFAULT_POOL_MAX_IDLE_PER_HOST);
    }
}
