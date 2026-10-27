//! HTTP client implementation backed by [`reqwest`].
//! Use [`build_client`] for a one-off client, or [`ReqwestHttpClient`] when you
//! need to satisfy [`cog_core::HttpClient`] in tests or generic code.

use crate::HttpClientConfig;
use cog_core::{HttpClient, HttpRequest, HttpResponse, SFError, SFResult};
use futures::TryStreamExt;
use reqwest::Client;
use std::time::Duration;
use tracing::{info, warn};

/// Build a [`reqwest::Client`] from [`HttpClientConfig`].
/// This is the simplest entry-point when you just need a concrete client.
///
/// Fails rather than handing back an unconfigured client. A client built
/// without the timeouts, pool and proxy the caller asked for is not the client
/// they asked for, and a silently substituted one reads downstream exactly like
/// a configured one — the caller cannot tell which settings are in force.
pub fn build_client(config: &HttpClientConfig) -> SFResult<Client> {
    builder(config)?
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .map_err(|e| SFError::Config(format!("http client could not be built from config: {e}")))
}

/// Build a [`reqwest::Client`] for a request that is expected to stay open —
/// a server-sent-events stream, a long poll — and return it.
///
/// Same settings as [`build_client`] (connect timeout, idle pool, proxy, user
/// agent) minus the **overall request timeout**. That one is a deadline for a
/// short exchange; a stream is supposed to outlive it, so a client carrying it
/// would cut exactly the connections this one exists for. The connect timeout
/// stays: a stream from a host that never answers should still fail rather
/// than hang.
///
/// The settings are read from the same config section, so a long-lived client
/// is not a second set of numbers — only the deadline that makes no sense for
/// one is left out.
pub fn build_streaming_client(config: &HttpClientConfig) -> SFResult<Client> {
    builder(config)?
        .build()
        .map_err(|e| SFError::Config(format!("http client could not be built from config: {e}")))
}

/// The settings both constructors share, without either one's deadline.
fn builder(config: &HttpClientConfig) -> SFResult<reqwest::ClientBuilder> {
    let mut builder = Client::builder()
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .pool_max_idle_per_host(config.pool_max_idle_per_host)
        .user_agent(&config.user_agent);

    if config.danger_accept_invalid_certs {
        warn!("danger_accept_invalid_certs is enabled — this should never be used in production");
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(ref proxy_url) = config.proxy_url {
        // An unparsable proxy is a configuration error, not a reason to carry
        // on without one: going direct is the one thing a caller who
        // configured a proxy did not ask for, and it would be discovered only
        // by the traffic appearing where it was not meant to.
        let proxy = reqwest::Proxy::all(proxy_url).map_err(|e| {
            SFError::Config(format!(
                "http_client.proxy_url {proxy_url:?} is not a URL: {e}"
            ))
        })?;
        info!(proxy_url = %proxy_url, "HTTP proxy configured");
        builder = builder.proxy(proxy);
    }

    Ok(builder)
}

/// A [`cog_core::HttpClient`] implementation backed by [`reqwest::Client`].
#[derive(Debug, Clone)]
pub struct ReqwestHttpClient {
    inner: Client,
}

impl ReqwestHttpClient {
    /// Create from an existing [`reqwest::Client`].
    pub fn new(inner: Client) -> Self {
        Self { inner }
    }

    /// Build from [`HttpClientConfig`] via [`build_client`].
    pub fn from_config(config: &HttpClientConfig) -> SFResult<Self> {
        Ok(Self::new(build_client(config)?))
    }
}

/// 动词名与 reqwest 方法之间只有这一张表。
///
/// 走整包与走流式是两条执行路径，两张表时加一个动词只会加到其中一张上；另一张
/// 落到 `parse()`，而 `parse()` 恰好也认得同一批标准动词，所以这种偏差不会在
/// 任何测试或日志里显出来——直到某个不走 `parse()` 的方法名（带下划线、带空白的
/// 变体）出现，两条路的答案才开始不一样。
fn method_of(name: &str) -> SFResult<reqwest::Method> {
    Ok(match name {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        "HEAD" => reqwest::Method::HEAD,
        "PATCH" => reqwest::Method::PATCH,
        other => other
            .parse()
            .map_err(|e| SFError::Agent(format!("invalid HTTP method '{}': {}", other, e)))?,
    })
}

#[async_trait::async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
        let method = method_of(&req.method)?;

        let mut request_builder = self.inner.request(method, &req.url);

        for (k, v) in &req.headers {
            request_builder = request_builder.header(k, v);
        }

        if let Some(body) = req.body {
            request_builder = request_builder.body(body);
        }

        if let Some(timeout_secs) = req.timeout_secs {
            request_builder = request_builder.timeout(Duration::from_secs(timeout_secs));
        }

        let resp = request_builder
            .send()
            .await
            .map_err(|e| SFError::Agent(format!("HTTP request failed: {}", e)))?;

        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
            .collect();
        let body = resp
            .bytes()
            .await
            .map_err(|e| SFError::Agent(format!("failed to read response body: {}", e)))?
            .to_vec();

        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn execute_stream(&self, req: HttpRequest) -> SFResult<cog_core::HttpStreamResponse> {
        let method = method_of(&req.method)?;

        let mut request_builder = self.inner.request(method, &req.url);

        for (k, v) in &req.headers {
            request_builder = request_builder.header(k, v);
        }

        if let Some(body) = req.body {
            request_builder = request_builder.body(body);
        }

        if let Some(timeout_secs) = req.timeout_secs {
            request_builder = request_builder.timeout(Duration::from_secs(timeout_secs));
        }

        let resp = request_builder
            .send()
            .await
            .map_err(|e| SFError::Agent(format!("HTTP request failed: {}", e)))?;

        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
            .collect();

        let stream = resp
            .bytes_stream()
            .map_err(|e| SFError::Agent(format!("HTTP stream error: {}", e)));

        Ok(cog_core::HttpStreamResponse {
            status,
            headers,
            stream: Box::pin(stream),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_factory_builds_client() {
        let _client = build_client(&HttpClientConfig::default())
            .expect("the default config is a buildable one");
    }

    #[test]
    fn custom_timeout_factory_builds_client() {
        let config = HttpClientConfig {
            timeout_secs: 60,
            connect_timeout_secs: 5,
            ..Default::default()
        };
        let _client = build_client(&config).expect("a timeout is a buildable config");
    }

    #[test]
    fn reqwest_http_client_from_config() {
        let client = ReqwestHttpClient::from_config(&HttpClientConfig::default())
            .expect("the default config is a buildable one");
        let _ = client.inner;
    }

    /// The point of the failure path: a config that cannot be honoured has to
    /// come back as an error. Handing back a client built without the proxy
    /// would leave the caller sending their traffic direct while believing it
    /// goes through the proxy they configured.
    #[test]
    fn an_unusable_config_is_an_error_and_not_a_default_client() {
        let config = HttpClientConfig {
            proxy_url: Some("not a url".into()),
            ..Default::default()
        };
        assert!(
            build_client(&config).is_err(),
            "an unparsable proxy has to fail the build, not be dropped"
        );
    }
}
