//! HTTP client implementation backed by [`reqwest`].
//! Use [`build_client`] for a one-off client, or [`ReqwestHttpClient`] when you
//! need to satisfy [`cog_core::HttpClient`] in tests or generic code.

use cog_core::{HttpClient, HttpClientConfig, HttpRequest, HttpResponse, SFError, SFResult};
use futures::TryStreamExt;
use reqwest::Client;
use std::time::Duration;
use tracing::{info, warn};

/// Build a [`reqwest::Client`] from [`HttpClientConfig`].
/// This is the simplest entry-point when you just need a concrete client.
pub fn build_client(config: &HttpClientConfig) -> Client {
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs))
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .pool_max_idle_per_host(config.pool_max_idle_per_host)
        .user_agent(&config.user_agent);

    if config.danger_accept_invalid_certs {
        warn!("danger_accept_invalid_certs is enabled — this should never be used in production");
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(ref proxy_url) = config.proxy_url {
        match reqwest::Proxy::all(proxy_url) {
            Ok(proxy) => {
                info!(proxy_url = %proxy_url, "HTTP proxy configured");
                builder = builder.proxy(proxy);
            }
            Err(e) => {
                warn!(proxy_url = %proxy_url, error = %e, "Failed to parse proxy URL; skipping proxy");
            }
        }
    }

    builder.build().unwrap_or_else(|e| {
        warn!(error = %e, "Failed to build HTTP client from config; falling back to default");
        Client::new()
    })
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
    pub fn from_config(config: &HttpClientConfig) -> Self {
        Self::new(build_client(config))
    }
}

#[async_trait::async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
        let method = match req.method.as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            "HEAD" => reqwest::Method::HEAD,
            "PATCH" => reqwest::Method::PATCH,
            other => other
                .parse()
                .map_err(|e| SFError::Agent(format!("invalid HTTP method '{}': {}", other, e)))?,
        };

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
        let method = match req.method.as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            "HEAD" => reqwest::Method::HEAD,
            "PATCH" => reqwest::Method::PATCH,
            other => other
                .parse()
                .map_err(|e| SFError::Agent(format!("invalid HTTP method '{}': {}", other, e)))?,
        };

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
        let _client = build_client(&HttpClientConfig::default());
    }

    #[test]
    fn custom_timeout_factory_builds_client() {
        let config = HttpClientConfig {
            timeout_secs: 60,
            connect_timeout_secs: 5,
            ..Default::default()
        };
        let _client = build_client(&config);
    }

    #[test]
    fn reqwest_http_client_from_config() {
        let client = ReqwestHttpClient::from_config(&HttpClientConfig::default());
        let _ = client.inner;
    }
}
