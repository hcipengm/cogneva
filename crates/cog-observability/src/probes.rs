/// Health probes and Pushgateway client.
/// - HTTP/TCP health probes with timeout and status validation
/// - SSL certificate expiry monitoring
/// - Batch job metrics push to Prometheus Pushgateway
///   **Phase 1**: HTTP probes + Pushgateway HTTP push.
///   **Phase 2**: TCP probes, DNS probes, ICMP probes.
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Result of a single probe.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub success: bool,
    pub duration_ms: u64,
    pub status_code: Option<u16>,
    /// Days until SSL certificate expires (None if not checked or no TLS).
    pub ssl_expiry_days: Option<u32>,
    pub error: Option<String>,
}

/// HTTP health probe configuration.
#[derive(Debug, Clone)]
pub struct HttpProbe {
    pub url: String,
    pub timeout: Duration,
    pub expected_status: u16,
    pub follow_redirects: bool,
    pub headers: HashMap<String, String>,
    pub check_ssl_expiry: bool,
    pub ssl_expiry_threshold_days: u32,
}

impl Default for HttpProbe {
    fn default() -> Self {
        Self {
            url: String::new(),
            timeout: Duration::from_secs(5),
            expected_status: 200,
            follow_redirects: true,
            headers: HashMap::new(),
            check_ssl_expiry: false,
            ssl_expiry_threshold_days: 30,
        }
    }
}

impl HttpProbe {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_timeout(mut self, sec: u64) -> Self {
        self.timeout = Duration::from_secs(sec);
        self
    }

    pub fn expect_status(mut self, code: u16) -> Self {
        self.expected_status = code;
        self
    }

    pub fn check_ssl(mut self, threshold_days: u32) -> Self {
        self.check_ssl_expiry = true;
        self.ssl_expiry_threshold_days = threshold_days;
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Execute the HTTP probe.
    pub async fn run(&self) -> ProbeResult {
        let start = Instant::now();
        let client = match reqwest::Client::builder()
            .timeout(self.timeout)
            .redirect(if self.follow_redirects {
                reqwest::redirect::Policy::default()
            } else {
                reqwest::redirect::Policy::none()
            })
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                return ProbeResult {
                    success: false,
                    duration_ms: 0,
                    status_code: None,
                    ssl_expiry_days: None,
                    error: Some(format!("client build failed: {}", e)),
                };
            }
        };

        let mut req = client.get(&self.url);
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let duration_ms = start.elapsed().as_millis() as u64;
                let ssl_expiry_days = if self.check_ssl_expiry {
                    Self::extract_ssl_expiry(&resp).await
                } else {
                    None
                };

                let success = status == self.expected_status;
                ProbeResult {
                    success,
                    duration_ms,
                    status_code: Some(status),
                    ssl_expiry_days,
                    error: if success {
                        None
                    } else {
                        Some(format!(
                            "status code {} != expected {}",
                            status, self.expected_status
                        ))
                    },
                }
            }
            Err(e) => ProbeResult {
                success: false,
                duration_ms: start.elapsed().as_millis() as u64,
                status_code: None,
                ssl_expiry_days: None,
                error: Some(format!("request failed: {}", e)),
            },
        }
    }

    async fn extract_ssl_expiry(resp: &reqwest::Response) -> Option<u32> {
        // reqwest does not expose peer certificate details in stable API.
        // Phase 2: use rustls::ClientConnection to inspect certs.
        let _ = resp;
        None
    }
}

/// Probe scheduler: runs multiple probes on an interval.
pub struct ProbeScheduler {
    probes: Vec<(HttpProbe, String)>, // (probe, probe_name)
    interval: Duration,
}

impl ProbeScheduler {
    pub fn new(interval: Duration) -> Self {
        Self {
            probes: Vec::new(),
            interval,
        }
    }

    pub fn add_probe(mut self, name: impl Into<String>, probe: HttpProbe) -> Self {
        self.probes.push((probe, name.into()));
        self
    }

    pub async fn run_loop<F>(&self, mut on_result: F)
    where
        F: FnMut(&str, &ProbeResult),
    {
        let mut interval = tokio::time::interval(self.interval);
        loop {
            interval.tick().await;
            for (probe, name) in &self.probes {
                let result = probe.run().await;
                on_result(name, &result);
            }
        }
    }
}

/// Prometheus Pushgateway client.
/// Pushgateway is used for batch / short-lived jobs that cannot be
/// scraped by Prometheus directly.
pub struct PushgatewayClient {
    endpoint: String,
    client: Option<std::sync::Arc<dyn cog_core::HttpClient>>,
    timeout_secs: u64,
}

impl PushgatewayClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: None,
            timeout_secs: 10,
        }
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    pub fn with_client(mut self, client: std::sync::Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Push metrics text to the Pushgateway.
    /// `job` is the job name (required).
    /// `grouping` adds labels to the URL path (e.g. `instance=foo`).
    /// `metrics_text` is standard Prometheus exposition format.
    pub async fn push(
        &self,
        job: &str,
        grouping: &HashMap<String, String>,
        metrics_text: &str,
    ) -> Result<(), anyhow::Error> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PushgatewayClient has no HttpClient configured"))?;
        let mut path = format!("/metrics/job/{}", percent_encode(job));
        for (k, v) in grouping {
            path.push_str(&format!("/{}/{}", percent_encode(k), percent_encode(v)));
        }
        let url = format!("{}{}", self.endpoint.trim_end_matches('/'), path);

        let req = cog_core::HttpRequest::post(&url)
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .body(metrics_text.as_bytes().to_vec())
            .timeout(self.timeout_secs);
        let resp = client.execute(req).await?;

        if !resp.is_success() {
            let body = resp.text().unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Pushgateway returned {}: {}",
                resp.status,
                body
            ));
        }
        Ok(())
    }

    /// Delete metrics for a job from the Pushgateway.
    pub async fn delete(
        &self,
        job: &str,
        grouping: &HashMap<String, String>,
    ) -> Result<(), anyhow::Error> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PushgatewayClient has no HttpClient configured"))?;
        let mut path = format!("/metrics/job/{}", percent_encode(job));
        for (k, v) in grouping {
            path.push_str(&format!("/{}/{}", percent_encode(k), percent_encode(v)));
        }
        let url = format!("{}{}", self.endpoint.trim_end_matches('/'), path);

        let req = cog_core::HttpRequest::delete(&url).timeout(self.timeout_secs);
        let resp = client.execute(req).await?;

        if !resp.is_success() {
            return Err(anyhow::anyhow!(
                "Pushgateway delete returned {}",
                resp.status
            ));
        }
        Ok(())
    }
}

fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    out
}
