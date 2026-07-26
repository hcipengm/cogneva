use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

/// Structured status snapshot sent to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupervisorStatus {
    pub cycle: u64,
    pub healthy_agents: usize,
    pub dead_agents: usize,
    pub pending_handoffs: usize,
    pub last_rebalance: Option<DateTime<Utc>>,
    pub timestamp: DateTime<Utc>,
}

/// Trait for reporting supervisor status to an external control plane.
#[async_trait]
pub trait ControlPlaneClient: Send + Sync {
    async fn report_status(
        &self,
        status: SupervisorStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// HTTP control plane client that POSTs JSON status to an endpoint.
pub struct HttpControlPlaneClient {
    client: Option<Arc<dyn cog_core::HttpClient>>,
    endpoint: String,
}

impl HttpControlPlaneClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            client: None,
            endpoint: endpoint.into(),
        }
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn client(
        &self,
    ) -> Result<&Arc<dyn cog_core::HttpClient>, Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .as_ref()
            .ok_or_else(|| "HttpControlPlaneClient has no HttpClient configured".into())
    }
}

#[async_trait]
impl ControlPlaneClient for HttpControlPlaneClient {
    async fn report_status(
        &self,
        status: SupervisorStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let req = cog_core::HttpRequest::post(&self.endpoint)
            .json(&status)
            .map_err(|e| format!("JSON serialization failed: {}", e))?
            .timeout(30);
        let resp = self
            .client()?
            .execute(req)
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if resp.is_success() {
            info!("Control plane status reported successfully");
            Ok(())
        } else {
            let status_code = resp.status;
            let body = resp.text().unwrap_or_default();
            warn!(
                "Control plane returned non-success status: {} — body: {}",
                status_code, body
            );
            Err(format!("control plane returned {}", status_code).into())
        }
    }
}

/// No-op control plane client for tests or disabled reporting.
pub struct NoopControlPlaneClient;

#[async_trait]
impl ControlPlaneClient for NoopControlPlaneClient {
    async fn report_status(
        &self,
        _status: SupervisorStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_client_always_succeeds() {
        let client = NoopControlPlaneClient;
        let status = SupervisorStatus {
            cycle: 1,
            healthy_agents: 2,
            dead_agents: 0,
            pending_handoffs: 3,
            last_rebalance: Some(Utc::now()),
            timestamp: Utc::now(),
        };
        let result = client.report_status(status).await;
        assert!(result.is_ok());
    }
}
