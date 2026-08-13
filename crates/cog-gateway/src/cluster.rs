//! Cluster component health — reports the three fixed deployments that make
//! up a Cogneva install (main app, security gateway, evolution sandbox) so
//! the topology view can render real nodes instead of placeholders.
//!
//! The main app is the process serving this request, so it is always "up".
//! The other two are read from the Kubernetes API; when the service account
//! lacks permission (or we are not in a cluster) they degrade to "unknown".

use axum::Json;
use serde::Serialize;

use crate::llm_admin::KubeClient;

#[derive(Debug, Serialize)]
pub struct ComponentStatus {
    /// Stable identifier consumed by the WebUI: main / security-gateway / evolution.
    pub id: String,
    /// up / down / unknown.
    pub status: String,
    pub ready_replicas: Option<u32>,
    pub desired_replicas: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ClusterComponents {
    pub components: Vec<ComponentStatus>,
}

async fn deployment_status(client: &KubeClient, id: &str, deployment: &str) -> ComponentStatus {
    let path = format!(
        "/apis/apps/v1/namespaces/{}/deployments/{}",
        client.namespace(),
        deployment
    );
    match client.get_json(&path).await {
        Ok(dep) => {
            let desired = dep
                .pointer("/spec/replicas")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32)
                .unwrap_or(1);
            let ready = dep
                .pointer("/status/readyReplicas")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32)
                .unwrap_or(0);
            ComponentStatus {
                id: id.to_string(),
                status: if ready > 0 { "up" } else { "down" }.to_string(),
                ready_replicas: Some(ready),
                desired_replicas: Some(desired),
            }
        }
        Err(_) => ComponentStatus {
            id: id.to_string(),
            status: "unknown".to_string(),
            ready_replicas: None,
            desired_replicas: None,
        },
    }
}

pub async fn cluster_components_handler() -> Json<ClusterComponents> {
    let mut components = vec![ComponentStatus {
        id: "main".to_string(),
        status: "up".to_string(),
        ready_replicas: None,
        desired_replicas: None,
    }];

    if let Ok(client) = KubeClient::in_cluster() {
        components
            .push(deployment_status(&client, "security-gateway", "cogneva-security-gateway").await);
        components.push(deployment_status(&client, "evolution", "cogneva-evolution").await);
    } else {
        for id in ["security-gateway", "evolution"] {
            components.push(ComponentStatus {
                id: id.to_string(),
                status: "unknown".to_string(),
                ready_replicas: None,
                desired_replicas: None,
            });
        }
    }

    Json(ClusterComponents { components })
}
