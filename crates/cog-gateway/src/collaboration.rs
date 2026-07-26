use axum::{
    extract::{Path, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{error::ApiError, GatewayState};

/// A directed link in the Agent collaboration graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollaborationLink {
    pub source_task_id: String,
    pub target_task_id: String,
    pub link_type: CollaborationLinkType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationLinkType {
    /// Task A completed -> Task B scheduled (DAG dependency or hand-off).
    HandOff,
    /// Task failed and was retried.
    Retry,
    /// Task failed permanently and moved to dead-letter queue.
    DeadLetter,
    /// Agent was respawned to continue a task.
    Respawn,
}

/// In-memory collaboration graph.
/// Updated by a background task listening to `TaskEvent`s.
#[derive(Default)]
pub struct CollaborationGraph {
    links: RwLock<Vec<CollaborationLink>>,
    /// Index: task_id -> list of link indices for fast lookup.
    index: RwLock<HashMap<String, Vec<usize>>>,
}

impl CollaborationGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_link(&self, link: CollaborationLink) {
        let mut links = self.links.write().await;
        let mut index = self.index.write().await;
        let idx = links.len();
        index
            .entry(link.source_task_id.clone())
            .or_default()
            .push(idx);
        index
            .entry(link.target_task_id.clone())
            .or_default()
            .push(idx);
        links.push(link);
    }

    pub async fn all_links(&self) -> Vec<CollaborationLink> {
        self.links.read().await.clone()
    }

    pub async fn links_for_task(&self, task_id: &str) -> Vec<CollaborationLink> {
        let links = self.links.read().await;
        let index = self.index.read().await;
        index
            .get(task_id)
            .map(|indices| indices.iter().map(|i| links[*i].clone()).collect())
            .unwrap_or_default()
    }

    pub async fn upstream_links(&self, task_id: &str) -> Vec<CollaborationLink> {
        self.links_for_task(task_id)
            .await
            .into_iter()
            .filter(|l| l.target_task_id == task_id)
            .collect()
    }

    pub async fn downstream_links(&self, task_id: &str) -> Vec<CollaborationLink> {
        self.links_for_task(task_id)
            .await
            .into_iter()
            .filter(|l| l.source_task_id == task_id)
            .collect()
    }

    pub async fn stats(&self) -> CollaborationStats {
        let links = self.links.read().await;
        let mut handoffs = 0usize;
        let mut retries = 0usize;
        let mut dead_letters = 0usize;
        let mut respawns = 0usize;
        for link in links.iter() {
            match link.link_type {
                CollaborationLinkType::HandOff => handoffs += 1,
                CollaborationLinkType::Retry => retries += 1,
                CollaborationLinkType::DeadLetter => dead_letters += 1,
                CollaborationLinkType::Respawn => respawns += 1,
            }
        }
        CollaborationStats {
            total_links: links.len(),
            handoffs,
            retries,
            dead_letters,
            respawns,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CollaborationStats {
    pub total_links: usize,
    pub handoffs: usize,
    pub retries: usize,
    pub dead_letters: usize,
    pub respawns: usize,
}

#[derive(Debug, Serialize)]
pub struct CollaborationGraphResponse {
    pub links: Vec<CollaborationLink>,
    pub stats: CollaborationStats,
}

#[derive(Debug, Serialize)]
pub struct TaskCollaborationResponse {
    pub task_id: String,
    pub upstream: Vec<CollaborationLink>,
    pub downstream: Vec<CollaborationLink>,
}

pub async fn collaboration_graph_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<CollaborationGraphResponse>, ApiError> {
    let graph = state
        .collaboration_graph
        .as_ref()
        .ok_or_else(|| ApiError::not_found("collaboration graph not initialized"))?;
    let links = graph.all_links().await;
    let stats = graph.stats().await;
    Ok(Json(CollaborationGraphResponse { links, stats }))
}

pub async fn task_collaboration_handler(
    State(state): State<Arc<GatewayState>>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskCollaborationResponse>, ApiError> {
    let graph = state
        .collaboration_graph
        .as_ref()
        .ok_or_else(|| ApiError::not_found("collaboration graph not initialized"))?;
    let upstream = graph.upstream_links(&task_id).await;
    let downstream = graph.downstream_links(&task_id).await;
    Ok(Json(TaskCollaborationResponse {
        task_id,
        upstream,
        downstream,
    }))
}
