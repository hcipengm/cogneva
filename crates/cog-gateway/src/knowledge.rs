//! Knowledge-layer REST API handler.
//!
//! One endpoint: ask the assembled knowledge layer a question. It exists
//! because that layer had no caller outside a running squad — the retrieval
//! pipeline (hybrid memory search, the wiki store, and the reranker that
//! reorders the two together) could only be reached through the collaboration
//! actors, so a deployment where no squad runs could not have its retrieval
//! exercised at all, and "nothing asked" was indistinguishable from "the
//! pipeline is not connected".
//!
//! The route is the composition, not the stores: `/api/v1/memory/search` and
//! `/api/v1/wiki/search` each answer with what one store holds, while this one
//! is what a retrieval actually consults. A request therefore costs one
//! embedding, one memory search over both halves of the hybrid vector, one
//! wiki search, and one reranking pass — which is also why the retrieval
//! outcome series is written here and not by either store route.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

use crate::GatewayState;

fn default_top_k() -> usize {
    10
}

/// The widest answer this route will assemble. Same ceiling the wiki search
/// applies, because both are asking a store to return a page of its contents
/// over HTTP rather than sizing an internal candidate window.
const MAX_TOP_K: usize = 100;

#[derive(Debug, Deserialize)]
pub struct KnowledgeSearchRequest {
    /// The question, in the same form a caller would ask the layer directly.
    pub query: String,
    /// How many entries the caller wants back.
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

#[derive(Debug, Serialize)]
pub struct KnowledgeHit {
    pub id: String,
    /// Which layer answered, as the retrieval names it (`memory:summary:<ns>`,
    /// `memory:schema:<ns>`, `wiki`). Worth carrying: an empty answer and an
    /// answer where one layer never spoke look the same in `results`.
    pub source: String,
    pub title: String,
    pub content: String,
    pub relevance_score: f32,
}

#[derive(Debug, Serialize)]
pub struct KnowledgeSearchResponse {
    pub results: Vec<KnowledgeHit>,
    pub count: usize,
}

/// `POST /api/v1/knowledge/search` — retrieve knowledge relevant to a query.
pub async fn search_handler(
    State(state): State<Arc<GatewayState>>,
    _claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<KnowledgeSearchRequest>,
) -> Response {
    search(state.knowledge_backend.as_ref(), req).await
}

/// The route's behaviour, separated from the extractors so it can be called
/// with a backend the caller chooses.
///
/// An absent backend answers 503 rather than an empty result set: a layer that
/// was never published and a layer that answered "nothing matches" are
/// different facts, and one empty list would report the second for both.
async fn search(
    backend: Option<&Arc<dyn cog_core::KnowledgeBackend>>,
    req: KnowledgeSearchRequest,
) -> Response {
    let Some(backend) = backend else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "knowledge backend disabled"})),
        )
            .into_response();
    };

    if req.query.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "query must not be empty"})),
        )
            .into_response();
    }

    let top_k = req.top_k.clamp(1, MAX_TOP_K);
    // The retrieval takes a task because its callers have one and build the
    // query key from it. Here there is no task: the query is the whole input.
    let task = cog_core::Task::new(
        "knowledge-search".to_string(),
        cog_core::TaskType::Custom("knowledge_search".into()),
        json!({ "goal": req.query }),
    );

    match backend.retrieve_relevant(&task, &req.query, top_k).await {
        Ok(entries) => {
            let count = entries.len();
            let results = entries.into_iter().map(hit_from).collect();
            (
                StatusCode::OK,
                Json(KnowledgeSearchResponse { results, count }),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("knowledge search failed: {}", e)})),
        )
            .into_response(),
    }
}

fn hit_from(entry: cog_core::KnowledgeEntry) -> KnowledgeHit {
    KnowledgeHit {
        id: entry.id,
        source: entry.source,
        title: entry.title,
        content: entry.content,
        relevance_score: entry.relevance_score,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// Answers with whatever it was handed, and records the question so the
    /// test can check what the route actually asked for.
    struct RecordingBackend {
        entries: Vec<cog_core::KnowledgeEntry>,
        asked: Mutex<Vec<(String, usize)>>,
    }

    impl RecordingBackend {
        fn new(entries: Vec<cog_core::KnowledgeEntry>) -> Arc<Self> {
            Arc::new(Self {
                entries,
                asked: Mutex::new(Vec::new()),
            })
        }
    }

    fn entry(id: &str, score: f32) -> cog_core::KnowledgeEntry {
        cog_core::KnowledgeEntry {
            id: id.to_string(),
            source: "memory:summary:knowledge".to_string(),
            title: "knowledge".to_string(),
            content: format!("text of {id}"),
            relevance_score: score,
            metadata: None,
        }
    }

    #[async_trait]
    impl cog_core::KnowledgeBackend for RecordingBackend {
        async fn retrieve_relevant(
            &self,
            _task: &cog_core::Task,
            query: &str,
            top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::KnowledgeEntry>> {
            self.asked.lock().unwrap().push((query.to_string(), top_k));
            Ok(self.entries.clone())
        }

        async fn retrieve_similar_decompositions(
            &self,
            _goal_class: &str,
            _goal: &str,
            _top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::TaskDecompositionPattern>> {
            Ok(Vec::new())
        }

        async fn retrieve_similar_implementations(
            &self,
            _task_type: &str,
            _input_summary: &str,
            _top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::ImplementationExample>> {
            Ok(Vec::new())
        }

        async fn retrieve_failure_patterns(
            &self,
            _task_type: &str,
            _top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::FailurePattern>> {
            Ok(Vec::new())
        }

        async fn retrieve_task_history(
            &self,
            _task_id: &str,
        ) -> cog_core::SFResult<Vec<cog_core::TaskExecutionRecord>> {
            Ok(Vec::new())
        }

        async fn archive_execution(
            &self,
            _task: &cog_core::Task,
            _result: &cog_core::TaskResult,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn archive_decomposition(
            &self,
            _task: &cog_core::Task,
            _sub_task_types: &[String],
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    async fn body_of(response: Response) -> (StatusCode, serde_json::Value) {
        use axum::body::to_bytes;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    fn request(query: &str, top_k: usize) -> KnowledgeSearchRequest {
        KnowledgeSearchRequest {
            query: query.to_string(),
            top_k,
        }
    }

    #[tokio::test]
    async fn an_absent_layer_is_a_service_error_not_an_empty_answer() {
        let (status, body) = body_of(search(None, request("anything", 5)).await).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "a layer that was never published must not read as one that found nothing"
        );
        assert_eq!(body["error"], "knowledge backend disabled");
    }

    #[tokio::test]
    async fn an_empty_query_is_rejected_before_the_backend_is_asked() {
        let backend = RecordingBackend::new(vec![]);
        let as_dyn: Arc<dyn cog_core::KnowledgeBackend> = backend.clone();
        let (status, _) = body_of(search(Some(&as_dyn), request("   ", 5)).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            backend.asked.lock().unwrap().is_empty(),
            "a rejected query must not reach the retrieval"
        );
    }

    #[tokio::test]
    async fn the_answer_keeps_the_retrievals_order_and_its_own_width() {
        let backend = RecordingBackend::new(vec![entry("b", 0.9), entry("a", 0.4)]);
        let as_dyn: Arc<dyn cog_core::KnowledgeBackend> = backend.clone();
        let (status, body) =
            body_of(search(Some(&as_dyn), request("why did it hang", 2)).await).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["count"], 2);
        assert_eq!(
            body["results"][0]["id"], "b",
            "the reranked order is the answer's order"
        );
        assert_eq!(body["results"][0]["source"], "memory:summary:knowledge");
        assert_eq!(
            backend.asked.lock().unwrap().as_slice(),
            &[("why did it hang".to_string(), 2)],
            "the query reaches the retrieval unchanged and carries the caller's width"
        );
    }

    #[tokio::test]
    async fn the_width_is_clamped_and_never_zero() {
        let backend = RecordingBackend::new(vec![]);
        let as_dyn: Arc<dyn cog_core::KnowledgeBackend> = backend.clone();
        let _ = body_of(search(Some(&as_dyn), request("q", 0)).await).await;
        let _ = body_of(search(Some(&as_dyn), request("q", 10_000)).await).await;

        let asked = backend.asked.lock().unwrap().clone();
        assert_eq!(
            asked,
            vec![("q".to_string(), 1), ("q".to_string(), MAX_TOP_K)],
            "zero would ask for no rows and an unbounded width would ask a store for everything"
        );
    }
}
