//! An ingest that could not store its entries does not report them as stored.
//!
//! `POST /api/v1/memory/ingest` (and its batch sibling) archived the raw bytes,
//! then wrote the schema entries and the summary, and answered `200` with
//! `schema_count` and `summary_id` regardless of what the store said: a failed
//! write was logged as a warning and the reply counted the entries as if they
//! were there. The raw really was archived -- that half of the work happened --
//! so the reply may not say the whole request finished, and it may not report a
//! count of entries a reader would look up and not find.
//!
//! The store below archives raw bytes and refuses every derived write. Every
//! other method panics, so a reply that leans on something this test does not
//! model fails loudly instead of passing by accident.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::Request;
use chrono::{DateTime, Utc};
use tower::ServiceExt;

use cog_core::{
    DecayReport, MemoryBackend, MemoryIngestor, MemoryMetrics, RawSource, RelationDirection,
    SFError, SFResult, SchemaEntry, SchemaKind, SchemaSearchResult, SourceRef, SummaryEntry,
    SummarySearchResult, UnifiedSearchResult,
};
use cog_gateway::create_internal_router;

use crate::router_state;

/// A memory store whose half of the ingest worked: the raw bytes land, the
/// derived entries never do.
struct ArchivesButStoresNothing;

#[async_trait::async_trait]
impl MemoryBackend for ArchivesButStoresNothing {
    async fn archive_raw(&self, source: &RawSource) -> SFResult<String> {
        Ok(format!("mem://{}", source.id))
    }
    async fn get_raw(&self, _ns: &str, _id: &str) -> SFResult<Option<RawSource>> {
        unimplemented!()
    }
    async fn list_raw(&self, _ns: &str, _p: Option<&str>) -> SFResult<Vec<String>> {
        unimplemented!()
    }
    async fn delete_raw(&self, _ns: &str, _id: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn presign_raw(
        &self,
        _ns: &str,
        _id: &str,
        _expiry_secs: u64,
    ) -> SFResult<Option<String>> {
        Ok(None)
    }
    async fn store_schema(&self, _ns: &str, _e: &SchemaEntry) -> SFResult<()> {
        Err(SFError::Database("connection refused".into()))
    }
    async fn get_schema(&self, _ns: &str, _id: &str) -> SFResult<Option<SchemaEntry>> {
        unimplemented!()
    }
    async fn search_schema(
        &self,
        _ns: &str,
        _q: &str,
        _l: usize,
    ) -> SFResult<Vec<SchemaSearchResult>> {
        unimplemented!()
    }
    async fn schema_for_raw(&self, _ns: &str, _id: &str) -> SFResult<Vec<SchemaEntry>> {
        unimplemented!()
    }
    async fn list_schema(&self, _ns: &str) -> SFResult<Vec<SchemaEntry>> {
        unimplemented!()
    }
    async fn delete_schema(&self, _ns: &str, _id: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn query_relations(
        &self,
        _ns: &str,
        _e: &str,
        _d: RelationDirection,
        _t: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>> {
        unimplemented!()
    }
    async fn update_schema(&self, _ns: &str, _e: &SchemaEntry) -> SFResult<()> {
        unimplemented!()
    }
    async fn store_summary(&self, _ns: &str, _e: &SummaryEntry) -> SFResult<()> {
        Err(SFError::Database("connection refused".into()))
    }
    async fn get_summary(&self, _ns: &str, _id: &str) -> SFResult<Option<SummaryEntry>> {
        unimplemented!()
    }
    async fn search_summary(
        &self,
        _ns: &str,
        _q: &[f32],
        _k: usize,
        _r: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        unimplemented!()
    }
    async fn summary_for_raw(&self, _ns: &str, _id: &str) -> SFResult<Vec<SummaryEntry>> {
        unimplemented!()
    }
    async fn list_summary(&self, _ns: &str) -> SFResult<Vec<SummaryEntry>> {
        unimplemented!()
    }
    async fn delete_summary(&self, _ns: &str, _id: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn update_summary(&self, _ns: &str, _e: &SummaryEntry) -> SFResult<()> {
        unimplemented!()
    }
    fn metrics(&self) -> MemoryMetrics {
        MemoryMetrics::default()
    }
    async fn health_check(&self) -> SFResult<()> {
        Ok(())
    }
    async fn search_all(
        &self,
        _ns: &str,
        _q: &str,
        _e: Option<&[f32]>,
        _k: usize,
        _r: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<UnifiedSearchResult>> {
        unimplemented!()
    }
    async fn ingest_explicit(
        &self,
        _ns: &str,
        _t: &str,
        _i: f32,
        _tags: Vec<String>,
    ) -> SFResult<()> {
        unimplemented!()
    }
    async fn forget(&self, _ns: &str, _id: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn record_retrievals(&self, _s: &[String], _u: &[String]) -> SFResult<()> {
        unimplemented!()
    }
    async fn decay(&self, _ns: &str, _age: u64, _imp: f32) -> SFResult<DecayReport> {
        unimplemented!()
    }
}

/// Extraction succeeds; the entries it produces are what the store refuses.
struct ExtractsOneEntry;

#[async_trait::async_trait]
impl MemoryIngestor for ExtractsOneEntry {
    async fn ingest(&self, source: &RawSource) -> SFResult<(Vec<SchemaEntry>, SummaryEntry)> {
        let schema = SchemaEntry::new(
            "entry-1",
            source.namespace.clone(),
            SchemaKind::Entity,
            "a fact",
            "fact",
            SourceRef::new(format!("mem://{}", source.id), "test"),
        );
        let summary = SummaryEntry::new(
            "summary-1",
            source.namespace.clone(),
            "a summary",
            vec![],
            "test",
            SourceRef::new(format!("mem://{}", source.id), "test"),
        );
        Ok((vec![schema], summary))
    }
}

fn state() -> Arc<cog_gateway::GatewayState> {
    router_state::with_memory_pipeline(
        Arc::new(ArchivesButStoresNothing),
        Arc::new(ExtractsOneEntry),
    )
}

fn ingest_request(body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/memory/ingest")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("the request is well formed")
}

/// 单条入口：原文归档了，派生条目一条也没写进去，答复不许说这次学习完成了。
#[tokio::test]
async fn a_learn_ingest_the_store_refused_does_not_report_its_entries() {
    let response = create_internal_router(state())
        .oneshot(ingest_request(serde_json::json!({
            "id": "raw-1",
            "text": "the payload",
            "purpose": "learn",
        })))
        .await
        .expect("the router answers a request");

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    let body = String::from_utf8_lossy(&bytes).into_owned();

    assert!(
        !status.is_success(),
        "nothing derived was stored, so the request did not finish: {status} {body}"
    );
    assert!(
        !body.contains("\"schema_count\"") && !body.contains("\"summary_id\""),
        "the reply may not hand out counts of entries nobody stored: {body}"
    );
}

/// 批量入口：一条没存成，它就不在 `results` 里，而在 `errors` 里带着原因。
///
/// 这条路由挂在登录面（单条那条挂在零凭证的内部面上），所以要在启动它的那个
/// router 上问，并且带上一个调用方的令牌。
#[tokio::test]
async fn a_batch_ingest_the_store_refused_reports_the_item_as_an_error() {
    let state = state();
    let token = router_state::admin_bearer(&state, "memory-ingest-probe").await;
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/memory/ingest/batch")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "items": [{ "id": "raw-1", "text": "the payload", "purpose": "learn" }]
                    })
                    .to_string(),
                ))
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");

    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("the batch reply is JSON");

    assert_eq!(
        body["results"].as_array().map(|a| a.len()),
        Some(0),
        "an item whose entries were never stored may not appear among the results: {body}"
    );
    let errors = body["errors"]
        .as_array()
        .expect("the reply has an errors list");
    assert_eq!(
        errors.first().and_then(|e| e["id"].as_str()),
        Some("raw-1"),
        "the item that did not land has to be named in the errors list: {body}"
    );
    assert!(
        errors
            .first()
            .and_then(|e| e["error"].as_str())
            .map(|s| s.contains("原文已归档到") && s.contains("schema"))
            .unwrap_or(false),
        "the error has to say what landed and what did not, not only that something failed: {body}"
    );
}
