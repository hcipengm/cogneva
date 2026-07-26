use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use cog_core::{
    DecayReport, MemoryBackend, MemoryMetrics, RawSource, SchemaEntry, SchemaSearchResult,
    SummaryEntry, SummarySearchResult,
};
use cog_core::{MetricsBackend, SFResult};

/// A [`MemoryBackend`] wrapper that records operation counters and latency
/// histograms into a [`MetricsBackend`].
/// Metric names produced:
/// - Counter `memory_operations_total`  (label `operation`)
/// - Histogram `memory_operation_latency_ms` (label `operation`)
/// - Counter `memory_operation_errors_total` (label `operation`)
///
/// Example Prometheus output after some activity:
/// ```text
/// memory_operations_total{operation="archive_raw"} 5
/// memory_operation_latency_ms_bucket{operation="archive_raw",le="10"} 4
/// ```
pub struct MetricsInstrumentedMemoryBackend {
    inner: Arc<dyn MemoryBackend>,
    metrics: Arc<dyn MetricsBackend>,
}

impl MetricsInstrumentedMemoryBackend {
    pub fn new(inner: Arc<dyn MemoryBackend>, metrics: Arc<dyn MetricsBackend>) -> Self {
        Self { inner, metrics }
    }

    async fn record(&self, operation: &str, start: Instant, is_err: bool) {
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let mut labels = HashMap::new();
        labels.insert("operation".into(), operation.into());

        let _ = self
            .metrics
            .record_counter("memory_operations_total", 1.0, labels.clone())
            .await;
        let _ = self
            .metrics
            .record_histogram("memory_operation_latency_ms", latency_ms, labels.clone())
            .await;

        if is_err {
            let _ = self
                .metrics
                .record_counter("memory_operation_errors_total", 1.0, labels)
                .await;
        }

        crate::observable::global_observable().record_memory_op(latency_ms as u64);
    }
}

#[async_trait]
impl MemoryBackend for MetricsInstrumentedMemoryBackend {
    async fn archive_raw(&self, source: &RawSource) -> SFResult<String> {
        let start = Instant::now();
        let result = self.inner.archive_raw(source).await;
        self.record("archive_raw", start, result.is_err()).await;
        result
    }

    async fn get_raw(&self, namespace: &str, id: &str) -> SFResult<Option<RawSource>> {
        let start = Instant::now();
        let result = self.inner.get_raw(namespace, id).await;
        self.record("get_raw", start, result.is_err()).await;
        result
    }

    async fn list_raw(&self, namespace: &str, prefix: Option<&str>) -> SFResult<Vec<String>> {
        let start = Instant::now();
        let result = self.inner.list_raw(namespace, prefix).await;
        self.record("list_raw", start, result.is_err()).await;
        result
    }

    async fn delete_raw(&self, namespace: &str, id: &str) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.delete_raw(namespace, id).await;
        self.record("delete_raw", start, result.is_err()).await;
        result
    }

    async fn store_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.store_schema(namespace, entry).await;
        self.record("store_schema", start, result.is_err()).await;
        result
    }

    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>> {
        let start = Instant::now();
        let result = self.inner.get_schema(namespace, id).await;
        self.record("get_schema", start, result.is_err()).await;
        result
    }

    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>> {
        let start = Instant::now();
        let result = self.inner.search_schema(namespace, query, limit).await;
        self.record("search_schema", start, result.is_err()).await;
        result
    }

    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>> {
        let start = Instant::now();
        let result = self.inner.schema_for_raw(namespace, raw_id).await;
        self.record("schema_for_raw", start, result.is_err()).await;
        result
    }

    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>> {
        let start = Instant::now();
        let result = self.inner.list_schema(namespace).await;
        self.record("list_schema", start, result.is_err()).await;
        result
    }

    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.delete_schema(namespace, id).await;
        self.record("delete_schema", start, result.is_err()).await;
        result
    }

    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: cog_core::RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>> {
        let start = Instant::now();
        let result = self
            .inner
            .query_relations(namespace, entity, direction, relation_type)
            .await;
        self.record("query_relations", start, result.is_err()).await;
        result
    }

    async fn update_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.update_schema(namespace, entry).await;
        self.record("update_schema", start, result.is_err()).await;
        result
    }

    async fn store_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.store_summary(namespace, entry).await;
        self.record("store_summary", start, result.is_err()).await;
        result
    }

    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>> {
        let start = Instant::now();
        let result = self.inner.get_summary(namespace, id).await;
        self.record("get_summary", start, result.is_err()).await;
        result
    }

    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let start = Instant::now();
        let result = self
            .inner
            .search_summary(namespace, query_embedding, top_k, time_range)
            .await;
        self.record("search_summary", start, result.is_err()).await;
        result
    }

    async fn search_summary_hybrid(
        &self,
        namespace: &str,
        query_dense: &[f32],
        query_sparse: Option<&cog_core::SparseEmbedding>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let start = Instant::now();
        let result = self
            .inner
            .search_summary_hybrid(namespace, query_dense, query_sparse, top_k, time_range)
            .await;
        self.record("search_summary_hybrid", start, result.is_err())
            .await;
        result
    }

    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>> {
        let start = Instant::now();
        let result = self.inner.summary_for_raw(namespace, raw_id).await;
        self.record("summary_for_raw", start, result.is_err()).await;
        result
    }

    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>> {
        let start = Instant::now();
        let result = self.inner.list_summary(namespace).await;
        self.record("list_summary", start, result.is_err()).await;
        result
    }

    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.delete_summary(namespace, id).await;
        self.record("delete_summary", start, result.is_err()).await;
        result
    }

    async fn update_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.update_summary(namespace, entry).await;
        self.record("update_summary", start, result.is_err()).await;
        result
    }

    fn metrics(&self) -> MemoryMetrics {
        self.inner.metrics()
    }

    async fn health_check(&self) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.health_check().await;
        self.record("health_check", start, result.is_err()).await;
        result
    }

    async fn search_all(
        &self,
        namespace: &str,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<cog_core::UnifiedSearchResult>> {
        let start = Instant::now();
        let result = self
            .inner
            .search_all(namespace, query, embedding, top_k, time_range)
            .await;
        self.record("search_all", start, result.is_err()).await;
        result
    }

    async fn ingest_explicit(
        &self,
        namespace: &str,
        text: &str,
        importance: f32,
        tags: Vec<String>,
    ) -> SFResult<()> {
        let start = Instant::now();
        let result = self
            .inner
            .ingest_explicit(namespace, text, importance, tags)
            .await;
        self.record("ingest_explicit", start, result.is_err()).await;
        result
    }

    async fn forget(&self, namespace: &str, id: &str) -> SFResult<()> {
        let start = Instant::now();
        let result = self.inner.forget(namespace, id).await;
        self.record("forget", start, result.is_err()).await;
        result
    }

    async fn decay(
        &self,
        namespace: &str,
        age_threshold_secs: u64,
        importance_threshold: f32,
    ) -> SFResult<DecayReport> {
        let start = Instant::now();
        let result = self
            .inner
            .decay(namespace, age_threshold_secs, importance_threshold)
            .await;
        self.record("decay", start, result.is_err()).await;
        result
    }
}
