//! No-op backends for graceful degradation when StoragePlugin is unavailable.

use async_trait::async_trait;
use cog_core::{MetricsBackend, SFResult, VectorBackend, VectorSearchResult};
use serde_json::Value;
use std::collections::HashMap;

/// No-op metrics backend that silently discards all recordings.
pub struct NoopMetricsBackend;

impl Default for NoopMetricsBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl NoopMetricsBackend {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MetricsBackend for NoopMetricsBackend {
    async fn record_gauge(
        &self,
        _name: &str,
        _value: f64,
        _labels: HashMap<String, String>,
    ) -> SFResult<()> {
        Ok(())
    }

    async fn record_counter(
        &self,
        _name: &str,
        _value: f64,
        _labels: HashMap<String, String>,
    ) -> SFResult<()> {
        Ok(())
    }

    async fn record_histogram(
        &self,
        _name: &str,
        _value: f64,
        _labels: HashMap<String, String>,
    ) -> SFResult<()> {
        Ok(())
    }

    async fn query_gauge_range(
        &self,
        _name: &str,
        _start: chrono::DateTime<chrono::Utc>,
        _end: chrono::DateTime<chrono::Utc>,
    ) -> SFResult<Vec<cog_core::MetricSample>> {
        Ok(Vec::new())
    }

    async fn query_counter_range(
        &self,
        _name: &str,
        _start: chrono::DateTime<chrono::Utc>,
        _end: chrono::DateTime<chrono::Utc>,
    ) -> SFResult<Vec<cog_core::MetricSample>> {
        Ok(Vec::new())
    }

    async fn query_histogram_range(
        &self,
        _name: &str,
        _start: chrono::DateTime<chrono::Utc>,
        _end: chrono::DateTime<chrono::Utc>,
    ) -> SFResult<Vec<cog_core::MetricSample>> {
        Ok(Vec::new())
    }

    async fn health_check(&self) -> SFResult<()> {
        Ok(())
    }
}

/// No-op vector backend that returns empty results for all operations.
pub struct NoopVectorBackend;

impl Default for NoopVectorBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl NoopVectorBackend {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl VectorBackend for NoopVectorBackend {
    async fn create_collection(&self, _collection: &str, _dimension: usize) -> SFResult<()> {
        Ok(())
    }

    async fn delete_collection(&self, _collection: &str) -> SFResult<()> {
        Ok(())
    }

    async fn insert(
        &self,
        _collection: &str,
        _vectors: Vec<Vec<f32>>,
        _metadata: Vec<Value>,
    ) -> SFResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn insert_sparse(
        &self,
        _collection: &str,
        _sparse: Vec<cog_core::SparseEmbedding>,
        _metadata: Vec<Value>,
    ) -> SFResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn search(
        &self,
        _collection: &str,
        _vector: &[f32],
        _top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        Ok(Vec::new())
    }

    async fn delete(&self, _collection: &str, _ids: &[String]) -> SFResult<()> {
        Ok(())
    }

    async fn collection_exists(&self, _collection: &str) -> SFResult<bool> {
        Ok(false)
    }
}
