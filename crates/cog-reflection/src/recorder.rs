//! Persistent recording of learning entries.
//! The [`LearningRecorder`] trait abstracts over storage backends so that
//! learnings can be archived in the same three-layer memory system as
//! regular agent memories (raw → schema → summary).

use std::sync::Arc;

use async_trait::async_trait;
use cog_core::SFResult;
use tracing::{debug, error, info};

use crate::types::{FeatureRequest, LearningFilter};
use cog_core::{ErrorEntry, Learning, LearningStatus, Resolution};

use cog_core::{RawSource, SourceRef};

/// Abstract recorder for learning entries.
#[async_trait]
pub trait LearningRecorder: Send + Sync {
    /// Persist a new or updated [`Learning`].
    async fn record_learning(&self, learning: Learning) -> SFResult<()>;

    /// Persist a new or updated [`ErrorEntry`].
    async fn record_error(&self, error: ErrorEntry) -> SFResult<()>;

    /// Persist a new or updated [`FeatureRequest`].
    async fn record_feature_request(&self, request: FeatureRequest) -> SFResult<()>;

    /// Resolve an existing entry by ID.
    async fn resolve(&self, id: &str, resolution: Resolution) -> SFResult<()>;

    /// Retrieve a single learning by ID.
    async fn get_learning(&self, id: &str) -> SFResult<Option<Learning>>;

    /// List learnings matching the optional filter.
    async fn list_learnings(&self, filter: Option<LearningFilter>) -> SFResult<Vec<Learning>>;
}

/// In-memory recorder backed by `tokio::sync::RwLock<Vec>`.
/// Suitable for testing and for Phase 1 when persistence requirements
/// are light. Production deployments should migrate to
/// [`MemoryBackendRecorder`].
#[derive(Debug, Clone)]
pub struct InMemoryRecorder {
    learnings: Arc<tokio::sync::RwLock<Vec<Learning>>>,
    errors: Arc<tokio::sync::RwLock<Vec<ErrorEntry>>>,
    features: Arc<tokio::sync::RwLock<Vec<FeatureRequest>>>,
}

impl Default for InMemoryRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRecorder {
    pub fn new() -> Self {
        Self {
            learnings: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            errors: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            features: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl LearningRecorder for InMemoryRecorder {
    async fn record_learning(&self, learning: Learning) -> SFResult<()> {
        let mut guard = self.learnings.write().await;
        // Update in-place if the ID already exists.
        if let Some(pos) = guard.iter().position(|l| l.id == learning.id) {
            guard[pos] = learning.clone();
            debug!("updated learning {}", learning.id);
        } else {
            guard.push(learning.clone());
            info!("recorded learning {}", learning.id);
        }
        Ok(())
    }

    async fn record_error(&self, error: ErrorEntry) -> SFResult<()> {
        let mut guard = self.errors.write().await;
        if let Some(pos) = guard.iter().position(|e| e.id == error.id) {
            guard[pos] = error.clone();
            debug!("updated error {}", error.id);
        } else {
            guard.push(error.clone());
            info!("recorded error {}", error.id);
        }
        Ok(())
    }

    async fn record_feature_request(&self, request: FeatureRequest) -> SFResult<()> {
        let mut guard = self.features.write().await;
        if let Some(pos) = guard.iter().position(|f| f.id == request.id) {
            guard[pos] = request.clone();
            debug!("updated feature request {}", request.id);
        } else {
            guard.push(request.clone());
            info!("recorded feature request {}", request.id);
        }
        Ok(())
    }

    async fn resolve(&self, id: &str, resolution: Resolution) -> SFResult<()> {
        let mut found = false;

        {
            let mut guard = self.learnings.write().await;
            if let Some(l) = guard.iter_mut().find(|l| l.id == id) {
                l.status = match resolution {
                    Resolution::Resolved { .. } => LearningStatus::Resolved,
                    Resolution::WontFix { .. } => LearningStatus::WontFix,
                };
                found = true;
            }
        }

        if !found {
            let mut guard = self.errors.write().await;
            if let Some(e) = guard.iter_mut().find(|e| e.id == id) {
                e.status = match resolution {
                    Resolution::Resolved { .. } => LearningStatus::Resolved,
                    Resolution::WontFix { .. } => LearningStatus::WontFix,
                };
                found = true;
            }
        }

        if !found {
            let mut guard = self.features.write().await;
            if let Some(f) = guard.iter_mut().find(|f| f.id == id) {
                f.status = match resolution {
                    Resolution::Resolved { .. } => LearningStatus::Resolved,
                    Resolution::WontFix { .. } => LearningStatus::WontFix,
                };
                found = true;
            }
        }

        if found {
            info!("resolved entry {}", id);
            Ok(())
        } else {
            error!("cannot resolve unknown entry {}", id);
            Err(cog_core::SFError::Validation(format!(
                "Learning/Error/Feature with id {} not found",
                id
            )))
        }
    }

    async fn get_learning(&self, id: &str) -> SFResult<Option<Learning>> {
        let guard = self.learnings.read().await;
        Ok(guard.iter().find(|l| l.id == id).cloned())
    }

    async fn list_learnings(&self, filter: Option<LearningFilter>) -> SFResult<Vec<Learning>> {
        let guard = self.learnings.read().await;
        let results: Vec<Learning> = guard
            .iter()
            .filter(|l| matches_learning_filter(l, filter.as_ref()))
            .cloned()
            .collect();
        Ok(results)
    }
}

/// Whether a learning satisfies a filter. `None` matches everything.
///
/// One definition shared by every recorder: a second copy of this predicate
/// drifts, and a dropped clause is invisible to callers, which only see a
/// short list they have no way to tell apart from an empty result.
fn matches_learning_filter(learning: &Learning, filter: Option<&LearningFilter>) -> bool {
    let Some(f) = filter else {
        return true;
    };
    f.status.as_ref().is_none_or(|s| *s == learning.status)
        && f.priority.as_ref().is_none_or(|p| *p == learning.priority)
        && f.area.as_ref().is_none_or(|a| *a == learning.area)
        && f.category.as_ref().is_none_or(|c| *c == learning.category)
        && f.source.as_ref().is_none_or(|s| *s == learning.source)
        && f.pattern_key
            .as_ref()
            .is_none_or(|pk| learning.pattern_key.as_ref() == Some(pk))
        && f.tags.iter().all(|t| learning.tags.contains(t))
        && f.since.is_none_or(|since| learning.last_seen >= since)
        && f.until.is_none_or(|until| learning.last_seen <= until)
}

/// Recorder backed by [`cog_core::MemoryBackend`].
/// Stores learnings as schema entries so they participate in the
/// three-layer memory pipeline (raw → schema → summary).
/// **Phase 2** implementation — depends on `cog-memory`.
pub struct MemoryBackendRecorder {
    backend: Arc<dyn cog_core::MemoryBackend>,
    namespace: String,
}

impl std::fmt::Debug for MemoryBackendRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBackendRecorder")
            .field("backend", &"<dyn MemoryBackend>")
            .field("namespace", &self.namespace)
            .finish()
    }
}

impl Clone for MemoryBackendRecorder {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            namespace: self.namespace.clone(),
        }
    }
}

impl MemoryBackendRecorder {
    pub fn new(backend: Arc<dyn cog_core::MemoryBackend>, namespace: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
        }
    }
}

#[async_trait]
impl LearningRecorder for MemoryBackendRecorder {
    async fn record_learning(&self, learning: Learning) -> SFResult<()> {
        let raw = RawSource::new(
            learning.id.clone(),
            &self.namespace,
            "application/json",
            serde_json::to_vec(&learning).map_err(cog_core::SFError::Serialization)?,
        );

        self.backend.archive_raw(&raw).await?;

        let source_ref = SourceRef::new(
            format!("memory://{}/{}", self.namespace, learning.id),
            "cog-reflection/1.0",
        );

        let schema = cog_core::SchemaEntry::new(
            learning.id.clone(),
            &self.namespace,
            cog_core::SchemaKind::Learning,
            &learning.summary,
            &learning.id,
            source_ref,
        )
        .with_properties(serde_json::to_value(&learning).map_err(cog_core::SFError::Serialization)?)
        .with_importance(match learning.priority {
            cog_core::Priority::Critical => 1.0,
            cog_core::Priority::High => 0.8,
            cog_core::Priority::Medium => 0.5,
            cog_core::Priority::Low => 0.3,
        });

        self.backend.store_schema(&self.namespace, &schema).await?;
        info!("persisted learning {} to memory backend", learning.id);
        Ok(())
    }

    async fn record_error(&self, error: ErrorEntry) -> SFResult<()> {
        let raw = RawSource::new(
            error.id.clone(),
            &self.namespace,
            "application/json",
            serde_json::to_vec(&error).map_err(cog_core::SFError::Serialization)?,
        );
        self.backend.archive_raw(&raw).await?;

        let source_ref = SourceRef::new(
            format!("memory://{}/{}", self.namespace, error.id),
            "cog-reflection/1.0",
        );

        let schema = cog_core::SchemaEntry::new(
            error.id.clone(),
            &self.namespace,
            cog_core::SchemaKind::ErrorPattern,
            &error.error_message,
            &error.id,
            source_ref,
        )
        .with_properties(serde_json::to_value(&error).map_err(cog_core::SFError::Serialization)?)
        .with_importance(match error.priority {
            cog_core::Priority::Critical => 1.0,
            cog_core::Priority::High => 0.8,
            cog_core::Priority::Medium => 0.5,
            cog_core::Priority::Low => 0.3,
        });

        self.backend.store_schema(&self.namespace, &schema).await?;
        info!("persisted error {} to memory backend", error.id);
        Ok(())
    }

    async fn record_feature_request(&self, request: FeatureRequest) -> SFResult<()> {
        let raw = RawSource::new(
            request.id.clone(),
            &self.namespace,
            "application/json",
            serde_json::to_vec(&request).map_err(cog_core::SFError::Serialization)?,
        );
        self.backend.archive_raw(&raw).await?;

        let source_ref = SourceRef::new(
            format!("memory://{}/{}", self.namespace, request.id),
            "cog-reflection/1.0",
        );

        let schema = cog_core::SchemaEntry::new(
            request.id.clone(),
            &self.namespace,
            cog_core::SchemaKind::Custom,
            &request.capability,
            &request.id,
            source_ref,
        )
        .with_properties(serde_json::to_value(&request).map_err(cog_core::SFError::Serialization)?);

        self.backend.store_schema(&self.namespace, &schema).await?;
        info!("persisted feature request {} to memory backend", request.id);
        Ok(())
    }

    async fn resolve(&self, id: &str, _resolution: Resolution) -> SFResult<()> {
        info!("would resolve entry {} in memory backend (Phase 2)", id);
        Ok(())
    }

    async fn get_learning(&self, id: &str) -> SFResult<Option<Learning>> {
        let schema = self.backend.get_schema(&self.namespace, id).await?;
        match schema {
            Some(s) => {
                let learning: Learning = serde_json::from_value(s.properties)
                    .map_err(cog_core::SFError::Serialization)?;
                Ok(Some(learning))
            }
            None => Ok(None),
        }
    }

    async fn list_learnings(&self, filter: Option<LearningFilter>) -> SFResult<Vec<Learning>> {
        let schemas = self.backend.list_schema(&self.namespace).await?;
        let mut learnings = Vec::with_capacity(schemas.len());
        for s in schemas {
            // The namespace also holds error patterns; they carry a different
            // shape, so a failed decode means "not a learning", not a fault.
            if let Ok(l) = serde_json::from_value::<Learning>(s.properties) {
                if matches_learning_filter(&l, filter.as_ref()) {
                    learnings.push(l);
                }
            }
        }
        Ok(learnings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::contract::memory::{
        MemoryMetrics, RelationDirection, SchemaSearchResult, SummaryEntry, SummarySearchResult,
        UnifiedSearchResult,
    };
    use cog_core::{Area, LearningCategory, LearningSource, Priority};
    use std::sync::Mutex;

    /// A `MemoryBackend` that only serves the schema layer.
    /// The other layers exist so the trait can be implemented; the recorder
    /// under test never reaches them.
    #[derive(Default)]
    struct SchemaOnlyBackend {
        schemas: Mutex<Vec<cog_core::SchemaEntry>>,
    }

    impl SchemaOnlyBackend {
        fn with(entries: Vec<cog_core::SchemaEntry>) -> Self {
            Self {
                schemas: Mutex::new(entries),
            }
        }
    }

    fn unsupported<T>() -> SFResult<T> {
        Err(cog_core::SFError::Validation(
            "not exercised by this fake".into(),
        ))
    }

    #[async_trait]
    impl cog_core::MemoryBackend for SchemaOnlyBackend {
        async fn archive_raw(&self, _source: &RawSource) -> SFResult<String> {
            unsupported()
        }
        async fn get_raw(&self, _ns: &str, _id: &str) -> SFResult<Option<RawSource>> {
            unsupported()
        }
        async fn list_raw(&self, _ns: &str, _prefix: Option<&str>) -> SFResult<Vec<String>> {
            unsupported()
        }
        async fn delete_raw(&self, _ns: &str, _id: &str) -> SFResult<()> {
            unsupported()
        }
        async fn store_schema(&self, _ns: &str, _entry: &cog_core::SchemaEntry) -> SFResult<()> {
            unsupported()
        }
        async fn get_schema(
            &self,
            _ns: &str,
            _id: &str,
        ) -> SFResult<Option<cog_core::SchemaEntry>> {
            unsupported()
        }
        async fn search_schema(
            &self,
            _ns: &str,
            _query: &str,
            _limit: usize,
        ) -> SFResult<Vec<SchemaSearchResult>> {
            unsupported()
        }
        async fn schema_for_raw(
            &self,
            _ns: &str,
            _raw_id: &str,
        ) -> SFResult<Vec<cog_core::SchemaEntry>> {
            unsupported()
        }
        async fn list_schema(&self, _ns: &str) -> SFResult<Vec<cog_core::SchemaEntry>> {
            Ok(self.schemas.lock().unwrap().clone())
        }
        async fn delete_schema(&self, _ns: &str, _id: &str) -> SFResult<()> {
            unsupported()
        }
        async fn query_relations(
            &self,
            _ns: &str,
            _entity: &str,
            _direction: RelationDirection,
            _relation_type: Option<&str>,
        ) -> SFResult<Vec<cog_core::SchemaEntry>> {
            unsupported()
        }
        async fn update_schema(&self, _ns: &str, _entry: &cog_core::SchemaEntry) -> SFResult<()> {
            unsupported()
        }
        async fn store_summary(&self, _ns: &str, _entry: &SummaryEntry) -> SFResult<()> {
            unsupported()
        }
        async fn get_summary(&self, _ns: &str, _id: &str) -> SFResult<Option<SummaryEntry>> {
            unsupported()
        }
        async fn search_summary(
            &self,
            _ns: &str,
            _query: &[f32],
            _top_k: usize,
            _range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
        ) -> SFResult<Vec<SummarySearchResult>> {
            unsupported()
        }
        async fn summary_for_raw(&self, _ns: &str, _raw_id: &str) -> SFResult<Vec<SummaryEntry>> {
            unsupported()
        }
        async fn list_summary(&self, _ns: &str) -> SFResult<Vec<SummaryEntry>> {
            unsupported()
        }
        async fn delete_summary(&self, _ns: &str, _id: &str) -> SFResult<()> {
            unsupported()
        }
        async fn update_summary(&self, _ns: &str, _entry: &SummaryEntry) -> SFResult<()> {
            unsupported()
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
            _query: &str,
            _embedding: Option<&[f32]>,
            _top_k: usize,
            _range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
        ) -> SFResult<Vec<UnifiedSearchResult>> {
            unsupported()
        }
        async fn ingest_explicit(
            &self,
            _ns: &str,
            _text: &str,
            _importance: f32,
            _tags: Vec<String>,
        ) -> SFResult<()> {
            unsupported()
        }
        async fn forget(&self, _ns: &str, _id: &str) -> SFResult<()> {
            unsupported()
        }
        async fn decay(
            &self,
            _ns: &str,
            _age: u64,
            _importance: f32,
        ) -> SFResult<cog_core::DecayReport> {
            unsupported()
        }
    }

    fn learning(priority: Priority, status: LearningStatus, tags: &[&str]) -> Learning {
        let mut l = Learning::new(
            LearningCategory::Insight,
            priority,
            Area::Infra,
            "summary",
            "details",
            "action",
            LearningSource::SelfReview,
        );
        l.status = status;
        l.tags = tags.iter().map(|t| t.to_string()).collect();
        l
    }

    fn schema_for(l: &Learning) -> cog_core::SchemaEntry {
        cog_core::SchemaEntry::new(
            l.id.clone(),
            "reflection",
            cog_core::SchemaKind::Learning,
            l.summary.clone(),
            l.id.clone(),
            SourceRef::new(format!("memory://reflection/{}", l.id), "test"),
        )
        .with_properties(serde_json::to_value(l).unwrap())
    }

    /// The backend recorder used to ignore the filter and hand back the whole
    /// namespace, so callers reading `high_priority_pending` or
    /// `recently_resolved` got every learning instead of the asked-for subset
    /// and had no way to tell, since a wider list looks like a valid answer.
    #[tokio::test]
    async fn backend_recorder_applies_the_filter() {
        let high_pending = learning(Priority::High, LearningStatus::Pending, &["a"]);
        let low_pending = learning(Priority::Low, LearningStatus::Pending, &[]);
        let high_resolved = learning(Priority::High, LearningStatus::Resolved, &[]);
        let backend = Arc::new(SchemaOnlyBackend::with(vec![
            schema_for(&high_pending),
            schema_for(&low_pending),
            schema_for(&high_resolved),
        ]));
        let recorder = MemoryBackendRecorder::new(backend, "reflection");

        let all = recorder.list_learnings(None).await.unwrap();
        assert_eq!(all.len(), 3);

        let filtered = recorder
            .list_learnings(Some(LearningFilter {
                status: Some(LearningStatus::Pending),
                priority: Some(Priority::High),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, high_pending.id);
    }

    /// A filter with no matching row must come back empty rather than falling
    /// back to the unfiltered list.
    #[tokio::test]
    async fn backend_recorder_returns_empty_when_nothing_matches() {
        let backend = Arc::new(SchemaOnlyBackend::with(vec![schema_for(&learning(
            Priority::Low,
            LearningStatus::Pending,
            &[],
        ))]));
        let recorder = MemoryBackendRecorder::new(backend, "reflection");

        let filtered = recorder
            .list_learnings(Some(LearningFilter {
                tags: vec!["absent".into()],
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(filtered.is_empty());
    }

    /// Both recorders must answer a filter identically; the in-memory one is
    /// the reference behaviour the persistent one has to match.
    #[tokio::test]
    async fn both_recorders_agree_on_a_filter() {
        let high = learning(Priority::High, LearningStatus::Pending, &["t"]);
        let low = learning(Priority::Low, LearningStatus::Pending, &[]);
        let filter = LearningFilter {
            priority: Some(Priority::High),
            tags: vec!["t".into()],
            ..Default::default()
        };

        let memory = InMemoryRecorder::new();
        memory.record_learning(high.clone()).await.unwrap();
        memory.record_learning(low.clone()).await.unwrap();

        let backend = Arc::new(SchemaOnlyBackend::with(vec![
            schema_for(&high),
            schema_for(&low),
        ]));
        let persistent = MemoryBackendRecorder::new(backend, "reflection");

        let from_memory = memory.list_learnings(Some(filter.clone())).await.unwrap();
        let from_backend = persistent.list_learnings(Some(filter)).await.unwrap();
        assert_eq!(from_memory.len(), 1);
        assert_eq!(from_backend.len(), from_memory.len());
        assert_eq!(from_memory[0].id, from_backend[0].id);
    }

    /// Each clause narrows; none is dropped. Asserted one clause at a time so a
    /// silently removed clause names itself in the failure.
    #[test]
    fn every_clause_of_the_filter_narrows() {
        let base = learning(Priority::High, LearningStatus::Pending, &["x", "y"]);
        assert!(matches_learning_filter(&base, None));
        assert!(matches_learning_filter(
            &base,
            Some(&LearningFilter {
                status: Some(LearningStatus::Pending),
                priority: Some(Priority::High),
                tags: vec!["x".into()],
                ..Default::default()
            })
        ));
        for mismatched in [
            LearningFilter {
                status: Some(LearningStatus::Resolved),
                ..Default::default()
            },
            LearningFilter {
                priority: Some(Priority::Low),
                ..Default::default()
            },
            LearningFilter {
                tags: vec!["z".into()],
                ..Default::default()
            },
            LearningFilter {
                since: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
                ..Default::default()
            },
            LearningFilter {
                until: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
                ..Default::default()
            },
        ] {
            assert!(
                !matches_learning_filter(&base, Some(&mismatched)),
                "filter matched but should not have: {mismatched:?}"
            );
        }
    }
}
