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
        let results: Vec<Learning> = match filter {
            Some(f) => guard
                .iter()
                .filter(|l| {
                    f.status.as_ref().is_none_or(|s| *s == l.status)
                        && f.priority.as_ref().is_none_or(|p| *p == l.priority)
                        && f.area.as_ref().is_none_or(|a| *a == l.area)
                        && f.category.as_ref().is_none_or(|c| *c == l.category)
                        && f.source.as_ref().is_none_or(|s| *s == l.source)
                        && f.pattern_key
                            .as_ref()
                            .is_none_or(|pk| l.pattern_key.as_ref() == Some(pk))
                        && f.tags.iter().all(|t| l.tags.contains(t))
                        && f.since.is_none_or(|since| l.last_seen >= since)
                        && f.until.is_none_or(|until| l.last_seen <= until)
                })
                .cloned()
                .collect(),
            None => guard.clone(),
        };
        Ok(results)
    }
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

    async fn list_learnings(&self, _filter: Option<LearningFilter>) -> SFResult<Vec<Learning>> {
        let schemas = self.backend.list_schema(&self.namespace).await?;
        let mut learnings = Vec::with_capacity(schemas.len());
        for s in schemas {
            if let Ok(l) = serde_json::from_value::<Learning>(s.properties) {
                learnings.push(l);
            }
        }
        Ok(learnings)
    }
}
