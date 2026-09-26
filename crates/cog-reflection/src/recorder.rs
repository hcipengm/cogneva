//! Persistent recording of learning entries.
//! The [`LearningRecorder`] trait abstracts over storage backends so that
//! learnings can be archived in the same three-layer memory system as
//! regular agent memories (raw → schema → summary).

use std::sync::Arc;

use async_trait::async_trait;
use cog_core::SFResult;
use tracing::{debug, error, info, warn};

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

/// 条目到派生层的映射。抽成一套共用函数是因为写入与补齐两条路径都要用它：
/// 各写一份迟早分叉，而分叉的表现是「补齐出来的条目和正常写入的条目不同」，
/// 比不补齐更难查。
fn reflection_source_ref(namespace: &str, id: &str) -> SourceRef {
    SourceRef::new(
        format!("memory://{}/{}", namespace, id),
        "cog-reflection/1.0",
    )
}

/// A reflection's importance is its priority placed on the system's one
/// importance scale — the same scale the model rates an extracted item on, so a
/// reflection and a fact rank against each other instead of each being
/// comparable only within its own producer. The placement belongs to the level,
/// not to this recorder, so it lives on [`cog_core::Priority`].
fn importance_for(priority: cog_core::Priority) -> f32 {
    priority.importance()
}

fn schema_for_learning(namespace: &str, learning: &Learning) -> SFResult<cog_core::SchemaEntry> {
    Ok(cog_core::SchemaEntry::new(
        learning.id.clone(),
        namespace,
        cog_core::SchemaKind::Learning,
        &learning.summary,
        &learning.id,
        reflection_source_ref(namespace, &learning.id),
    )
    .with_properties(serde_json::to_value(learning).map_err(cog_core::SFError::Serialization)?)
    .with_importance(importance_for(learning.priority)))
}

fn schema_for_error(namespace: &str, error: &ErrorEntry) -> SFResult<cog_core::SchemaEntry> {
    Ok(cog_core::SchemaEntry::new(
        error.id.clone(),
        namespace,
        cog_core::SchemaKind::ErrorPattern,
        &error.error_message,
        &error.id,
        reflection_source_ref(namespace, &error.id),
    )
    .with_properties(serde_json::to_value(error).map_err(cog_core::SFError::Serialization)?)
    .with_importance(importance_for(error.priority)))
}

fn schema_for_feature_request(
    namespace: &str,
    request: &FeatureRequest,
) -> SFResult<cog_core::SchemaEntry> {
    Ok(cog_core::SchemaEntry::new(
        request.id.clone(),
        namespace,
        cog_core::SchemaKind::Custom,
        &request.capability,
        &request.id,
        reflection_source_ref(namespace, &request.id),
    )
    .with_properties(serde_json::to_value(request).map_err(cog_core::SFError::Serialization)?))
}

/// 从归档的条目内容重建它的派生层。类型按 id 前缀认——前缀由各类型的
/// `generate_id` 生成，是这个存储里唯一的类型标记；条目自报的 id 与文件名
/// 不一致说明这份归档对不上号，宁可跳过也不拿它给别人盖章。
fn schema_from_payload(namespace: &str, id: &str, payload: &[u8]) -> Option<cog_core::SchemaEntry> {
    if let Some(rest) = id.strip_prefix("LRN-") {
        if rest.is_empty() {
            return None;
        }
        let learning: Learning = serde_json::from_slice(payload).ok()?;
        return (learning.id == id)
            .then(|| schema_for_learning(namespace, &learning).ok())
            .flatten();
    }
    if let Some(rest) = id.strip_prefix("ERR-") {
        if rest.is_empty() {
            return None;
        }
        let error: ErrorEntry = serde_json::from_slice(payload).ok()?;
        return (error.id == id)
            .then(|| schema_for_error(namespace, &error).ok())
            .flatten();
    }
    if let Some(rest) = id.strip_prefix("FEAT-") {
        if rest.is_empty() {
            return None;
        }
        let request: FeatureRequest = serde_json::from_slice(payload).ok()?;
        return (request.id == id)
            .then(|| schema_for_feature_request(namespace, &request).ok())
            .flatten();
    }
    None
}

/// 从 schema 的 `source_ref.raw_uri`（`memory://<namespace>/<条目 id>`）取回它
/// 覆盖的 raw id。取不回就当作「没覆盖」——多补一条比漏补一条好。
fn schema_raw_id(uri: &str, namespace: &str) -> Option<String> {
    uri.strip_prefix(&format!("memory://{}/", namespace))
        .filter(|rest| !rest.is_empty())
        .map(str::to_string)
}

/// 一次补齐扫描的结果。两个数分开报是因为它们对应两个决定：补齐的是这一轮
/// 自己救回来的，认不出的是需要人看一眼的。合并成一个数会让后者随前者一起
/// 变化而看不见。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SchemaRepair {
    /// 这轮从 raw 内容重建出派生层的条目数。
    pub repaired: usize,
    /// 已归档、缺派生层、且无法从内容认出类型的条目数。
    pub unrepairable: usize,
}

impl SchemaRepair {
    /// 这轮发现的缺派生层的条目总数。
    pub fn found(&self) -> usize {
        self.repaired + self.unrepairable
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

    /// 补齐「已归档、但没有派生层」的条目。归档与派生层是两次独立写：第二次
    /// 失败或进程在此重启，就留下只有一个 raw 的孤儿。而条目是按 schema 检索
    /// 的，孤儿虽然落了盘却再也查不到——它不会自己报出来，因为「做完了没」的
    /// 判据正是「派生层在不在」，缺席查不出缺席。
    ///
    /// 补齐不需要 LLM：raw 里存的就是条目本身，按 id 前缀还原成对应类型即可
    /// 重建派生层，所以这条路径在上游断供时照样能跑。认不出类型的只计数，
    /// 不猜也不丢：宁可不补，也不凭空造一条派生层去冒充原始记录。
    pub async fn repair_missing_schemas(&self) -> SFResult<SchemaRepair> {
        let ids = self.backend.list_raw(&self.namespace, None).await?;
        let covered: std::collections::HashSet<String> = self
            .backend
            .list_schema(&self.namespace)
            .await?
            .into_iter()
            .filter_map(|s| schema_raw_id(&s.source_ref.raw_uri, &self.namespace))
            .collect();

        let mut repair = SchemaRepair::default();
        for id in ids {
            if covered.contains(&id) {
                continue;
            }
            let Some(raw) = self.backend.get_raw(&self.namespace, &id).await? else {
                // 归档列表里有、读不到：既不能确认它缺派生层，也不能重建。
                // 算作需要人看一眼的那种，不静默略过。
                repair.unrepairable += 1;
                continue;
            };
            match schema_from_payload(&self.namespace, &id, &raw.payload) {
                Some(schema) => {
                    self.backend.store_schema(&self.namespace, &schema).await?;
                    repair.repaired += 1;
                }
                None => repair.unrepairable += 1,
            }
        }
        Ok(repair)
    }
}

/// This loop's name in the liveness census.
pub const MEMORY_SCHEMA_REPAIR_LOOP: &str = "memory_schema_repair";

/// 按间隔补齐缺失的派生层，直到收到停机信号。间隔为 0 表示不重扫。
///
/// 属主不用配置判定，用结构性证据：谁手里有这批 raw 才能补谁。反思条目的
/// 归档落在各部署自己的数据卷上，而删掉派生层的缺口只可能出现在有归档的那
/// 一侧，所以每个部署都跑一遍并不会互相重复劳动——没有归档的那份扫出来是空
/// 的，有的是 upsert，重复执行也不会写坏。反过来用一个配置开关去指定属主会
/// 指定错：拿执行器职责当开关，就会让握着归档的控制面跳过、让空手的执行器
/// 白跑。
pub fn spawn_schema_repair_loop(
    recorder: MemoryBackendRecorder,
    interval: std::time::Duration,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    // This loop repairs schemas for entries that were archived without one. A
    // loop that died leaves no trace of its own -- the repairs it would have made
    // are simply never made, and the archived entries stay unreachable.
    cog_core::loop_health::spawn(
        MEMORY_SCHEMA_REPAIR_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let recorder = recorder.clone();
            let shutdown = shutdown.clone();
            async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    // Stamped on every cycle: most find nothing to repair, and that is the
                    // healthy state rather than evidence the loop stopped.
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => return,
                    }
                    match recorder.repair_missing_schemas().await {
                        Ok(repair) if repair.found() > 0 => info!(
                            repaired = repair.repaired,
                            unrepairable = repair.unrepairable,
                            "rebuilt missing memory schemas from archived entries"
                        ),
                        Ok(_) => debug!("memory schema repair: no orphaned entries"),
                        Err(e) => warn!("memory schema repair failed: {}", e),
                    }
                }
            }
        },
    )
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

        let schema = schema_for_learning(&self.namespace, &learning)?;
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

        let schema = schema_for_error(&self.namespace, &error)?;
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

        let schema = schema_for_feature_request(&self.namespace, &request)?;
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
        MemoryBackend, MemoryMetrics, RelationDirection, SchemaSearchResult, SummaryEntry,
        SummarySearchResult, UnifiedSearchResult,
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

    /// A `MemoryBackend` that serves the raw and schema layers in memory.
    /// The repair path reads one and writes the other, so it needs both; the
    /// remaining layers exist only so the trait can be implemented.
    #[derive(Default)]
    struct RawAndSchemaBackend {
        raws: Mutex<Vec<RawSource>>,
        schemas: Mutex<Vec<cog_core::SchemaEntry>>,
    }

    #[async_trait]
    impl cog_core::MemoryBackend for RawAndSchemaBackend {
        async fn archive_raw(&self, source: &RawSource) -> SFResult<String> {
            let mut raws = self.raws.lock().unwrap();
            raws.retain(|r| r.id != source.id);
            raws.push(source.clone());
            Ok(format!("memory://{}/{}", source.namespace, source.id))
        }
        async fn get_raw(&self, _ns: &str, id: &str) -> SFResult<Option<RawSource>> {
            Ok(self
                .raws
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.id == id)
                .cloned())
        }
        async fn list_raw(&self, _ns: &str, prefix: Option<&str>) -> SFResult<Vec<String>> {
            Ok(self
                .raws
                .lock()
                .unwrap()
                .iter()
                .filter(|r| prefix.is_none_or(|p| r.content_type.starts_with(p)))
                .map(|r| r.id.clone())
                .collect())
        }
        async fn delete_raw(&self, _ns: &str, _id: &str) -> SFResult<()> {
            unsupported()
        }
        async fn store_schema(&self, _ns: &str, entry: &cog_core::SchemaEntry) -> SFResult<()> {
            let mut schemas = self.schemas.lock().unwrap();
            schemas.retain(|s| s.id != entry.id);
            schemas.push(entry.clone());
            Ok(())
        }
        async fn get_schema(&self, _ns: &str, id: &str) -> SFResult<Option<cog_core::SchemaEntry>> {
            Ok(self
                .schemas
                .lock()
                .unwrap()
                .iter()
                .find(|s| s.id == id)
                .cloned())
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
            raw_id: &str,
        ) -> SFResult<Vec<cog_core::SchemaEntry>> {
            Ok(self
                .schemas
                .lock()
                .unwrap()
                .iter()
                .filter(|s| s.source_ref.raw_uri.ends_with(raw_id))
                .cloned()
                .collect())
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

    /// 归档与派生层是两次独立写，第二次失败就留下一个「在盘上、但按 schema
    /// 检索不到」的孤儿。它不会自己报出来——判定「做完了没」的判据正是「派生层
    /// 在不在」，缺席查不出缺席——所以只能靠一次主动重扫把它补回来。
    #[tokio::test]
    async fn repair_rebuilds_the_schema_of_an_orphaned_entry() {
        let backend = Arc::new(RawAndSchemaBackend::default());
        let recorder = MemoryBackendRecorder::new(backend.clone(), "reflection");
        let orphan = learning(Priority::High, LearningStatus::Pending, &["a"]);
        recorder.record_learning(orphan.clone()).await.unwrap();
        // 抹掉派生层，复现「归档在、schema 缺席」的现场。
        backend.schemas.lock().unwrap().clear();
        assert!(recorder.list_learnings(None).await.unwrap().is_empty());

        let repair = recorder.repair_missing_schemas().await.unwrap();
        assert_eq!(repair.repaired, 1);
        assert_eq!(repair.unrepairable, 0);

        let schema = backend
            .get_schema("reflection", &orphan.id)
            .await
            .unwrap()
            .expect("the rebuilt schema must be retrievable");
        assert_eq!(schema.kind, cog_core::SchemaKind::Learning);
        assert_eq!(
            schema.source_ref.raw_uri,
            format!("memory://reflection/{}", orphan.id)
        );
        assert_eq!(
            serde_json::from_value::<Learning>(schema.properties)
                .unwrap()
                .id,
            orphan.id,
            "the rebuilt entry must carry the archived record, not a summary of it"
        );
        let recovered = recorder.list_learnings(None).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, orphan.id);
    }

    #[tokio::test]
    async fn repair_leaves_entries_that_still_have_a_schema_alone() {
        let backend = Arc::new(RawAndSchemaBackend::default());
        let recorder = MemoryBackendRecorder::new(backend.clone(), "reflection");
        recorder
            .record_learning(learning(Priority::High, LearningStatus::Pending, &[]))
            .await
            .unwrap();

        let repair = recorder.repair_missing_schemas().await.unwrap();
        assert_eq!(repair.found(), 0);
        assert_eq!(
            backend.schemas.lock().unwrap().len(),
            1,
            "an entry that already has a schema must not be written twice"
        );
    }

    #[tokio::test]
    async fn repair_counts_entries_it_cannot_type_instead_of_guessing() {
        let backend = Arc::new(RawAndSchemaBackend::default());
        let recorder = MemoryBackendRecorder::new(backend.clone(), "reflection");
        backend
            .archive_raw(&RawSource::new(
                "WRENCH-20260919-00000001",
                "reflection",
                "application/json",
                br#"{"kind":"something else"}"#.to_vec(),
            ))
            .await
            .unwrap();

        let repair = recorder.repair_missing_schemas().await.unwrap();
        assert_eq!(repair.repaired, 0);
        assert_eq!(
            repair.unrepairable, 1,
            "an entry the repair cannot type must be counted, not silently skipped"
        );
        assert!(
            backend.schemas.lock().unwrap().is_empty(),
            "a made-up schema would make the entry look recovered when it is not"
        );
    }

    #[tokio::test]
    async fn repair_skips_payloads_whose_id_disagrees_with_the_archive() {
        let backend = Arc::new(RawAndSchemaBackend::default());
        let recorder = MemoryBackendRecorder::new(backend.clone(), "reflection");
        let mut recorded_elsewhere = learning(Priority::High, LearningStatus::Pending, &[]);
        recorded_elsewhere.id = "LRN-20260916-00000001".into();
        backend
            .archive_raw(&RawSource::new(
                "LRN-20260916-00000002",
                "reflection",
                "application/json",
                serde_json::to_vec(&recorded_elsewhere).unwrap(),
            ))
            .await
            .unwrap();

        let repair = recorder.repair_missing_schemas().await.unwrap();
        assert_eq!(repair.repaired, 0);
        assert_eq!(repair.unrepairable, 1);
        assert!(backend.schemas.lock().unwrap().is_empty());
    }
}
