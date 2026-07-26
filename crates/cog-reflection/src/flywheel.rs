//! 学习数据飞轮（审计 4.4）：学习记录持久化到数仓，用于离线分析和新策略训练。
//!
//! [`WarehouseRecorder`] 包装任意 [`LearningRecorder`]，在本地持久化之外，
//! 把每条学习/错误/特性请求记录同步导出到 [`LearningSink`]。
//! 默认提供 JSONL 文件 sink（数仓原始区）；ClickHouse/Postgres 等
//! 分析后端可在装配层用同一 trait 桥接，无需 cog-reflection 新增依赖。

use std::path::PathBuf;
use std::sync::Arc;

use cog_core::{ErrorEntry, Learning, Resolution, SFError, SFResult};

use crate::types::{FeatureRequest, LearningFilter};
use crate::LearningRecorder;

/// 学习记录数仓导出端。
#[async_trait::async_trait]
pub trait LearningSink: Send + Sync {
    /// kind 为记录类别："learning" / "error" / "feature_request"。
    async fn export(&self, kind: &str, payload: serde_json::Value) -> SFResult<()>;
}

/// JSONL 文件 sink：每条记录一行，追加写入 `{dir}/{kind}.jsonl`。
pub struct JsonlFileSink {
    dir: PathBuf,
}

impl JsonlFileSink {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

#[async_trait::async_trait]
impl LearningSink for JsonlFileSink {
    async fn export(&self, kind: &str, payload: serde_json::Value) -> SFResult<()> {
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(|e| SFError::IO(format!("failed to create {}: {}", self.dir.display(), e)))?;
        let path = self.dir.join(format!("{}.jsonl", kind));
        let line = serde_json::to_string(&payload).map_err(SFError::Serialization)?;
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| SFError::IO(format!("failed to open {}: {}", path.display(), e)))?;
        file.write_all(line.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("failed to append {}: {}", path.display(), e)))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| SFError::IO(format!("failed to append {}: {}", path.display(), e)))?;
        Ok(())
    }
}

/// 包装 recorder：先写本地，再导出数仓。导出失败只告警、不影响主链路。
pub struct WarehouseRecorder {
    inner: Arc<dyn LearningRecorder>,
    sink: Arc<dyn LearningSink>,
}

impl WarehouseRecorder {
    pub fn new(inner: Arc<dyn LearningRecorder>, sink: Arc<dyn LearningSink>) -> Self {
        Self { inner, sink }
    }

    async fn export_warn(&self, kind: &str, payload: serde_json::Value) {
        if let Err(e) = self.sink.export(kind, payload).await {
            tracing::warn!(kind = kind, error = %e, "learning warehouse export failed");
        }
    }
}

#[async_trait::async_trait]
impl LearningRecorder for WarehouseRecorder {
    async fn record_learning(&self, learning: Learning) -> SFResult<()> {
        let payload = serde_json::to_value(&learning).map_err(SFError::Serialization)?;
        self.inner.record_learning(learning).await?;
        self.export_warn("learning", payload).await;
        Ok(())
    }

    async fn record_error(&self, error: ErrorEntry) -> SFResult<()> {
        let payload = serde_json::to_value(&error).map_err(SFError::Serialization)?;
        self.inner.record_error(error).await?;
        self.export_warn("error", payload).await;
        Ok(())
    }

    async fn record_feature_request(&self, request: FeatureRequest) -> SFResult<()> {
        let payload = serde_json::to_value(&request).map_err(SFError::Serialization)?;
        self.inner.record_feature_request(request).await?;
        self.export_warn("feature_request", payload).await;
        Ok(())
    }

    async fn resolve(&self, id: &str, resolution: Resolution) -> SFResult<()> {
        self.inner.resolve(id, resolution).await
    }

    async fn get_learning(&self, id: &str) -> SFResult<Option<Learning>> {
        self.inner.get_learning(id).await
    }

    async fn list_learnings(&self, filter: Option<LearningFilter>) -> SFResult<Vec<Learning>> {
        self.inner.list_learnings(filter).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Area, LearningCategory, LearningSource, Priority};

    fn sample_learning() -> Learning {
        Learning::new(
            LearningCategory::Insight,
            Priority::Medium,
            Area::Backend,
            "test summary",
            "details",
            "action",
            LearningSource::SelfReview,
        )
    }

    #[tokio::test]
    async fn warehouse_recorder_writes_through_to_jsonl() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn LearningRecorder> = Arc::new(crate::InMemoryRecorder::new());
        let sink = Arc::new(JsonlFileSink::new(tmp.path()));
        let rec = WarehouseRecorder::new(inner.clone(), sink);

        let learning = sample_learning();
        let id = learning.id.clone();
        rec.record_learning(learning).await.unwrap();

        // 本地可查
        let stored = inner.get_learning(&id).await.unwrap();
        assert!(stored.is_some());

        // JSONL 已导出且可解析
        let line = std::fs::read_to_string(tmp.path().join("learning.jsonl")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["id"], id);
        assert_eq!(parsed["summary"], "test summary");
    }

    #[tokio::test]
    async fn sink_failure_does_not_break_main_path() {
        struct FailingSink;
        #[async_trait::async_trait]
        impl LearningSink for FailingSink {
            async fn export(&self, _kind: &str, _payload: serde_json::Value) -> SFResult<()> {
                Err(SFError::IO("sink down".into()))
            }
        }

        let inner: Arc<dyn LearningRecorder> = Arc::new(crate::InMemoryRecorder::new());
        let rec = WarehouseRecorder::new(inner.clone(), Arc::new(FailingSink));
        let learning = sample_learning();
        let id = learning.id.clone();
        // sink 失败不影响 record
        rec.record_learning(learning).await.unwrap();
        assert!(inner.get_learning(&id).await.unwrap().is_some());
    }
}
