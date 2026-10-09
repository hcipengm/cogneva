//! 记忆维护循环——把「低价值记忆自动衰减」从一句文档承诺变成一条真跑的通路。
//!
//! 在它之前，`MemoryBackend::decay` 有完整实现却**没有任何生产调用点**：文档
//! 承诺的自动衰减永远不会发生，只有测试戳过它一次。衰减是**要被周期驱动**的
//! 动作——一次调用不会随时间自己再来一回——所以缺的是一台驱动它、并且把自己每
//! 一轮跑成没跑成读出来的循环。
//!
//! 这条循环按 `memory.maintenance.decay_interval_secs` 的节拍扫过配置里列出的
//! 命名空间，对每个命名空间调一次 `decay`，并把这一轮落在哪一格记进
//! `cogneva_memory_decay_total{outcome}`（闭集：`idle`／`decayed`／`archived`／
//! `failed`）。它自己不判阈值对错——阈值是配置，规则是另一件事。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use cog_core::{MemoryBackend, MetricsBackend};

use crate::config::MaintenanceConfig;

/// 这条循环在进程存活注册表里的名字，也是 `cogneva_loop_tick_age_seconds{loop=…}`
/// 的取值。
pub const MEMORY_DECAY_LOOP: &str = "memory_decay";

/// 一次衰减扫描把合格条目的重要性乘上的系数。
///
/// 衰减刻意做成**逐次**的：一次扫描只降一档，条目跌到
/// [`DECAY_ARCHIVE_FLOOR`] 才被归档。于是「归档」是**反复**衰减的后果，而不是
/// 一次低分就能触发的删除——一条刚入库、重要性恰好很低但还在被用的记忆，不会在
/// 第一次扫描时消失。
pub const DECAY_IMPORTANCE_FACTOR: f32 = 0.5;

/// 降到这个重要性（含）以下的合格条目离开可检索的摘要层。
///
/// 取得很低，保证只有跨过多次衰减的条目才够得着，把「归档」钉在长期无人强化的
/// 那一段上，而不是任何一次扫描的即时判词。
pub const DECAY_ARCHIVE_FLOOR: f32 = 0.02;

/// 一轮扫描（针对一个命名空间）的结局。闭集，每次恰好落一格。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecayPass {
    /// 扫描跑完，没有够格衰减的条目。
    Idle,
    /// 扫描降低了至少一条合格条目的重要性，但一条都没归档。
    Decayed,
    /// 扫描至少归档了一条合格条目（重要性已跌到地板）。
    Archived,
    /// 扫描没能跑完（后端报错）。
    Failed,
}

impl DecayPass {
    fn cell(&self) -> &'static str {
        match self {
            DecayPass::Idle => "idle",
            DecayPass::Decayed => "decayed",
            DecayPass::Archived => "archived",
            DecayPass::Failed => "failed",
        }
    }
}

/// 起一条进程级维护循环，按配置节拍衰减记忆。`decay_interval_secs` 为 0 时直接
/// 不起（整条维护关掉）。
///
/// 用 `spawn_unstoppable`：这条循环没有别人能给它的停止信号，它的退出就等于进程
/// 退出，所以任何一次提前退出都是一次没人要求的死，交给存活族记账。
pub fn spawn_decay_loop(
    backend: Arc<dyn MemoryBackend>,
    metrics: Arc<dyn MetricsBackend>,
    config: MaintenanceConfig,
) {
    let secs = config.decay_interval_secs;
    if secs == 0 {
        info!("Memory decay maintenance disabled (maintenance.decay_interval_secs=0)");
        return;
    }
    let period = Duration::from_secs(secs);
    let namespaces = config.decay_namespaces.clone();
    let age_threshold_secs = config.decay_age_threshold_secs;
    let importance_threshold = config.decay_importance_threshold;
    info!(
        interval_secs = secs,
        age_threshold_secs,
        importance_threshold,
        ?namespaces,
        "Memory decay maintenance enabled"
    );

    drop(cog_core::loop_health::spawn_unstoppable(
        MEMORY_DECAY_LOOP,
        cog_core::loop_health::Cadence::Periodic(period),
        // 每次尝试重建闭包，所以体里用到的东西在这里克隆一份。
        move |beat| {
            let backend = backend.clone();
            let metrics = metrics.clone();
            let namespaces = namespaces.clone();
            async move {
                let mut ticker = tokio::time::interval(period);
                // 第一次 tick 立即返回；先吃掉它，让第一轮扫描落在起点之后一个
                // 周期，而不是插件刚 start 就和启动期抢同一把锁。
                ticker.tick().await;
                loop {
                    beat.beat();
                    ticker.tick().await;
                    for namespace in &namespaces {
                        let pass = run_pass(
                            &backend,
                            namespace,
                            age_threshold_secs,
                            importance_threshold,
                        )
                        .await;
                        if pass == DecayPass::Failed {
                            warn!(namespace = %namespace, "Memory decay pass failed");
                        }
                        record_pass(&metrics, pass).await;
                    }
                }
            }
        },
    ));
}

/// 对一个命名空间跑一轮衰减，把结果压成一格结局。
async fn run_pass(
    backend: &Arc<dyn MemoryBackend>,
    namespace: &str,
    age_threshold_secs: u64,
    importance_threshold: f32,
) -> DecayPass {
    match backend
        .decay(namespace, age_threshold_secs, importance_threshold)
        .await
    {
        Ok(report) if report.entries_archived > 0 => DecayPass::Archived,
        Ok(report) if report.entries_decayed > 0 => DecayPass::Decayed,
        Ok(_) => DecayPass::Idle,
        Err(e) => {
            // 错误本身也落一句，方便定位；读数那一格由调用方记。
            warn!(namespace = %namespace, error = %e, "Memory decay pass errored");
            DecayPass::Failed
        }
    }
}

/// 把一格结局写进闭集计数器。写不进去只 warn：读数失败不该拖垮维护循环。
async fn record_pass(metrics: &Arc<dyn MetricsBackend>, pass: DecayPass) {
    let mut labels = HashMap::new();
    labels.insert("outcome".to_string(), pass.cell().to_string());
    if let Err(e) = metrics
        .record_counter(cog_core::metric_names::MEMORY_DECAY_TOTAL, 1.0, labels)
        .await
    {
        warn!(error = %e, "Memory decay: could not record pass outcome");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use cog_core::{
        DecayReport, MemoryMetrics, RawSource, SFResult, SchemaEntry, SchemaSearchResult,
        SummaryEntry, SummarySearchResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A backend that answers `decay` with whatever it was told, and counts how
    /// many passes reached it. Everything else is unimplemented for this test's
    /// purpose, but must exist to satisfy the trait.
    struct ScriptedBackend {
        archived: usize,
        decayed: usize,
        fail: bool,
        passes: AtomicUsize,
        last_ns: Mutex<Option<String>>,
    }

    impl ScriptedBackend {
        fn new(decayed: usize, archived: usize, fail: bool) -> Arc<Self> {
            Arc::new(Self {
                archived,
                decayed,
                fail,
                passes: AtomicUsize::new(0),
                last_ns: Mutex::new(None),
            })
        }
    }

    #[async_trait]
    impl MemoryBackend for ScriptedBackend {
        async fn decay(&self, namespace: &str, _age: u64, _imp: f32) -> SFResult<DecayReport> {
            self.passes.fetch_add(1, Ordering::SeqCst);
            *self.last_ns.lock().unwrap() = Some(namespace.to_string());
            if self.fail {
                return Err(cog_core::SFError::Agent("scripted decay failure".into()));
            }
            Ok(DecayReport {
                namespace: namespace.to_string(),
                entries_decayed: self.decayed,
                entries_archived: self.archived,
            })
        }
        async fn archive_raw(&self, _s: &RawSource) -> SFResult<String> {
            unimplemented!()
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
        async fn store_schema(&self, _ns: &str, _e: &SchemaEntry) -> SFResult<()> {
            unimplemented!()
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
            _d: cog_core::RelationDirection,
            _t: Option<&str>,
        ) -> SFResult<Vec<SchemaEntry>> {
            unimplemented!()
        }
        async fn update_schema(&self, _ns: &str, _e: &SchemaEntry) -> SFResult<()> {
            unimplemented!()
        }
        async fn store_summary(&self, _ns: &str, _e: &SummaryEntry) -> SFResult<()> {
            unimplemented!()
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
        ) -> SFResult<Vec<cog_core::UnifiedSearchResult>> {
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
    }

    #[tokio::test]
    async fn a_pass_that_archives_reports_the_archive_cell() {
        let backend = ScriptedBackend::new(3, 1, false);
        let pass = run_pass(
            &(backend.clone() as Arc<dyn MemoryBackend>),
            "default",
            0,
            1.0,
        )
        .await;
        assert_eq!(pass, DecayPass::Archived);
    }

    #[tokio::test]
    async fn a_pass_that_only_decays_reports_the_decay_cell() {
        let backend = ScriptedBackend::new(2, 0, false);
        let pass = run_pass(
            &(backend.clone() as Arc<dyn MemoryBackend>),
            "default",
            0,
            1.0,
        )
        .await;
        assert_eq!(pass, DecayPass::Decayed);
    }

    #[tokio::test]
    async fn an_idle_pass_reports_the_idle_cell() {
        let backend = ScriptedBackend::new(0, 0, false);
        let pass = run_pass(
            &(backend.clone() as Arc<dyn MemoryBackend>),
            "default",
            0,
            1.0,
        )
        .await;
        assert_eq!(pass, DecayPass::Idle);
    }

    #[tokio::test]
    async fn a_failing_pass_reports_the_failure_cell() {
        let backend = ScriptedBackend::new(0, 0, true);
        let pass = run_pass(
            &(backend.clone() as Arc<dyn MemoryBackend>),
            "default",
            0,
            1.0,
        )
        .await;
        assert_eq!(pass, DecayPass::Failed);
    }
}
