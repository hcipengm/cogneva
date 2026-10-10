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
//!
//! 它扫的条目存储在 composite 后端下是**所有进程共用**的（摘要条目落在共享 PG，
//! 向量落在共享 Qdrant；而衰减的判词与写回只经条目存储，向量只在写回时顺带更新），
//! 所以这条循环是一个单写者角色：两个进程各扫一遍，同一个条目的重要性就在同一段
//! 时间里乘两次，比策略说的更快跌到归档地板，而 `cogneva_memory_decay_total` 也变
//! 成两倍。谁来扫由租约决定，见 [`MEMORY_DECAY_ROLE`]。
//!
//! 租约只回答「谁**可以**扫」，不回答「扫的那个看到的是不是同一份条目」。一个进程
//! 若在关闭严格持久化时把条目存储降级成了进程内的那份，它扫到的是空命名空间、每轮
//! 都记 `idle`，而租约已经让它成为唯一扫的人——共享的记忆再也无人衰减，读数却说
//! 「没东西可衰减」。不够格的进程因此靠**不起这条循环**被排除在候选集之外，而不是
//! 靠租约拒它。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use cog_core::{MemoryBackend, MetricsBackend, OwnerLeaseBroker, ShutdownSignal};

use crate::config::MaintenanceConfig;

/// 这条循环在进程存活注册表里的名字，也是 `cogneva_loop_tick_age_seconds{loop=…}`
/// 的取值。
pub const MEMORY_DECAY_LOOP: &str = "memory_decay";

/// 这条循环在租约上的角色名：谁在衰减这份共享的记忆。
///
/// 取这个名字而不是复用循环名，是因为两个面读的是两个不同的东西：循环名是存活
/// 与角色读数的标签（`{loop=…}`），角色名是租约上那一行
/// （`cogneva_owner_leases.role`）。两者现在同值，但改一个不等于改了另一个。
pub const MEMORY_DECAY_ROLE: &str = "memory_decay";

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
/// `role` 是这棵进程能拿到的租约中介（由持有共用库的插件发布）：有它就先问
/// 「这个角色是不是我的」，问不到就不动手；没有它就照旧自己扫自己的——按共用库
/// 不存在处理。**这个兜底押在一句断言上**：「中介缺席 ⟺ 没有第二个写者」，而它只在
/// 缺席的成因只可能是「这个部署本来就没有共用库」时成立。发布方建中介失败那一支
/// 同样只留下 `None`（只 warn 不报错），那时库在、第二个写者也可能在。⇒ 「有没有
/// 中介」不能当资格判据，它量的是有没有仲裁，不是有没有别人在写同一份数据。
///
/// 循环随进程的 `shutdown` 一起停。它不是「没有停止信号」的那种循环，但也没有
/// 别人能提前叫停它：任何**没人要求**的提前退出仍然要被存活族记账，这由
/// `loop_health` 的死亡哨兵负责（见那里的 `watch_death`）。
pub fn spawn_decay_loop(
    backend: Arc<dyn MemoryBackend>,
    metrics: Arc<dyn MetricsBackend>,
    config: MaintenanceConfig,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    shutdown: ShutdownSignal,
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
        arbitrated = role.is_some(),
        "Memory decay maintenance enabled"
    );

    drop(cog_core::loop_health::spawn(
        MEMORY_DECAY_LOOP,
        cog_core::loop_health::Cadence::Periodic(period),
        shutdown.clone(),
        // 每次尝试重建闭包，所以体里用到的东西在这里克隆一份。
        move |beat| {
            let backend = backend.clone();
            let metrics = metrics.clone();
            let namespaces = namespaces.clone();
            let role = role.clone();
            let shutdown = shutdown.clone();
            async move {
                // 角色按**租约自己的节拍**续，而不是按这条循环的节拍问一次：扫描周期是
                // 小时级，租期是分钟级，一轮问一次的话两轮之间这个进程早就不是持有者了，
                // 而它那时还在扫，另一个进程则已经接手了同一份数据——正是租约要挡的重复。
                // 循环体里仍然每轮问一次，因为丢了角色要在**丢的那一轮**停下来。
                let hold =
                    cog_core::RoleHold::start(role, MEMORY_DECAY_ROLE, beat.clone(), &shutdown);

                let mut ticker = tokio::time::interval(period);
                // 第一次 tick 立即返回；先吃掉它，让第一轮扫描落在起点之后一个
                // 周期，而不是插件刚 start 就和启动期抢同一把锁。
                ticker.tick().await;
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    // 这一份数据只该有一个进程在扫。答案由 beat 记（拿到／被别人拿着／
                    // 问不到），这里只照答案决定动手不动手：问不到也**不**动手——不知道
                    // 是不是只有自己，和知道只有自己不是一回事。
                    if !hold.may_act().await {
                        continue;
                    }
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
        async fn presign_raw(
            &self,
            _ns: &str,
            _id: &str,
            _expiry_secs: u64,
        ) -> SFResult<Option<String>> {
            Ok(None)
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

    /// 一个按剧本作答的租约：`held` 说的是「这个角色是不是本进程的」，并记下问了
    /// 几次——「没扫」要和「还没走到门口」分开，靠的就是这个计数。
    struct ScriptedLease {
        held: bool,
        asks: AtomicUsize,
    }

    impl ScriptedLease {
        fn new(held: bool) -> Arc<Self> {
            Arc::new(Self {
                held,
                asks: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl cog_core::OwnerLease for ScriptedLease {
        fn role(&self) -> &str {
            MEMORY_DECAY_ROLE
        }

        async fn try_hold(&self) -> SFResult<bool> {
            self.asks.fetch_add(1, Ordering::SeqCst);
            Ok(self.held)
        }
    }

    struct ScriptedBroker(Arc<ScriptedLease>);

    impl cog_core::OwnerLeaseBroker for ScriptedBroker {
        fn lease(&self, _role: &str, _ttl: Duration) -> Arc<dyn cog_core::OwnerLease> {
            self.0.clone()
        }
    }

    /// 扫描周期压到一秒，让测试能在秒级看见一轮。周期本身不是被测的东西。
    fn fast_config() -> MaintenanceConfig {
        MaintenanceConfig {
            decay_interval_secs: 1,
            ..MaintenanceConfig::default()
        }
    }

    fn spawn_with_lease(
        backend: Arc<ScriptedBackend>,
        lease: Arc<ScriptedLease>,
        shutdown: &cog_core::ShutdownSignal,
    ) {
        let broker: Arc<dyn cog_core::OwnerLeaseBroker> = Arc::new(ScriptedBroker(lease));
        spawn_decay_loop(
            backend as Arc<dyn MemoryBackend>,
            Arc::new(crate::NoopMetricsBackend::new()),
            fast_config(),
            Some(broker),
            shutdown.clone(),
        );
    }

    /// 轮询到条件成立或超时，返回是否成立。等待的是「循环走到了哪里」，不是
    /// 一个假想的耗时。
    async fn wait_until(mut done: impl FnMut() -> bool) -> bool {
        for _ in 0..150 {
            if done() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        done()
    }

    /// 角色在别的进程手里时，这一轮一个命名空间都不扫。断言「没扫」的前提是它
    /// **确实走到了门口**——问过租约——否则这条测试分不清「守住了」和「还没起来」。
    #[tokio::test]
    async fn a_round_scans_nothing_while_another_process_holds_the_role() {
        let backend = ScriptedBackend::new(0, 0, false);
        let shutdown = cog_core::ShutdownSignal::default();
        let lease = ScriptedLease::new(false);
        spawn_with_lease(backend.clone(), Arc::clone(&lease), &shutdown);

        let asked = wait_until(|| lease.asks.load(Ordering::SeqCst) > 0).await;
        // 再放一段时间，让「问过之后还是没扫」不只是同一瞬间的巧合。
        tokio::time::sleep(Duration::from_millis(1200)).await;
        shutdown.trigger();

        assert!(
            asked,
            "the loop never reached the lease, so this test has not shown what it claims"
        );
        assert_eq!(
            backend.passes.load(Ordering::SeqCst),
            0,
            "another process holds the decay role; this one must not decay the shared \
             entries again"
        );
    }

    /// 拿着角色的一轮照扫：守住的判词不能顺手把活也停了。
    #[tokio::test]
    async fn the_holder_scans_the_namespaces() {
        let backend = ScriptedBackend::new(0, 0, false);
        let shutdown = cog_core::ShutdownSignal::default();
        let lease = ScriptedLease::new(true);
        spawn_with_lease(backend.clone(), Arc::clone(&lease), &shutdown);

        let scanned = wait_until(|| backend.passes.load(Ordering::SeqCst) > 0).await;
        shutdown.trigger();

        assert!(
            scanned,
            "the process holds the decay role and its round did not reach the backend"
        );
        assert_eq!(
            *backend.last_ns.lock().unwrap(),
            Some("default".to_string()),
            "the round has to scan the configured namespaces, not just any call"
        );
    }

    /// 默认扫描周期比租期长，所以「一轮问一次」不足以持有角色：两轮之间角色已经
    /// 易主，而那一轮还没扫完。这条断言把 `RoleHold`（按租约节拍续）钉在默认配置
    /// 上——把默认周期调到租期以内、或把续期方式换回一轮一次的人，会在这里撞上。
    #[test]
    fn the_default_scan_period_outlasts_a_lease_term() {
        let period = Duration::from_secs(MaintenanceConfig::default().decay_interval_secs);
        assert!(
            period > cog_core::owner_lease::TERM,
            "the decay loop declares a period of {period:?}, inside the lease term {:?}; a claim \
             asked once per round would be enough there, and this loop would not have to renew on \
             the lease's own cadence",
            cog_core::owner_lease::TERM
        );
    }
}
