//! 把「环境拒绝」被误当成学习写下的条目，从记忆层里清扫掉。
//!
//! 上游断供、被限速、确定性环境失败的那些轮次，过去每一轮都落一条 Correction：
//! 摘要和细节都是那段上游错误正文。它不是关于这份工作的一份经验——同一个断言
//! 在模式选择器上早就是「这一轮不算这个模式的试验」——而写入端现在也不再产出
//! 它。这条循环处理的是它以前写下的行。
//!
//! 存量不是小数目：实测 2026-11-03 的读数里，reflection 命名空间 9860 条中有
//! 4735 条属于这一类，实测占掉该命名空间 202 MB 中的 190 MB。而每次读学习库
//! 都要把整个命名空间拉进内存、逐条嵌入一遍，于是这批行让每一次读都贵九成四。
//!
//! 删两样东西，缺一不可：归档原文和派生行。只删派生行是错的——补齐循环正是按
//! 归档重建缺派生层的条目，删完下一轮就原样长回来，而读数会说「扫干净了」。
//! 两样的身份是同一个 id：写入时归档与派生层用的是同一个学习 id。
//!
//! 判据与写入端共用 `cog_core` 的那两个前缀常量，不另写一份字符串：两边各写
//! 一次，改措辞时就只有一边跟着变，另一边从此匹配不到——那读起来正好是「这类
//! 条目不再产生了」。
//!
//! 这一轮读的量与读学习库的那一次相同，所以它没有引入新的一类代价；第一轮之后
//! 命名空间里剩下的就是真正该在的那些行。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use cog_core::contract::outcome::{
    declares, TERMINAL_ENV_FAILURE_PREFIX, UPSTREAM_UNAVAILABLE_PREFIX,
};
use cog_core::{
    MemoryBackend, MetricsBackend, OwnerLeaseBroker, SFResult, SchemaEntry, ShutdownSignal,
};

/// 这条循环在进程存活注册表里的名字，也是 `cogneva_loop_tick_age_seconds{loop=…}`
/// 的取值。
pub const REFUSAL_SWEEP_LOOP: &str = "memory_refusal_sweep";

/// 这条循环在租约上的角色名。删的是共用条目存储里的行，所以它和衰减一样是
/// 单写者角色：两个进程一起删并不会删坏（删是幂等的），但两个进程会各记一遍
/// `cogneva_memory_dropped_entries_total`，同一批行就被数了两回。
pub const REFUSAL_SWEEP_ROLE: &str = "memory_refusal_sweep";

/// 一轮清扫的结果。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    /// 归档原文与派生行都删掉了的条目数。
    pub removed: usize,
    /// 认出来了却没能删掉的条目数（先删归档，删不掉就不动派生行）。
    pub failed: usize,
}

/// 一条条目是不是「按环境拒绝写下的那条学习」。
///
/// 只看内容声明了什么，不看写入方是谁：这条判据要与写入端的终止判据是同一个，
/// 否则清扫会把写入端认为合法的行删掉，或者留下写入端已经不再产出的行。
fn is_environment_refusal(entry: &SchemaEntry) -> bool {
    let Some(details) = entry.properties.get("details").and_then(|v| v.as_str()) else {
        return false;
    };
    declares(details, UPSTREAM_UNAVAILABLE_PREFIX) || declares(details, TERMINAL_ENV_FAILURE_PREFIX)
}

/// 对一个命名空间做一轮清扫。
pub async fn sweep_namespace(
    backend: &Arc<dyn MemoryBackend>,
    namespace: &str,
) -> SFResult<SweepReport> {
    let entries = backend.list_schema(namespace).await?;
    let mut report = SweepReport::default();
    for entry in entries.iter().filter(|e| is_environment_refusal(e)) {
        // 归档先走：只删派生行的话，补齐循环会按归档把它原样重建，而这一轮
        // 已经把它记成「删掉了」。归档删失败就不动派生行——留下一个查不到的
        // 孤儿，下一轮还能再试一次。
        if let Err(e) = backend.delete_raw(namespace, &entry.id).await {
            report.failed += 1;
            warn!(namespace, id = %entry.id, error = %e,
                "refusal sweep: the archive would not delete, leaving the row for the next pass");
            continue;
        }
        match backend.delete_schema(namespace, &entry.id).await {
            Ok(()) => report.removed += 1,
            Err(e) => {
                report.failed += 1;
                warn!(namespace, id = %entry.id, error = %e,
                    "refusal sweep: the archive is gone but the derived row would not delete");
            }
        }
    }
    Ok(report)
}

/// 起一条进程级清扫循环。
///
/// 间隔为 0 表示不起这条循环。第一轮落在起点后一个周期，而不是插件刚 start
/// 就扫：启动期已经有补齐与回填在跑，多一条同量级的大读没有好处。
pub fn spawn_refusal_sweep_loop(
    backend: Arc<dyn MemoryBackend>,
    metrics: Arc<dyn MetricsBackend>,
    interval: Duration,
    namespaces: Vec<String>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    shutdown: ShutdownSignal,
) {
    if interval.is_zero() {
        info!("Memory refusal sweep disabled (maintenance.refusal_sweep_interval_secs=0)");
        return;
    }
    info!(
        interval_secs = interval.as_secs(),
        ?namespaces,
        arbitrated = role.is_some(),
        "Memory refusal sweep enabled"
    );

    drop(cog_core::loop_health::spawn(
        REFUSAL_SWEEP_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        move |beat| {
            let backend = backend.clone();
            let metrics = metrics.clone();
            let namespaces = namespaces.clone();
            let role = role.clone();
            let shutdown = shutdown.clone();
            async move {
                let hold =
                    cog_core::RoleHold::start(role, REFUSAL_SWEEP_ROLE, beat.clone(), &shutdown);

                let mut ticker = tokio::time::interval(interval);
                // 第一次 tick 立即返回；先吃掉它，让第一轮清扫落在起点之后一个
                // 周期，与衰减循环同一处置。
                ticker.tick().await;
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    if !hold.may_act().await {
                        continue;
                    }
                    for namespace in &namespaces {
                        match sweep_namespace(&backend, namespace).await {
                            Ok(report) if report.removed > 0 || report.failed > 0 => {
                                info!(
                                    namespace = %namespace,
                                    removed = report.removed,
                                    failed = report.failed,
                                    "removed memory entries that recorded an environment refusal as a lesson"
                                );
                                record_removed(&metrics, namespace, report.removed).await;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!(namespace = %namespace, error = %e, "Memory refusal sweep failed");
                            }
                        }
                    }
                }
            }
        },
    ));
}

/// 把「离开可搜索层」的条数记进闭集计数器。写不进去只 warn：读数失败不该拖垮
/// 维护循环。
///
/// 与衰减归档共用 `cogneva_memory_dropped_entries_total`——两件事说的都是「这些
/// 条目不再在可搜索层里」，单位也都是条目；分开记名会让同一件事有两个来源。
async fn record_removed(metrics: &Arc<dyn MetricsBackend>, namespace: &str, removed: usize) {
    if removed == 0 {
        return;
    }
    let mut labels = HashMap::new();
    labels.insert(crate::LABEL_NAMESPACE.to_string(), namespace.to_string());
    if let Err(e) = metrics
        .record_counter(
            cog_core::metric_names::MEMORY_DROPPED_ENTRIES_TOTAL,
            removed as f64,
            labels,
        )
        .await
    {
        warn!(error = %e, "Memory refusal sweep: could not record dropped entries");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryMemoryBackend;
    use cog_core::{RawSource, SchemaKind, SourceRef};

    fn learning(id: &str, details: &str) -> (RawSource, SchemaEntry) {
        let payload = serde_json::json!({ "id": id, "summary": "s", "details": details });
        let raw = RawSource::new(
            id,
            "reflection",
            cog_core::CONTENT_TYPE_JSON,
            serde_json::to_vec(&payload).unwrap(),
        );
        let mut entry = SchemaEntry::new(
            id,
            "reflection",
            SchemaKind::Learning,
            id,
            id,
            SourceRef::new(id, "test"),
        );
        entry.properties = payload;
        (raw, entry)
    }

    async fn backend_holding(entries: Vec<(RawSource, SchemaEntry)>) -> Arc<dyn MemoryBackend> {
        let backend = MemoryMemoryBackend::new();
        for (raw, entry) in entries {
            backend.archive_raw(&raw).await.unwrap();
            backend.store_schema("reflection", &entry).await.unwrap();
        }
        Arc::new(backend)
    }

    #[tokio::test]
    async fn a_refusal_loses_both_layers_and_a_real_lesson_keeps_both() {
        let noise = learning(
            "LRN-noise",
            "upstream_unavailable: environment_error: LLM upstream refused (server_error): HTTP 429",
        );
        let real = learning("LRN-real", "evaluator: criteria unmet — no test artifacts");
        let backend = backend_holding(vec![noise, real]).await;

        let report = sweep_namespace(&backend, "reflection").await.unwrap();
        assert_eq!(report.removed, 1, "one row is a refusal, the other is not");
        assert_eq!(report.failed, 0);

        assert!(
            backend
                .get_schema("reflection", "LRN-noise")
                .await
                .unwrap()
                .is_none(),
            "the refusal's derived row is gone"
        );
        assert!(
            backend
                .get_raw("reflection", "LRN-noise")
                .await
                .unwrap()
                .is_none(),
            "the archive is gone too: leaving it lets the repair loop rebuild the row"
        );

        assert!(
            backend
                .get_schema("reflection", "LRN-real")
                .await
                .unwrap()
                .is_some(),
            "a lesson about the work stays"
        );
        assert!(backend
            .get_raw("reflection", "LRN-real")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn a_second_pass_finds_nothing_and_writes_nothing() {
        let noise = learning(
            "LRN-noise",
            "terminal_env_failure: upstream 503 on the first call",
        );
        let backend = backend_holding(vec![noise]).await;

        assert_eq!(
            sweep_namespace(&backend, "reflection")
                .await
                .unwrap()
                .removed,
            1
        );
        assert_eq!(
            sweep_namespace(&backend, "reflection").await.unwrap(),
            SweepReport::default(),
            "a namespace already swept must read as clean rather than keep counting"
        );
    }

    #[tokio::test]
    async fn an_entry_that_only_mentions_a_refusal_mid_text_is_left_alone() {
        let quoted = learning(
            "LRN-quoted",
            "evaluator: criteria unmet — the log said \"upstream_unavailable\" once",
        );
        let backend = backend_holding(vec![quoted]).await;
        assert_eq!(
            sweep_namespace(&backend, "reflection").await.unwrap(),
            SweepReport::default(),
            "a declaration is a labelled prefix, not a word quoted somewhere in the text"
        );
    }
}
