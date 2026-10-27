//! 项目面写操作的审计出口。
//!
//! 写操作要留下「谁、何时、对哪个资源、改前改后」。这条信息有两个读者：安全评审问
//! 「这张任务被谁改过、改成什么」，事故复盘问「谁在什么时候把这张卡挪走了」。两者
//! 问的都是**按资源**取的一段历史，而哈希链是按顺序取的一整条——它证明历史没被动过，
//! 不回答「这一张的改动」。
//!
//! 所以这里是一个出口、一个落点：`audit_logs` 表（写者与读者在
//! `cog_storage::postgres::project_audit_log`）。出口做成进程级单例而不是
//! `GatewayState` 的一个槽：那个结构每加一格，五个测试夹具与生产装配都要跟着改一遍，
//! 而漏掉的那一处不会报错——它会让审计安静地不写。这里的接线在网关插件里发生一次，
//! 缺席时 [`record`] 直接把「没接上」记成一个计数，读数面上看得见。
//!
//! 落库失败不让写操作失败，但也不许静默：编排侧的状态已经改了，这时候回 500 只会让
//! 客户端重试一次、把改动再做一遍；而一声不响地丢掉这一行，就是「改过但查不到」——
//! 那种洞只有下一次审计才会发现。所以失败进计数，计数进 `/metrics`。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use cog_storage::postgres::{AuditEntry, ProjectAuditLog};
use tracing::warn;

/// 装好的那个写者。`None` 是「这个进程没接上审计存储」，与「接上了但一条没写」不同。
fn sink_slot() -> &'static RwLock<Option<Arc<dyn ProjectAuditLog>>> {
    static SLOT: OnceLock<RwLock<Option<Arc<dyn ProjectAuditLog>>>> = OnceLock::new();
    SLOT.get_or_init(|| RwLock::new(None))
}

/// 接线：网关插件从存储插件手里拿到写者时调一次。
pub fn install(sink: Arc<dyn ProjectAuditLog>) {
    if let Ok(mut slot) = sink_slot().write() {
        *slot = Some(sink);
    }
}

/// 这台进程的写者，没接上时 `None`。
pub fn sink() -> Option<Arc<dyn ProjectAuditLog>> {
    sink_slot().read().ok().and_then(|s| s.clone())
}

/// 只给测试用：装上/摘下写者，让同一进程里的用例互不污染。
#[doc(hidden)]
pub fn install_for_test(sink: Option<Arc<dyn ProjectAuditLog>>) {
    if let Ok(mut slot) = sink_slot().write() {
        *slot = sink;
    }
}

/// 三个计数。
///
/// `writes` 是被要求留下的行数，`failures` 是其中没落成的，`unwired` 是根本没有写者
/// 的。三个分开是因为修法不同：`unwired` 去查装配，`failures` 去查库，`writes` 只是
/// 分母——没有它，「失败 3 次」说不出是千分之三还是全部。
#[derive(Debug, Default)]
pub struct AuditWrites {
    pub writes: AtomicU64,
    pub failures: AtomicU64,
    pub unwired: AtomicU64,
}

pub fn counters() -> &'static AuditWrites {
    static C: OnceLock<AuditWrites> = OnceLock::new();
    C.get_or_init(AuditWrites::default)
}

pub const WRITES_TOTAL: &str = "cogneva_project_audit_writes_total";
pub const FAILURES_TOTAL: &str = "cogneva_project_audit_write_failures_total";
pub const UNWIRED_TOTAL: &str = "cogneva_project_audit_unwired_total";

/// 那一组计数的 Prometheus 文本。
pub fn readings() -> Vec<cog_core::observability::RawMetric> {
    let c = counters();
    vec![
        cog_core::observability::RawMetric::new(
            WRITES_TOTAL,
            c.writes.load(Ordering::Relaxed) as f64,
        ),
        cog_core::observability::RawMetric::new(
            FAILURES_TOTAL,
            c.failures.load(Ordering::Relaxed) as f64,
        ),
        cog_core::observability::RawMetric::new(
            UNWIRED_TOTAL,
            c.unwired.load(Ordering::Relaxed) as f64,
        ),
    ]
}

/// 记一行。调用方在写操作成功之后调它，**不**因为它失败而回滚或报错。
pub async fn record(entry: AuditEntry) {
    let c = counters();
    c.writes.fetch_add(1, Ordering::Relaxed);
    let action = entry.action.clone();
    match sink() {
        Some(sink) => {
            if let Err(e) = sink.record(entry).await {
                c.failures.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, action = %action, "project audit row did not land");
            }
        }
        None => {
            c.unwired.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// 一个项目的改动时间线，最近的在前。
///
/// 三种结果要分开，两个 `Result` 不够：`Ok(None)` 是「这台部署没有审计存储」，
/// `Err` 是「有存储但这次没读出来」，`Ok(Some(空))` 是「读了，这个项目没有改动」。
/// 把中间那种塌进第一种，界面就会在一次数据库抖动之后对客户说「这台部署没有审计」——
/// 一句关于部署的假话，而它恰好是最难被客户反驳的那类：他看不到库。
pub async fn recent(
    project_id: &str,
    limit: i64,
) -> Result<Option<Vec<cog_storage::postgres::AuditRow>>, String> {
    let Some(sink) = sink() else {
        return Ok(None);
    };
    sink.recent(project_id, limit)
        .await
        .map(Some)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        rows: Mutex<Vec<AuditEntry>>,
        fail: bool,
    }

    #[async_trait]
    impl ProjectAuditLog for Recorder {
        async fn record(&self, entry: AuditEntry) -> cog_core::SFResult<()> {
            if self.fail {
                return Err(cog_core::SFError::Database("boom".into()));
            }
            self.rows.lock().unwrap().push(entry);
            Ok(())
        }

        async fn recent(
            &self,
            _project_id: &str,
            _limit: i64,
        ) -> cog_core::SFResult<Vec<cog_storage::postgres::AuditRow>> {
            // 读的那一半也跟着 `fail` 走：一个只会写失败、读永远成功的夹具，
            // 恰好盖住「读失败被报成没有审计」这个最贵的错。
            if self.fail {
                return Err(cog_core::SFError::Database("boom".into()));
            }
            Ok(vec![])
        }
    }

    /// 这四个测试共用同一个进程级单例（sink 与计数器），并行跑就是互相踩：
    /// 一个测试刚装上夹具，另一个把它换成 `None`，断言于是看到别人的数。
    /// 串起来是唯一诚实的做法——按文件名分片跑（`--test-threads=1`）也能绿，
    /// 但那样是把正确性挂在别人怎么调 cargo 上。
    ///
    /// 用 tokio 的互斥量而不是 std 的：守卫要在测试体里跨过 `record(…).await`，
    /// std 的守卫跨 await 点持有既会被 clippy 拦（`await_holding_lock`），也会在
    /// 单线程运行时上把同线程的其它任务卡死。
    async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
        static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        LOCK.lock().await
    }

    fn entry() -> AuditEntry {
        AuditEntry {
            actor: "system".into(),
            action: "project.card_moved".into(),
            resource_type: "task".into(),
            resource_id: "t1".into(),
            project_id: "p1".into(),
            before: serde_json::json!({"status": "todo"}),
            after: serde_json::json!({"status": "running"}),
        }
    }

    /// 接上了就真的写进去，改前改后都在。
    #[tokio::test]
    async fn a_wired_sink_receives_the_row() {
        let _serial = serial().await;
        let recorder = Arc::new(Recorder::default());
        install_for_test(Some(recorder.clone()));
        let before = counters().writes.load(Ordering::Relaxed);
        record(entry()).await;
        let rows = recorder.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].before["status"], "todo");
        assert_eq!(rows[0].after["status"], "running");
        assert!(counters().writes.load(Ordering::Relaxed) > before);
        install_for_test(None);
    }

    /// 没接上不许当成「写过了」：它进 unwired 计数，读者才分得开「没审计」与「没改动」。
    #[tokio::test]
    async fn an_unwired_process_counts_itself_as_unwired() {
        let _serial = serial().await;
        install_for_test(None);
        let before = counters().unwired.load(Ordering::Relaxed);
        record(entry()).await;
        assert_eq!(counters().unwired.load(Ordering::Relaxed), before + 1);
        // 同一条记录同时被数进 writes——「未接上 3 次」要有分母才知道是不是全部。
        assert!(counters().writes.load(Ordering::Relaxed) > 0);
    }

    /// 落库失败被数出来，且不吞掉——写操作已经发生，这一行丢了必须有人看得见。
    #[tokio::test]
    async fn a_failing_sink_is_counted_and_the_write_still_returns() {
        let _serial = serial().await;
        let recorder = Arc::new(Recorder {
            fail: true,
            ..Default::default()
        });
        install_for_test(Some(recorder));
        let before = counters().failures.load(Ordering::Relaxed);
        record(entry()).await;
        assert_eq!(counters().failures.load(Ordering::Relaxed), before + 1);
        install_for_test(None);
    }

    /// 「没接存储」「接了但这次没读到」「读到但一条没有」是三种结果，读面必须分得开。
    ///
    /// 塌成两种的代价不对称：一次数据库抖动会被报成「这台部署没有审计」，而客户
    /// 既看不到库，也没有办法反驳这句话。
    #[tokio::test]
    async fn a_read_that_failed_is_not_the_same_answer_as_no_store() {
        let _serial = serial().await;
        // 没接存储
        install_for_test(None);
        assert!(
            matches!(recent("p1", 10).await, Ok(None)),
            "没接存储时要说「没有审计」，不是「读了没有」"
        );

        // 接了、读失败
        install_for_test(Some(Arc::new(Recorder {
            fail: true,
            ..Default::default()
        })));
        let err = recent("p1", 10).await;
        assert!(
            err.is_err(),
            "读失败必须与「没有审计存储」分开，实际 {err:?}"
        );

        // 接了、读到了、这个项目没有改动
        install_for_test(Some(Arc::new(Recorder::default())));
        match recent("p1", 10).await {
            Ok(Some(rows)) => assert!(rows.is_empty(), "空表就是空表"),
            other => panic!("读到空表应当是 Ok(Some([]))，实际 {other:?}"),
        }
        install_for_test(None);
    }
}
