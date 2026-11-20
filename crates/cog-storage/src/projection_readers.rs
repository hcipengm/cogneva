//! 投影表的读者声明。
//!
//! 一张投影表是「从别处重算出来的表」：真源在别的地方，它存在是因为那个问题用真源
//! 回答太贵。这句话必须被写下来，因为一张投影表与一份重复的副本在库里长得一模一样，
//! 而副本的失败方式是没有失败——没人读它，它也不会报错，只会一直跟着真源重算，占着
//! 库、占着重算的时间，还让读的人以为那份事实在这里。
//!
//! 声明放在这一 crate，是因为建这些表的人在这里：`postgres::*` 的每一个 `DDL` 常量
//! 与每一次 `CREATE TABLE` 都住在同一个 crate，加一张投影表的人离这一份名单一步远。
//! 判据在 `crates/cogneva/tests/projection_has_a_declared_reader_test.rs`，它从 DDL
//! 文本里算出带 `as_of` 水位的表，两头都判。

/// 一张投影表，以及它为什么存在、谁在读它。
#[derive(Debug, Clone, Copy)]
pub struct ProjectionReader {
    /// 表名，与 `CREATE TABLE` 里的写法逐字相同。
    pub table: &'static str,
    /// 读它的地方，写成树里的一个路径；判据会去看那个路径在不在。
    pub reader: &'static str,
    /// 为什么那个问题不能用真源回答。
    pub why_not_the_source: &'static str,
    /// 它由**哪些事实**重算出来。一句话，点名那份正本。
    ///
    /// 这一格存在的理由：一张投影表最坏的失败不是算错，是**没人知道它该跟谁说一致**。
    /// 「由哪些事实重算」写不出来，说明这张表不是任何事实的投影，只是一份副本。
    pub rebuilt_from: &'static str,
    /// 重写它的那处代码，写成树里的一个路径；判据会去看那个路径在不在。
    ///
    /// 有重写路线不等于有重写**测试**——那一半还没做（见台账 S33）。这一格先保证：
    /// 说得出这张表由什么重算的人，指得出按什么把它重算一遍。
    pub rebuild: &'static str,
}

/// 每一张投影表的读者声明。
///
/// 「投影表」的判据是水位列（`as_of` 开头的那一列），不是名字：有水位说明这张表
/// 自报「我读到哪了」，而那是重建得出的表才需要说的话。
pub const PROJECTION_READERS: &[ProjectionReader] = &[
    ProjectionReader {
        table: "cog_metric_counter_totals",
        reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
        why_not_the_source: "这一行是被观测的进程自己累加的数，真源是 Prometheus 里那条序列；从真源读一次要走一趟网络、并且 Prometheus 不在时读不到，而屏上的每一次刷新都在问它。库里的这一份是投影，用 `cogneva rebuild-metric-projections` 可以按真源整表重写。",
        rebuilt_from: "Prometheus 里 `<name>_total` 那条序列（进程累加、由观测面保存）",
        rebuild: "crates/cogneva/src/metric_projection_rebuild.rs",
    },
    ProjectionReader {
        table: "cog_metric_histogram_buckets",
        reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
        why_not_the_source: "直方图的每档观测数由进程累加、由 Prometheus 保存，从真源算一次 `histogram_quantile` 要读整条序列的历史；库里这一份是那一次累加的投影，重建命令按真源整表重写。",
        rebuilt_from: "Prometheus 里 `<name>_bucket` 那条序列的每一档",
        rebuild: "crates/cogneva/src/metric_projection_rebuild.rs",
    },
    ProjectionReader {
        table: "cog_metric_histogram_sums",
        reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
        why_not_the_source: "观测值之和与每档计数同源：真源是 Prometheus 的 `<name>_sum`。库里这一份是投影，重建命令按真源整表重写。",
        rebuilt_from: "Prometheus 里 `<name>_sum` 那条序列",
        rebuild: "crates/cogneva/src/metric_projection_rebuild.rs",
    },
    ProjectionReader {
        table: "task_current",
    reader: "crates/cog-storage/src/postgres/task_ledger.rs",
    why_not_the_source: "台账是一串永不改写的转移行，「现在哪些任务还能动」要读每条任务的全部历史再折一次，而派发每一轮都问这一个问题；投影把那次折叠放在写时做掉，读时是一行一次索引。",
    rebuilt_from: "task 台账里只追加的转移行（每一条任务的完整历史）",
    rebuild: "crates/cog-storage/src/postgres/task_ledger.rs",
}];
