//! principle: feedback_a_switch_that_exists_before_its_reading_is_a_decision_nobody_measured
//!
//! 事件流在**四条触发读数都为假**的时候，只能有一条写路径：直接写 PostgreSQL。
//!
//! 换存储不是「PG 换 NATS」，而是写入口前移（JetStream 收追加、物化器批量写 PG、PG 仍是
//! 正本）。那条路只有在下面四条之一量到为真时才允许存在：
//!
//!   1. 追加 p99 持续大于 UI 刷新周期的一半
//!   2. `event_stream_lag_seconds{kind}` 持续上升
//!   3. vacuum / 最老事务年龄追不上
//!   4. `store_growth` 的余量除以增速，短于保留目标
//!
//! 今天这四条一条都还没有发布，所以**切换项不许先在树里**：一个还没被读数要出来的
//! 开关，是有人先拍了板、再去找理由。NATS 作为进程间总线已经存在，那是另一件事——
//! 它不做 `cogneva_event` 的写入口。
//!
//! 判据是树判据，不是运行读数：`cogneva_event` 的写语句只出现在那一个 store 里，且
//! 没有任何地方用 JetStream 发布这一类事件。

// 仓库根只有一份算法（`common::routes::repo_root()`）：各写一遍时，根一漂读数就
// 变成零，与「没有题目」同形。
use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;

/// 事件流的写入口：每一处 `INSERT INTO cogneva_event` 都必须是**登记在案的写者**，
/// 而且必须落在 store 层（`crates/cog-storage/src/postgres/`）。
///
/// 这条判据原本判「只有一个文件写」，那是单写者时代的形状。现在每一种事实有它自己的
/// 类型化写者（调用、工具调用、轮次、候选变更、擦除…），判断的落点因此从「几处」变成
/// 「是不是登记过的那几处」——判据的力没有减：多出来的那一处仍然红，只是红在
/// 「谁批准了这条写路径」上，而不是红在一个会随事实种类增长的数上。
///
/// 它报的不是「谁写了」，是「有几条写路径」：第二处出现就是第二条路径，无论它当时
/// 被哪个开关挡着——开关默认关着的写路径，与没有这条路径，在树里长得不一样。
const WRITERS: &[(&str, &str)] = &[
    (
        "crates/cog-storage/src/postgres/llm_call_events.rs",
        "一次上游调用（`llm_call`）",
    ),
    (
        "crates/cog-storage/src/postgres/tool_call_events.rs",
        "一次工具调用（`tool_call`）",
    ),
    (
        "crates/cog-storage/src/postgres/round_events.rs",
        "一轮开工/关轮（`round_opened` / `round_closed`）",
    ),
    (
        "crates/cog-storage/src/postgres/change_candidate_store.rs",
        "候选变更的四个阶段（`change_candidate` / `change_measured` / `change_accepted` / `change_rejected`）",
    ),
    (
        "crates/cog-storage/src/postgres/task_ledger.rs",
        "任务的每一次状态转移（`task_transition`）",
    ),
    (
        "crates/cog-storage/src/postgres/redaction.rs",
        "一段正文被销毁（`redacted`）",
    ),
    (
        "crates/cog-storage/src/postgres/usage_backfill.rs",
        "旧用量行回填成调用事实（`llm_call`）",
    ),
    (
        "crates/cog-storage/src/postgres/intent_events.rs",
        "一句到达与它的接手（`intent_received` / `intent_triaged`）",
    ),
];

#[test]
fn every_insert_site_is_a_registered_writer() {
    let root = repo_root();
    let mut inserts: Vec<String> = Vec::new();
    for (path, text) in production_texts(&root) {
        let upper = text.to_uppercase();
        if upper.contains("INSERT INTO COGNEVA_EVENT") {
            inserts.push(
                path.strip_prefix(&root)
                    .unwrap_or(&path)
                    .display()
                    .to_string(),
            );
        }
    }
    let registered: Vec<&str> = WRITERS.iter().map(|(f, _)| *f).collect();
    let unknown: Vec<&String> = inserts
        .iter()
        .filter(|p| !registered.contains(&p.as_str()))
        .collect();
    assert!(
        unknown.is_empty(),
        "这些文件写事件流却不在登记表里：{unknown:?}——新写者要么并进已有的那一族，         要么在 `WRITERS` 里登记它写的是哪种事实；写路径只有登记过的那几条"
    );
    let inside_store: Vec<&String> = inserts
        .iter()
        .filter(|p| !p.starts_with("crates/cog-storage/src/postgres/"))
        .collect();
    assert!(
        inside_store.is_empty(),
        "这些写入口在 store 层之外：{inside_store:?}——换存储之前，写入口只许落在 \
         `crates/cog-storage/src/postgres/` 里"
    );
    let stale: Vec<&&str> = registered
        .iter()
        .filter(|f| !inserts.iter().any(|p| p == *f))
        .collect();
    assert!(
        stale.is_empty(),
        "这些登记的写者已经不再写事件流了：{stale:?}——删掉登记，别让名单替空处作保"
    );
}

/// `文本里是否把 cogneva_event 当作一个名字提到`——不是子串。
///
/// 裸子串会把指标名 `cogneva_event_bus_lagged_total` 读成「这个文件在写事件流」，
/// 而那是总线滞后读数，与写入口无关。名字后面紧跟 `_` 或字母数字时，它是另一个名字。
fn mentions_the_stream(text: &str) -> bool {
    let mut from = 0;
    while let Some(at) = text[from..].find("cogneva_event") {
        let start = from + at;
        let after = text[start + "cogneva_event".len()..].chars().next();
        let boundary = !matches!(after, Some(c) if c.is_ascii_alphanumeric() || c == '_');
        if boundary {
            return true;
        }
        from = start + "cogneva_event".len();
    }
    false
}

/// 没有 JetStream 发布 `cogneva_event` 的路径。
///
/// 这条判据与上一条分开写：一个把追加塞进总线、再由物化器写库的实现，可以只有登记过的
/// `INSERT`，却已经有两条写路径了。
#[test]
fn no_bus_publishes_the_event_stream() {
    let root = repo_root();
    let mut publishers: Vec<String> = Vec::new();
    for (path, text) in production_texts(&root) {
        if !text.contains("jetstream") && !text.contains("JetStream") {
            continue;
        }
        if mentions_the_stream(&text) {
            publishers.push(
                path.strip_prefix(&root)
                    .unwrap_or(&path)
                    .display()
                    .to_string(),
            );
        }
    }
    assert!(
        publishers.is_empty(),
        "这些文件同时提到 JetStream 与事件流：切换项不能在四条触发读数之前先存在：{publishers:?}"
    );
}
