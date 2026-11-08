//! principle: feedback_a_rollout_replaces_the_log_that_would_have_recorded_it
//!
//! 一条 firing 写进哪里，它就能活多久。写进 Pod 的日志里，它的寿命与写着它的那具
//! 身体同生共死：发布换掉 Pod 的那一刻，正是最需要复盘的那一刻，于是「那条告警说了
//! 什么、通知出去没有」只剩一句 warn 留在已经被删掉的流里。
//!
//! 所以这条原则要的落点是表，不是流：
//!
//!   1. 同一条条件按 `dedup_key` 只占一行，历史不随发布次数增长；
//!   2. 「还没通知」是 `alerts` 上的一列，加一条部分索引，新边沿把它置回 NULL；
//!   3. 通知由扫表驱动，写成功才标记——反过来那一轮会把一条通知永久记成已发；
//!   4. 三种丢掉的通知各留一格 `write_failed` 计数器，不只留一句随 Pod 走掉的 warn；
//!   5. 同 role 的两具身体各署各的名（`writer` 是那一具，`role` 是哪一种），把它们
//!      读成两个写者的分歧，就会在每个上线窗口里报一次假告警。
//!
//! 这一份只读同一修订上的这棵树：判据是「声明与函数体还写着这些词」，不连库、不起
//! 进程。每条红的时候，坏的不是测试，是那句判词又被放回了会被这次发布删掉的地方。

use crate::common;

use common::root::repo_root;
use common::routes::read;
use common::rust_production::{production_text_keeping_comments, relative_name};
use common::rust_text::rust_function_body;

/// 判据只读这三份源。
const ALERT_STORE: &str = "crates/cog-observability/src/alert_store.rs";
const BELL: &str = "crates/cog-observability/src/bell.rs";
const ALERTS: &str = "crates/cog-observability/src/alerts.rs";

/// 一份源的生产半段：`#[cfg(test)]` 模块切掉，注释留着。
///
/// `bell.rs` 的测试模块里有 `mark` 与 `warn!` 的替身，被注释掉的旧顺序也躺在同一份
/// 文件里。切错一边，下面每一条都会拿替身自我通过，所以切法只有这一处，路径也只有
/// 一种写法（`relative_name`）。
fn production(rel: &str) -> String {
    let root = repo_root();
    let path = root.join(rel);
    let name = relative_name(&root, &path);
    assert_eq!(name, rel, "路径只有一种写法");
    production_text_keeping_comments(&read(&path))
}

/// 空白折叠：判据问的是几个词挨在一起，不是它们之间几个空格。
fn collapsed(text: &str) -> String {
    let words: Vec<&str> = text.split_whitespace().collect();
    words.join(" ")
}

/// 一个词在一段文字里出现几次。
fn count(text: &str, needle: &str) -> usize {
    text.matches(needle).count()
}

/// 读的是生产半段，不是整份文件：否则下面四条会拿测试模块里的字自我通过。
#[test]
fn the_production_half_is_the_half_that_is_judged() {
    for rel in [ALERT_STORE, BELL, ALERTS] {
        let text = production(rel);
        assert!(!text.is_empty(), "{rel} 的生产半段是空的");
    }
    let bell = production(BELL);
    assert!(!bell.contains("struct Marks"), "{BELL}: 没切掉测试模块");
    let drained = bell.contains("pub async fn drain_for");
    assert!(drained, "{BELL}: 没接上生产半段");
}

/// 一、firing 是行，不是一行日志：同一条条件按 `dedup_key` 只占一行。
#[test]
fn a_firing_condition_is_one_row_keyed_by_dedup_key() {
    let ddl = collapsed(&production(ALERT_STORE));
    let keyed = ddl.contains("dedup_key TEXT NOT NULL UNIQUE");
    assert!(keyed, "{ALERT_STORE}: dedup_key 不再唯一");
}

/// 二、待办是这张表上的一列：`notified_at` 加一条部分索引，新边沿把它置回 NULL。
#[test]
fn an_unannounced_firing_stays_a_row_the_next_body_can_read() {
    let store = production(ALERT_STORE);
    let ddl = collapsed(&store);
    let column = ddl.contains("IF NOT EXISTS notified_at TIMESTAMPTZ");
    assert!(column, "{ALERT_STORE}: notified_at 不是一列");
    let index = ddl.contains("idx_alerts_unnotified");
    assert!(index, "{ALERT_STORE}: 没有 idx_alerts_unnotified");
    let unread = ddl.contains("AND notified_at IS NULL");
    assert!(unread, "{ALERT_STORE}: 索引不问 notified_at IS NULL");
    let reused = ddl.contains("ON CONFLICT (dedup_key) DO UPDATE");
    assert!(reused, "{ALERT_STORE}: 不再更新同一行");
    let edge = ddl.contains("notified_at = NULL");
    assert!(edge, "{ALERT_STORE}: 新边沿不置回 NULL");
}

/// 三、放送由扫表驱动，写成功才标记。
#[test]
fn a_notification_is_marked_only_after_the_row_is_written() {
    let bell = production(BELL);
    let at = bell.find("for crate::alert_store::PostgresAlertStore");
    let at = at.expect("bell.rs 的扫表队列接不上");
    let wiring = &bell[at..];
    let pending = rust_function_body(wiring, "pending");
    let pending = pending.expect("pending 的函数体读不出来");
    let asked = pending.contains("self.unnotified_firing(limit)");
    assert!(asked, "{BELL}: pending 不扫 unnotified_firing");
    let mark = rust_function_body(wiring, "mark");
    let mark = mark.expect("mark 的函数体读不出来");
    let kept = mark.contains("self.mark_notified(dedup_key)");
    assert!(kept, "{BELL}: mark 不落 mark_notified");
    let drain = rust_function_body(&bell, "drain_for");
    let drain = drain.expect("drain_for 的函数体读不出来");
    let writes = drain.find("if write_one(");
    let marks = drain.find("queue.mark(");
    assert!(writes.is_some(), "{BELL}: drain_for 不调 write_one");
    assert!(marks.is_some(), "{BELL}: drain_for 不标记");
    assert!(writes < marks, "{BELL}: queue.mark 早于 write_one");
}

/// 四、丢掉的那一条留读数，不只留一句随 Pod 走掉的 warn。
#[test]
fn a_lost_notification_leaves_a_counter_not_only_a_warning() {
    let bell = production(BELL);
    let alerts = production(ALERTS);
    let warnings = count(&bell, "warn!");
    let failed = count(&bell, "OUTCOME_WRITE_FAILED");
    let delivered = count(&bell, "OUTCOME_DELIVERED");
    assert!(warnings <= failed, "{BELL}: 有 warn 没有 write_failed");
    assert!(failed >= 3, "{BELL}: 三条失败去路没各留一格");
    assert!(delivered >= 1, "{BELL}: 成功那一路没留 delivered");
    let family = alerts.contains("pub const OUTCOME_WRITE_FAILED: &str");
    assert!(family, "{ALERTS}: 没有声明 OUTCOME_WRITE_FAILED");
}

/// 五、滚动的影子不算两个写者的分歧：`writer` 是那一具，`role` 是哪一种。
#[test]
fn a_rolling_overlap_is_one_role_writing_under_two_names() {
    let store = production(ALERT_STORE);
    let ddl = collapsed(&store);
    let writer = ddl.contains("IF NOT EXISTS writer TEXT");
    assert!(writer, "{ALERT_STORE}: alerts 不记 writer");
    let roles = ddl.contains("role TEXT NOT NULL");
    assert!(roles, "{ALERT_STORE}: alert_assertions 不记 role");
    let identity = rust_function_body(&store, "process_identity");
    let identity = identity.expect("process_identity 的函数体读不出来");
    let host = identity.contains("HOSTNAME");
    assert!(host, "{ALERT_STORE}: 身份不读 HOSTNAME");
    let kind = rust_function_body(&store, "process_role");
    let kind = kind.expect("process_role 的函数体读不出来");
    let deployment = kind.contains("cog_core::env::DEPLOYMENT_NAME_ENV");
    assert!(deployment, "{ALERT_STORE}: role 不读部署名");
    let apart = !kind.contains("process_identity");
    assert!(apart, "{ALERT_STORE}: role 又抄了一遍身份");
}
