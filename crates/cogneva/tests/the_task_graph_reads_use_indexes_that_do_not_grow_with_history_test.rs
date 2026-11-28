//! 任务图的两条读数必须走「与 ORDER BY 同形」的覆盖索引，否则它们随历史排序。
//!
//! 现网实测（2026-11-26，迁移 36,014 行／定义 46,454 行）：没有索引时全库状态计数
//! 78.7ms（Seq Scan + Sort）、工作区计数 136.2ms（46k 行外部归并排序）；同一份行
//! 加上这两条索引后 16.8ms／32.4ms，计划都是 Index Only Scan、没有 Sort。十倍历史
//! 时排序方案会越过一秒，而索引方案的代价只按行线性增长。
//!
//! 门判三件事，缺一条这个修法就会静默失效：
//! - 迁移里有这两条索引，且列序与查询的 ORDER BY 逐列一致（前导列不对，规划器照旧
//!   排序，读数又回到秒级）；
//! - 两条查询的 `ORDER BY` 仍是这两个形状（改查询等于改索引的前提）；
//! - 迁移有一份会删掉它们的 down。
//!
//! 没有这次改动这条会红：两条索引不在树里，查询照旧排序。

use crate::common::routes::repo_root;

const MIGRATION: &str = "crates/cog-storage/migrations/postgres/138_task_graph_read_indexes";

#[test]
fn the_indexes_match_the_order_the_two_readings_ask_for() {
    let root = repo_root();
    let up = std::fs::read_to_string(root.join(format!("{MIGRATION}.up.sql")))
        .expect("138 的 up 迁移读不到");
    let down = std::fs::read_to_string(root.join(format!("{MIGRATION}.down.sql")))
        .expect("138 的 down 迁移读不到");

    for needle in [
        // 工作区计数：先按 workspace 过滤，再按 (task_id, rev DESC) 取每任务最新一版定义。
        "ON task_defined (workspace_id, task_id, rev DESC)",
        // 全库计数：按 (task_id, seq DESC) 取每任务最后一条迁移，状态随索引一起读。
        "ON task_transition (task_id, seq DESC) INCLUDE (to_state)",
    ] {
        assert!(up.contains(needle), "迁移里没有与查询同形的索引：{needle}");
    }
    for index in [
        "idx_task_defined_workspace_task_rev",
        "idx_task_transition_task_seq_state",
    ] {
        assert!(
            down.contains(index),
            "down 迁移没有删掉 {index}：回滚之后索引还在，却没人知道它从哪来"
        );
    }
}

#[test]
fn the_two_readings_still_ask_in_the_order_the_indexes_serve() {
    let backend = std::fs::read_to_string(
        repo_root().join("crates/cog-storage/src/postgres/state_backend.rs"),
    )
    .expect("state_backend.rs 读不到");
    for needle in [
        // 工作区计数的定义那一侧。
        "WHERE workspace_id = $1\n         ORDER BY task_id, rev DESC",
        // 全库计数的迁移那一侧。
        "ORDER BY task_id, seq DESC",
    ] {
        assert!(
            backend.contains(needle),
            "查询的排序形状变了（{needle:?} 不在 state_backend.rs 里）：索引得跟着改，\
             否则规划器回到排序，读数又随历史变慢"
        );
    }
}
