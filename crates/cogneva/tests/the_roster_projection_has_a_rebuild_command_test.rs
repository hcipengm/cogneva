//! 名册是员工事实的投影；投影要有重建命令，否则「删了能不能回来」只能读代码。
//!
//! 门判三段：口令在子命令表里、入口调用 `rebuild_roster`、主分发接上。连库重建
//! 由 `crates/cog-storage/tests/a_roster_rebuilds_from_the_stream.rs`（ignored）判。
//!
//! 没有这次改动这条会红：`roster-rebuild` 不存在，名册只能写不能从流上重来。

use crate::common::routes::repo_root;

#[test]
fn the_roster_can_be_rebuilt_from_the_employee_facts() {
    let root = repo_root();
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/cli.rs")).expect("cli.rs");
    assert!(
        cli.contains("(\"roster-rebuild\", Command::RosterRebuild)"),
        "子命令表里没有 roster-rebuild"
    );
    let entry = std::fs::read_to_string(root.join("crates/cogneva/src/roster_rebuild.rs"))
        .expect("roster_rebuild.rs");
    assert!(
        entry.contains("rebuild_roster(&pool)"),
        "重建入口没有调投影自己的重建函数"
    );
    let main = std::fs::read_to_string(root.join("crates/cogneva/src/main.rs")).expect("main.rs");
    assert!(
        main.contains("Command::RosterRebuild =>"),
        "主分发没有接上这条命令"
    );
}
