//! 删一条记忆之前，先按纪律问一次「世界还留着另一份吗」。
//!
//! 记忆的两条删除路径（summary 与 schema）一直是「先归档、读回逐字相等、再删」。
//! 这条门要求那次判断走 `cog_core::contract::irreversible` 的同一句话，而不是两条
//! 路径各自写一遍「不相等就拒」——后者是两份判词，改一处另一处不会红。
//!
//! 没有这次改动这条会红：两条路径都不引用 `deletion_is_allowed`。

use crate::common::routes::repo_root;

#[test]
fn both_memory_delete_paths_ask_the_discipline_first() {
    let root = repo_root();
    for relative in [
        "crates/cog-memory/src/postgres_entry_store.rs",
        "crates/cog-memory/src/postgres_schema.rs",
    ] {
        let source = std::fs::read_to_string(root.join(relative)).expect(relative);
        let ask = source
            .find("deletion_is_allowed(back == body)")
            .unwrap_or_else(|| panic!("{relative} 删除之前没有问「归档读回来了吗」"));
        let delete = source
            .find("DELETE FROM ")
            .unwrap_or_else(|| panic!("{relative} 没有删除语句？"));
        assert!(
            ask < delete,
            "{relative} 先删后问：判断必须发生在 DELETE 之前"
        );
    }
}

#[test]
fn a_worktree_removal_is_confirmed_by_reading_the_path_back() {
    // 「到没到」不是「我们发过删除」，而是「它现在不在」。目录还在时不能当成功走开。
    let source =
        std::fs::read_to_string(repo_root().join("crates/cog-reflection/src/workspace.rs"))
            .expect("workspace.rs");
    let removal = source.find("remove_dir_all(path)").expect("工作树移除点");
    let check = source
        .find("RealityCheck::PathAbsent")
        .expect("移除之后没有读回路径");
    assert!(
        removal < check,
        "先报告成功、后读现实：判断必须在移除之后、且确实读了路径"
    );
}

#[test]
fn a_revocation_asks_what_stops_verifying_before_it_acts() {
    let source =
        std::fs::read_to_string(repo_root().join("crates/cogneva/src/keys.rs")).expect("keys.rs");
    let ask = source
        .find("bundle_lost_after_revocation")
        .expect("吊销之前没有重验原则包");
    let act = source.find("SecurityKeys::revoke").expect("吊销调用点");
    assert!(ask < act, "先吊销、后问影响：判断必须在动手之前");
    assert!(
        source.contains("--force"),
        "影响清单点名之后要有显式的接受方式，否则每一次吊销都会被拒"
    );
}

#[test]
fn the_discipline_refuses_a_delete_whose_archive_did_not_read_back() {
    // 纪律本体在 cog-core；这里只是确认它的拒绝理由是纪律自己的那句话。
    use cog_core::contract::irreversible::{loss, Action, ActionKind, Loss};
    assert!(matches!(
        loss(Action {
            kind: ActionKind::Delete,
            recoverable: false,
        }),
        Loss::Permanent(_)
    ));
}
