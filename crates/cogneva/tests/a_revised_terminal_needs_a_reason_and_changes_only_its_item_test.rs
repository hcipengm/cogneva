//! 判据只在下一次计划时定下，之后每一圈都对着**同一段话**说；换它只有一条路——
//! 一条带理由的修订，且只换它点名的那一条。
//!
//! 三件事各缺一半都会让「每圈换一个目标」重新变得可能：
//!
//! 1. 事实形状：修订落在流上（`task`／`terminal_revised`），换之前那段、换之后那段、
//!    为什么换、谁换的都在一条里；
//! 2. 写入口：没有理由的修订被拒，且什么都不写——空理由等于把目标悄悄换掉；
//! 3. 读与用：每一轮开始前读一次，只改被点名的那一条，同批里别的判据一个字不动。
//!
//! 没有这次改动这四条会红：变体与清单项不存在、`check_revision` 不存在、循环里没有
//! 每轮读修订、`apply_revisions` 不存在。

use chrono::Utc;
use cog_core::contract::ledger_item::{LedgerItem, LedgerState};
use cog_core::contract::terminal_revision::{apply_revisions, check_revision, TerminalRevision};

fn revision(item_id: &str, previous: &str, terminal: &str, reason: &str) -> TerminalRevision {
    TerminalRevision {
        item_id: item_id.into(),
        previous: previous.into(),
        terminal: terminal.into(),
        reason: reason.into(),
        actor: "owner".into(),
        at: Utc::now(),
    }
}

fn item(id: &str, terminal: &str) -> LedgerItem {
    LedgerItem {
        id: id.into(),
        terminal: terminal.into(),
        now: String::new(),
        gap: String::new(),
        state: LedgerState::Open,
    }
}

#[test]
fn a_revision_without_a_reason_is_refused() {
    assert!(check_revision(&revision(
        "S1",
        "old",
        "new",
        "the old one judged the wrong surface"
    ))
    .is_ok());
    assert!(check_revision(&revision("S1", "old", "new", " ")).is_err());
    assert!(check_revision(&revision("S1", "old", "old", "no change")).is_err());
    assert!(check_revision(&revision("S1", "old", "  ", "empty terminal")).is_err());
}

#[test]
fn a_revision_changes_only_the_item_it_names() {
    let mut items = vec![item("S1", "first goal"), item("S2", "second goal")];
    let revisions = vec![revision(
        "S1",
        "first goal",
        "the corrected goal",
        "wrong surface",
    )];
    assert_eq!(apply_revisions(&mut items, &revisions), 1);
    assert_eq!(items[0].terminal, "the corrected goal");
    assert_eq!(
        items[1].terminal, "second goal",
        "同批里别人的判据被顺手改了"
    );
    // 标记对不上：一条都不改。
    let mut others = vec![item("S9", "someone else's goal")];
    assert_eq!(apply_revisions(&mut others, &revisions), 0);
    assert_eq!(others[0].terminal, "someone else's goal");
}

/// 事实的形状与清单项：修订是 `task` 这一族里 `terminal_revised` 那一步，载荷的
/// 每一格都在清单里点了名——少一格，读的人就不知道那一条里有什么。
#[test]
fn the_fact_and_its_payload_are_catalogued() {
    let fields =
        cog_core::contract::event_payload::payload_fields_for("task", "terminal_revised", 1)
            .expect("terminal_revised 没有登记在载荷清单里");
    let names: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
    for needed in ["item_id", "previous", "terminal", "reason", "actor"] {
        assert!(
            names.contains(&needed),
            "载荷清单里没有 {needed}：{names:?}"
        );
    }
}

/// 生产里确实接上了：写入口校验理由，循环每一轮读一次修订。两处少任何一处，
/// 上面那几条就只是库里的函数。
#[test]
fn the_writer_checks_the_reason_and_the_loop_reads_revisions_every_round() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let writer =
        std::fs::read_to_string(root.join("crates/cog-storage/src/postgres/terminal_revisions.rs"))
            .expect("terminal_revisions.rs");
    assert!(
        writer.contains("check_revision"),
        "写入口没有校验理由：空理由的修订会被当成一次修订落下来"
    );
    let loop_source = std::fs::read_to_string(
        root.join("crates/cog-collaboration/src/squad/ralph/ralph_loop.rs"),
    )
    .expect("ralph_loop.rs");
    assert!(
        loop_source.contains("latest_for(&ids)"),
        "循环没有在每一轮开始前读修订：判据换了，后面每一圈仍对着旧的那段说"
    );
    assert!(
        loop_source.contains("apply_revisions"),
        "读到的修订没有套到这一批待办上：写了没人用"
    );
    let census = std::fs::read_to_string(root.join("crates/cog-storage/src/table_shapes.rs"))
        .expect("table_shapes.rs");
    assert!(
        census.contains("terminal_revisions_for"),
        "修订的读口不在 STREAM_READERS 里：普查看不见这个读面"
    );
}
