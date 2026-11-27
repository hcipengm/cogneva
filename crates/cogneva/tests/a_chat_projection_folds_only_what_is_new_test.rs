//! 会话投影和变更账一样，是一份可以随时丢掉的投影：它有自己的水位（这段会话折到流上
//! 哪个位置），之后只折水位之后的事实。没有水位时它只能整段重放——而重放一次就写一遍
//! 重复的轮（`chat_turns` 的插入没有冲突键）。
//!
//! 水位按「这一轮任一事实的最深 seq」记，不是只按意图：后到的工具调用与附件同属这一轮，
//! 只按意图过滤会把它们落在水位之后的那一轮跳过。
//!
//! 没有这次改动这几条会红：水位表不存在、重建不读不写水位、旧会话没有整段重建的兜底。

use cog_storage::table_shapes::{TableShape, MUTABLE_TABLES};

#[test]
fn the_chat_projection_carries_its_own_watermark() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    assert!(
        root.join("crates/cog-storage/migrations/postgres/137_chat_turn_watermark.up.sql")
            .is_file(),
        "没有水位的迁移：投影只能整段重放"
    );
    let rebuild =
        std::fs::read_to_string(root.join("crates/cog-storage/src/postgres/chat_turn_rebuild.rs"))
            .expect("chat_turn_rebuild.rs");
    assert!(
        rebuild.contains("chat_turn_watermarks") && rebuild.contains("folded_seq"),
        "重建不读不写水位"
    );
    assert!(
        rebuild.contains("full_rebuild") && rebuild.contains("DELETE FROM chat_turns"),
        "旧会话（没有水位）没有整段重建的兜底：增量会在旧行上写重复的轮"
    );
    assert!(
        rebuild.contains("turn.max_seq > floor"),
        "过滤不是按这一轮最深的那条事实：后到的工具/附件会让那一轮被跳过"
    );
    assert!(
        rebuild.contains("GREATEST(chat_turn_watermarks.folded_seq"),
        "水位会倒退：重折一段旧会话之后，下一次又把新的一轮当成旧的"
    );
}

/// 水位表自己也有形状：它是被改写的一行，不是事实，也不是投影。
#[test]
fn the_watermark_table_declares_its_shape_and_its_census_class() {
    let declaration = MUTABLE_TABLES
        .iter()
        .find(|table| table.table == "chat_turn_watermarks")
        .expect("水位表没有形状声明");
    assert!(matches!(declaration.shape, TableShape::Document { .. }));
}
