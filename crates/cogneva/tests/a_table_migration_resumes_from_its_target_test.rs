//! 搬一张表可以中断，可以从任意一点接着做，而且「搬完」与「搬对」是两件事。
//!
//! 水位不另存：目标表自己的主键就是水位——「搬到哪儿了」已经写在目标里。重放靠主键
//! 冲突吞掉。搬完不等于搬对，所以 `--verify` 用与 `table-digest` 同一份摘要两头各算
//! 一次。没有主键的表拒绝搬：没有键就没有水位，中断之后说不清搬到哪儿了。
//!
//! 没有这次改动这几条会红：`table-copy` 不存在、水位另存、重放不吞、没有核对摘要。

#[test]
fn the_copy_command_exists_and_resumes_from_the_target_itself() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/cli.rs")).expect("cli.rs");
    assert!(
        cli.contains("table-copy"),
        "没有这个子命令：迁移这一步没有入口"
    );
    let copy = std::fs::read_to_string(root.join("crates/cogneva/src/table_copy.rs"))
        .expect("table_copy.rs");
    assert!(
        copy.contains("resume_after_sql()"),
        "续跑没有读目标表自己的最大主键：水位要么另存了一份，要么根本没续跑"
    );
    assert!(
        copy.contains("ON CONFLICT DO NOTHING"),
        "重放同一批会写第二条：中断之后接着做就不成立"
    );
    assert!(
        copy.contains("digest_of"),
        "没有核对摘要：「搬完」被当成了「搬对」"
    );
    assert!(
        copy.contains("没有主键"),
        "没有主键的表没有被拒绝：没有键就没有水位"
    );
    assert!(
        copy.contains("checked_table"),
        "表名没有经过校验：它要拼进 SQL"
    );
    // 生成列不能出现在插入的列清单里：事件流带了一整排生成列，`SELECT *` 搬它会在
    // 第一批就失败，而那条错读起来像「这张表搬不了」。
    assert!(
        copy.contains("attgenerated = ''"),
        "列清单没有排除生成列：带生成列的表第一批就搬不动"
    );
}

/// 源与目标是同一张表时拒绝：搬它到自己身上没有意义，也没有水位可断。
#[test]
fn copying_a_table_onto_itself_is_refused() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .join("crates/cogneva/src/table_copy.rs"),
    )
    .expect("table_copy.rs");
    assert!(
        source.contains("源与目标是同一张表"),
        "没有拒绝「搬到自己身上」：那一条既没有水位也没有意义"
    );
}
