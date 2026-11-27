//! 设计文档第七节列出的每一项检查，
//! 在树里都指得出它落在哪一行代码：每一条都配一段只在这条检查里出现的错误文本或实现形状。
//! 指不出的要删掉——文档里有、代码里没有的检查，读的人会以为它在跑。
//!
//! 没有这次改动这条会红：文档与代码之间没有任何东西在比对。

#[test]
fn every_check_in_section_seven_has_its_own_line_of_code() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let plugin = std::fs::read_to_string(root.join("crates/cog-reflection/src/plugin.rs"))
        .expect("plugin.rs");
    // 每一行：文档里的那一项 → 只在它这一项里出现的实现证据。
    let items: &[(&str, &str)] = &[
        (
            "project_root 存在且是目录",
            "project_root does not exist or is not a directory",
        ),
        (
            "cargo／git／rustc 在 PATH 且可执行",
            "for tool in [\"cargo\", \"git\", \"rustc\"]",
        ),
        (
            "change_dir／binary_dir／backup_dir 可创建可写",
            "tempfile::NamedTempFile::new_in",
        ),
        ("目录创建失败有具名错误", "Failed to create directory"),
        ("目录不可写有具名错误", "is not writable"),
        ("项目目录在 git 仓里", "is not a git repository"),
        (
            "self_exec 模式下二进制路径不一致给 WARN",
            "Current executable path does not match configured binary_dir/cogneva",
        ),
        (
            "校验发生在自进化循环启动之前",
            "validate_self_evolution_environment",
        ),
    ];
    let missing: Vec<&str> = items
        .iter()
        .filter(|(_, evidence)| !plugin.contains(evidence))
        .map(|(item, _)| *item)
        .collect();
    assert!(
        missing.is_empty(),
        "文档第七节点名的这些项在代码里找不到证据（要么补上实现，要么从文档里删掉）：{missing:?}"
    );
}
