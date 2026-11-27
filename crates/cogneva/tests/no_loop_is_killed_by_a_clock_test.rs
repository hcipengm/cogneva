//! 判停只有一条理由：**没有前进**。时钟不是理由。
//!
//! 「到点杀」的循环把「慢」当成「死」：一条编了很久的命令与一条卡死的命令在时钟眼里
//! 同形，而它们要做的处置相反。这一条扫全树，要求：
//!
//! - 任何 `kill` 都发生在**销毁**的语境里（测试/沙箱清理、VM 销毁），不是判停；
//! - 判停的唯一入口是 `declared_wall`（它读 beat 与静默窗口，且地板是两拍）。
//!
//! 白名单只有一份，写在下面；名单变长要被看见——加一处的理由必须说得出来。

use std::path::PathBuf;

use crate::common::rust_production::rust_files_under;

/// 允许出现 `kill` 的文件：它们杀的是**对象**，不是「这个进程是不是死了」。
const DESTRUCTION_SITES: &[&str] = &[
    "crates/cog-reflection/src/firecracker.rs", // 销毁 microVM
];

#[test]
fn every_kill_is_a_destruction_and_none_of_them_judges_stalling() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/cogneva is two levels under the workspace root")
        .to_path_buf();

    let mut offenders = Vec::new();
    for file in rust_files_under(&root.join("crates")) {
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(&file)
            .to_string_lossy()
            .to_string();
        // 测试文件里的 kill 是夹具，不是判停。
        if rel.contains("/tests/") || rel.ends_with("_test.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap_or(Default::default());
        // 产出与夹具的切法只有一份（`common::rust_production`）：在第一个
        // `#[cfg(test)]` 上切，会在文件中间带着 test-only 项时把后面的产出整段丢掉。
        let production = crate::common::rust_production::production_text(&text);
        let production = production.as_str();
        if !production.contains(".kill()") {
            continue;
        }
        if !DESTRUCTION_SITES.contains(&rel.as_str()) {
            offenders.push(rel);
        }
    }

    assert!(
        offenders.is_empty(),
        "这些文件里有 `kill`，而它不在销毁语境里（判停只许走 declared_wall）：\n  {}",
        offenders.join("\n  ")
    );
}
