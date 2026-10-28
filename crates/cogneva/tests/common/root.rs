//! 「仓库根在哪」的唯一算法。
//!
//! 每个 crate 的集成测试都从某个 crate 目录里跑起来，于是每个 crate 都写过一份
//! `../..`、`.parent().parent()`、`.ancestors().nth(2)`——它们指同一个目录，差别只在
//! 某一处被改动的那天出现：根错一级的门会读到空集，而读空与「这条判据没有题目」在
//! 输出上同形，绿得一样。
//!
//! 起点是 `env!("CARGO_MANIFEST_DIR")`，它由编译器按**正在编译的那个 crate** 填。
//! 这正是共享需要的那条性质：本文件被别的 crate 的测试 `#[path]` 编进去时，起点自动
//! 换成那个 crate，不必再写一份「我在 `crates/` 下第几层」的假设。根则是它上面最近的、
//! 声明了 `[workspace]` 的那份 `Cargo.toml` 所在目录——工作区根由 cargo 定义，不由路径
//! 层数定义，所以新增一层目录树也推不翻它。

use std::path::PathBuf;

/// 这棵树的根：`CARGO_MANIFEST_DIR` 往上第一份声明了 `[workspace]` 的 `Cargo.toml`。
///
/// 起点先 `canonicalize`：根要和别人比。`cargo metadata` 报的是绝对干净路径，门在那边
/// 也 `canonicalize` 了一遍，而这里若留着带 `..` 或带软链的写法，两侧就是同一个目录的
/// 两种拼写——比较起来不相等，报告的形状是「这些文件没有成员编译」，与真正的原因无关。
/// 解不出（目录被删、权限不够）就退回原文：读不到路径比读错路径好，后续的读取会自己报错。
pub fn repo_root() -> PathBuf {
    let raw = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let start = raw.canonicalize().unwrap_or(raw);
    let mut dir = start.clone();
    loop {
        let manifest = dir.join("Cargo.toml");
        if let Ok(text) = std::fs::read_to_string(&manifest) {
            if text.contains("[workspace]") {
                return dir;
            }
        }
        if !dir.pop() {
            panic!(
                "{} 往上没有声明 [workspace] 的 Cargo.toml：仓库根读不出来",
                start.display()
            );
        }
    }
}
