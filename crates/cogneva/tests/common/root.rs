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

use std::path::{Path, PathBuf};

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

/// 渲染产物的根：`<仓根>/deploy/rendered`，返回之前先让 chart 现场把它生成出来。
///
/// 这份产物不进仓（它由 chart + profiles 完全决定），所以每一个读它的判据都必须在
/// **读的那一刻**生成，而不是指望某次提交把字节带过来。生成是幂等的：输入没变就是
/// 一次摘要比较。失败就停在这里——让下游读到一棵空树时，「没有 profile」与「这条
/// 判据没有题目」在输出上同形，而它会让每一条判据都绿得没有题目。
// 这个模块被各 crate 以 `#[cfg(test)] #[path]` 纳入：多数只要 `repo_root`，而
// 「没用到就不许存在」会把每一处纳入都逼出一行空调用。真正需要判的是调用点。
#[allow(dead_code)]
pub fn rendered_root() -> PathBuf {
    rendered_at(&repo_root())
}

/// 让**指定那棵树**的 `deploy/rendered` 就位，并返回它的路径。
///
/// 判据读的是传进来的 `root`，不一定是本仓：夹具树把自己的 `deploy/rendered`
/// 摆成题目时，生成器只该作用在那棵树上。生成器永远从本仓取（夹具树里没有
/// 渲染链），而它对没有 chart 的夹具树原样放过。
#[allow(dead_code)]
pub fn rendered_at(root: &Path) -> PathBuf {
    let script = repo_root().join("deploy/scripts/lib/rendered-cache.sh");
    let status = std::process::Command::new("bash")
        .arg(&script)
        .arg("--ensure")
        .arg(root)
        .status()
        .unwrap_or_else(|e| panic!("run {}: {e}", script.display()));
    assert!(
        status.success(),
        "{} --ensure 失败：渲染产物由 chart 现场生成，先修好渲染链（这条判据读的就是它）",
        script.display()
    );
    root.join("deploy/rendered")
}
