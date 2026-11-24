//! 基准分支：判据要问「这一笔之前是什么样」时读的那棵树。
//!
//! 把一份读数抄进树里——一张「文件 → 条数」的基线表，一份豁免名单——是同一件事
//! 的两种写法：抄件只在量的那一天是真的，之后每一笔改动都得靠人记得改它，而人记
//! 不得。判据真正要问的只有一句「这一笔有没有比之前多一条」，这句话的答案在基分
//! 支上：同一台解析器、同一套规则、另一个来源。
//!
//! 读不到基分支是 **panic**，不是退回「判自己改的那棵树」：一份自己跟自己对读的
//! 判据永远绿，而它要判的那件事一次都没被问过。

// 每个测试二进制各挑自己要的那几个 helper，没挑到的会报 `never used`。
#![allow(dead_code)]

use std::path::Path;

/// 基分支的几种叫法，按顺序试。
///
/// 叫法因检出而异而意思不变：CI 的检出叫 `origin/main`，部署器自己的克隆把基分支
/// 留作本地 `main`、把它拉取的那个远端叫 `upstream`，开发机上是 `origin`。
pub const BASE_NAMES: &[&str] = &["origin/main", "main", "upstream/main", "local/main"];

/// 基分支的提交号：第一个解析得出来的叫法。
pub fn base_rev(root: &Path) -> String {
    for name in BASE_NAMES {
        let out = std::process::Command::new("git")
            .current_dir(root)
            .args(["rev-parse", "--verify", "--quiet", &format!("{name}^{{commit}}")])
            .output()
            .unwrap_or_else(|e| panic!("could not run git in {}: {e}", root.display()));
        if out.status.success() {
            return String::from_utf8_lossy(&out.stdout).trim().to_string();
        }
    }
    panic!(
        "none of {BASE_NAMES:?} resolves in {}：这条判据问的是「这一笔比基分支多了什么」，\
         答案在基分支上。一个看不见基分支的检出回答不了它——把基分支取下来，别让判据\
         靠读不到而变绿。",
        root.display()
    )
}

/// 基分支上这个文件的内容。基分支里没有它（这一笔新建的文件）就是 `None`：
/// 新文件在基线里是零条，它并不因此免掉任何一条判据。
pub fn base_text(root: &Path, rev: &str, rel: &str) -> Option<String> {
    let out = std::process::Command::new("git")
        .current_dir(root)
        .args(["show", &format!("{rev}:{rel}")])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}
