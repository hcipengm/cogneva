//! 「怎么读一个文件」「怎么走一棵树」：两条各只许有一份的短规则。
//!
//! 它们短到看起来不值得共享，所以在夹具里被抄了十二遍和两遍，`src/` 的单测里又抄了
//! 一遍——而抄件里那句「读不出来就 panic」是注释，不是机制。跳过一个读不出来的子目录
//! 不会让任何断言变红：剩下的那份普查往往仍高于下限，「少读了一块」与「那一块都合规」
//! 在判词上分不开。
//!
//! 这份**不 import 仓里的任何别的东西**，是为了让 `src/` 也能编它：单测挂在 crate 自己
//! 的模块树里，够不着 `common`，只有一份不依赖兄弟模块的文件才挂得过去。`routes` 把
//! 这两个函数原样再导一次，所以夹具那边一处都不用改。
//!
//! 判据在 `the_tree_walk_and_the_file_read_are_written_once_test`。

#![allow(dead_code)]

use std::path::{Path, PathBuf};

pub fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

pub fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    // 读不出来就 panic：递归遍历里跳过一个子目录不会让任何断言变红，而剩下的那份
    // 普查往往仍高于下限——「少读了一块」与「那一块都合规」在判词上分不开。
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| {
        panic!(
            "{} 读不出来（{e}）：这棵树的普查面少了一块，而用剩下的那份判出来的\
             结论与「整片区都合规」同形",
            dir.display()
        )
    });
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, extensions, out);
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| extensions.contains(&e))
        {
            out.push(path);
        }
    }
}
