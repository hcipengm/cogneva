//! 「怎么读一个文件」「怎么走一棵树」：两条各只许有一份的短规则。
//!
//! 它们短到看起来不值得共享，所以「递归下目录、按扩展名收文件」这一段在夹具里各自
//! 写了十来遍，连那句「读不出来就 panic」的理由都逐字抄了四份——而抄件里那句话是注释，
//! 不是机制。跳过一个读不出来的子目录不会让任何断言变红：剩下的那份普查往往仍高于
//! 下限，「少读了一块」与「那一块都合规」在判词上分不开。
//!
//! 这份**不 import 仓里的任何别的东西**，是为了让 `src/` 也能编它：单测挂在 crate 自己
//! 的模块树里，够不着 `common`，只有一份不依赖兄弟模块的文件才挂得过去。`routes` 把
//! 这几个函数原样再导一次，所以夹具那边一处都不用改。
//!
//! 判据在 `the_fixture_reading_steps_are_written_once_test`。

#![allow(dead_code)]

use std::path::{Path, PathBuf};

pub fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// 树底下的每一个文件：the one recursion in the fixture harness.
///
/// `extensions` 为空表示全收，否则只收这些扩展名。`skip` 点名不下钻的目录
/// （构建产物、`.git`）——那是某一棵树的事，不是走树这件事的一部分，所以由调用方说。
///
/// 读不出来就 panic：递归遍历里跳过一个子目录不会让任何断言变红，而剩下的那份
/// 普查往往仍高于下限——「少读了一块」与「那一块都合规」在判词上分不开。
pub fn walk_files(dir: &Path, extensions: &[&str], skip: &[&str], out: &mut Vec<PathBuf>) {
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
            if path
                .file_name()
                .is_some_and(|name| skip.iter().any(|s| name == *s))
            {
                continue;
            }
            walk_files(&path, extensions, skip, out);
        } else if extensions.is_empty()
            || path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| extensions.contains(&e))
        {
            out.push(path);
        }
    }
}

pub fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    walk_files(dir, extensions, &[], out)
}

/// Every file under `dir`, whatever its extension.
pub fn walk_all(dir: &Path, out: &mut Vec<PathBuf>) {
    walk_files(dir, &[], &[], out)
}
