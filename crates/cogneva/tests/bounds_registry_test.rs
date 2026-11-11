//! 系统给自己设的界，在 `cogneva.json` 的 `bounds` 段里逐条登记。
//!
//! 这个系统默认不该杀一个还在干活的进程：过了一个事先挑好的钟点，正确动作是登
//! 一行读数然后继续等。已经拆掉的那一批（推送墙、构建预算、测试预算、验证门禁、
//! kubectl 读写、git 命令、flat retry 预算）共用一条控制律
//! `crates/cog-core/src/declared_wall.rs`。
//!
//! 剩下的界只有一只篮子：
//!
//! - `kept`：判定为不该拆。每条必须写出**为什么不拆**——不是"以后再说"，是这条
//!   界结束的东西本来就不是正在产出的工作（评测口径、安全控制、只读采样、停机宽限），
//!   或者它根本没有在杀任何进程（过期的排队登记、只写进账本的一行读数、控制器只写
//!   状态条件的那一格）。
//!
//! 曾经有一只 `scheduled` 篮子，装「与已拆那批同一个形状、只是这一轮没轮到」的界。
//! 它已经清空并删掉了：最后一个按钟杀 Job 的期限（滚动判定 Job 的
//! `activeDeadlineSeconds`）换成了进程写进度文件、容器上的存活探针读它。留着那只
//! 篮子只会变成「登记了就等于交代过了」，而那正是这条原则不接受的形状。
//!
//! 三条判据，任何一条不成立就让这条门红：
//!
//! 1. 每条登记都点名一个真实存在的文件，并且那个符号真的在文件里。界是从代码里
//!    长出来的；代码里的界没了而登记还留着，读者会以为自己看见的是现场。
//! 2. `kept` 里没有重复的 id。
//! 3. 每条都有 `why`。一句空话不算理由：至少写满一句话的长度。
//!
//! 篮子本身不许为空——空了说明有人把登记整段删掉而没有留下任何判据。

use crate::common;

use common::root::repo_root;
use serde_json::Value;

fn read_registry() -> Value {
    let path = repo_root().join("deploy/helm/cogneva/files/cogneva.json");
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读不到 {}: {e}", path.display()));
    let doc: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("{} 不是合法 JSON: {e}", path.display()));
    doc.get("bounds")
        .cloned()
        .expect("cogneva.json 里没有 bounds 段：界没有登记处了")
}

fn kept(registry: &Value) -> &Vec<Value> {
    registry
        .get("kept")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("bounds.kept 不是数组"))
}

fn field<'a>(entry: &'a Value, key: &str) -> &'a str {
    entry
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("登记项缺 {key}: {}", entry))
}

/// 判据 1：每条登记都点名一个真实的文件与一个真在文件里的符号。
#[test]
fn every_registered_bound_names_a_symbol_that_is_still_there() {
    let registry = read_registry();
    let root = repo_root();
    let mut missing = Vec::new();
    for entry in kept(&registry) {
        let file = field(entry, "file");
        let symbol = field(entry, "symbol");
        let path = root.join(file);
        match std::fs::read_to_string(&path) {
            Err(e) => missing.push(format!("{} {}: {e}", entry["id"], file)),
            Ok(text) if !text.contains(symbol) => {
                missing.push(format!("{}: {file} 里已经没有 {symbol} 了", entry["id"]))
            }
            Ok(_) => {}
        }
    }
    assert!(
        missing.is_empty(),
        "登记簿指着代码里已经不存在的东西——删掉那条界时，把这一行一起删：\n  {}",
        missing.join("\n  ")
    );
}

/// 判据 2：id 唯一。
#[test]
fn no_bound_is_registered_twice() {
    let registry = read_registry();
    let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    let mut dupes = Vec::new();
    for entry in kept(&registry) {
        let id = field(entry, "id");
        if !seen.insert(id) {
            dupes.push(id.to_string());
        }
    }
    assert!(dupes.is_empty(), "重复的 id：{}", dupes.join(", "));
}

/// 判据 3：`why` 写满了，不是一句空话。
#[test]
fn every_entry_says_why_it_is_kept() {
    let registry = read_registry();
    let mut thin = Vec::new();
    for entry in kept(&registry) {
        let why = field(entry, "why").trim();
        // 一句话的长度门槛：一个"以后再看"过不了它，一条真理由过得去。
        if why.chars().count() < 40 {
            thin.push(format!(
                "{}: why 太短（{} 字）",
                entry["id"],
                why.chars().count()
            ));
        }
    }
    assert!(
        thin.is_empty(),
        "登记簿里有没写清楚的行：\n  {}",
        thin.join("\n  ")
    );
}

/// 判据 4：篮子还在。空了说明有人把登记整段删掉而没有留下任何判据——
/// 「一条不该拆的界都不剩」的处置是删掉这段登记与这条门，不是让它悄悄为空。
#[test]
fn the_basket_is_present_and_populated() {
    let registry = read_registry();
    assert!(
        !kept(&registry).is_empty(),
        "kept 空了：如果真的一条不该拆的界都不剩，删掉这段登记并把这条门一起改掉"
    );
}
