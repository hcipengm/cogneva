//! `cogneva ledger status`：读五格台账，说还剩几条、每条差什么。
//!
//! 它住在 evaluator 这一层，因为「这条还差什么」是判者的问题，不是编排的问题：
//! 编排把活推进，判者回答「到了没有」；同一本台账，读它的人与判它的人是同一个角色。
//!
//! 这是把「台账怎么读」从带外的 Python 读者搬进产品的第一段：读法是
//! `cog_core::contract::ledger_item::parse_md`，批次规则是
//! `LedgerItem::is_in_batch`，两处都与平台自己的类型是同一份，不另写一套。
//!
//! 台账地址的解析顺序与产品别处一致：`COGNEVA_TODO_SOURCE` → `COGNEVA_TODO_LEDGER`
//! → 本机声明 `~/.config/cogneva/todo-ledger` 的第一行。都没有就报「读不到」并退
//! 非零——不回落共享清单（两个工人读同一本，其中一边的活永远没人做）。

use std::path::PathBuf;

/// 这台机器上这一本台账在哪。
pub fn ledger_path() -> Result<PathBuf, String> {
    for key in ["COGNEVA_TODO_SOURCE", "COGNEVA_TODO_LEDGER"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                let path = value.strip_prefix("md:").unwrap_or(value);
                return Ok(PathBuf::from(path));
            }
        }
    }
    let host = std::env::var("HOME")
        .map(|h| PathBuf::from(h).join(".config/cogneva/todo-ledger"))
        .map_err(|_| "没有 HOME，也没有显式台账地址".to_string())?;
    let first = std::fs::read_to_string(&host)
        .map_err(|e| format!("台账地址读不到：{}（{e}）", host.display()))?;
    let line = first
        .lines()
        .next()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .ok_or_else(|| format!("{} 是空的：这一台机器没有声明台账", host.display()))?;
    Ok(PathBuf::from(line))
}

/// 读这一本台账并打印未完成的那几条。
///
/// 退 1 表示「还有没做完的」，退 0 表示整本都到了终态；读不到退 2——三种情况分开，
/// 因为「读不到」与「都做完了」在只看退出码的调用方眼里是同一件事。
pub fn status() -> i32 {
    let path = match ledger_path() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ledger: 台账读不到：{}（{e}）", path.display());
            return 2;
        }
    };
    let items = match cog_core::contract::ledger_item::parse_md(&text) {
        Ok(items) => items,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let open: Vec<_> = items.iter().filter(|i| i.is_in_batch()).collect();
    println!(
        "ledger: {}（{} 条未完成 / {} 条读到）",
        path.display(),
        open.len(),
        items.len()
    );
    for item in &open {
        println!("  未完成 {}  {}", item.id, item.gap);
    }
    if open.is_empty() {
        0
    } else {
        1
    }
}

/// `cogneva ledger <action>` 的分派。今天只有 `status`；别的名字点名说没有，
/// 不静默成功——一个不认识的子命令退 0 会被读成「跑过了」。
pub fn run() -> i32 {
    match std::env::args().nth(2).as_deref() {
        Some("status") | None => status(),
        Some(other) => {
            eprintln!("ledger: 不认识的动作 `{other}`（今天只有 `status`）");
            2
        }
    }
}
