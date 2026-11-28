//! `select!` 里带 sleep/tick 的等待点，每一个都在分类账上回答过「取消的是什么」。
//!
//! 这一层是 `tokio::time::timeout(` 那道门的另一半。`timeout` 的取消由构造保证，
//! 所以判据可以落在调用行的判词上；`select!` 里的一支 sleep 只有在读懂**另一支**才
//! 知道它是节拍还是墙——节拍那一支取消的是等待本身（读、tick，重来即可），墙那一支
//! 取消的是一段做了一半的活。静态判不出来，于是把判断写在一处账上，逐条给理由：
//! 账只许减，新增一处不分类就红（`deploy/scripts/select-sleep-sites.tsv`）。
//!
//! 账不许比树大同样要判：一份分类留在账上而树里那一处已经不在了，读的人会把它当成
//! 一条还在生效的判断。

use crate::common::routes::repo_root;
use crate::common::rust_production::{
    production_numbered_lines_keeping_comments, production_texts,
};

/// 从 `select!` 开始往下看多少行去找 sleep/tick 那一支。
const WINDOW: usize = 22;
const BOOK: &str = "deploy/scripts/select-sleep-sites.tsv";

fn is_sleep_branch(line: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") {
        return false;
    }
    (line.contains("sleep(") || line.contains(".tick()") || line.contains("tick()"))
        && line.contains("=>")
}

/// 树里每一处 `select!` 的 sleep/tick 分支：`(路径, 分支行, 同文件同行的第几个)`。
fn sites(root: &std::path::Path) -> Vec<(String, String, usize)> {
    let mut out = Vec::new();
    for (path, text) in production_texts(root) {
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        let numbered = production_numbered_lines_keeping_comments(&text);
        let lines: Vec<&str> = numbered.iter().map(|(_, line)| *line).collect();
        let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains("select!") || line.trim_start().starts_with("//") {
                continue;
            }
            let end = (index + WINDOW).min(lines.len());
            let Some(branch) = lines[index..end]
                .iter()
                .find(|candidate| is_sleep_branch(candidate))
            else {
                continue;
            };
            let normalized = branch.trim().to_string();
            let occurrence = seen.entry(normalized.clone()).or_insert(0);
            *occurrence += 1;
            out.push((relative.clone(), normalized, *occurrence));
        }
    }
    out
}

/// 账上一行：路径、分支行、第几个、类别与理由。
struct Entry {
    path: String,
    branch: String,
    occurrence: usize,
    kind: String,
    reason: String,
}

fn book(root: &std::path::Path) -> Vec<Entry> {
    let text = std::fs::read_to_string(root.join(BOOK))
        .unwrap_or_else(|e| panic!("读不到分类账 {BOOK}: {e}"));
    let mut entries = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') || line.starts_with("path\t") {
            continue;
        }
        let cells: Vec<&str> = line.split('\t').collect();
        assert_eq!(
            cells.len(),
            5,
            "{BOOK}:{} 不是五格（path/branch/occurrence/kind/reason）：{line}",
            number + 1
        );
        entries.push(Entry {
            path: cells[0].to_string(),
            branch: cells[1].to_string(),
            occurrence: cells[2].parse().unwrap_or_else(|_| {
                panic!("{BOOK}:{} 的 occurrence 不是数：{}", number + 1, cells[2])
            }),
            kind: cells[3].to_string(),
            reason: cells[4].to_string(),
        });
    }
    entries
}

/// 判据自己先证明会响：sleep 分支认得出来，`{}` 空体的节拍也认得出来，别的不会。
#[test]
fn the_predicate_finds_the_sleep_branch_and_only_it() {
    assert!(is_sleep_branch("_ = tokio::time::sleep(bound) => {"));
    assert!(is_sleep_branch("_ = ticker.tick() => {}"));
    assert!(!is_sleep_branch("item = rx.recv() => work(item),"));
    assert!(!is_sleep_branch("// _ = tokio::time::sleep(x) => {}"));
}

/// 树里每一处 select-sleep 都在账上分过类，账上每一条也都还在树里。
#[test]
fn every_select_sleep_is_classified() {
    let root = repo_root();
    let entries = book(&root);
    assert!(!entries.is_empty(), "分类账是空的：这不是「都分过类」");
    let found = sites(&root);

    let key = |path: &str, branch: &str, occurrence: usize| {
        (path.to_string(), branch.to_string(), occurrence)
    };
    let in_book: std::collections::HashSet<_> = entries
        .iter()
        .map(|entry| key(&entry.path, &entry.branch, entry.occurrence))
        .collect();

    let mut problems = Vec::new();
    for (path, branch, occurrence) in &found {
        if !in_book.contains(&key(path, branch, *occurrence)) {
            problems.push(format!(
                "{path}: 一处 select-sleep 没有分类：{branch}。到 `{BOOK}` 补一行——\n\
                 节拍那一支写 `pace` 并说清它一拍做的是什么；取消一段活的写 `wall` 并答\n\
                 `等=… / 等不到=… / 续跑=…`"
            ));
        }
    }
    let found_set: std::collections::HashSet<_> = found
        .iter()
        .map(|(path, branch, occurrence)| key(path, branch, *occurrence))
        .collect();
    for entry in &entries {
        if !found_set.contains(&key(&entry.path, &entry.branch, entry.occurrence)) {
            problems.push(format!(
                "{BOOK} 记了 {} 的 `{}`（第 {} 个），树里已经没有这一处：账不许比树大",
                entry.path, entry.branch, entry.occurrence
            ));
        }
        assert!(
            entry.kind == "pace" || entry.kind == "wall",
            "{} 的类别是 `{}`：只有 pace 与 wall 两种",
            entry.path,
            entry.kind
        );
        if entry.kind == "wall" {
            for field in ["等=", "等不到=", "续跑="] {
                assert!(
                    entry.reason.contains(field),
                    "{} 的墙判词缺 `{field}`：{}",
                    entry.path,
                    entry.reason
                );
            }
        } else {
            assert!(
                entry.reason.contains("取消的是等待本身"),
                "{} 的 pace 判词要说清它取消的是等待本身，而不是一段活：{}",
                entry.path,
                entry.reason
            );
        }
    }
    assert!(
        problems.is_empty(),
        "select-sleep 的分类账与树对不上：\n  {}",
        problems.join("\n  ")
    );
}
