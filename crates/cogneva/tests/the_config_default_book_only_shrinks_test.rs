//! 配置里那些「没人说它凭什么取这个数」的默认值，只许减。
//!
//! 常量面上的 `// cap: bound` 已经有棘轮（`the_census_finds_no_undisposed_bound_or_loss_test.rs` 与 `scripts/bounds-census.py`），
//! 但同一个病在配置面上换了两个形状，两个都没有东西守着：
//!
//! - `fn default_xxx() -> T`：serde 的默认函数，值是函数体里那一行；
//! - `impl Default for X` 里 `字段: 数字`：构造函数里的字面量。
//!
//! 两者在运行期与「跟着读数走的默认值」同形——读数变了它不变时，纸上和代码里都看不出
//! 差别，直到有人按旧数算错一次。判法沿用常量面：那一行（或紧挨着它的上一行）写了
//! `// cap: bound` 或 `// cap: fixed`，就算**声明过来源**，不进这本账；没写的就记在
//! 账上，只许减。减的办法只有两条：改成由一条读数算出（并补一条换输入值就跟着变的
//! 测试），或删掉这个默认让它由别的读数看见。
//!
//! 底账按文件记，理由与宽标签那道门相同：一个文件涨两条、另一个降两条时总数不动，
//! 而涨的那一处正是没人判过的新界。
//!
//! 没有这次改动这条会红：配置面的默认值没有任何棘轮，新加一个回头就能再长回来。

use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;

/// 直接长在那一行上面的判词：有它就算声明过来源。
fn declared(lines: &[&str], index: usize) -> bool {
    index > 0 && {
        let previous = lines[index - 1];
        previous.contains("// cap: bound") || previous.contains("// cap: fixed")
    }
}

/// 一行里有没有数字 token：注释与字符串不算，`A2A`、`"1.0.0"` 这类不是数。
fn has_number(line: &str) -> bool {
    let code = line.split("//").next().unwrap_or("");
    let mut in_string = false;
    let chars: Vec<char> = code.chars().collect();
    for (index, ch) in chars.iter().enumerate() {
        if *ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string || !ch.is_ascii_digit() {
            continue;
        }
        let before_bad = index > 0
            && (chars[index - 1].is_ascii_alphanumeric()
                || chars[index - 1] == '_'
                || chars[index - 1] == '.');
        if !before_bad {
            return true;
        }
    }
    false
}

/// serde 默认函数：`fn default_…() -> T {` 一行，值在函数体第一行。
///
/// 只在函数体那一行**带数字字面量**时才算一条：函数体只是一个具名常量时，
/// 那个数已经在那条常量上判过（或记在常量面的账上），在这里再记一次是把同一笔
/// 欠账数两遍；`true`／`false` 也不是数。
fn serde_defaults(lines: &[&str]) -> Vec<usize> {
    lines
        .iter()
        .enumerate()
        .filter(|(index, line)| {
            let trimmed = line.trim();
            if !(trimmed.starts_with("fn default_") && trimmed.contains("() ->")) {
                return false;
            }
            lines[index + 1..(index + 3).min(lines.len())]
                .iter()
                .any(|body| has_number(body))
        })
        .map(|(index, _)| index)
        .collect()
}

/// `impl Default for X` 块里 `字段: 数字` 的行号。
fn default_impl_literals(lines: &[&str]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut inside = false;
    let mut depth = 0i64;
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if !inside && trimmed.starts_with("impl Default for ") {
            inside = true;
            depth = 0;
        }
        if !inside {
            continue;
        }
        depth += line.matches('{').count() as i64 - line.matches('}').count() as i64;
        if depth > 0 {
            let mut parts = trimmed.splitn(2, ':');
            let field = parts.next().unwrap_or("").trim();
            let value = parts.next().unwrap_or("").trim_start();
            let field_is_name = !field.is_empty()
                && field
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            let value_is_number = value
                .chars()
                .next()
                .map(|c| c.is_ascii_digit())
                .unwrap_or(false);
            if field_is_name && value_is_number {
                out.push(index);
            }
        }
        if depth <= 0 {
            inside = false;
        }
    }
    out
}

#[test]
fn the_config_default_book_only_shrinks() {
    let root = repo_root();
    let baseline_path = root.join("deploy/scripts/config-default-bounds.baseline");
    let baseline_text = std::fs::read_to_string(&baseline_path)
        .expect("配置默认值的底账读不到（deploy/scripts/config-default-bounds.baseline）");
    let mut baseline: std::collections::BTreeMap<String, usize> = Default::default();
    for line in baseline_text.lines() {
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let (path, count) = line.split_once('\t').expect("底账是一行两格");
        baseline.insert(
            path.to_string(),
            count.trim().parse().expect("条数是一个数"),
        );
    }

    let mut seen = 0usize;
    let mut offenders = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        if relative.contains("/tests/") {
            continue;
        }
        let lines: Vec<&str> = text.lines().collect();
        let mut undeclared = 0usize;
        for index in serde_defaults(&lines)
            .into_iter()
            .chain(default_impl_literals(&lines))
        {
            if !declared(&lines, index) {
                undeclared += 1;
            }
        }
        seen += undeclared;
        match baseline.get(&relative) {
            Some(allowed) if undeclared > *allowed => offenders.push(format!(
                "{relative}: {undeclared} 条未声明来源的默认值，底账 {allowed} 条"
            )),
            Some(_) => {}
            None if undeclared > 0 => offenders.push(format!(
                "{relative}: 新出现 {undeclared} 条未声明来源的默认值，底账里没有这个文件"
            )),
            None => {}
        }
    }
    for path in baseline.keys() {
        if !root.join(path).exists() {
            offenders.push(format!("{path}: 底账点名的文件不在树里（底账过期了）"));
        }
    }
    assert!(
        offenders.is_empty(),
        "配置默认值的存量涨了或有新文件带着它进来：\n  {}\n（生产代码合计 {seen} 条）",
        offenders.join("\n  ")
    );
}
