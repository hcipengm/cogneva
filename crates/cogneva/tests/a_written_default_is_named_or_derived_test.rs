//! principle: feedback_no_hardcoding_config_surface
//!
//! 一个数写进 `impl Default` 或 `fn default_*`，也是「这个程序取这个值」的一句话，
//! 与交付文档里的叶子、与 `const` 里的数一样，都要回答它凭什么。容量登记表
//! （`rust_capacity_constants_registry_test`）把这半边明确划了出去——它头部写着
//! 「`impl Default` 不是常量，这份测试永远看不到它；那些数的问题归配置面」——而
//! 配置面一直只有两道走查：文档写的键有字段接着、类型能读的键有文档写出来。两道
//! 都不问数的来源，于是几百个数字写在最显眼的地方而没有任何一道门看得见它们。
//!
//! 这道门补的就是这一格，判的是**终态**，不是欠账：
//!
//! - 具名常量或含标识符的表达式（`A2A_LIST_PAGE_SIZE as usize`、
//!   `DEFAULT_REDIS_READ_BLOCK_MS / 1000`）：这个数在它被写下的地方已经有人回答过
//!   「它凭什么」——`// cap:` 行与它的证明测试就在那里。门不再问第二遍：同一个问题
//!   问两处，两处迟早各答各的。
//! - 哨兵 `0`：它不是谁选的数，是「没写」这一件事本身。读到 0 的那一层负责把它
//!   换成一个真实的来源，或者拒绝启动——换什么归那一层的测试判，不归这道门。
//! - 除此之外的一切裸数字（含 `24 * 1024` 这种只由数字组成的算式）都是红。
//!
//! 为什么是零容忍而不是「不许增长」：一张按文件记数的表会读成「已入账」，而它真正
//! 的含义只是「我看到的这些没超」。题目是树本身——`crates/*/src` 里所有
//! `impl Default` 与 `fn default_*`——判据是那里没有裸数字，不是它们不再变多。
//! 按形状立门、从没数过全集的教训已经付过一次。
//!
//! 不判什么，说清楚：这道门不判一个具名常量**是不是真的**由哪条实测导出——那是
//! `// cap:` 行与容量登记表的事；也不判哨兵 0 在读侧换回了什么——那是读侧那条
//! 「0 不能变成坏掉的界」的测试的事。

use crate::common::lex::Dialect;
use crate::common::routes::{matching_brace, repo_root};
use crate::common::rust_production::{
    production_numbered_lines, production_sources, relative_name,
};
use crate::common::rust_text::{bare_number, struct_fields};

/// One bare number a default invents, and where it is written.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Written {
    file: String,
    line: usize,
    what: String,
    value: String,
}

#[test]
fn a_written_default_is_named_or_derived() {
    let root = repo_root();
    let mut found = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rel = relative_name(&root, &path);
        found.extend(bare_numbers(&text, &rel));
    }
    found.sort();
    assert!(
        found.is_empty(),
        "这些默认值自己发明了一个数。一个默认值要么是一个具名常量、或一条只由名字\
         组成的表达式（它的 `// cap:` 行与证明测试就在声明处），要么是哨兵 `0`\
         （「没写」，由读到它的那一层换回真实来源或拒绝启动）。裸数字三种都不是：\n{}",
        found
            .iter()
            .map(|w| format!("{}:{}  {} = {}", w.file, w.line, w.what, w.value))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn bare_numbers(text: &str, rel: &str) -> Vec<Written> {
    let production = production_numbered_lines(text);
    let joined: String = production
        .iter()
        .map(|(_, line)| *line)
        .collect::<Vec<_>>()
        .join("\n");
    let mut out = Vec::new();

    // 第一处形状：`impl Default for X { ... }`。
    let mut at = 0;
    while let Some(found) = joined[at..].find("impl Default for ") {
        let start = at + found;
        at = start + "impl Default for ".len();
        let rest = &joined[at..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if name.is_empty() {
            continue;
        }
        let Some(open) = joined[start..].find('{') else {
            continue;
        };
        let open = start + open;
        let Some(end) = matching_brace(&joined, open, Dialect::Rust) else {
            continue;
        };
        let body = &joined[open + 1..end];
        for (offset, field, value) in struct_fields(body) {
            let Some(bare) = bare_number(&value) else {
                continue;
            };
            out.push(Written {
                file: rel.to_string(),
                line: line_at(&joined, &production, open + 1 + offset),
                what: format!("{name}::{field}"),
                value: bare,
            });
        }
    }

    // 第二处形状：`fn default_xxx() -> <类型> { ... }`。
    let mut at = 0;
    while let Some(found) = joined[at..].find("fn default_") {
        let start = at + found;
        at = start + "fn default_".len();
        let Some(open) = joined[start..].find('{') else {
            continue;
        };
        let open = start + open;
        if !joined[start..open].contains("->") {
            continue;
        }
        let Some(end) = matching_brace(&joined, open, Dialect::Rust) else {
            continue;
        };
        let body = joined[open + 1..end].trim();
        let name = function_name(&joined[start..open]);
        // 体就是一个数：`fn default_x() -> u32 { 30 }`。
        if let Some(bare) = bare_number(body) {
            out.push(Written {
                file: rel.to_string(),
                line: line_at(&joined, &production, open + 1),
                what: name,
                value: bare,
            });
            continue;
        }
        // 体是一个结构体字面量：`QuotaLimits { soft_limit: 80_000 }`。里面的字段
        // 与 `impl Default` 里的一样，是写在默认值上的数。
        for (offset, field, value) in struct_fields(body) {
            let Some(bare) = bare_number(&value) else {
                continue;
            };
            out.push(Written {
                file: rel.to_string(),
                line: line_at(&joined, &production, open + 1 + offset),
                what: format!("{}::{field}", name),
                value: bare,
            });
        }
    }
    out
}

/// 一个拼接后文本里的字节偏移，回到原文件的行号。
fn line_at(joined: &str, production: &[(usize, &str)], offset: usize) -> usize {
    let line_no = joined[..offset.min(joined.len())].matches('\n').count() + 1;
    production
        .get(line_no.saturating_sub(1))
        .map(|(n, _)| *n)
        .unwrap_or(line_no)
}

fn function_name(head: &str) -> String {
    head.trim_start_matches("fn ")
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect()
}

// ---------------------------------------------------------------------------
// 这条门不是空转。

#[test]
fn the_gate_names_a_number_a_default_invents_and_lets_a_name_through() {
    let fixture = r#"
const NAMED: u32 = 30;

impl Default for Plain {
    fn default() -> Self {
        Self {
            retries: 3,
            backoff: MILLIS_PER_SECOND,
            scaled: 24 * 1024,
            unit: A2A_LIST_PAGE_SIZE as usize,
            unwritten: 0,
            unwritten_float: 0.0f64,
        }
    }
}

impl Default for Quiet {
    fn default() -> Self {
        Self { timeout: NAMED, off: false }
    }
}

fn default_quota() -> QuotaLimits {
    QuotaLimits { soft_limit: 80_000, hard_limit: 100_000 }
}

fn default_flag() -> bool {
    true
}

#[cfg(test)]
mod tests {
    impl Default for Fixture {
        fn default() -> Self {
            Self { retries: 7 }
        }
    }

    fn default_fixture() -> u32 {
        9
    }
}
"#;
    let found = bare_numbers(fixture, "probe.rs");
    let named: Vec<String> = found
        .iter()
        .map(|w| format!("{} = {}", w.what, w.value))
        .collect();
    assert_eq!(
        named,
        vec![
            "Plain::retries = 3".to_string(),
            "Plain::scaled = 24 * 1024".to_string(),
            "default_quota::soft_limit = 80_000".to_string(),
            "default_quota::hard_limit = 100_000".to_string(),
        ],
        "夹具里的 `#[cfg(test)]` 是读它的测试的一部分，不归这道门管；\
         名字、哨兵 0、类型为 bool 的默认都不该被点名"
    );
}

/// 行号要指回原文件那一行，不是拼接之后的第几行——跳过夹具之后两者对不上。
#[test]
fn the_finding_points_at_the_line_in_the_file() {
    let fixture = "#[cfg(test)]\nmod tests {\n    // 夹具\n}\n\nimpl Default for P {\n    fn default() -> Self {\n        Self { retries: 3 }\n    }\n}\n";
    let found = bare_numbers(fixture, "probe.rs");
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].line, 8, "指回原文件的第 8 行");
}
