//! principle: feedback_no_hardcoding_config_surface
//!
//! 第三处声明默认值的地方：不是 `impl Default`，也不是 `fn default_xxx`，而是
//! 一个叫 `defaults()` 的构建函数——它返回一张「标准配置表」，表里每行都是一组
//! 写在结构体字面量里的数。
//!
//! 前两处各有一道门看着：`impl Default` 与 `fn default_*` 里的裸数归 D5 那条
//! （`a_written_default_is_named_or_derived_test`），`const` 里的数归容量登记表。
//! `fn defaults()` 两边都不认：D5 的题目只匹配 `fn default_*`，容量登记表只认
//! `const`，于是 `RetryMatrix::defaults()` 那整张退避表——重试次数、熔断阈值、
//! 半开探测数——一个判词都没有，而两道门都读绿。这正是「按方便的形状立门」的
//! 老毛病：形状换了，数还在。
//!
//! 判据与 D5 那条同一套词汇：每个数要么是一个具名常量（理由写在它的 `// cap:`
//! 行上，容量登记表也看得见它），要么是一条只由名字组成的表达式，要么是哨兵
//! `0`（「没写」）。裸数字除此之外都不是。

use crate::common::lex::Dialect;
use crate::common::routes::{matching_brace, repo_root};
use crate::common::rust_production::{
    production_numbered_lines, production_sources, relative_name,
};
use crate::common::rust_text::{bare_number, struct_fields};

/// 一处裸数字：哪个文件的哪个函数里，哪个字段。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Written {
    file: String,
    line: usize,
    function: String,
    field: String,
    value: String,
}

#[test]
fn a_defaults_table_names_its_numbers() {
    let root = repo_root();
    let mut found = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rel = relative_name(&root, &path);
        found.extend(bare_numbers_in_defaults(&text, &rel));
    }
    found.sort();
    assert!(
        found.is_empty(),
        "`fn defaults()` 返回的是出厂配置本身，表里每个数都是「这个程序取这个值」的\
         一句话。它要么是一个具名常量（`// cap:` 行与它的证明就在声明处），要么是一条\
         只由名字组成的表达式，要么是哨兵 `0`（「没写」）。裸数字三种都不是：\n{}",
        found
            .iter()
            .map(|w| format!(
                "{}:{}  {}::{} = {}",
                w.file, w.line, w.function, w.field, w.value
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn bare_numbers_in_defaults(text: &str, rel: &str) -> Vec<Written> {
    let production = production_numbered_lines(text);
    let joined: String = production
        .iter()
        .map(|(_, line)| *line)
        .collect::<Vec<_>>()
        .join("\n");
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(found) = joined[at..].find("fn defaults") {
        let start = at + found;
        at = start + "fn defaults".len();
        // 只要 `fn defaults(`：`fn defaults_for(...)` 是别的题目。
        if !joined[at..].trim_start().starts_with('(') {
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
            let line_no = joined[..open + 1 + offset].matches('\n').count() + 1;
            let line_no = production
                .get(line_no.saturating_sub(1))
                .map(|(n, _)| *n)
                .unwrap_or(line_no);
            out.push(Written {
                file: rel.to_string(),
                line: line_no,
                function: "defaults".to_string(),
                field,
                value: bare,
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// 这条门不是空转。

#[test]
fn the_defaults_gate_names_the_table_and_not_a_named_value() {
    let fixture = r#"
impl Matrix {
    pub fn defaults() -> Self {
        let mut rows = HashMap::new();
        rows.insert(
            Kind::Llm,
            Config {
                retries: 3,
                backoff: MILLIS_PER_SECOND,
                breaker: Some(Breaker { threshold: 5, probes: 0 }),
            },
        );
        Self { rows, limit: 24 * 1024 }
    }

    pub fn defaults_for(kind: Kind) -> Self {
        Self { retries: 9 }
    }
}

#[cfg(test)]
mod tests {
    fn defaults() -> Self {
        Self { retries: 7 }
    }
}
"#;
    let found = bare_numbers_in_defaults(fixture, "probe.rs");
    let named: Vec<String> = found
        .iter()
        .map(|w| format!("{}::{} = {}", w.function, w.field, w.value))
        .collect();
    assert_eq!(
        named,
        vec![
            "defaults::retries = 3".to_string(),
            "defaults::threshold = 5".to_string(),
            "defaults::limit = 24 * 1024".to_string(),
        ]
    );
}
