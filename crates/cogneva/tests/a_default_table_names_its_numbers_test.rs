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

/// 体里所有 `字段: 值`，按字节偏移给出位置。
///
/// 不像 `impl Default` 那条门那样按逗号切块：一张表里的行是嵌套的
/// （`circuit_breaker: Some(CircuitBreakerConfig { failure_threshold: 5 })`），
/// 按逗号切会把里面那几个字段留在外层的值里，数就漏了。这里逐个词找
/// 「小写标识符 + 冒号」，值读到下一个逗号或右花括号为止。
fn struct_fields(body: &str) -> Vec<(usize, String, String)> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_lowercase() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let name = &body[start..i];
            let mut j = i;
            while j < bytes.len() && bytes[j] == b' ' {
                j += 1;
            }
            // `::` 是路径分隔符，不是字段赋值。
            if j < bytes.len() && bytes[j] == b':' && bytes.get(j + 1) != Some(&b':') {
                let mut k = j + 1;
                while k < bytes.len() && bytes[k] == b' ' {
                    k += 1;
                }
                let value_start = k;
                while k < bytes.len() && !matches!(bytes[k], b',' | b'}' | b'\n') {
                    k += 1;
                }
                out.push((
                    start,
                    name.to_string(),
                    body[value_start..k].trim().to_string(),
                ));
                // 停在冒号后面而不是值的末尾：值是嵌套结构时
                // （`Some(Breaker { threshold: 5 })`），里面的字段还在这个值里，
                // 跳过整个值就再也看不见它们了。
                i = j + 1;
                continue;
            }
            continue;
        }
        i += 1;
    }
    out
}

/// 哨兵：0 不是谁挑的数，是「没写」这件事本身。
fn is_sentinel_zero(value: &str) -> bool {
    let trimmed = value.trim();
    let digits: String = trimmed
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '_' || *c == '.')
        .filter(|c| *c != '_')
        .collect();
    !digits.is_empty() && digits.parse::<f64>() == Ok(0.0)
}

/// 一个只由数字组成的值：`30`、`1_000`、`4.1`、`24 * 1024`、`3600u64`。
///
/// 只要有一个部分不是以数字开头（`MILLIS_PER_SECOND`、`per_second`），它就是
/// 一条由名字组成的表达式——那个名字的来源归它自己的声明管。
fn bare_number(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || is_sentinel_zero(value) {
        return None;
    }
    let mut any = false;
    for part in value.split(['*', '+', '/']) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let first = part.chars().next()?;
        if !first.is_ascii_digit() {
            return None;
        }
        if !part
            .chars()
            .all(|c| c.is_ascii_digit() || c == '_' || c == '.' || c.is_ascii_lowercase())
        {
            return None;
        }
        any = true;
    }
    any.then(|| value.to_string())
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
