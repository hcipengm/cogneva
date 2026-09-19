//! Series names a PromQL expression reads.
//!
//! Both observability contract tests need this, and they need the same answer:
//! a series one of them refuses to extract is a series that side of the
//! contract silently stops checking. Included with `#[path]` rather than
//! through `common/mod.rs` so a test that only wants this does not have to
//! compile the harness and mocks alongside it.

use std::collections::BTreeSet;

/// Aggregation operators and keywords that look like identifiers but name no
/// series.
const NOT_SERIES: &[&str] = &[
    "and",
    "avg",
    "bool",
    "bottomk",
    "by",
    "count",
    "count_values",
    "end",
    "group",
    "group_left",
    "group_right",
    "ignoring",
    "infinity",
    "limit_ratio",
    "limitk",
    "max",
    "min",
    "nan",
    "offset",
    "on",
    "or",
    "quantile",
    "start",
    "stddev",
    "stdvar",
    "sum",
    "topk",
    "unless",
    "without",
];

/// Drop `"..."` and `` `...` `` spans, keeping escapes inside a quoted span.
/// Those hold label values, which are not series names.
fn strip_quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '"' || c == '`' {
            let quote = c;
            while let Some(c2) = chars.next() {
                if c2 == '\\' && quote == '"' {
                    chars.next();
                    continue;
                }
                if c2 == quote {
                    break;
                }
            }
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out
}

/// Blank out everything inside `{...}`, nested included. Label matchers hold
/// label names and values, neither of which is a series name.
fn strip_braces(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut depth = 0usize;
    for c in s.chars() {
        match c {
            '{' => {
                depth += 1;
                out.push(' ');
            }
            '}' => {
                depth = depth.saturating_sub(1);
                out.push(' ');
            }
            _ if depth > 0 => out.push(' '),
            _ => out.push(c),
        }
    }
    out
}

/// Blank out `kw(...)`, e.g. `by (stream, pod)`. The parenthesised list holds
/// label names, not series names.
fn strip_keyword_parens(s: &str, kw: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        let boundary = i == 0 || {
            let prev = bytes[i - 1] as char;
            !prev.is_ascii_alphanumeric() && prev != '_'
        };
        if boundary && s[i..].starts_with(kw) {
            let after = i + kw.len();
            let trimmed = s[after..].trim_start();
            if trimmed.starts_with('(') {
                let open = after + (s[after..].len() - trimmed.len());
                let mut depth = 0usize;
                let mut j = open;
                while j < bytes.len() {
                    match bytes[j] as char {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                out.push(' ');
                i = if j < bytes.len() { j + 1 } else { bytes.len() };
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Series names a PromQL expression reads.
///
/// The expression is stripped of the places identifiers are not series names
/// (quoted spans, `{...}` matchers, `by (...)`/`on (...)` lists), then every
/// remaining identifier is taken unless it is a function name (followed by
/// `(`), a keyword or aggregation, or the tail of a duration (`30m`, `[1h]`,
/// `[1m:1m]`) — identified by the digit in front of it.
pub fn metric_names_in(expr: &str) -> BTreeSet<String> {
    let mut s = strip_braces(&strip_quoted(expr));
    for kw in [
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
    ] {
        s = strip_keyword_parens(&s, kw);
    }

    let bytes = s.as_bytes();
    let mut names = BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_ascii_alphabetic() || c == '_' || c == ':' {
            let start = i;
            while i < bytes.len() && {
                let ch = bytes[i] as char;
                ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
            } {
                i += 1;
            }
            let token = &s[start..i];
            let part_of_duration = start > 0 && (bytes[start - 1] as char).is_ascii_digit();
            let is_call = s[i..].trim_start().starts_with('(');
            if !part_of_duration && !is_call && !NOT_SERIES.contains(&token) {
                names.insert(token.to_string());
            }
            continue;
        }
        i += 1;
    }
    names
}
