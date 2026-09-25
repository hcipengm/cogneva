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

/// Binary operators. Two operands cannot stand side by side without one of
/// these between them, and neither can a matching clause be put there instead.
const BINARY_OPERATORS: &[&str] = &[
    "+", "-", "*", "/", "%", "^", "==", "!=", ">", "<", ">=", "<=", "=~", "!~", "and", "or",
    "unless", "atan2",
];

/// Aggregations, i.e. the words a `by`/`without` clause may follow.
const AGGREGATIONS: &[&str] = &[
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "stddev",
    "stdvar",
    "topk",
    "bottomk",
    "quantile",
    "count_values",
    "group",
    "limitk",
    "limit_ratio",
];

/// Which kind of unit a clause keyword is: one that selects the labels a binary
/// operation matches on, or one that selects the labels an aggregation keeps.
#[derive(PartialEq, Eq, Clone, Copy)]
enum Clause {
    Matching,
    Grouping,
}

fn clause_kind(word: &str) -> Option<Clause> {
    match word {
        "on" | "ignoring" | "group_left" | "group_right" => Some(Clause::Matching),
        "by" | "without" => Some(Clause::Grouping),
        _ => None,
    }
}

/// The last token before `end`, skipping the units PromQL allows to sit between
/// an operator and what follows it: a `bool` modifier, and the matching clause
/// of a binary operation (`on (...)` / `ignoring (...)` / `group_left (...)`),
/// which may itself be preceded by another one.
///
/// Returns `None` at the start of the string.
fn token_before(s: &str, end: usize) -> Option<&str> {
    let bytes = s.as_bytes();
    let mut i = end;
    loop {
        while i > 0 && (bytes[i - 1] as char).is_ascii_whitespace() {
            i -= 1;
        }
        if i == 0 {
            return None;
        }
        let c = bytes[i - 1] as char;
        if c == ')' || c == ']' {
            // Walk back to the opening bracket, then see whether a clause
            // keyword owns it; a clause unit is skipped, a group is an operand.
            let (open, close) = if c == ')' { ('(', ')') } else { ('[', ']') };
            let mut depth = 0usize;
            let mut j = i;
            while j > 0 {
                let d = bytes[j - 1] as char;
                if d == close {
                    depth += 1;
                } else if d == open {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                j -= 1;
            }
            if c == ']' {
                return Some(&s[j..i]);
            }
            let mut k = j - 1;
            while k > 0 && (bytes[k - 1] as char).is_ascii_whitespace() {
                k -= 1;
            }
            let word_start = s[..k]
                .rfind(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                .map(|p| p + 1)
                .unwrap_or(0);
            if clause_kind(&s[word_start..k]).is_some() {
                i = word_start;
                continue;
            }
            return Some(&s[j..i]);
        }
        if "+-*/%^<>=!".contains(c) {
            let mut j = i;
            while j > 0 && "+-*/%^<>=!".contains(bytes[j - 1] as char) {
                j -= 1;
            }
            return Some(&s[j..i]);
        }
        let word_start = s[..i]
            .rfind(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'))
            .map(|p| p + 1)
            .unwrap_or(0);
        let word = &s[word_start..i];
        if word == "bool" {
            i = word_start;
            continue;
        }
        return Some(word);
    }
}

/// What a reader can decide about a PromQL expression without a parser, on the
/// side where the failure is silent.
///
/// An expression Prometheus cannot parse draws a blank panel and fires no rule:
/// the dashboard shows an empty graph that reads as "nothing happening" and the
/// alert stays quiet through the incident it was written for. The name-level
/// checks in both contract tests pass it, because every series in it is really
/// produced — what is missing is the operator. That is the shape this catches:
/// a matching or grouping clause with no binary operator before it, which is
/// what a hand-edited `A{...} on(pod) (B{...} > 0)` looks like when the `/` is
/// dropped. Unbalanced brackets are caught for the same reason.
///
/// This is not a parser, and being explicit about the ceiling is the point: a
/// well-formed-shape expression naming a series that does not exist, a wrong
/// label in a matcher, or a mistyped aggregation all pass. What it decides is
/// the one edit that leaves every series name intact and the expression
/// unparseable, which is the edit whose result nothing else in this repository
/// can see.
pub fn shape_complaints(expr: &str) -> Vec<String> {
    let mut out = Vec::new();
    let stripped = strip_braces(&strip_quoted(expr));

    let bytes = stripped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if !(c.is_ascii_alphabetic() || c == '_')
            || (i > 0 && {
                let prev = bytes[i - 1] as char;
                prev.is_ascii_alphanumeric() || prev == '_' || prev == ':'
            })
        {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && {
            let ch = bytes[i] as char;
            ch.is_ascii_alphanumeric() || ch == '_'
        } {
            i += 1;
        }
        let word = &stripped[start..i];
        let Some(kind) = clause_kind(word) else {
            continue;
        };
        let mut j = i;
        while j < bytes.len() && (bytes[j] as char).is_ascii_whitespace() {
            j += 1;
        }
        let has_list = j < bytes.len() && bytes[j] as char == '(';
        // `group_left` / `group_right` may omit the label list — `on(node)
        // group_left B` is valid — so only the others are a parse error without
        // one. Either way the operator check below still applies.
        if !has_list && !matches!(word, "group_left" | "group_right") {
            out.push(format!("`{word}` 后面没有标签列表"));
            continue;
        }
        let before = token_before(&stripped, start);
        let ok = match (kind, before) {
            (Clause::Matching, Some(t)) => BINARY_OPERATORS.contains(&t),
            (Clause::Grouping, Some(t)) => AGGREGATIONS.contains(&t),
            (_, None) => false,
        };
        if !ok {
            let shown = before.unwrap_or("<表达式开头>");
            out.push(match kind {
                Clause::Matching => format!(
                    "`{word} (...)` 前面是 `{shown}`，没有二元运算符：\
                     这一句 Prometheus 解析不了，面板会空着、规则不会触发"
                ),
                Clause::Grouping => format!(
                    "`{word} (...)` 前面是 `{shown}`，不是聚合函数名：\
                     这一句 Prometheus 解析不了，面板会空着、规则不会触发"
                ),
            });
        }
    }

    let mut depth = 0i32;
    for c in stripped.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth < 0 {
            out.push("括号不配对：多出一个 `)`".to_string());
            break;
        }
    }
    if depth > 0 {
        out.push(format!("括号不配对：少 {depth} 个 `)`"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shapes the two consumers really write. A gate that reports one of
    /// these is a gate someone has to work around, and a worked-around gate is
    /// the one that stops being read.
    const WRITTEN_BY_HAND: &[&str] = &[
        // Plain rate over one series, quoted matcher and all.
        r#"sum(ALERTS{alertstate="firing"})"#,
        // An aggregation with its clause, dividing by another one.
        r#"sum by (intent) (rate(a{b="c|d"}[30m])) / sum by (intent) (rate(e{b="c|d"}[30m]))"#,
        // A matching clause in a division, with a nested comparison on the right.
        r#"a{container!="", container!="POD"} / on(namespace, pod, container) (b{resource="memory"} > 0)"#,
        // The set operator form: the clause follows the operator, not the operand.
        r#"sum by (node, resource) (a{b=~"x|y"} and on(namespace, pod) c{phase="Running"} == 1)"#,
        // `bool` between the operator and the clause.
        r#"a{b="c"} == bool on(pod) d{p="q"}"#,
        // A clause after a clause, which is where `group_left` sits.
        r#"sum by (node, resource) (a) / on(node, resource) group_left sum by (node, resource) (b)"#,
        // `group_left` may omit its label list.
        r#"sum by (node) (a) / on(node) group_left sum by (node) (b)"#,
        // A range selector and a subquery take brackets a bare count would flag.
        r#"avg_over_time((count(a{b="c"} and on(pod) d{p="q"}) - count(e))[15m:1m])"#,
    ];

    #[test]
    fn the_shapes_both_consumers_write_are_silent() {
        for expr in WRITTEN_BY_HAND {
            assert!(
                shape_complaints(expr).is_empty(),
                "{expr} 是合法形态，却被判据报了: {:?}",
                shape_complaints(expr)
            );
        }
    }

    /// The defect this gate was written for: the operator between the two
    /// operands was dropped, every series name survived, and the name-level
    /// checks in both contract tests still pass.
    #[test]
    fn a_matching_clause_with_no_operator_before_it_is_reported() {
        let complaints = shape_complaints(
            r#"a{container!="POD"} on(namespace, pod) (b{resource="memory"} > 0)"#,
        );
        assert_eq!(complaints.len(), 1, "{complaints:?}");
        assert!(complaints[0].contains("`on"), "{}", complaints[0]);
    }

    #[test]
    fn a_grouping_clause_that_no_aggregation_owns_is_reported() {
        let complaints = shape_complaints("sum(rate(a[5m])) by (job)");
        assert_eq!(complaints.len(), 1, "{complaints:?}");
        assert!(complaints[0].contains("`by"), "{}", complaints[0]);
    }

    #[test]
    fn a_clause_with_no_label_list_is_reported_unless_it_may_omit_one() {
        assert!(
            shape_complaints("a / on b")
                .iter()
                .any(|c| c.contains("标签列表")),
            "`on` 没有标签列表时也解析不了"
        );
        assert!(
            shape_complaints("a by (job)")
                .iter()
                .any(|c| c.contains("不是聚合函数名")),
            "`by` 只能跟在聚合函数后面"
        );
        // `group_left` / `group_right` may omit theirs, so the operator check is
        // the only one that applies — and an operator is there.
        assert!(shape_complaints("a / on(pod) group_left b").is_empty());
    }

    #[test]
    fn brackets_that_do_not_balance_are_reported() {
        assert!(shape_complaints("sum(a").iter().any(|c| c.contains("少 1")));
        assert!(shape_complaints("sum(a))")
            .iter()
            .any(|c| c.contains("多出一个")));
        assert!(shape_complaints("sum((a)")
            .iter()
            .any(|c| c.contains("少 1")));
    }

    /// The first thing the gate can see is the first clause in the expression:
    /// a clause at the very start has no operator before it and no operand
    /// either.
    #[test]
    fn a_clause_at_the_start_of_an_expression_is_reported() {
        let complaints = shape_complaints("on(pod) (a)");
        assert_eq!(complaints.len(), 1, "{complaints:?}");
        assert!(complaints[0].contains("表达式开头"), "{}", complaints[0]);
    }
}
