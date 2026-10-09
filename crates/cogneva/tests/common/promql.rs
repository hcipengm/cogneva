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

/// Range selectors with something after them that PromQL does not allow there.
///
/// A `[...]` may stand in one place only -- as a function's argument -- so the
/// tokens that may follow it are the bracket closing that call, a comma, the
/// `offset` or `@` modifier, and the end of the expression. Everything else is
/// a parse error, and the shape it is reached by is a guard attached to the
/// selector instead of to the aggregate: `min_over_time(x[6m] and (...))` reads
/// as though the guard narrows the samples the minimum is taken over, and what
/// it is is a range vector handed to a set operator.
///
/// The complaint is worth its own function because this is the one unparseable
/// edit the clause scan cannot see: the operator is present and the brackets
/// balance, so every other question in this module is answered as though the
/// text were fine.
fn range_selector_operand_complaints(stripped: &str) -> Vec<String> {
    let bytes = stripped.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b']' {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        i = j;
        if j >= bytes.len() {
            continue;
        }
        let c = bytes[j] as char;
        if c == ')' || c == ',' || c == '@' {
            continue;
        }
        let is_offset = stripped[j..].starts_with("offset")
            && (j + "offset".len() == bytes.len() || !token_char(bytes[j + "offset".len()]));
        if is_offset {
            continue;
        }
        let end = stripped[j..]
            .find(|ch: char| ch.is_ascii_whitespace() || "(),".contains(ch))
            .map(|p| j + p)
            .unwrap_or(bytes.len());
        out.push(format!(
            "区间选择子 `[...]` 后面跟着 `{}`：`[...]` 只能当函数的实参，\
             区间向量不是任何运算符的操作数，这一句 Prometheus 返回 400、规则一次都不会求值",
            &stripped[j..end]
        ));
    }
    out
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
/// The other shape is the mirror of that one: the operator is present and in
/// the wrong place. A range selector is only ever a function's argument, so a
/// binary or set operator sitting right after one leaves the expression
/// unparseable -- `min_over_time(x[6m] and (y > 0))` is where a guard written
/// for a store-served gauge lands when the closing bracket is put after the
/// guard instead of before it, and it fails the same way: a 400 from
/// Prometheus, a query that never returns, and a rule that is silent through
/// the fault it names. The clause scan above cannot see it, because nothing is
/// missing *within* an operand: the operator is there, the brackets balance,
/// and every series name is real.
///
/// This is not a parser, and being explicit about the ceiling is the point: a
/// well-formed-shape expression naming a series that does not exist, a wrong
/// label in a matcher, or a mistyped aggregation all pass. What it decides is
/// the edits that leave every series name intact and the expression
/// unparseable. No other check in this repository decides those, and for a rule
/// the only reading behind this one is the watcher's eval-failure self-alert,
/// which is what fires once the expression has already shipped and keeps
/// failing; a panel has nothing behind it at all.
pub fn shape_complaints(expr: &str) -> Vec<String> {
    let stripped = strip_braces(&strip_quoted(expr));
    let mut out = range_selector_operand_complaints(&stripped);

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

/// Whether a byte can be part of an identifier token.
fn token_char(b: u8) -> bool {
    let c = b as char;
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

/// How many times `word` occurs in `s` as a whole identifier token.
fn count_token(s: &str, word: &str) -> usize {
    let bytes = s.as_bytes();
    let mut n = 0;
    let mut i = 0;
    while i + word.len() <= bytes.len() {
        if s.is_char_boundary(i)
            && s[i..].starts_with(word)
            && (i == 0 || !token_char(bytes[i - 1]))
            && (i + word.len() == bytes.len() || !token_char(bytes[i + word.len()]))
        {
            n += 1;
            i += word.len();
            continue;
        }
        i += 1;
    }
    n
}

/// The `[` (or `{`) that opens the group closed at `close_at`.
fn matching_open(s: &str, close_at: usize, open: u8, close: u8) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut j = close_at;
    loop {
        let b = *bytes.get(j)?;
        if b == close {
            depth += 1;
        } else if b == open {
            depth -= 1;
            if depth == 0 {
                return Some(j);
            }
        }
        j = j.checked_sub(1)?;
    }
}

/// The series named immediately before `at`, whether what follows is the
/// `offset` keyword or a range selector.
///
/// `a offset 1h`, `a{x="y"} offset 1h` and `a[5m] offset 1h` all read `a`; so do
/// `a[5m]` and `a{x="y"}[5m]`, which is the same question with the bracket in
/// the other role. An expression whose selector is parenthesized
/// (`(a + b)[5m] offset 1h`) is not classified — this feeds a positive finding,
/// so a shape it does not classify is a weaker gate, not a wrong one.
fn series_before(s: &str, at: usize) -> Option<&str> {
    let bytes = s.as_bytes();
    let skip_ws = |mut i: usize| {
        while i > 0 && bytes[i - 1].is_ascii_whitespace() {
            i -= 1;
        }
        i
    };
    let mut end = skip_ws(at);
    if end > 0 && bytes[end - 1] == b']' {
        end = skip_ws(matching_open(s, end - 1, b'[', b']')?);
    }
    if end > 0 && bytes[end - 1] == b'}' {
        end = skip_ws(matching_open(s, end - 1, b'{', b'}')?);
    }
    let start = s[..end]
        .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
        .map(|p| p + 1)
        .unwrap_or(0);
    let name = &s[start..end];
    if name.is_empty() || !name.bytes().all(token_char) {
        return None;
    }
    Some(name)
}

/// Series names read through a lagging selector: the identifier a bare `offset`
/// applies to.
fn names_read_at_an_offset(s: &str) -> Vec<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + "offset".len() <= bytes.len() {
        if s.is_char_boundary(i)
            && s[i..].starts_with("offset")
            && (i == 0 || !token_char(bytes[i - 1]))
            && (i + "offset".len() == bytes.len() || !token_char(bytes[i + "offset".len()]))
        {
            if let Some(name) = series_before(s, i) {
                if !NOT_SERIES.contains(&name) {
                    out.push(name.to_string());
                }
            }
            i += "offset".len();
            continue;
        }
        i += 1;
    }
    out
}

/// Expressions that measure persistence by comparing a series to its own value
/// at an offset.
///
/// `A == (A offset 30m)` reads as "nothing has changed for half an hour" and is
/// not that: it is the equality of two samples. Whenever the producer is
/// periodic, two samples one period apart land on the same phase on a schedule,
/// so the expression holds on a healthy system — the deployed
/// `orphans_unreaped` rule, which ships in exactly this shape, fired 45 times
/// over three days against a reaper that was working, every firing lasting one
/// poll interval while its summary claimed half an hour. A duration is a
/// property of *every* sample in the window; that is what a range aggregate
/// reads and what a point comparison cannot.
///
/// The ceiling, stated the way the rest of this module states it: only `==` is
/// reported (comparing a series to its lagged self with `<` or `>` claims a
/// *change*, which is a different and legitimate reading), a parenthesized
/// selector is not classified, and the shape is matched rather than parsed — so
/// a rule that reads one name twice, once lagged, without comparing them is
/// reported too, and has to say in its own words why that is not this.
pub fn lagged_equality_complaints(expr: &str) -> Vec<String> {
    let stripped = strip_braces(&strip_quoted(expr));
    // `==` cannot appear inside an identifier, and quoted spans are blanked, so
    // this is the operator and not a label value.
    if !stripped.contains("==") {
        return Vec::new();
    }
    names_read_at_an_offset(&stripped)
        .into_iter()
        .filter(|name| count_token(&stripped, name) > 1)
        .map(|name| {
            format!(
                "`{name}` 与它自己 offset 之后的采样判相等：两个采样点的值相同不是「持续」。\
                 产出方只要有周期，相隔该时长的两个采样就会周期性地落到同一相位，\
                 于是这条判据在没有持续现象时也成立。要测持续就用区间聚合\
                 （`min_over_time({name}[30m]) > 0`）"
            )
        })
        .collect()
}

/// Seconds in a PromQL duration literal (`30s`, `5m`, `3h`, `2d`, `1w`).
pub fn duration_seconds(text: &str) -> Option<u64> {
    let text = text.trim();
    let digits: String = text.chars().take_while(|c| c.is_ascii_digit()).collect();
    let scale = match &text[digits.len()..] {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        _ => return None,
    };
    Some(digits.parse::<u64>().ok()? * scale)
}

/// Every range selector in `expr` that hangs off one of `gated`, as
/// `(series, window-in-seconds)`.
///
/// The question this answers is which series a rule's window actually looks
/// back over: `max_over_time(x[30m])` and `max(x)` read the same series and
/// only the first reads a window, while `x[30m]` and `y[30m]` read different
/// ones. A `[...]` whose selector is an expression rather than a name
/// (`(a + b)[30m]`) belongs to no series here and is not reported; the caller
/// of this function feeds a positive finding, so a shape it cannot attribute is
/// a gap in the gate rather than a wrong complaint.
pub fn windows_over(expr: &str, gated: &BTreeSet<String>) -> Vec<(String, u64)> {
    let stripped = strip_braces(&strip_quoted(expr));
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = stripped[from..].find('[') {
        let at = from + rel;
        from = at + 1;
        let Some(close) = stripped[at..].find(']').map(|p| at + p) else {
            break;
        };
        // A subquery carries a step after the window (`[15m:1m]`); the window is
        // what precedes the colon.
        let window = stripped[at + 1..close].split(':').next().unwrap_or("");
        let Some(seconds) = duration_seconds(window) else {
            continue;
        };
        if let Some(name) = series_before(&stripped, at) {
            if gated.contains(name) {
                out.push((name.to_string(), seconds));
            }
        }
    }
    out
}

/// The age bound a rule puts on one series' companion, or `None` when it puts
/// none.
///
/// The companion a rule guards a store-served gauge with is not itself in
/// `gated` -- it is the reading that answers the question, not a gauge the store
/// serves under that name -- so [`windows_over`] cannot find it. What a guard
/// names is a number, and that number is what has to clear the writer's own
/// cadence: an age bound no longer than the gap between two of the writer's
/// stamps is failed by a healthy writer sitting in that gap.
///
/// The alert-rule contract is the only reader: it is where a guard's bound is
/// compared to a writer's cadence, and the dashboard contract compares nothing
/// to a heartbeat. Kept here rather than there so both sides of the contract
/// read bounds through one function instead of two that drift.
///
/// The bound is read off the compacted expression, so the rule may spell the
/// arithmetic with any spacing; a `>` instead of a `<` is not an age bound but
/// the inverse reading (a rule that fires on the reading being stale), and is
/// deliberately not returned.
#[allow(dead_code)]
pub fn companion_age_bound(expr: &str, series: &str) -> Option<u64> {
    companion_age_bound_in(&strip_braces(&strip_quoted(expr)), series)
}

fn companion_age_bound_in(stripped: &str, series: &str) -> Option<u64> {
    let companion = cog_core::observability_text::observed_timestamp_name(series);
    let compact: String = stripped.chars().filter(|c| !c.is_whitespace()).collect();
    let needle = format!("time()-{companion}");
    let after = compact.find(&needle)? + needle.len();
    let rest = compact[after..]
        .strip_prefix(')')
        .unwrap_or(&compact[after..]);
    let rest = rest.strip_prefix('<')?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

/// The window of every call to `func` in `expr`, in seconds.
///
/// `func` includes its opening paren, and the window taken is the first `[...]`
/// after it — the argument's own range selector. A call with no range selector
/// contributes nothing, which is how a `min_over_time` over an instant vector is
/// left alone.
fn call_windows(expr: &str, func: &str) -> Vec<u64> {
    let bytes = expr.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = expr[from..].find(func) {
        let at = from + rel;
        from = at + func.len();
        if at > 0 && token_char(bytes[at - 1]) {
            continue;
        }
        let Some(open) = expr[from..].find('[').map(|p| from + p) else {
            continue;
        };
        let Some(close) = expr[open..].find(']').map(|p| open + p) else {
            continue;
        };
        // A subquery carries a step after the window (`[15m:1m]`); the window is
        // what precedes the colon.
        let window = expr[open + 1..close].split(':').next().unwrap_or("");
        if let Some(seconds) = duration_seconds(window) {
            out.push(seconds);
        }
    }
    out
}

/// Rules that certify a window they may not have been able to see.
///
/// `min_over_time(x[30m]) > 0` reads as "at every scrape in the last half hour",
/// and what a range aggregate actually reads is the samples that *exist* in the
/// window. A series younger than the window has fewer of them, so a pod whose
/// first scrape showed one zombie satisfied the half-hour claim thirty seconds
/// into its life: replayed against six hours of the deployment's own series, the
/// guarded form of the `orphans_unreaped` rule is silent and the unguarded one
/// fires five times, once per pod start, every firing inside the pod's first
/// minute.
///
/// The fix is a second count over a wider window, which turns the window's
/// coverage into a fact rather than an assumption:
///
/// ```text
/// min_over_time(x[30m]) > 0
///   and count_over_time(x[1h]) > count_over_time(x[30m])
/// ```
///
/// Only `min_over_time` and `avg_over_time` are reported: those are the
/// aggregates whose value is a statement about the whole window.
/// `max_over_time` and `last_over_time` answer their question from whatever
/// samples exist, so a partial window weakens the reading rather than falsifying
/// it. The check is a presence test — some `count_over_time` in the same
/// expression whose window is at least twice the certified one — because tying
/// the count to the aggregate's own argument would need that argument's text,
/// which is to say a parser.
pub fn uncovered_window_complaints(expr: &str) -> Vec<String> {
    let stripped = strip_braces(&strip_quoted(expr));
    let counts = call_windows(&stripped, "count_over_time(");
    let mut out = Vec::new();
    for func in ["min_over_time(", "avg_over_time("] {
        for window in call_windows(&stripped, func) {
            if counts.iter().any(|w| *w >= window * 2) {
                continue;
            }
            out.push(format!(
                "`{agg}` 的值是对整个窗口的判断，而窗口里有多少采样由数据决定——序列比窗口年轻时\
                 （进程刚起）这个聚合读到的是那一个采样本身。要这句话成立就得再要求窗口被铺满：\
                 `count_over_time(<同一个东西>[{wide}s]) > count_over_time(<同一个东西>[{window}s])`",
                agg = func.trim_end_matches('('),
                wide = window * 2,
            ));
        }
    }
    out
}

/// Store-served gauges a rule reads over a window without saying their writer
/// is still there.
///
/// The store hands back each series' newest row forever, so a gauge whose
/// writer stopped goes on being scraped at its last value. A window over it
/// reads that frozen sample as an observation of the window, and a rule firing
/// on it stays fired with nothing left to move the value back: no repair to
/// what the number measures can clear it, because the series the rule reads is
/// no longer connected to what it names. The companion
/// `_observed_timestamp_seconds` is the one series that moves when the writer
/// does, at a spelling the exposition and every reader share, and its value is
/// the instant the row was stored -- so what a rule needs is an age bound on it,
/// `(time() - companion) < <bound>`, in the same expression.
///
/// The obvious-looking alternative is refused. A `changes(companion[W]) > 0`
/// guard reads the same question and answers a different one: the companion is
/// rendered per scrape, so each serving pod carries its own series and a range
/// over it can only count changes since that series began. A pod younger than
/// the writer's cadence therefore reads zero for a writer that is writing
/// normally, and the guard reports the reader's uptime as the writer's absence.
/// An age is the same number in every reader, which is what makes it a statement
/// about the writer.
///
/// Two shapes read a stored value as an observation, and both are checked. A
/// range selector hung off the series reads the stored value as an observation
/// of a window. A read with no window at all is an observation too, unless it
/// is somebody's parameter: the row budget a sample log is measured against and
/// the timestamp a `time() - …` clock is built from are operands of arithmetic,
/// while a series a rule compares against a constant is the measurement itself.
/// Which one a windowless read is, mechanically, is [`bare_observation_reads`].
/// A name outside `gated` is not a gauge the store serves at all, and a family
/// whose writers stamp a heartbeat is checked against that heartbeat instead.
///
/// The ceiling, stated the way the rest of this module states ceilings: the
/// guard is matched by presence, so its bound is not compared to the value's and
/// its position is not decided here. An age bound sitting after a reduction that
/// has already merged the writers passes this check and is still the shape that
/// lets one live stream carry the gate for a stopped one; that arrangement is
/// asserted rather than assumed in the tests below, and where it was found in
/// the shipped rules it was moved inside the reduction instead.
pub fn unguarded_store_reading_complaints(expr: &str, gated: &BTreeSet<String>) -> Vec<String> {
    let stripped = strip_braces(&strip_quoted(expr));
    let mut out = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (name, window) in windows_over(expr, gated) {
        if !seen.insert(name.clone()) {
            continue;
        }
        let companion = cog_core::observability_text::observed_timestamp_name(&name);
        if guarded_by_its_companion(&stripped, &name) {
            continue;
        }
        if guard_counts_changes(&stripped, &name) {
            out.push(change_count_complaint(&name, &companion));
            continue;
        }
        out.push(format!(
            "`{name}` 是共享表服务的 gauge：写者停了它不会缺席，只会把最后一笔值永久送出去，\
             于是规则把这一刻的值读成窗口里的观测。这条规则对 `{name}` 取了 {window}s 的窗口，\
             却没有在同一表达式里要求写者还在写——`(time() - {companion}) < <界>`，界要宽过写者\
             自己的节奏。少了这一项，一个死在点火那侧的写者会让这条规则永久点亮，谁也没法熄灭它"
        ));
    }
    for name in bare_observation_reads(expr, gated) {
        if !seen.insert(name.clone()) {
            continue;
        }
        let companion = cog_core::observability_text::observed_timestamp_name(&name);
        if guarded_by_its_companion(&stripped, &name) {
            continue;
        }
        if guard_counts_changes(&stripped, &name) {
            out.push(change_count_complaint(&name, &companion));
            continue;
        }
        out.push(format!(
            "`{name}` 是共享表服务的 gauge，而这条规则把它读成自己观测的量：没有窗口，也不在\
             任何一个算术量的位置上（不是阈值、不是 `time() - …` 的那一半）。写者停了它不会\
             缺席，只会把最后一笔值永久送出去，于是判词说的一直是最后那一刻的事，而没有任何\
             东西能把那条判词改回来。要求写者还在写：`{name}` 与 `(time() - {companion}) < <界>` \
             取交（判据只认有没有，不比对界的长短）"
        ));
    }
    out
}

/// The complaint for the guard that looks like a writer check and is not one.
///
/// Split out because both reading shapes can carry it and the reason is the same
/// sentence: what the count reads is the reader, not the writer.
fn change_count_complaint(name: &str, companion: &str) -> String {
    format!(
        "`{name}` 是共享表服务的 gauge，这条规则用 `changes({companion}[<窗>]) > 0` 守它，\
         而这个计数读的不是写者。伴生钟是按抓取渲染的：每个服务 Pod 各有一条自己的序列，\
         `changes()` 只能数到**这条序列开始之后**的变化，所以一个比写者节奏还年轻的服务 Pod，\
         对一条正常在写的写者读出来也是 0——守卫把「我这个 Pod 起了多久」当成了「写者停了没有」，\
         读者在役时长被报成了写者的缺席。改成对绝对时刻取年龄：`(time() - {companion}) < <界>`，\
         界要宽过写者自己的节奏（实测：一条守六小时一轮、窗取十二小时的规则，在 30 条被服务的\
         序列里 26 条读出 0，两条在役的也在内，而库里最新一行只有 2.4 小时新）"
    )
}

/// Whether `stripped` asks after the writer of `name` in the shape that reads an
/// absolute instant.
///
/// The companion is rendered beside every series the store serves, and its value
/// is the instant the writer's newest row was stored. So `time() - companion` is
/// a statement about the writer that no reader's own uptime can move: it is the
/// same number in a pod that has been up for a second as in one that has been up
/// for a week. That is the shape this gate looks for, and the companion's name
/// being present is the whole of the test.
///
/// A `changes(companion[W]) > 0` guard is not accepted, and says why in its
/// complaint: a range over a per-scrape series can only count changes since that
/// series began, so a serving pod younger than the writer's cadence reads zero
/// for a writer that is running perfectly well. The guard then answers how long
/// the reader has been up and reports it as the writer's absence. Measured on
/// this deployment: a rule guarding a six-hourly round with a twelve-hour window
/// read zero on 26 of the 30 series the store served, both live ones included,
/// while the newest stored row was 2.4 hours old.
///
/// Matched by presence, on purpose: whether the bound a rule names is longer than
/// the writer's own cadence is a different criterion, held where the bound and
/// the cadence can be compared.
fn guarded_by_its_companion(stripped: &str, name: &str) -> bool {
    companion_age_bound_in(stripped, name).is_some()
}

/// Whether `stripped` guards `name` with a change count over its companion at
/// all, which is the shape [`guarded_by_its_companion`] refuses.
fn guard_counts_changes(stripped: &str, name: &str) -> bool {
    let companion = cog_core::observability_text::observed_timestamp_name(name);
    stripped.contains(&format!("changes({companion}["))
}

/// Store-served gauges a rule reads with no window at all and treats as the
/// thing it is judging.
///
/// A windowless read is not automatically a parameter. `x > <gauge>` and
/// `time() - <gauge>` use the number as a bound or as a clock -- the rule turns
/// on the *other* operand crossing it -- while `<gauge> > 0` reads the number
/// as the measurement, and a frozen writer leaves that judgement standing with
/// nothing left able to move it. So a read that is nobody's operand but a
/// comparison's is the same defect as a window over a frozen value, reached
/// without a window: the shape the window-only check cannot see.
///
/// Parameter position is decided by arithmetic, which is where a bound or a
/// clock is built: expanding outward from the read over the call wrappers and
/// aggregation clauses that hold it, the operator at the operand's left is one
/// of `+ - * / %` or it is not. Only the left side is consulted -- `<gauge> * 2`
/// is a measurement scaled, not a bound -- and a read whose operator the
/// expansion does not reach is reported, because this feeds a positive finding.
pub fn bare_observation_reads(expr: &str, gated: &BTreeSet<String>) -> Vec<String> {
    let stripped = strip_braces(&strip_quoted(expr));
    let bytes = stripped.as_bytes();
    let mut out: Vec<String> = Vec::new();
    for name in gated {
        let mut from = 0usize;
        while let Some(rel) = stripped[from..].find(name.as_str()) {
            let at = from + rel;
            let end = at + name.len();
            from = end;
            // Whole tokens only: `<name>_observed_timestamp_seconds` is another
            // series, and its prefix must not be read as this one.
            if at > 0 && token_char(bytes[at - 1]) {
                continue;
            }
            if end < bytes.len() && token_char(bytes[end]) {
                continue;
            }
            // A range selector hanging off the read is the windowed shape, and
            // that one is checked against the window it opens.
            let mut k = end;
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if bytes.get(k) == Some(&b'[') {
                continue;
            }
            if parameter_of_arithmetic(&stripped, at) {
                continue;
            }
            if !out.contains(name) {
                out.push(name.clone());
            }
        }
    }
    out
}

/// Whether the read starting at `at` sits in an operand of an arithmetic
/// operator.
fn parameter_of_arithmetic(s: &str, at: usize) -> bool {
    let bytes = s.as_bytes();
    let mut i = enclosing_operand_start(s, at);
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    i > 0 && matches!(bytes[i - 1], b'+' | b'-' | b'*' | b'/' | b'%')
}

/// The start of the operand the read at `at` sits in, after expanding outward
/// over the aggregations, call wrappers and clauses that hold it.
///
/// `1.05 * max without (a, b) (x)` reads `x` at the end of a chain of wrappers;
/// the operand is the whole call, and its left neighbour is the `*`. A shape
/// the expansion does not recognise stops it and leaves the read where it was,
/// which reports the read rather than passes it.
fn enclosing_operand_start(s: &str, at: usize) -> usize {
    let bytes = s.as_bytes();
    let skip_ws_left = |mut i: usize| {
        while i > 0 && bytes[i - 1].is_ascii_whitespace() {
            i -= 1;
        }
        i
    };
    let mut lo = at;
    loop {
        let i = skip_ws_left(lo);
        if i == 0 || bytes[i - 1] != b'(' {
            return lo;
        }
        // The paren before the read belongs to a call or to an aggregation's
        // clause list; either way the operand starts at the word introducing
        // it, and that word may itself be wrapped again.
        let mut k = i - 1;
        let start = loop {
            let j = skip_ws_left(k);
            if j == 0 {
                break None;
            }
            if bytes[j - 1] == b')' {
                let Some(open) = matching_open(s, j - 1, b'(', b')') else {
                    break None;
                };
                match word_before(s, open) {
                    Some((start, _)) => k = start,
                    None => break None,
                }
                continue;
            }
            break word_before(s, j).map(|(start, _)| start);
        };
        match start {
            Some(start) => lo = start,
            None => return lo,
        }
    }
}

/// The identifier ending at `end`, with its start.
fn word_before(s: &str, end: usize) -> Option<(usize, &str)> {
    let bytes = s.as_bytes();
    let mut e = end;
    while e > 0 && bytes[e - 1].is_ascii_whitespace() {
        e -= 1;
    }
    let start = s[..e]
        .rfind(|c: char| !token_char(c as u8))
        .map(|p| p + 1)
        .unwrap_or(0);
    if start >= e {
        return None;
    }
    Some((start, &s[start..e]))
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

    /// The mirror of the dropped operator: the operator is present and the
    /// bracket sits on the wrong side of the guard. The clause scan is blind to
    /// it -- every series name is real, no clause is missing one, the brackets
    /// balance -- and Prometheus rejects the whole expression, so the rule goes
    /// quiet through the fault it names.
    #[test]
    fn a_range_selector_used_as_an_operand_is_reported() {
        let broken = "(max without (pod) (min_over_time(x[6m] and \
             ((time() - x_observed_timestamp_seconds) < 600))) == 1)";
        let complaints = shape_complaints(broken);
        assert_eq!(complaints.len(), 1, "{complaints:?}");
        assert!(complaints[0].contains("`and`"), "{}", complaints[0]);

        // The fix, which is the shape the store-served-gauge guards ship: the
        // call closes before the guard, and the selector ends the operand.
        let fixed = "(max without (pod) (min_over_time(x[6m]) and \
             ((time() - x_observed_timestamp_seconds) < 600)) == 1)";
        assert!(
            shape_complaints(fixed).is_empty(),
            "{:?}",
            shape_complaints(fixed)
        );

        // The continuations PromQL does allow after a selector stay silent:
        // the bracket that closes the call, a comma, and the modifiers.
        for expr in [
            "sum(rate(a[5m]))",
            "quantile_over_time(0.9, a[5m])",
            "rate(a[5m] offset 1h)",
            "a[5m] @ end()",
        ] {
            assert!(
                shape_complaints(expr).is_empty(),
                "{expr}: {:?}",
                shape_complaints(expr)
            );
        }
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

    /// The shape both rules shipped with: a series compared to its own value at
    /// an offset, which is two samples being equal and not a duration.
    #[test]
    fn a_series_compared_to_its_lagged_self_is_reported() {
        for expr in [
            "(cogneva_process_zombies > 0) and (cogneva_process_zombies == (cogneva_process_zombies offset 30m))",
            "(cogneva_trace_tier_overdue > 0) and (cogneva_trace_tier_overdue == (cogneva_trace_tier_overdue offset 3h))",
            r#"sum(rate(a{b="c"}[5m])) == sum(rate(a{b="c"}[5m] offset 1h))"#,
        ] {
            let complaints = lagged_equality_complaints(expr);
            assert_eq!(complaints.len(), 1, "{expr}: {complaints:?}");
            assert!(complaints[0].contains("相位"), "{}", complaints[0]);
        }
    }

    /// The readings a duration claim is legitimately built from, and the
    /// neighbouring edits this has to stay silent on.
    #[test]
    fn a_window_aggregate_and_a_change_comparison_are_not_reported() {
        for expr in [
            // The fix: the whole window has to stay above zero.
            "min_over_time(cogneva_process_zombies[30m]) > 0",
            // A change, not a duration: comparing to a lagged self with an
            // inequality is a legitimate reading, so the detector stays out of it.
            "node_filesystem_avail_bytes < (node_filesystem_avail_bytes offset 1h) - 1e9",
            "rate(a[5m]) != rate(a[5m] offset 1h)",
            // An offset and an equality that never read the same series twice.
            "(a offset 30m) == 0",
            "(a > 0) and (b == (b offset 30m) and c == 1)",
            // A range selector with an offset, read once: nothing is compared to
            // its lagged self here either.
            "(a[5m] offset 1h) == 0",
        ] {
            let complaints = lagged_equality_complaints(expr);
            // `b` is the only name read twice with one of them lagged, and it is
            // not the one the equality compares.
            assert!(
                complaints.iter().all(|c| c.contains("`b`")),
                "{expr}: {complaints:?}"
            );
        }
    }

    /// A lagged read behind a range selector or a label list names the series
    /// just as plainly as a bare one, so all three are classified — the shape the
    /// detector is for does not care which selector form it arrives in.
    #[test]
    fn a_lagging_selector_is_seen_through_its_brackets_and_labels() {
        for expr in [
            r#"sum(rate(a{b="c"}[5m])) == sum(rate(a{b="c"}[5m] offset 1h))"#,
            "(a > 0) and (a == (a[5m] offset 30m))",
            "(a > 0) and (a == (a{b=\"c\"} offset 30m))",
        ] {
            let complaints = lagged_equality_complaints(expr);
            assert_eq!(complaints.len(), 1, "{expr}: {complaints:?}");
            assert!(complaints[0].contains("`a`"), "{}", complaints[0]);
        }
    }

    /// Both ceilings this detector has, asserted rather than assumed: the shape
    /// is matched, not parsed, and an unclassified selector is not guessed at.
    #[test]
    fn the_two_ceilings_are_what_they_say_they_are() {
        // Reported even though the equality is about `sum(a)` and the lagged
        // read of `a` sits on the other side of an `and`: the rule's author gets
        // the complaint and says why, which is the intended direction to fail in.
        let reported = lagged_equality_complaints("sum(a) == 1 and (a offset 5m) > 0");
        assert_eq!(reported.len(), 1, "{reported:?}");

        // Not reported: the selector is a parenthesized expression, and naming
        // what it reads would mean parsing it. The expression is not claimed to
        // be clean, only unclassified.
        assert!(
            lagged_equality_complaints("(a > 0) and (a == ((a + b)[5m] offset 30m))").is_empty()
        );
    }

    /// The aggregate that claims the whole window has to require the window to
    /// have been observed; the one that answers from whatever samples exist does
    /// not.
    #[test]
    fn an_uncovered_window_aggregate_is_reported_and_a_covered_one_is_not() {
        for expr in [
            "min_over_time(cogneva_process_zombies[30m]) > 0",
            "avg_over_time(cogneva_trace_tier_overdue[15m:1m]) > 0.5",
            // Only half a window of coverage: a count the aggregate can satisfy
            // with a window no older than the one it certifies is not coverage.
            "min_over_time(a[30m]) > 0 and count_over_time(a[30m]) > 0",
        ] {
            let complaints = uncovered_window_complaints(expr);
            assert_eq!(complaints.len(), 1, "{expr}: {complaints:?}");
            assert!(complaints[0].contains("铺满"), "{}", complaints[0]);
        }

        assert!(uncovered_window_complaints(
            "min_over_time(cogneva_process_zombies[30m]) > 0 \
                 and count_over_time(cogneva_process_zombies[1h]) \
                 > count_over_time(cogneva_process_zombies[30m])"
        )
        .is_empty());
        assert!(uncovered_window_complaints(
            "min_over_time(cogneva_trace_tier_overdue[3h]) > 0 \
                 and count_over_time(cogneva_trace_tier_overdue[6h]) \
                 > count_over_time(cogneva_trace_tier_overdue[3h])"
        )
        .is_empty());
        // Whatever samples exist answer this one, so a partial window is a
        // weaker reading rather than a false claim.
        assert!(uncovered_window_complaints("max_over_time(a[30m]) > 0").is_empty());
        // The other ceiling: the count is matched by shape, so a count over the
        // *wrong* series satisfies the check. Asserted rather than assumed, so
        // that the sentence above about parsers stays honest.
        assert!(uncovered_window_complaints(
            "min_over_time(a[30m]) > 0 and count_over_time(b[1h]) > 0"
        )
        .is_empty());
    }

    #[test]
    fn durations_are_read_in_the_units_prometheus_writes_them() {
        assert_eq!(duration_seconds("30s"), Some(30));
        assert_eq!(duration_seconds("5m"), Some(300));
        assert_eq!(duration_seconds("3h"), Some(10_800));
        assert_eq!(duration_seconds("2d"), Some(172_800));
        assert_eq!(duration_seconds("1w"), Some(604_800));
        // Not a duration this reads: an unreadable window must not become a
        // certificate that the window is covered.
        assert_eq!(duration_seconds("1y"), None);
        assert_eq!(duration_seconds(""), None);
    }

    /// A window is attributed to the series its selector names, and to no other
    /// series in the same expression.
    #[test]
    fn a_window_is_attributed_to_the_series_it_hangs_off() {
        let gated: BTreeSet<String> = ["a", "c"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            windows_over(r#"max_over_time(a{upstream=~".+"}[30m]) < 1"#, &gated),
            vec![("a".to_string(), 1_800)]
        );
        // A rule reading two gated series reports both, and `b` — gated by
        // nothing here — is not reported even though it is read.
        assert_eq!(
            windows_over("max(a[1h]) - max(c[30m]) + max(b[5m])", &gated),
            vec![("a".to_string(), 3_600), ("c".to_string(), 1_800)]
        );
        // Reading a gated series without a window is not a window over it.
        assert_eq!(windows_over("max(a) and max(b[5m])", &gated), Vec::new());
        // A subquery's step is not part of its window.
        assert_eq!(
            windows_over("max_over_time(a[15m:1m])", &gated),
            vec![("a".to_string(), 900)]
        );
        // A parenthesized selector names no series and is not attributed.
        assert!(windows_over("max_over_time((a + c)[30m])", &gated).is_empty());
    }

    /// The store-gauge guard, on the shapes the shipped rules really carry: the
    /// age bound on the companion is what separates reading a live writer's
    /// series from reading a value the store will hand back forever -- over a
    /// window, and with no window at all.
    #[test]
    fn a_store_gauge_without_its_companion_guard_is_reported() {
        let gated: BTreeSet<String> = ["metrics_samples_rows"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        // The shape the shipped rules write, with the guard inside the
        // reduction so it pairs with each writer's own series.
        assert!(unguarded_store_reading_complaints(
            "(max without (pod, container, instance) \
             (min_over_time(metrics_samples_rows[30m]) \
             and ((time() - metrics_samples_rows_observed_timestamp_seconds) < 1800))) > 0",
            &gated
        )
        .is_empty());

        // The guard dropped, which is what a new rule looks like.
        let complaints = unguarded_store_reading_complaints(
            "max without (pod, container, instance) (min_over_time(metrics_samples_rows[30m])) > 0",
            &gated,
        );
        assert_eq!(complaints.len(), 1, "{complaints:?}");
        assert!(
            complaints[0].contains("metrics_samples_rows_observed_timestamp_seconds"),
            "{}",
            complaints[0]
        );

        // The guard that looks like one and reads the reader instead: the
        // change count over a per-scrape companion is zero for a writer that is
        // running, whenever the serving pod is younger than the writer's own
        // cadence. Refused with the reason, in both reading shapes.
        for expr in [
            "max by (table) (min_over_time(metrics_samples_rows[1h]) \
             and (changes(metrics_samples_rows_observed_timestamp_seconds[1h]) > 0)) > 0",
            "max_over_time(metrics_samples_rows[30m]) > 0 \
             and changes(metrics_samples_rows_observed_timestamp_seconds[30m]) > 0",
        ] {
            let counted = unguarded_store_reading_complaints(expr, &gated);
            assert_eq!(counted.len(), 1, "{expr}: {counted:?}");
            assert!(counted[0].contains("在役时长"), "{}", counted[0]);
        }

        // A windowless read is not automatically a parameter: compared against
        // a constant, the stored value is what the rule is judging, and that is
        // the same defect with the window left out.
        let bare = unguarded_store_reading_complaints("metrics_samples_rows > 0", &gated);
        assert_eq!(bare.len(), 1, "{bare:?}");
        assert!(
            bare[0].contains("metrics_samples_rows_observed_timestamp_seconds"),
            "{}",
            bare[0]
        );
        // What a parameter looks like: the operand of a bound or of a clock.
        // The rule turns on the other side of the comparison, so a writer that
        // stopped cannot leave a judgement standing.
        assert!(unguarded_store_reading_complaints(
            "max(a_measurement) > 1.05 * max(metrics_samples_rows)",
            &gated
        )
        .is_empty());
        assert!(unguarded_store_reading_complaints(
            "(time() - max(metrics_samples_rows)) > 3600",
            &gated
        )
        .is_empty());
        // Outside the family it is not a stored gauge at all.
        assert!(unguarded_store_reading_complaints(
            "max_over_time(cogneva_process_zombies[30m]) > 0",
            &gated
        )
        .is_empty());

        // Both ceilings, asserted rather than assumed. The guard is matched by
        // presence, so one sitting after the reduction that merged the writers
        // is accepted even though a writer that is still running can carry it
        // for one that has stopped -- the arrangement this gate does not see.
        assert!(unguarded_store_reading_complaints(
            "(max by (table) (min_over_time(metrics_samples_rows[1h])) \
             and max by (table) ((time() - metrics_samples_rows_observed_timestamp_seconds) < 3600)) > 0",
            &gated
        )
        .is_empty());
        // The guard's bound is not compared to the value's: any age bound
        // satisfies the check, because whether that bound is long enough is a
        // question about the writer's cadence, which this module has no way to
        // see.
        assert!(unguarded_store_reading_complaints(
            "max_over_time(metrics_samples_rows[30m]) > 0 \
             and ((time() - metrics_samples_rows_observed_timestamp_seconds) < 30)",
            &gated
        )
        .is_empty());
        // The inverse comparison is not an age bound: a rule that fires on the
        // reading being *stale* is asking a different question, and the gate
        // does not silently accept it as the writer-liveness guard.
        assert_eq!(
            unguarded_store_reading_complaints(
                "max_over_time(metrics_samples_rows[30m]) > 0 \
                 and ((time() - metrics_samples_rows_observed_timestamp_seconds) > 3600)",
                &gated
            )
            .len(),
            1
        );
    }

    /// The bound is read off the arithmetic, however the rule spaces it, and
    /// only from the freshness side of the comparison.
    #[test]
    fn the_age_bound_is_read_from_the_companion_arithmetic() {
        let shipped = "min without (pod, container, instance) (max_over_time(x[30m]) and \
             ((time() - x_observed_timestamp_seconds) < 43200)) < 1";
        assert_eq!(companion_age_bound(shipped, "x"), Some(43_200));
        assert_eq!(
            companion_age_bound("(time()-x_observed_timestamp_seconds)<1800", "x"),
            Some(1_800)
        );
        // Not the inverse reading, not an unrelated series, not absent.
        assert_eq!(
            companion_age_bound("(time() - x_observed_timestamp_seconds) > 3600", "x"),
            None
        );
        assert_eq!(
            companion_age_bound("(time() - y_observed_timestamp_seconds) < 60", "x"),
            None
        );
        assert_eq!(companion_age_bound("max_over_time(x[30m]) > 0", "x"), None);
    }
}
