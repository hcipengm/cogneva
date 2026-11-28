//! 配置字段的消费普查：树上有没有生产代码真的把某个字段读出来。
//!
//! 配置面的另一份判据（`config_surface`）只回答「文档写的键有字段接着」。
//! 字段有、文档写了、代码从不读，值就等价于写死：运维改了它，没有任何东西跟着变。
//! 这个模块回答第三个问题——**谁读它**。
//!
//! 读取按三条线归属，任一条成立即算读到：
//!
//! * **节路径**：`self_evolution.build_gate.wait_secs` 这样一段连续的点分链出现
//!   在生产源里。直接读整份配置的代码都长这样（`ctx.config().system.x`）。
//! * **类型绑定**：字段所属的类型在某个函数作用域里有名字（参数、`let`、`impl`
//!   的 `self`、或从别的绑定推出来的类型），该作用域里出现 `.字段` 访问。这样
//!   `let supervisor = ctx.config().supervisor.clone(); supervisor.quota_threshold`
//!   与 `state.config.read().unwrap().notification_limit` 都算读到，而
//!   `client.config().base_url` 里恰好同名的 `base_url` 不会记到某个配置类型头上
//!   ——它的接收者类型在作用域里查不到。
//! * **解构**：`let SupervisorConfig { quota_threshold, .. } = ...`、
//!   `match cfg { X { f, .. } => ... }` 这类取值模式算读。构造字面量
//!   `X { f: v }` 不算：写进去不等于读出来。
//!
//! 剥注释与字符串用 `lex::scan(...).blanked`：注释里的名字、夹具字符串里拼出的
//! 名字都不是读取。生产/测试的边界用 `rust_production`，一份切口共用。
//!
//! 判不了什么：宏展开后新造出来的访问链、trait 对象上调用的方法返回配置类型
//! （返回值类型写不出来）、以及把一个配置值整个序列化出去（字段逐个出现但没人
//! 读它）。前两种会表现为「没读到」——宁可红在缺证据上，也不把同名当读到。

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::config_surface::{container_inner, ConfigField, TypeGraph, TypeShape};
use super::lex::{scan, Dialect};
use super::routes::matching_blanked;
use super::rust_production::{production_sources, production_text};

/// 一份生产源文本里对配置字段的读取证据。
#[derive(Default)]
pub struct Evidence {
    /// 命中的 `section.path`（列表标记 `[]` 已去掉）。
    section_paths: BTreeSet<String>,
    /// `(crate, type, ident)`：某个作用域里接收者类型确定、字段确定的一次访问。
    fields: BTreeSet<(String, String, String)>,
    /// 读过多少条访问链，给自证与下限用。
    chains: usize,
}

impl Evidence {
    /// 这份字段是否被这份证据读到。
    pub fn covers(&self, field: &ConfigField) -> bool {
        self.fields.contains(&(
            field.owner_crate.clone(),
            field.owner_type.clone(),
            field.ident.clone(),
        )) || self
            .section_paths
            .contains(&field.section_path.replace("[]", ""))
    }

    pub fn chains(&self) -> usize {
        self.chains
    }

    fn merge(&mut self, other: Evidence) {
        self.section_paths.extend(other.section_paths);
        self.fields.extend(other.fields);
        self.chains += other.chains;
    }
}

/// 走一遍树里的生产源，返回对 `fields` 的读取证据。
pub fn census(root: &Path, graph: &TypeGraph, fields: &[ConfigField]) -> Evidence {
    let mut evidence = Evidence::default();
    let by_path: BTreeSet<String> = fields
        .iter()
        .map(|field| field.section_path.replace("[]", ""))
        .collect();
    for path in production_sources(root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let production = production_text(&text);
        evidence.merge(scan_text(&production, graph, &path, &by_path));
    }
    evidence
}

/// 一份源文本（已切掉测试半场）的读取证据。
pub fn scan_text(
    text: &str,
    graph: &TypeGraph,
    path: &Path,
    section_paths: &BTreeSet<String>,
) -> Evidence {
    let code = scan(text, Dialect::Rust).blanked;
    let crate_name = crate_of(path);
    let mut evidence = Evidence::default();

    let chains = chains(&code);
    evidence.chains = chains.len();
    for chain in &chains {
        // 节路径：调用段把点分链断开，只看非调用段组成的连续窗口。
        let mut window: Vec<&str> = Vec::new();
        for segment in &chain.segments {
            if segment.call {
                collect_windows(&window, section_paths, &mut evidence.section_paths);
                window.clear();
                continue;
            }
            window.push(&segment.ident);
        }
        collect_windows(&window, section_paths, &mut evidence.section_paths);
    }

    let tokens = tokens(&code);
    let uses = imports(&tokens);
    let impls = impl_blocks(&tokens);
    for function in functions(&tokens, &impls) {
        let mut scope = Scope::new(graph, &crate_name, path, &uses);
        if let Some(owner) = &function.owner {
            scope.bind("self", owner.clone());
        }
        function.params(&tokens, &mut scope);
        function.locals(&tokens, &mut scope);
        function.patterns(&tokens, &mut scope);
        for chain in chains_in(&code, function.byte_start, function.byte_end) {
            scope.read_chain(&chain, &mut evidence);
        }
    }

    // 解构模式不在函数作用域里也成立：参数位置、match 臂、let 左边。
    for (crate_name, owner, ident) in destructured(&tokens, graph, &crate_name, path, &uses) {
        evidence.fields.insert((crate_name, owner, ident));
    }

    evidence
}

/// 元组绑定的快照：每个模式位置上推出来的元素类型，与初始化器的对应元素。
type TupleSnapshots<'t> = Vec<(Vec<Option<String>>, Vec<Vec<&'t Token>>)>;

fn collect_windows(window: &[&str], wanted: &BTreeSet<String>, out: &mut BTreeSet<String>) {
    for start in 0..window.len() {
        for end in start + 2..=window.len() {
            let dotted = window[start..end].join(".");
            if wanted.contains(&dotted) {
                out.insert(dotted);
            }
        }
    }
}

/// 一个文件在树里的 crate 名。
pub fn crate_of(path: &Path) -> String {
    let mut previous = String::new();
    for part in path.components() {
        let name = part.as_os_str().to_string_lossy();
        if previous == "crates" {
            return name.replace('-', "_");
        }
        previous = name.to_string();
    }
    String::new()
}

// ---------------------------------------------------------------------------
// 词法
// ---------------------------------------------------------------------------

/// 点分访问链的一段：名字与它后面有没有调用。
#[derive(Clone, Debug)]
pub struct Segment {
    pub ident: String,
    pub call: bool,
}

/// 一条从标识符开始的访问链。
#[derive(Clone, Debug)]
pub struct Chain {
    pub segments: Vec<Segment>,
}

/// 扫出文本里所有 `a.b(c).d` 形状的链。
pub fn chains(code: &str) -> Vec<Chain> {
    chains_in(code, 0, code.len())
}

fn chains_in(code: &str, start: usize, end: usize) -> Vec<Chain> {
    let bytes = code.as_bytes();
    let mut out = Vec::new();
    let mut i = start;
    while i < end {
        // `.name` 是上一条链的尾巴，已经在上一轮读掉了。
        if i > start && bytes[i] == b'.' {
            i += 1;
            continue;
        }
        let Some((_name, after)) = ident_at(bytes, i) else {
            i += 1;
            continue;
        };
        let mut segments = Vec::new();
        let mut cursor = i;
        while let Some((name, after_name)) = ident_at(bytes, cursor) {
            let mut call = false;
            let mut tail = after_name;
            loop {
                let mut t = tail;
                while t < end && (bytes[t] as char).is_whitespace() {
                    t += 1;
                }
                if t < end && bytes[t] == b'(' {
                    call = true;
                    // 热路径：`code` 已经剥过注释与字符串，直接按这一份配平。
                    match matching_blanked(code, t, b'(', b')') {
                        Some(close) => {
                            // 参数里的读取也属于生产代码：`f(ctx.config().x.y)`、
                            // `.map(|i| cfg.z)` 不能被外层链的括号一起吞掉。
                            out.extend(chains_in(code, t + 1, close));
                            tail = close + 1;
                        }
                        None => tail = end,
                    }
                    let mut u = tail;
                    while u < end && (bytes[u] as char).is_whitespace() {
                        u += 1;
                    }
                    if u < end && bytes[u] == b'?' {
                        tail = u + 1;
                        continue;
                    }
                    break;
                }
                break;
            }
            segments.push(Segment {
                ident: name.to_string(),
                call,
            });
            cursor = tail;
            let mut dot = cursor;
            while dot < end && (bytes[dot] as char).is_whitespace() {
                dot += 1;
            }
            // `crate::config::X::load()` 是一条链：类型路径也是锚点的一部分。
            if dot + 1 < end && bytes[dot] == b':' && bytes[dot + 1] == b':' {
                let mut next = dot + 2;
                while next < end && (bytes[next] as char).is_whitespace() {
                    next += 1;
                }
                if next < end && ident_at(bytes, next).is_some() {
                    cursor = next;
                    continue;
                }
            }
            if dot < end && bytes[dot] == b'.' {
                let mut next = dot + 1;
                while next < end && (bytes[next] as char).is_whitespace() {
                    next += 1;
                }
                cursor = next;
                continue;
            }
            break;
        }
        out.push(Chain { segments });
        i = cursor.max(after);
    }
    out
}

fn ident_at(bytes: &[u8], at: usize) -> Option<(&str, usize)> {
    let mut i = at;
    if i >= bytes.len() {
        return None;
    }
    let c = bytes[i] as char;
    if !(c.is_ascii_alphabetic() || c == '_') {
        return None;
    }
    i += 1;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_ascii_alphanumeric() || c == '_' {
            i += 1;
        } else {
            break;
        }
    }
    std::str::from_utf8(&bytes[at..i])
        .ok()
        .map(|name| (name, i))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Kind {
    Ident,
    Punct,
}

#[derive(Clone, Debug)]
struct Token {
    text: String,
    kind: Kind,
    start: usize,
    end: usize,
}

fn tokens(code: &str) -> Vec<Token> {
    let bytes = code.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let (name, after) = ident_at(bytes, i).expect("checked above");
            out.push(Token {
                text: name.to_string(),
                kind: Kind::Ident,
                start: i,
                end: after,
            });
            i = after;
            continue;
        }
        if c.is_ascii_digit() {
            let mut j = i + 1;
            while j < bytes.len()
                && ((bytes[j] as char).is_ascii_alphanumeric() || bytes[j] == b'_')
            {
                j += 1;
            }
            i = j;
            continue;
        }
        let two = &code[i..(i + 2).min(code.len())];
        let width = if matches!(two, "::" | "->" | "=>" | "..") {
            2
        } else {
            1
        };
        out.push(Token {
            text: code[i..i + width].to_string(),
            kind: Kind::Punct,
            start: i,
            end: i + width,
        });
        i += width;
    }
    out
}

// ---------------------------------------------------------------------------
// 作用域与类型解析
// ---------------------------------------------------------------------------

/// 调用后仍返回自身/内部值的标准方法。容器已经在前面剥掉；这里放行的是
/// `unwrap`、`expect`、`clone` 这类不改变类型的调用。
/// 容器上的方法：返回的仍是同一个容器（`Option<T>.clone()` 还是 `Option<T>`）。
fn keeps_container(name: &str) -> bool {
    matches!(
        name,
        "clone"
            | "as_ref"
            | "as_mut"
            | "borrow"
            | "borrow_mut"
            | "to_owned"
            | "as_deref"
            | "map"
            | "and_then"
            | "or"
            | "or_else"
            | "filter"
            | "take"
            | "iter"
            | "into_iter"
            | "as_slice"
    )
}

/// 容器上的方法：把内层类型取出来。
fn unwraps_container(name: &str) -> bool {
    matches!(
        name,
        "unwrap"
            | "expect"
            | "unwrap_or"
            | "unwrap_or_else"
            | "unwrap_or_default"
            | "into_inner"
            | "read"
            | "write"
            | "lock"
            | "get_mut"
    )
}

/// 值上的方法：链的类型保持不变（克隆、借出、以及返回 `Self` 的构造/加载）。
fn keeps_value(name: &str) -> bool {
    matches!(
        name,
        "clone"
            | "as_ref"
            | "as_mut"
            | "borrow"
            | "borrow_mut"
            | "to_owned"
            | "to_string"
            | "as_deref"
            | "as_slice"
            | "as_str"
            | "unwrap"
            | "expect"
            | "unwrap_or"
            | "unwrap_or_else"
            | "unwrap_or_default"
            | "into_inner"
            | "load"
            | "load_from"
            | "default"
            | "from_env"
            | "from_file"
            | "from_path"
            | "new"
    )
}

/// 类型文本去掉前导引用（含生命周期）与 `mut`。
fn trim_reference(ty: &str) -> String {
    let mut text = ty.trim();
    while let Some(rest) = text.strip_prefix('&') {
        text = rest.trim_start();
        if let Some(rest) = text.strip_prefix('\'') {
            let rest = rest.trim_start_matches(|c: char| c.is_alphanumeric() || c == '_');
            text = rest.trim_start();
        }
        text = text
            .strip_prefix("mut ")
            .map(str::trim_start)
            .unwrap_or(text);
    }
    text.to_string()
}

/// 字段类型写在声明它的文件里，`crate::` 指的是**那个** crate；离开声明文件后要补全。
fn normalize_type(ty: &str, owner_crate: &str) -> String {
    if !ty.contains("crate::") {
        return ty.to_string();
    }
    ty.replace("crate::", &format!("{owner_crate}::"))
}

/// 元组类型文本拆成元素类型。
fn tuple_types(ty: &str) -> Option<Vec<String>> {
    let bare = trim_reference(ty);
    if !bare.starts_with('(') {
        return None;
    }
    let mut depth = 0i64;
    for (at, c) in bare.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(split_top_text(&bare[1..at]));
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_text(inner: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i64;
    let mut start = 0;
    for (at, c) in inner.char_indices() {
        match c {
            '(' | '[' | '<' => depth += 1,
            ')' | ']' | '>' => depth -= 1,
            ',' if depth == 0 => {
                out.push(inner[start..at].trim().to_string());
                start = at + 1;
            }
            _ => {}
        }
    }
    out.push(inner[start..].trim().to_string());
    out
}

/// 文件里 `use` 进来的名字 → 完整路径。
///
/// 同名类型可以同时存在于两个 crate（`SupervisorConfig` 在 `cog-core` 与
/// `cog-supervisor` 各有一份），只按最后一段名字查会得到两票、只能放弃。
/// 源码里写的是哪个，`use` 行已经说清楚了。
type Uses = BTreeMap<String, String>;

fn imports(tokens: &[Token]) -> Uses {
    let mut out = BTreeMap::new();
    let mut index = 0;
    while index < tokens.len() {
        if tokens[index].text != "use" || tokens[index].kind != Kind::Ident {
            index += 1;
            continue;
        }
        let mut semi = index + 1;
        let mut depth = 0i64;
        while semi < tokens.len() {
            match tokens[semi].text.as_str() {
                "{" => depth += 1,
                "}" => depth -= 1,
                ";" if depth == 0 => break,
                _ => {}
            }
            semi += 1;
        }
        if semi >= tokens.len() {
            break;
        }
        record_use(&tokens[index + 1..semi], &mut out);
        index = semi + 1;
    }
    out
}

fn record_use(span: &[Token], out: &mut Uses) {
    if let Some(open) = span.iter().position(|token| token.text == "{") {
        let Some(close) = delimiter_close(span, open, "{", "}") else {
            return;
        };
        let prefix: String = span[..open]
            .iter()
            .map(|token| token.text.as_str())
            .collect();
        let mut item_start = open + 1;
        let mut depth = 0i64;
        for at in open + 1..=close {
            match span[at].text.as_str() {
                "<" | "(" | "[" | "{" => depth += 1,
                ">" | ")" | "]" | "}" => depth -= 1,
                "," if depth == 0 => {
                    record_simple(&span[item_start..at], &prefix, out);
                    item_start = at + 1;
                }
                _ => {}
            }
        }
        return;
    }
    record_simple(span, "", out);
}

fn record_simple(span: &[Token], prefix: &str, out: &mut Uses) {
    let as_at = span
        .iter()
        .position(|token| token.text == "as" && token.kind == Kind::Ident);
    let (path, name) = match as_at {
        Some(at) => (
            &span[..at],
            span.get(at + 1).map(|token| token.text.clone()),
        ),
        None => (
            span,
            span.iter()
                .rev()
                .find(|token| token.kind == Kind::Ident)
                .map(|token| token.text.clone()),
        ),
    };
    let Some(name) = name else {
        return;
    };
    if matches!(name.as_str(), "self" | "as") {
        return;
    }
    let mut full = prefix.to_string();
    for token in path {
        full.push_str(&token.text);
    }
    out.insert(name, full);
}

/// 把一个可能写成短名的类型解析成文件里 `use` 的全路径。
fn resolve_uses(uses: &Uses, ty: &str) -> String {
    let trimmed = ty.trim().trim_start_matches('&').trim();
    let trimmed = trimmed
        .strip_prefix("mut ")
        .map(str::trim)
        .unwrap_or(trimmed);
    if trimmed.is_empty()
        || trimmed.contains("::")
        || trimmed.contains('<')
        || trimmed.contains(' ')
    {
        return ty.to_string();
    }
    uses.get(trimmed).cloned().unwrap_or_else(|| ty.to_string())
}

/// 表达式开头的 `a::b::Type::assoc_fn(..)` / `a::b::Type` 路径 → 类型文本。
fn path_type(uses: &Uses, expr: &[&Token]) -> Option<String> {
    let first = expr.first()?;
    if first.kind != Kind::Ident {
        return None;
    }
    let mut segments = vec![first.text.clone()];
    let mut at = 1;
    while at + 1 < expr.len() && expr[at].text == "::" && expr[at + 1].kind == Kind::Ident {
        segments.push(expr[at + 1].text.clone());
        at += 2;
    }
    if segments.len() < 2 {
        return None;
    }
    let followed_by_call = expr.get(at).is_some_and(|token| token.text == "(");
    let last = segments.last()?;
    let previous_is_type = starts_upper(&segments[segments.len() - 2]);
    if (followed_by_call && !starts_upper(last)) || (!followed_by_call && previous_is_type) {
        // `Type::assoc_fn(..)` 与 `Enum::Variant`：类型是前一段。
        segments.pop();
    }
    if segments.is_empty() {
        return None;
    }
    Some(resolve_uses(uses, &segments.join("::")))
}

/// 从类型名 token 往回拼出 `a::b::Type`：解构模式里可以直接写全路径。
fn qualified_before(tokens: &[Token], at: usize) -> String {
    let mut start = at;
    while start >= 2 && tokens[start - 1].text == "::" && tokens[start - 2].kind == Kind::Ident {
        start -= 2;
    }
    tokens[start..=at]
        .iter()
        .map(|token| token.text.as_str())
        .collect()
}

/// 元组模式 `(a, b, _)` 里的名字，按位置。
fn tuple_pattern(pattern: &[Token]) -> Option<Vec<Option<String>>> {
    if pattern.first()?.text != "(" {
        return None;
    }
    let close = paren_close(pattern, 0)?;
    if close + 1 != pattern.len() {
        return None;
    }
    Some(
        split_top_slice(&pattern[1..close])
            .into_iter()
            .map(binding_name_slice)
            .collect(),
    )
}

/// 初始化器末尾的元组字面量元素。
///
/// `expr` 可能是块：`{ let config = ..; (e1, e2, ..) }`。从右往左找第一个
/// 「右括号之后只剩块收尾 `}`」的 `(`，并排除 `f(...)` 这种调用（前一个 token
/// 是标识符或右括号）。
fn tuple_elements<'t>(expr: &[&'t Token]) -> Option<Vec<Vec<&'t Token>>> {
    for open in (0..expr.len()).rev() {
        if expr[open].text != "(" {
            continue;
        }
        let Some(close) = pair_close(expr, open) else {
            continue;
        };
        if !expr[close + 1..].iter().all(|token| token.text == "}") {
            continue;
        }
        if let Some(before) = open.checked_sub(1).map(|at| expr[at]) {
            if before.kind == Kind::Ident || matches!(before.text.as_str(), ")" | "]" | "}" | ">") {
                continue;
            }
        }
        return Some(split_top_refs(&expr[open + 1..close]));
    }
    None
}

/// 括号 token 的配对位置。
///
/// 与 `routes::matching_paren` 不是同一条规则：那一份在**字符**上按字节偏移配平，
/// 这一份在已经过 `lex::scan` 的 **token 序列**上按下标配平。分词时字符串与注释
/// 已经不在流里，再走一遍字符扫描只会把这里已有的偏移又译回去。
fn pair_close(tokens: &[&Token], open: usize) -> Option<usize> {
    let mut depth = 0i64;
    for (at, token) in tokens.iter().enumerate().skip(open) {
        match token.text.as_str() {
            "(" => depth += 1,
            ")" => {
                depth -= 1;
                if depth == 0 {
                    return Some(at);
                }
            }
            _ => {}
        }
    }
    None
}

/// 从 `match`/`if let` 的模式头往后，深度 0 的第一个 `{`。
fn block_open(tokens: &[Token], from: usize) -> Option<usize> {
    let mut depth = 0i64;
    for (at, token) in tokens.iter().enumerate().skip(from) {
        match token.text.as_str() {
            "(" | "[" => depth += 1,
            ")" | "]" => depth -= 1,
            "{" if depth == 0 => return Some(at),
            "{" => depth += 1,
            "}" => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ";" if depth == 0 => return None,
            _ => {}
        }
    }
    None
}

/// `match` 体里每个臂的模式，去掉 guard，按顶层 `|` 拆成备选；返回下标区间。
fn arm_patterns(tokens: &[Token], open: usize, close: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut at = open + 1;
    while at < close {
        let mut depth = 0i64;
        let mut arrow = None;
        let mut k = at;
        while k < close {
            match tokens[k].text.as_str() {
                "(" | "[" | "{" => depth += 1,
                ")" | "]" | "}" => depth -= 1,
                "=>" if depth == 0 => {
                    arrow = Some(k);
                    break;
                }
                _ => {}
            }
            k += 1;
        }
        let Some(arrow) = arrow else {
            break;
        };
        let raw = &tokens[at..arrow];
        // guard：顶层 `if` 之前的才是模式。
        let mut depth = 0i64;
        let mut cut = raw.len();
        for (i, token) in raw.iter().enumerate() {
            match token.text.as_str() {
                "(" | "[" | "{" | "<" => depth += 1,
                ")" | "]" | "}" | ">" => depth -= 1,
                "if" if depth == 0 && token.kind == Kind::Ident => {
                    cut = i;
                    break;
                }
                _ => {}
            }
        }
        let pattern = &raw[..cut];
        let mut depth = 0i64;
        let mut start = 0;
        for (i, token) in pattern.iter().enumerate() {
            match token.text.as_str() {
                "(" | "[" | "{" | "<" => depth += 1,
                ")" | "]" | "}" | ">" => depth -= 1,
                "|" if depth == 0 => {
                    if start < i {
                        out.push((at + start, at + i));
                    }
                    start = i + 1;
                }
                _ => {}
            }
        }
        if start < pattern.len() {
            out.push((at + start, at + pattern.len()));
        }
        // 跳过臂体：块表达式可以不带逗号，其余到深度 0 的 `,` 为止。
        let starts_block = tokens.get(arrow + 1).is_some_and(|token| token.text == "{");
        let mut depth = 0i64;
        let mut k = arrow + 1;
        while k < close {
            match tokens[k].text.as_str() {
                "(" | "[" | "{" => depth += 1,
                ")" | "]" | "}" => {
                    depth -= 1;
                    if starts_block && depth == 0 {
                        k += 1;
                        break;
                    }
                }
                "," if depth == 0 => {
                    k += 1;
                    break;
                }
                _ => {}
            }
            k += 1;
        }
        at = k;
    }
    out
}

/// 反向找与 `close` 处的 `)` / `]` 配对的左括号。
fn matching_open(tokens: &[Token], close: usize) -> Option<usize> {
    let (open_text, close_text) = match tokens[close].text.as_str() {
        ")" => ("(", ")"),
        "]" => ("[", "]"),
        _ => return None,
    };
    let mut depth = 0i64;
    let mut at = close;
    loop {
        match tokens[at].text.as_str() {
            text if text == close_text => depth += 1,
            text if text == open_text => {
                depth -= 1;
                if depth == 0 {
                    return Some(at);
                }
            }
            _ => {}
        }
        if at == 0 {
            return None;
        }
        at -= 1;
    }
}

/// 方法调用接收者的起点：`a.b().c` 里 `.c` 之前的 `a`。
fn receiver_start(tokens: &[Token], dot: usize) -> Option<usize> {
    let mut at = dot.checked_sub(1)?;
    loop {
        match tokens[at].text.as_str() {
            ")" | "]" => {
                let open = matching_open(tokens, at)?;
                let callee = open.checked_sub(1)?;
                if tokens[callee].kind != Kind::Ident {
                    return None;
                }
                at = callee;
            }
            _ if tokens[at].kind == Kind::Ident => {}
            _ => return None,
        }
        if at >= 2 && tokens[at - 1].text == "." {
            at -= 2;
            continue;
        }
        if at >= 2 && tokens[at - 1].text == "::" {
            at -= 2;
            continue;
        }
        return Some(at);
    }
}

fn binding_name_slice(item: &[Token]) -> Option<String> {
    item.iter()
        .find(|token| {
            token.kind == Kind::Ident && !matches!(token.text.as_str(), "mut" | "ref" | "_")
        })
        .map(|token| token.text.clone())
}

fn split_top_slice(tokens: &[Token]) -> Vec<&[Token]> {
    split_commas(tokens.len(), |at| tokens[at].text.as_str())
        .into_iter()
        .map(|(start, end)| &tokens[start..end])
        .collect()
}

fn split_top_refs<'t>(tokens: &[&'t Token]) -> Vec<Vec<&'t Token>> {
    split_commas(tokens.len(), |at| tokens[at].text.as_str())
        .into_iter()
        .map(|(start, end)| tokens[start..end].to_vec())
        .collect()
}

/// 按深度 0 的逗号切分，返回每段的下标区间。
fn split_commas<'a>(len: usize, text: impl Fn(usize) -> &'a str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut depth = 0i64;
    let mut start = 0;
    for at in 0..len {
        match text(at) {
            "{" | "(" | "[" => depth += 1,
            "}" | ")" | "]" => depth -= 1,
            "," if depth == 0 => {
                out.push((start, at));
                start = at + 1;
            }
            _ => {}
        }
    }
    out.push((start, len));
    out
}

/// 一个函数作用域里能被读到的绑定。
struct Scope<'a> {
    graph: &'a TypeGraph,
    crate_name: &'a str,
    path: &'a Path,
    uses: &'a Uses,
    /// 名字 → 它可能是的类型文本（同名变量在不同分支里可能不同）。
    bindings: BTreeMap<String, BTreeSet<String>>,
}

impl<'a> Scope<'a> {
    fn new(graph: &'a TypeGraph, crate_name: &'a str, path: &'a Path, uses: &'a Uses) -> Self {
        Self {
            graph,
            crate_name,
            path,
            uses,
            bindings: BTreeMap::new(),
        }
    }

    fn bind(&mut self, name: &str, ty: String) {
        self.bindings
            .entry(name.to_string())
            .or_default()
            .insert(ty);
    }

    fn types_of(&self, name: &str) -> BTreeSet<String> {
        self.bindings.get(name).cloned().unwrap_or_default()
    }

    /// 解析一条访问链：沿途的字段访问记成读取，返回链末端可能的类型。
    fn read_chain(&mut self, chain: &Chain, evidence: &mut Evidence) -> Option<String> {
        if chain.segments.len() < 2 {
            return None;
        }
        let first = &chain.segments[0];
        // (根类型, 从第几段开始走)。`x.config()` 的 `config` 是方法，
        // 不在 Config 上，所以补出来的根从调用之后的字段开始。
        let mut roots: Vec<(String, usize)> = if !first.call {
            self.types_of(&first.ident)
                .into_iter()
                .map(|ty| (ty, 1))
                .collect()
        } else {
            Vec::new()
        };
        // `ctx.config()` / `state.config()` 这类根上，`config` 是方法而不是字段：
        // 无论接收者类型认不认得出来，都把进程配置整体补成一个可能的根。
        if chain.segments.len() >= 2
            && chain.segments[1].ident == "config"
            && chain.segments[1].call
        {
            roots.push(("cog_core::Config".to_string(), 2));
        }
        // 自由函数 `config()` 直接返回整份配置。
        if first.call && first.ident == "config" && chain.segments.len() >= 2 {
            roots.push(("cog_core::Config".to_string(), 1));
        }
        // `crate::X::load()?.field` / `X::assoc()?.field`：类型路径本身就是根，
        // 路径以大写类型名收尾时，从它后面的方法段开始走。
        let path_len = chain
            .segments
            .iter()
            .take_while(|segment| !segment.call)
            .count();
        if path_len > 0 && starts_upper(&chain.segments[path_len - 1].ident) {
            let joined = chain.segments[..path_len]
                .iter()
                .map(|segment| segment.ident.as_str())
                .collect::<Vec<_>>()
                .join("::");
            roots.push((resolve_uses(self.uses, &joined), path_len));
        }
        if roots.is_empty() {
            return None;
        }
        roots.sort();
        roots.dedup();
        let mut last = None;
        for (root, from) in roots {
            let (reads, end) = self.walk(&root, &chain.segments[from..]);
            for (crate_name, owner, ident) in reads {
                evidence.fields.insert((crate_name, owner, ident));
            }
            if end.is_some() {
                last = end;
            }
        }
        last
    }

    fn walk(
        &self,
        start: &str,
        segments: &[Segment],
    ) -> (Vec<(String, String, String)>, Option<String>) {
        let mut reads = Vec::new();
        let mut current = Some(start.to_string());
        for segment in segments {
            let Some(ty) = current.take() else {
                break;
            };
            let (next, found) = self.step(&ty, segment);
            reads.extend(found);
            current = next;
        }
        (reads, current)
    }

    fn step(&self, ty: &str, segment: &Segment) -> (Option<String>, Vec<(String, String, String)>) {
        let mut reads = Vec::new();
        let bare = trim_reference(ty);

        if segment.call {
            // 方法调用的结果类型：容器上的方法要么保留容器、要么取出内层；
            // 值上的方法只认「原样返回」的那一组，其余一律不再推断。
            if container_inner(&bare).is_some() {
                if keeps_container(&segment.ident) {
                    return (Some(bare), reads);
                }
                if unwraps_container(&segment.ident) {
                    return (container_inner(&bare), reads);
                }
                return (None, reads);
            }
            let resolved = resolve_uses(self.uses, &bare);
            if self
                .graph
                .shape_named(&resolved, self.crate_name, Some(self.path))
                .is_some()
            {
                return if keeps_value(&segment.ident) {
                    (Some(bare), reads)
                } else {
                    (None, reads)
                };
            }
            return (None, reads);
        }

        // 字段访问：一路剥掉容器壳，直到能解析出声明。
        let mut current = bare;
        loop {
            if let Some(inner) = container_inner(&current) {
                current = inner;
                continue;
            }
            let resolved = resolve_uses(self.uses, &current);
            let Some(shape) = self
                .graph
                .shape_named(&resolved, self.crate_name, Some(self.path))
            else {
                return (None, reads);
            };
            let Some(field) = shape.field(&segment.ident) else {
                return (None, reads);
            };
            reads.push((
                shape.crate_name.clone(),
                shape.name.clone(),
                field.ident.clone(),
            ));
            return (Some(self.qualify(&field.ty, &shape.crate_name)), reads);
        }
    }

    /// 字段类型离开声明文件后补全 `crate::`，并给会撞车的裸名补上声明它的 crate。
    fn qualify(&self, ty: &str, owner_crate: &str) -> String {
        let text = normalize_type(ty, owner_crate);
        let bare = text.trim();
        if let Some(open) = bare.find('<') {
            if bare.ends_with('>') {
                let head = &bare[..open];
                let inner = &bare[open + 1..bare.len() - 1];
                let pieces = split_top_text(inner)
                    .into_iter()
                    .map(|piece| self.qualify(&piece, owner_crate))
                    .collect::<Vec<_>>()
                    .join(", ");
                return format!("{head}<{pieces}>");
            }
        }
        if bare.starts_with('(') && bare.ends_with(')') {
            let pieces = split_top_text(&bare[1..bare.len() - 1])
                .into_iter()
                .map(|piece| self.qualify(&piece, owner_crate))
                .collect::<Vec<_>>()
                .join(", ");
            return format!("({pieces})");
        }
        if !bare.contains("::")
            && !bare.contains(' ')
            && bare.chars().next().is_some_and(|c| c.is_uppercase())
            && self.graph.ambiguous_name(bare)
            && self.graph.declared_in(owner_crate, bare)
        {
            return format!("{owner_crate}::{bare}");
        }
        text
    }

    /// 把一个模式里的名字按已知的被匹配类型绑定。
    ///
    /// `Some(config)` / `Ok(cfg)` 剥一层容器；元组按位置拆；裸名字就是这个类型。
    fn bind_pattern(&mut self, pattern: &[Token], ty: &str) {
        let mut p = pattern;
        while let Some(first) = p.first() {
            match first.text.as_str() {
                "&" | "mut" | "ref" => p = &p[1..],
                _ => break,
            }
        }
        let Some(first) = p.first() else {
            return;
        };
        if p.len() >= 2 && p[1].text == "(" && matches!(first.text.as_str(), "Some" | "Ok") {
            if let Some(close) = paren_close(p, 1) {
                if let Some(inner) = container_inner(ty) {
                    self.bind_pattern(&p[2..close], &inner);
                }
            }
            return;
        }
        if first.text == "(" {
            if let Some(close) = paren_close(p, 0) {
                let elements = split_top_slice(&p[1..close]);
                let tys =
                    tuple_types(ty).or_else(|| container_inner(ty).and_then(|t| tuple_types(&t)));
                if let Some(tys) = tys {
                    for (element, element_ty) in elements.iter().zip(tys.iter()) {
                        self.bind_pattern(element, element_ty);
                    }
                }
            }
            return;
        }
        if p.len() == 1
            && first.kind == Kind::Ident
            && first.text != "_"
            && !starts_upper(&first.text)
        {
            let bare = trim_reference(ty);
            self.bind(&first.text, bare);
        }
    }
}

// ---------------------------------------------------------------------------
// 函数与 impl 的边界
// ---------------------------------------------------------------------------

struct ImplBlock {
    start: usize,
    end: usize,
    owner: Option<String>,
}

fn impl_blocks(tokens: &[Token]) -> Vec<ImplBlock> {
    let mut out = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token.text != "impl" || token.kind != Kind::Ident {
            continue;
        }
        let Some(open) = tokens[index..]
            .iter()
            .position(|token| token.text == "{" && token.kind == Kind::Punct)
            .map(|offset| index + offset)
        else {
            continue;
        };
        let span = &tokens[index + 1..open];
        // `impl<T> Foo<T>`：先跳过 impl 自己的泛型参数声明，否则 `T` 会被当成 owner。
        let span = if span.first().is_some_and(|token| token.text == "<") {
            let mut depth = 0i64;
            let mut after = span.len();
            for (position, token) in span.iter().enumerate() {
                match token.text.as_str() {
                    "<" => depth += 1,
                    ">" => {
                        depth -= 1;
                        if depth == 0 {
                            after = position + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            &span[after..]
        } else {
            span
        };
        let after_for = span
            .iter()
            .rposition(|token| token.text == "for")
            .map(|position| &span[position + 1..])
            .unwrap_or(span);
        let owner = after_for
            .iter()
            .find(|token| token.kind == Kind::Ident && starts_upper(&token.text))
            .map(|token| token.text.clone());
        let Some(end) = brace_close(tokens, open) else {
            continue;
        };
        out.push(ImplBlock {
            start: token.start,
            end: tokens[end].end,
            owner,
        });
    }
    out
}

struct Function {
    owner: Option<String>,
    params_start: usize,
    params_end: usize,
    /// 函数体在 token 序列里的范围（闭区间左端、开区间右端是收尾的 `}`）。
    body_start: usize,
    body_end: usize,
    /// 函数体在源文本字节上的范围，给访问链扫描用。
    byte_start: usize,
    byte_end: usize,
}

impl Function {
    fn params(&self, tokens: &[Token], scope: &mut Scope) {
        let mut current: Vec<&Token> = Vec::new();
        let mut depth = 0i64;
        let mut out: Vec<Vec<&Token>> = Vec::new();
        for token in &tokens[self.params_start..self.params_end] {
            match token.text.as_str() {
                "<" | "(" | "[" => depth += 1,
                ">" | ")" | "]" => depth -= 1,
                "," if depth == 0 => {
                    out.push(std::mem::take(&mut current));
                    continue;
                }
                _ => {}
            }
            current.push(token);
        }
        if !current.is_empty() {
            out.push(current);
        }
        for param in out {
            if param.iter().any(|token| token.text == "self") {
                continue;
            }
            let Some(colon) = param
                .iter()
                .position(|token| token.text == ":" && token.kind == Kind::Punct)
            else {
                continue;
            };
            let Some(name) = param[..colon]
                .iter()
                .rev()
                .find(|token| {
                    token.kind == Kind::Ident && token.text != "mut" && token.text != "ref"
                })
                .map(|token| token.text.clone())
            else {
                continue;
            };
            let ty = join(&param[colon + 1..]);
            scope.bind(&name, ty);
        }
    }

    fn locals(&self, tokens: &[Token], scope: &mut Scope) {
        let body = &tokens[self.body_start..self.body_end];
        // 元组快照：`let (a, b, ..) = { let config = ctx.config(); (..., config.agent.clone(), ..) };`
        // 元素类型要等块里的 `let config` 绑定完才解析得出来，所以先记下来最后再绑。
        let mut tuples: TupleSnapshots<'_> = Vec::new();
        let mut index = 0;
        while index < body.len() {
            if body[index].text == "let"
                && body[index].kind == Kind::Ident
                // `if let` / `while let` 是表达式，没有语句结尾的 `;`，按 let 语句扫描会
                // 吞掉后面的代码；它们也不绑定配置快照。
                && !matches!(
                    index.checked_sub(1).map(|at| body[at].text.as_str()),
                    Some("if") | Some("while")
                )
            {
                // 语句结尾是深度 0 的 `;`：块初始化器里的 `let config = ...;`
                // 不能把外层语句截断。
                let mut semi = index + 1;
                let mut depth = 0i64;
                while semi < body.len() {
                    match body[semi].text.as_str() {
                        "{" | "(" | "[" => depth += 1,
                        "}" | ")" | "]" => depth -= 1,
                        ";" if depth == 0 => break,
                        _ => {}
                    }
                    semi += 1;
                }
                if semi >= body.len() {
                    // 找不到语句结尾：不是一个完整的 let，跳过这个词而不是放弃整个函数。
                    index += 1;
                    continue;
                }
                let statement = &body[index + 1..semi];
                let eq = statement
                    .iter()
                    .position(|token| token.text == "=" && token.kind == Kind::Punct);
                let (pattern, expr) = match eq {
                    Some(position) => (&statement[..position], Some(&statement[position + 1..])),
                    None => (statement, None),
                };
                if let Some(open) = pattern
                    .iter()
                    .position(|token| token.text == "{" && token.kind == Kind::Punct)
                {
                    if let Some(close) = brace_close(body, index + 1 + open) {
                        if let Some(type_at) = pattern[..open]
                            .iter()
                            .rposition(|token| token.kind == Kind::Ident)
                        {
                            let owner =
                                resolve_uses(scope.uses, &qualified_before(pattern, type_at));
                            if let Some(shape) =
                                scope
                                    .graph
                                    .shape_named(&owner, scope.crate_name, Some(scope.path))
                            {
                                for token in &body[index + 2 + open..close] {
                                    if token.kind == Kind::Ident {
                                        if let Some(field) = shape.field(&token.text) {
                                            scope.bind(&token.text, field.ty.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if let (Some(names), Some(expr)) = (tuple_pattern(pattern), expr) {
                    let refs: Vec<&Token> = expr.iter().collect();
                    if let Some(elements) = tuple_elements(&refs) {
                        tuples.push((names, elements));
                        index += 1;
                        continue;
                    }
                }
                let mut cursor = 0;
                while cursor < pattern.len()
                    && matches!(pattern[cursor].text.as_str(), "mut" | "ref")
                {
                    cursor += 1;
                }
                if cursor >= pattern.len() {
                    index += 1;
                    continue;
                }
                let name = &pattern[cursor];
                if name.kind != Kind::Ident
                    || matches!(name.text.as_str(), "Some" | "Ok" | "Err" | "None")
                {
                    index += 1;
                    continue;
                }
                if cursor + 1 < pattern.len() && pattern[cursor + 1].text == ":" {
                    let ty = join_slice(&pattern[cursor + 2..]);
                    scope.bind(&name.text, ty);
                } else if cursor + 1 == pattern.len() {
                    if let Some(expr) = expr {
                        let refs: Vec<&Token> = expr.iter().collect();
                        if let Some(ty) = scope.expr_type(&refs) {
                            scope.bind(&name.text, ty);
                        }
                    }
                }
                index += 1;
                continue;
            }
            if body[index].text == "for" && body[index].kind == Kind::Ident {
                if let Some(in_at) = body[index..]
                    .iter()
                    .position(|token| token.text == "in")
                    .map(|offset| index + offset)
                {
                    let Some(name) = body.get(index + 1) else {
                        break;
                    };
                    let expr: Vec<&Token> = body[in_at + 1..]
                        .iter()
                        .take_while(|token| token.text != "{" && token.text != ";")
                        .collect();
                    if name.kind == Kind::Ident && !expr.is_empty() {
                        if let Some(ty) = scope.expr_type(&expr) {
                            if let Some(inner) = container_inner(&ty) {
                                scope.bind(&name.text, inner);
                            }
                        }
                    }
                    index = in_at + 1;
                    continue;
                }
            }
            index += 1;
        }
        for (names, elements) in tuples {
            if names.len() != elements.len() {
                continue;
            }
            for (name, element) in names.into_iter().zip(elements) {
                let Some(name) = name else { continue };
                if name == "_" {
                    continue;
                }
                if let Some(ty) = scope.expr_type(&element) {
                    scope.bind(&name, ty);
                }
            }
        }
    }

    /// 模式绑定：`match` 臂、`if let` / `while let`、闭包参数。
    ///
    /// 这些都是「读」：`Some(config) => config.rules` 读的是 `Option<BoundaryConfig>`
    /// 里的 `rules`，不因为中间隔了一个模式就消失。
    fn patterns(&self, tokens: &[Token], scope: &mut Scope) {
        let body = &tokens[self.body_start..self.body_end];
        let mut index = 0;
        while index < body.len() {
            let token = &body[index];
            if token.kind == Kind::Ident && token.text == "match" {
                if let Some(open) = block_open(body, index + 1) {
                    if let Some(close) = brace_close(body, open) {
                        let scrutinee: Vec<&Token> = body[index + 1..open].iter().collect();
                        if let Some(ty) = scope.expr_type(&scrutinee) {
                            for (start, end) in arm_patterns(body, open, close) {
                                scope.bind_pattern(&body[start..end], &ty);
                            }
                        }
                        index = open;
                    }
                }
            } else if token.kind == Kind::Ident
                && matches!(token.text.as_str(), "if" | "while")
                && body.get(index + 1).is_some_and(|t| t.text == "let")
            {
                if let Some(open) = block_open(body, index + 2) {
                    let mut depth = 0i64;
                    let mut eq = None;
                    for (at, token) in body.iter().enumerate().take(open).skip(index + 2) {
                        match token.text.as_str() {
                            "(" | "[" | "{" => depth += 1,
                            ")" | "]" | "}" => depth -= 1,
                            "=" if depth == 0 => {
                                eq = Some(at);
                                break;
                            }
                            _ => {}
                        }
                    }
                    if let Some(eq) = eq {
                        let expr: Vec<&Token> = body[eq + 1..open].iter().collect();
                        if let Some(ty) = scope.expr_type(&expr) {
                            scope.bind_pattern(&body[index + 2..eq], &ty);
                        }
                    }
                    index = open;
                }
            } else if token.text == "|"
                && index >= 3
                && body[index - 1].text == "("
                && body[index - 2].kind == Kind::Ident
                && body[index - 3].text == "."
            {
                // `.filter(|c| ..)` / `.map(|(c, _)| ..)`：参数是被调容器的元素。
                let mut close_bar = index + 1;
                while close_bar < body.len() && body[close_bar].text != "|" {
                    close_bar += 1;
                }
                let dot = index - 3;
                if close_bar < body.len() {
                    if let Some(start) = receiver_start(body, dot) {
                        let receiver: Vec<&Token> = body[start..dot].iter().collect();
                        if let Some(ty) = scope.expr_type(&receiver) {
                            let element =
                                container_inner(&ty).unwrap_or_else(|| trim_reference(&ty));
                            let params = &body[index + 1..close_bar];
                            let items = split_top_slice(params);
                            if items.len() == 1 {
                                scope.bind_pattern(items[0], &element);
                            } else if let Some(tys) = tuple_types(&element) {
                                for (item, item_ty) in items.iter().zip(tys.iter()) {
                                    scope.bind_pattern(item, item_ty);
                                }
                            }
                            index = close_bar;
                        }
                    }
                }
            }
            index += 1;
        }
    }
}

fn functions(tokens: &[Token], impls: &[ImplBlock]) -> Vec<Function> {
    let mut out = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token.text != "fn" || token.kind != Kind::Ident {
            continue;
        }
        let Some(open) = tokens[index..]
            .iter()
            .position(|candidate| candidate.text == "(" && candidate.kind == Kind::Punct)
            .map(|offset| index + offset)
        else {
            continue;
        };
        let Some(close) = paren_close(tokens, open) else {
            continue;
        };
        // 先遇到 `;` 的是 trait 方法声明，没有函数体。
        let mut body_open = None;
        for (offset, candidate) in tokens[close + 1..].iter().enumerate() {
            if candidate.text == ";" && candidate.kind == Kind::Punct {
                break;
            }
            if candidate.text == "{" && candidate.kind == Kind::Punct {
                body_open = Some(close + 1 + offset);
                break;
            }
        }
        let Some(body_open) = body_open else {
            continue;
        };
        let Some(body_close) = brace_close(tokens, body_open) else {
            continue;
        };
        let owner = impls
            .iter()
            .filter(|block| block.start <= token.start && token.start <= block.end)
            .min_by_key(|block| block.end - block.start)
            .and_then(|block| block.owner.clone());
        out.push(Function {
            owner,
            params_start: open + 1,
            params_end: close,
            body_start: body_open + 1,
            body_end: body_close,
            byte_start: tokens[body_open].end,
            byte_end: tokens[body_close].start,
        });
    }
    out
}

fn brace_close(tokens: &[Token], open: usize) -> Option<usize> {
    delimiter_close(tokens, open, "{", "}")
}

fn paren_close(tokens: &[Token], open: usize) -> Option<usize> {
    delimiter_close(tokens, open, "(", ")")
}

fn delimiter_close(tokens: &[Token], open: usize, left: &str, right: &str) -> Option<usize> {
    let mut depth = 0i64;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        if token.text == left {
            depth += 1;
        } else if token.text == right {
            depth -= 1;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

fn join(tokens: &[&Token]) -> String {
    tokens.iter().map(|token| token.text.as_str()).collect()
}

fn join_slice(tokens: &[Token]) -> String {
    tokens.iter().map(|token| token.text.as_str()).collect()
}

fn starts_upper(text: &str) -> bool {
    text.chars().next().is_some_and(|c| c.is_uppercase())
}

impl<'a> Scope<'a> {
    fn expr_type(&mut self, expr: &[&Token]) -> Option<String> {
        if let Some(path) = path_type(self.uses, expr) {
            return Some(path);
        }
        // 裸变量：它自己的绑定就是表达式的类型（`match cfg { Some(c) => .. }`）。
        if let [only] = expr {
            if only.kind == Kind::Ident {
                return self.types_of(&only.text).into_iter().next();
            }
        }
        // 链解析会先吐出调用参数里的链（`.or_else(|| x.f())` 的 `x.f()`）。
        // 表达式自身的类型是以表达式第一个标识符为根、且最长的那条链。
        let first = expr
            .iter()
            .find(|token| token.kind == Kind::Ident)
            .map(|token| token.text.clone())?;
        let text = join(expr);
        let mut rooted: Vec<Chain> = chains(&text)
            .into_iter()
            .filter(|chain| chain.segments.first().is_some_and(|s| s.ident == first))
            .collect();
        rooted.sort_by_key(|chain| std::cmp::Reverse(chain.segments.len()));
        for chain in rooted {
            if let Some(ty) = self.read_chain(&chain, &mut Evidence::default()) {
                return Some(ty);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// 解构
// ---------------------------------------------------------------------------

/// 取值模式里的字段：`T { f, .. }` 出现在 `let` 左边、match 臂或参数位置。
fn destructured(
    tokens: &[Token],
    graph: &TypeGraph,
    crate_name: &str,
    path: &Path,
    uses: &Uses,
) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token.kind != Kind::Ident || !starts_upper(&token.text) {
            continue;
        }
        let adjacent = tokens.get(index + 1);
        if !adjacent.is_some_and(|token| token.text == "{" && token.kind == Kind::Punct) {
            continue;
        }
        let open = index + 1;
        let Some(close) = brace_close(tokens, open) else {
            continue;
        };
        if !is_pattern(tokens, index, close) {
            continue;
        }
        let owner = resolve_uses(uses, &qualified_before(tokens, index));
        let Some(shape) = graph.shape_named(&owner, crate_name, Some(path)) else {
            continue;
        };
        let mut depth = 0i64;
        let mut field_name: Option<String> = None;
        for candidate in &tokens[open + 1..close] {
            match candidate.text.as_str() {
                "<" | "(" | "[" | "{" => depth += 1,
                ">" | ")" | "]" | "}" => depth -= 1,
                "," if depth == 0 => {
                    push_pattern_field(&mut out, &shape, field_name.take());
                    continue;
                }
                _ => {}
            }
            if candidate.kind == Kind::Ident
                && depth == 0
                && field_name.is_none()
                && !matches!(candidate.text.as_str(), "ref" | "mut")
            {
                field_name = Some(candidate.text.clone());
            }
        }
        push_pattern_field(&mut out, &shape, field_name);
    }
    out
}

fn push_pattern_field(
    out: &mut Vec<(String, String, String)>,
    shape: &TypeShape,
    name: Option<String>,
) {
    let Some(name) = name else {
        return;
    };
    if shape.field(&name).is_some() {
        out.push((shape.crate_name.clone(), shape.name.clone(), name));
    }
}

/// `T { ... }` 是不是取值模式而不是构造字面量。
fn is_pattern(tokens: &[Token], type_at: usize, close: usize) -> bool {
    // 右花括号之后是 `=` 或 `=>`：`let T { .. } =`、match 臂。
    if tokens
        .get(close + 1)
        .is_some_and(|token| token.text == "=" || token.text == "=>")
    {
        return true;
    }
    // 参数位置（前一个是 `(` 或 `,` 或 `|`），或 `for T { .. } in`。
    let before = tokens[..type_at]
        .iter()
        .rev()
        .find(|token| !matches!(token.text.as_str(), "&" | "mut" | "ref"));
    matches!(
        before.map(|token| token.text.as_str()),
        Some("(") | Some(",") | Some("|") | Some("for")
    )
}

// ---------------------------------------------------------------------------
// 自证
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::root::repo_root;
    use std::path::Path;

    fn graph() -> TypeGraph {
        TypeGraph::new(&repo_root())
    }

    fn field(section: &str, ident: &str, owner: &str) -> ConfigField {
        ConfigField {
            section_path: section.to_string(),
            ident: ident.to_string(),
            owner_type: owner.to_string(),
            owner_crate: "cog_core".to_string(),
        }
    }

    #[test]
    fn a_match_arm_and_an_if_let_binding_count() {
        let graph = graph();
        let fields = [field(
            "supervisor.quota_threshold",
            "quota_threshold",
            "SupervisorConfig",
        )];
        let paths: BTreeSet<String> = BTreeSet::new();
        let matched = "fn f(cfg: &Option<cog_core::config::SupervisorConfig>) {\n\
                       let q = match cfg { Some(c) => c.quota_threshold, None => 0 };\n\
                       drop(q);\n}\n";
        let evidence = scan_text(matched, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            evidence.covers(&fields[0]),
            "a read through a match-arm binding was not attributed"
        );

        let if_let = "fn f(cfg: &Option<cog_core::config::SupervisorConfig>) {\n\
                      if let Some(c) = cfg { if c.quota_threshold > 1 {} }\n}\n";
        let evidence = scan_text(if_let, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            evidence.covers(&fields[0]),
            "a read through an if-let binding was not attributed"
        );
    }

    #[test]
    fn a_closure_parameter_takes_the_element_type_of_its_receiver() {
        let graph = graph();
        let fields = [field(
            "supervisor.quota_threshold",
            "quota_threshold",
            "SupervisorConfig",
        )];
        let paths: BTreeSet<String> = BTreeSet::new();
        let text = "fn f(items: &[cog_core::config::SupervisorConfig]) {\n\
                    let n = items.iter().filter(|c| c.quota_threshold > 1).count();\n\
                    drop(n);\n}\n";
        let evidence = scan_text(text, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            evidence.covers(&fields[0]),
            "a read through a closure parameter was not attributed"
        );
    }

    #[test]
    fn an_association_path_is_anchored_by_its_type() {
        let graph = graph();
        let fields = [ConfigField {
            section_path: "tuning.stream_capacity".to_string(),
            ident: "stream_capacity".to_string(),
            owner_type: "TuningConfig".to_string(),
            owner_crate: "cog_llm".to_string(),
        }];
        let paths: BTreeSet<String> = BTreeSet::new();
        let text = "fn f() -> usize { cog_llm::TuningConfig::load().unwrap().stream_capacity }\n";
        let evidence = scan_text(text, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            evidence.covers(&fields[0]),
            "a read anchored by `Type::load()?.field` was not attributed"
        );
    }

    #[test]
    fn a_field_type_keeps_the_crate_that_declares_it() {
        let graph = graph();
        let fields = [field(
            "dag_executor.decomposition_orphan_watch_enabled",
            "decomposition_orphan_watch_enabled",
            "DagExecutorConfig",
        )];
        let paths: BTreeSet<String> = BTreeSet::new();
        // `DagExecutorConfig` 在 cog-core 与 cog-orchestrator 各有一份；从
        // `cog_core::Config` 的字段走过来的那一份是 cog-core 的。
        let text = "fn f(c: &cog_core::Config) {\n\
                    let on = c.dag_executor.decomposition_orphan_watch_enabled;\n\
                    drop(on);\n}\n";
        let evidence = scan_text(text, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            evidence.covers(&fields[0]),
            "a field read was resolved against the wrong same-named type"
        );
    }

    #[test]
    fn a_section_path_counts_and_a_string_or_comment_does_not() {
        let graph = graph();
        let paths: BTreeSet<String> = ["system.tool_timeout_secs".to_string()].into();
        let text = "fn f(c: &cog_core::Config) { let x = c.system.tool_timeout_secs; }\n";
        let evidence = scan_text(text, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(evidence.section_paths.contains("system.tool_timeout_secs"));

        let prose = "// c.system.tool_timeout_secs is prose\n\
                     fn g() { let s = \"c.system.tool_timeout_secs\"; }\n";
        let evidence = scan_text(prose, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            !evidence.section_paths.contains("system.tool_timeout_secs"),
            "a comment or a string was read as a field access"
        );
    }

    #[test]
    fn a_typed_local_attributes_the_read_but_a_stranger_does_not() {
        let graph = graph();
        let fields = [field(
            "supervisor.quota_threshold",
            "quota_threshold",
            "SupervisorConfig",
        )];
        let paths: BTreeSet<String> = BTreeSet::new();
        let text =
            "fn f(cfg: &cog_core::config::SupervisorConfig) { if cfg.quota_threshold > 1 {} }\n";
        let evidence = scan_text(text, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(evidence.covers(&fields[0]), "typed parameter was not read");

        let stranger = "fn g(client: &HttpClient) { if client.quota_threshold > 1 {} }\n";
        let evidence = scan_text(stranger, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            !evidence.covers(&fields[0]),
            "a same-named field on a receiver of unknown type was credited"
        );
    }

    #[test]
    fn a_destructuring_pattern_counts_and_a_literal_does_not() {
        let graph = graph();
        let fields = [field(
            "supervisor.quota_threshold",
            "quota_threshold",
            "SupervisorConfig",
        )];
        let paths: BTreeSet<String> = BTreeSet::new();
        let pattern = "fn f(cfg: cog_core::config::SupervisorConfig) {\n\
                       let cog_core::config::SupervisorConfig { quota_threshold, .. } = cfg;\n\
                       drop(quota_threshold);\n}\n";
        let evidence = scan_text(pattern, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            evidence.covers(&fields[0]),
            "a destructuring pattern was not read"
        );

        let literal = "fn f() -> cog_core::config::SupervisorConfig {\n\
                       cog_core::config::SupervisorConfig { quota_threshold: 1, ..Default::default() }\n}\n";
        let evidence = scan_text(literal, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            !evidence.covers(&fields[0]),
            "a construction was read as consumption"
        );
    }

    #[test]
    fn a_read_through_a_container_is_attributed_to_the_inner_type() {
        let graph = graph();
        let fields = [field(
            "gateway.notification_limit",
            "notification_limit",
            "GatewayConfig",
        )];
        let paths: BTreeSet<String> = BTreeSet::new();
        let text =
            "fn f(state: &std::sync::Arc<std::sync::RwLock<cog_core::config::GatewayConfig>>) {\n\
                    let n = state.read().unwrap().notification_limit;\n}\n";
        let evidence = scan_text(text, &graph, Path::new("crates/x/src/a.rs"), &paths);
        assert!(
            evidence.covers(&fields[0]),
            "a read through Arc<RwLock<T>> was not attributed"
        );
    }
}
