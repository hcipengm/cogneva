//! Every namespace the code reads must be a namespace something in this
//! workspace writes.
//!
//! A read against a namespace nothing writes to returns empty, and it returns
//! empty exactly the way a namespace with no rows in it does. Nothing errors,
//! nothing logs, and the caller reads the emptiness as "no relevant experience"
//! — which is a plausible answer. So a namespace that is only ever read stays
//! broken indefinitely, and every call site that depends on it — the Planner's
//! similar decompositions, the Generator's reference implementations, the
//! Evaluator's failure patterns — degrades to a lookup that always misses.
//!
//! The pair cannot be checked at run time from either end, because both ends
//! look identical to a working one. It can be checked statically: a namespace is
//! a name, and the name has to appear on both a read and a write. That is what
//! this gate does, over `crates/*/src`.
//!
//! What it does not see, so that the claim stays honest: a namespace passed
//! through a variable (`search_schema(namespace, ...)`) is plumbing between
//! layers and carries no name to pair. Those rows are visible only where they
//! are bound, and the layer that binds them is what this gate checks. A
//! namespace whose writer is not a call in this tree is listed in
//! [`EXTERNAL_WRITERS`] with the file and the pattern that proves the writer
//! exists: an exemption is a claim about the code, so a claim that stops being
//! true fails the test rather than quietly licensing the read.
//!
//! A writer is not enough. An entry point that writes a different namespace per
//! arm of the envelope it is handed — the archive writes one for runs that
//! delivered and another for runs that failed — has a writer for both and still
//! leaves one of them empty forever if the calling code only ever builds one
//! way for it to end. Both ends look like a working namespace from the reading
//! side, so the reachability of each arm is checked the same way: statically,
//! over the code that calls the entry point. See [`ARCHIVE_ARMS`].

use std::collections::BTreeSet;
use std::path::PathBuf;

/// A call that writes rows into a namespace, and the argument positions that
/// name it. `upsert` carries the namespace second because it takes the store
/// first; every other writer names it first.
const WRITE_CALLS: &[(&str, usize)] = &[
    ("store_schema(", 2),
    ("store_summary(", 2),
    ("update_schema(", 2),
    ("upsert(", 2),
];

/// A call that reads rows out of a namespace. All of them name it first.
const READ_CALLS: &[&str] = &["search_schema(", "search_all("];

/// A namespace read here whose writer is a call this gate cannot see, with the
/// patterns that have to still be in the tree for the exemption to hold.
struct ExternalWriter {
    namespace: &'static str,
    why: &'static str,
    witnesses: &'static [(&'static str, &'static str)],
}

const EXTERNAL_WRITERS: &[ExternalWriter] = &[
    ExternalWriter {
        namespace: "default",
        why: "the namespace the ingest path substitutes when a caller names none, \
              so its writer is the write path rather than a call that spells it; \
              the store's health check reads one row from it to exercise both layers",
        witnesses: &[
            (
                "crates/cog-gateway/src/memory.rs",
                "const DEFAULT_NS: &str = \"default\";",
            ),
            (
                "crates/cog-gateway/src/memory.rs",
                "store_schema(&ns, entry)",
            ),
        ],
    },
    ExternalWriter {
        namespace: "knowledge",
        why: "the namespace ingested knowledge lands in, chosen by the raw source \
              rather than by any one writer; the ingestor forwards whatever \
              namespace its input carries",
        witnesses: &[(
            "crates/cog-memory/src/ingestor.rs",
            "self.backend.store_schema(&raw.namespace, entry)",
        )],
    },
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every `.rs` file under `crates/*/src`, as (repo-relative path, text).
fn crate_sources() -> Vec<(String, String)> {
    let crates = repo_root().join("crates");
    let mut out = Vec::new();
    let mut crate_dirs: Vec<PathBuf> = std::fs::read_dir(&crates)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", crates.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    crate_dirs.sort();
    for dir in crate_dirs {
        let src = dir.join("src");
        if src.is_dir() {
            collect_rs(&src, &mut out);
        }
    }
    assert!(
        !out.is_empty(),
        "no sources found under {}",
        crates.display()
    );
    out
}

fn collect_rs(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
            let rel = path
                .strip_prefix(repo_root())
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            out.push((rel, text));
        }
    }
}

// ---------------------------------------------------------------------------
// Source scanning
// ---------------------------------------------------------------------------

fn find_from(hay: &[char], needle: &str, from: usize) -> Option<usize> {
    let needle: Vec<char> = needle.chars().collect();
    if needle.is_empty() || hay.len() < needle.len() || from >= hay.len() {
        return None;
    }
    (from..=hay.len() - needle.len()).find(|&i| hay[i..i + needle.len()] == needle[..])
}

/// The source with comments blanked out and string literals kept, so a call
/// named in a comment is not a call site and a namespace spelled in a literal is
/// still readable. Newlines survive, so offsets and line numbers still point at
/// the real file.
fn blank_comments(src: &str) -> String {
    let chars: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '/' && next == Some('/') {
            while i < chars.len() && chars[i] != '\n' {
                out.push(' ');
                i += 1;
            }
        } else if c == '/' && next == Some('*') {
            out.push(' ');
            out.push(' ');
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                out.push(if chars[i] == '\n' { '\n' } else { ' ' });
                i += 1;
            }
            if i < chars.len() {
                out.push(' ');
                out.push(' ');
                i += 2;
            }
        } else if c == '"' {
            out.push('"');
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    out.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                out.push('"');
                i += 1;
            }
        } else if c == '\'' {
            // A character literal is copied whole, so a quote inside it cannot
            // open a string; a lifetime is not one, and swallowing the code
            // after it as if it were would hide every call on the line.
            let literal_end = if chars.get(i + 1) == Some(&'\\') && chars.get(i + 3) == Some(&'\'')
            {
                Some(i + 3)
            } else if chars.get(i + 2) == Some(&'\'') {
                Some(i + 2)
            } else {
                None
            };
            match literal_end {
                Some(end) => {
                    out.extend(chars.iter().take(end + 1).skip(i));
                    i = end + 1;
                }
                None => {
                    out.push(c);
                    i += 1;
                }
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

/// The comment-blanked source with every `#[cfg(test)]` item blanked out too.
///
/// A namespace written only by a test is not written: the rows a test stores
/// live in the test's own store and are gone when it ends. Leaving test writers
/// in would let a fixture stand in as the producer for a production read, which
/// is the very pairing this gate is about.
fn production_code(src: &str) -> String {
    let chars: Vec<char> = blank_comments(src).chars().collect();
    let mut out = chars.clone();
    let mut search = 0;
    while let Some(pos) = find_from(&chars, "#[cfg(test)]", search) {
        let mut i = pos + "#[cfg(test)]".chars().count();
        let mut depth = 0i32;
        let mut end = chars.len().saturating_sub(1);
        while i < chars.len() {
            match chars[i] {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                ';' if depth == 0 => {
                    end = i;
                    break;
                }
                _ => {}
            }
            i += 1;
        }
        for slot in out.iter_mut().take(end + 1).skip(pos) {
            if *slot != '\n' {
                *slot = ' ';
            }
        }
        search = end + 1;
    }
    out.into_iter().collect()
}

/// The text between the parentheses of the call opening at `open`.
fn call_body(chars: &[char], open: usize) -> &[char] {
    bracketed(chars, open, '(', ')')
}

/// The text between the braces of the literal opening at `open`.
fn brace_body(chars: &[char], open: usize) -> &[char] {
    bracketed(chars, open, '{', '}')
}

/// The text inside the bracket pair opening at `open`.
fn bracketed(chars: &[char], open: usize, open_char: char, close_char: char) -> &[char] {
    let mut depth = 0i32;
    let mut i = open;
    while i < chars.len() {
        if chars[i] == open_char {
            depth += 1;
        } else if chars[i] == close_char {
            depth -= 1;
            if depth == 0 {
                return &chars[open + 1..i];
            }
        }
        i += 1;
    }
    &[]
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Whether `needle` sits at `i` in `chars` as a whole word, so `success` is not
/// read out of `succeeded`.
fn word_at(chars: &[char], i: usize, needle: &[char]) -> bool {
    if chars.len() < i + needle.len() || chars[i..i + needle.len()] != *needle {
        return false;
    }
    let before = i == 0 || !is_ident_char(chars[i - 1]);
    let after = !chars
        .get(i + needle.len())
        .is_some_and(|c| is_ident_char(*c));
    before && after
}

/// The first `n` comma-separated arguments of an already unparenthesised body.
fn first_arguments(body: &[char], n: usize) -> Vec<String> {
    let mut args = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for &c in body {
        match c {
            '(' | '[' | '{' => {
                depth += 1;
                cur.push(c);
            }
            ')' | ']' | '}' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => {
                args.push(std::mem::take(&mut cur));
                if args.len() == n {
                    return args;
                }
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        args.push(cur);
    }
    args
}

/// The namespace names an argument text spells: an all-caps identifier, or a
/// string literal. Text inside a string literal is not read as an identifier, so
/// a name quoted in a message is not mistaken for a use of it.
fn namespaces_in(arg: &str) -> Vec<String> {
    let chars: Vec<char> = arg.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            let mut j = i + 1;
            while j < chars.len() && chars[j] != '"' {
                if chars[j] == '\\' {
                    j += 1;
                }
                j += 1;
            }
            out.push(chars[i + 1..j.min(chars.len())].iter().collect());
            i = j + 1;
            continue;
        }
        let word_start = chars[i].is_ascii_uppercase()
            && (i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_'));
        if word_start {
            let start = i;
            while i < chars.len()
                && (chars[i].is_ascii_uppercase() || chars[i].is_ascii_digit() || chars[i] == '_')
            {
                i += 1;
            }
            if i - start >= 2 {
                out.push(chars[start..i].iter().collect());
            }
        } else {
            i += 1;
        }
    }
    out
}

/// Every call to `name` in `code`, as (line number, argument body).
fn call_sites(code: &str, name: &str, arity: usize) -> Vec<(usize, Vec<String>)> {
    let chars: Vec<char> = code.chars().collect();
    let mut out = Vec::new();
    let mut search = 0;
    while let Some(pos) = find_from(&chars, name, search) {
        search = pos + name.chars().count();
        // `fn search_schema(` declares the call; only calls are call sites.
        let before: Vec<char> = chars[..pos]
            .iter()
            .rev()
            .skip_while(|c| c.is_whitespace())
            .take(2)
            .copied()
            .collect();
        if before == vec!['n', 'f'] {
            continue;
        }
        let open = pos + name.chars().count() - 1;
        let line = chars[..pos].iter().filter(|c| **c == '\n').count() + 1;
        out.push((line, first_arguments(call_body(&chars, open), arity)));
    }
    out
}

fn read_namespaces(code: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for name in READ_CALLS {
        for (line, args) in call_sites(code, name, 1) {
            let Some(first) = args.first() else {
                continue;
            };
            for ns in namespaces_in(first) {
                out.push((line, ns));
            }
        }
    }
    out
}

fn written_namespaces(code: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (name, arity) in WRITE_CALLS {
        for (_, args) in call_sites(code, name, *arity) {
            for arg in &args {
                out.extend(namespaces_in(arg));
            }
        }
    }
    out
}

/// The namespace-constant definitions in a source, identifier to the name it
/// stands for.
///
/// The contract is about namespaces, not spellings: a read that names the
/// namespace through a constant and a write that names it as a literal are the
/// same pairing. Resolving both sides to the value is what makes them meet, and
/// it is what lets an exemption be written as the namespace it is about.
fn namespace_constants(code: &str) -> Vec<(String, String)> {
    let chars: Vec<char> = code.chars().collect();
    let mut out = Vec::new();
    let mut search = 0;
    while let Some(pos) = find_from(&chars, "const ", search) {
        search = pos + "const ".len();
        let mut i = pos + "const ".len();
        let start = i;
        while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
            i += 1;
        }
        let ident: String = chars[start..i].iter().collect();
        let rest: Vec<char> = chars[i..].to_vec();
        let mut j = 0;
        while j < rest.len() && rest[j].is_whitespace() {
            j += 1;
        }
        if rest.get(j) != Some(&':') {
            continue;
        }
        j += 1;
        while j < rest.len() && rest[j].is_whitespace() {
            j += 1;
        }
        let ty: Vec<char> = rest[j..].iter().copied().take(4).collect();
        if ty != vec!['&', 's', 't', 'r'] {
            continue;
        }
        j += 4;
        while j < rest.len() && rest[j].is_whitespace() {
            j += 1;
        }
        if rest.get(j) != Some(&'=') {
            continue;
        }
        j += 1;
        while j < rest.len() && rest[j].is_whitespace() {
            j += 1;
        }
        if rest.get(j) != Some(&'"') {
            continue;
        }
        j += 1;
        let value_start = j;
        while j < rest.len() && rest[j] != '"' {
            j += 1;
        }
        out.push((ident, rest[value_start..j].iter().collect()));
    }
    out
}

/// What one pass over the sources saw: every namespace read, as
/// (path, line, namespace), and every namespace written.
struct Scan {
    reads: Vec<(String, usize, String, String)>,
    written: BTreeSet<String>,
}

fn scan(sources: &[(String, String)]) -> Scan {
    let code: Vec<String> = sources
        .iter()
        .map(|(_, text)| production_code(text))
        .collect();
    let constants: Vec<(String, String)> =
        code.iter().flat_map(|c| namespace_constants(c)).collect();
    let resolve = |name: &str| -> String {
        constants
            .iter()
            .find(|(ident, _)| ident == name)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| name.to_string())
    };

    let written: BTreeSet<String> = code
        .iter()
        .flat_map(|c| written_namespaces(c))
        .map(|ns| resolve(&ns))
        .collect();

    let mut reads = Vec::new();
    for ((path, _), code) in sources.iter().zip(&code) {
        for (line, ns) in read_namespaces(code) {
            reads.push((path.clone(), line, resolve(&ns), ns));
        }
    }

    Scan { reads, written }
}

/// The reads that reach a namespace nothing in `sources` writes.
fn findings(scan: &Scan) -> Vec<String> {
    let mut out = Vec::new();
    for (path, line, value, spelled) in &scan.reads {
        if scan.written.contains(value) || EXTERNAL_WRITERS.iter().any(|e| e.namespace == value) {
            continue;
        }
        let spelled = if value == spelled {
            String::new()
        } else {
            format!(" ( spelled `{spelled}` )")
        };
        out.push(format!(
            "{path}:{line}: reads namespace `{value}`{spelled}, which nothing in this workspace writes"
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Arm reachability
// ---------------------------------------------------------------------------

/// One arm of an archive entry point that selects between namespaces.
///
/// The gate above asks whether a namespace has a writer. This asks the question
/// the answer to that one cannot cover: whether the branch the writer sits in
/// can be reached at all. An entry point that branches on a field of the
/// envelope it is handed has one arm per value of that field, and an arm whose
/// value the calling code never builds is a namespace nothing will ever write —
/// however many writers the namespace has, and however green the gate above
/// stays.
struct ArchiveArm {
    /// The file implementing the entry point. The arm's namespace is written
    /// there, so an entry naming a namespace that file no longer writes is
    /// stale bookkeeping rather than a live arm.
    owner: &'static str,
    /// The call spelling production uses to hand an envelope to the archive.
    entry: &'static str,
    /// The envelope field that selects the arm.
    discriminator: &'static str,
    /// The value of that field which selects this arm.
    value: &'static str,
    /// The namespace this arm writes.
    namespace: &'static str,
}

const ARCHIVE_ARMS: &[ArchiveArm] = &[
    ArchiveArm {
        owner: "crates/cog-wiki/src/unified_knowledge_backend.rs",
        entry: "archive_execution(",
        discriminator: "success",
        value: "true",
        namespace: "implementation",
    },
    ArchiveArm {
        owner: "crates/cog-wiki/src/unified_knowledge_backend.rs",
        entry: "archive_execution(",
        discriminator: "success",
        value: "false",
        namespace: "failure_pattern",
    },
];

/// The entry point and the field this gate reads, taken from the table so the
/// reach assertions cannot drift from the arms they are there to guard.
const ARCHIVE_ENTRY: &str = ARCHIVE_ARMS[0].entry;
const ARCHIVE_DISCRIMINATOR: &str = ARCHIVE_ARMS[0].discriminator;

/// Whether `code` calls `entry`. A declaration of it is not a call.
fn has_call(code: &str, entry: &str) -> bool {
    !call_sites(code, entry, 1).is_empty()
}

/// The values `field` carries in the envelopes handed to `entry`.
///
/// An envelope reaches the call either built in place or through a binding the
/// call site passes, so both are read. Only those two count: the file's other
/// `TaskResult` literals are results this entry point is never handed, and
/// reading them would let an envelope built for something else stand in for the
/// arm being checked. An argument that resolves to neither contributes nothing,
/// which leaves its arm unproven — the safe direction, since the defect here is
/// a namespace that stays empty while everything looks written.
fn archived_values(code: &str, entry: &str, field: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (_, args) in call_sites(code, entry, usize::MAX) {
        for arg in &args {
            if arg.contains("TaskResult") {
                out.extend(discriminator_values(arg, field));
            } else if let Some(name) = bound_ident(arg) {
                out.extend(binding_discriminator(code, &name, field));
            }
        }
    }
    out
}

/// The identifier `arg` names, when it is one: `&task_result` and `mut x` name
/// a binding, a call or a field access names nothing this can follow.
fn bound_ident(arg: &str) -> Option<String> {
    let text = arg.trim().trim_start_matches('&').trim();
    let text = text.strip_prefix("mut ").unwrap_or(text).trim();
    let mut chars = text.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic() && first != '_' {
        return None;
    }
    if !text.chars().all(is_ident_char) {
        return None;
    }
    Some(text.to_string())
}

/// The value `field` carries in the `TaskResult` bound to `name`, if the binding
/// in `code` builds one.
fn binding_discriminator(code: &str, name: &str, field: &str) -> Option<String> {
    const BINDING: &str = "=TaskResult{";
    let chars: Vec<char> = code.chars().collect();
    let needle = format!("let {name}");
    let field: Vec<char> = field.chars().collect();
    let mut search = 0;
    while let Some(pos) = find_from(&chars, &needle, search) {
        search = pos + needle.chars().count();
        if chars.get(search).is_some_and(|c| is_ident_char(*c)) {
            continue;
        }
        // The spelling with the whitespace taken out, so a line break between
        // the `=` and the type still reads as one binding.
        let mut i = search;
        let mut text = String::new();
        while i < chars.len() && text.len() < BINDING.len() {
            if !chars[i].is_whitespace() {
                text.push(chars[i]);
            }
            i += 1;
        }
        if text == BINDING {
            return field_value(brace_body(&chars, i - 1), &field);
        }
    }
    None
}

/// Every `TaskResult` struct literal in `code`, as the text assigned to `field`.
///
/// Only the literals count, and a literal that does not set the field is not
/// reported: the arm is selected by what the envelope says, so an envelope
/// built without the field says nothing about which arm it takes.
fn discriminator_values(code: &str, field: &str) -> Vec<String> {
    let chars: Vec<char> = code.chars().collect();
    let needle: Vec<char> = field.chars().collect();
    let mut out = Vec::new();
    let mut search = 0;
    while let Some(pos) = find_from(&chars, "TaskResult", search) {
        search = pos + "TaskResult".len();
        let mut i = search;
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if chars.get(i) != Some(&'{') {
            continue;
        }
        if let Some(value) = field_value(brace_body(&chars, i), &needle) {
            out.push(value);
        }
    }
    out
}

/// The text assigned to `field` in one struct-literal body, read at the body's
/// own depth so a field of a nested literal is not taken for this one's.
fn field_value(body: &[char], field: &[char]) -> Option<String> {
    let mut depth = 0i32;
    let mut i = 0;
    while i < body.len() {
        match body[i] {
            '{' | '(' | '[' => {
                depth += 1;
                i += 1;
                continue;
            }
            '}' | ')' | ']' => {
                depth -= 1;
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 && word_at(body, i, field) {
            let mut j = i + field.len();
            while j < body.len() && body[j].is_whitespace() {
                j += 1;
            }
            if body.get(j) != Some(&':') {
                i += 1;
                continue;
            }
            j += 1;
            let start = j;
            let mut inner = 0i32;
            while j < body.len() {
                match body[j] {
                    '{' | '(' | '[' => inner += 1,
                    '}' | ')' | ']' => inner -= 1,
                    ',' if inner == 0 => break,
                    _ => {}
                }
                j += 1;
            }
            let text: String = body[start..j].iter().collect();
            return Some(text.trim().to_string());
        }
        i += 1;
    }
    None
}

/// The arms whose value the code that archives never builds.
///
/// The surface this reads is the file that hands envelopes to the entry point:
/// a value built anywhere else is not evidence that this call site can produce
/// it. That is a requirement on the code, not a limitation of the parser — the
/// envelope is built where the outcome is known, which is where the archive is
/// called from. Failing closed when the construction moves away is the safe
/// direction: a gate that says nothing when it can see nothing is the silence
/// this file exists to refuse.
fn arm_findings(sources: &[(String, String)]) -> Vec<String> {
    let code: Vec<(String, String)> = sources
        .iter()
        .map(|(path, text)| (path.clone(), production_code(text)))
        .collect();

    let mut entries: Vec<&str> = ARCHIVE_ARMS.iter().map(|arm| arm.entry).collect();
    entries.sort_unstable();
    entries.dedup();

    let mut out = Vec::new();
    for entry in entries {
        let arms: Vec<&ArchiveArm> = ARCHIVE_ARMS
            .iter()
            .filter(|arm| arm.entry == entry)
            .collect();
        let callers: Vec<&str> = code
            .iter()
            .filter(|(_, text)| has_call(text, entry))
            .map(|(path, _)| path.as_str())
            .collect();
        if callers.is_empty() {
            out.push(format!(
                "`{entry}` 没有任何调用点：表里登记在它名下的每条臂都没有产出方，\
                 命名空间 {:?} 与登记本身一起过期了",
                arms.iter().map(|arm| arm.namespace).collect::<Vec<_>>()
            ));
            continue;
        }

        let values: Vec<String> = code
            .iter()
            .filter(|(_, text)| has_call(text, entry))
            .flat_map(|(_, text)| archived_values(text, entry, arms[0].discriminator))
            .collect();

        for value in &values {
            if !arms.iter().any(|arm| arm.value == value) {
                out.push(format!(
                    "`{entry}` 的信封里 {} 的取值 `{value}` 不是字面量，这条臂可不可达从源码读不出来。\
                     判别字段写成字面量，门禁才有东西可读",
                    arms[0].discriminator
                ));
            }
        }

        let missing: Vec<&&ArchiveArm> = arms
            .iter()
            .filter(|arm| !values.iter().any(|value| value == arm.value))
            .collect();
        if missing.is_empty() {
            continue;
        }
        out.push(format!(
            "`{entry}` 的调用点只构造过 {} = {} 的信封，从没构造过 {}：\n  \
             调用这个入口的文件：{}\n  \
             于是命名空间 {} 的写入分支到不了，它永远为空——而空读起来和「这件事还没发生过」一样，\n  \
             读它的那一侧拿到的就是一条永远为空的答案。在失败的路径上也构造信封并归档。",
            arms[0].discriminator,
            values.join(" / "),
            missing
                .iter()
                .map(|arm| arm.value)
                .collect::<Vec<_>>()
                .join(" / "),
            callers.join(", "),
            missing
                .iter()
                .map(|arm| format!("`{}`", arm.namespace))
                .collect::<Vec<_>>()
                .join("、")
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn every_namespace_read_is_one_something_writes() {
    let scan = scan(&crate_sources());

    // The gate is only as good as its reach, and a scanner that matches nothing
    // reports nothing. These are the namespaces the knowledge layer owns: they
    // are read and written today, so a pass over the tree has to have seen them.
    // If this fails while the assertion below still passes, the scan stopped
    // reaching the call sites and the clean result means nothing.
    let seen_reads: BTreeSet<&str> = scan.reads.iter().map(|(_, _, ns, _)| ns.as_str()).collect();
    for expected in [
        "task_decomposition",
        "implementation",
        "failure_pattern",
        "task_execution",
        "knowledge",
    ] {
        assert!(
            seen_reads.contains(expected),
            "扫描没看到 `{expected}` 的读取：扫描面漏了，下面的空结果不算数。看到的读取: {seen_reads:?}"
        );
    }
    for expected in ["task_decomposition", "implementation", "failure_pattern"] {
        assert!(
            scan.written.contains(expected),
            "扫描没看到 `{expected}` 的写入。看到的写入: {:?}",
            scan.written
        );
    }

    let bad = findings(&scan);
    assert!(
        bad.is_empty(),
        "有个命名空间只被读、没人写：读它永远拿到空，而空看起来和「还没有经验」一模一样，\n\
         所以坏着也不会有人发现。补上写入侧，或登记进 EXTERNAL_WRITERS 并附证据：\n{}",
        bad.join("\n")
    );
}

/// An exemption is a claim that the writer exists somewhere this gate cannot
/// see. The patterns that make the claim true are read from the files: an
/// exemption whose writer was renamed or removed stops justifying anything and
/// fails here, instead of turning into a permanent license for the read.
#[test]
fn every_exemption_names_a_writer_that_is_still_there() {
    let scan = scan(&crate_sources());
    let seen_reads: BTreeSet<&str> = scan.reads.iter().map(|(_, _, ns, _)| ns.as_str()).collect();

    for entry in EXTERNAL_WRITERS {
        assert!(
            seen_reads.contains(entry.namespace),
            "豁免「{}」已经没人读了，是过期的账：{}\n\
             留着的豁免会替以后真正需要它的读放行，所以读没了就该删。",
            entry.namespace,
            entry.why
        );
        for (file, pattern) in entry.witnesses {
            let path = repo_root().join(file);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
            assert!(
                text.contains(pattern),
                "豁免「{}」的证据不在 {} 里了（找不到 `{}`）：{}\n\
                 证据没了，这条豁免就不再成立，得重新找写入侧或把它删掉。",
                entry.namespace,
                file,
                pattern,
                entry.why
            );
        }
    }
}

/// The gate has to be able to fail. This drives it over two namespaces that look
/// the same to it except for the writer, so a scanner that reported nothing —
/// because it stopped matching call sites, say — cannot pass as a clean tree.
#[test]
fn the_gate_reports_a_read_whose_namespace_nothing_writes() {
    let source = r#"
        const NS_WRITTEN: &str = "written";
        const NS_ORPHAN: &str = "orphan";

        fn look(stores: &Mixed) {
            let a = stores.memory.search_schema(NS_WRITTEN, "q", 3).await;
            let b = stores.memory.search_schema(NS_ORPHAN, "q", 3).await;
        }

        fn record(stores: &Mixed, entry: &Entry) {
            stores.memory.store_schema(NS_WRITTEN, entry).await;
        }
    "#;
    let found = findings(&scan(&[("fixture.rs".into(), source.into())]));
    assert_eq!(found.len(), 1, "只该报没有写入侧的那个: {found:?}");
    assert!(found[0].contains("NS_ORPHAN"), "{found:?}");
    assert!(found[0].starts_with("fixture.rs:"), "{found:?}");
}

/// A write inside a test module is not a writer: it stores into the test's own
/// store and is gone when the test ends. Without this the fixture above would
/// pass, and so would a namespace whose only writer is a test.
#[test]
fn a_write_in_a_test_module_is_not_a_writer() {
    let source = r#"
        const NS_TEST_ONLY: &str = "test_only";

        fn look(stores: &Mixed) {
            let a = stores.memory.search_all(NS_TEST_ONLY, "q", None, 3, None).await;
        }

        #[cfg(test)]
        mod tests {
            use super::*;

            #[tokio::test]
            async fn a_row_is_stored() {
                let stores = Mixed::new();
                stores.memory.store_schema(NS_TEST_ONLY, &entry()).await;
            }
        }
    "#;
    let found = findings(&scan(&[("fixture.rs".into(), source.into())]));
    assert_eq!(found.len(), 1, "测试里的写入不算写入侧: {found:?}");
    assert!(found[0].contains("NS_TEST_ONLY"), "{found:?}");
}

/// Every arm of every archive entry point has to be built by the code that
/// archives. A namespace with a writer is not a namespace anything writes to:
/// if the only value the calling code ever puts in the envelope is `true`, the
/// failure arm — and the namespace it owns — is unreachable, while the gate
/// above sees a writer and stays green.
#[test]
fn every_arm_of_an_archive_is_built_by_the_code_that_calls_it() {
    let sources = crate_sources();

    // The scan is only as good as its reach, and a scan that stopped matching
    // the call sites reports nothing, which looks the same as a tree where
    // every arm is built. The archive is dispatched from one file today, so a
    // pass has to have seen it and read an envelope through it. What it read is
    // not asserted here: "only one value" is the finding below, and asserting it
    // here as well would report a missing arm as a broken scanner.
    let code: Vec<(String, String)> = sources
        .iter()
        .map(|(path, text)| (path.clone(), production_code(text)))
        .collect();
    let callers: Vec<&str> = code
        .iter()
        .filter(|(_, text)| has_call(text, ARCHIVE_ENTRY))
        .map(|(path, _)| path.as_str())
        .collect();
    assert!(
        callers.contains(&"crates/cog-collaboration/src/collaboration_executor.rs"),
        "扫描没看到归档的调用点（看到的是 {callers:?}）：下面的空结果不算数"
    );
    let seen: Vec<String> = code
        .iter()
        .filter(|(_, text)| has_call(text, ARCHIVE_ENTRY))
        .flat_map(|(_, text)| archived_values(text, ARCHIVE_ENTRY, ARCHIVE_DISCRIMINATOR))
        .collect();
    assert!(
        !seen.is_empty(),
        "扫描读不到归档调用点上任何一个信封的判别字段：扫描面漏了，下面的空结果不算数"
    );

    let bad = arm_findings(&sources);
    assert!(
        bad.is_empty(),
        "归档的某条臂没有任何调用点构造它的信封：那条臂写不进去，而读它的一侧拿到的空\n\
         和「这件事还没发生过」一模一样。\n{}",
        bad.join("\n")
    );
}

/// An arm's namespace has to be one the file implementing the entry point still
/// writes. The table says which namespace each arm owns so the message can name
/// what goes empty; a table that keeps saying it after the arm stopped writing
/// there would send the next reader after the wrong namespace.
#[test]
fn every_arm_names_a_namespace_its_owner_still_writes() {
    let sources = crate_sources();
    let mut bad = Vec::new();
    for arm in ARCHIVE_ARMS {
        let text = sources
            .iter()
            .find(|(path, _)| path == arm.owner)
            .map(|(_, text)| text.clone())
            .unwrap_or_else(|| panic!("{} 不在 crates/*/src 里了", arm.owner));
        let code = production_code(&text);
        let constants: Vec<(String, String)> = namespace_constants(&code);
        let written: BTreeSet<String> = written_namespaces(&code)
            .into_iter()
            .map(|ns| {
                constants
                    .iter()
                    .find(|(ident, _)| *ident == ns)
                    .map(|(_, value)| value.clone())
                    .unwrap_or(ns)
            })
            .collect();
        if !written.contains(arm.namespace) {
            bad.push(format!(
                "{} 登记的臂说 `{}` 由 {} 写，那个文件里没有它的写入",
                arm.entry, arm.namespace, arm.owner
            ));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// The gate has to be able to fail. This drives it over a fixture that archives
/// outcomes and only ever builds one of them, so a scanner that reported
/// nothing — because it stopped reading the envelopes, say — cannot pass as a
/// tree where both arms exist.
#[test]
fn the_gate_reports_an_arm_the_calling_code_never_builds() {
    let source = r#"
        fn record(&self, task: &Task, result: &SquadResult) {
            let task_result = TaskResult {
                success: true,
                output: serde_json::json!({ "squad_result": result }),
                metadata: TaskResultMetadata::new("collaboration"),
            };
            self.archive_execution(task, &task_result);
        }
    "#;
    let found = arm_findings(&[("fixture.rs".into(), source.into())]);
    assert_eq!(found.len(), 1, "只该报没人构造的那条臂: {found:?}");
    assert!(found[0].contains("failure_pattern"), "{found:?}");
}

/// The discriminator has to be a literal for the gate to read anything. A value
/// computed at the call site leaves the arm unprovable, and unproven is not
/// proven: reporting it keeps the gate from passing on a tree where nothing can
/// be said about reachability.
#[test]
fn the_gate_reports_an_arm_it_cannot_read_from_the_source() {
    let source = r#"
        fn record(&self, task: &Task, delivered: bool) {
            self.archive_execution(
                task,
                &TaskResult {
                    success: delivered,
                    output: serde_json::json!({}),
                    metadata: TaskResultMetadata::new("collaboration"),
                },
            );
        }
    "#;
    let found = arm_findings(&[("fixture.rs".into(), source.into())]);
    assert!(
        found.iter().any(|f| f.contains("不是字面量")),
        "没把不可读的判别字段报出来: {found:?}"
    );
}
