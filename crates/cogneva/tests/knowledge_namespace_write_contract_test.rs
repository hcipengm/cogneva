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
    let mut depth = 0i32;
    let mut i = open;
    while i < chars.len() {
        match chars[i] {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &chars[open + 1..i];
                }
            }
            _ => {}
        }
        i += 1;
    }
    &[]
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
