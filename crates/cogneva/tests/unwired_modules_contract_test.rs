//! Gate: a `.rs` the repository carries under a crate's `src/` is code only if some
//! `mod` declaration reaches it.
//!
//! `cargo` compiles a crate from its roots — `lib.rs`, `main.rs`, a `[[bin]]` path —
//! and follows the `mod` declarations it finds from there. A file that no declaration
//! reaches is never compiled: no build target, no test, no lint, and nothing that
//! starts from a symbol ever reads a byte of it. It sits in the source tree and reads
//! as landed while the compiler has never seen it.
//!
//! The sibling gate in `crate_source_membership_contract_test.rs` asks a different
//! question about the same tree: whether the *directory* a file sits in is a workspace
//! member. Its own header says the gate is drawn around the directory and not a file
//! list, so a file inside a member's `src/` that nothing declares passes it. That is
//! the gap this gate closes, and the two are meant to be read together.
//!
//! What is judged is the *declaration*, not the build. A module written
//! `#[cfg(feature = "rhai")] pub mod script;` is reached: the declaration is there and
//! the feature decides whether the compiler takes it, which is a question about this
//! build and not about the tree. Judging by "does this build compile it" would report a
//! cfg-gated module as an orphan on every configuration that leaves it out.
//!
//! The gate has no roster. It used to carry one — a table of files known to be unreached,
//! each with where it was going or that it was going away — so that a newly orphaned file
//! could be recorded as debt instead of failing the build. That table stayed empty, which
//! is the reading it was built to reach, and the rule it was protecting is the sentence
//! above: a file nothing declares reaches no build target. A roster that is allowed to
//! grow is also a place for an orphan to hide, so the rule is asserted whole and the
//! roster is gone.
//!
//! Which files are in scope. The universe is what the repository *carries*:
//! `git ls-files` decides, not a walk of the working tree. A file that is not in the
//! repository has not landed, so nothing is owed for it — and one shape relies on
//! exactly that. A probe placed in the working tree to prove another census gate really
//! reads the whole tree is written *outside* the module tree on purpose, so that the
//! compiler never sees it and it costs no rebuild while it is there, and it is removed
//! before the tree is committed. A walk of the working tree would red on that probe in
//! the middle of a measurement; reading what the repository carries leaves it alone,
//! while a probe that gets *committed* is caught like any other file. The universe is
//! read once per run, so a probe costs this gate nothing and buys no exemption.
//!
//! Known gap: a module brought in with `#[path = "..."]` is resolved here to `name.rs`
//! beside its declaration, so such a file would read as unreached even though cargo
//! compiles it. No `src/` file uses the attribute today. Rather than carry a resolver
//! nothing exercises, the gate fails loudly the moment one appears, naming what to
//! extend — a wrong answer that is loud beats a resolver that is quietly wrong.
//!
//! principle: feedback_a_change_lands_only_if_it_reaches_a_build_target
//! principle: feedback_a_gate_that_forbids_a_shape_must_judge_what_the_repository_carries

mod common;

use common::routes::repo_root;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The workspace members under `crates/`, each with the source files cargo is told to
/// start compiling it from.
///
/// Membership and the roots are both read through `cargo metadata` rather than by
/// parsing a manifest: a target's `path` can be spelled in the manifest, a crate can be
/// re-rooted, and the question here is what the compiler is handed, not how the file
/// format spells it. The roots are restricted to files under each member's `src/`,
/// because that is the universe this gate is about — a target compiled from outside
/// `src/` has no `src/`-relative module tree to check.
fn member_roots() -> BTreeMap<PathBuf, BTreeSet<PathBuf>> {
    let root = repo_root();
    let manifest = root.join("Cargo.toml");
    let out = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
        ])
        .arg(&manifest)
        .output()
        .expect("cargo metadata must be runnable from a test binary");
    assert!(
        out.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let doc: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cargo metadata must emit JSON");

    let mut members: BTreeMap<PathBuf, BTreeSet<PathBuf>> = BTreeMap::new();
    for package in doc["packages"]
        .as_array()
        .expect("cargo metadata carries packages")
    {
        let manifest_path = PathBuf::from(
            package["manifest_path"]
                .as_str()
                .expect("a package has a manifest path"),
        );
        let dir = manifest_path
            .parent()
            .expect("a manifest path has a directory");
        // cargo reports absolute paths; the walk below builds its own from the same
        // root, and `..` left in one side would make equal directories compare unequal.
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        let crate_dir = match dir.strip_prefix(root.join("crates")) {
            Ok(_) => dir.clone(),
            // A member outside `crates/` is not this gate's business.
            Err(_) => continue,
        };
        let src = crate_dir.join("src");
        let mut roots = BTreeSet::new();
        for target in package["targets"]
            .as_array()
            .expect("a package carries its targets")
        {
            let kinds: BTreeSet<&str> = target["kind"]
                .as_array()
                .expect("a target carries its kinds")
                .iter()
                .filter_map(|k| k.as_str())
                .collect();
            // Only the targets the compiler *starts* from. `test`, `example` and
            // `bench` targets live outside `src/` and are their own roots anyway.
            let starts_a_build = kinds
                .iter()
                .any(|k| matches!(*k, "lib" | "bin" | "proc-macro"));
            if !starts_a_build {
                continue;
            }
            let src_path = PathBuf::from(
                target["src_path"]
                    .as_str()
                    .expect("a target has a source path"),
            );
            let src_path = std::fs::canonicalize(&src_path).unwrap_or(src_path);
            if src_path.starts_with(&src) {
                roots.insert(src_path);
            }
        }
        members.entry(crate_dir).or_default().extend(roots);
    }
    members
}

/// Every `.rs` under `crates/` that the repository carries, as absolute paths.
///
/// Read from the index rather than from a walk of the working tree: the question is
/// what the repository carries, and an untracked file is a probe or a scratch file that
/// has not landed. See the module header for the shape this is protecting.
fn carried_sources() -> BTreeSet<PathBuf> {
    let root = repo_root();
    let out = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z", "--", "crates"])
        .output()
        .expect("git must be runnable from a test binary");
    assert!(
        out.status.success(),
        "git ls-files failed, so the set of files the repository carries is unknown: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = std::str::from_utf8(&out.stdout).expect("this repository's paths are UTF-8");
    text.split('\0')
        .filter(|rel| !rel.is_empty())
        .map(|rel| root.join(rel))
        .filter(|path| path.extension().is_some_and(|e| e == "rs"))
        .collect()
}

/// The text with comments, string literals and character literals blanked, newlines
/// kept.
///
/// A declaration scanner has to look at code and not at prose: a doc comment that
/// mentions a module, a test string that spells one, and a `#[path = "..."]` attribute
/// all contain the word `mod` in a position no declaration is. Blanking the non-code
/// keeps the line structure, so a reader of a failure can still be pointed at a line.
fn code_only(text: &str) -> String {
    let c: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let blank = |out: &mut String, from: usize, to: usize, c: &[char]| {
        for ch in &c[from..to] {
            out.push(if *ch == '\n' { '\n' } else { ' ' });
        }
    };
    let mut i = 0;
    while i < c.len() {
        // A line comment runs to the end of the line, newline excluded.
        if c[i] == '/' && c.get(i + 1) == Some(&'/') {
            let start = i;
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
            blank(&mut out, start, i, &c);
            continue;
        }
        // A block comment nests, so the depth has to be counted.
        if c[i] == '/' && c.get(i + 1) == Some(&'*') {
            let start = i;
            let mut depth = 0usize;
            while i < c.len() {
                if c[i] == '/' && c.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if c[i] == '*' && c.get(i + 1) == Some(&'/') {
                    i += 2;
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            blank(&mut out, start, i.min(c.len()), &c);
            continue;
        }
        // A raw string (`r"..."`, `r#"..."#`, and the `b`-prefixed forms) ends at a
        // quote followed by as many hashes as opened it.
        if let Some(end) = raw_string_end(&c, i) {
            blank(&mut out, i, end, &c);
            i = end;
            continue;
        }
        // An ordinary string literal, escapes honoured.
        if c[i] == '"' {
            let start = i;
            i += 1;
            while i < c.len() {
                if c[i] == '\\' {
                    i += 2;
                } else if c[i] == '"' {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            blank(&mut out, start, i.min(c.len()), &c);
            continue;
        }
        // A character literal holds exactly one character, escapes included. A lone
        // quote that opens neither is a lifetime, and the quote itself is code.
        if c[i] == '\'' {
            let is_char = matches!(
                (c.get(i + 1), c.get(i + 2)),
                (Some(&'\\'), _) | (Some(_), Some(&'\''))
            );
            if is_char {
                let start = i;
                i += 1;
                while i < c.len() {
                    if c[i] == '\\' {
                        i += 2;
                    } else if c[i] == '\'' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
                blank(&mut out, start, i.min(c.len()), &c);
                continue;
            }
        }
        out.push(c[i]);
        i += 1;
    }
    out
}

/// Where a raw string literal that starts at `start` ends, one past its closing quote.
///
/// `None` when no raw string starts there, which leaves an ordinary `r` identifier or a
/// `b` byte prefix to the rest of the scanner.
fn raw_string_end(c: &[char], start: usize) -> Option<usize> {
    let mut j = start;
    if c.get(j) == Some(&'b') {
        j += 1;
    }
    if c.get(j) != Some(&'r') {
        return None;
    }
    j += 1;
    let mut hashes = 0;
    while c.get(j) == Some(&'#') {
        hashes += 1;
        j += 1;
    }
    if c.get(j) != Some(&'"') {
        return None;
    }
    j += 1;
    while j < c.len() {
        if c[j] == '"' {
            let closes = (0..hashes).all(|k| c.get(j + 1 + k) == Some(&'#'));
            if closes {
                return Some(j + 1 + hashes);
            }
        }
        j += 1;
    }
    // An unterminated literal: blanking to the end is the safe reading, since the file
    // cannot compile anyway.
    Some(c.len())
}

/// The module names declared by `mod name;` in `text`.
///
/// A declaration is read as the keyword followed by a name and a `;`; a name followed by
/// a `{` is an inline module and declares no file. The scan is over [`code_only`], so a
/// `mod` inside a comment or a string is not one.
fn declared_modules(text: &str) -> Vec<String> {
    let code: Vec<char> = code_only(text).chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < code.len() {
        if !is_keyword(&code, i, "mod") {
            i += 1;
            continue;
        }
        i += 3;
        while i < code.len() && code[i].is_whitespace() {
            i += 1;
        }
        let start = i;
        while i < code.len() && (code[i].is_alphanumeric() || code[i] == '_') {
            i += 1;
        }
        if i == start {
            continue;
        }
        let name: String = code[start..i].iter().collect();
        let mut j = i;
        while j < code.len() && code[j].is_whitespace() {
            j += 1;
        }
        // `;` ends a file module; `{` opens an inline one, which declares no file.
        if code.get(j) == Some(&';') {
            out.push(name);
        }
    }
    out
}

/// Whether `word` stands at `at` as a whole word, so that `mod` inside `submod` or
/// `modify` is not read as the keyword.
fn is_keyword(code: &[char], at: usize, word: &str) -> bool {
    let chars: Vec<char> = word.chars().collect();
    if code.get(at..at + chars.len()) != Some(&chars[..]) {
        return false;
    }
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    if at > 0 && ident(code[at - 1]) {
        return false;
    }
    !matches!(code.get(at + chars.len()), Some(c) if ident(*c))
}

/// The directory a module's child modules are declared in.
///
/// A crate root's children sit beside it (`src/lib.rs` declares `src/foo.rs`), and so do
/// the children of a file named `mod.rs`; every other file's children sit in a directory
/// named after it (`src/foo.rs` declares `src/foo/bar.rs`).
fn child_dir(file: &Path, is_crate_root: bool) -> PathBuf {
    let parent = file.parent().unwrap_or(Path::new(".")).to_path_buf();
    if is_crate_root {
        return parent;
    }
    let stem = file
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    if stem == "mod" {
        parent
    } else {
        parent.join(stem)
    }
}

/// Every file reachable from `roots` by following `mod` declarations.
///
/// A declaration that resolves to no file is skipped rather than reported: a `mod`
/// naming a missing file is a compile error, and this gate is about the files that are
/// there and unreachable, not about declarations that are broken.
fn reachable(roots: &BTreeSet<PathBuf>) -> BTreeSet<PathBuf> {
    let mut seen: BTreeSet<PathBuf> = BTreeSet::new();
    let mut stack: Vec<(PathBuf, bool)> = roots.iter().cloned().map(|r| (r, true)).collect();
    while let Some((file, is_root)) = stack.pop() {
        if !seen.insert(file.clone()) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let dir = child_dir(&file, is_root);
        for name in declared_modules(&text) {
            let flat = dir.join(format!("{name}.rs"));
            let nested = dir.join(&name).join("mod.rs");
            let child = if flat.is_file() {
                flat
            } else if nested.is_file() {
                nested
            } else {
                continue;
            };
            if !seen.contains(&child) {
                stack.push((child, false));
            }
        }
    }
    seen
}

/// A path as the report spells it: relative to the repository root, with `/`.
fn repo_relative(path: &Path) -> String {
    let root = repo_root();
    path.strip_prefix(&root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Every `.rs` the repository carries under a workspace member's `src/` reaches a build
/// target by a `mod` declaration.
///
/// There is no exemption list: the assertion is the rule itself, in one direction. A
/// file that reaches nothing fails the build with its own name in the message, which is
/// the moment to either declare it where it belongs or delete it.
#[test]
fn every_rust_source_a_crate_carries_reaches_a_build_target() {
    let members = member_roots();
    assert!(
        !members.is_empty(),
        "cargo metadata resolved no workspace members under crates/, so there is nothing to \
         start a module walk from and this gate would pass on nothing"
    );

    let mut roots: BTreeSet<PathBuf> = BTreeSet::new();
    for roots_of in members.values() {
        roots.extend(roots_of.iter().cloned());
    }
    assert!(
        !roots.is_empty(),
        "no member under crates/ named a lib, bin or proc-macro source, so the walk has no root \
         and every file would read as unreachable"
    );

    // The universe is a member's `src/` as the repository carries it. Reading the walk's
    // own roots back out of it is the anti-vacuity reading: if the metadata paths and
    // the carried paths disagree about spelling, the two sets would be disjoint and the
    // whole tree would read as unreached, which is a broken gate and not a finding.
    let mut universe: BTreeSet<PathBuf> = BTreeSet::new();
    for source in carried_sources() {
        if members
            .keys()
            .any(|dir| source.starts_with(dir.join("src")))
        {
            universe.insert(source);
        }
    }
    let absent: Vec<String> = roots
        .iter()
        .filter(|root| !universe.contains(*root))
        .map(|root| repo_relative(root))
        .collect();
    assert!(
        absent.is_empty(),
        "the compiler's roots are not files the repository carries: {absent:?} — the metadata and \
         the index agree about where a crate starts, so a root missing from one of them means the \
         walk is reading a different tree than the build does"
    );

    let reached = reachable(&roots);
    let unwired: Vec<String> = universe
        .iter()
        .filter(|source| !reached.contains(*source))
        .map(|source| repo_relative(source))
        .collect();
    assert!(
        unwired.is_empty(),
        "these source files reach no build target: {unwired:?} — a `.rs` under a crate's `src/` \
         that no `mod` declaration reaches is compiled by nothing, so it reads as landed while the \
         compiler has never seen it. Either declare it where it belongs or delete it"
    );
}
