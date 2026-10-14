//! The actor label on the LLM series is a closed set, and this is its census.
//!
//! `x-cogneva-actor` is what the cost and quota views group by, so a value that
//! reaches the label without being declared is a series nobody decided to carry
//! — and on the reading side it looks exactly like a feature someone meant to
//! add. The gateway now admits only declared values
//! (`cog_gateway::actor_labels::declared`), which closes the vocabulary at the
//! edge; this test closes it at the source, over the call sites that write the
//! header.
//!
//! What it judges: every actor literal the tree writes is declared, every
//! declared value is one the tree actually writes (so a declaration cannot
//! outlive its caller), and the set of files that write the header is exactly
//! the inventory below — a new call site anywhere has to be listed, with the
//! actor it reports under, before it can land.
//!
//! What it cannot judge, in the open: a call site that forgets the actor
//! entirely writes no literal, so no text census can see it. That case is
//! readable only at run time, as traffic in the `actor="unknown"` cell, and the
//! reader for that cell is not this test. This is the same division the closed
//! sets elsewhere in the platform use: the census bounds the vocabulary, the
//! counter shows the omissions.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use cog_gateway::actor_labels::{declared, ACTOR_LABELS, ACTOR_LABEL_PREFIXES, UNKNOWN_ACTOR};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Files that write the actor header, and the actor each reports under. Kept as
/// an inventory rather than derived, so that adding a call site is a decision
/// someone makes here instead of something that just happens.
const PRODUCERS: &[(&str, &[&str])] = &[
    ("crates/cog-agent/src/agent.rs", &["self_review:{role}"]),
    (
        "crates/cog-agent/src/agent_kernel/runtime.rs",
        &["agent:{role}"],
    ),
    (
        "crates/cog-agent/src/self_review.rs",
        &["self_review", "self_review:{role}"],
    ),
    (
        "crates/cog-collaboration/src/collaboration_executor.rs",
        &["intent_assess"],
    ),
    (
        "crates/cog-collaboration/src/squad/ralph/ralph_loop.rs",
        &["ralph"],
    ),
    ("crates/cog-eval/src/adapters/hle.rs", &["eval"]),
    ("crates/cog-eval/src/judge.rs", &["eval"]),
    (
        "crates/cog-extension/src/hostdocs_assist.rs",
        &["sandbox-document-organizer"],
    ),
    (
        "crates/cog-gateway/src/chat.rs",
        &["intent_gateway", "web_chat"],
    ),
    ("crates/cog-llm/src/origin.rs", &["agent:generator"]),
    ("crates/cog-memory/src/extractor.rs", &["memory"]),
    ("crates/cog-reflection/src/evolution.rs", &["evolution"]),
    (
        "crates/cog-reflection/src/extractor.rs",
        &["skill_extractor"],
    ),
    ("crates/cog-wiki/src/llm_maintainer.rs", &["wiki"]),
    (
        "crates/cogneva/tests/collaboration_self_review_test.rs",
        &["self_review:{role}"],
    ),
];

/// Files whose actor is not a value written at the call site: the value is
/// threaded in from the caller that built the loop.
const THREADED: &[(&str, &str)] = &[
    (
        "crates/cog-agent/src/agent.rs",
        "reports under the self-review role the agent itself is configured with",
    ),
    (
        "crates/cog-agent/src/self_review.rs",
        "each step takes the actor the loop was built with",
    ),
];

/// Files the census does not read, and why.
const EXCLUDED: &[(&str, &str)] = &[
    (
        "crates/cog-core/src/contract/llm.rs",
        "it defines the header and the setter; the value it writes is the caller's",
    ),
    (
        "crates/cogneva/tests/llm_actor_label_contract_test.rs",
        "this census, whose samples are not call sites",
    ),
];

/// Declared values that no call site writes directly, and what writes them.
const NOT_WRITTEN_AT_A_CALL_SITE: &[(&str, &str)] = &[(
    "self_review",
    "the default a self-review loop starts with when its caller names no actor",
)];

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Site {
    /// A literal written at the call site.
    Literal(String),
    /// A role family, written as a format string whose first literal is the
    /// declared prefix.
    Family(String),
    /// An expression the census cannot evaluate; its file has to be in
    /// `THREADED`.
    Unresolved(String),
}

/// Every site in one file's text that writes an actor value.
fn sites_in(text: &str) -> Vec<Site> {
    let consts = const_strings(text);
    let mut sites = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find(".with_actor(") {
        rest = &rest[at + ".with_actor(".len()..];
        let Some(arg) = argument(rest) else { continue };
        sites.push(classify(arg, &consts));
    }
    // The header written directly rather than through `with_actor`.
    for line in text.lines() {
        if !line.contains("LLM_ACTOR_HEADER")
            || !(line.contains(".header(") || line.contains(".insert("))
        {
            continue;
        }
        let after = &line[line.rfind("LLM_ACTOR_HEADER").unwrap() + "LLM_ACTOR_HEADER".len()..];
        if let Some(arg) = after.split_once(',').map(|(_, arg)| arg.trim()) {
            let arg = arg.trim_end_matches(')').trim();
            sites.push(classify(arg, &consts));
        }
    }
    sites.sort();
    sites
}

/// The text of the first argument of a call whose `(` has just been consumed,
/// up to its matching `)`.
fn argument(rest: &str) -> Option<&str> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, b) in rest.bytes().enumerate() {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'(' => depth += 1,
            b')' if depth == 0 => return Some(rest[..i].trim()),
            b')' => depth -= 1,
            _ => {}
        }
    }
    None
}

/// `const NAME: &str = "value";` in one file, so that a site naming a constant
/// is judged on the value the constant holds.
fn const_strings(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        let Some(rest) = line
            .strip_prefix("const ")
            .or_else(|| line.strip_prefix("pub const "))
        else {
            continue;
        };
        let Some((name, tail)) = rest.split_once(':') else {
            continue;
        };
        let Some(value) = tail.split('"').nth(1) else {
            continue;
        };
        out.insert(name.trim().to_string(), value.to_string());
    }
    out
}

fn classify(arg: &str, consts: &BTreeMap<String, String>) -> Site {
    if let Some(lit) = arg.strip_prefix('"').and_then(|a| a.split('"').next()) {
        return Site::Literal(lit.to_string());
    }
    // A role family, written as `format!("agent:{}", role)` or concatenated.
    if arg.contains("format!(") || arg.contains(" + ") {
        if let Some(prefix) = arg.split('"').nth(1).and_then(|l| l.split('{').next()) {
            return Site::Family(prefix.to_string());
        }
    }
    // The same family, built by the helper the roles share.
    if arg.contains("self_review_actor(") {
        return Site::Family("self_review:".to_string());
    }
    let bare = arg.rsplit("::").next().unwrap_or(arg).trim();
    if let Some(value) = consts.get(bare.trim_end_matches(".to_string()")) {
        return Site::Literal(value.clone());
    }
    Site::Unresolved(arg.to_string())
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|n| n == "target" || n == ".git")
            {
                continue;
            }
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The census, over the working tree. Returns the sites by file.
fn census(root: &Path) -> BTreeMap<String, Vec<Site>> {
    let mut files = Vec::new();
    rust_files(&root.join("crates"), &mut files);
    let mut found = BTreeMap::new();
    for file in files {
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .display()
            .to_string();
        if EXCLUDED.iter().any(|(path, _)| *path == rel) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let sites = sites_in(&text);
        if !sites.is_empty() {
            found.insert(rel, sites);
        }
    }
    found
}

#[test]
fn the_header_is_written_by_exactly_the_files_that_declare_an_actor() {
    let root = repo_root();
    let found = census(&root);
    let expected: BTreeSet<&str> = PRODUCERS.iter().map(|(path, _)| *path).collect();
    let actual: BTreeSet<&str> = found.keys().map(String::as_str).collect();
    let added: Vec<_> = actual.difference(&expected).collect();
    let gone: Vec<_> = expected.difference(&actual).collect();
    assert!(
        added.is_empty(),
        "these files write the actor header but are not in the inventory: {added:?}\n\
         Add them with the actor they report under, or give the call site an actor."
    );
    assert!(
        gone.is_empty(),
        "these files are in the inventory but no longer write the actor header: {gone:?}\n\
         Drop them, so the inventory keeps naming the real writers."
    );
}

#[test]
fn every_actor_the_tree_writes_is_declared() {
    let found = census(&repo_root());
    let mut complaints = Vec::new();
    for (file, sites) in &found {
        for site in sites {
            match site {
                Site::Literal(value) if !declared(value) => {
                    complaints.push(format!("{file} writes the undeclared actor {value:?}"));
                }
                Site::Family(prefix) if !ACTOR_LABEL_PREFIXES.contains(&prefix.as_str()) => {
                    complaints.push(format!(
                        "{file} builds an actor from the undeclared family {prefix:?}"
                    ));
                }
                _ => {}
            }
        }
    }
    assert!(complaints.is_empty(), "{}", complaints.join("\n"));
}

#[test]
fn every_declared_actor_is_one_the_tree_writes() {
    let found = census(&repo_root());
    let mut written: BTreeSet<String> = BTreeSet::new();
    for sites in found.values() {
        for site in sites {
            match site {
                Site::Literal(value) => {
                    written.insert(value.clone());
                }
                Site::Family(prefix) => {
                    // A family is written by whoever formats it; the sample
                    // stands in for the roles configuration supplies.
                    written.insert(format!("{prefix}planner"));
                }
                Site::Unresolved(_) => {}
            }
        }
    }
    let exempt: BTreeSet<&str> = NOT_WRITTEN_AT_A_CALL_SITE
        .iter()
        .map(|(value, _)| *value)
        .collect();
    let orphans: Vec<&str> = ACTOR_LABELS
        .iter()
        .copied()
        .filter(|value| !written.contains(*value) && !exempt.contains(value))
        .collect();
    assert!(
        orphans.is_empty(),
        "these actors are declared but nothing writes them: {orphans:?}\n\
         A declaration that outlives its caller is a value the gateway will keep \
         admitting while no traffic can carry it."
    );
    for (value, reason) in NOT_WRITTEN_AT_A_CALL_SITE {
        assert!(
            ACTOR_LABELS.contains(value),
            "{value} is exempted from being written but is not declared ({reason})"
        );
        assert!(
            !written.contains(*value),
            "{value} is written now; drop the exemption ({reason})"
        );
    }
    assert!(declared(UNKNOWN_ACTOR));
}

#[test]
fn a_threaded_actor_is_named_as_threaded() {
    let found = census(&repo_root());
    let expected: BTreeSet<&str> = THREADED.iter().map(|(path, _)| *path).collect();
    let actual: BTreeSet<&str> = found
        .iter()
        .filter(|(_, sites)| sites.iter().any(|s| matches!(s, Site::Unresolved(_))))
        .map(|(file, _)| file.as_str())
        .collect();
    assert_eq!(
        actual, expected,
        "the files whose actor the census cannot evaluate are not the ones listed as threaded"
    );
}

#[test]
fn the_census_reports_an_undeclared_actor() {
    // The reverse control: a reader that cannot tell a declared actor from an
    // undeclared one would pass the census above by never complaining.
    let sample = r#"ChatOptions::default().with_actor("planner")"#;
    let sites = sites_in(sample);
    assert_eq!(sites, vec![Site::Literal("planner".to_string())]);
    assert!(!declared("planner"), "the sample has to be undeclared");

    // And a declared one is not reported, so the reader is not merely always red.
    let ok = sites_in(r#"ChatOptions::default().with_actor("ralph")"#);
    assert!(matches!(ok.as_slice(), [Site::Literal(v)] if v == "ralph"));
    assert!(declared("ralph"));
}
