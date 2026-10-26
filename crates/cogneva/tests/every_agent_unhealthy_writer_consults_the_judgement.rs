//! principle: feedback_the_half_that_is_easy_to_enumerate_is_not_the_half_that_fires
//! principle: feedback_a_gate_at_one_of_n_writers_is_no_gate_at_all
//!
//! `AgentUnhealthy` has more than one writer, and a filter installed at one of
//! them moved nothing.
//!
//! The health pass decides which ids are worth reporting and which have already
//! left the fleet; `HealthReport::announces` is that one answer. But the event
//! is constructed in two places -- the supervisor's own pass, and the lifecycle
//! coordinator, which sends on the same channel and runs *before* it. A first
//! attempt filtered only the supervisor's tail: the coordinator's copy still
//! refreshed every departed id's row each pass, so the alert bridge kept
//! finding them fresh, the sweep never collected them, and the live reading was
//! exactly as wrong as before the change. The gate was green and the fix had no
//! effect.
//!
//! What makes that failure hard to catch is that it is invisible from the
//! outside: the code that was edited does what it says, every test of it
//! passes, and the only place the truth shows up is a firing count that takes a
//! deploy plus a sweep to read. So this test compares the two populations
//! directly -- every site that *constructs* the event, and the files that
//! consult the judgement -- instead of trusting that whoever adds the next
//! writer remembers.
//!
//! Two legs:
//!
//! 1. The set of production construction sites, named by file and enclosing
//!    function, is exactly the list declared below. A new writer makes this red
//!    and asks the question at the moment it is cheap to answer: does it read
//!    `announces` first?
//! 2. Every file that constructs the event also contains a call to
//!    `announces(`. This is a file-level check and its reach is exactly that:
//!    it fires when a file loses the judgement *entirely*, which is the shape
//!    of the bug above -- one writer guarded, the other file's writer never
//!    asking at all.
//!
//!    It does not fire when one of several guards inside the same file is
//!    deleted, and it must not be read as if it did. The coordinator guards
//!    three branches separately; with two deleted, the survivor still names
//!    `announces(`. Per-site coverage therefore lives where it can actually see
//!    a branch: the behavioural tests in
//!    `supervisor_lifecycle_coordinator_test.rs` hold one leg per branch (each
//!    ablation turns exactly its own test red), and the supervisor's own
//!    event-stream test holds its single site. A textual test cannot stand in
//!    for those, and pretending it does would make this file's silence mean
//!    more than it does.
//!
//! The ceiling: this parses source, so a writer built through a macro, an
//! `include!`, or a helper that takes the event as an argument has no
//! construction site here to find. It also stops at the first `#[cfg(test)]` in
//! each file, because test fixtures construct the event on purpose and are not
//! writers of anything. Both known constructions use field-init syntax
//! (`agent_id:`); match arms bind by shorthand (`agent_id,`), which is how the
//! two are told apart. Each declared site is required to be found, so a
//! refactor that moves a construction out of the shape this test reads fails
//! loudly instead of passing on an empty population.

use std::path::{Path, PathBuf};

/// Every production site that constructs `SupervisorEvent::AgentUnhealthy`,
/// as `path::enclosing_function`.
///
/// Being on this list is not a claim that the guard lives *inside* the function
/// -- the coordinator's emitter is guarded by its callers. It is a claim that
/// this site was looked at and the judgement is consulted somewhere on the way.
const DECLARED_WRITERS: &[&str] = &[
    "crates/cog-supervisor/src/lifecycle_coordinator.rs::emit_lifecycle_event",
    "crates/cog-supervisor/src/supervisor.rs::announce_verdicts",
];

/// The one function that decides whether a verdict is news. A file that writes
/// the event without ever naming it is writing without asking.
const JUDGEMENT: &str = "announces(";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every `.rs` file under `crates/*/src/`, recursively.
fn production_sources() -> Vec<PathBuf> {
    let crates_dir = repo_root().join("crates");
    let mut out = Vec::new();
    let mut stack = vec![crates_dir];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => panic!("{} unreadable: {e}", dir.display()),
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let src = path.join("src");
                if src.is_dir() {
                    collect_rs(&src, &mut out);
                }
            }
        }
    }
    out.sort();
    out
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{} unreadable: {e}", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn relative(path: &Path) -> String {
    path.strip_prefix(repo_root())
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// The lines before this file's first `#[cfg(test)]`, which is where test
/// fixtures start. Test modules built with `#[path]` or a separate file have no
/// marker here, so their fixtures are not counted either.
fn production_lines(source: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = Vec::new();
    for line in source.lines() {
        if line.trim_start().starts_with("#[cfg(test)]") {
            break;
        }
        lines.push(line);
    }
    lines
}

/// The nearest function above `line_index`, which is where the construction
/// sits. `None` if the line is not inside one.
fn enclosing_function(lines: &[&str], line_index: usize) -> Option<String> {
    for line in lines[..line_index].iter().rev() {
        let trimmed = line.trim_start();
        let prefixed = trimmed
            .strip_prefix("pub async fn ")
            .or_else(|| trimmed.strip_prefix("pub(crate) async fn "))
            .or_else(|| trimmed.strip_prefix("async fn "))
            .or_else(|| trimmed.strip_prefix("pub fn "))
            .or_else(|| trimmed.strip_prefix("pub(crate) fn "))
            .or_else(|| trimmed.strip_prefix("fn "));
        let Some(after_prefix) = prefixed else {
            continue;
        };
        let name: String = after_prefix
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

/// Construction sites in one file, as `path::function`.
fn writers_in(path: &Path) -> Vec<String> {
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => panic!("{} unreadable: {e}", path.display()),
    };
    let lines = production_lines(&source);
    let mut sites = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        // Only the qualified form is a construction. The variant's own
        // declaration in the event enum is written bare (`AgentUnhealthy {`)
        // and its fields are types, which read exactly like initialisers.
        if !line.contains("SupervisorEvent::AgentUnhealthy {") {
            continue;
        }
        // A construction initialises fields (`agent_id:`); a match arm binds
        // them by shorthand (`agent_id,`). Only the first block is read, which
        // is enough: both shapes are written one field per line.
        let block = &lines[i..lines.len().min(i + 8)];
        let constructs = block
            .iter()
            .take_while(|l| !l.contains('}'))
            .any(|l| l.contains("agent_id:"));
        if !constructs {
            continue;
        }
        let function = enclosing_function(&lines, i).unwrap_or_else(|| {
            panic!(
                "{}:{}: writer with no enclosing function",
                relative(path),
                i + 1
            )
        });
        sites.push(format!("{}::{}", relative(path), function));
    }
    sites
}

#[test]
fn every_writer_of_agent_unhealthy_is_the_one_that_was_reviewed() {
    let mut found = Vec::new();
    for path in production_sources() {
        found.extend(writers_in(&path));
    }
    found.sort();

    let mut declared: Vec<String> = DECLARED_WRITERS.iter().map(|s| s.to_string()).collect();
    declared.sort();

    assert!(
        !found.is_empty(),
        "no construction site found at all: the scan is not reading what it thinks it is, \
         so its silence says nothing about the writers"
    );
    assert_eq!(
        found, declared,
        "a writer of `AgentUnhealthy` appeared or moved. Before changing this list, answer the \
         question the last one cost a deploy to learn: does it consult `HealthReport::announces` \
         first? A writer that does not keeps departed ids looking fresh, and the sweep that would \
         have collected their rows never fires."
    );
}

#[test]
fn a_file_that_writes_the_event_also_asks_whether_it_may_announce() {
    let mut ungated = Vec::new();
    for path in production_sources() {
        if writers_in(&path).is_empty() {
            continue;
        }
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
        let production = production_lines(&source).join("\n");
        if !production.contains(JUDGEMENT) {
            ungated.push(relative(&path));
        }
    }
    assert!(
        ungated.is_empty(),
        "these files write `AgentUnhealthy` without ever consulting the judgement: {ungated:?}. \
         The supervisor and the lifecycle coordinator both send on the same channel, and the \
         coordinator runs first -- a guard on one of them leaves the other refreshing rows for \
         agents that already left."
    );
}
