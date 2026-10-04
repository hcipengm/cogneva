//! Gate: every task state a task can rest in has a code path that revisits it.
//!
//! A state whose only exit is an external event — the ready message being
//! consumed, the agent it was handed to answering — has no owner inside any
//! process. When that event happens and the state does not move, nothing is
//! looking, and the state it rests in is indistinguishable from an idle
//! system: no error, no series going quiet, nothing to alert on. Fifty-seven
//! tasks sat in `Scheduled` for two days that way while the ready stream read
//! clean, because the publisher scanned `Pending`, the timeout checker
//! reclaimed `Running`, and no rule read a task state at all.
//!
//! That is not a mistake a reviewer catches either. A new state is one more
//! variant in an enum, and whether something revisits it is a property of code
//! elsewhere; the omission costs nothing until the day the state occurs. So the
//! requirement is asserted over the sources: each state is either terminal —
//! an end nothing has to revisit — or names the function that revisits it, the
//! file that defines that function, and a file that calls it.
//!
//! What this checks is reachability, not correctness: that the sweep exists and
//! is called from production code. Whether the sweep leaves a reading of its own
//! is the other half and is enforced where readings are — a reclaimer that
//! reports through a series has that series in the closed name set and, if a
//! deployed rule reads it, in the alert-rule contract.
//!
//! The classification is exhaustive over [`TaskStatus::ALL`], so a variant added
//! later fails here until someone writes down which of the two it is. Marking a
//! state terminal is a real answer only when the reason says why nothing has to
//! return to it; that is what the prose in the tables is for.

use cog_core::TaskStatus;
use std::path::{Path, PathBuf};

/// A state that nothing has to revisit, with why it is an end.
const TERMINAL: &[(TaskStatus, &str)] = &[
    (
        TaskStatus::Completed,
        "the work finished; the task's dependents are released by the completion path and \
         nothing waits on the task itself",
    ),
    (
        TaskStatus::Failed,
        "the retry budget was spent; the failure path cancels the dependents that will now \
         never run and pushes the task to the dead-letter queue",
    ),
    (
        TaskStatus::Cancelled,
        "the task was cancelled, by its own cancel path or as a dependent of a failure, and \
         a cancelled task is deliberately not run again",
    ),
];

/// A non-terminal state and the production code that revisits it.
struct Revisited {
    status: TaskStatus,
    /// The function that walks the state and moves what it finds out of it.
    reviser: &'static str,
    /// Where that function is defined, relative to the workspace root.
    defined_in: &'static str,
    /// A file that calls it outside of tests.
    called_from: &'static str,
    /// What the state waits for, and what revisits it when the wait does not end.
    reason: &'static str,
}

/// The revisers, one per non-terminal state.
const REVISITED: &[Revisited] = &[
    Revisited {
        status: TaskStatus::Pending,
        reviser: "find_ready_tasks",
        defined_in: "crates/cog-orchestrator/src/dag_executor/orchestrator.rs",
        called_from: "crates/cog-orchestrator/src/dag_executor/mod.rs",
        reason: "waits for a worker to be handed the task; the publisher's tick scans Pending \
                 and writes each ready task to the ready stream, so a pending task is picked \
                 up again on the next tick without anything external having to arrive",
    },
    Revisited {
        status: TaskStatus::Scheduled,
        reviser: "reclaim_stalled_scheduled",
        defined_in: "crates/cog-orchestrator/src/dag_executor/orchestrator.rs",
        called_from: "crates/cog-orchestrator/src/plugin.rs",
        reason: "waits for the ready message that put it here to be consumed; the message can \
                 be acked without the task ever starting, and the publisher's tick puts the \
                 task back in line without charging an attempt — the age it reads is the same \
                 whether the message was queued or lost, so only the consumer that sees the \
                 acknowledgement may charge, and it does that where the ack happens",
    },
    Revisited {
        status: TaskStatus::Running,
        reviser: "check_timeouts",
        defined_in: "crates/cog-orchestrator/src/dag_executor/orchestrator.rs",
        called_from: "crates/cog-gateway/src/executor.rs",
        reason: "waits for its owner to finish or to renew the lease; the sweep fails a task \
                 whose lease expired or that ran past its budget, which is the only way a task \
                 whose owner died is ever taken out of this state",
    },
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cogneva")
        .to_path_buf()
}

/// Everything up to the first test-only item: a call inside a test module is not
/// production reachability.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// Whether this source defines `name` as a function.
fn defines(production: &str, name: &str) -> bool {
    production.contains(&format!("fn {name}("))
}

/// Whether this source calls `name`.
fn calls(production: &str, name: &str) -> bool {
    production.contains(&format!(".{name}(")) || production.contains(&format!("{name}("))
}

/// Read a source below the workspace root, panicking with the path when it is
/// missing — a renamed file has to fail here rather than make the check vacuous.
fn source_at(root: &Path, relative: &str) -> String {
    std::fs::read_to_string(root.join(relative)).unwrap_or_else(|e| {
        panic!("{relative} cannot be read ({e}); a table entry points at a file that moved")
    })
}

#[test]
fn the_scan_reads_what_it_claims_to() {
    // Without this, a rename that made the scan vacuous would leave the checks
    // below green while enforcing nothing.
    assert!(defines(
        "pub async fn find_ready_tasks(&self) {}",
        "find_ready_tasks"
    ));
    assert!(!defines(
        "// find_ready_tasks( is mentioned in a comment",
        "find_ready_tasks"
    ));
    // The `fn` has to be a definition, not a call: a table entry pointing at a
    // file that only calls the name would otherwise pass.
    assert!(!defines(
        "self.find_ready_tasks().await;",
        "find_ready_tasks"
    ));
    assert!(calls("self.check_timeouts().await;", "check_timeouts"));
    assert!(calls("dag.find_ready_tasks().await", "find_ready_tasks"));
    // A call in a test module is not production reachability.
    assert!(!calls(
        production_source("fn f() {}\nmod tests { fn t() { dag.find_ready_tasks().await; } }"),
        "find_ready_tasks"
    ));

    let root = workspace_root();
    for row in REVISITED {
        let def = source_at(&root, row.defined_in);
        assert!(
            defines(production_source(&def), row.reviser),
            "{}: no `fn {}` here, so nothing revisits `{}` any more",
            row.defined_in,
            row.reviser,
            row.status.as_str()
        );
        let caller = source_at(&root, row.called_from);
        assert!(
            calls(production_source(&caller), row.reviser),
            "{} does not call {} outside of tests, so `{}` is left with no reviser in \
             production",
            row.called_from,
            row.reviser,
            row.status.as_str()
        );
    }
}

#[test]
fn every_state_is_terminal_or_revisited() {
    let mut classified: Vec<(TaskStatus, bool)> = Vec::new();
    for (status, _) in TERMINAL {
        classified.push((*status, true));
    }
    for row in REVISITED {
        classified.push((row.status, false));
    }

    for status in TaskStatus::ALL {
        let matches: Vec<bool> = classified
            .iter()
            .filter(|(s, _)| *s == status)
            .map(|(_, terminal)| *terminal)
            .collect();
        match matches.as_slice() {
            [] => panic!(
                "{} is classified by neither table: name the function that revisits it, or \
                 write down why nothing has to",
                status.as_str()
            ),
            [_] => {}
            _ => panic!(
                "{} is classified twice; the two tables have to partition the states",
                status.as_str()
            ),
        }
    }

    // The reverse direction: a row for a status this version does not have is a
    // table that has drifted from the enum.
    for (status, _) in &classified {
        assert!(
            TaskStatus::ALL.contains(status),
            "a table names {} , which TaskStatus no longer has",
            status.as_str()
        );
    }

    // A state nobody has to revisit is a claim that needs a reason, and the
    // reason is what tells the next reader whether it still holds.
    for (status, reason) in TERMINAL {
        assert!(
            reason.trim().len() >= 60,
            "{}: the reason is too short to be one — say why nothing has to return to it",
            status.as_str()
        );
    }
    for row in REVISITED {
        assert!(
            row.reason.trim().len() >= 60,
            "{}: say what the state waits for and what revisits it",
            row.status.as_str()
        );
    }
}
