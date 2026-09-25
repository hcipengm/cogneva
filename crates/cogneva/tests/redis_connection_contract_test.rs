//! Gate: every redis connection in this workspace is replaceable and bounded.
//!
//! Two failures this exists for, both of which cost hours in the cluster:
//!
//! 1. **A bare multiplexed connection never replaces a socket that died.** Once
//!    the server side goes away, every later command is issued on the same dead
//!    socket and fails with the same I/O error, so the read loop retries forever
//!    and its owner stops making progress while reporting nothing but "broken
//!    pipe". On 2026-09-25 an evolution pod sat in exactly that state for hours
//!    with zero clients left on the server, while the message plane it consumed
//!    from went unread. Nothing crashes, no health check fails, and the log line
//!    looks like ordinary retry noise.
//!
//! 2. **The replacement manager's default constructor stalls for minutes.** It
//!    hands the driver's `factor` (milliseconds) to a backoff builder that reads
//!    it as a multiplier, on top of a one-second floor and no ceiling, so an
//!    unreachable address is retried for minutes before an error comes back —
//!    measured, see `cog_redis`'s ignored test. Almost every call site here
//!    treats "redis is not reachable" as a reason to degrade, and degrading
//!    cannot be measured in minutes: the startup path stops reporting progress
//!    instead of failing.
//!
//! Neither defect is visible in review, because both are about *which
//! constructor* a line calls, and both are invisible at runtime until they bite.
//! So they are asserted over the sources.
//!
//! The shared constructor in `cog_redis` is the one approved way: a manager that
//! replaces a dead socket, within a budget. Both rules are scoped to what the
//! failure actually costs: the first is about a connection that has to keep
//! working for hours and is checked over production code, the second is about a
//! call that hangs for minutes and is checked everywhere.
//!
//! Note on the literals below: they are assembled at compile time, so that this
//! file — which is itself one of the sources scanned — never contains the text it
//! forbids. Otherwise the gate's first finding would be itself.

use std::path::{Path, PathBuf};

/// The bare connection type, and the constructor that hands one out.
const BARE_TYPE: &str = concat!("Multiplexed", "Connection");
const BARE_CONSTRUCTOR: &str = concat!("get_multiplexed_", "async_connection");

/// The manager's default constructor, which retries for minutes.
const UNBOUNDED_CALL: &str = concat!("Connection", "Manager::new(");
/// The same call written with its module path.
const UNBOUNDED_QUALIFIED: &str = concat!("redis::aio::Connection", "Manager::new(");
/// How the redis manager is imported. An unqualified call only means the driver
/// when this is present: the workspace also has a websocket registry of the same
/// name, whose constructor is not a redis connection at all.
const MANAGER_IMPORT: &str = concat!("aio::Connection", "Manager");

/// The approved way to build a managed connection.
const APPROVED: &str = concat!("cog_redis::", "connect");

/// The one file allowed to name the unbounded constructor: `cog_redis` records
/// the driver's stall as a runnable test, which is the evidence for its own
/// budget. The exemption is checked below, so renaming or deleting that file
/// fails this gate instead of quietly widening it.
const RECORDING_FILE: &str = "crates/cog-redis/src/lib.rs";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cogneva")
        .to_path_buf()
}

/// Everything up to the first test-only item, for the rule that is about
/// production code.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// The bare-type rule, as a pure function of one source file's text.
fn holds_a_bare_connection(source: &str) -> bool {
    source.contains(BARE_TYPE) || source.contains(BARE_CONSTRUCTOR)
}

/// The bounded-construction rule, as a pure function of one file's text.
fn builds_an_unbounded_manager(source: &str) -> bool {
    source.contains(UNBOUNDED_QUALIFIED)
        || (source.contains(MANAGER_IMPORT) && source.contains(UNBOUNDED_CALL))
}

/// A file under `crates/<crate>/tests/`, whose whole body is fixture code. A
/// fixture stands in for an outside client: it is short-lived, and it reconnects
/// by being re-run, so a bare connection there is not the failure rule 1 is about.
fn is_integration_test(relative: &str) -> bool {
    relative.split('/').nth(2) == Some("tests")
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every `.rs` file under `crates/<crate>/{src,tests}`, keyed by
/// workspace-relative path.
fn scan() -> Vec<(String, String)> {
    let root = workspace_root();
    let mut found = Vec::new();
    for crate_dir in std::fs::read_dir(root.join("crates"))
        .expect("workspace has a crates directory")
        .flatten()
    {
        for sub in ["src", "tests"] {
            let dir = crate_dir.path().join(sub);
            if !dir.is_dir() {
                continue;
            }
            let mut sources = Vec::new();
            rust_sources(&dir, &mut sources);
            for path in sources {
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                let relative = path
                    .strip_prefix(&root)
                    .expect("scanned path is under the workspace root")
                    .to_string_lossy()
                    .replace('\\', "/");
                found.push((relative, text));
            }
        }
    }
    found
}

#[test]
fn the_detectors_see_both_shapes_and_leave_the_other_ones_alone() {
    // Without this, a rename that made the rules vacuous would leave the scans
    // below green while enforcing nothing.
    assert!(holds_a_bare_connection(concat!(
        "let conn = client.get_multiplexed_",
        "async_connection().await?;"
    )));
    assert!(holds_a_bare_connection(concat!(
        "conn: redis::aio::Multiplexed",
        "Connection,"
    )));
    assert!(!holds_a_bare_connection(concat!(
        "let conn = cog_redis::",
        "connect(&client).await?;"
    )));
    // The cut stops at the test module, so a fixture inside a production file is
    // not a finding.
    assert!(!holds_a_bare_connection(production_source(concat!(
        "let conn = make();\n#[cfg(test)]\nmod tests { fn f(c: Multiplexed",
        "Connection) {} }"
    ))));

    // The unbounded constructor: both spellings, and not its bounded sibling.
    assert!(builds_an_unbounded_manager(concat!(
        "use redis::aio::Connection",
        "Manager;\nlet conn = Connection",
        "Manager::new(client).await?;"
    )));
    assert!(builds_an_unbounded_manager(concat!(
        "let conn = redis::aio::Connection",
        "Manager::new(client).await?;"
    )));
    assert!(!builds_an_unbounded_manager(
        "ConnectionManager::new_with_config(client, config)"
    ));
    // The websocket registry's own manager is a different type: a file that
    // never imports the redis one is not building a redis connection.
    assert!(!builds_an_unbounded_manager(concat!(
        "pub struct Connection",
        "Manager { connections: Vec<u8> }\nlet m = Connection",
        "Manager::new();"
    )));
}

#[test]
fn the_recorded_evidence_file_still_exists() {
    // The exemption for the unbounded constructor is narrow, and this is what
    // keeps it narrow: the file it names has to be there, and has to actually
    // contain the thing it is exempted for.
    let path = workspace_root().join(RECORDING_FILE);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{RECORDING_FILE} is exempted but unreadable: {e}"));
    assert!(
        builds_an_unbounded_manager(&text),
        "{RECORDING_FILE} is exempted from the rule below but no longer builds the \
         driver's default manager; drop the exemption instead of leaving it stale"
    );
}

#[test]
fn no_production_source_holds_a_bare_multiplexed_connection() {
    let mut bare = Vec::new();
    let mut approved = Vec::new();
    for (relative, text) in scan() {
        if text.contains(APPROVED) {
            approved.push(relative.clone());
        }
        if is_integration_test(&relative) {
            continue;
        }
        if holds_a_bare_connection(production_source(&text)) {
            bare.push(relative);
        }
    }
    assert!(
        approved.len() > 1,
        "the scan found no source using {APPROVED} at all, so the rule below is \
         vacuous — check that the walk still reaches the crates: {approved:?}"
    );
    assert!(
        bare.is_empty(),
        "these sources hold a connection that is never replaced when its socket \
         dies, so their owner stops making progress and only logs I/O errors: {bare:?}. \
         Use {APPROVED} instead — it swaps the socket on an I/O error and surfaces that \
         one error to the caller, so an existing retry recovers and no command is issued \
         twice.",
    );
}

#[test]
fn no_source_builds_a_manager_that_retries_for_minutes() {
    let mut unbounded = Vec::new();
    for (relative, text) in scan() {
        if relative == RECORDING_FILE {
            continue;
        }
        if builds_an_unbounded_manager(&text) {
            unbounded.push(relative);
        }
    }
    assert!(
        unbounded.is_empty(),
        "these sources build a connection manager with the driver's defaults, which \
         retry an unreachable address for minutes before returning an error (its \
         `factor` is milliseconds, handed to a multiplier): {unbounded:?}. \
         Use {APPROVED}: the same self-healing socket replacement, bounded by a budget.",
    );
}
