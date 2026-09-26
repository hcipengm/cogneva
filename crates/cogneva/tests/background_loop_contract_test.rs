//! Gate: every background loop is either watchable or listed here with a reason.
//!
//! A background loop is a bare spawn around a ticker: when it dies, the series it
//! published go quiet, and "quiet" is what a healthy subsystem with nothing to do
//! also looks like. The liveness family in `cog_core::loop_health` is what makes
//! the difference readable, but it only helps for loops that were actually
//! registered — an unregistered loop that dies is invisible in exactly the way
//! the family exists to prevent, and no rule can fire on a series that was never
//! published.
//!
//! That is not a mistake a reviewer catches: registering a loop is a second thing
//! to remember next to writing it, and forgetting costs nothing until the day the
//! loop stops. So the requirement is asserted over the sources instead: a file
//! that starts cadence-driven work either registers it, or appears below with a
//! reason someone had to write down.
//!
//! The exemption list is the backlog made countable. A stale entry — file gone,
//! marker gone, or already registered — fails here rather than quietly widening
//! the exemption, and an entry with no reason fails too.
//!
//! The marker is a ticker, so this scans a subset of long-running work and a green
//! run says nothing about the rest: a task that waits on `sleep` between rounds of
//! its own work is not scanned, and neither is anything spawned where the loop is
//! implicit. Those populations overlap with retries and backoff waits whose end is
//! a return rather than a defect, so widening the marker means first writing the
//! rule that tells the two apart — until then the narrower claim is the honest one.
//!
//! Note on the literals: they are assembled at compile time, so this file never
//! contains the text it forbids (it is itself one of the sources scanned).

use std::path::{Path, PathBuf};

/// The call that starts cadence-driven work.
const MARKER: &str = concat!("tokio::time::", "interval(");
/// The calls that make such work watchable. `spawn` registers as well: it wraps
/// the body and hands it a [`Beat`].
const REGISTRATIONS: [&str; 2] = [
    concat!("loop_health::", "register("),
    concat!("loop_health::", "spawn("),
];

/// Cadence-driven work that is not registered yet, each with why.
///
/// `backlog:` is a loop the family should eventually watch; the reason names the
/// concrete thing in the way. `not-a-loop:` is a marker that does not start a
/// loop of the kind the family watches — a helper with no production call site,
/// or a ticker inside a task whose end is a legitimate end rather than a defect.
const NOT_REGISTERED: &[(&str, &str)] = &[
    (
        "crates/cog-agent/src/agent.rs",
        "not-a-loop: the registry heartbeat is per agent instance, not per process — \
         it starts and is aborted with the agent's own lifecycle, so its end is \
         usually intended, and one series per agent would make the label unbounded. \
         Whether the registration is alive is the registry's TTL to report, not this \
         family's",
    ),
    (
        "crates/cog-observability/src/probes.rs",
        "not-a-loop: a probe-sweep helper with no production call site",
    ),
    (
        "crates/cog-storage/src/raw/logger.rs",
        "not-a-loop: the flush ticker bounds flush latency inside the writer task, \
         whose only exit is its channel closing — the logger being shut down, not \
         a defect, and the age of a demand-driven flush means nothing",
    ),
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cogneva")
        .to_path_buf()
}

/// Everything up to the first test-only item: fixtures are short-lived by
/// construction, so a ticker inside one is not a loop of the process.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// Whether this source starts cadence-driven work.
fn starts_cadence_driven_work(production: &str) -> bool {
    production.contains(MARKER)
}

/// Whether this source registers such work with the liveness family.
fn registers_a_loop(production: &str) -> bool {
    REGISTRATIONS.iter().any(|r| production.contains(r))
}

/// Whether this file starts work that nothing can observe: cadence-driven, not
/// registered, and not exempted with a reason.
fn is_unobservable_work(relative: &str, text: &str) -> bool {
    let production = production_source(text);
    starts_cadence_driven_work(production)
        && !registers_a_loop(production)
        && !NOT_REGISTERED.iter().any(|(path, _)| *path == relative)
}

/// The name a registration reports, as written at the call site. The call may be
/// written on one line or with the arguments one per line, so the first
/// non-empty piece after the opening parenthesis is the argument.
fn registration_name(production: &str, at: usize) -> &str {
    let rest = &production[at..];
    let after_paren = rest.split_once('(').map(|(_, r)| r).unwrap_or(rest);
    after_paren
        .split([',', '\n', ')'])
        .map(str::trim)
        .find(|piece| !piece.is_empty())
        .unwrap_or("")
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
fn the_scan_sees_the_work_and_the_registrations() {
    // Without this, a rename that made the scan vacuous would leave the checks
    // below green while enforcing nothing.
    assert!(starts_cadence_driven_work(concat!(
        "let mut t = tokio::time::",
        "interval(d);\nloop { t.tick().await; }"
    )));
    // A ticker inside a test module is not a loop of the process: the cut has to
    // hide it from the rule.
    assert!(!starts_cadence_driven_work(production_source(concat!(
        "fn f() {}\n#[cfg(test)]\nmod tests { fn g() { let mut t = tokio::time::",
        "interval(d); } }"
    ))));
    assert!(registers_a_loop(concat!(
        "let beat = cog_core::loop_health::",
        "register(NAME, Cadence::EventDriven);"
    )));
    assert!(registers_a_loop(concat!(
        "cog_core::loop_health::",
        "spawn(NAME, c, s, |beat| async move { beat.beat(); })"
    )));
    assert!(!registers_a_loop("let beat = register(NAME, cadence);"));

    // The name at a call site is read up to the first separator, so the check
    // below sees the identifier rather than the whole call.
    let source = concat!(
        "cog_core::loop_health::",
        "register(\n    SOME_LOOP,\n    Cadence::EventDriven,\n);"
    );
    let at = source.find("register(").expect("marker present");
    assert_eq!(registration_name(source, at), "SOME_LOOP");

    // The rule itself, against synthetic sources: a file that starts a loop
    // without registering it is a finding, and the two ways out of that finding
    // both have to work — registering it, or a reason in the table.
    let unregistered = concat!(
        "let mut t = tokio::time::",
        "interval(d);\nloop { t.tick().await; }"
    );
    assert!(is_unobservable_work("crates/x/src/y.rs", unregistered));
    assert!(!is_unobservable_work(
        "crates/x/src/y.rs",
        &format!(
            "{unregistered}\nlet beat = cog_core::loop_health::{}",
            "register(LOOP_NAME, cadence);"
        )
    ));
    assert!(!is_unobservable_work(NOT_REGISTERED[0].0, unregistered));
}

#[test]
fn every_cadence_driven_file_is_registered_or_listed_with_a_reason() {
    let mut unlisted = Vec::new();
    let mut registered = 0usize;
    for (relative, text) in scan() {
        if registers_a_loop(production_source(&text)) {
            registered += 1;
            continue;
        }
        if is_unobservable_work(&relative, &text) {
            unlisted.push(relative);
        }
    }
    assert!(
        unlisted.is_empty(),
        "these files start cadence-driven work that nothing can observe — register it \
         with cog_core::loop_health, or list it in NOT_REGISTERED with a reason: {unlisted:#?}"
    );
    // The loop half of the same guard: a scan that stopped finding registrations
    // would make this test pass by finding nothing at all.
    assert!(
        registered >= 20,
        "only {registered} files were seen registering a loop; the scan is not \
         looking at what it thinks it is"
    );
}

#[test]
fn every_exemption_is_current_and_says_what_kind_it_is() {
    let sources = scan();
    for (path, reason) in NOT_REGISTERED {
        let Some((_, text)) = sources.iter().find(|(p, _)| p == path) else {
            panic!("{path} is exempted from the loop gate but does not exist; remove the entry");
        };
        let production = production_source(text);
        assert!(
            starts_cadence_driven_work(production),
            "{path} is exempted but no longer starts cadence-driven work; remove the entry"
        );
        assert!(
            !registers_a_loop(production),
            "{path} is exempted but now registers its loop; remove the entry"
        );
        let (kind, why) = reason.split_once(':').unwrap_or_else(|| {
            panic!(
                "{path}: the reason has to start with `not-a-loop:` or `backlog:`, or a later \
                    reader cannot tell a deliberate boundary from an oversight"
            )
        });
        assert!(
            matches!(kind, "not-a-loop" | "backlog"),
            "{path}: `{kind}` is not a kind of exemption; use `not-a-loop:` or `backlog:`"
        );
        assert!(
            why.trim().len() >= 30,
            "{path}: the reason is too short to be one — name the concrete thing in the way"
        );
    }
}

#[test]
fn every_registration_names_a_declared_loop() {
    let mut names: Vec<(String, String)> = Vec::new();
    let mut anonymous = Vec::new();
    for (relative, text) in scan() {
        let production = production_source(&text);
        for marker in REGISTRATIONS {
            let mut from = 0usize;
            while let Some(offset) = production[from..].find(marker) {
                let at = from + offset;
                let name = registration_name(production, at);
                // A name written at the call site as a literal cannot be declared
                // once, and two loops sharing one name let a dead one hide behind
                // its live sibling's beats.
                if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    anonymous.push(format!("{relative}: {name}"));
                }
                from = at + marker.len();
            }
        }
        // Declarations, for the uniqueness check: `NAME_LOOP: &str = "value";`.
        for line in production.lines() {
            let line = line.trim();
            let Some((left, right)) = line.split_once(": &str = ") else {
                continue;
            };
            if left.starts_with("pub const ") && left.ends_with("_LOOP") {
                let value = right
                    .trim_start_matches('"')
                    .split('"')
                    .next()
                    .unwrap_or("")
                    .to_string();
                if !value.is_empty() {
                    names.push((value, relative.clone()));
                }
            }
        }
    }
    assert!(
        anonymous.is_empty(),
        "these registrations do not name a declared loop; pass a `*_LOOP` constant or a \
         binding built from configuration, never a literal written at the call site: \
         {anonymous:#?}"
    );

    names.sort();
    let mut duplicates = Vec::new();
    for pair in names.windows(2) {
        if pair[0].0 == pair[1].0 && pair[0].1 != pair[1].1 {
            duplicates.push(format!(
                "{} is declared in both {} and {}",
                pair[0].0, pair[0].1, pair[1].1
            ));
        }
    }
    assert!(
        duplicates.is_empty(),
        "two loops would report under one name, and one of them could then die without \
         the reading changing: {duplicates:#?}"
    );
}
