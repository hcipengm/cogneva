//! An observable published after the plugins initialise reaches no scrape.
//!
//! The metrics endpoint and the store sampler both take their snapshot of the
//! published observables **during `init`**: the gateway plugin consumes
//! `dyn Observable` from the service registry while it initialises and keeps
//! that vector on its state, and every handler that renders or stores readings
//! iterates that one vector. `start_all` runs only after `init_all` has
//! finished for every plugin, so a handle published from `start()` is real,
//! held by a live loop, and read by nobody: its series never appear in a scrape,
//! and the alert rule written for it stays quiet through exactly the incident it
//! was written for.
//!
//! Nothing else notices. The name is in the registry, the producer is in the
//! source, both observability contract tests are green -- they ask whether a
//! file names the series, not whether the handle carrying it was published
//! before the readers looked. This test covers the part they cannot: the
//! lifecycle point at which the publish happens.
//!
//! The rule is mechanical because the premise is: publishing during `init` is
//! what makes a handle visible. It is checked over the whole workspace rather
//! than a list of files, so a new plugin gets it without being added anywhere.
//! Its ceiling, stated once: this reads text, so it decides the shape of the
//! call and not its reachability -- a publish inside a helper called from
//! `start()` is beyond it.

use std::path::{Path, PathBuf};

/// Every `async fn <name>(` body in `text` that belongs to an impl, paired with
/// the body.
///
/// A body runs from the call site's opening brace to the matching close, so a
/// method ending in a nested item is not read into the next one. Braces inside
/// string, char and comment text are skipped: `format!("{name}")` is balanced
/// either way, but a lone brace in a literal would otherwise desynchronise the
/// scan for the rest of the file.
fn method_bodies(text: &str, name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let needle = format!("async fn {name}(");
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(rel) = text[from..].find(&needle) {
        let at = from + rel;
        from = at + needle.len();
        let Some(open) = text[from..].find('{') else {
            break;
        };
        let start = from + open;
        let mut depth = 0usize;
        let mut i = start;
        let mut in_line_comment = false;
        let mut in_block_comment = 0usize;
        let mut in_string = false;
        let mut in_char = false;
        while i < bytes.len() {
            let c = bytes[i] as char;
            let prev = if i > 0 { bytes[i - 1] as char } else { ' ' };
            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
            } else if in_block_comment > 0 {
                if prev == '*' && c == '/' {
                    in_block_comment -= 1;
                } else if prev == '/' && c == '*' {
                    in_block_comment += 1;
                }
            } else if in_string {
                if c == '"' && prev != '\\' {
                    in_string = false;
                }
            } else if in_char {
                if c == '\'' && prev != '\\' {
                    in_char = false;
                }
            } else if prev == '/' && c == '/' {
                in_line_comment = true;
            } else if prev == '/' && c == '*' {
                in_block_comment = 1;
            } else if c == '"' {
                in_string = true;
            } else if c == '\'' {
                in_char = true;
            } else if c == '{' {
                depth += 1;
            } else if c == '}' {
                depth -= 1;
                if depth == 0 {
                    out.push(text[start..=i].to_string());
                    from = i + 1;
                    break;
                }
            }
            i += 1;
        }
    }
    out
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cogneva sits two levels under the workspace root")
        .to_path_buf()
}

#[test]
fn no_observable_is_published_from_a_plugin_start() {
    let root = workspace_root();
    let files = rust_sources(&root);
    assert!(
        files.len() > 50,
        "scanned {} source files, which is too few to be the workspace",
        files.len()
    );

    let mut starts = 0usize;
    let mut init_publishes = 0usize;
    let mut offenders = Vec::new();
    for path in &files {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        for body in method_bodies(&text, "start") {
            starts += 1;
            if body.contains("publish_observable") {
                offenders.push(path.display().to_string());
            }
        }
        for body in method_bodies(&text, "init") {
            if body.contains("publish_observable") {
                init_publishes += 1;
            }
        }
    }

    // The two counts are what make the rule above mean anything: a scan that
    // finds no `start` bodies has nothing to complain about, and one that never
    // sees a publish at all cannot tell a moved call from a renamed one.
    assert!(
        starts > 10,
        "found {starts} plugin start bodies; the scan is not reading the workspace"
    );
    assert!(
        init_publishes > 0,
        "no observable is published from any init body; the scan is not reading publishes"
    );

    assert!(
        offenders.is_empty(),
        "these plugins publish an observable from start(), where the readers have \
         already taken their snapshot and will never see it: {offenders:?}. Publish the \
         handle in init and attach it to the loop in start() -- the build-cache, \
         registry-footprint and governance-drift handles are all shaped that way."
    );
}

/// The premise the rule above rests on, read from the reader rather than assumed.
///
/// The rule is only right while the observable snapshot is taken during init. If
/// the consumer ever re-reads the registry later -- at scrape time, or from
/// `start` -- then a handle published from `start` becomes reachable and this
/// file would be enforcing a habit that no longer has a reason. Pinning the
/// premise here means that change fails this test and has to be made
/// deliberately, instead of silently turning the rule above into lore.
#[test]
fn the_observable_snapshot_is_taken_during_init_not_start() {
    let path = workspace_root().join("crates/cog-gateway/src/plugin.rs");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let consume = "consume_all_services::<dyn cog_core::Observable>";

    let in_init = method_bodies(&text, "init")
        .iter()
        .filter(|body| body.contains(consume))
        .count();
    let in_start = method_bodies(&text, "start")
        .iter()
        .filter(|body| body.contains(consume))
        .count();

    assert_eq!(
        in_init, 1,
        "the gateway no longer takes its observable snapshot while initialising, so the \
         rule in this file is checking a lifecycle point that no longer decides anything"
    );
    assert_eq!(
        in_start, 0,
        "the gateway now also reads observables from start(), where a handle published by \
         another plugin's start may or may not be visible depending on scheduling: the \
         snapshot is no longer a well-defined point"
    );
}
