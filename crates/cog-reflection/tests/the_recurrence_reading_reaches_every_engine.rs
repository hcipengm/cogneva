//! principle: feedback_a_reading_must_ride_the_branch_the_deployment_runs
//! Gate: every engine the reflection plugin builds reports the recurrence at
//! its thresholds into the reading the process publishes.
//!
//! The failure this exists for: the count the thresholds act on went unread for
//! three weeks, and when a reading was finally wired it went onto the
//! **production** branch — while the engine that carries the learning load in
//! the running deployment is the **in-memory fallback**, built by the other
//! branch. That shape produces a published series frozen at zero: the reading
//! exists, the scrape reaches it, and it says "nothing is being counted" about
//! an engine that counted thousands. Nothing goes red on its own, because a
//! wired-but-never-handed-over handle and a pipeline that counts nothing are the
//! same reading.
//!
//! So the gate is structural, over the plugin's production source: the plugin
//! must not build an engine through a constructor that has no room for the
//! reading, both branches' constructors must name the handle, and the handle
//! must be published — a handle handed to an engine but never published is a
//! series no scrape can reach, which is the same silence one layer further out.

use std::path::{Path, PathBuf};

const PLUGIN: &str = "crates/cog-reflection/src/plugin.rs";

/// The constructor that builds the in-memory engine without anywhere to report
/// the recurrence. Naming it in the plugin's production source is the defect,
/// not the fix: the plugin is where a deployment decides which engine runs, so
/// it is the one place that must always have a reading to hand over.
const UNREPORTING_IN_MEMORY_CTOR: &str = "new_in_memory(";

/// The same engine, with the reading attached.
const REPORTING_IN_MEMORY_CTOR: &str = "new_in_memory_reporting(";

/// The other engine builder, whose last argument is the reading.
const SELF_EVOLUTION_CTOR: &str = "new_self_evolution(";

/// The handle both constructors are handed, and the one the scrape reads.
const HANDLE: &str = "recurrence_readings";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cog-reflection")
        .to_path_buf()
}

/// Everything up to the first test-only item. Occurrences inside test modules
/// are fixtures, not the plugin's production wiring.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// The argument text of the `nth` call to `call` in `source`: everything between
/// its opening parenthesis and the one that closes it.
///
/// Positional on purpose — the reading is the last argument of one constructor
/// and the second of the other, and a substring search over the whole file would
/// call a name in a comment a wiring.
fn call_args(source: &str, call: &str, nth: usize) -> Option<String> {
    let mut search_from = 0;
    for _ in 1..nth {
        search_from += source[search_from..].find(call)? + call.len();
    }
    // `call` ends with the opening parenthesis, so `open` is its index: the
    // argument text starts one character later.
    let open = search_from + source[search_from..].find(call)? + call.len() - 1;
    let rest = &source[open..];
    if !rest.starts_with('(') {
        return None;
    }
    let mut depth = 0usize;
    for (offset, ch) in rest.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(rest[1..offset].to_owned());
                }
            }
            _ => {}
        }
    }
    None
}

fn plugin_production() -> String {
    let path = workspace_root().join(PLUGIN);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{PLUGIN}: {e}"));
    production_source(&text).to_owned()
}

/// The plugin builds its engine on one of two branches and the deployment picks
/// which by whether a memory backend and an LLM provider are reachable. Both
/// branches consult the recurrence thresholds, so both have to be handed the
/// reading — and the fallback branch is the one a deployment without a reachable
/// backend runs, where the corpus dies with the process, so the published
/// distribution is the only place the accumulation of patterns can be seen.
#[test]
fn both_engine_branches_are_handed_the_recurrence_reading() {
    let source = plugin_production();

    assert!(
        !source.contains(UNREPORTING_IN_MEMORY_CTOR),
        "{PLUGIN} builds an engine with `{UNREPORTING_IN_MEMORY_CTOR}`, which takes no recurrence \
         reading: the counts that engine's thresholds act on are then invisible for the life of \
         the process. Use `{REPORTING_IN_MEMORY_CTOR}`."
    );

    let production_args = call_args(&source, SELF_EVOLUTION_CTOR, 1)
        .unwrap_or_else(|| panic!("{PLUGIN} no longer calls {SELF_EVOLUTION_CTOR}"));
    assert!(
        production_args.contains(HANDLE),
        "the production engine is built without the recurrence reading: its series would resolve \
         to a published set that can only ever read zero. Arguments were: {production_args}"
    );

    let fallback_args = call_args(&source, REPORTING_IN_MEMORY_CTOR, 1).unwrap_or_else(|| {
        panic!("{PLUGIN} no longer builds the in-memory engine: the deployment fallback is gone")
    });
    assert!(
        fallback_args.contains(HANDLE),
        "the fallback engine is built without the recurrence reading. Arguments were: \
         {fallback_args}"
    );
}

/// A reading the scrape cannot reach is the same silence as no reading at all,
/// and the gateway takes its snapshot of the observables during `init` — a
/// handle published from `start()` reaches no scrape in any deployment.
#[test]
fn the_handle_is_published_where_the_gateway_takes_its_snapshot() {
    let source = plugin_production();
    assert!(
        source.contains(&format!("publish_observable({HANDLE}")),
        "{PLUGIN} never publishes `{HANDLE}`, so the series is in no scrape: the promoters report \
         into a handle nobody reads."
    );
}

/// The call extractor has to actually read the call, not the whole file: a test
/// whose helper silently matched everything would stay green through the defect
/// it was written for.
#[test]
fn the_argument_reader_stops_at_the_closing_parenthesis() {
    let text = "let x = f(a, b); let y = g(c);";
    assert_eq!(call_args(text, "f(", 1).as_deref(), Some("a, b"));
    assert_eq!(call_args(text, "g(", 1).as_deref(), Some("c"));
    assert_eq!(call_args(text, "h(", 1), None);
    // Nested calls must not end the outer one early, which is the shape the
    // plugin's production branch has.
    assert_eq!(
        call_args("e(a.clone(), f(b), c)", "e(", 1).as_deref(),
        Some("a.clone(), f(b), c")
    );
}
