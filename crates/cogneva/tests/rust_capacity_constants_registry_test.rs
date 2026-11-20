//! principle: feedback_a_verdict_table_beside_the_code_is_a_second_copy_put_it_on_the_object
//!
//! Every number written into a Rust constant has to say where it comes from,
//! and a new one fails this test until it does.
//!
//! The chart half answers this with a comment block above each numeric leaf:
//! one leaf, one judgement, `derived` or `fixed`. A chart leaf is
//! read at run time, so `derived` is something a leaf can be -- a measurement
//! moves it. A `const` cannot: its value is fixed when the crate is compiled and
//! no later reading moves it. The words a constant can carry are therefore
//! `bound` (pinned against the world; the reason says what pins it, and a loop
//! that could have moved it but does not is the debt stated there) and `fixed`
//! (not a dial; the reason names the agreement it comes from). `derived` is
//! refused on a constant on purpose: a constant that claims to follow a reading
//! is exactly the shape that let a hand-measured figure outlive its measurement
//! and start reading as a fact about the deployment. When a reading really does
//! move a working value, the derivation belongs on the value that moves, and its
//! evidence belongs there with it.
//!
//! Rust had nothing, so a number describing the machine could be typed into a
//! `const` and nothing anywhere asked where it came from.
//!
//! The answer lives on the constant, as a `// cap: bound` or `// cap: fixed`
//! line in the comment block right above it: one word for which kind of answer
//! this is, and a reason after a dash. The reason goes on that same line rather
//! than in the prose above, because a block can describe a number without ever
//! saying why it is the number it is -- `/// Default capacity for the stream.`
//! is prose, and it answers nothing -- so a rule that accepts prose accepts a
//! description in place of a decision.
//!
//! It used to live in a table beside the code with a row per constant,
//! and that table was the same number restated twice: its `value` column was
//! whatever the declaration said, its `note` was the doc comment above it, and
//! only the one word was new. A restatement drifts, and it drifts in the
//! direction nobody catches -- regenerate it after a sloppy edit and the drift
//! is written down as the truth.
//!
//! Keeping it on the constant also deletes a whole failure mode: a row for a
//! constant that no longer exists is impossible, because the row *is* the
//! constant. What is left for this test is the census and the word.
//!
//! What it does not judge: numeric defaults. A `field: 30` inside an
//! `impl Default` is not a constant and this test never sees it; the same
//! question for those numbers belongs to the config face. Nor does it judge the
//! delivered document — its leaves are asked for a same-place note by
//! `shipped_config_numbers_are_registered_test`. A `// cap:` word here says
//! nothing about whether the number in `cogneva.json` came from anywhere.
//!
//! **It enumerates every written number, not only the ones whose name reads
//! like a quantity.** The first version filtered by name -- bytes, sizes, caps,
//! intervals -- on the argument that a `u64` holding a protocol version is not
//! a number about this machine. The argument was about half right and cost more
//! than it saved: the filter is a guess about a name, and the numbers it guessed
//! wrong are exactly the ones nobody then looks at (`RENDER_ATTEMPTS`,
//! `DEFAULT_SEARCH_RESULTS`, `CHARS_PER_ESTIMATED_TOKEN`). A number a
//! deployment's behaviour depends on does not become harmless because its name
//! does not contain `LIMIT`. So the population is now every `const` of a numeric
//! type or `Duration` whose initializer holds a typed digit, and the judgement
//! that a given one is not a dial belongs in the `cap:` word, where it is read
//! by a person, rather than in a name filter, where it is made by a string
//! match.
//!
//! There is no third answer and no place to park one: a constant that arrives
//! without a `cap:` line is red, and the two words are the whole closed set. A
//! debt column would have been the easy shape and the wrong one -- "not judged
//! yet" is exactly what a registry of every number was supposed to end. The same
//! reasoning refuses `derived` rather than admitting it as a third word: a
//! constant carrying it does not become a moving value, it only reads like one.
//!
//! The other half of the same question is on this file too: a number written at
//! the point of *use* rather than in a declaration. The census above cannot see
//! it, and the copies that drifted were exactly that shape -- a literal handed
//! to `Duration::from_secs` in three call sites, counted by nothing. Those have
//! their own register and their own ceiling at the bottom of this file.
//!
//! Fixtures are not production numbers. Constants inside an inline
//! `#[cfg(test)] mod tests` are excluded -- the first version counted them,
//! which asked reviewers to justify test data and inflated the population by
//! exactly the rows that were never dials.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::common;

use common::routes::repo_root;

use common::rust_production::{production_sources, sources_of_tree};

/// The word that answers for a constant, in the comment block above it.
const CAP_MARKER: &str = "// cap:";

/// The whole closed set of answers a constant may carry.
const CAP_SOURCES: [&str; 2] = ["bound", "fixed"];

/// The chart's word for a value a measurement moves. A `const` is frozen at
/// compile time, so it is not an answer a constant can give; naming it here
/// keeps the refusal separate from "a word we have never heard of".
const DERIVED: &str = "derived";

/// One written number: where it is, what it says, and what the block above it
/// answered.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    file: String,
    name: String,
    value: String,
    /// `bound` or `fixed`; empty when the constant carries no marker.
    source: String,
    /// The reason after the dash. Empty means the marker is a label and nothing
    /// else, which is the thing this test exists to refuse.
    note: String,
}

/// Every `const NAME: <numeric type> = <initializer holding a written number>;`
/// in `files`.
///
/// What counts is a typed digit, not the shape of the expression around it.
/// `const LIMIT: u64 = 4096 * 1024;` is two typed digits and no measurement,
/// and so is `const HALF: u64 = WINDOW * 3 / 2;`. Reading only the initializer's
/// first character put the second shape outside both registers: the census
/// skipped it, and the use-site scan skips declarations, so the 3 and the 2 had
/// no row anywhere. Dropping the identifiers first and asking whether a digit is
/// left is still syntactic -- no expression is parsed -- and an initializer made
/// only of names (`const SAME: u64 = OTHER;`) holds no number of its own and
/// stays out.
///
/// The population is the caller's: shipping sources, and a fixture's numbers
/// are the fixture's business. This used to decide that for itself by keeping
/// paths that contain `/src/` -- a second answer to a question the census
/// already answers, and one that reads a caller's mistaken list as a complete
/// register rather than as a short one.
fn scan(root: &Path, files: &[PathBuf]) -> Vec<Row> {
    let mut rows = Vec::new();
    for entry in files {
        let rel = entry
            .strip_prefix(root)
            .unwrap_or(entry)
            .to_string_lossy()
            .to_string();
        let Ok(text) = std::fs::read_to_string(entry) else {
            continue;
        };
        rows.extend(production_rows(&text, &rel));
    }
    rows.sort();
    rows.dedup();
    rows
}

/// One file's constants, with fixture bodies skipped by their braces.
///
/// A `break` at the first test module is what the literal scan used to do, and
/// the comment there records what it cost: a number below the module read as
/// invisible while the register still read as complete. The same hole is silent
/// here -- a constant after a mid-file `mod tests` would be missing from the
/// scan *and* from the register, so neither side of the comparison names it.
///
/// The name only decides the module when nothing guards it: under
/// `#[cfg(test)]` the name is the author's.
fn production_rows(text: &str, rel: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    let mut depth: i32 = 0;
    let mut fixtures: Option<i32> = None;
    let mut expecting_test_module = false;
    let mut scan = ScanState::default();
    let mut above = Above::default();
    for line in text.lines() {
        // 这一行的判词是**上一行之前**攒下来的那一块：常量自己这一行会把块清空，
        // 所以先留一份快照，再让 observe 处理本行。
        let block_above = above.clone();
        above.observe(line);
        let trimmed = line.trim();
        let code = code_only(line, &mut scan);
        // `#[cfg(test)]` guards whatever comes next -- a module, or a single
        // `fn for_test` -- but attributes and doc comments may sit in between,
        // so the guard waits for the next line that is code.
        let opens_tests = if trimmed.starts_with("//") {
            false
        } else if trimmed.starts_with("#[") {
            if trimmed.starts_with("#[cfg(test") {
                expecting_test_module = true;
            }
            false
        } else if expecting_test_module {
            expecting_test_module = false;
            has_inline_module_body(line, true) || code.contains('{')
        } else {
            has_inline_module_body(line, false)
        };
        let in_fixtures = fixtures.is_some();
        let depth_before = depth;
        depth += code.matches('{').count() as i32 - code.matches('}').count() as i32;
        let opened_tests = opens_tests && fixtures.is_none();
        if opened_tests {
            fixtures = Some(depth_before);
        }
        // The opening line may carry no brace of its own, so it must not be the
        // line that closes the span it just opened.
        if !opened_tests {
            if let Some(entry_depth) = fixtures {
                if depth <= entry_depth {
                    fixtures = None;
                }
            }
        }
        if in_fixtures || opens_tests {
            continue;
        }
        let Some(mut row) = parse_const(line, rel) else {
            continue;
        };
        if let Some((source, reason)) = block_above.marker.clone() {
            row.source = source;
            row.note = reason;
        }
        rows.push(row);
    }
    rows
}

/// The comment block sitting immediately above a declaration.
///
/// Carried across blank lines and attributes because that is how the block is
/// written: `/// why it is this`, then maybe an `#[allow]`, then the constant.
/// Anything else ends it -- a marker two functions up governs the function, not
/// the number below it.
#[derive(Debug, Default, Clone)]
struct Above {
    /// `(source, reason)`, when a `// cap:` line was seen in this block.
    marker: Option<(String, String)>,
}

impl Above {
    fn observe(&mut self, line: &str) {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix(CAP_MARKER) {
            if rest.is_empty() || rest.starts_with(' ') || rest.starts_with('\u{2014}') {
                let rest = rest.trim();
                let (word, reason) = match rest.split_once('\u{2014}') {
                    Some((w, r)) => (w.trim(), r.trim()),
                    None => (rest, ""),
                };
                self.marker = Some((word.to_string(), reason.to_string()));
                return;
            }
        }
        if t.starts_with("///") {
            return;
        }
        if t.starts_with("//") || t.starts_with("#[") || t.is_empty() {
            return;
        }
        *self = Above::default();
    }
}

/// Whether this line opens an inline module whose contents are fixtures.
///
/// Inline test modules carry a body (`mod tests {`); a one-line `mod tests;` is
/// a declaration whose body lives in another file, and treating that as the end
/// of production code would silently hide every constant after it -- which is
/// how the first version of this scan lost one.
///
/// The name only decides it when nothing guards the module: under
/// `#[cfg(test)]` the name is the author's (`mod topology_tests`, `mod
/// byte_units`), and a scan that knows two names read one gateway file's whole
/// fixture block as production.
fn has_inline_module_body(line: &str, any_name: bool) -> bool {
    let trimmed = line.trim();
    let after_vis = trimmed
        .strip_prefix("pub(")
        .and_then(|rest| rest.find(')').map(|i| rest[i + 1..].trim_start()))
        .or_else(|| trimmed.strip_prefix("pub ").map(str::trim_start))
        .unwrap_or(trimmed);
    let Some(rest) = after_vis.strip_prefix("mod ") else {
        return false;
    };
    let name = rest.trim_start();
    let ident_end = name
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(name.len());
    if ident_end == 0 {
        return false;
    }
    let opens_body = name[ident_end..].trim_start().starts_with('{');
    opens_body && (any_name || ["tests", "test_support"].contains(&&name[..ident_end]))
}

/// `COGNEVA_BARE_LITERAL_SITES=1` prints every counted site as `file:line`.
///
/// A register of counts says how many are left and not where they are, which is
/// enough to hold a ratchet and not enough to pay it down.
fn sites_wanted() -> bool {
    std::env::var_os("COGNEVA_BARE_LITERAL_SITES").is_some()
}

/// Drop the module path a type or an initializer may carry, leaving the name
/// the registry reads: `std::time::Duration` and `Duration` are one type, and
/// only the bare spelling used to be matched, so the qualified one read as a
/// constant holding no number.
fn strip_qualified(text: &str) -> &str {
    match text.find("Duration") {
        Some(at) if text[..at].ends_with("::") => &text[at..],
        _ => text,
    }
}

/// Whether the initializer opens with a number, sign included.
fn opens_with_a_number(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(c) if c.is_ascii_digit() => true,
        Some('-') | Some('+') => chars.next().is_some_and(|c| c.is_ascii_digit()),
        _ => false,
    }
}

/// A cadence spelled as a single integer inside a unit constructor:
/// `Duration::from_secs(120)` reads as `120s`.
///
/// Arithmetic inside the parens is not read here; it answers the same question
/// as arithmetic anywhere else, in [`holds_a_written_number`].
fn duration_literal(value: &str) -> Option<String> {
    ["secs", "millis", "hours", "mins"].iter().find_map(|u| {
        let inner = value
            .strip_prefix(&format!("Duration::from_{u}("))?
            .strip_suffix(')')?
            .trim();
        let literal = inner
            .chars()
            .all(|c| c.is_ascii_digit() || c == '_')
            .then(|| inner.to_string())?;
        let suffix = match *u {
            "secs" => "s",
            "millis" => "ms",
            "hours" => "h",
            _ => "m",
        };
        Some(format!("{literal}{suffix}"))
    })
}

/// Whether the initializer types a number anywhere, not only in front.
///
/// Identifiers are skipped whole, so the digits in `WINDOW_SECS_2` are part of
/// a name and not a literal; what is left is literals and punctuation, and one
/// digit in it is a number a person wrote.
fn holds_a_written_number(value: &str) -> bool {
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_alphabetic() || c == '_' {
            while chars
                .peek()
                .is_some_and(|n| n.is_ascii_alphanumeric() || *n == '_')
            {
                chars.next();
            }
        } else if c.is_ascii_digit() {
            return true;
        }
    }
    false
}

fn parse_const(line: &str, file: &str) -> Option<Row> {
    let line = line.trim_start();
    let line = match line.strip_prefix("pub(") {
        Some(rest) => rest[rest.find(')')? + 1..].trim_start(),
        None => line
            .strip_prefix("pub ")
            .map(str::trim_start)
            .unwrap_or(line),
    };
    let line = line.strip_prefix("const ")?;
    let (name, rest) = line.split_once(':')?;
    let name = name.trim();
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return None;
    }
    let rest = rest.trim_start();
    // The type and the initializer are both spelled fully qualified about as
    // often as not (`std::time::Duration`), and reading only the bare name made
    // those constants look like they held no number at all: the declaration was
    // invisible here and the call site had already been given the name, so the
    // number left both registers at once.
    let ty = strip_qualified(rest.split('=').next()?.trim());
    // One line, one constant: the initializer has to close on the same line.
    let value = rest.split_once('=')?.1.trim();
    let value = value.strip_suffix(';')?.trim();
    let value = strip_qualified(value);
    let value = match ty {
        // Any initializer that holds a written digit is a written number,
        // arithmetic included: `4096 * 1024` is two typed digits and no
        // measurement, and so is `SECS * 3 / 2`. Calling the second shape
        // "computed" is what let the multiplier hide.
        "u64" | "u32" | "u16" | "u8" | "usize" | "u128" | "i64" | "i32" | "i16" | "i8" | "i128"
        | "f64" | "f32" => {
            if !opens_with_a_number(value) && !holds_a_written_number(value) {
                return None;
            }
            value.to_string()
        }
        // A cadence written as `Duration::from_secs(120)` is the same kind of
        // number as `120` in a `u64`, and reading only the integers left the
        // most common spelling of a period outside the registry altogether.
        // The arithmetic and unit spellings (`from_secs(30 * 60)`,
        // `Duration::hours(24)`) carry a typed digit too and fall through to
        // the same question as the integers above.
        "Duration" => match duration_literal(value) {
            Some(literal) => literal,
            None if holds_a_written_number(value) => value.to_string(),
            None => return None,
        },
        _ => return None,
    };
    Some(Row {
        file: file.to_string(),
        name: name.to_string(),
        value: value.to_string(),
        source: String::new(),
        note: String::new(),
    })
}

/// Every written number carries a word above it saying which kind of answer it
/// is, and the word is one of two.
///
/// There is no debt column and no place to park a new number: a constant that
/// arrives without a `cap:` line is red on the run that adds it. That is the
/// whole point -- a register with an "unreviewed" slot absorbs exactly the
/// numbers nobody decided about, and then reads green.
///
/// The rows the census refuses, split by which refusal it is: no marker at all,
/// a word outside the vocabulary, a `derived` claim on a constant, and a marker
/// with no reason on it.
///
/// `derived` gets its own bucket rather than joining the unknown words because
/// the refusal is about the shape, not the spelling: a reader who wrote it was
/// answering a question a constant cannot answer, and the message has to say
/// what to do instead. Filing it under "not in the vocabulary" would hide that
/// behind a set listing.
///
/// Split out from the test so a fixture tree can be judged by the same code the
/// real tree is judged by -- a fixture that restated the partition would agree
/// with the test no matter which way either one was written.
fn refuses(rows: &[Row]) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
    let mut unmarked = Vec::new();
    let mut unknown = Vec::new();
    let mut moves = Vec::new();
    let mut unreasoned = Vec::new();
    for row in rows {
        if row.source.is_empty() {
            unmarked.push(format!("{}:{} = {}", row.file, row.name, row.value));
            continue;
        }
        if row.source == DERIVED {
            moves.push(format!("{}:{} = {}", row.file, row.name, row.value));
            continue;
        }
        if !CAP_SOURCES.contains(&row.source.as_str()) {
            unknown.push(format!(
                "{}:{} says `cap: {}`",
                row.file, row.name, row.source
            ));
            continue;
        }
        // The reason has to be in the marker itself. A block above can describe
        // a number without saying why it is that number, and a description in
        // the place of a decision is what a register of labels looks like.
        if row.note.is_empty() {
            unreasoned.push(format!("{}:{} = {}", row.file, row.name, row.value));
        }
    }
    (unmarked, unknown, moves, unreasoned)
}

/// A `// cap: bound` says which kind of answer the number is; the reason after
/// the dash says what the answer rests on. Only the second is checkable, and a
/// block of prose above is not a substitute for it: `/// Default capacity for
/// the stream.` describes the number and answers nothing, so a rule that takes
/// prose takes a description in place of a decision. Requiring the reason on the
/// marker's own line is what keeps this from degrading into a label every new
/// constant gets and nobody reads.
#[test]
fn every_capacity_number_in_rust_says_where_it_comes_from() {
    let root = repo_root();
    let scanned = scan(&root, &production_sources(&root));
    assert!(
        scanned.len() > 100,
        "the scan found {} constants, far below what crates/*/src holds: the walk is \
         broken, not the tree",
        scanned.len()
    );

    let (unmarked, unknown, moves, unreasoned) = refuses(&scanned);
    assert!(
        unmarked.is_empty(),
        "these written numbers say nothing about where they come from. Put a line in \
         the block above each one -- `// cap: bound` when the value is pinned, \
         `// cap: fixed` when it is not a dial -- and the reason it rests on after a \
         dash. The reason goes on that same line, not in the prose above: a block can \
         describe a number without saying why it is that one:\n{}",
        unmarked.join("\n")
    );
    assert!(
        unknown.is_empty(),
        "these say something the vocabulary does not accept; a constant's answers are \
         {CAP_SOURCES:?}:\n{}",
        unknown.join("\n")
    );
    assert!(
        moves.is_empty(),
        "these claim to follow a reading, and a constant cannot: its value is fixed \
         when the crate is compiled, so no measurement moves it and a test that \
         changes its input can only change it through a rebuild. Label the pinned \
         value `bound` (say what pins it) or the agreement `fixed` (name it). A \
         value a reading really moves gets its derivation -- and its evidence -- on \
         the value that moves, not on a const that restates the number:\n{}",
        moves.join("\n")
    );
    assert!(
        unreasoned.is_empty(),
        "these carry a `cap:` word with no reason on it -- a bare label is the \
         answer a reader cannot check:\n{}",
        unreasoned.join("\n")
    );
}

/// One row per file: how many constants there answered `bound` on 2026-11-17.
///
/// `bound` is the one word in the vocabulary nothing can check. The reason says
/// what pins the value, and no rule reads that reason well enough to tell a pin
/// from a preference — so it is the shape a hand-picked number arrives in: an
/// override wearing a justification. The two shapes that can be checked are
/// `fixed` (it names an agreement) and a value computed at run time from the
/// setting or the reading it follows (the read-file page is the worked example:
/// it derives from the window the tools serve instead of a constant page size).
///
/// Recorded per path, not as a total: with a total, fixing fifteen in one file
/// and writing fifteen in another leaves the number unchanged and the gate
/// silent — a green that covers a new one. A file may come in under its row;
/// that is a repaid debt, printed as STALE so the row gets lowered. Going over
/// a row is red, and a file that is not here has a row of zero.
///
/// The rows are the measured population, not a budget somebody chose.
const BOUND_AT_CENSUS: &[(&str, u64)] = &[
    ("crates/bootstrap/src/apt.rs", 1),
    ("crates/bootstrap/src/kubespray.rs", 1),
    ("crates/bootstrap/src/lib.rs", 6),
    ("crates/bootstrap/src/main.rs", 11),
    ("crates/cog-agent/src/agent.rs", 2),
    ("crates/cog-agent/src/agent_kernel/replay_guard.rs", 1),
    ("crates/cog-agent/src/agent_kernel/runtime.rs", 5),
    ("crates/cog-agent/src/archive.rs", 1),
    ("crates/cog-agent/src/background.rs", 3),
    ("crates/cog-agent/src/consumer.rs", 3),
    ("crates/cog-agent/src/context.rs", 3),
    ("crates/cog-agent/src/drain.rs", 1),
    ("crates/cog-agent/src/hooks/lifecycle.rs", 1),
    ("crates/cog-agent/src/hooks/types.rs", 2),
    ("crates/cog-agent/src/observable.rs", 1),
    ("crates/cog-agent/src/permission.rs", 1),
    ("crates/cog-agent/src/plugin.rs", 4),
    ("crates/cog-agent/src/tools.rs", 8),
    ("crates/cog-agent/src/web.rs", 2),
    ("crates/cog-auth/src/keycloak.rs", 3),
    ("crates/cog-auth/src/session.rs", 1),
    ("crates/cog-collaboration/src/collaboration_executor.rs", 2),
    ("crates/cog-collaboration/src/config.rs", 1),
    ("crates/cog-collaboration/src/profile.rs", 5),
    ("crates/cog-collaboration/src/squad/executor.rs", 2),
    ("crates/cog-collaboration/src/squad/ralph/ralph_loop.rs", 1),
    ("crates/cog-core/src/build_cache.rs", 5),
    ("crates/cog-core/src/build_cache_readings.rs", 2),
    ("crates/cog-core/src/claim_footprint.rs", 2),
    ("crates/cog-core/src/config.rs", 12),
    ("crates/cog-core/src/config_sections.rs", 2),
    ("crates/cog-core/src/contract/alerts.rs", 1),
    ("crates/cog-core/src/contract/build_cache_reclaim.rs", 2),
    ("crates/cog-core/src/contract/conversation.rs", 2),
    ("crates/cog-core/src/contract/event.rs", 1),
    ("crates/cog-core/src/contract/face_registry.rs", 1),
    ("crates/cog-core/src/contract/governance_acts.rs", 1),
    ("crates/cog-core/src/contract/hook.rs", 1),
    ("crates/cog-core/src/contract/loop_health.rs", 1),
    ("crates/cog-core/src/contract/memory.rs", 2),
    ("crates/cog-core/src/contract/owner_lease.rs", 2),
    ("crates/cog-core/src/contract/reflection.rs", 4),
    ("crates/cog-core/src/contract/resilience.rs", 2),
    ("crates/cog-core/src/contract/storage.rs", 4),
    ("crates/cog-core/src/contract/taste.rs", 4),
    ("crates/cog-core/src/contract/work_item.rs", 1),
    ("crates/cog-core/src/day.rs", 1),
    ("crates/cog-core/src/declared_wall.rs", 2),
    ("crates/cog-core/src/pacing.rs", 2),
    ("crates/cog-core/src/progress.rs", 7),
    ("crates/cog-core/src/reading_cadence.rs", 1),
    ("crates/cog-core/src/retention.rs", 4),
    ("crates/cog-core/src/token_estimate.rs", 2),
    ("crates/cog-core/src/types/agent_state.rs", 2),
    ("crates/cog-eval/src/adapters/swe_pro.rs", 1),
    ("crates/cog-eval/src/admission.rs", 9),
    ("crates/cog-eval/src/backoff.rs", 4),
    ("crates/cog-eval/src/exec_seam.rs", 2),
    ("crates/cog-eval/src/long_run.rs", 2),
    ("crates/cog-eval/src/rig.rs", 1),
    ("crates/cog-eval/src/scaffolds/mod.rs", 1),
    ("crates/cog-eval/src/trend.rs", 3),
    ("crates/cog-extension/src/build_cache.rs", 1),
    ("crates/cog-extension/src/checkpoints.rs", 1),
    ("crates/cog-extension/src/command_server.rs", 3),
    ("crates/cog-extension/src/executor_lease.rs", 1),
    ("crates/cog-extension/src/hostdocs.rs", 5),
    ("crates/cog-extension/src/hostdocs_assist.rs", 3),
    ("crates/cog-extension/src/lane_sizing.rs", 1),
    ("crates/cog-extension/src/runtime/remote.rs", 6),
    ("crates/cog-extension/src/workdir.rs", 10),
    ("crates/cog-gateway/src/a2a.rs", 1),
    ("crates/cog-gateway/src/agents.rs", 5),
    ("crates/cog-gateway/src/auth/mod.rs", 2),
    ("crates/cog-gateway/src/backup_admin.rs", 2),
    ("crates/cog-gateway/src/buildgate.rs", 2),
    ("crates/cog-gateway/src/chat.rs", 5),
    ("crates/cog-gateway/src/cluster.rs", 2),
    ("crates/cog-gateway/src/contribution_admin.rs", 1),
    ("crates/cog-gateway/src/document_egress.rs", 1),
    ("crates/cog-gateway/src/eval_reports.rs", 1),
    ("crates/cog-gateway/src/evolution.rs", 2),
    ("crates/cog-gateway/src/git_identity.rs", 2),
    ("crates/cog-gateway/src/git_mirror.rs", 14),
    ("crates/cog-gateway/src/git_routing.rs", 3),
    ("crates/cog-gateway/src/github_app.rs", 2),
    ("crates/cog-gateway/src/governance.rs", 5),
    ("crates/cog-gateway/src/ide.rs", 10),
    ("crates/cog-gateway/src/knowledge.rs", 1),
    ("crates/cog-gateway/src/lib.rs", 2),
    ("crates/cog-gateway/src/llm_admin.rs", 2),
    ("crates/cog-gateway/src/memory.rs", 2),
    ("crates/cog-gateway/src/observability.rs", 2),
    ("crates/cog-gateway/src/output.rs", 2),
    ("crates/cog-gateway/src/pace_reservations.rs", 2),
    ("crates/cog-gateway/src/plugin.rs", 2),
    ("crates/cog-gateway/src/process_reports.rs", 4),
    ("crates/cog-gateway/src/project_control.rs", 1),
    ("crates/cog-gateway/src/project_rate_limit.rs", 2),
    ("crates/cog-gateway/src/projects.rs", 5),
    ("crates/cog-gateway/src/readings.rs", 1),
    ("crates/cog-gateway/src/scm.rs", 7),
    ("crates/cog-gateway/src/security_gateway.rs", 18),
    ("crates/cog-gateway/src/source_line.rs", 1),
    ("crates/cog-gateway/src/task_ledger.rs", 2),
    ("crates/cog-gateway/src/tasks.rs", 2),
    ("crates/cog-gateway/src/usage_landing.rs", 2),
    ("crates/cog-gateway/src/venues.rs", 1),
    ("crates/cog-github/src/ci_refusals.rs", 1),
    ("crates/cog-github/src/conversation.rs", 1),
    ("crates/cog-github/src/cross_validation.rs", 1),
    ("crates/cog-github/src/discovery_loop.rs", 10),
    ("crates/cog-github/src/landing.rs", 1),
    ("crates/cog-github/src/plugin.rs", 1),
    ("crates/cog-github/src/provider/github.rs", 3),
    ("crates/cog-llm/src/provider/mod.rs", 2),
    ("crates/cog-llm/src/routing.rs", 1),
    ("crates/cog-memory/src/bge_m3_sparse_linear.rs", 1),
    ("crates/cog-memory/src/config.rs", 1),
    ("crates/cog-memory/src/extractor.rs", 7),
    ("crates/cog-memory/src/ingestor.rs", 1),
    ("crates/cog-memory/src/maintenance.rs", 2),
    ("crates/cog-memory/src/pg_summary_pool.rs", 2),
    ("crates/cog-memory/src/vector_summary_backend.rs", 3),
    ("crates/cog-observability/src/alert_store.rs", 1),
    ("crates/cog-observability/src/capability.rs", 3),
    ("crates/cog-observability/src/claim_store.rs", 2),
    ("crates/cog-observability/src/config.rs", 4),
    ("crates/cog-observability/src/config_delivery.rs", 1),
    ("crates/cog-observability/src/exchange_retention.rs", 3),
    ("crates/cog-observability/src/host_ledger.rs", 1),
    ("crates/cog-observability/src/image_store.rs", 2),
    ("crates/cog-observability/src/infra_watch.rs", 2),
    ("crates/cog-observability/src/llm_usage_rollup.rs", 5),
    ("crates/cog-observability/src/logs.rs", 1),
    ("crates/cog-observability/src/plugin.rs", 3),
    ("crates/cog-observability/src/probes.rs", 1),
    ("crates/cog-observability/src/process_zombies.rs", 1),
    ("crates/cog-observability/src/raw_stream.rs", 1),
    ("crates/cog-observability/src/spend_verdict.rs", 4),
    ("crates/cog-observability/src/usage_retention.rs", 3),
    (
        "crates/cog-orchestrator/src/action_planner/action_plan_orchestrator.rs",
        3,
    ),
    ("crates/cog-orchestrator/src/backup_runs.rs", 1),
    ("crates/cog-orchestrator/src/change_ledger.rs", 28),
    ("crates/cog-orchestrator/src/dag_executor/mod.rs", 5),
    (
        "crates/cog-orchestrator/src/dag_executor/orchestrator.rs",
        3,
    ),
    (
        "crates/cog-orchestrator/src/dag_executor/task_checkpoint.rs",
        1,
    ),
    ("crates/cog-orchestrator/src/face_drift.rs", 1),
    ("crates/cog-orchestrator/src/face_lifecycle.rs", 1),
    ("crates/cog-orchestrator/src/face_staffing.rs", 1),
    ("crates/cog-orchestrator/src/governance_acts.rs", 1),
    ("crates/cog-orchestrator/src/lane_memory.rs", 2),
    ("crates/cog-orchestrator/src/outward.rs", 6),
    ("crates/cog-orchestrator/src/plugin.rs", 3),
    ("crates/cog-orchestrator/src/principle_ledger.rs", 8),
    ("crates/cog-orchestrator/src/ready_queue.rs", 2),
    ("crates/cog-orchestrator/src/resource_ledger.rs", 4),
    ("crates/cog-orchestrator/src/subscribe_backoff.rs", 1),
    ("crates/cog-orchestrator/src/task_executor_router.rs", 6),
    ("crates/cog-prompt/src/loader.rs", 1),
    ("crates/cog-protocol/src/convert.rs", 1),
    ("crates/cog-protocol/src/plugin.rs", 1),
    ("crates/cog-quota/src/hierarchy.rs", 1),
    ("crates/cog-reflection/src/auto_promoter.rs", 2),
    ("crates/cog-reflection/src/baseline_port.rs", 1),
    ("crates/cog-reflection/src/build_gate/mod.rs", 16),
    ("crates/cog-reflection/src/build_rss_peak.rs", 3),
    ("crates/cog-reflection/src/change_execution.rs", 3),
    ("crates/cog-reflection/src/change_pipeline.rs", 10),
    ("crates/cog-reflection/src/config.rs", 3),
    ("crates/cog-reflection/src/evolution_build_readings.rs", 1),
    ("crates/cog-reflection/src/firecracker.rs", 1),
    ("crates/cog-reflection/src/gitops_puller.rs", 1),
    ("crates/cog-reflection/src/image_rollout.rs", 1),
    ("crates/cog-reflection/src/lib.rs", 2),
    ("crates/cog-reflection/src/mainline_deployer.rs", 20),
    ("crates/cog-reflection/src/mainline_rollout.rs", 11),
    ("crates/cog-reflection/src/meta_learning.rs", 1),
    ("crates/cog-reflection/src/observability_stack.rs", 2),
    ("crates/cog-reflection/src/platform_http.rs", 2),
    ("crates/cog-reflection/src/plugin.rs", 3),
    ("crates/cog-reflection/src/promotion_eval.rs", 3),
    ("crates/cog-reflection/src/promotion_trend.rs", 4),
    ("crates/cog-reflection/src/registry_footprint.rs", 2),
    ("crates/cog-reflection/src/signal_watcher.rs", 3),
    ("crates/cog-reflection/src/store_readouts.rs", 2),
    ("crates/cog-reflection/src/volume_capacity.rs", 7),
    ("crates/cog-reflection/src/workload_resources.rs", 5),
    ("crates/cog-reflection/src/workspace.rs", 3),
    ("crates/cog-storage/src/backends/file_trace_store.rs", 1),
    ("crates/cog-storage/src/backends/object_backends.rs", 2),
    ("crates/cog-storage/src/checkpoint_cap.rs", 2),
    ("crates/cog-storage/src/metrics_retirement.rs", 2),
    ("crates/cog-storage/src/metrics_sample_cap.rs", 1),
    ("crates/cog-storage/src/observability_event_cap.rs", 4),
    ("crates/cog-storage/src/partition_maintainer.rs", 2),
    ("crates/cog-storage/src/plugin.rs", 3),
    (
        "crates/cog-storage/src/postgres/attempt_journal_store.rs",
        2,
    ),
    ("crates/cog-storage/src/postgres/change_ledger_store.rs", 4),
    ("crates/cog-storage/src/postgres/decision_stats_store.rs", 3),
    ("crates/cog-storage/src/postgres/metrics_backend.rs", 4),
    (
        "crates/cog-storage/src/postgres/observability_gateway.rs",
        5,
    ),
    (
        "crates/cog-storage/src/postgres/principle_ledger_store.rs",
        6,
    ),
    ("crates/cog-storage/src/postgres/resource_event_store.rs", 3),
    ("crates/cog-storage/src/schema_ddl.rs", 4),
    ("crates/cog-storage/src/task_event_retention.rs", 1),
    ("crates/cog-storage/src/wal/mod.rs", 3),
    ("crates/cog-stream/src/redis.rs", 2),
    ("crates/cog-supervisor/src/alert_store.rs", 1),
    ("crates/cog-supervisor/src/autonomous.rs", 1),
    ("crates/cog-supervisor/src/behavior_monitor.rs", 1),
    ("crates/cog-supervisor/src/event_aggregator.rs", 1),
    ("crates/cog-supervisor/src/health_checker.rs", 3),
    ("crates/cog-supervisor/src/multi_backend_consumer.rs", 1),
    ("crates/cog-supervisor/src/registry.rs", 3),
    ("crates/cog-supervisor/src/supervisor.rs", 7),
    ("crates/cog-supervisor/src/udp_heartbeat.rs", 2),
    ("crates/cog-wiki/src/bounds.rs", 1),
    ("crates/cog-wiki/src/distiller.rs", 3),
    ("crates/cog-wiki/src/indexer.rs", 2),
    ("crates/cog-wiki/src/unified_knowledge_backend.rs", 1),
    ("crates/cogneva/src/aof_repair.rs", 2),
    ("crates/cogneva/src/backup.rs", 7),
    ("crates/cogneva/src/backup_crypto.rs", 4),
    ("crates/cogneva/src/checkpoints.rs", 1),
    ("crates/cogneva/src/conversation.rs", 15),
    ("crates/cogneva/src/eval_container.rs", 1),
    ("crates/cogneva/src/eval_mcp_in_pod.rs", 1),
    ("crates/cogneva/src/eval_platform.rs", 4),
    ("crates/cogneva/src/eval_table.rs", 3),
    ("crates/cogneva/src/eval_toolathlon.rs", 3),
    ("crates/cogneva/src/eval_toolathlon_local.rs", 2),
    ("crates/cogneva/src/gateway_observability.rs", 2),
    ("crates/cogneva/src/health_check.rs", 2),
    ("crates/cogneva/src/owner_memory.rs", 2),
    ("crates/cogneva/src/process_census.rs", 2),
    ("crates/cogneva/src/process_report.rs", 2),
    ("crates/cogneva/src/scraper_endpoint.rs", 1),
    ("crates/cogneva/src/startup_guard.rs", 1),
    ("crates/cogneva/src/work.rs", 6),
    ("crates/cogneva/src/write_role.rs", 5),
];

/// Compare the tree against the rows: files over their row, and rows above what
/// the file now carries.
///
/// Separate from the test so the refusal can be pinned on a handful of rows: a
/// rule that only reads the real tree cannot show that it would catch one file
/// growing while another shrinks by the same amount, which is exactly the shape
/// a total would call green.
fn judge_bound_census(
    by_file: &BTreeMap<String, u64>,
    pinned: &[(&str, u64)],
) -> (Vec<String>, Vec<String>) {
    let pinned: BTreeMap<&str, u64> = pinned.iter().copied().collect();
    let over: Vec<String> = by_file
        .iter()
        .filter_map(|(file, count)| {
            let allowed = pinned.get(file.as_str()).copied().unwrap_or(0);
            (*count > allowed).then(|| format!("{file}: {count} > {allowed}"))
        })
        .collect();
    let stale: Vec<String> = pinned
        .iter()
        .filter_map(|(file, allowed)| {
            let now = by_file.get(*file).copied().unwrap_or(0);
            (now < *allowed).then(|| format!("{file}: {now} < {allowed}"))
        })
        .collect();
    (over, stale)
}

#[test]
fn the_uncheckable_bound_answer_only_comes_down() {
    let root = repo_root();
    let scanned = scan(&root, &production_sources(&root));
    let mut by_file: BTreeMap<String, u64> = BTreeMap::new();
    for row in scanned.iter().filter(|row| row.source == "bound") {
        *by_file.entry(row.file.clone()).or_default() += 1;
    }

    let (over, stale) = judge_bound_census(&by_file, BOUND_AT_CENSUS);

    assert!(
        over.is_empty(),
        "these files carry more `bound` constants than their row in BOUND_AT_CENSUS. \
         `bound` is the one answer nothing checks, so the debt only comes down: name \
         the agreement in a `fixed` line, or compute the value at run time from the \
         setting or the reading it follows:\n{}",
        over.join("\n")
    );
    if !stale.is_empty() {
        eprintln!(
            "STALE: these files carry fewer `bound` constants than their row; lower the \
             row to the number shown:\n{}",
            stale.join("\n")
        );
    }
}

/// A file that grows while another shrinks is red, even when the two cancel out.
///
/// This is the shape a total would read as green, and the reason the rows are
/// per file. The shrinking file is also the half that must not red: a repaid
/// debt prints STALE -- the row is a reading, not a quota.
#[test]
fn the_bound_ratchet_counts_per_file_not_in_total() {
    let by_file = BTreeMap::from([
        ("crates/a/src/one.rs".to_string(), 1),
        ("crates/b/src/two.rs".to_string(), 1),
    ]);
    let pinned = [("crates/a/src/one.rs", 2), ("crates/b/src/two.rs", 0)];
    let (over, stale) = judge_bound_census(&by_file, &pinned);
    assert_eq!(
        over,
        vec!["crates/b/src/two.rs: 1 > 0".to_string()],
        "the growth in two.rs must be red on its own file even though one.rs shrank by one"
    );
    assert_eq!(
        stale,
        vec!["crates/a/src/one.rs: 1 < 2".to_string()],
        "a repaid debt is a stale row to lower, not a refusal"
    );
}

/// The refusal this census exists for, pinned on a tree written for the purpose.
///
/// It is a tree of its own rather than `crates/` because the population is at
/// zero: a test that the real scan finds at least one bare marker would go red
/// exactly when the work succeeds. What the fixture pins is the rule -- a marker
/// with a prose block above it and no reason on its own line is a label, and the
/// block saying something `about` the number does not make it an answer. The
/// row claiming `derived` is refused for a different reason -- a constant cannot
/// follow a reading -- and the row carrying a real reason is the one the rule
/// must not touch, so a version that simply refused every marker cannot pass
/// either.
#[test]
fn a_marker_is_a_label_until_it_carries_its_reason() {
    let root = std::env::temp_dir().join(format!("cog-cap-marker-{}", std::process::id()));
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("numbers.rs"),
        r##"
/// Default capacity for the stream.
// cap: bound
pub const PROSE_ONLY: usize = 256;

// cap: bound
pub const A_LABEL: usize = 8;

// cap: bound — 一小时：值得归因的构建跑几十分钟，更短会把一次构建显示成一串小的
pub const EXPLAINED: usize = 3600;

pub const UNMARKED: usize = 4;

// cap: 凭空捏的
pub const INVENTED: usize = 2;

// cap: derived — 跟着读数走
pub const FOLLOWS_A_READING: usize = 16;
"##,
    )
    .unwrap();
    let rows = scan(&root, &sources_of_tree(&root));
    assert_eq!(
        rows.len(),
        6,
        "the fixture declares six numbers and the scan read {} of them: the walk is \
         broken, not the fixture",
        rows.len()
    );
    let (unmarked, unknown, moves, unreasoned) = refuses(&rows);
    assert_eq!(
        unmarked,
        vec!["crates/probe/src/numbers.rs:UNMARKED = 4".to_string()],
        "a number with no marker at all went unreported"
    );
    assert_eq!(
        unknown,
        vec!["crates/probe/src/numbers.rs:INVENTED says `cap: 凭空捏的`".to_string()],
        "a word outside the vocabulary went unreported"
    );
    assert_eq!(
        moves,
        vec!["crates/probe/src/numbers.rs:FOLLOWS_A_READING = 16".to_string()],
        "a constant claiming to follow a reading went unreported, so the one word a \
         frozen number must never carry still passes"
    );
    assert_eq!(
        unreasoned,
        vec![
            "crates/probe/src/numbers.rs:A_LABEL = 8".to_string(),
            "crates/probe/src/numbers.rs:PROSE_ONLY = 256".to_string(),
        ],
        "a marker with no reason on it went unreported, either because the prose \
         above counted as one or because the marker was never read"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The scan's two easy failures both read as green, so both are pinned: a scan
/// that matches nothing makes every later comparison vacuous, and a scan that
/// ignores the `#[cfg(test)]` rule turns fixture data into debt somebody has to
/// pay.
#[test]
fn the_scan_sees_production_constants_and_not_fixtures() {
    let root = repo_root();
    let scanned = scan(&root, &production_sources(&root));
    assert!(
        scanned.len() > 100,
        "the scan found {} constants, far below what crates/*/src holds: the walk is \
         broken, not the tree",
        scanned.len()
    );
    let has = |file: &str, name: &str| scanned.iter().any(|r| r.file == file && r.name == name);
    assert!(
        has(
            "crates/cog-reflection/src/build_gate/mod.rs",
            "EVENT_RING_CAPACITY"
        ),
        "a production constant the whole build gate is built on was not found"
    );
    assert!(
        !has("crates/cog-core/src/admission.rs", "GB"),
        "a constant inside an inline test module was counted as a production dial"
    );
    // A one-line `mod tests;` declares a module whose body is another file; it
    // is not the end of this file's production code. Treating it as one hid
    // every constant after it in a file that has one near the top.
    assert!(
        has("crates/cog-reflection/src/lib.rs", "CHANGE_EVIDENCE_BUDGET"),
        "a constant declared after a one-line `mod test_support;` was skipped"
    );
}

/// A constant whose type and initializer are written fully qualified holds a
/// number like any other, and the census has to read it.
///
/// Matching only the bare `Duration` spelling made every `std::time::Duration`
/// declaration look like a type the census does not know, so those constants
/// appeared in neither half: not here, and no longer at the call site either,
/// once the call sites were given the name. The number then had no row anywhere
/// -- not an unreviewed one, not a reviewed one, nothing to be red about.
#[test]
fn a_fully_qualified_duration_is_still_a_number() {
    let root = std::env::temp_dir().join(format!("cog-qualified-{}", std::process::id()));
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("dials.rs"),
        r#"
const BARE: Duration = Duration::from_secs(7);
const QUALIFIED: std::time::Duration = std::time::Duration::from_millis(250);
const PLAIN: u64 = 30;
"#,
    )
    .unwrap();
    let value_of = |name: &str| {
        scan(&root, &sources_of_tree(&root))
            .into_iter()
            .find(|r| r.name == name)
            .map(|r| r.value)
    };
    assert_eq!(value_of("BARE").as_deref(), Some("7s"));
    assert_eq!(value_of("QUALIFIED").as_deref(), Some("250ms"));
    assert_eq!(value_of("PLAIN").as_deref(), Some("30"));
    std::fs::remove_dir_all(&root).ok();
}

/// A multiplier, an offset or a unit conversion inside the initializer is a
/// written number like a leading one, and the census has to read it.
///
/// Reading only the first character left `SECS * 3 / 2` in neither register:
/// the census skipped it because it opens with a name, and the use-site scan
/// skips declarations. The 3 and the 2 then had no row anywhere -- no reason
/// to review, and no row to be red about. An initializer that is only a name
/// holds no number of its own and stays out.
#[test]
fn a_number_inside_the_initializer_is_still_typed_in() {
    let root = std::env::temp_dir().join(format!("cog-computed-{}", std::process::id()));
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("dials.rs"),
        r#"
const HALF: u64 = WINDOW * 3 / 2;
const OFFSET: Duration = Duration::from_secs(30 * 60);
const UNITS: usize = NAMES.len() + 1;
const SAME: u64 = OTHER;
"#,
    )
    .unwrap();
    let value_of = |name: &str| {
        scan(&root, &sources_of_tree(&root))
            .into_iter()
            .find(|r| r.name == name)
            .map(|r| r.value)
    };
    assert_eq!(value_of("HALF").as_deref(), Some("WINDOW * 3 / 2"));
    assert_eq!(
        value_of("OFFSET").as_deref(),
        Some("Duration::from_secs(30 * 60)")
    );
    assert_eq!(value_of("UNITS").as_deref(), Some("NAMES.len() + 1"));
    assert_eq!(value_of("SAME"), None);
    std::fs::remove_dir_all(&root).ok();
}

/// A constant declared after an inline test module is production, and the scan
/// has to read on past the module to find it.
///
/// Stopping at the first module is the cheap version of this rule, and it fails
/// in the one direction a register cannot afford: the number is missing from the
/// scan *and* from the register, so neither side of the comparison names it and
/// the gate reads clean. `mainline_deployer.rs` carries a `#[cfg(test)]` item a
/// third of the way in with production below it; the same shape with a module
/// would have hidden every constant under it.
#[test]
fn a_constant_after_a_test_module_is_still_counted() {
    let root = std::env::temp_dir().join(format!("cog-after-module-{}", std::process::id()));
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("dials.rs"),
        r#"
const BEFORE: u64 = 30;

#[cfg(test)]
mod tests {
    const FIXTURE: usize = 12;

    #[test]
    fn a_case() {
        assert_eq!(super::BEFORE, 30);
    }
}

const AFTER: u64 = 45;
"#,
    )
    .unwrap();
    let names: Vec<String> = scan(&root, &sources_of_tree(&root))
        .into_iter()
        .map(|row| row.name)
        .collect();
    assert_eq!(names, vec!["AFTER".to_string(), "BEFORE".to_string()]);
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// The other half: numbers written where they are *used*.
//
// The census above reads declarations. The shape that actually bit us was not a
// declaration at all: the same 1800 seconds typed into three call sites, in
// three crates, where nothing counted it because no `const` was involved. A
// register of declared numbers cannot see that, so a rule that only counted
// declarations would have reported a clean tree while the copies drifted.
//
// So this half counts two shapes that are unambiguous quantities wherever they
// appear -- a literal handed to `Duration::from_*`, and a literal multiplying
// bytes -- anywhere in production code that is not a `const` declaration (those
// belong to the census above) and not an inline test module. A new one is red;
// the total is capped by [`BARE_LITERAL_CEILING`] and the cap only comes down.
//
// Counted per file and per shape, and the breakdown is printed when the cap is
// crossed rather than kept in a file: four hundred rows of `.../mod.rs 1800`
// would name the numbers without making them cheaper to find, and a registry
// that has to be regenerated after every edit is one more document that can say
// the tree is clean when it is not.
// ---------------------------------------------------------------------------

/// The shape names, as they appear in the registry.
const BARE_CADENCE: &str = "cadence";
const BARE_BYTES: &str = "bytes";
/// A bare integer handed to a kubectl call as its wall clock: `kubectl(&[…], 60)`.
///
/// It has its own name because it was invisible to the other two: no
/// `Duration::from_*` (the timeout is a plain `u64` seconds parameter) and no
/// byte multiplier. Fourteen call sites spelled it this way, the same read
/// carried 30 in one place and 60 in another, and nothing counted the pair.
const BARE_KUBECTL_TIMEOUT: &str = "kubectl-timeout";

/// How many use-site literals of both shapes may still exist in total.
///
/// The value is the measured population, not a budget somebody chose. Every
/// row is a number a deployment's behaviour depends on and no caller can move;
/// the ceiling exists so the next one arrives with a decision, which is what
/// makes this a ratchet rather than a count. It sits at zero: every use site
/// now names a constant, so a new literal is a new decision rather than one
/// more row in a debt nobody is paying.
const BARE_LITERAL_CEILING: u64 = 0;

/// 扫描跨行时要带下去的那点状态：raw string 的井号数，以及「这一行结束时还
/// 在一个普通字符串里面」。
///
/// 两样都必须跨行。fixture 里常见的
/// `format!("#[cfg(test)]\nmod tests {{…}}\n")` 是用行尾 `\` 续行的普通字符串
/// 写的，读到第二行时它仍然是字符串，而按行读会把那一行当代码——它净 `-4` 个
/// 花括号，`mod tests` 的深度随即被打穿，之后整个测试模块都按生产代码扫。
#[derive(Default)]
struct ScanState {
    /// raw string 的井号数（`r#"…"#` 是 1）。
    raw: Option<usize>,
    /// 走到行尾时仍在普通字符串里。
    in_str: bool,
}

/// `Duration::from_*(<literal>)` and byte multipliers, per file and shape.
///
/// Line-oriented and deliberately crude: a number on the same line as a `//`
/// comment after it is still counted, because the alternative is half-reading
/// Rust expressions and a half-read expression in a register reads as a
/// decision nobody made. What it must not do is count fixtures or declarations,
/// and both of those exclusions are exact.
fn scan_bare_literals(root: &Path, files: &[PathBuf]) -> BTreeMap<(String, String), usize> {
    let mut out: BTreeMap<(String, String), usize> = BTreeMap::new();
    for entry in files {
        let rel = entry
            .strip_prefix(root)
            .unwrap_or(entry)
            .to_string_lossy()
            .to_string();
        // The population (shipping sources only) is the caller's, the same one
        // `scan` gets. A declaration is excluded here because it is a named
        // number, not a written one.
        if DECLARATION_FILES.contains(&rel.as_str()) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(entry) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        // Two regions are skipped by their braces rather than by a `break`: a
        // `break` would stop reading the file at the first `mod tests`, and a
        // literal below it would then be invisible while the register still
        // read as complete -- an unread byte that reads as a decision.
        let mut depth: i32 = 0;
        let mut fixtures: Option<i32> = None;
        let mut expecting_test_module = false;
        let mut scan = ScanState::default();
        // A kubectl call split across lines, accumulated until its parens close.
        let mut kubectl_window: Option<String> = None;
        for (i, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            let code = code_only(line, &mut scan);
            // `#[cfg(test)]` guards whatever follows it -- a module, or a single
            // `fn for_test` -- but attributes and doc comments may sit in
            // between, so the guard waits for the next line that is code.
            let opens_tests = if trimmed.starts_with("//") {
                false
            } else if trimmed.starts_with("#[") {
                if trimmed.starts_with("#[cfg(test") {
                    expecting_test_module = true;
                }
                false
            } else if expecting_test_module {
                expecting_test_module = false;
                // A guarded module is a fixture block; a guarded item is one
                // fixture whose body the braces bound. A guarded one-liner has
                // no body to bound and only its own line is skipped.
                has_inline_module_body(line, true) || code.contains('{')
            } else {
                has_inline_module_body(line, false)
            };
            let in_fixtures = fixtures.is_some();
            let depth_before = depth;
            depth += code.matches('{').count() as i32 - code.matches('}').count() as i32;
            let opened_tests = opens_tests && fixtures.is_none();
            if opened_tests {
                fixtures = Some(depth_before);
            }
            // The opening line itself may carry no brace (an attribute line, or
            // a signature whose `{` is on the next line), so it must not be the
            // line that closes the span it just opened.
            if !opened_tests {
                if let Some(entry_depth) = fixtures {
                    if depth <= entry_depth {
                        fixtures = None;
                    }
                }
            }
            if in_fixtures || opens_tests || declares_a_const(&code) {
                kubectl_window = None;
                continue;
            }
            // The third shape: a bare integer handed to a kubectl call as its
            // wall clock. This is the one that was invisible to both readers --
            // no `const`, no `Duration::from_*`, just `, 60)` at fourteen call
            // sites with two different values for the same read. A call split
            // across lines is the common spelling (the argv array alone takes a
            // line), so the window collects code until the parens close.
            if kubectl_window.is_some() || code.contains("kubectl(") {
                let window = kubectl_window.get_or_insert_with(String::new);
                window.push(' ');
                window.push_str(&code);
                let open = window.matches('(').count() as i32 - window.matches(')').count() as i32;
                if open <= 0 {
                    let hits = kubectl_timeout_literals(window);
                    if hits > 0 {
                        if sites_wanted() {
                            eprintln!("{rel}:{} {BARE_KUBECTL_TIMEOUT} x{hits}", i + 1);
                        }
                        *out.entry((rel.clone(), BARE_KUBECTL_TIMEOUT.to_string()))
                            .or_default() += hits;
                    }
                    kubectl_window = None;
                }
            }
            let cadence = cadence_literals(&code);
            if cadence > 0 {
                if sites_wanted() {
                    eprintln!("{rel}:{} {BARE_CADENCE} x{cadence}", i + 1);
                }
                *out.entry((rel.clone(), BARE_CADENCE.to_string()))
                    .or_default() += cadence;
            }
            let bytes = byte_literals(&code);
            if bytes > 0 {
                if sites_wanted() {
                    eprintln!("{rel}:{} {BARE_BYTES} x{bytes}", i + 1);
                }
                *out.entry((rel.clone(), BARE_BYTES.to_string()))
                    .or_default() += bytes;
            }
        }
    }
    out
}

/// The line's code: string literals, char literals and any `//` comment removed.
///
/// Braces inside a string move a depth counter, and one assertion on
/// `"outcome=\"reclaimed\"} 1"` would then close the scan's notion of the test
/// module a hundred lines early -- the rest of the module would read as
/// production. A number inside a message is not a dial either, so the same pass
/// serves both readers. Approximate on purpose: the only thing carried across
/// lines is the two ways a string outlives its line — a raw string, and an
/// ordinary one, whether the break is spelled `\` or is a real newline.
/// Measured 2026-11-09: a `format!` fixture continued with a trailing `\` left
/// `mod tests` four braces too shallow (`change_pipeline.rs`), and every test
/// line under it read as production code.
fn code_only(line: &str, scan: &mut ScanState) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if let Some(hashes) = scan.raw {
            let closes = chars[i] == '"' && (0..hashes).all(|k| chars.get(i + 1 + k) == Some(&'#'));
            if closes {
                i += 1 + hashes;
                scan.raw = None;
            } else {
                i += 1;
            }
            continue;
        }
        // Resuming inside an ordinary string: the line that opened it ended
        // before its closing quote, so what is here is payload, not code.
        if scan.in_str {
            match chars[i] {
                // A trailing `\` is the continuation itself, and the string
                // runs on into the next line with `in_str` still set.
                '\\' => i = (i + 2).min(chars.len()),
                '"' => {
                    scan.in_str = false;
                    i += 1;
                }
                _ => i += 1,
            }
            continue;
        }
        let c = chars[i];
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            break;
        }
        // Raw and byte strings open with `r#*"` (`br#*"`); the hash count has to
        // survive across lines, because a shell script in a fixture is very
        // likely to hold an unbalanced brace.
        // The `r` (or the `b` before it) has to start a token. This reader is
        // line-oriented, so a string continued with a trailing `\` is not
        // tracked and its tail is read as code -- a line ending `... accounted
        // for"` then spells `r"`, which opened a raw string that never closed
        // and put every closing brace after it out of the scan's sight.
        // Measured 2026-11-08: one such line ended `mod tests` in
        // change_ledger.rs 446 lines early, and a test-only
        // `Duration::from_secs(60)` inside it counted as production.
        let continues_an_identifier = i > 0
            && chars
                .get(i - 1)
                .is_some_and(|prev| prev.is_alphanumeric() || *prev == '_');
        let mut j = i;
        if c == 'b' {
            j += 1;
        }
        if !continues_an_identifier && chars.get(j) == Some(&'r') {
            let mut hashes = 0;
            let mut k = j + 1;
            while chars.get(k) == Some(&'#') {
                hashes += 1;
                k += 1;
            }
            if chars.get(k) == Some(&'"') {
                scan.raw = Some(hashes);
                i = k + 1;
                continue;
            }
        }
        match c {
            '"' => {
                i += 1;
                let mut closed = false;
                while i < chars.len() {
                    match chars[i] {
                        // At the end of the line this is the continuation, and
                        // the line ends still inside the string.
                        '\\' => i = (i + 2).min(chars.len()),
                        '"' => {
                            i += 1;
                            closed = true;
                            break;
                        }
                        _ => i += 1,
                    }
                }
                if !closed {
                    scan.in_str = true;
                }
            }
            '\'' => {
                let three = (chars.get(i + 1), chars.get(i + 2));
                match three {
                    (Some(inner), Some('\'')) if *inner != '\\' => i += 2 + 1,
                    (Some('\\'), _) => {
                        let mut k = i + 2;
                        while k < chars.len() && chars[k] != '\'' {
                            k += 1;
                        }
                        i = k + 1;
                    }
                    _ => {
                        out.push(c);
                        i += 1;
                    }
                }
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

/// Files whose numbers are declarations by construction.
///
/// `config.rs` is where a deployment's defaults are written down; a default that
/// was not a literal somewhere would have to be read from something, and what it
/// would be read from is a file that has to hold it. The census still reads its
/// `const`s, so a named number there is not invisible -- it is in the other
/// register.
const DECLARATION_FILES: [&str; 1] = ["crates/cog-core/src/config.rs"];

/// A line that declares a constant: its numbers are the census's rows, and
/// counting them here too would ask the same number for two decisions.
fn declares_a_const(line: &str) -> bool {
    let mut rest = line.trim_start();
    for prefix in ["pub(crate) ", "pub(super) ", "pub(in crate) ", "pub "] {
        if let Some(tail) = rest.strip_prefix(prefix) {
            rest = tail.trim_start();
        }
    }
    rest.starts_with("const ") || rest.starts_with("static ")
}

/// Literals handed straight to a `Duration` constructor, ignoring 0 and 1.
///
/// Zero and one are the two numbers that mean a cardinality rather than a
/// duration: "no wait" and "one tick" say what to do, not how long it is, and
/// asking for a name for them would produce names like `ONE`.
fn cadence_literals(code: &str) -> usize {
    const CONSTRUCTORS: [&str; 6] = [
        "from_secs(",
        "from_millis(",
        "from_micros(",
        "from_nanos(",
        "from_secs_f64(",
        "from_secs_f32(",
    ];
    let mut count = 0;
    for ctor in CONSTRUCTORS {
        let mut rest = code;
        while let Some(at) = rest.find(ctor) {
            let after = &rest[at + ctor.len()..];
            let digits: String = after
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '_')
                .collect();
            if !digits.is_empty() && digits != "0" && digits != "1" {
                count += 1;
            }
            rest = after;
        }
    }
    count
}

/// Byte sizes written as a multiple of a kibibyte, per line.
///
/// The unit itself is not the number: `kb * 1024` and `1024 * 1024 * 1024` say
/// "these are bytes", and asking them for a declaration would produce names like
/// `KIBIBYTE`. What is countable is a *quantity* spelled that way -- `24 * 1024`
/// is twenty-four of something -- so this counts a literal times 1024 and skips
/// when that literal is itself 1024 or 1. Shifts are unit conversions by
/// construction and are not counted at all.
fn byte_literals(code: &str) -> usize {
    let mut count = 0;
    let mut rest = code;
    while let Some(at) = rest.find("1024") {
        rest = &rest[at + 4..];
        // `1024.0` and `10240` are one number, not a `1024` and a tail: skipping
        // only the four digits left a ".0" behind that read as a multiplier.
        let longer = rest
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit() || c == '_' || c == '.');
        if longer {
            continue;
        }
        let head = code[..code.len() - rest.len() - 4].trim_end();
        let Some(head) = head.strip_suffix('*') else {
            continue;
        };
        // The whole token, not just its digits: `1024u64 * 1024` has `u64`
        // where a multiplier goes, and reading only the digits turned the type
        // suffix into a `64`. A token with letters in it is not a literal.
        let token: String = head
            .trim_end()
            .chars()
            .rev()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let plain = token.replace('_', "");
        let numeric = plain.chars().all(|c| c.is_ascii_digit() || c == '.');
        if numeric && plain != "1024" && plain != "1024.0" && plain != "1" {
            count += 1;
        }
    }
    count
}

/// The wall clock written straight into a kubectl call, per call.
///
/// The last top-level argument is that call's timeout. Nesting is tracked over
/// brackets as well as parens because the argv is an array: without it every
/// comma between `"get"` and the resource name would read as the argument
/// boundary, and the literal in the middle of the argv would be judged instead.
///
/// Zero and one are exempt for the same reason as the cadence shape: they say
/// "do not wait" and "one tick", not how long to wait.
fn kubectl_timeout_literals(window: &str) -> usize {
    let Some(at) = window.rfind("kubectl(") else {
        return 0;
    };
    let call = &window[at + "kubectl(".len()..];
    let mut depth: i32 = 0;
    let mut last_comma = None;
    let mut close_at = None;
    for (i, c) in call.char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' if depth == 0 => {
                close_at = Some(i);
                break;
            }
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => last_comma = Some(i),
            _ => {}
        }
    }
    let (Some(close_at), Some(last_comma)) = (close_at, last_comma) else {
        return 0;
    };
    let arg = call[last_comma + 1..close_at].trim().replace('_', "");
    let bare = !arg.is_empty() && arg.chars().all(|c| c.is_ascii_digit());
    usize::from(bare && arg != "0" && arg != "1")
}

// Same shape as the reviewed-debt cap above: the ceiling reads zero once the
// debt is paid, and the comparison has to survive it being raised again.
#[allow(clippy::absurd_extreme_comparisons)]
#[test]
fn a_number_at_the_point_of_use_is_counted_where_it_is_used() {
    let root = repo_root();
    let scanned = scan_bare_literals(&root, &production_sources(&root));
    let total: u64 = scanned.values().map(|count| *count as u64).sum();
    let held: Vec<String> = scanned
        .iter()
        .map(|((file, kind), count)| format!("{file} ({kind}): {count}"))
        .collect();
    assert!(
        total <= BARE_LITERAL_CEILING,
        "{total} numbers are still written where they are used and the ceiling is \
         {BARE_LITERAL_CEILING}. The debt only comes down: give the number a declaration \
         (a `const` the census asks about) or make it configurable.\n{}",
        held.join("\n")
    );
}

/// The use-site scan's failures also read as green, so both directions are
/// pinned on a tree written for the purpose: a number above and below a
/// fixture block is counted, the declared one and the fixture's are not, a
/// brace inside a fixture's raw string does not end that block early, and a
/// fixture continued with a trailing backslash does not turn its own second
/// line -- braces, `from_secs(3)` and all -- into production code.
///
/// It is a tree of its own rather than `crates/` because the population is
/// coming down to zero: an assertion that the real scan finds more than N would
/// fail exactly when the work succeeds.
#[test]
fn the_use_site_scan_sees_production_and_not_fixtures() {
    let root = std::env::temp_dir().join(format!("cog-bare-literals-{}", std::process::id()));
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("production.rs"),
        r##"
const DECLARED: Duration = Duration::from_secs(7);

fn wait() {
    tokio::time::sleep(Duration::from_secs(3600)).await;
}

fn call(ctl: &Ctl) {
    ctl.kubectl(&["get", "deployment", "-o", "json"], 60).await;
    ctl.kubectl(
        &["get", "deployment", "-o", "json"],
        KUBECTL_READ_TIMEOUT_SECS,
    ).await;
    ctl.kubectl(&["delete", "job", "x", "--ignore-not-found"], 1).await;
}

/// A message continued with a trailing backslash, whose second line ends in
/// `r"`. The scan reads one line at a time, so that tail is code to it, and a
/// lexer that opens a raw string on any `r` goes blind from here to the end of
/// the file -- everything below would then be either counted twice or missed.
fn a_message() -> &'static str {
    "a batch's commit names one task, so every row behind \
     the anchor was accounted for"
}

/// A fixture written as a string and continued with a trailing backslash, with
/// a real literal on the continuation line. Read as code, that line is the
/// fixture's own body: the braces in it close `mod tests` four too early and
/// the `from_secs(3)` on it counts as a dial this tree turns.
fn write_sleeping_crate(root: &std::path::Path) {
    std::fs::write(
        root.join("src/lib.rs"),
        format!(
            "#[cfg(test)]\nmod tests {{\n    #[test]\n    fn slow() {{\n        \
             std::thread::sleep(std::time::Duration::from_secs(3));\n    }}\n}}\n"
        ),
    )
    .unwrap();
}

#[cfg(test)]
mod tests {
    fn shell() -> &'static str {
        r#"if [ -f x ]; then echo }"#
    }

    fn fixture() -> Duration {
        Duration::from_secs(9)
    }

    fn fixture_call(ctl: &Ctl) {
        ctl.kubectl(&["get", "pods"], 45).await;
    }
}

fn below_the_module() -> Duration {
    Duration::from_secs(11)
}
"##,
    )
    .unwrap();
    // A gate file in the same crate, with the same number typed into it. It is
    // not shipped, so it is not debt -- and this file is what makes the
    // assertion below able to fail: with only the src file present, "every row
    // came from src/" holds no matter how the population is read.
    let tests = root.join("crates/probe/tests");
    std::fs::create_dir_all(&tests).unwrap();
    std::fs::write(
        tests.join("a_gate.rs"),
        "fn a_case() {\n    tokio::time::sleep(Duration::from_secs(3600)).await;\n}\n",
    )
    .unwrap();
    let scanned = scan_bare_literals(&root, &sources_of_tree(&root));
    let row = scanned
        .get(&(
            "crates/probe/src/production.rs".to_string(),
            BARE_CADENCE.to_string(),
        ))
        .copied()
        .unwrap_or(0);
    assert_eq!(
        row, 2,
        "expected the two production numbers (above and below the fixture block) and neither \
         the declared one, the fixture's, nor the one written inside the continued fixture \
         string; the scan found {row}"
    );
    let kubectl_row = scanned
        .get(&(
            "crates/probe/src/production.rs".to_string(),
            BARE_KUBECTL_TIMEOUT.to_string(),
        ))
        .copied()
        .unwrap_or(0);
    assert_eq!(
        kubectl_row, 1,
        "expected only the call whose timeout is a bare number: the named one, the `1`, and the \
         one inside the fixture block are not debt; the scan found {kubectl_row}"
    );
    assert!(
        scanned.keys().all(|(file, _)| file.contains("/src/")),
        "a test or fixture file was counted as production hardcoding: {:?}",
        scanned.keys().collect::<Vec<_>>()
    );
    std::fs::remove_dir_all(&root).ok();
}

/// 界形的常量不许标成 `fixed`。
///
/// 常量的词表只有两个词：`bound`（被某个事实钉住，理由说明是什么钉的）与
/// `fixed`（不是旋钮，是一项约定，比如协议端口）。而一个名字就是界形的常量
/// ——`MAX_*`、`MIN_*`、`*_LIMIT`、`*_RETRY`、`*_TIMEOUT`、`*_CAP`——既不是协议常数，
/// 也不是"量出来的"：它是一条把未发生的事拦在外面的规则。
///
/// 这种数的合法归宿与图里那张表一样：**被一条读数导出，或者不存在**。名单是
/// 2026-11-16 的读数，**只减不增**：新提出一条界形常量标 `fixed` 就是红。
const BOUND_SHAPED_FIXED_AT_CENSUS: &[&str] = &[
    "MAX_RECOVERED_RETRY_AFTER_SECS",
    "DEFAULT_MAX_CONTEXT_TOKENS",
    "MAX_KEY_LEN",
    "MAX_INBOUND_FRAME_BYTES",
    "MAX_UTF8_BYTES_PER_CHAR",
    "PUSH_RETRY_CEILING_SECONDS",
    "PUSH_RETRY_SECONDS",
    "PUSH_TIMEOUT_SECS",
    "SUSPECT_BACKOFF_CAP_SECS",
    "MAX_ATTACHMENT_BYTES",
    "MAX_LENGTH",
    "MIN_INTERVAL_SECS",
    "MIN_REPLICAS",
    "MAX_CHARS",
];

/// 名字里出现这些片段，就算界形。
const BOUND_SHAPED_NAME_PARTS: &[&str] = &[
    "MAX",
    "MIN",
    "CAP",
    "LIMIT",
    "RETRY",
    "RETRIES",
    "TIMEOUT",
    "CEILING",
    "THRESHOLD",
];

#[test]
fn a_bound_shaped_constant_is_never_fixed() {
    let root = repo_root();
    let scanned = scan(&root, &production_sources(&root));
    let flagged: Vec<String> = scanned
        .iter()
        .filter(|row| row.source == "fixed")
        .filter(|row| {
            BOUND_SHAPED_NAME_PARTS
                .iter()
                .any(|part| row.name.contains(part))
        })
        .map(|row| format!("{} {} = {}", row.file, row.name, row.value))
        .collect();
    let known: std::collections::BTreeSet<&str> =
        BOUND_SHAPED_FIXED_AT_CENSUS.iter().copied().collect();
    let new: Vec<&String> = flagged
        .iter()
        .filter(|line| {
            let name = line.split_whitespace().nth(1).unwrap_or_default();
            !known.contains(name)
        })
        .collect();
    assert!(
        new.is_empty(),
        "这几条名字就是界形的常量却标着 `fixed`：\n{}\n\
         界不是约定：它要么由一条读数导出（`bound` + 理由写明是什么钉的），\
         要么删掉，改用进度检测与事实反馈。",
        new.iter()
            .map(|line| format!("  {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
