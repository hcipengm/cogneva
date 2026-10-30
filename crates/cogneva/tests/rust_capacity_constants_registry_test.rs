//! Every number written into a Rust constant has to say where it comes from,
//! and a new one fails this test until it does.
//!
//! The chart has had `deploy/helm/cogneva/tunables.yaml` for this: one row per
//! numeric leaf, each row either naming the reading the value is derived from
//! or saying why it is a bound rather than a measurement. Rust had nothing, so
//! a number describing the machine could be typed into a `const` and nothing
//! anywhere asked where it came from -- which is how a hand-measured figure
//! outlives the measurement and starts reading as a fact about the deployment.
//!
//! The answer lives on the constant, as a `// cap: derived` or `// cap: bound`
//! line in the comment block right above it: one word for which kind of answer
//! this is, and -- when the block carries no prose of its own -- a reason after
//! a dash. It used to live in a table beside the code with a row per constant,
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
//! **It enumerates every written number, not only the ones whose name reads
//! like a quantity.** The first version filtered by name -- bytes, sizes, caps,
//! intervals -- on the argument that a `u64` holding a protocol version is not
//! a number about this machine. The argument was about half right and cost more
//! than it saved: the filter is a guess about a name, and the numbers it guessed
//! wrong are exactly the ones nobody then looks at (`RENDER_ATTEMPTS`,
//! `DEFAULT_SEARCH_RESULTS`, `CHARS_PER_ESTIMATED_TOKEN`). A number a
//! deployment's behaviour depends on does not become harmless because its name
//! does not contain `LIMIT`. So the population is now every `const` of a numeric
//! type or `Duration` whose initializer starts with a digit, and the judgement
//! that a given one is not a dial belongs in the `cap:` word, where it is read
//! by a person, rather than in a name filter, where it is made by a string
//! match.
//!
//! There is no third answer and no place to park one: a constant that arrives
//! without a `cap:` line is red, and the two words are the whole closed set. A
//! debt column would have been the easy shape and the wrong one -- "not judged
//! yet" is exactly what a registry of every number was supposed to end.
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

mod common;

use common::routes::repo_root;

use common::rust_production::{production_sources, sources_of_tree};

/// The word that answers for a constant, in the comment block above it.
const CAP_MARKER: &str = "// cap:";

/// The whole closed set of answers.
const CAP_SOURCES: [&str; 2] = ["derived", "bound"];

/// One written number: where it is, what it says, and what the block above it
/// answered.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    file: String,
    name: String,
    value: String,
    /// `derived` or `bound`; empty when the constant carries no marker.
    source: String,
    /// The reason after the dash, when the marker carries one.
    note: String,
    /// Whether the block above carries prose of its own.
    doc: bool,
}

/// Every `const NAME: <numeric type> = <initializer starting with a digit>;`
/// in `files`.
///
/// Deliberately syntactic. A number computed from other constants at compile
/// time (`const SIXTY: u64 = SECS * 60;`) is not a written number and is not an
/// entry; `const LIMIT: u64 = 4096 * 1024;` is, because the 4096 is typed in and
/// no measurement will move it. The initializer's first character is the whole
/// test -- parsing expressions would mean half-reading them, and a half-read
/// expression in a registry reads as a decision.
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
    let mut raw: Option<usize> = None;
    let mut above = Above::default();
    for line in text.lines() {
        // 这一行的判词是**上一行之前**攒下来的那一块：常量自己这一行会把块清空，
        // 所以先留一份快照，再让 observe 处理本行。
        let block_above = above.clone();
        above.observe(line);
        let trimmed = line.trim();
        let code = code_only(line, &mut raw);
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
        row.doc = block_above.doc;
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
    /// Whether the block carries prose of its own, which is what makes a bare
    /// `// cap: bound` an answer rather than a label with nothing behind it.
    doc: bool,
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
            self.doc = true;
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
        // Any initializer that starts with a number is a written number,
        // arithmetic included: `4096 * 1024` is two typed digits and no
        // measurement, and calling it "computed" is what let the class hide.
        "u64" | "u32" | "u16" | "u8" | "usize" | "u128" | "i64" | "i32" | "i16" | "i8" | "i128"
        | "f64" | "f32" => {
            let mut chars = value.chars();
            let first = chars.next()?;
            let numeric = first.is_ascii_digit()
                || ((first == '-' || first == '+') && chars.next()?.is_ascii_digit());
            if !numeric {
                return None;
            }
            value.to_string()
        }
        // A cadence written as `Duration::from_secs(120)` is the same kind of
        // number as `120` in a `u64`, and reading only the integers left the
        // most common spelling of a period outside the registry altogether.
        "Duration" => ["secs", "millis", "hours", "mins"].iter().find_map(|u| {
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
        })?,
        _ => return None,
    };
    Some(Row {
        file: file.to_string(),
        name: name.to_string(),
        value: value.to_string(),
        source: String::new(),
        note: String::new(),
        doc: false,
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
/// A bare `// cap: bound` is only an answer when the block above it says why, so
/// a marker with no reason behind it must carry the reason itself. That is what
/// keeps this from degrading into a label every new constant gets and nobody
/// reads.
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

    let mut unmarked = Vec::new();
    let mut unknown = Vec::new();
    let mut unreasoned = Vec::new();
    for row in &scanned {
        if row.source.is_empty() {
            unmarked.push(format!("{}:{} = {}", row.file, row.name, row.value));
            continue;
        }
        if !CAP_SOURCES.contains(&row.source.as_str()) {
            unknown.push(format!(
                "{}:{} says `cap: {}`",
                row.file, row.name, row.source
            ));
            continue;
        }
        // A reason in the marker stands on its own; without one, the doc block
        // above has to carry it -- which `Above` records as `doc`.
        if row.note.is_empty() && !row.doc {
            unreasoned.push(format!("{}:{} = {}", row.file, row.name, row.value));
        }
    }
    assert!(
        unmarked.is_empty(),
        "these written numbers say nothing about where they come from. Put a line in \
         the block above each one -- `// cap: derived` when it follows a reading, \
         `// cap: bound` when no measurement should move it -- and, if the block has no \
         prose of its own, the reason after a dash:\n{}",
        unmarked.join("\n")
    );
    assert!(
        unknown.is_empty(),
        "these say something the vocabulary does not have; the set is {CAP_SOURCES:?}:\n{}",
        unknown.join("\n")
    );
    assert!(
        unreasoned.is_empty(),
        "these carry a `cap:` word with no reason anywhere above it -- a bare label is \
         the answer a reader cannot check:\n{}",
        unreasoned.join("\n")
    );
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

/// How many use-site literals of both shapes may still exist in total.
///
/// The value is the measured population, not a budget somebody chose. Every
/// row is a number a deployment's behaviour depends on and no caller can move;
/// the ceiling exists so the next one arrives with a decision, which is what
/// makes this a ratchet rather than a count. It sits at zero: every use site
/// now names a constant, so a new literal is a new decision rather than one
/// more row in a debt nobody is paying.
const BARE_LITERAL_CEILING: u64 = 0;

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
        let mut raw: Option<usize> = None;
        for (i, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            let code = code_only(line, &mut raw);
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
                continue;
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
/// serves both readers. Approximate on purpose: a raw string spanning lines is
/// not tracked, and every line starts outside one.
fn code_only(line: &str, raw: &mut Option<usize>) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if let Some(hashes) = *raw {
            let closes = chars[i] == '"' && (0..hashes).all(|k| chars.get(i + 1 + k) == Some(&'#'));
            if closes {
                i += 1 + hashes;
                *raw = None;
            } else {
                i += 1;
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
        let mut j = i;
        if c == 'b' {
            j += 1;
        }
        if chars.get(j) == Some(&'r') {
            let mut hashes = 0;
            let mut k = j + 1;
            while chars.get(k) == Some(&'#') {
                hashes += 1;
                k += 1;
            }
            if chars.get(k) == Some(&'"') {
                *raw = Some(hashes);
                i = k + 1;
                continue;
            }
        }
        match c {
            '"' => {
                i += 1;
                while i < chars.len() {
                    match chars[i] {
                        '\\' => i += 2,
                        '"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
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
/// fixture block is counted, the declared one and the fixture's are not, and a
/// brace inside a fixture's raw string does not end that block early.
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

#[cfg(test)]
mod tests {
    fn shell() -> &'static str {
        r#"if [ -f x ]; then echo }"#
    }

    fn fixture() -> Duration {
        Duration::from_secs(9)
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
        "expected the two production numbers (above and below the fixture block) and          neither the declared one nor the fixture's; the scan found {row}"
    );
    assert!(
        scanned.keys().all(|(file, _)| file.contains("/src/")),
        "a test or fixture file was counted as production hardcoding: {:?}",
        scanned.keys().collect::<Vec<_>>()
    );
    std::fs::remove_dir_all(&root).ok();
}
