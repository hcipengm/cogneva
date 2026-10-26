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
//! The registry is a snapshot rather than a filter: the entries are the
//! constants that exist today, `source` says which kind each one is, and the
//! test fails in both directions -- a constant with no entry (a new hardcode),
//! and an entry with no constant (a stale row pretending something is still
//! governed). `COGNEVA_CAPACITY_REGISTRY_WRITE=1` rewrites the file from the
//! tree, so maintaining it is one command and never hand-editing.
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
//! that a given one is not a dial belongs in the `source` column, where it is
//! read by a person, rather than in a name filter, where it is made by a string
//! match.
//!
//! The rows that have not been judged carry `source = unreviewed`, which is debt
//! and not an answer. Their number is capped by [`DEBT_CEILING`] and the cap
//! only comes down: a constant added tomorrow cannot be parked there, because
//! there is no room, so it has to arrive with a source and a note.
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

/// Where the registry lives, relative to the workspace root.
const REGISTRY: &str = "crates/cogneva/tests/data/rust-capacity-constants.tsv";

/// How many rows may still be `unreviewed`.
///
/// The ceiling is the gate's teeth: with a finitely large debt, adding a
/// constant without deciding what it is would go unnoticed, because the file
/// would simply grow. Pinning the count means the next one arrives with an
/// answer, and paying a debt is the only thing that makes room.
const DEBT_CEILING: usize = 159;

/// One line of the registry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    file: String,
    name: String,
    value: String,
    source: String,
    note: String,
}

fn workspace_root() -> PathBuf {
    // The test binary runs from the workspace root under `cargo test`; walking
    // up from the manifest is what makes it work from a subdirectory too.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cogneva sits two levels below the workspace root")
        .to_path_buf()
}

/// Every `const NAME: <numeric type> = <initializer starting with a digit>;`.
///
/// Deliberately syntactic. A number computed from other constants at compile
/// time (`const SIXTY: u64 = SECS * 60;`) is not a written number and is not an
/// entry; `const LIMIT: u64 = 4096 * 1024;` is, because the 4096 is typed in and
/// no measurement will move it. The initializer's first character is the whole
/// test -- parsing expressions would mean half-reading them, and a half-read
/// expression in a registry reads as a decision.
fn scan(root: &Path) -> Vec<Row> {
    let mut rows = Vec::new();
    for entry in walk_rs(&root.join("crates")) {
        let rel = entry
            .strip_prefix(root)
            .unwrap_or(&entry)
            .to_string_lossy()
            .to_string();
        // A crate's own `tests/`, `benches/` and `examples/` are not shipped
        // bounds: a fixture's numbers are the fixture's business.
        if !rel.contains("/src/") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&entry) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        let mut expecting_test_module = false;
        for line in lines.iter() {
            let trimmed = line.trim();
            let is_test_module = if trimmed.starts_with("//") {
                false
            } else if trimmed.starts_with("#[") {
                if trimmed.starts_with("#[cfg(test") {
                    expecting_test_module = true;
                }
                false
            } else if expecting_test_module {
                expecting_test_module = false;
                has_inline_module_body(line, true)
            } else {
                has_inline_module_body(line, false)
            };
            if is_test_module {
                break;
            }
            let Some(row) = parse_const(line, &rel) else {
                continue;
            };
            rows.push(row);
        }
    }
    rows.sort();
    rows.dedup();
    rows
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

fn walk_rs(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_rs(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out
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
    let ty = rest.split('=').next()?.trim();
    // One line, one constant: the initializer has to close on the same line.
    let value = rest.split_once('=')?.1.trim();
    let value = value.strip_suffix(';')?.trim();
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
        source: "unreviewed".to_string(),
        note: String::new(),
    })
}

fn read_registry(root: &Path) -> Vec<Row> {
    let path = root.join(REGISTRY);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        // The column line describes the file rather than being a row in it.
        .filter(|l| !l.starts_with("file\t"))
        .filter_map(|l| {
            let mut f = l.split('\t');
            Some(Row {
                file: f.next()?.to_string(),
                name: f.next()?.to_string(),
                value: f.next()?.to_string(),
                source: f.next()?.to_string(),
                note: f.next().unwrap_or("").to_string(),
            })
        })
        .collect()
}

fn write_registry(root: &Path, scanned: &[Row], existing: &[Row]) {
    let carried: BTreeMap<(&str, &str), &Row> = existing
        .iter()
        .map(|r| ((r.file.as_str(), r.name.as_str()), r))
        .collect();
    let mut out = String::from(
        "# Every number written into a Rust constant: one row per `const`, so a number that\n\
         # arrives without a decision about it is red rather than invisible. `source` is\n\
         # either the reading the value is derived from (`derived`) or the reason it is a\n\
         # bound (`bound`); `unreviewed` is debt, not an answer, and its row count is capped\n\
         # in the test that reads this file. Regenerate with\n\
         # COGNEVA_CAPACITY_REGISTRY_WRITE=1 cargo test -p cogneva --test\n\
         # rust_capacity_constants_registry_test\n\
         file\tname\tvalue\tsource\tnote\n",
    );
    for row in scanned {
        let mut row = row.clone();
        if let Some(prev) = carried.get(&(row.file.as_str(), row.name.as_str())) {
            if prev.source != "unreviewed" {
                row.source = prev.source.clone();
                row.note = prev.note.clone();
            }
        }
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\n",
            row.file, row.name, row.value, row.source, row.note
        ));
    }
    let path = root.join(REGISTRY);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&path, out).expect("write the registry");
}

#[test]
fn every_capacity_number_in_rust_says_where_it_comes_from() {
    let root = workspace_root();
    let scanned = scan(&root);
    let existing = read_registry(&root);
    if std::env::var("COGNEVA_CAPACITY_REGISTRY_WRITE").as_deref() == Ok("1") {
        write_registry(&root, &scanned, &existing);
        return;
    }
    assert!(
        !existing.is_empty(),
        "no registry at {REGISTRY}: run COGNEVA_CAPACITY_REGISTRY_WRITE=1 cargo test -p \
         cogneva --test rust_capacity_constants_registry_test"
    );
    let registered: BTreeMap<(&str, &str), &Row> = existing
        .iter()
        .map(|r| ((r.file.as_str(), r.name.as_str()), r))
        .collect();
    let mut missing = Vec::new();
    for row in &scanned {
        match registered.get(&(row.file.as_str(), row.name.as_str())) {
            None => missing.push(format!(
                "{file}:{name} = {value}",
                file = row.file,
                name = row.name,
                value = row.value
            )),
            Some(old) if old.value != row.value => missing.push(format!(
                "{file}:{name} changed {old} -> {new}: say where the new number comes from",
                file = row.file,
                name = row.name,
                old = old.value,
                new = row.value
            )),
            Some(_) => {}
        }
    }
    let present: BTreeMap<(&str, &str), ()> = scanned
        .iter()
        .map(|r| ((r.file.as_str(), r.name.as_str()), ()))
        .collect();
    let stale: Vec<String> = existing
        .iter()
        .filter(|r| !present.contains_key(&(r.file.as_str(), r.name.as_str())))
        .map(|r| format!("{}:{} (gone)", r.file, r.name))
        .collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "the capacity registry is out of step with the tree.\n\
         New or changed numbers (register them, saying which reading they follow or why they \
         are a bound):\n{}\nStale rows (the constant is gone; regenerate):\n{}\n\
         Regenerate with COGNEVA_CAPACITY_REGISTRY_WRITE=1 cargo test -p cogneva --test \
         rust_capacity_constants_registry_test",
        missing.join("\n"),
        stale.join("\n")
    );

    // The ceiling is what makes a row an answer rather than a place to park a
    // number: regeneration writes new rows as `unreviewed`, so without a cap the
    // file would absorb any new constant and every later reader would see a
    // green gate. With the cap the regeneration step succeeds and the next run
    // says how many decisions are owed.
    let unreviewed: Vec<&Row> = existing
        .iter()
        .filter(|r| r.source == "unreviewed")
        .collect();
    assert!(
        unreviewed.len() <= DEBT_CEILING,
        "{} constants are registered as `unreviewed` and the ceiling is {DEBT_CEILING}. \
         The debt only comes down: give the new ones a `derived` source naming the reading \
         they follow, or a `bound` source saying why no measurement should move them.\n{}",
        unreviewed.len(),
        unreviewed
            .iter()
            .map(|r| format!("{}:{} = {}", r.file, r.name, r.value))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// The scan's two easy failures both read as green, so both are pinned: a scan
/// that matches nothing makes every later comparison vacuous, and a scan that
/// ignores the `#[cfg(test)]` rule turns fixture data into debt somebody has to
/// pay.
#[test]
fn the_scan_sees_production_constants_and_not_fixtures() {
    let root = workspace_root();
    let scanned = scan(&root);
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
// Counted per file and per shape rather than one row each. Four hundred rows of
// `.../mod.rs 1800` would name the numbers without making them cheaper to find,
// and the thing a reader needs from this file is which files still hold the
// hardcoding and how much of it.
// ---------------------------------------------------------------------------

/// Where the use-site numbers are registered, relative to the workspace root.
const BARE_LITERALS: &str = "crates/cogneva/tests/data/rust-bare-literals.tsv";

/// The shape names, as they appear in the registry.
const BARE_CADENCE: &str = "cadence";
const BARE_BYTES: &str = "bytes";

/// How many use-site literals of both shapes may still exist in total.
///
/// The value is the measured population, not a budget somebody chose. Every
/// row is a number a deployment's behaviour depends on and no caller can move;
/// the ceiling exists so the next one arrives with a decision, which is what
/// makes this a ratchet rather than a count.
const BARE_LITERAL_CEILING: usize = 101;

/// `Duration::from_*(<literal>)` and byte multipliers, per file and shape.
///
/// Line-oriented and deliberately crude: a number on the same line as a `//`
/// comment after it is still counted, because the alternative is half-reading
/// Rust expressions and a half-read expression in a register reads as a
/// decision nobody made. What it must not do is count fixtures or declarations,
/// and both of those exclusions are exact.
fn scan_bare_literals(root: &Path) -> BTreeMap<(String, String), usize> {
    let mut out: BTreeMap<(String, String), usize> = BTreeMap::new();
    for entry in walk_rs(&root.join("crates")) {
        let rel = entry
            .strip_prefix(root)
            .unwrap_or(&entry)
            .to_string_lossy()
            .to_string();
        if !rel.contains("/src/") || DECLARATION_FILES.contains(&rel.as_str()) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&entry) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        // Two regions are skipped by their braces rather than by a `break`: a
        // `break` would stop reading the file at the first `mod tests`, and a
        // literal below it would then be invisible while the register still
        // read as complete -- an unread byte that reads as a decision.
        let mut depth: i32 = 0;
        let mut defaults: Option<i32> = None;
        let mut fixtures: Option<i32> = None;
        let mut expecting_test_module = false;
        for line in lines.iter() {
            let trimmed = line.trim();
            let code = code_only(line);
            // `#[cfg(test)]` guards the module that follows it, but attributes
            // and doc comments may sit in between, so the guard waits for the
            // next line that is code.
            let opens_tests = if trimmed.starts_with("//") {
                false
            } else if trimmed.starts_with("#[") {
                if trimmed.starts_with("#[cfg(test") {
                    expecting_test_module = true;
                }
                false
            } else if expecting_test_module {
                expecting_test_module = false;
                has_inline_module_body(line, true)
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
            // A `Default` impl writes the same thing down in a function instead
            // of in a `const`, and the census reaches it the same way, so its
            // numbers are declarations too.
            let opens_defaults = code.contains("fn default(");
            let declared = defaults.is_some() || opens_defaults;
            let opened_defaults = opens_defaults && defaults.is_none();
            if opened_defaults {
                defaults = Some(depth_before);
            }
            if !opened_defaults {
                if let Some(entry_depth) = defaults {
                    if depth <= entry_depth {
                        defaults = None;
                    }
                }
            }
            if declared {
                continue;
            }
            let cadence = cadence_literals(&code);
            if cadence > 0 {
                *out.entry((rel.clone(), BARE_CADENCE.to_string()))
                    .or_default() += cadence;
            }
            let bytes = byte_literals(&code);
            if bytes > 0 {
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
fn code_only(line: &str) -> String {
    let mut out = String::new();
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '/' if chars.peek() == Some(&'/') => break,
            '"' => {
                while let Some(c) = chars.next() {
                    match c {
                        '\\' => {
                            chars.next();
                        }
                        '"' => break,
                        _ => {}
                    }
                }
            }
            '\'' => {
                let mut probe = chars.clone();
                match (probe.next(), probe.next()) {
                    (Some(inner), Some('\'')) if inner != '\\' => {
                        chars.next();
                        chars.next();
                    }
                    (Some('\\'), _) => {
                        // `'\n'`, `'\''` and friends: consume to the closing quote.
                        chars.next();
                        for _ in 0..2 {
                            match chars.next() {
                                Some('\'') => break,
                                Some(_) => {}
                                None => break,
                            }
                        }
                    }
                    _ => out.push(c),
                }
            }
            _ => out.push(c),
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
        let number: String = head
            .trim_end()
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit() || *c == '_' || *c == '.')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let plain = number.replace('_', "");
        if !number.is_empty() && plain != "1024" && plain != "1024.0" && plain != "1" {
            count += 1;
        }
    }
    count
}

fn read_bare_literals(root: &Path) -> Vec<(String, String, usize)> {
    let path = root.join(BARE_LITERALS);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.starts_with("file\t") && !l.trim().is_empty())
        .filter_map(|l| {
            let mut f = l.split('\t');
            Some((
                f.next()?.to_string(),
                f.next()?.to_string(),
                f.next()?.parse().ok()?,
            ))
        })
        .collect()
}

fn write_bare_literals(root: &Path, scanned: &BTreeMap<(String, String), usize>) {
    let mut out = String::from(
        "# Numbers written where they are used rather than declared: a literal handed to a\n\
         # `Duration` constructor, or a byte multiplier. One row per file and shape; `count`\n\
         # is how many are still in that file. The const census does not see these, which is\n\
         # how the same period could be typed into three call sites unnoticed. A new one is\n\
         # red, and the total is capped in the test that reads this file. Regenerate with\n\
         # COGNEVA_BARE_LITERAL_REGISTRY_WRITE=1 cargo test -p cogneva --test\n\
         # rust_capacity_constants_registry_test\n\
         file\tkind\tcount\n",
    );
    for ((file, kind), count) in scanned {
        out.push_str(&format!("{file}\t{kind}\t{count}\n"));
    }
    let path = root.join(BARE_LITERALS);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&path, out).expect("write the bare literal registry");
}

#[test]
fn a_number_at_the_point_of_use_is_counted_where_it_is_used() {
    let root = workspace_root();
    let scanned = scan_bare_literals(&root);
    let existing = read_bare_literals(&root);
    if std::env::var("COGNEVA_BARE_LITERAL_REGISTRY_WRITE").as_deref() == Ok("1") {
        write_bare_literals(&root, &scanned);
        return;
    }
    let present: BTreeMap<(String, String), usize> = existing
        .iter()
        .map(|(f, k, c)| ((f.clone(), k.clone()), *c))
        .collect();
    let mut drifted = Vec::new();
    for ((file, kind), count) in &scanned {
        match present.get(&(file.clone(), kind.clone())) {
            None => drifted.push(format!("{file} ({kind}): {count} new, unregistered")),
            Some(old) if old != count => drifted.push(format!(
                "{file} ({kind}): {old} -> {count}: say where the new number comes from"
            )),
            Some(_) => {}
        }
    }
    for (file, kind, count) in &existing {
        if !scanned.contains_key(&(file.clone(), kind.clone())) {
            drifted.push(format!(
                "{file} ({kind}): {count} gone (regenerate the file)"
            ));
        }
    }
    assert!(
        drifted.is_empty(),
        "use-site numbers moved.\n{}\nRegenerate with COGNEVA_BARE_LITERAL_REGISTRY_WRITE=1 \
         cargo test -p cogneva --test rust_capacity_constants_registry_test",
        drifted.join("\n")
    );

    let total: usize = existing.iter().map(|(_, _, count)| count).sum();
    assert!(
        total <= BARE_LITERAL_CEILING,
        "{total} numbers are still written where they are used and the ceiling is \
         {BARE_LITERAL_CEILING}. The debt only comes down: give the number a declaration \
         (a `const` the census asks about) or make it configurable."
    );
}

/// The use-site scan's failures also read as green, so both directions are
/// pinned: it must find the shape it exists for, and it must not read a
/// fixture's durations as production hardcoding.
#[test]
fn the_use_site_scan_sees_production_and_not_fixtures() {
    let root = workspace_root();
    let scanned = scan_bare_literals(&root);
    let total: usize = scanned.values().sum();
    assert!(
        total > 50,
        "the use-site scan found {total} numbers, far below what crates/*/src holds: the \
         walk or the shape match is broken, not the tree"
    );
    assert!(
        scanned.keys().all(|(file, _)| file.contains("/src/")),
        "a test or fixture file was counted as production hardcoding"
    );
    // A number inside a declaration is the census's row, not this one's: the
    // shared heartbeat is declared as a `const`, so this scan must be silent
    // about it even though the file holds a `from_secs` call.
    assert!(
        !scanned.contains_key(&(
            "crates/cog-core/src/reading_cadence.rs".to_string(),
            BARE_CADENCE.to_string()
        )),
        "a number in a `const` declaration was counted as a use-site number"
    );
}
