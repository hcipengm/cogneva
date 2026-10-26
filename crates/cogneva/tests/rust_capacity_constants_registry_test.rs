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
const DEBT_CEILING: usize = 168;

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
        for (i, line) in lines.iter().enumerate() {
            let next = lines[i + 1..]
                .iter()
                .find(|l| !l.trim().is_empty())
                .copied();
            if opens_inline_test_module(line, next) {
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
fn opens_inline_test_module(line: &str, next: Option<&str>) -> bool {
    if has_inline_module_body(line) {
        return true;
    }
    line.trim() == "#[cfg(test)]" && next.is_some_and(has_inline_module_body)
}

fn has_inline_module_body(line: &str) -> bool {
    let trimmed = line.trim();
    let after_vis = trimmed
        .strip_prefix("pub(")
        .and_then(|rest| rest.find(')').map(|i| rest[i + 1..].trim_start()))
        .or_else(|| trimmed.strip_prefix("pub ").map(str::trim_start))
        .unwrap_or(trimmed);
    ["mod tests", "mod test_support"].iter().any(|module| {
        after_vis
            .strip_prefix(module)
            .is_some_and(|rest| rest.trim_start().starts_with('{'))
    })
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
            "BUILD_DEMAND_CORES"
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
