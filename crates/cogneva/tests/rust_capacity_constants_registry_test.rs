//! Every capacity or cadence number written in Rust has to say where it comes
//! from, and a new one fails this test until it does.
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
//! The names it looks for are the ones whose subject *is* a quantity: bytes,
//! sizes, caps, intervals, timeouts, budgets. Restricting the scan this way is
//! the difference between a registry and a lint that everyone disables: a
//! `u64` holding a protocol version or a bit width is not a number about this
//! machine.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Where the registry lives, relative to the workspace root.
const REGISTRY: &str = "crates/cogneva/tests/data/rust-capacity-constants.tsv";

/// The words that make a constant's subject a quantity rather than a value.
///
/// Matched against the constant's name: `MAX_BYTES`, `SCAN_INTERVAL_SECS`,
/// `ASSESS_TASK_TIMEOUT`, `CACHE_HEADROOM_PERCENT`, `GC_OVERHEAD_MULTIPLE`.
/// Anything else -- a port, a
/// protocol version, a channel count -- is not a statement about capacity.
const QUANTITY_WORDS: &[&str] = &[
    "SECS",
    "SECONDS",
    "MS",
    "MILLIS",
    "INTERVAL",
    "TIMEOUT",
    "TTL",
    "BACKOFF",
    "BYTES",
    "MB",
    "MIB",
    "GB",
    "GIB",
    "KIB",
    "LIMIT",
    "MAX",
    "MIN",
    "CAP",
    "SIZE",
    "COUNT",
    "CONCURREN",
    "REPLICA",
    "WORKSPACE",
    "THRESHOLD",
    "RETENTION",
    "BUDGET",
    "QUOTA",
    "PARALLEL",
    "HEADROOM",
    "DEPTH",
    "AGE",
    "WINDOW",
    "WEIGHT",
    "RATIO",
    "MULTIPLE",
    "FRACTION",
    "PERCENT",
];

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

/// Every `const NAME: integer-or-float = <literal>;` whose name says it is a
/// quantity.
///
/// Deliberately syntactic: an initializer that is an expression is already a
/// computed value, and that is the shape this registry wants more of, so it is
/// not an entry. Only a bare numeric literal is a written number.
fn scan(root: &Path) -> Vec<Row> {
    let mut rows = Vec::new();
    let crates = root.join("crates");
    for entry in walk_rs(&crates) {
        let rel = entry
            .strip_prefix(root)
            .unwrap_or(&entry)
            .to_string_lossy()
            .to_string();
        if rel.contains("/tests/") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&entry) else {
            continue;
        };
        for line in text.lines() {
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
    let line = line
        .strip_prefix("pub(crate) ")
        .or_else(|| line.strip_prefix("pub "))
        .unwrap_or(line);
    let line = line.strip_prefix("const ")?;
    let (name, rest) = line.split_once(':')?;
    let name = name.trim();
    let rest = rest.trim_start();
    let ty = rest.split('=').next()?.trim();
    let value = rest.split_once('=')?.1.trim().trim_end_matches(';').trim();
    let value = match ty {
        "u64" | "u32" | "u16" | "usize" | "i64" | "i32" | "f64" | "f32" => {
            // Only a literal. Anything with an operator, a call or a cast is a
            // number computed at compile time from other values, which is the
            // shape this registry is trying to move towards rather than police.
            if !value
                .chars()
                .all(|c| c.is_ascii_digit() || c == '_' || c == '.')
            {
                return None;
            }
            value.to_string()
        }
        // A cadence written as `Duration::from_secs(120)` is the same kind of
        // number as `120` in a `u64`, and reading only the integers left the
        // most common spelling of a period outside the registry altogether.
        "Duration" => {
            let unit = ["secs", "millis", "hours", "mins"].iter().find_map(|u| {
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
            })?;
            unit
        }
        _ => return None,
    };
    if value
        .trim_matches(|c| c == '_' || c == '.' || c == '0')
        .is_empty()
    {
        // Zero is the "unset" sentinel far more often than it is a capacity,
        // and a registry that lists every zero buries the ones that matter.
        return None;
    }
    if !QUANTITY_WORDS.iter().any(|w| name.contains(w)) {
        return None;
    }
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
        "# Capacity and cadence numbers written in Rust. `source` is either the reading the\n\
         # value is derived from or the reason it is a bound; `unreviewed` is debt, not an\n\
         # answer. Regenerate with COGNEVA_CAPACITY_REGISTRY_WRITE=1 cargo test -p cogneva\n\
         # --test rust_capacity_constants_registry_test\n\
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
}
