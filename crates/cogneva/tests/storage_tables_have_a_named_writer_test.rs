//! Every table the migrations create is named by some code that is not a test.
//!
//! A table nobody writes is not storage: it is a promise made in a migration and
//! never kept. It costs a row in the schema dump, a line in every backup, and --
//! worse -- it makes a reader believe the platform records something it does not.
//! The failure is silent in the other direction too: a table that no code names
//! cannot be found by anyone looking for where a fact is kept, so the fact looks
//! like it is kept nowhere.
//!
//! The check is deliberately shallow: it asks whether the table's name appears
//! outside a test, not whether an `INSERT` reaches it. A name is a lower bar than
//! a write, and it is the bar that separates "this table is talked about" from
//! "this table is residue" -- a real writer can go through a query builder, a
//! dynamic statement, or an upsert helper, and a name-only check stays quiet for
//! all of them. What it cannot see is a writer that builds the name from pieces;
//! no table does that here, and a table added that way would have to be listed
//! below with that reason, which is the same review a wrong entry gets.
//!
//! Tables the list below names are debt, not permission: the entries may only
//! shrink. Wiring a writer or dropping the table makes this test red until the
//! row is removed, so the count cannot quietly grow back.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Tables with no code that names them, and why each one is still here.
///
/// Each entry is a decision someone has to make, not an exemption: either the
/// feature arrives and the name shows up in the source, or the table goes. The
/// test fails if a listed table gains a name (the row is stale) and if an
/// unlisted table loses one (a new promise nobody keeps).
const FROZEN_UNWRITTEN: &[(&str, &str)] = &[
    (
        "agent_performance",
        "no reader and no writer in this tree; zero rows on the running platform",
    ),
    (
        "model_pricing",
        "no reader and no writer in this tree; zero rows on the running platform",
    ),
    (
        "task_dependencies",
        "no reader and no writer in this tree; zero rows on the running platform",
    ),
    (
        "token_face_beat",
        "written by a scheduled job on the host, outside this repository, so no source file here names it",
    ),
    (
        "user_biometric_keys",
        "no reader and no writer in this tree; zero rows on the running platform",
    ),
    (
        "user_devices",
        "no reader and no writer in this tree; zero rows on the running platform",
    ),
    (
        "user_quotas",
        "no reader and no writer in this tree; zero rows on the running platform",
    ),
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the crate sits two levels below the workspace root")
        .to_path_buf()
}

/// A SQL statement with its comment stripped. Migrations explain themselves in
/// `--` lines that mention other tables by name, and reading those as statements
/// would invent tables that were never created.
fn statement(line: &str) -> &str {
    line.split("--").next().unwrap_or("").trim()
}

fn migrations() -> BTreeMap<String, String> {
    let dir = workspace_root().join("crates/cog-storage/migrations/postgres");
    let mut out = BTreeMap::new();
    let mut names: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("the migration directory is readable")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".up.sql"))
        .collect();
    names.sort();
    for path in names {
        let file = path.file_name().unwrap().to_string_lossy().to_string();
        let text = fs::read_to_string(&path).expect("a migration is readable");
        out.insert(file, text);
    }
    out
}

fn word_after<'a>(line: &'a str, keyword: &str) -> Option<&'a str> {
    let lower = line.to_ascii_lowercase();
    let at = lower.find(keyword)?;
    let rest = &line[at + keyword.len()..];
    rest.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .find(|w| !w.is_empty())
}

/// Tables that the migrations create and never drop, with the migration that
/// created each one.
fn live_tables() -> BTreeMap<String, String> {
    let mut created: BTreeMap<String, String> = BTreeMap::new();
    let mut dropped: BTreeSet<String> = BTreeSet::new();
    for (file, text) in migrations() {
        for line in text.lines() {
            let line = statement(line);
            if let Some(name) = word_after(line, "create table") {
                // `create table if not exists x` leaves `if` under the cursor;
                // the table's own name is the word after `exists`.
                let name = if name.eq_ignore_ascii_case("if") {
                    word_after(line, "exists").unwrap_or_default()
                } else {
                    name
                };
                if !name.is_empty() {
                    created.entry(name.to_string()).or_insert(file.clone());
                }
            }
            if let Some(name) = word_after(line, "drop table") {
                let name = if name.eq_ignore_ascii_case("if") {
                    word_after(line, "exists").unwrap_or_default()
                } else {
                    name
                };
                if !name.is_empty() {
                    dropped.insert(name.to_string());
                }
            }
        }
    }
    // Dropping a partitioned table drops its partitions, which the migration
    // never names: a child whose parent is gone is gone with it.
    let partition = |name: &str| -> Option<String> {
        let (parent, tail) = name.rsplit_once('_')?;
        let tail = tail.strip_prefix('y')?;
        let (year, month) = tail.split_once('m')?;
        if year.len() == 4 && month.len() == 2 {
            Some(parent.to_string())
        } else {
            None
        }
    };
    let dropped_all: BTreeSet<String> = dropped.iter().cloned().collect();
    created
        .into_iter()
        .filter(|(name, _)| {
            if dropped_all.contains(name) {
                return false;
            }
            match partition(name) {
                Some(parent) => !dropped_all.contains(&parent),
                None => true,
            }
        })
        .collect()
}

fn source_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if name == "tests" || name == "target" {
                continue;
            }
            source_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Source text with every inline test module cut away.
///
/// `#[cfg(test)]` marks where a file stops being production code; a table that
/// only a test inserts into is a table the running platform never fills, which
/// is the case this test exists to name. Production code never needs the marker
/// before its last item, so cutting at the first occurrence costs nothing.
fn production_text() -> String {
    let crates = workspace_root().join("crates");
    let mut files = Vec::new();
    for entry in fs::read_dir(&crates)
        .expect("the crates directory is readable")
        .flatten()
    {
        let src = entry.path().join("src");
        if src.is_dir() {
            source_files(&src, &mut files);
        }
    }
    let mut text = String::new();
    for path in files {
        let body = fs::read_to_string(&path).unwrap_or_default();
        match body.find("#[cfg(test)]") {
            Some(at) => text.push_str(&body[..at]),
            None => text.push_str(&body),
        }
        text.push('\n');
    }
    text
}

fn mentions(text: &str, name: &str) -> bool {
    let mut from = 0;
    while let Some(at) = text[from..].find(name) {
        let start = from + at;
        let end = start + name.len();
        let before_ok = text[..start]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        let after_ok = text[end..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        if before_ok && after_ok {
            return true;
        }
        from = end;
    }
    false
}

#[test]
fn every_live_table_is_named_by_production_code_or_listed_as_debt() {
    let text = production_text();
    let live = live_tables();
    assert!(
        live.len() > 30,
        "the migration scan found {} tables, which is too few to be the schema",
        live.len()
    );

    let mut unnamed: Vec<(String, String)> = Vec::new();
    for (name, file) in &live {
        if mentions(&text, name) {
            continue;
        }
        // A partition child is addressed through its parent's name: no source
        // file has a reason to say `messages_y2026m05`.
        let parent = name.rsplit_once('_').and_then(|(parent, tail)| {
            let tail = tail.strip_prefix('y')?;
            let (year, month) = tail.split_once('m')?;
            (year.len() == 4 && month.len() == 2).then(|| parent.to_string())
        });
        if let Some(parent) = parent {
            if live.contains_key(&parent) {
                continue;
            }
        }
        unnamed.push((name.clone(), file.clone()));
    }

    let frozen: BTreeMap<&str, &str> = FROZEN_UNWRITTEN.iter().copied().collect();
    let found: BTreeSet<&str> = unnamed.iter().map(|(n, _)| n.as_str()).collect();
    let known: BTreeSet<&str> = frozen.keys().copied().collect();

    let new: Vec<String> = unnamed
        .iter()
        .filter(|(n, _)| !frozen.contains_key(n.as_str()))
        .map(|(n, f)| format!("{n} (created by {f})"))
        .collect();
    let stale: Vec<&str> = known.difference(&found).copied().collect();

    assert!(
        new.is_empty(),
        "这些表的建表语句没有任何非测试代码提到它们，等于一次没人兑现的承诺：\n  {}\n\
         要么把写点接上（表名出现在 crates/*/src 里），要么加一条迁移删掉它；\n\
         真要保留，就在 FROZEN_UNWRITTEN 里写明为什么。",
        new.join("\n  ")
    );
    assert!(
        stale.is_empty(),
        "FROZEN_UNWRITTEN 里这些表已经被代码点名或已经不存在了，列表只许变短，删掉这几行：\n  {}",
        stale.join("\n  ")
    );
}

/// The scan's own shape: comments must not create tables, and a partition of a
/// dropped parent must not survive as a live table.
#[test]
fn the_migration_scan_reads_statements_not_comments() {
    let live = live_tables();
    assert!(
        !live.contains_key("named"),
        "一个注释里出现过的词被当成了表名"
    );
    assert!(
        !live.contains_key("messages_y2026m05"),
        "父表已删，它的分区还被算成活表"
    );
    assert!(
        live.contains_key("datasets"),
        "最新的迁移没有被读到：扫描的路径或形状不对"
    );
}
