//! principle: feedback_a_closed_set_copied_into_sql_drifts_from_the_one_that_validates_it
//!
//! The face a change is filed under lives in three places that cannot import each
//! other: the Rust enum the service reads and publishes, the SQL function that
//! folds the citations into the column, and the CHECK constraints that refuse a
//! value outside the set. A name added to one and not the others does not fail --
//! it does something quieter. Added to the enum only, a document naming that face
//! has its answer folded away by the function and the row reads as "nobody named
//! a face": the same string the account uses for "nobody was read", so the work
//! is not merely unfiled, it is indistinguishable from work that was never asked
//! for. Added to the function only, the generated column produces a value the
//! CHECK refuses and the whole fold statement fails, which stops every row behind
//! it rather than the one that named the new face.
//!
//! So the sets are compared here, across the three places, from the files
//! themselves. No database is opened -- the migration directory is what the
//! runner reads, and the enum is what the service reads.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cog_core::contract::change_ledger::ChangeBlock;

fn postgres_migrations() -> PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations/postgres")).to_path_buf()
}

/// The newest migration that defines the folding function.
fn latest_definition_of_the_fold() -> (u64, String) {
    let mut best: Option<(u64, String)> = None;
    for entry in std::fs::read_dir(postgres_migrations()).expect("read the migrations") {
        let path = entry.expect("a readable directory entry").path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) if name.ends_with(".up.sql") => name.to_string(),
            _ => continue,
        };
        let version: u64 = match name.split('_').next().and_then(|v| v.parse().ok()) {
            Some(version) => version,
            None => continue,
        };
        let sql = std::fs::read_to_string(&path).expect("read a migration");
        if !sql.contains("CREATE OR REPLACE FUNCTION change_ledger_block(") {
            continue;
        }
        if best.as_ref().is_none_or(|(seen, _)| version > *seen) {
            best = Some((version, sql));
        }
    }
    best.expect("some migration defines change_ledger_block")
}

/// The quoted names inside `WHERE entry->>'block' IN (...)`.
fn names_the_fold_recognises(sql: &str) -> BTreeSet<String> {
    let start = sql.find("IN ('").expect("the fold has an IN list");
    let rest = &sql[start + 4..];
    let end = rest.find(')').expect("the IN list closes");
    rest[..end]
        .split(',')
        .map(|part| part.trim().trim_matches('\'').to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

/// The quoted names across every `block IN (...)` CHECK in the migration.
fn names_the_constraints_accept(sql: &str) -> Vec<BTreeSet<String>> {
    let mut found = Vec::new();
    let mut rest = sql;
    while let Some(at) = rest.find("CHECK (block IN (") {
        let after = &rest[at + "CHECK (block IN (".len()..];
        let end = after.find(')').expect("a CHECK list closes");
        found.push(
            after[..end]
                .split(',')
                .map(|part| part.trim().trim_matches('\'').to_string())
                .filter(|part| !part.is_empty())
                .collect(),
        );
        rest = &after[end..];
    }
    found
}

#[test]
fn the_fold_recognises_exactly_the_named_faces_the_enum_carries() {
    let (version, sql) = latest_definition_of_the_fold();
    let named: BTreeSet<String> = ChangeBlock::ALL
        .iter()
        .filter(|block| block.is_named())
        .map(|block| block.as_str().to_string())
        .collect();
    let folded = names_the_fold_recognises(&sql);
    assert_eq!(
        folded, named,
        "migration {version} folds a different set than the enum carries; a face on one side \
         and not the other files work under `none`, which is the same value the account uses \
         for `nobody named a face`"
    );
}

#[test]
fn both_constraints_accept_every_value_the_column_can_hold() {
    let (version, sql) = latest_definition_of_the_fold();
    let accepted: BTreeSet<String> = ChangeBlock::ALL
        .iter()
        .map(|block| block.as_str().to_string())
        .collect();
    let checks = names_the_constraints_accept(&sql);
    assert_eq!(
        checks.len(),
        2,
        "migration {version} should set the closed set on both the account and the intake"
    );
    for check in checks {
        assert_eq!(
            check, accepted,
            "migration {version} has a constraint that accepts a different set than the \
             contract can produce; the generated column writes what the fold answers, so a \
             name the constraint refuses stops the fold statement rather than the one row"
        );
    }
}
