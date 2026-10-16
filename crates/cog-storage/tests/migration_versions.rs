//! One version number, one migration -- and one set of them per dialect.
//!
//! The runner decides what to apply by version alone: it reads
//! `schema_migrations WHERE version = $1` and skips the file if the row is
//! there. Two files numbered the same are therefore *one* migration as far as
//! the runner is concerned, and whichever the directory happens to list second
//! is never applied -- with no error, no warning, and no row saying so. The
//! column it was supposed to add is simply absent on every database that
//! applied the other one, and the first thing to notice is an insert naming a
//! column the table does not have, far from the file that caused it.
//!
//! This is not hypothetical: `042_change_ledger_declared_priority` was written
//! against a tree where `042_change_reopen` already held 42, and nothing in the
//! build, the tests or CI said a word. The version is a namespace of exactly
//! the size the directory uses, and nothing was guarding it.
//!
//! The two dialects are checked together because the same hole has a second
//! door: a migration that reaches one dialect and not the other is a schema
//! that differs by deployment, and every store method that assumes the column
//! then works in CI and fails in the other half of the fleet. Only one
//! direction of that difference is legal -- PostgreSQL carries things MySQL has
//! no analogue for (`partition_maintenance`, whose subject is range
//! partitioning) -- so the rule is that MySQL is never *ahead*: every version
//! MySQL has, PostgreSQL has too.
//!
//! No database is opened. This reads the migration directory, which is what the
//! runner reads.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use cog_storage::migrate::{discover_up_migrations, Driver};

fn migrations_dir() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
}

fn versions(driver: Driver) -> Vec<(i64, String)> {
    discover_up_migrations(migrations_dir(), driver)
        .expect("read the migration directory")
        .into_iter()
        .map(|m| (m.version, m.name))
        .collect()
}

#[test]
fn no_two_migrations_share_a_version_in_either_dialect() {
    for driver in [Driver::Postgres, Driver::Mysql] {
        let found = versions(driver);
        let mut by_version: BTreeMap<i64, Vec<String>> = BTreeMap::new();
        for (version, name) in found {
            by_version.entry(version).or_default().push(name);
        }
        let doubled: Vec<String> = by_version
            .iter()
            .filter(|(_, names)| names.len() > 1)
            .map(|(version, names)| format!("{version}: {}", names.join(" + ")))
            .collect();
        assert!(
            doubled.is_empty(),
            "{driver:?} has more than one migration on a version, and the runner \
             tracks versions rather than files, so all but one of each of these \
             is skipped silently: {doubled:#?}"
        );
    }
}

#[test]
fn mysql_is_never_ahead_of_postgres() {
    let pg: BTreeSet<i64> = versions(Driver::Postgres)
        .into_iter()
        .map(|(v, _)| v)
        .collect();
    let my: BTreeSet<i64> = versions(Driver::Mysql)
        .into_iter()
        .map(|(v, _)| v)
        .collect();
    let my_only: Vec<i64> = my.difference(&pg).copied().collect();
    let named: Vec<String> = my_only
        .iter()
        .map(|v| {
            let mut names: Vec<String> = versions(Driver::Mysql)
                .into_iter()
                .filter(|(n, _)| n == v)
                .map(|(_, name)| name)
                .collect();
            names.sort();
            format!("{v}: {}", names.join(" + "))
        })
        .collect();
    assert!(
        my_only.is_empty(),
        "MySQL holds versions PostgreSQL does not, so a MySQL deployment would \
         come out ahead of the schema every other deployment enforces: {named:?}"
    );
}
