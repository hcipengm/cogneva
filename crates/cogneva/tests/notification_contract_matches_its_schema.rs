//! The notifications table requires columns the notification contract can
//! supply, and this keeps it that way.
//!
//! A table and a struct describing the same records are two statements about one
//! shape, and nothing makes them agree. They did not: the migration made
//! `user_id` and `ntype` `NOT NULL` with no default while `cog_core::Notification`
//! had no field for either, so `NotificationStore::create(&Notification)` could
//! not fill the table it was the store for. The contract was the side that
//! changed -- a notification belongs to one user, and the table had said so all
//! along. This file used to name that disagreement; now it holds the answer open.
//!
//! The check is a census, not a verdict. It derives the required columns from
//! the migration and the suppliable ones from the contract type itself -- by
//! serialising a value and reading its keys, so a field added, renamed, or
//! renamed-for-the-wire moves the answer without anyone editing this file -- and
//! then asserts the difference is exactly the list below. That makes it read in
//! both directions: a new required column the contract cannot supply fails, and
//! so does a gap that quietly closes while this list still names it. The list is
//! empty, which is a reading too: it says the two sides agree today.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use chrono::Utc;

/// The required columns the contract has no field for, by name.
///
/// A gap written down by name is a gap someone can act on; one behind a count is
/// a number nobody can look up. When this list changes, the reason has to be in
/// the commit that changes it.
///
/// Empty since the contract grew `user_id` and `ntype`. It is not a list that
/// can be deleted: the assertion below is what makes a *new* required column,
/// or a dropped field, fail here instead of at the first insert.
const COLUMNS_THE_CONTRACT_CANNOT_SUPPLY: &[&str] = &[];

/// The migration that creates the table, found by name rather than by number so
/// a renumbering does not turn this into a test about a missing file.
fn notification_migration() -> PathBuf {
    let dir =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/cog-storage/migrations/postgres");
    let mut found: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("migration directory {} is unreadable: {e}", dir.display()))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            name.contains("notifications") && name.ends_with(".up.sql")
        })
        .collect();
    found.sort();
    assert_eq!(
        found.len(),
        1,
        "expected exactly one notifications migration, found {found:?}"
    );
    found.pop().expect("checked just above")
}

/// The `notifications` table's columns, as (name, NOT NULL, has DEFAULT).
fn columns() -> Vec<(String, bool, bool)> {
    let path = notification_migration();
    let sql = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    column_definitions(&sql, &path.display().to_string())
}

/// Read the column list out of a `CREATE TABLE notifications` statement.
///
/// Shared with the store's own DDL below, so that both sides are read by one
/// scanner: a difference the comparison reports is then a difference in the
/// statements, not in how they were read.
fn column_definitions(sql: &str, label: &str) -> Vec<(String, bool, bool)> {
    let body = sql
        .split_once("CREATE TABLE IF NOT EXISTS notifications (")
        .or_else(|| sql.split_once("CREATE TABLE notifications ("))
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("{label} has no notifications CREATE TABLE"));

    let mut columns = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }
        // The closing parenthesis ends the column list; nothing past it belongs
        // to this table's shape.
        if line.starts_with(')') {
            break;
        }
        let upper = line.to_uppercase();
        let name = line
            .split_whitespace()
            .next()
            .expect("a non-empty line has a first token")
            .to_string();
        columns.push((
            name,
            upper.contains("NOT NULL") || upper.contains("PRIMARY KEY"),
            upper.contains("DEFAULT"),
        ));
    }
    assert!(
        !columns.is_empty(),
        "parsed no columns out of {label}: the scanner stopped matching it"
    );
    columns
}

/// What the contract can put into a row: the keys of the value it serialises to.
///
/// Read off a real value rather than listed here, so the answer follows the type
/// -- including a serde rename, which is what decides the key.
fn what_the_contract_supplies() -> BTreeSet<String> {
    let sample = cog_core::Notification {
        id: "00000000-0000-4000-8000-000000000001".into(),
        user_id: "00000000-0000-4000-8000-0000000000ff".into(),
        ntype: cog_core::NotificationType::System,
        title: "probe".into(),
        body: String::new(),
        is_read: false,
        created_at: Utc::now(),
        read_at: None,
    };
    let value = serde_json::to_value(&sample).expect("a notification serialises");
    value
        .as_object()
        .expect("a notification serialises to an object")
        .keys()
        .cloned()
        .collect()
}

#[test]
fn the_table_requires_only_columns_the_contract_can_supply() {
    let supplied = what_the_contract_supplies();
    let required: BTreeSet<String> = columns()
        .into_iter()
        .filter(|(_, not_null, has_default)| *not_null && !*has_default)
        .map(|(name, _, _)| name)
        .collect();

    assert!(
        !required.is_empty(),
        "the table declares no required column, so this comparison would pass by \
         asking nothing"
    );
    assert!(
        required.iter().any(|column| supplied.contains(column)),
        "no required column matches a contract field at all: the two sides are \
         being read in different namespaces, and every difference below would be \
         an artefact of that"
    );

    let unsupplied: Vec<String> = required.difference(&supplied).cloned().collect();
    assert_eq!(
        unsupplied, COLUMNS_THE_CONTRACT_CANNOT_SUPPLY,
        "the set of NOT NULL columns with no default that the contract cannot fill \
         has moved. Each name here is a column a row cannot be written without and \
         the contract has no field for -- the table and the contract disagree about \
         the shape of a notification"
    );
}

/// The table a store creates at startup has to be the table the migration
/// describes.
///
/// Two statements about one shape, and the store's copy is the one that runs:
/// `PostgresNotificationStore::init_schema` creates the table itself, because
/// the migration's index statements cannot be applied twice and the store
/// starts more than once. The migration is what the running cluster already
/// has, so a store whose copy drifts does not fail loudly -- it writes to
/// whatever columns it names and reads whatever the migration left there, and
/// the first insert to notice is a `NOT NULL` column the store does not fill.
#[test]
fn the_store_creates_the_table_the_migration_describes() {
    let migration = columns();
    let store = column_definitions(
        &cog_storage::postgres::notification_store::create_table_sql(),
        "the notification store's own DDL",
    );

    assert_eq!(
        migration, store,
        "the table the store creates has a different shape from the one the \
         migration describes. Whichever of the two is wrong, a row written by the \
         store is read by the other"
    );
}
