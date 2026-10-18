//! Every table the application's own stores create, in one list.
//!
//! Restoring a package means replaying, into an empty database, the schema the
//! *image being restored by* knows — before any `COPY` runs. The migrations are
//! only part of that schema: a store that creates its table on first start
//! leaves the table in the live database and in no migration file, so a restore
//! that ran migrations alone would land on a database that is missing every one
//! of those tables. That is not a near miss — the first `COPY` fails, and the
//! drill reports the package as unrestorable when it was the image that could
//! not rebuild its own shape.
//!
//! The list is made of the stores' own constants, not of text copied here, so a
//! store changing its table cannot leave this file behind. Order is real:
//! `platform_identities` and `notifications` reference `users`, so the tables
//! that carry foreign keys come after the table they point at. Everything else
//! is self-contained.
//!
//! Tables made by migrations, and the partitions the partition maintainer
//! creates under a migrated parent, are deliberately absent: the migrations
//! rebuild the parents, and a partition is data placed inside a table, not a
//! shape a fresh database needs in order to accept the rows.

/// The statements, in the order they may be run.
///
/// Strings rather than `&str` because one entry is built from a configurable
/// table name; the rest are constants borrowed from the stores that own them.
pub fn statements() -> Vec<String> {
    let mut statements = vec![
        // The user tables first: two later tables carry foreign keys onto them.
        cog_storage::postgres::user_store::DDL.to_string(),
        cog_storage::postgres::notification_store::CREATE_TABLE_SQL.to_string(),
        cog_storage::postgres::snapshot_store::DDL.to_string(),
        cog_storage::postgres::hook_archive::DDL.to_string(),
        cog_storage::postgres::audit_recorder::DDL.to_string(),
        cog_storage::postgres::taste_intent_store::DDL.to_string(),
        cog_storage::postgres::metrics_backend::DDL.to_string(),
        cog_storage::postgres::state_backend::DDL.to_string(),
        cog_storage::postgres::observability_gateway::OBSERVABILITY_DDL.to_string(),
        cog_storage::owner_lease::DDL.to_string(),
        cog_observability::alert_store::DDL.to_string(),
        cog_observability::skip_store::DDL.to_string(),
        cog_observability::producer_heartbeat::DDL.to_string(),
        cog_observability::spend_verdict::DDL.to_string(),
        cog_observability::usage_store::DDL.to_string(),
        // The analytics table's name is a setting, so its schema comes from the
        // function the store itself calls rather than from a constant.
        cog_observability::analytics::postgres_table_ddl(
            cog_observability::analytics::DEFAULT_TABLE,
        ),
        cog_memory::postgres_schema::SCHEMA_ENTRIES_DDL.to_string(),
        cog_memory::postgres_entry_store::SUMMARY_ENTRIES_DDL.to_string(),
    ];
    // The notification indexes are per-query rather than per-table, so they are
    // a separate list in the store that owns them.
    statements.extend(
        cog_storage::postgres::notification_store::CREATE_INDEX_SQL
            .iter()
            .map(|sql| sql.to_string()),
    );
    statements.into_iter().map(terminated).collect()
}

/// Make sure a run of text ends where the driver will see the end of a
/// statement.
///
/// A store runs its own text as the whole of a query, so a single statement
/// does not need a semicolon and several of them were written without one.
/// Glued one after another, a text without one would make the next statement's
/// opening words part of its own syntax — and the error names the statement
/// after the boundary rather than the one that is actually unfinished.
fn terminated(sql: String) -> String {
    let sql = sql.trim_end();
    if sql.ends_with(';') {
        sql.to_string()
    } else {
        format!("{sql}\n;")
    }
}

/// Run the list against a database that already has the migrations.
pub async fn apply(pool: &sqlx::PgPool) -> cog_core::SFResult<()> {
    let statements = statements();
    let borrowed: Vec<&str> = statements.iter().map(String::as_str).collect();
    cog_storage::schema_ddl::apply_all(pool, &borrowed).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The files the bundle draws from. A new store outside this list is not
    /// seen here — the scripts that census the tree cover discovery; this test
    /// covers the case that actually happens, a table added to a store that is
    /// already bundled.
    const SOURCES: &[&str] = &[
        include_str!("../../cog-storage/src/postgres/user_store.rs"),
        include_str!("../../cog-storage/src/postgres/notification_store.rs"),
        include_str!("../../cog-storage/src/postgres/snapshot_store.rs"),
        include_str!("../../cog-storage/src/postgres/hook_archive.rs"),
        include_str!("../../cog-storage/src/postgres/audit_recorder.rs"),
        include_str!("../../cog-storage/src/postgres/taste_intent_store.rs"),
        include_str!("../../cog-storage/src/postgres/metrics_backend.rs"),
        include_str!("../../cog-storage/src/postgres/state_backend.rs"),
        include_str!("../../cog-storage/src/postgres/observability_gateway.rs"),
        include_str!("../../cog-storage/src/owner_lease.rs"),
        include_str!("../../cog-observability/src/alert_store.rs"),
        include_str!("../../cog-observability/src/skip_store.rs"),
        include_str!("../../cog-observability/src/producer_heartbeat.rs"),
        include_str!("../../cog-observability/src/spend_verdict.rs"),
        include_str!("../../cog-observability/src/usage_store.rs"),
        include_str!("../../cog-observability/src/analytics.rs"),
        include_str!("../../cog-memory/src/postgres_schema.rs"),
        include_str!("../../cog-memory/src/postgres_entry_store.rs"),
    ];

    /// The table names a piece of text creates. A name left as a `{placeholder}`
    /// is not a name yet, so it is not counted: those sites are the ones built
    /// from a function the bundle calls with its default table name.
    fn table_names(sql: &str) -> BTreeSet<String> {
        let needle = "CREATE TABLE IF NOT EXISTS";
        let mut names = BTreeSet::new();
        let mut rest = sql;
        while let Some(at) = rest.find(needle) {
            rest = &rest[at + needle.len()..];
            let name: String = rest
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                names.insert(name);
            }
        }
        names
    }

    /// Gluing in a text that ends mid-statement makes the *next* statement
    /// the one the error names, so the boundary is checked here rather than
    /// left to be rediscovered from a syntax error pointing at the wrong line.
    #[test]
    fn every_entry_ends_where_the_driver_sees_a_statement_end() {
        for statement in statements() {
            assert!(
                statement.trim_end().ends_with(';'),
                "entry does not end with a semicolon: {statement:.80}"
            );
        }
    }

    /// A table a bundled store creates must be in the bundle, and the bundle
    /// must not carry a table no store creates.
    ///
    /// The failure this catches is the one that made the restore drill red: a
    /// store grew a table, the app created it on first start, and the restore
    /// replayed a schema that had never heard of it. Neither the compiler nor
    /// any runtime test would say so — the drift is between two lists that are
    /// never read together.
    #[test]
    fn the_bundle_names_exactly_the_tables_the_stores_create() {
        let mut declared: BTreeSet<String> = SOURCES.iter().flat_map(|s| table_names(s)).collect();
        // The analytics table's name is a placeholder in the source, so the
        // declared side has to be rendered through the same function the
        // bundle calls. This one entry is checked for presence, not for text.
        declared.extend(table_names(
            &cog_observability::analytics::postgres_table_ddl(
                cog_observability::analytics::DEFAULT_TABLE,
            ),
        ));
        let bundled: BTreeSet<String> = statements().iter().flat_map(|s| table_names(s)).collect();
        let missing: Vec<&String> = declared.difference(&bundled).collect();
        let extra: Vec<&String> = bundled.difference(&declared).collect();
        assert!(
            missing.is_empty(),
            "stores create tables the restore would not rebuild: {missing:?}"
        );
        assert!(
            extra.is_empty(),
            "the bundle carries tables no bundled store creates: {extra:?}"
        );
    }
}
