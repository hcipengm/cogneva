//! What a store's schema batch may contain, and what it may not.
//!
//! A schema text is sent as one simple query, and the driver runs a
//! multi-statement message as one implicit transaction: the `ACCESS EXCLUSIVE`
//! the first `ALTER` takes is held until the last statement in the text ends.
//! Any statement that walks the table therefore keeps every reader of that
//! table out for as long as the walk takes -- measured on a rolling restart,
//! the two `ADD COLUMN`s were done in twelve seconds and the correction that
//! shared their transaction then kept the table closed for twenty-six more,
//! out of a start that spent a hundred and forty-five seconds in this layer
//! before serving anything.
//!
//! So the rows a schema needs filled are filled by a batch of their own, which
//! asks only for `ROW EXCLUSIVE` -- the strongest lock no reader waits behind.
//! This test is what keeps a later correction from being appended to the DDL
//! text again, because nothing else would notice: the statements still succeed,
//! the tables still come out right, and the only symptom is that every reader
//! of that table waits through the walk.
//!
//! It reads the constants rather than a database, because the property is a
//! property of the text a store hands to the runner -- a server would tell us
//! the same thing only by being held open, which is the thing being prevented.

use cog_memory::postgres_entry_store::{SUMMARY_ENTRIES_DDL, SUMMARY_ENTRIES_REPAIR};
use cog_memory::postgres_schema::{
    SCHEMA_ENTRIES_AFTER_REPAIR, SCHEMA_ENTRIES_DDL, SCHEMA_ENTRIES_REPAIR,
};

/// The verbs that make a statement read or write rows rather than describe a
/// table. `ALTER`, `CREATE` and `DROP` change the shape; these change the data.
const TABLE_WALKS: [&str; 3] = ["UPDATE", "DELETE", "INSERT"];

/// The first word of every statement in `sql`, in order.
fn statement_verbs(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(str::trim)
        // A trailing `;` leaves an empty piece, and a statement may be empty
        // once its leading comment lines are gone.
        .filter(|statement| !statement.is_empty())
        .filter_map(|statement| {
            statement
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with("--"))
        })
        .filter_map(|first_line| first_line.split_whitespace().next())
        .map(|word| word.to_ascii_uppercase())
        .collect()
}

#[test]
fn a_schema_batch_holds_its_exclusive_lock_for_the_shape_and_no_longer() {
    for (name, sql) in [
        ("SCHEMA_ENTRIES_DDL", SCHEMA_ENTRIES_DDL),
        ("SUMMARY_ENTRIES_DDL", SUMMARY_ENTRIES_DDL),
        ("SCHEMA_ENTRIES_AFTER_REPAIR", SCHEMA_ENTRIES_AFTER_REPAIR),
    ] {
        for verb in statement_verbs(sql) {
            assert!(
                !TABLE_WALKS.contains(&verb.as_str()),
                "{name} starts a {verb} statement. This text runs as one \
                 transaction that holds ACCESS EXCLUSIVE from its first ALTER \
                 to its last statement, so a statement that walks the table \
                 keeps every reader of that table out for the whole walk -- \
                 measured at twenty-six seconds on a rolling restart. Put the \
                 rows in {} and run that after this one.",
                if name.starts_with("SCHEMA") {
                    "SCHEMA_ENTRIES_REPAIR"
                } else {
                    "SUMMARY_ENTRIES_REPAIR"
                }
            );
        }
    }
}

/// The other half, and the one that keeps the test above from being satisfied
/// by deleting the correction: the rows still have to be filled, in a list of
/// their own.
#[test]
fn the_rows_a_schema_needs_filled_are_filled_by_a_list_of_their_own() {
    for (name, sql) in [
        ("SCHEMA_ENTRIES_REPAIR", SCHEMA_ENTRIES_REPAIR),
        ("SUMMARY_ENTRIES_REPAIR", SUMMARY_ENTRIES_REPAIR),
    ] {
        let verbs = statement_verbs(sql);
        assert!(
            !verbs.is_empty(),
            "{name} is empty, so whatever the schema needs filled stays unfilled"
        );
        assert!(
            verbs
                .iter()
                .all(|verb| TABLE_WALKS.contains(&verb.as_str())),
            "{name} is the list that fills rows; a statement that changes the \
             shape belongs in the DDL batch, which is where the exclusive lock \
             it needs is already being taken. Got: {verbs:?}"
        );
    }
}

#[test]
fn the_verbs_are_read_off_the_statements_rather_than_the_text() {
    let sql = "-- a comment that says UPDATE\r\nUPDATE t SET a = 1;\n  create index i on t(a);";
    assert_eq!(statement_verbs(sql), vec!["UPDATE", "CREATE"]);
}
