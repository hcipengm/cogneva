//! principle: feedback_a_copied_closed_set_reveals_additions_but_not_redefinitions
//!
//! The face a change is filed under used to be a closed set living in three
//! places that cannot import each other: a variant list in the service, an `IN`
//! list in the SQL function that folds the citations into the column, and two
//! CHECK constraints. A face added to one and not the others did not fail -- it
//! did something quieter. Added to the service only, a document naming that face
//! had its answer folded away and the row read as "nobody named a face". Added
//! to the function only, the generated column produced a value the CHECK refused
//! and the fold statement failed, stopping every row behind it.
//!
//! That copy has since been removed rather than kept in step: nothing outside a
//! document decides which faces exist. What all three still have to agree on is
//! the *shape* of the value, because the value travels through SQL, JSONB and
//! metric labels. This checks that agreement from the files themselves -- and
//! that no list grows back into the fold.

use std::path::{Path, PathBuf};

use cog_core::contract::change_ledger::ChangeBlock;

/// The shape migration 087 pins for the column. Taken from the service rather
/// than written out again here: the point of this test is that the SQL and the
/// service agree, and a third copy in the test would agree with neither.
const SHAPE_IN_SQL: &str = ChangeBlock::SHAPE;

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

/// What the pinned shape accepts, written out once so the Rust rule can be
/// compared against the same definition rather than against a transcribed set
/// of names.
fn the_shape_accepts(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

#[test]
fn the_fold_carries_no_list_of_faces() {
    let (version, sql) = latest_definition_of_the_fold();
    assert!(
        !sql.contains("IN ('"),
        "migration {version} went back to folding against a list of faces; a face declared \
         after it was written then files its work under `none`, which is the value the \
         account uses for `nobody named a face`"
    );
    // Two constraints, and both are about the shape rather than membership.
    let checks = sql.matches("CHECK (block ~ '").count();
    assert_eq!(
        checks, 2,
        "migration {version} should set the shape on both the account and the intake"
    );
}

#[test]
fn the_sql_and_the_service_accept_the_same_names() {
    let (version, sql) = latest_definition_of_the_fold();
    assert!(
        sql.contains(SHAPE_IN_SQL),
        "migration {version} no longer folds on the shape the service validates ({SHAPE_IN_SQL}); \
         a name one side accepts and the other refuses is either work filed under `none` or a \
         fold statement that fails every row behind it"
    );
    let longest = "a".repeat(64);
    let too_long = "a".repeat(65);
    for name in [
        "tasks",
        "token",
        "infra",
        "a_face_declared_today",
        "a",
        "a1_b",
        &longest,
    ] {
        let by_sql = the_shape_accepts(name);
        let by_service = ChangeBlock::named(name).is_ok();
        assert!(by_sql, "{name:?} is inside the shape");
        assert_eq!(by_service, by_sql, "{name:?} is accepted on one side only");
    }
    for name in ["Tasks", "1abc", "has space", "端到端演练", &too_long] {
        assert!(!the_shape_accepts(name), "{name:?} is outside the shape");
        assert!(
            ChangeBlock::named(name).is_err(),
            "{name:?} is outside the shape and must not become a label value"
        );
    }
}
