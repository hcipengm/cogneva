//! The notification store against a live PostgreSQL.
//!
//! What a database decides is the point of this file: that the insert this
//! store builds is a statement the server accepts, that a read comes back
//! scoped to one user, and that the page boundary is a fact about the rows
//! rather than about the process that asked. None of it can be checked without
//! a server — a column the contract does not supply, a type that does not
//! decode, an ordering the index cannot serve, all keep every DB-free test
//! green. Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test notification_store -- --ignored
//! ```
//!
//! One case rather than several, for the same reason as the identity test: the
//! tables are global, and split-up parts would compete over the same rows.
//! Tables are cleared first, so the database is left as it was found.

use chrono::{Duration as ChronoDuration, Utc};
use cog_core::{Notification, NotificationFilter, NotificationStore, NotificationType};
use cog_storage::{PostgresNotificationStore, PostgresUserStore};
use sqlx::PgPool;
use uuid::Uuid;

use cog_test_support::database_url;

/// A fixed clock for the whole case, truncated to the precision the column
/// keeps.
///
/// `timestamptz` stores microseconds, so a timestamp taken straight from
/// `Utc::now()` comes back different in its last digits and an equality on it
/// would be a test about the clock rather than about the page boundary. One
/// base for every row also makes the gaps exact, which is what lets the second
/// page be checked against the first.
fn clock() -> chrono::DateTime<Utc> {
    chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
        .expect("the current time is a representable instant")
}

fn notice(user_id: &Uuid, id: Uuid, created_at: chrono::DateTime<Utc>) -> Notification {
    Notification {
        id: id.to_string(),
        user_id: user_id.to_string(),
        ntype: NotificationType::System,
        title: "a notice".to_string(),
        body: "something happened".to_string(),
        is_read: false,
        created_at,
        read_at: None,
    }
}

fn page(cursor: Option<String>) -> NotificationFilter {
    NotificationFilter {
        unread_only: false,
        limit: 2,
        cursor,
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn notifications_are_stored_per_user_and_paged_by_row() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    // The users table first: the notification table's owner column points at it.
    PostgresUserStore::new(pool.clone())
        .init_schema()
        .await
        .expect("create the user schema");
    let store = PostgresNotificationStore::new(pool.clone());
    store
        .init_schema()
        .await
        .expect("create the notification schema");

    // Children before parents.
    sqlx::query("DELETE FROM notifications")
        .execute(&pool)
        .await
        .expect("clear notifications");
    sqlx::query("DELETE FROM platform_identities")
        .execute(&pool)
        .await
        .expect("clear platform identities");
    sqlx::query("DELETE FROM users")
        .execute(&pool)
        .await
        .expect("clear users");

    let owner: Uuid =
        sqlx::query_scalar("INSERT INTO users (username) VALUES ('owner') RETURNING id")
            .fetch_one(&pool)
            .await
            .expect("create the owner");
    let other: Uuid =
        sqlx::query_scalar("INSERT INTO users (username) VALUES ('other') RETURNING id")
            .fetch_one(&pool)
            .await
            .expect("create the second user");

    // The insert has to be a statement the server accepts at all. The contract
    // carries `user_id` as a string and the column is a `uuid`; the day kinds,
    // ids and owners stop being spelled the way the columns want them, this is
    // the only place that says so.
    let now = clock();
    let age = |seconds_ago: i64| now - ChronoDuration::seconds(seconds_ago);
    for (index, seconds_ago) in [30i64, 20, 10].iter().enumerate() {
        store
            .create(notice(&owner, Uuid::new_v4(), age(*seconds_ago)))
            .await
            .unwrap_or_else(|e| panic!("insert {} must be accepted by the server: {e}", index));
    }
    store
        .create(notice(&other, Uuid::new_v4(), age(15)))
        .await
        .expect("the other user's notice");

    // Newest first, and one page at a time: the second page starts after the
    // last row of the first, which is what the cursor names.
    let first = store
        .list(&owner.to_string(), page(None))
        .await
        .expect("list the owner's first page");
    assert_eq!(first.items.len(), 2, "the page is as long as the limit");
    assert!(first.has_more, "the third row is past this page");
    assert_eq!(
        first.unread_count, 3,
        "the badge counts the owner's unread rows, not the rows on this page"
    );
    let cursor = first
        .next_cursor
        .clone()
        .expect("a page with more rows names where the next one starts");

    let second = store
        .list(&owner.to_string(), page(Some(cursor)))
        .await
        .expect("list the page after the cursor");
    assert_eq!(second.items.len(), 1);
    assert!(!second.has_more, "the list ends here");
    assert_eq!(
        second.items[0].created_at,
        first.items[1].created_at - ChronoDuration::seconds(10),
        "the second page continues where the first stopped"
    );
    assert!(
        first
            .items
            .iter()
            .all(|n| second.items.iter().all(|m| m.id != n.id)),
        "a page must not repeat rows the previous page already returned"
    );

    // One user's list is one user's: the other account's row is in the table
    // and must not appear.
    let theirs = store
        .list(&other.to_string(), page(None))
        .await
        .expect("list the other user's notifications");
    assert_eq!(theirs.items.len(), 1);
    assert_eq!(theirs.unread_count, 1);

    // An id that exists but belongs to someone else is not this caller's to
    // mark read, and the row has to stay unread when the call is refused.
    let owners_row = first.items[0].id.clone();
    let refused = store
        .mark_read(&other.to_string(), &owners_row)
        .await
        .expect("a refused mark is an answer, not an error");
    assert!(!refused, "another user's notification is not found for me");
    let still_unread = store
        .list(&owner.to_string(), page(None))
        .await
        .expect("re-list the owner");
    assert_eq!(
        still_unread.unread_count, 3,
        "the refused call changed nothing"
    );

    let marked = store
        .mark_read(&owner.to_string(), &owners_row)
        .await
        .expect("mark the owner's own notification read");
    assert!(marked);
    let after = store
        .list(&owner.to_string(), page(None))
        .await
        .expect("list after marking one read");
    assert_eq!(after.unread_count, 2);
    assert!(
        after
            .items
            .iter()
            .find(|n| n.id == owners_row)
            .unwrap()
            .is_read,
        "the row that was marked comes back read"
    );

    let changed = store
        .mark_all_read(&owner.to_string())
        .await
        .expect("mark the rest read");
    assert_eq!(changed, 2, "the count is of the rows this call changed");
    assert_eq!(
        store
            .list(&owner.to_string(), page(None))
            .await
            .expect("list after marking all read")
            .unread_count,
        0
    );
    assert_eq!(
        store
            .list(&other.to_string(), page(None))
            .await
            .expect("the other user is untouched")
            .unread_count,
        1,
        "one user marking all read must not reach another user's rows"
    );

    // A value the column cannot hold is a validation failure, not "no such
    // notification": answering `false` would read as an absence for a request
    // that never reached the table.
    let bad = store.mark_read(&owner.to_string(), "not-a-uuid").await;
    assert!(
        matches!(bad, Err(cog_core::SFError::Validation(_))),
        "a non-UUID id must be reported as what it is, got {bad:?}"
    );
}
