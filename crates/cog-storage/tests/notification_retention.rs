//! principle: feedback_unread_records_must_not_expire_by_age
//! Notification retention, against a live PostgreSQL: the pass **counts, it
//! does not delete**.
//!
//! A notification is the platform having said something to a person, and the
//! row is the only copy of it. An age bound over a fact is the shape this
//! family refuses: past the window the correct next step is "archive a
//! readable copy, check it, then delete", and until that path exists the pass
//! may only report how many rows are due. The earlier shape deleted them, so
//! the deletion could regrow without any other code noticing — an empty table
//! and a table whose rows were swept on time look the same from every reader.
//!
//! What this file pins:
//!
//! - the count names exactly the read rows outside the window (the unread row
//!   is the one a person still has to open, and the recent read row is inside);
//! - after the pass, **every** row is still there — the row the count named is
//!   the row an archive owes, not the row to drop.
//!
//! Ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test notification_retention -- --ignored
//! ```
//!
//! Every run gets its own schema and drops it at the end. The store's statements
//! name `notifications` and `users` without a schema, so the connection's
//! `search_path` is what keeps this run's rows away from any other table that
//! happens to have those names.

use chrono::{Duration, Utc};

use cog_core::{Notification, NotificationStore, NotificationType};
use cog_storage::PostgresNotificationStore;

const KEEP_DAYS: i64 = 90;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

/// One notification for `user`, `age_days` old, already read or not.
fn notification_for(user: &str, age_days: i64, read: bool) -> Notification {
    let created = Utc::now() - Duration::days(age_days);
    Notification {
        id: uuid::Uuid::new_v4().to_string(),
        user_id: user.to_string(),
        ntype: NotificationType::System,
        title: format!("probe {age_days}d read={read}"),
        body: "probe".to_string(),
        is_read: read,
        created_at: created,
        read_at: read.then_some(created),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_pass_counts_the_due_rows_and_deletes_nothing() {
    let url = database_url();
    let schema = own_schema(&url, "notification_retention").await;

    let pool = schema_pool(&url, &schema).await;
    // The notifications table carries a foreign key to the users table, so the
    // schema has to have one before the store can create its own table. Only
    // the column the key names matters here.
    sqlx::query("CREATE TABLE users (id UUID PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("the probe users table is created");
    let store = PostgresNotificationStore::new(pool.clone());
    store
        .init_schema()
        .await
        .expect("the store creates its own table");

    let user_id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO users (id) VALUES ($1)")
        .bind(uuid::Uuid::parse_str(&user_id).expect("a uuid"))
        .execute(&pool)
        .await
        .expect("the probe user is written");

    let long_read = notification_for(&user_id, KEEP_DAYS + 10, true);
    let long_unread = notification_for(&user_id, KEEP_DAYS + 10, false);
    let recent_read = notification_for(&user_id, KEEP_DAYS - 10, true);
    for notification in [&long_read, &long_unread, &recent_read] {
        store
            .create(notification.clone())
            .await
            .expect("the probe notification is written");
    }

    let due = store
        .count_read_past_retention(KEEP_DAYS)
        .await
        .expect("the retention reading runs");
    assert_eq!(
        due, 1,
        "only the read row outside the window is due: the unread row is the one \
         a person still has to open, and the recent read row is inside the window"
    );

    // The count is a reading, not an action: every row survives it, including
    // the one it named. A deletion that came back would have to answer for the
    // archive first, and there is no archive here.
    for notification in [&long_read, &long_unread, &recent_read] {
        let present: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM notifications WHERE id = $1)")
                .bind(uuid::Uuid::parse_str(&notification.id).expect("a uuid"))
                .fetch_one(&pool)
                .await
                .expect("the probe row is read back");
        assert!(
            present,
            "row {:?} was deleted by a counting pass",
            notification.title
        );
    }

    pool.close().await;
    drop_schema(&url, &schema).await;
}
