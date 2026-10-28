//! Notification retention, against a live PostgreSQL.
//!
//! The rule is which rows survive, and that is the database's decision: read
//! rows older than the window go, unread rows stay however old they are, and a
//! read row inside the window stays. A file-backed store cannot be asked any of
//! it, and the statement that deletes is exactly the line where the exception
//! for unread rows can be dropped without any other code noticing -- the table
//! then shrinks the same way whether the exception is there or not, until the
//! first notification nobody has opened is deleted before its reader sees it.
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

use std::str::FromStr;

use chrono::{Duration, Utc};
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection, PgPool};

use cog_core::{Notification, NotificationStore, NotificationType};
use cog_storage::PostgresNotificationStore;

const KEEP_DAYS: i64 = 90;

use cog_test_support::database_url;

/// A pool whose every connection resolves unqualified names inside `schema`.
async fn schema_pool(url: &str, schema: &str) -> PgPool {
    let options = PgConnectOptions::from_str(url)
        .expect("the test database URL parses")
        .options([("search_path", schema)]);
    PgPool::connect_with(options)
        .await
        .expect("the throwaway database is reachable")
}

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
async fn read_rows_expire_and_unread_rows_do_not() {
    let url = database_url();
    let schema = format!("notification_retention_{}", uuid::Uuid::new_v4().simple());

    let mut admin = PgConnection::connect(&url)
        .await
        .expect("the throwaway database is reachable");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await
        .expect("the probe schema is created");

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

    let removed = store
        .prune_read(KEEP_DAYS)
        .await
        .expect("the retention pass runs");
    assert_eq!(
        removed, 1,
        "only the read row outside the window is due: the unread row is the one \
         a person still has to open, and the recent read row is inside the window"
    );

    for (notification, expected) in [
        (&long_read, false),
        (&long_unread, true),
        (&recent_read, true),
    ] {
        let present: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM notifications WHERE id = $1)")
                .bind(uuid::Uuid::parse_str(&notification.id).expect("a uuid"))
                .fetch_one(&pool)
                .await
                .expect("the probe row is read back");
        assert_eq!(
            present, expected,
            "row {:?} survived={present}, expected {expected}",
            notification.title
        );
    }

    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&mut admin)
        .await
        .expect("the probe schema is dropped");
}
