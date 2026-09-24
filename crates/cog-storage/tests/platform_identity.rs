//! Platform-identity lookup and first-login ownership, against a live PostgreSQL.
//!
//! What these tests check is what the database decides: that looking a platform
//! identity up is a statement the server accepts at all, that the first
//! identity to arrive becomes the instance owner, and that a later arrival does
//! not take that over. None of it can be checked without a server — the column
//! ambiguity this file exists for is raised at parse time by PostgreSQL, so a
//! statement that can never run keeps every DB-free test green. The test is
//! ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test platform_identity -- --ignored
//! ```
//!
//! It is one case rather than several because the identity tables are global
//! (an instance has one owner, not one per probe): splitting it up would have
//! the parts competing over the same rows instead of judging a fresh premise.
//! The schema is created by the store itself and the tables are cleared first,
//! so the database is left as it was found.

use cog_core::contract::auth::{PlatformIdentityStore, UserType};
use cog_storage::PostgresUserStore;
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn identity_lookup_runs_and_first_arrival_owns_the_instance() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let store = PostgresUserStore::new(pool.clone());
    store.init_schema().await.expect("create the user schema");
    // Children before parents: the identity rows point at the users.
    sqlx::query("DELETE FROM platform_identities")
        .execute(&pool)
        .await
        .expect("clear platform identities");
    sqlx::query("DELETE FROM users")
        .execute(&pool)
        .await
        .expect("clear users");

    // The lookup has to be a statement the server accepts. Both tables carry
    // their own `id`, `created_at` and `updated_at`, so an unqualified
    // reference is rejected outright — and because that check happens before
    // any row is read, it is invisible to every test that never reaches a
    // database.
    let found = store
        .find_user_by_identity("github", "1")
        .await
        .expect("the lookup statement must be accepted by the server");
    assert!(found.is_none(), "an empty instance has no identities");

    let (owner, created) = store
        .find_or_create_by_identity("github", "1", "hcipengm", None, None, None, None)
        .await
        .expect("create the first identity");
    assert!(created, "the first identity must create a user");
    assert_eq!(
        owner.user_type,
        UserType::Admin,
        "the first account of an instance is the operator's only way in, so it \
         has to carry the admin role"
    );
    assert_eq!(owner.username, "github:hcipengm");

    let (second, created) = store
        .find_or_create_by_identity("github", "2", "someone", None, None, None, None)
        .await
        .expect("create the second identity");
    assert!(created);
    assert_eq!(
        second.user_type,
        UserType::Standard,
        "ownership must not move to whoever logs in next"
    );

    // Coming back is a lookup, not a second account.
    let (again, created) = store
        .find_or_create_by_identity("github", "1", "hcipengm", None, None, None, None)
        .await
        .expect("look the first identity up again");
    assert!(!created, "a returning identity must not create a user");
    assert_eq!(again.id, owner.id);
    assert_eq!(again.user_type, UserType::Admin);

    let found = store
        .find_user_by_identity("github", "1")
        .await
        .expect("lookup")
        .expect("the identity was just written");
    assert_eq!(found.id, owner.id);
}
