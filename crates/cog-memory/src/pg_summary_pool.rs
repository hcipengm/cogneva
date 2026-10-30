//! The pool this crate's own tables live on.
//!
//! The summary and schema layers keep their rows in the platform database, and
//! the pool for them is opened here rather than taken from the crate that
//! publishes the shared backends. Taking that handle would make this crate name
//! that crate in its build for a value whose entire content is a driver type
//! core cannot hold -- and a memory layer that only exists in processes which
//! link a storage crate is a memory layer that cannot be reused anywhere else.
//!
//! The budget is deliberately small. These are two tables read at recall time,
//! not a serving pool: the platform's connection count is already divided
//! between the storage plugin's own pools, and a second reader taking a share
//! the size of one of those would be paid for by every process on the box.

use std::time::Duration;

use sqlx::PgPool;

/// How many connections the summary and schema layers may hold.
///
/// Two, because the two layers are read concurrently and neither holds a
/// connection between calls: one would serialise them, three would be a share
/// taken from the serving pools for no measured gain.
// cap: bound
pub const SUMMARY_POOL_CONNECTIONS: u32 = 2;

/// How long a caller waits for one of those two connections before being told
/// the database is busy.
// cap: bound
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Open the pool for the summary and schema layers.
///
/// `ceiling` is what the component that divided the server's connections gave
/// one pool. The smaller of the two wins: this pool never needs more than
/// [`SUMMARY_POOL_CONNECTIONS`], and it never takes a share larger than the
/// division said it could have, which is the number the rest of the host is
/// counting on.
pub async fn connect(database_url: &str, ceiling: u32) -> Result<PgPool, sqlx::Error> {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(ceiling.clamp(1, SUMMARY_POOL_CONNECTIONS))
        .min_connections(0)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect(database_url)
        .await
}
