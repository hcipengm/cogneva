//! Running the schema statements a store owns, in one place.
//!
//! Every store keeps its own statements but hands them here to run, so the
//! application's first start and a restore drill execute the same list. Two
//! independent copies is how a table can exist everywhere the app looks and
//! nowhere a restore looks.
//!
//! Statements are idempotent by construction (`IF NOT EXISTS`), which is why a
//! restore can replay them after the migrations without checking anything
//! first.

use cog_core::{SFError, SFResult};
use sqlx::PgPool;

/// Run one store's schema. The text may hold several statements; the driver's
/// simple-query path is what makes that legal, and it is also why this is not
/// a prepared statement.
pub async fn apply(pool: &PgPool, schema: &str) -> SFResult<()> {
    sqlx::raw_sql(schema)
        .execute(pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(())
}

/// Run several stores' schema in the order given. Order matters across stores
/// when one table's definition references another's.
pub async fn apply_all(pool: &PgPool, schemas: &[&str]) -> SFResult<()> {
    for schema in schemas {
        apply(pool, schema).await?;
    }
    Ok(())
}
