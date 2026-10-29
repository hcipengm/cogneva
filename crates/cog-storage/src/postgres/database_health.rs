//! 「这个库现在还答不答」。
//!
//! 报健康的进程（网关）不持有连接池：池是落库这一侧的资产，而报生死只需要一个
//! 能答是或否的入口。所以池留在这里，网关按 [`cog_core::DatabaseHealth`] 问。

use sqlx::PgPool;

/// PostgreSQL 的健康入口：一次往返。
pub struct PgDatabaseHealth {
    pool: PgPool,
}

impl PgDatabaseHealth {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl cog_core::DatabaseHealth for PgDatabaseHealth {
    async fn ping(&self) -> cog_core::SFResult<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| cog_core::SFError::Database(format!("database ping: {e}")))
    }
}
