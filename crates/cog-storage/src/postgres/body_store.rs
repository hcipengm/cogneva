//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//!
//! `BodyStore` 的大对象实现：把 `large_object` 的三个操作包成契约里那个接口。
//!
//! 一个句柄一段大对象；`seal` 把整段分块读回、算哈希与长度。不缓存、不合并、不设上限。

use async_trait::async_trait;
use sqlx::PgPool;

use cog_core::{BodyStore, BodyWriter, SFResult};

use crate::postgres::large_object;

/// 一段大对象上的句柄。
pub struct LargeObjectWriter {
    pool: PgPool,
    oid: u32,
}

#[async_trait]
impl BodyWriter for LargeObjectWriter {
    fn oid(&self) -> u32 {
        self.oid
    }

    async fn write(&self, chunk: &[u8]) -> SFResult<()> {
        large_object::write_block(&self.pool, self.oid, chunk).await
    }

    async fn seal(&self) -> SFResult<(String, u64)> {
        large_object::seal(&self.pool, self.oid).await
    }

    async fn discard(&self) -> SFResult<()> {
        large_object::unlink(&self.pool, self.oid).await
    }
}

/// 开一段正文的存储。装配层持有一个，交给网关。
pub struct PostgresBodyStore {
    pool: PgPool,
}

impl PostgresBodyStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BodyStore for PostgresBodyStore {
    async fn open(&self) -> SFResult<Box<dyn BodyWriter>> {
        let oid = large_object::open(&self.pool).await?;
        Ok(Box::new(LargeObjectWriter {
            pool: self.pool.clone(),
            oid,
        }))
    }
}
