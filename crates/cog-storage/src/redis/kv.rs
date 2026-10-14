use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use cog_core::contract::kv::KeyValueStore;
use cog_core::{SFError, SFResult};

/// Map a driver error into the contract's error type.
///
/// The core declares the contract and cannot name this driver, so the mapping
/// lives here, once, instead of at every method.
fn err(error: redis::RedisError) -> SFError {
    SFError::Redis(error.to_string())
}

/// Redis implementation of [`KeyValueStore`].
///
/// Wraps a connection manager rather than a client: the manager clones cheaply
/// and multiplexes over one socket, so a process that publishes this as a
/// service and a caller that takes a handle per request share one connection
/// instead of each paying for a handshake.
#[derive(Clone)]
pub struct RedisKeyValueStore {
    connection: ConnectionManager,
}

impl RedisKeyValueStore {
    /// Wrap an already-open connection.
    pub fn new(connection: ConnectionManager) -> Self {
        Self { connection }
    }

    /// Open a connection to `redis_url`.
    pub async fn connect(redis_url: &str) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(err)?;
        let connection = cog_redis::connect(&client).await.map_err(err)?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl KeyValueStore for RedisKeyValueStore {
    async fn get(&self, key: &str) -> SFResult<Option<String>> {
        self.connection.clone().get(key).await.map_err(err)
    }

    async fn set(&self, key: &str, value: &str) -> SFResult<()> {
        let _: () = self.connection.clone().set(key, value).await.map_err(err)?;
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, ttl_secs: u64) -> SFResult<()> {
        let _: () = self
            .connection
            .clone()
            .set_ex(key, value, ttl_secs)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn del(&self, key: &str) -> SFResult<()> {
        let _: () = self.connection.clone().del(key).await.map_err(err)?;
        Ok(())
    }

    async fn expire(&self, key: &str, ttl_secs: i64) -> SFResult<()> {
        let _: () = self
            .connection
            .clone()
            .expire(key, ttl_secs)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn ttl(&self, key: &str) -> SFResult<i64> {
        self.connection.clone().ttl(key).await.map_err(err)
    }

    async fn incr(&self, key: &str, by: i64) -> SFResult<i64> {
        self.connection.clone().incr(key, by).await.map_err(err)
    }

    async fn decr(&self, key: &str, by: i64) -> SFResult<i64> {
        self.connection.clone().decr(key, by).await.map_err(err)
    }

    async fn sadd(&self, key: &str, member: &str) -> SFResult<()> {
        let _: () = self
            .connection
            .clone()
            .sadd(key, member)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn srem(&self, key: &str, member: &str) -> SFResult<()> {
        let _: () = self
            .connection
            .clone()
            .srem(key, member)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn smembers(&self, key: &str) -> SFResult<Vec<String>> {
        self.connection.clone().smembers(key).await.map_err(err)
    }

    async fn hset(&self, key: &str, field: &str, value: &str) -> SFResult<()> {
        let _: () = self
            .connection
            .clone()
            .hset(key, field, value)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn hget(&self, key: &str, field: &str) -> SFResult<Option<String>> {
        self.connection.clone().hget(key, field).await.map_err(err)
    }

    async fn hdel(&self, key: &str, field: &str) -> SFResult<()> {
        let _: () = self
            .connection
            .clone()
            .hdel(key, field)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn hvals(&self, key: &str) -> SFResult<Vec<String>> {
        self.connection.clone().hvals(key).await.map_err(err)
    }

    async fn hgetall(&self, key: &str) -> SFResult<Vec<(String, String)>> {
        let fields: std::collections::HashMap<String, String> =
            self.connection.clone().hgetall(key).await.map_err(err)?;
        Ok(fields.into_iter().collect())
    }

    async fn hlen(&self, key: &str) -> SFResult<u64> {
        let count: i64 = self.connection.clone().hlen(key).await.map_err(err)?;
        Ok(count.max(0) as u64)
    }
}
