//! Redis connection pool and runtime backend for Type 1 Memory (session, cache, online status).

use redis::{aio::ConnectionManager, AsyncCommands, RedisError};

use cog_core::{SFError, SFResult};

/// Redis backend wrapping a [`ConnectionManager`] (auto-reconnecting multiplexed connection).
#[derive(Clone)]
pub struct RedisBackend {
    conn: ConnectionManager,
}

impl RedisBackend {
    /// Create a new backend from a Redis URL (e.g. `redis://127.0.0.1:6379`).
    pub async fn new(redis_url: &str) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| SFError::Adapter {
            provider: "redis".into(),
            message: format!("open client failed: {}", e),
        })?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "redis".into(),
                message: format!("connect failed: {}", e),
            })?;
        Ok(Self { conn })
    }

    // ─── Basic KV ───

    pub async fn get(&self, key: &str) -> SFResult<Option<String>> {
        let mut c = self.conn.clone();
        c.get(key).await.map_err(|e| map_redis_err("get", e))
    }

    pub async fn set(&self, key: &str, value: &str) -> SFResult<()> {
        let mut c = self.conn.clone();
        c.set(key, value).await.map_err(|e| map_redis_err("set", e))
    }

    pub async fn set_ex(&self, key: &str, value: &str, seconds: u64) -> SFResult<()> {
        let mut c = self.conn.clone();
        c.set_ex(key, value, seconds)
            .await
            .map_err(|e| map_redis_err("set_ex", e))
    }

    // ─── Binary KV ───

    pub async fn get_bytes(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
        let mut c = self.conn.clone();
        c.get(key).await.map_err(|e| map_redis_err("get_bytes", e))
    }

    pub async fn set_ex_bytes(&self, key: &str, value: &[u8], seconds: u64) -> SFResult<()> {
        let mut c = self.conn.clone();
        c.set_ex(key, value, seconds)
            .await
            .map_err(|e| map_redis_err("set_ex_bytes", e))
    }

    pub async fn del(&self, key: &str) -> SFResult<()> {
        let mut c = self.conn.clone();
        let _: Option<String> = c.del(key).await.map_err(|e| map_redis_err("del", e))?;
        Ok(())
    }

    pub async fn expire(&self, key: &str, seconds: u64) -> SFResult<()> {
        let mut c = self.conn.clone();
        c.expire(key, seconds as i64)
            .await
            .map_err(|e| map_redis_err("expire", e))
    }

    // ─── Pub/Sub ───

    pub async fn publish(&self, channel: &str, message: &str) -> SFResult<()> {
        let mut c = self.conn.clone();
        c.publish(channel, message)
            .await
            .map_err(|e| map_redis_err("publish", e))
    }

    // ─── Sorted Set ───

    pub async fn zadd(&self, key: &str, score: f64, member: &str) -> SFResult<()> {
        let mut c = self.conn.clone();
        c.zadd::<_, _, _, ()>(key, member, score)
            .await
            .map_err(|e| map_redis_err("zadd", e))?;
        Ok(())
    }

    pub async fn zrem(&self, key: &str, member: &str) -> SFResult<()> {
        let mut c = self.conn.clone();
        c.zrem::<_, _, ()>(key, member)
            .await
            .map_err(|e| map_redis_err("zrem", e))?;
        Ok(())
    }

    pub async fn zrevrange(&self, key: &str, start: isize, stop: isize) -> SFResult<Vec<String>> {
        let mut c = self.conn.clone();
        c.zrevrange(key, start, stop)
            .await
            .map_err(|e| map_redis_err("zrevrange", e))
    }

    // ─── Health ───

    pub async fn ping(&self) -> SFResult<String> {
        let mut c = self.conn.clone();
        redis::cmd("PING")
            .query_async(&mut c)
            .await
            .map_err(|e| map_redis_err("ping", e))
    }
}

fn map_redis_err(op: &str, e: RedisError) -> SFError {
    SFError::Adapter {
        provider: "redis".into(),
        message: format!("{} failed: {}", op, e),
    }
}
