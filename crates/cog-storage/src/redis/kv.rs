use std::sync::Arc;

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

/// INCRBY with the expiry set in the same round trip, when the key has none.
///
/// Server-side rather than two commands from here: between an INCRBY and a
/// separate EXPIRE there is a window in which the key exists with no expiry,
/// and a process that dies inside that window leaves a counter that never
/// resets. `TTL < 0` covers both the key this call created and one that already
/// existed without an expiry, so the script also repairs a counter a previous
/// crash left permanent.
const INCR_WITH_TTL_SCRIPT: &str = r#"
local count = redis.call('INCRBY', KEYS[1], ARGV[1])
if redis.call('TTL', KEYS[1]) < 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return count
"#;

/// Where this store gets its connection.
///
/// Two arms because a process legitimately comes by a connection in two ways: it
/// opened one itself and shares it with the rest of its backends, or it was
/// handed an address and must not be judged by whether the store answered at
/// that particular second.
#[derive(Clone)]
enum Connection {
    /// Opened before this store existed.
    Held(ConnectionManager),
    /// Opened at the first call that needs one, and re-attempted by the next
    /// call after a failure.
    OnDemand(Arc<cog_redis::Reconnecting>),
}

/// Redis implementation of [`KeyValueStore`].
///
/// Wraps a connection manager rather than a client: the manager clones cheaply
/// and multiplexes over one socket, so a process that publishes this as a
/// service and a caller that takes a handle per request share one connection
/// instead of each paying for a handshake.
///
/// The store also carries *when* that connection comes into being, because the
/// answer decides whether a process that starts while the store is away spends
/// its life blind. Nothing here latches: a call that could not reach the store
/// answers with an error, and the next call tries again. A store that is briefly
/// away -- a pod restarting, a rollout that recreated both ends, a resolver that
/// has not answered yet -- costs one call, not the process.
#[derive(Clone)]
pub struct RedisKeyValueStore {
    connection: Connection,
}

impl RedisKeyValueStore {
    /// Wrap a connection the caller already holds.
    pub fn new(connection: ConnectionManager) -> Self {
        Self {
            connection: Connection::Held(connection),
        }
    }

    /// Reach the store at `redis_url` on demand, bounded by the budget from the
    /// environment.
    ///
    /// The URL is parsed here and nothing is connected: a face that can only be
    /// built against a live store is a face that a process loses for the rest of
    /// its life when it starts one second early. A URL that does not parse is
    /// still an error -- that one is a fact about the configuration, not about
    /// this second.
    pub fn on_demand(redis_url: &str) -> SFResult<Self> {
        Self::on_demand_with_budget(redis_url, cog_redis::ConnectBudget::from_env())
    }

    /// The same, with an explicit budget. For call sites with a reason to
    /// differ, and for tests that need a fixed bound.
    pub fn on_demand_with_budget(
        redis_url: &str,
        budget: cog_redis::ConnectBudget,
    ) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(err)?;
        Ok(Self {
            connection: Connection::OnDemand(Arc::new(cog_redis::Reconnecting::with_budget(
                client, budget,
            ))),
        })
    }

    /// Hand out a connection for one call.
    ///
    /// `Err` means this attempt did not reach the store -- not a verdict about
    /// what is stored, because nothing was read.
    async fn connection(&self) -> SFResult<ConnectionManager> {
        match &self.connection {
            Connection::Held(connection) => Ok(connection.clone()),
            Connection::OnDemand(channel) => channel.get().await.ok_or_else(|| {
                SFError::Redis("the shared key-value store could not be reached".into())
            }),
        }
    }
}

#[async_trait]
impl KeyValueStore for RedisKeyValueStore {
    async fn get(&self, key: &str) -> SFResult<Option<String>> {
        self.connection().await?.get(key).await.map_err(err)
    }

    async fn set(&self, key: &str, value: &str) -> SFResult<()> {
        let _: () = self
            .connection()
            .await?
            .set(key, value)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, ttl_secs: u64) -> SFResult<()> {
        let _: () = self
            .connection()
            .await?
            .set_ex(key, value, ttl_secs)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn del(&self, key: &str) -> SFResult<()> {
        let _: () = self.connection().await?.del(key).await.map_err(err)?;
        Ok(())
    }

    async fn expire(&self, key: &str, ttl_secs: i64) -> SFResult<()> {
        let _: () = self
            .connection()
            .await?
            .expire(key, ttl_secs)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn ttl(&self, key: &str) -> SFResult<i64> {
        self.connection().await?.ttl(key).await.map_err(err)
    }

    async fn incr(&self, key: &str, by: i64) -> SFResult<i64> {
        self.connection().await?.incr(key, by).await.map_err(err)
    }

    async fn decr(&self, key: &str, by: i64) -> SFResult<i64> {
        self.connection().await?.decr(key, by).await.map_err(err)
    }

    async fn incr_ex(&self, key: &str, by: i64, ttl_secs: u64) -> SFResult<i64> {
        let mut connection = self.connection().await?;
        redis::Script::new(INCR_WITH_TTL_SCRIPT)
            .key(key)
            .arg(by)
            .arg(ttl_secs)
            .invoke_async(&mut connection)
            .await
            .map_err(err)
    }

    async fn sadd(&self, key: &str, member: &str) -> SFResult<()> {
        let _: () = self
            .connection()
            .await?
            .sadd(key, member)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn srem(&self, key: &str, member: &str) -> SFResult<()> {
        let _: () = self
            .connection()
            .await?
            .srem(key, member)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn smembers(&self, key: &str) -> SFResult<Vec<String>> {
        self.connection().await?.smembers(key).await.map_err(err)
    }

    async fn hset(&self, key: &str, field: &str, value: &str) -> SFResult<()> {
        let _: () = self
            .connection()
            .await?
            .hset(key, field, value)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn hget(&self, key: &str, field: &str) -> SFResult<Option<String>> {
        self.connection().await?.hget(key, field).await.map_err(err)
    }

    async fn hdel(&self, key: &str, field: &str) -> SFResult<()> {
        let _: () = self
            .connection()
            .await?
            .hdel(key, field)
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn hvals(&self, key: &str) -> SFResult<Vec<String>> {
        self.connection().await?.hvals(key).await.map_err(err)
    }

    async fn hgetall(&self, key: &str) -> SFResult<Vec<(String, String)>> {
        let fields: std::collections::HashMap<String, String> =
            self.connection().await?.hgetall(key).await.map_err(err)?;
        Ok(fields.into_iter().collect())
    }

    async fn hlen(&self, key: &str) -> SFResult<u64> {
        let count: i64 = self.connection().await?.hlen(key).await.map_err(err)?;
        Ok(count.max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An address that refuses immediately: the shape of "the store is not up".
    const REFUSED: &str = "redis://127.0.0.1:1";

    fn budget() -> cog_redis::ConnectBudget {
        cog_redis::ConnectBudget {
            budget_ms: 300,
            retries: 1,
        }
    }

    /// The channel behind a store built from a URL. Read here rather than
    /// exposed: how many times the store has tried to reach redis is a fact this
    /// crate's tests need and no production reader asks for.
    fn channel_of(store: &RedisKeyValueStore) -> &Arc<cog_redis::Reconnecting> {
        match &store.connection {
            Connection::OnDemand(channel) => channel,
            Connection::Held(_) => {
                panic!("built from a url, so this store reaches the store on demand")
            }
        }
    }

    /// The regression this arm exists for. A face built while the store is away
    /// must exist: the alternative is a process that came up one second early
    /// and never tried again. Building it is not an attempt, so there is nothing
    /// to fail.
    #[tokio::test]
    async fn building_a_face_from_a_url_does_not_reach_the_store() {
        let store = RedisKeyValueStore::on_demand_with_budget(REFUSED, budget())
            .expect("a well-formed url parses whether or not the store answers");
        assert_eq!(
            channel_of(&store).failed_attempts(),
            0,
            "building the face must not be an attempt to reach the store"
        );
    }

    /// Away is a per-call answer, not a state the store settles into. Each call
    /// asks the channel again -- which is where the retry lives -- so a store
    /// that comes back is picked up by the next call rather than by a restart.
    #[tokio::test]
    async fn a_store_that_is_away_is_asked_again_by_the_next_call() {
        let store = RedisKeyValueStore::on_demand_with_budget(REFUSED, budget())
            .expect("a well-formed url");
        for attempt in 1..=3 {
            let result = store.get("cog-storage:kv:absent").await;
            assert!(
                matches!(result, Err(SFError::Redis(_))),
                "nothing listens on {REFUSED}, so this cannot read anything: {result:?}"
            );
            assert_eq!(
                channel_of(&store).failed_attempts(),
                attempt,
                "call {attempt} has to reach the channel rather than a latched answer"
            );
        }
    }

    /// And the face is a working store, not only a polite failure: against a
    /// reachable redis the same construction reads back what it wrote.
    #[tokio::test]
    async fn a_reachable_store_gives_a_working_face() {
        let url = std::env::var("COGNEVA_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let store =
            RedisKeyValueStore::on_demand_with_budget(&url, budget()).expect("a well-formed url");
        let key = "cog-storage:kv:on-demand";
        if let Err(e) = store.set(key, "1").await {
            eprintln!("SKIP: no redis reachable for the happy path ({e})");
            return;
        }
        assert_eq!(store.get(key).await.expect("readable"), Some("1".into()));
        store.del(key).await.expect("deletable");
        assert_eq!(store.get(key).await.expect("readable"), None);
        assert_eq!(
            channel_of(&store).failed_attempts(),
            0,
            "a store that answered was reached on the first attempt"
        );
    }
}
