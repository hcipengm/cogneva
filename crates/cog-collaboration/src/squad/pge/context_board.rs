//! Shared **Context Board** for Roundtable PGE mode.
//! the three roundtable agents (Planner, Generator,
//! Evaluator) communicate **not directly** but through a shared structured
//! "context board" — a key/value bag scoped to a single Squad. The Roundtable
//! orchestrator reads the latest state into each agent's prompt context, and
//! writes the agent's output back to the board after each phase so the next
//! agent (or the next round) can see it.
//! Two implementations are provided:
//! - [`InMemoryContextBoard`]: a `Mutex<HashMap>` for single-process use and
//!   for tests. Backwards-compatible with the existing
//!   `RoundtableConfig::context_board: Option<serde_json::Value>` field.
//! - [`KeyValueContextBoard`]: a hash at the key
//!   `orchestrator:squad:{squad_id}:board`, held through the core
//!   [`cog_core::KeyValueStore`] contract rather than through a driver, so the
//!   debate logic does not link the cache implementation.
//! Both implementations expose the same async [`ContextBoard`] trait so the
//! Roundtable can be configured with either at construction time without
//! changing the debate logic.

use async_trait::async_trait;
use cog_core::{KeyValueStore, SFError, SFResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Per-Squad shared structured state for Roundtable debates.
/// Conceptually a `HashMap<String, serde_json::Value>` shared by the three
/// agents. The Roundtable orchestrator is responsible for surfacing the board
/// into each agent's prompt context and persisting agent outputs back to the
/// board.
/// `Debug` is a supertrait so that callers can store `Arc<dyn ContextBoard>`
/// inside `#[derive(Debug)]` structs (e.g. `RoundtableConfig`).
#[async_trait]
pub trait ContextBoard: Send + Sync + std::fmt::Debug {
    /// Retrieve a single field.
    async fn get(&self, field: &str) -> SFResult<Option<serde_json::Value>>;

    /// Set / overwrite a single field.
    async fn set(&self, field: &str, value: serde_json::Value) -> SFResult<()>;

    /// Retrieve all fields as a `serde_json::Value::Object`.
    async fn snapshot(&self) -> SFResult<serde_json::Value>;

    /// Clear all fields.
    async fn clear(&self) -> SFResult<()>;
}

// ---------------------------------------------------------------------------
// In-memory implementation
// ---------------------------------------------------------------------------

/// In-memory [`ContextBoard`] backed by a [`tokio::sync::Mutex`] over a
/// [`HashMap`].
/// Useful for unit tests and for single-process deployments where the
/// Roundtable does not need to share state across agents that run in
/// different processes.
#[derive(Debug, Clone, Default)]
pub struct InMemoryContextBoard {
    inner: Arc<Mutex<HashMap<String, serde_json::Value>>>,
}

impl InMemoryContextBoard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-populate the board from an existing JSON object. Non-object values
    /// produce an empty board (logged at debug level).
    pub fn from_value(value: serde_json::Value) -> Self {
        let map = match value {
            serde_json::Value::Object(map) => map.into_iter().collect(),
            _ => HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(map)),
        }
    }
}

#[async_trait]
impl ContextBoard for InMemoryContextBoard {
    async fn get(&self, field: &str) -> SFResult<Option<serde_json::Value>> {
        Ok(self.inner.lock().await.get(field).cloned())
    }

    async fn set(&self, field: &str, value: serde_json::Value) -> SFResult<()> {
        self.inner.lock().await.insert(field.to_string(), value);
        Ok(())
    }

    async fn snapshot(&self) -> SFResult<serde_json::Value> {
        let guard = self.inner.lock().await;
        let map: serde_json::Map<String, serde_json::Value> =
            guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        Ok(serde_json::Value::Object(map))
    }

    async fn clear(&self) -> SFResult<()> {
        self.inner.lock().await.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared key-value implementation
// ---------------------------------------------------------------------------

/// [`ContextBoard`] that persists to a hash in a shared key-value store: one
/// field per board entry, addressed by
/// `orchestrator:squad:{squad_id}:board`.
///
/// Each field is stored as JSON-encoded text so structured values round-trip
/// through the store's string commands without further encoding hops.
#[derive(Clone)]
pub struct KeyValueContextBoard {
    store: Arc<dyn KeyValueStore>,
    key: String,
}

impl std::fmt::Debug for KeyValueContextBoard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyValueContextBoard")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl KeyValueContextBoard {
    /// Bind a board to the given Squad ID over an already-published store.
    ///
    /// The caller supplies the store rather than a URL: which process holds the
    /// connection, and whether it is a cache or another key-value backend, is
    /// the publisher's decision, and a board built from a URL would take that
    /// decision back — along with a link to the driver that implements it.
    /// The key is computed as `orchestrator:squad:{squad_id}:board`.
    pub fn with_store(store: Arc<dyn KeyValueStore>, squad_id: impl Into<String>) -> Self {
        let squad: String = squad_id.into();
        Self {
            store,
            key: format!("orchestrator:squad:{}:board", squad),
        }
    }

    /// Hash key in use (mainly exposed for diagnostics and tests).
    pub fn key(&self) -> &str {
        &self.key
    }
}

#[async_trait]
impl ContextBoard for KeyValueContextBoard {
    async fn get(&self, field: &str) -> SFResult<Option<serde_json::Value>> {
        match self.store.hget(&self.key, field).await? {
            Some(s) => {
                let v = serde_json::from_str(&s).map_err(SFError::Serialization)?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    async fn set(&self, field: &str, value: serde_json::Value) -> SFResult<()> {
        let json = serde_json::to_string(&value).map_err(SFError::Serialization)?;
        self.store.hset(&self.key, field, &json).await
    }

    async fn snapshot(&self) -> SFResult<serde_json::Value> {
        let fields = self.store.hgetall(&self.key).await?;
        let mut out = serde_json::Map::with_capacity(fields.len());
        for (k, v) in fields {
            let parsed: serde_json::Value =
                serde_json::from_str(&v).map_err(SFError::Serialization)?;
            out.insert(k, parsed);
        }
        Ok(serde_json::Value::Object(out))
    }

    async fn clear(&self) -> SFResult<()> {
        self.store.del(&self.key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_board_roundtrip() {
        let board = InMemoryContextBoard::new();
        board
            .set("foo", serde_json::json!({"bar": 42}))
            .await
            .unwrap();
        let v = board.get("foo").await.unwrap();
        assert_eq!(v, Some(serde_json::json!({"bar": 42})));

        let snap = board.snapshot().await.unwrap();
        assert_eq!(snap, serde_json::json!({"foo": {"bar": 42}}));

        board.clear().await.unwrap();
        assert!(board.get("foo").await.unwrap().is_none());
        assert_eq!(board.snapshot().await.unwrap(), serde_json::json!({}));
    }

    #[tokio::test]
    async fn in_memory_board_from_value_seeds_state() {
        let initial = serde_json::json!({"round": 1, "score": 80});
        let board = InMemoryContextBoard::from_value(initial.clone());
        assert_eq!(board.snapshot().await.unwrap(), initial);
    }

    #[tokio::test]
    async fn in_memory_board_from_non_object_is_empty() {
        let board = InMemoryContextBoard::from_value(serde_json::json!("not-an-object"));
        assert_eq!(board.snapshot().await.unwrap(), serde_json::json!({}));
    }

    /// The hash operations the shared board uses, over a map. The board is
    /// reached through the core contract, so the suite exercises it without a
    /// server, and the store's own expiry behaviour is tested with the driver
    /// that implements it.
    #[derive(Default)]
    struct FakeStore {
        hashes: Mutex<HashMap<String, HashMap<String, String>>>,
    }

    #[async_trait]
    impl KeyValueStore for FakeStore {
        async fn get(&self, _key: &str) -> SFResult<Option<String>> {
            unreachable!("the context board does not use plain keys")
        }

        async fn set(&self, _key: &str, _value: &str) -> SFResult<()> {
            unreachable!("the context board does not use plain keys")
        }

        async fn set_ex(&self, _key: &str, _value: &str, _ttl_secs: u64) -> SFResult<()> {
            unreachable!("the context board does not use plain keys")
        }

        async fn del(&self, key: &str) -> SFResult<()> {
            self.hashes.lock().await.remove(key);
            Ok(())
        }

        async fn expire(&self, _key: &str, _ttl_secs: i64) -> SFResult<()> {
            unreachable!("the context board sets no expiry")
        }

        async fn ttl(&self, _key: &str) -> SFResult<i64> {
            unreachable!("the context board sets no expiry")
        }

        async fn incr(&self, _key: &str, _by: i64) -> SFResult<i64> {
            unreachable!("the context board does not count")
        }

        async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> SFResult<i64> {
            unreachable!("the context board does not count")
        }

        async fn decr(&self, _key: &str, _by: i64) -> SFResult<i64> {
            unreachable!("the context board does not count")
        }

        async fn sadd(&self, _key: &str, _member: &str) -> SFResult<()> {
            unreachable!("the context board does not use sets")
        }

        async fn srem(&self, _key: &str, _member: &str) -> SFResult<()> {
            unreachable!("the context board does not use sets")
        }

        async fn smembers(&self, _key: &str) -> SFResult<Vec<String>> {
            unreachable!("the context board does not use sets")
        }

        async fn hset(&self, key: &str, field: &str, value: &str) -> SFResult<()> {
            self.hashes
                .lock()
                .await
                .entry(key.to_string())
                .or_default()
                .insert(field.to_string(), value.to_string());
            Ok(())
        }

        async fn hget(&self, key: &str, field: &str) -> SFResult<Option<String>> {
            Ok(self
                .hashes
                .lock()
                .await
                .get(key)
                .and_then(|hash| hash.get(field))
                .cloned())
        }

        async fn hdel(&self, key: &str, field: &str) -> SFResult<()> {
            if let Some(hash) = self.hashes.lock().await.get_mut(key) {
                hash.remove(field);
            }
            Ok(())
        }

        async fn hvals(&self, key: &str) -> SFResult<Vec<String>> {
            Ok(self
                .hashes
                .lock()
                .await
                .get(key)
                .map(|hash| hash.values().cloned().collect())
                .unwrap_or_default())
        }

        async fn hgetall(&self, key: &str) -> SFResult<Vec<(String, String)>> {
            Ok(self
                .hashes
                .lock()
                .await
                .get(key)
                .map(|hash| hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default())
        }

        async fn hlen(&self, key: &str) -> SFResult<u64> {
            Ok(self
                .hashes
                .lock()
                .await
                .get(key)
                .map(|hash| hash.len() as u64)
                .unwrap_or(0))
        }
    }

    #[tokio::test]
    async fn key_value_board_roundtrip_uses_the_design_doc_key() {
        let board = KeyValueContextBoard::with_store(Arc::new(FakeStore::default()), "squad-42");
        assert_eq!(board.key(), "orchestrator:squad:squad-42:board");

        board
            .set(
                "latest_plan",
                serde_json::json!({"approach": "incremental"}),
            )
            .await
            .unwrap();
        assert_eq!(
            board.get("latest_plan").await.unwrap(),
            Some(serde_json::json!({"approach": "incremental"}))
        );
        assert_eq!(board.get("absent").await.unwrap(), None);

        assert_eq!(
            board.snapshot().await.unwrap(),
            serde_json::json!({"latest_plan": {"approach": "incremental"}})
        );

        board.clear().await.unwrap();
        assert_eq!(board.snapshot().await.unwrap(), serde_json::json!({}));
        assert_eq!(board.get("latest_plan").await.unwrap(), None);
    }

    /// Two boards for the same squad over the same store see each other's
    /// writes: that sharing is the whole reason the durable board exists, and
    /// it is also what the in-memory one cannot do across processes.
    #[tokio::test]
    async fn key_value_boards_for_one_squad_share_state() {
        let store: Arc<dyn KeyValueStore> = Arc::new(FakeStore::default());
        let planner = KeyValueContextBoard::with_store(store.clone(), "squad-7");
        let generator = KeyValueContextBoard::with_store(store, "squad-7");
        planner.set("round", serde_json::json!(2)).await.unwrap();
        assert_eq!(
            generator.get("round").await.unwrap(),
            Some(serde_json::json!(2))
        );
    }

    /// Concurrent setters from multiple tasks must serialize cleanly through
    /// the inner Mutex.
    #[tokio::test]
    async fn in_memory_board_concurrent_writes() {
        let board = Arc::new(InMemoryContextBoard::new());
        let mut handles = Vec::new();
        for i in 0..10 {
            let b = board.clone();
            handles.push(tokio::spawn(async move {
                b.set(&format!("k{i}"), serde_json::json!(i)).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let snap = board.snapshot().await.unwrap();
        let obj = snap.as_object().unwrap();
        assert_eq!(obj.len(), 10);
        for i in 0..10 {
            assert_eq!(obj.get(&format!("k{i}")), Some(&serde_json::json!(i)));
        }
    }
}
