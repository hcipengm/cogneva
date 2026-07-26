//! Redis-backed and in-memory [`AgentRegistry`] implementations.
//! Moved from `cog-core` so that `cog-core` only contains the trait + data
//! structures and concrete implementations live in `sf-db`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError};
use std::collections::HashMap;
use std::sync::RwLock;

use cog_core::{AgentRegistration, AgentRegistry, SFError, SFResult};

fn agent_key(agent_id: &str) -> String {
    format!("orchestrator:agents:{agent_id}")
}

const REGISTRY_INDEX_KEY: &str = "orchestrator:agents:index";

// ─── Redis implementation ───

/// Redis-backed [`AgentRegistry`].
pub struct RedisAgentRegistry {
    connection: MultiplexedConnection,
    ttl_seconds: u64,
}

impl RedisAgentRegistry {
    pub fn new(connection: MultiplexedConnection) -> Self {
        Self {
            connection,
            ttl_seconds: 30,
        }
    }

    pub fn with_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    pub async fn from_url(url: &str) -> SFResult<Self> {
        let client = redis::Client::open(url).map_err(|e| SFError::Redis(e.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SFError::Redis(e.to_string()))?;
        Ok(Self::new(connection))
    }
}

#[async_trait]
impl AgentRegistry for RedisAgentRegistry {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
        let key = agent_key(&registration.agent_id);
        let payload = serde_json::to_string(registration).map_err(SFError::Serialization)?;
        let mut conn = self.connection.clone();
        let _: () = conn
            .set(&key, payload)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let _: () = conn
            .expire(&key, self.ttl_seconds as i64)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let _: () = conn
            .sadd(REGISTRY_INDEX_KEY, &registration.agent_id)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn heartbeat(&self, agent_id: &str) -> SFResult<()> {
        let key = agent_key(agent_id);
        let mut conn = self.connection.clone();
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let Some(raw) = raw else {
            return Err(SFError::Agent(format!(
                "heartbeat: agent {agent_id} not registered (key expired?)"
            )));
        };
        let mut reg: AgentRegistration =
            serde_json::from_str(&raw).map_err(SFError::Serialization)?;
        reg.last_heartbeat = Utc::now();
        let payload = serde_json::to_string(&reg).map_err(SFError::Serialization)?;
        let _: () = conn
            .set(&key, payload)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let _: () = conn
            .expire(&key, self.ttl_seconds as i64)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> SFResult<()> {
        let key = agent_key(agent_id);
        let mut conn = self.connection.clone();
        let _: () = conn
            .del(&key)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let _: () = conn
            .srem(REGISTRY_INDEX_KEY, agent_id)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        let key = agent_key(agent_id);
        let mut conn = self.connection.clone();
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        match raw {
            Some(s) => Ok(Some(
                serde_json::from_str(&s).map_err(SFError::Serialization)?,
            )),
            None => Ok(None),
        }
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        let mut conn = self.connection.clone();
        let ids: Vec<String> = conn
            .smembers(REGISTRY_INDEX_KEY)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(reg) = AgentRegistry::get(self, &id).await? {
                out.push(reg);
            } else {
                // Stale id — its key TTL expired but the index still holds it. Clean up.
                let _: () = conn
                    .srem(REGISTRY_INDEX_KEY, &id)
                    .await
                    .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
            }
        }
        Ok(out)
    }

    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all.into_iter().filter(|r| r.role == role).collect())
    }

    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all
            .into_iter()
            .filter(|r| r.capabilities.contains(&capability.to_string()))
            .collect())
    }
}

// ─── In-memory implementation ───

#[derive(Debug, Default)]
struct MemoryStore {
    agents: HashMap<String, (AgentRegistration, DateTime<Utc>)>, // (reg, expires_at)
}

/// In-memory [`AgentRegistry`] for unit tests.  TTL is honoured logically:
/// `get` / `list` filter out entries whose `expires_at` has passed.
pub struct MemoryAgentRegistry {
    store: RwLock<MemoryStore>,
    ttl_seconds: u64,
}

impl MemoryAgentRegistry {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(MemoryStore::default()),
            ttl_seconds: 30,
        }
    }

    pub fn with_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    fn expires_at(&self) -> DateTime<Utc> {
        Utc::now() + chrono::Duration::seconds(self.ttl_seconds as i64)
    }
}

impl Default for MemoryAgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AgentRegistry for MemoryAgentRegistry {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        store.agents.insert(
            registration.agent_id.clone(),
            (registration.clone(), self.expires_at()),
        );
        Ok(())
    }

    async fn heartbeat(&self, agent_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        let entry = store.agents.get_mut(agent_id).ok_or_else(|| {
            SFError::Agent(format!(
                "heartbeat: agent {agent_id} not registered (key expired?)"
            ))
        })?;
        if entry.1 < Utc::now() {
            // Already expired. Remove and reject — caller must re-register.
            store.agents.remove(agent_id);
            return Err(SFError::Agent(format!(
                "heartbeat: agent {agent_id} ttl expired"
            )));
        }
        entry.0.last_heartbeat = Utc::now();
        entry.1 = Utc::now() + chrono::Duration::seconds(self.ttl_seconds as i64);
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        store.agents.remove(agent_id);
        Ok(())
    }

    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        match store.agents.get(agent_id) {
            Some((reg, expires)) if *expires >= Utc::now() => Ok(Some(reg.clone())),
            _ => Ok(None),
        }
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        let now = Utc::now();
        Ok(store
            .agents
            .values()
            .filter(|(_, exp)| *exp >= now)
            .map(|(r, _)| r.clone())
            .collect())
    }

    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all.into_iter().filter(|r| r.role == role).collect())
    }

    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all
            .into_iter()
            .filter(|r| r.capabilities.contains(&capability.to_string()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn memory_registry_register_get_list_heartbeat() {
        let reg: Arc<dyn AgentRegistry> = Arc::new(MemoryAgentRegistry::new());
        let agent_id = crate::generate_agent_id("host-1", "10.0.0.1", "planner", "uuid-1");
        let r = AgentRegistration::new(
            agent_id.clone(),
            "host-1",
            "10.0.0.1",
            "planner",
            "ws-1",
            vec!["code".into()],
            cog_core::ResourceInfo {
                cpu_cores: 4,
                memory_gb: 8,
            },
        );

        reg.register(&r).await.unwrap();
        let got = reg.get(&r.agent_id).await.unwrap().expect("registered");
        assert_eq!(got.agent_id, r.agent_id);
        assert_eq!(got.role, "planner");

        let all = reg.list().await.unwrap();
        assert_eq!(all.len(), 1);

        // heartbeat advances last_heartbeat
        let before = got.last_heartbeat;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        reg.heartbeat(&r.agent_id).await.unwrap();
        let after = reg.get(&r.agent_id).await.unwrap().unwrap();
        assert!(after.last_heartbeat > before);

        // deregister removes
        reg.deregister(&r.agent_id).await.unwrap();
        assert!(reg.get(&r.agent_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_registry_ttl_expiry_filters_stale_entries() {
        let reg: Arc<dyn AgentRegistry> = Arc::new(MemoryAgentRegistry::new().with_ttl_seconds(0));
        let agent_id = crate::generate_agent_id("host-1", "10.0.0.1", "planner", "uuid-1");
        let r = AgentRegistration::new(
            agent_id,
            "host-1",
            "10.0.0.1",
            "planner",
            "ws-1",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        reg.register(&r).await.unwrap();
        // ttl_seconds = 0 means already expired by now.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(reg.get(&r.agent_id).await.unwrap().is_none());
        assert_eq!(reg.list().await.unwrap().len(), 0);
        // heartbeat after expiry must fail
        let err = reg.heartbeat(&r.agent_id).await;
        assert!(err.is_err());
    }
}
