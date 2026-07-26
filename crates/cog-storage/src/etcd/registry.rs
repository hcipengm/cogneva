//! etcd-backed [`AgentRegistry`] for production environments.
//! Uses etcd **leases** for TTL-based expiration, providing stronger
//! consistency and lower memory footprint than Redis for small,
//! temporary registry data.

use async_trait::async_trait;
use etcd_client::{Client, PutOptions};
use serde_json;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

use cog_core::{AgentRegistration, AgentRegistry, SFError, SFResult};

const KEY_PREFIX: &str = "/sf/agents/";

/// etcd-backed [`AgentRegistry`].
pub struct EtcdAgentRegistry {
    client: Arc<Mutex<Client>>,
    ttl_seconds: i64,
    /// In-memory mapping of agent_id -> lease_id so heartbeat can refresh
    /// the correct lease.  This is process-local: each agent is heartbeated
    /// by the process that registered it.
    leases: Arc<RwLock<HashMap<String, i64>>>,
}

impl EtcdAgentRegistry {
    pub async fn new(endpoints: &[&str]) -> SFResult<Self> {
        let client = Client::connect(endpoints, None)
            .await
            .map_err(|e| SFError::Redis(format!("etcd connect failed: {e}")))?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            ttl_seconds: 30,
            leases: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn from_url(url: &str) -> SFResult<Self> {
        Self::new(&[url]).await
    }

    pub fn with_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds as i64;
        self
    }

    fn key(agent_id: &str) -> String {
        format!("{}{}", KEY_PREFIX, agent_id)
    }
}

#[async_trait]
impl AgentRegistry for EtcdAgentRegistry {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
        let mut client = self.client.lock().await;
        let lease_resp = client
            .lease_grant(self.ttl_seconds, None)
            .await
            .map_err(|e| SFError::Agent(format!("etcd lease grant failed: {e}")))?;
        let lease_id = lease_resp.id();
        let payload = serde_json::to_string(registration).map_err(SFError::Serialization)?;
        client
            .put(
                Self::key(&registration.agent_id),
                payload,
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await
            .map_err(|e| SFError::Agent(format!("etcd put failed: {e}")))?;
        let mut leases = self
            .leases
            .write()
            .map_err(|_| SFError::Agent("etcd registry lease lock poisoned".into()))?;
        leases.insert(registration.agent_id.clone(), lease_id);
        Ok(())
    }

    async fn heartbeat(&self, agent_id: &str) -> SFResult<()> {
        let lease_id = {
            let leases = self
                .leases
                .read()
                .map_err(|_| SFError::Agent("etcd registry lease lock poisoned".into()))?;
            match leases.get(agent_id) {
                Some(&id) => id,
                None => {
                    return Err(SFError::Agent(format!(
                        "heartbeat: agent {agent_id} not registered (no lease)"
                    )))
                }
            }
        };
        let mut client = self.client.lock().await;
        let _ = client
            .lease_keep_alive(lease_id)
            .await
            .map_err(|e| SFError::Agent(format!("etcd lease keep-alive failed: {e}")))?;
        let get_resp = client
            .get(Self::key(agent_id), None)
            .await
            .map_err(|e| SFError::Agent(format!("etcd get failed: {e}")))?;
        let kv = get_resp
            .kvs()
            .first()
            .ok_or_else(|| SFError::Agent(format!("heartbeat: agent {agent_id} key missing")))?;
        let mut reg: AgentRegistration =
            serde_json::from_slice(kv.value()).map_err(SFError::Serialization)?;
        reg.last_heartbeat = chrono::Utc::now();
        let payload = serde_json::to_string(&reg).map_err(SFError::Serialization)?;
        client
            .put(
                Self::key(agent_id),
                payload,
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await
            .map_err(|e| SFError::Agent(format!("etcd put failed: {e}")))?;
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> SFResult<()> {
        let lease_id = {
            let leases = self
                .leases
                .read()
                .map_err(|_| SFError::Agent("etcd registry lease lock poisoned".into()))?;
            leases.get(agent_id).copied()
        };
        let mut client = self.client.lock().await;
        if let Some(id) = lease_id {
            let _ = client.lease_revoke(id).await;
        }
        let _ = client
            .delete(Self::key(agent_id), None)
            .await
            .map_err(|e| SFError::Agent(format!("etcd delete failed: {e}")))?;
        let mut leases = self
            .leases
            .write()
            .map_err(|_| SFError::Agent("etcd registry lease lock poisoned".into()))?;
        leases.remove(agent_id);
        Ok(())
    }

    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        let mut client = self.client.lock().await;
        let resp = client
            .get(Self::key(agent_id), None)
            .await
            .map_err(|e| SFError::Agent(format!("etcd get failed: {e}")))?;
        match resp.kvs().first() {
            Some(kv) => {
                let reg: AgentRegistration =
                    serde_json::from_slice(kv.value()).map_err(SFError::Serialization)?;
                Ok(Some(reg))
            }
            None => Ok(None),
        }
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        let mut client = self.client.lock().await;
        let resp = client
            .get(
                KEY_PREFIX,
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await
            .map_err(|e| SFError::Agent(format!("etcd prefix get failed: {e}")))?;
        let mut out = Vec::new();
        for kv in resp.kvs() {
            match serde_json::from_slice::<AgentRegistration>(kv.value()) {
                Ok(reg) => out.push(reg),
                Err(e) => tracing::warn!("failed to deserialize agent reg: {e}"),
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
