//! In-memory object backend for testing and local development.

use async_trait::async_trait;
use cog_core::{ObjectBackend, SFError, SFResult};
use std::collections::HashMap;
use std::sync::RwLock;

/// In-memory object backend for testing and local development.
#[derive(Debug, Default)]
pub struct MemoryObjectBackend {
    objects: RwLock<HashMap<String, Vec<u8>>>,
}

impl MemoryObjectBackend {
    pub fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ObjectBackend for MemoryObjectBackend {
    async fn put(&self, key: &str, data: &[u8]) -> SFResult<String> {
        let mut store = self
            .objects
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.insert(key.into(), data.into());
        Ok(format!("memory://{}", key))
    }

    async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
        let store = self
            .objects
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.get(key).cloned())
    }

    async fn delete(&self, key: &str) -> SFResult<()> {
        let mut store = self
            .objects
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.remove(key);
        Ok(())
    }

    async fn presign_url(&self, key: &str, _expiry_secs: u64) -> SFResult<String> {
        Ok(format!("memory://{}", key))
    }

    async fn exists(&self, key: &str) -> SFResult<bool> {
        let store = self
            .objects
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.contains_key(key))
    }

    async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>> {
        let store = self
            .objects
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let keys: Vec<String> = match prefix {
            Some(p) => store.keys().filter(|k| k.starts_with(p)).cloned().collect(),
            None => store.keys().cloned().collect(),
        };
        Ok(keys)
    }
}
