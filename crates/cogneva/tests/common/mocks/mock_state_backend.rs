use async_trait::async_trait;
use cog_core::{AgentState, ContextBoard, Event, SFError, SFResult, StateBackend, TaskCheckpoint};
use std::collections::HashMap;
use std::sync::Mutex;

/// A mock [`StateBackend`] for testing.
/// Uses an in-memory `Mutex<HashMap>` and tracks all `set_agent_state` calls
/// for spy assertions.
pub struct MockStateBackend {
    store: Mutex<HashMap<String, serde_json::Value>>,
    state_calls: Mutex<Vec<(String, AgentState)>>,
}

#[allow(dead_code)]
impl MockStateBackend {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            state_calls: Mutex::new(Vec::new()),
        }
    }

    /// Return all recorded `set_agent_state` calls.
    pub fn recorded_state_calls(&self) -> Vec<(String, AgentState)> {
        self.state_calls.lock().unwrap().clone()
    }

    fn key_agent(agent_id: &str) -> String {
        format!("agent:{}", agent_id)
    }

    fn key_checkpoint(task_id: &str) -> String {
        format!("checkpoint:{}", task_id)
    }

    fn key_events(task_id: &str) -> String {
        format!("events:{}", task_id)
    }

    fn key_board(task_id: &str) -> String {
        format!("board:{}", task_id)
    }
}

impl Default for MockStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackend for MockStateBackend {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        let store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        match store.get(&Self::key_agent(agent_id)) {
            Some(v) => {
                let state: AgentState =
                    serde_json::from_value(v.clone()).map_err(SFError::Serialization)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    async fn set_agent_state(&self, agent_id: &str, state: &AgentState) -> SFResult<()> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut calls = self
            .state_calls
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let value = serde_json::to_value(state).map_err(SFError::Serialization)?;
        store.insert(Self::key_agent(agent_id), value);
        calls.push((agent_id.into(), *state));
        Ok(())
    }

    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &AgentState,
        new: &AgentState,
    ) -> SFResult<bool> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let key = Self::key_agent(agent_id);
        let current = store
            .get(&key)
            .and_then(|v| serde_json::from_value::<AgentState>(v.clone()).ok());
        if current.as_ref() == Some(expected) {
            let value = serde_json::to_value(new).map_err(SFError::Serialization)?;
            store.insert(key, value);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        match store.get(&Self::key_checkpoint(task_id)) {
            Some(v) => {
                let cp: TaskCheckpoint =
                    serde_json::from_value(v.clone()).map_err(SFError::Serialization)?;
                Ok(Some(cp))
            }
            None => Ok(None),
        }
    }

    async fn save_checkpoint(&self, checkpoint: &TaskCheckpoint) -> SFResult<()> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let value = serde_json::to_value(checkpoint).map_err(SFError::Serialization)?;
        store.insert(Self::key_checkpoint(&checkpoint.task_id), value);
        Ok(())
    }

    async fn append_event(&self, task_id: &str, event: &Event) -> SFResult<u64> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let key = Self::key_events(task_id);
        let mut list: Vec<Event> = match store.get(&key) {
            Some(v) => serde_json::from_value(v.clone()).unwrap_or_default(),
            None => Vec::new(),
        };
        list.push(event.clone());
        let len = list.len() as u64;
        store.insert(
            key,
            serde_json::to_value(list).map_err(SFError::Serialization)?,
        );
        Ok(len)
    }

    async fn get_events(&self, task_id: &str, offset: u64, limit: usize) -> SFResult<Vec<Event>> {
        let store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        match store.get(&Self::key_events(task_id)) {
            Some(v) => {
                let list: Vec<Event> = serde_json::from_value(v.clone()).unwrap_or_default();
                let start = offset as usize;
                let end = (start + limit).min(list.len());
                if start >= list.len() {
                    Ok(Vec::new())
                } else {
                    Ok(list[start..end].to_vec())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    async fn get_board(&self, task_id: &str) -> SFResult<Option<ContextBoard>> {
        let store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        match store.get(&Self::key_board(task_id)) {
            Some(v) => {
                let board: ContextBoard =
                    serde_json::from_value(v.clone()).map_err(SFError::Serialization)?;
                Ok(Some(board))
            }
            None => Ok(None),
        }
    }

    async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let key = Self::key_board(task_id);
        let mut board: ContextBoard = match store.get(&key) {
            Some(v) => serde_json::from_value(v.clone()).unwrap_or_else(|_| ContextBoard {
                task_id: task_id.into(),
                fields: HashMap::new(),
                updated_at: chrono::Utc::now(),
            }),
            None => ContextBoard {
                task_id: task_id.into(),
                fields: HashMap::new(),
                updated_at: chrono::Utc::now(),
            },
        };
        board.fields.insert(field.into(), value.into());
        board.updated_at = chrono::Utc::now();
        store.insert(
            key,
            serde_json::to_value(board).map_err(SFError::Serialization)?,
        );
        Ok(())
    }
    async fn delete_checkpoint(&self, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.remove(&Self::key_checkpoint(task_id));
        Ok(())
    }

    async fn delete_board(&self, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.remove(&Self::key_board(task_id));
        Ok(())
    }

    async fn remove_board_field(&self, task_id: &str, field: &str) -> SFResult<()> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let key = Self::key_board(task_id);
        if let Some(v) = store.get(&key) {
            let mut board: ContextBoard =
                serde_json::from_value(v.clone()).unwrap_or_else(|_| ContextBoard {
                    task_id: task_id.into(),
                    fields: HashMap::new(),
                    updated_at: chrono::Utc::now(),
                });
            board.fields.remove(field);
            board.updated_at = chrono::Utc::now();
            store.insert(
                key,
                serde_json::to_value(board).map_err(SFError::Serialization)?,
            );
        }
        Ok(())
    }
}
