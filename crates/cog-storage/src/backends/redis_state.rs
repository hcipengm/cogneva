use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError};
use std::collections::HashMap;

use chrono::Utc;
use cog_core::{AgentState, ContextBoard, Event, SFError, SFResult, StateBackend, TaskCheckpoint};

// ─── Redis implementation ───

/// Redis-backed state backend.
pub struct RedisStateBackend {
    connection: MultiplexedConnection,
}

impl RedisStateBackend {
    pub async fn new(redis_url: &str) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| SFError::Redis(e.to_string()))?;
        let mut last_err = None;
        for attempt in 0..3 {
            match client.get_multiplexed_async_connection().await {
                Ok(connection) => return Ok(Self { connection }),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            200 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        Err(SFError::Redis(
            last_err
                .unwrap_or_else(|| {
                    redis::RedisError::from((redis::ErrorKind::IoError, "unknown connection error"))
                })
                .to_string(),
        ))
    }

    fn agent_key(agent_id: &str) -> String {
        format!("sf:agent:{agent_id}:state")
    }

    fn checkpoint_key(task_id: &str) -> String {
        format!("sf:task:{task_id}:checkpoint")
    }

    fn events_key(task_id: &str) -> String {
        format!("sf:task:{task_id}:events")
    }

    fn board_key(task_id: &str) -> String {
        format!("sf:task:{task_id}:board")
    }

    /// Retry a Redis operation with exponential backoff.
    async fn retry<T, F, Fut>(&self, mut op: F) -> SFResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, RedisError>>,
    {
        let mut last_err = None;
        for attempt in 0..3 {
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            200 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        Err(SFError::Redis(
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown redis error".into()),
        ))
    }
}

#[async_trait]
impl StateBackend for RedisStateBackend {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        let key = Self::agent_key(agent_id);
        let value: Option<String> = self
            .retry(|| async { self.connection.clone().get(&key).await })
            .await?;
        match value {
            Some(s) => {
                let state: AgentState = serde_json::from_str(&s).map_err(SFError::Serialization)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    async fn set_agent_state(&self, agent_id: &str, state: &AgentState) -> SFResult<()> {
        let key = Self::agent_key(agent_id);
        let value = serde_json::to_string(state)?;
        let _: () = self
            .retry(|| async { self.connection.clone().set(&key, value.clone()).await })
            .await?;
        Ok(())
    }

    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &AgentState,
        new: &AgentState,
    ) -> SFResult<bool> {
        let key = Self::agent_key(agent_id);
        let expected_json = serde_json::to_string(expected)?;
        let new_json = serde_json::to_string(new)?;

        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local expected = ARGV[1]
            local new_val = ARGV[2]
            local current = redis.call("get", key)
            if current == expected or (current == false and expected == "null") then
                redis.call("set", key, new_val)
                return 1
            else
                return 0
            end
            "#,
        );

        let result: i64 = self
            .retry(|| async {
                let mut conn = self.connection.clone();
                script
                    .key(&key)
                    .arg(&expected_json)
                    .arg(&new_json)
                    .invoke_async(&mut conn)
                    .await
            })
            .await?;

        Ok(result == 1)
    }

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let key = Self::checkpoint_key(task_id);
        let value: Option<String> = self
            .retry(|| async { self.connection.clone().get(&key).await })
            .await?;
        match value {
            Some(s) => {
                let cp: TaskCheckpoint =
                    serde_json::from_str(&s).map_err(SFError::Serialization)?;
                Ok(Some(cp))
            }
            None => Ok(None),
        }
    }

    async fn save_checkpoint(&self, checkpoint: &TaskCheckpoint) -> SFResult<()> {
        let key = Self::checkpoint_key(&checkpoint.task_id);
        let value = serde_json::to_string(checkpoint)?;
        let _: () = self
            .retry(|| async { self.connection.clone().set(&key, value.clone()).await })
            .await?;
        Ok(())
    }

    async fn append_event(&self, task_id: &str, event: &Event) -> SFResult<u64> {
        let key = Self::events_key(task_id);
        let value = serde_json::to_string(event)?;
        let len: i64 = self
            .retry(|| async { self.connection.clone().rpush(&key, value.clone()).await })
            .await?;
        Ok(len as u64)
    }

    async fn get_events(&self, task_id: &str, offset: u64, limit: usize) -> SFResult<Vec<Event>> {
        let key = Self::events_key(task_id);
        let start = offset as isize;
        let stop = start + limit as isize - 1;
        let values: Vec<String> = self
            .retry(|| async { self.connection.clone().lrange(&key, start, stop).await })
            .await?;
        let mut events = Vec::with_capacity(values.len());
        for v in values {
            let event: Event = serde_json::from_str(&v).map_err(SFError::Serialization)?;
            events.push(event);
        }
        Ok(events)
    }

    async fn get_board(&self, task_id: &str) -> SFResult<Option<ContextBoard>> {
        let key = Self::board_key(task_id);
        let fields: HashMap<String, String> = self
            .retry(|| async { self.connection.clone().hgetall(&key).await })
            .await?;
        if fields.is_empty() {
            return Ok(None);
        }
        Ok(Some(ContextBoard {
            task_id: task_id.into(),
            fields,
            updated_at: Utc::now(),
        }))
    }

    async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
        let key = Self::board_key(task_id);
        let _: () = self
            .retry(|| async { self.connection.clone().hset(&key, field, value).await })
            .await?;
        Ok(())
    }

    async fn delete_checkpoint(&self, task_id: &str) -> SFResult<()> {
        let key = Self::checkpoint_key(task_id);
        let _: () = self
            .retry(|| async { self.connection.clone().del(&key).await })
            .await?;
        Ok(())
    }

    async fn delete_board(&self, task_id: &str) -> SFResult<()> {
        let key = Self::board_key(task_id);
        let _: () = self
            .retry(|| async { self.connection.clone().del(&key).await })
            .await?;
        Ok(())
    }

    async fn remove_board_field(&self, task_id: &str, field: &str) -> SFResult<()> {
        let key = Self::board_key(task_id);
        let _: () = self
            .retry(|| async { self.connection.clone().hdel(&key, field).await })
            .await?;
        Ok(())
    }
}
