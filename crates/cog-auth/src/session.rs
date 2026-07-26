use chrono::Utc;
use redis::{aio::MultiplexedConnection, AsyncCommands};
use uuid::Uuid;

use crate::error::{AuthError, AuthResult};
use cog_core::SessionInfo;

/// Session TTL in seconds (7 days).
const SESSION_TTL_SECONDS: u64 = 7 * 24 * 60 * 60;

fn session_redis_key(user_id: Uuid, session_id: Uuid) -> String {
    format!("session:{user_id}:{session_id}")
}

fn user_sessions_key(user_id: Uuid) -> String {
    format!("user_sessions:{user_id}")
}

/// Redis-backed session manager.
#[derive(Clone)]
pub struct SessionManager {
    redis: std::sync::Arc<tokio::sync::Mutex<MultiplexedConnection>>,
    ttl_seconds: u64,
}

impl SessionManager {
    pub fn new(redis: MultiplexedConnection) -> Self {
        Self {
            redis: std::sync::Arc::new(tokio::sync::Mutex::new(redis)),
            ttl_seconds: SESSION_TTL_SECONDS,
        }
    }

    /// Create a new session and return its session ID.
    pub async fn create(&self, session: SessionInfo) -> AuthResult<Uuid> {
        let session_id = Uuid::new_v4();
        let key = session_redis_key(session.user_id, session_id);
        let value = serde_json::to_string(&session)?;

        let mut conn = self.redis.lock().await;
        let _: () = conn
            .set_ex(&key, value, self.ttl_seconds)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        // Track session in user's session set
        let set_key = user_sessions_key(session.user_id);
        let _: () = conn
            .sadd(&set_key, session_id.to_string())
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;
        // Expire the set key alongside individual sessions
        let _: () = conn
            .expire(&set_key, self.ttl_seconds as i64)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        Ok(session_id)
    }

    /// Retrieve a session by user ID and session ID.
    pub async fn get(&self, user_id: Uuid, session_id: Uuid) -> AuthResult<Option<SessionInfo>> {
        let key = session_redis_key(user_id, session_id);
        let mut conn = self.redis.lock().await;
        let value: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        match value {
            Some(v) => {
                let session: SessionInfo =
                    serde_json::from_str(&v).map_err(|e| AuthError::Internal(e.to_string()))?;
                Ok(Some(session))
            }
            None => Ok(None),
        }
    }

    /// Refresh the TTL of an existing session and update `last_active`.
    pub async fn refresh(&self, user_id: Uuid, session_id: Uuid) -> AuthResult<()> {
        let key = session_redis_key(user_id, session_id);
        let mut conn = self.redis.lock().await;
        let value: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        let mut session: SessionInfo = match value {
            Some(v) => serde_json::from_str(&v).map_err(|e| AuthError::Internal(e.to_string()))?,
            None => return Err(AuthError::SessionNotFound(session_id.to_string())),
        };

        session.last_active = Utc::now();
        let updated = serde_json::to_string(&session)?;

        let _: () = conn
            .set_ex(&key, updated, self.ttl_seconds)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        Ok(())
    }

    /// Destroy a single session.
    pub async fn destroy(&self, user_id: Uuid, session_id: Uuid) -> AuthResult<()> {
        let key = session_redis_key(user_id, session_id);
        let mut conn = self.redis.lock().await;
        let _: () = conn
            .del(&key)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        let set_key = user_sessions_key(user_id);
        let _: () = conn
            .srem(&set_key, session_id.to_string())
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        Ok(())
    }

    /// Destroy all sessions for a user.
    pub async fn destroy_all(&self, user_id: Uuid) -> AuthResult<()> {
        let set_key = user_sessions_key(user_id);
        let mut conn = self.redis.lock().await;
        let session_ids: Vec<String> = conn
            .smembers(&set_key)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        for sid in session_ids {
            let key = format!("session:{user_id}:{sid}");
            let _: () = conn
                .del(&key)
                .await
                .map_err(|e| AuthError::RedisError(e.to_string()))?;
        }

        let _: () = conn
            .del(&set_key)
            .await
            .map_err(|e| AuthError::RedisError(e.to_string()))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl cog_core::SessionManager for SessionManager {
    async fn create(&self, session: cog_core::SessionInfo) -> cog_core::SFResult<Uuid> {
        self.create(session)
            .await
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }

    async fn get(
        &self,
        user_id: Uuid,
        session_id: Uuid,
    ) -> cog_core::SFResult<Option<cog_core::SessionInfo>> {
        self.get(user_id, session_id)
            .await
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }

    async fn destroy(&self, user_id: Uuid, session_id: Uuid) -> cog_core::SFResult<()> {
        self.destroy(user_id, session_id)
            .await
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }

    async fn refresh(&self, user_id: Uuid, session_id: Uuid) -> cog_core::SFResult<()> {
        self.refresh(user_id, session_id)
            .await
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }

    async fn destroy_all(&self, user_id: Uuid) -> cog_core::SFResult<()> {
        self.destroy_all(user_id)
            .await
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }
}
