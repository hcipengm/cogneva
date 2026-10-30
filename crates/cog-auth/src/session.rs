use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

use crate::error::{AuthError, AuthResult};
use cog_core::{KeyValueStore, SessionInfo};

/// Session TTL in seconds (7 days).
// cap: bound
const SESSION_TTL_SECONDS: u64 = 7 * 24 * 60 * 60;

fn session_redis_key(user_id: Uuid, session_id: Uuid) -> String {
    format!("session:{user_id}:{session_id}")
}

fn user_sessions_key(user_id: Uuid) -> String {
    format!("user_sessions:{user_id}")
}

/// Map a store failure into the auth error type.
///
/// The store reports a cache failure as `SFError::Redis`; keeping that as
/// `AuthError::RedisError` rather than letting it fall through the blanket
/// `From<SFError>` (which maps to `Internal`) preserves the `redis_error` code
/// the HTTP layer already returns for a cache outage.
fn store_error(e: cog_core::SFError) -> AuthError {
    AuthError::RedisError(e.to_string())
}

/// Key-value-backed session manager.
///
/// Holds the contract rather than a driver connection: the session payloads are
/// JSON strings and the bookkeeping is a set of session ids per user, both of
/// which [`KeyValueStore`] covers, so this crate links no cache implementation.
#[derive(Clone)]
pub struct SessionManager {
    store: Arc<dyn KeyValueStore>,
    ttl_seconds: u64,
}

impl SessionManager {
    /// Build a session manager over a key-value store.
    pub fn new(store: Arc<dyn KeyValueStore>) -> Self {
        Self {
            store,
            ttl_seconds: SESSION_TTL_SECONDS,
        }
    }

    /// Create a new session and return its session ID.
    pub async fn create(&self, session: SessionInfo) -> AuthResult<Uuid> {
        let session_id = Uuid::new_v4();
        let key = session_redis_key(session.user_id, session_id);
        let value = serde_json::to_string(&session)?;

        self.store
            .set_ex(&key, &value, self.ttl_seconds)
            .await
            .map_err(store_error)?;

        // Track session in user's session set
        let set_key = user_sessions_key(session.user_id);
        self.store
            .sadd(&set_key, &session_id.to_string())
            .await
            .map_err(store_error)?;
        // Expire the set key alongside individual sessions
        self.store
            .expire(&set_key, self.ttl_seconds as i64)
            .await
            .map_err(store_error)?;

        Ok(session_id)
    }

    /// Retrieve a session by user ID and session ID.
    pub async fn get(&self, user_id: Uuid, session_id: Uuid) -> AuthResult<Option<SessionInfo>> {
        let key = session_redis_key(user_id, session_id);
        let value = self.store.get(&key).await.map_err(store_error)?;

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
        let value = self.store.get(&key).await.map_err(store_error)?;

        let mut session: SessionInfo = match value {
            Some(v) => serde_json::from_str(&v).map_err(|e| AuthError::Internal(e.to_string()))?,
            None => return Err(AuthError::SessionNotFound(session_id.to_string())),
        };

        session.last_active = Utc::now();
        let updated = serde_json::to_string(&session)?;

        self.store
            .set_ex(&key, &updated, self.ttl_seconds)
            .await
            .map_err(store_error)?;

        Ok(())
    }

    /// Destroy a single session.
    pub async fn destroy(&self, user_id: Uuid, session_id: Uuid) -> AuthResult<()> {
        let key = session_redis_key(user_id, session_id);
        self.store.del(&key).await.map_err(store_error)?;

        let set_key = user_sessions_key(user_id);
        self.store
            .srem(&set_key, &session_id.to_string())
            .await
            .map_err(store_error)?;

        Ok(())
    }

    /// Destroy all sessions for a user.
    pub async fn destroy_all(&self, user_id: Uuid) -> AuthResult<()> {
        let set_key = user_sessions_key(user_id);
        let session_ids = self.store.smembers(&set_key).await.map_err(store_error)?;

        for sid in session_ids {
            let key = format!("session:{user_id}:{sid}");
            self.store.del(&key).await.map_err(store_error)?;
        }

        self.store.del(&set_key).await.map_err(store_error)?;

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
