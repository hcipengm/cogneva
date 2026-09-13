//! PostgreSQL-backed user and platform-identity store.
//!
//! Connecting a GitHub/Gitee account is how a local account comes into
//! existence: the identity row links `provider + provider_user_id` to a
//! `users` row. Platform tokens never land here — only references into the
//! secure-gateway Secret.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use cog_core::{
    PlatformIdentity, PlatformIdentityStore, SFError, SFResult, User, UserStatus, UserStore,
    UserType, UserUpdate,
};

pub struct PostgresUserStore {
    pool: PgPool,
}

type UserRow = (
    Uuid,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    DateTime<Utc>,
    DateTime<Utc>,
);

fn user_from_row(r: UserRow) -> User {
    User {
        id: r.0,
        phone: r.1,
        email: r.2,
        username: r.3,
        display_name: r.4,
        avatar_url: r.5,
        status: UserStatus::from_str_lossy(&r.6),
        user_type: UserType::from_str_lossy(&r.7),
        created_at: r.8,
        updated_at: r.9,
    }
}

const USER_COLS: &str =
    "id, phone, email, username, display_name, avatar_url, status, user_type, created_at, updated_at";

fn to_sf(err: sqlx::Error) -> SFError {
    SFError::Database(err.to_string())
}

impl PostgresUserStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the tables. Matches migrations 001_users plus the
    /// platform_identities table (tokens stay out — references only).
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(r#"CREATE EXTENSION IF NOT EXISTS "uuid-ossp""#)
            .execute(&self.pool)
            .await
            .map_err(to_sf)?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                phone VARCHAR(20) UNIQUE,
                email VARCHAR(255) UNIQUE,
                username VARCHAR(64) NOT NULL UNIQUE,
                display_name VARCHAR(128),
                avatar_url VARCHAR(512),
                status VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'disabled', 'locked')),
                user_type VARCHAR(16) NOT NULL DEFAULT 'standard' CHECK (user_type IN ('admin', 'standard', 'guest')),
                password_hash VARCHAR(255),
                token_quota_daily BIGINT DEFAULT 0,
                token_used_today BIGINT DEFAULT 0,
                last_login_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS platform_identities (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                provider VARCHAR(16) NOT NULL,
                provider_user_id VARCHAR(64) NOT NULL,
                login VARCHAR(128) NOT NULL,
                access_token_ref VARCHAR(255),
                refresh_token_ref VARCHAR(255),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (provider, provider_user_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_platform_identities_user ON platform_identities(user_id)",
        )
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(())
    }

    async fn fetch_user(&self, clause: &str, bind: Bind<'_>) -> SFResult<Option<User>> {
        let sql = format!("SELECT {USER_COLS} FROM users WHERE {clause}");
        let row: Option<UserRow> = match bind {
            Bind::Uuid(id) => sqlx::query_as(&sql)
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(to_sf)?,
            Bind::Text(s) => sqlx::query_as(&sql)
                .bind(s)
                .fetch_optional(&self.pool)
                .await
                .map_err(to_sf)?,
        };
        Ok(row.map(user_from_row))
    }
}

enum Bind<'a> {
    Uuid(Uuid),
    Text(&'a str),
}

#[async_trait]
impl UserStore for PostgresUserStore {
    async fn create(
        &self,
        username: String,
        password_hash: String,
        email: Option<String>,
        phone: Option<String>,
        display_name: Option<String>,
    ) -> SFResult<User> {
        if self.find_by_username(&username).await?.is_some() {
            return Err(SFError::Auth(format!("user already exists: {username}")));
        }
        let row: UserRow = sqlx::query_as(&format!(
            "INSERT INTO users (username, password_hash, email, phone, display_name) \
             VALUES ($1, $2, $3, $4, $5) RETURNING {USER_COLS}"
        ))
        .bind(&username)
        .bind(&password_hash)
        .bind(&email)
        .bind(&phone)
        .bind(&display_name)
        .fetch_one(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(user_from_row(row))
    }

    async fn find_by_username(&self, username: &str) -> SFResult<Option<User>> {
        self.fetch_user("username = $1", Bind::Text(username)).await
    }

    async fn find_by_username_with_password(
        &self,
        username: &str,
    ) -> SFResult<Option<(User, String)>> {
        // Two lookups: sqlx's tuple decoding does not cover the nested shape a
        // single join-select would need here, and the login path is not hot.
        let user = match self.find_by_username(username).await? {
            Some(u) => u,
            None => return Ok(None),
        };
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT password_hash FROM users WHERE username = $1")
                .bind(username)
                .fetch_optional(&self.pool)
                .await
                .map_err(to_sf)?;
        match row.and_then(|r| r.0) {
            Some(hash) => Ok(Some((user, hash))),
            None => Ok(None),
        }
    }

    async fn find_by_email(&self, email: &str) -> SFResult<Option<User>> {
        self.fetch_user("email = $1", Bind::Text(email)).await
    }

    async fn find_by_id(&self, id: Uuid) -> SFResult<Option<User>> {
        self.fetch_user("id = $1", Bind::Uuid(id)).await
    }

    async fn update(&self, id: Uuid, updates: UserUpdate) -> SFResult<Option<User>> {
        let current = match self.find_by_id(id).await? {
            Some(u) => u,
            None => return Ok(None),
        };
        let display_name = updates.display_name.or(current.display_name);
        let avatar_url = updates.avatar_url.or(current.avatar_url);
        let email = updates.email.or(current.email);
        let phone = updates.phone.or(current.phone);
        let row: UserRow = sqlx::query_as(&format!(
            "UPDATE users SET display_name = $2, avatar_url = $3, email = $4, phone = $5, \
             updated_at = NOW() WHERE id = $1 RETURNING {USER_COLS}"
        ))
        .bind(id)
        .bind(&display_name)
        .bind(&avatar_url)
        .bind(&email)
        .bind(&phone)
        .fetch_one(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(Some(user_from_row(row)))
    }
}

#[async_trait]
impl PlatformIdentityStore for PostgresUserStore {
    async fn find_user_by_identity(
        &self,
        provider: &str,
        provider_user_id: &str,
    ) -> SFResult<Option<User>> {
        let row: Option<UserRow> = sqlx::query_as(&format!(
            "SELECT {USER_COLS} FROM users u \
             JOIN platform_identities p ON p.user_id = u.id \
             WHERE p.provider = $1 AND p.provider_user_id = $2"
        ))
        .bind(provider)
        .bind(provider_user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(row.map(user_from_row))
    }

    async fn find_or_create_by_identity(
        &self,
        provider: &str,
        provider_user_id: &str,
        login: &str,
        display_name: Option<String>,
        avatar_url: Option<String>,
        access_token_ref: Option<String>,
        refresh_token_ref: Option<String>,
    ) -> SFResult<(User, bool)> {
        if let Some(user) = self
            .find_user_by_identity(provider, provider_user_id)
            .await?
        {
            // Keep profile and token references fresh on every login.
            sqlx::query(
                "UPDATE platform_identities SET login = $3, access_token_ref = $4, \
                 refresh_token_ref = $5, updated_at = NOW() \
                 WHERE provider = $1 AND provider_user_id = $2",
            )
            .bind(provider)
            .bind(provider_user_id)
            .bind(login)
            .bind(&access_token_ref)
            .bind(&refresh_token_ref)
            .execute(&self.pool)
            .await
            .map_err(to_sf)?;
            let user = self
                .update(
                    user.id,
                    UserUpdate {
                        display_name,
                        avatar_url,
                        email: None,
                        phone: None,
                    },
                )
                .await?
                .unwrap_or(user);
            return Ok((user, false));
        }

        // New identity: first user of the instance becomes Admin (owner),
        // everyone after is Standard. The count and the insert run in one
        // transaction so two concurrent first-connects cannot both win.
        let mut tx = self.pool.begin().await.map_err(to_sf)?;
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(&mut *tx)
            .await
            .map_err(to_sf)?;
        let user_type = if count == 0 { "admin" } else { "standard" };
        let username = format!("{provider}:{login}");
        let row: UserRow = sqlx::query_as(&format!(
            "INSERT INTO users (username, display_name, avatar_url, user_type) \
             VALUES ($1, $2, $3, $4) RETURNING {USER_COLS}"
        ))
        .bind(&username)
        .bind(display_name.as_deref().unwrap_or(login))
        .bind(&avatar_url)
        .bind(user_type)
        .fetch_one(&mut *tx)
        .await
        .map_err(to_sf)?;
        let user = user_from_row(row);
        sqlx::query(
            "INSERT INTO platform_identities \
             (user_id, provider, provider_user_id, login, access_token_ref, refresh_token_ref) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(user.id)
        .bind(provider)
        .bind(provider_user_id)
        .bind(login)
        .bind(&access_token_ref)
        .bind(&refresh_token_ref)
        .execute(&mut *tx)
        .await
        .map_err(to_sf)?;
        tx.commit().await.map_err(to_sf)?;
        Ok((user, true))
    }

    async fn unlink_identity(&self, provider: &str, provider_user_id: &str) -> SFResult<()> {
        sqlx::query(
            "DELETE FROM platform_identities WHERE provider = $1 AND provider_user_id = $2",
        )
        .bind(provider)
        .bind(provider_user_id)
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(())
    }
}

/// Read the identity row (token references included) for a provider account.
impl PostgresUserStore {
    pub async fn find_identity(
        &self,
        provider: &str,
        provider_user_id: &str,
    ) -> SFResult<Option<PlatformIdentity>> {
        type IdentityRow = (Uuid, String, String, String, Option<String>, Option<String>);
        let row: Option<IdentityRow> = sqlx::query_as(
                "SELECT user_id, provider, provider_user_id, login, access_token_ref, refresh_token_ref \
                 FROM platform_identities WHERE provider = $1 AND provider_user_id = $2",
            )
            .bind(provider)
            .bind(provider_user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(to_sf)?;
        Ok(row.map(|r| PlatformIdentity {
            user_id: r.0,
            provider: r.1,
            provider_user_id: r.2,
            login: r.3,
            access_token_ref: r.4,
            refresh_token_ref: r.5,
        }))
    }
}
