use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool, Postgres, Row};
use uuid::Uuid;

use crate::error::AuthResult;
use cog_core::{AuthType, User, UserStatus, UserType};

/// External authentication method bound to a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAuthMethod {
    pub id: Uuid,
    pub user_id: Uuid,
    pub auth_type: AuthType,
    pub auth_id: String,
    pub extra: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// Repository for user persistence.
/// Generic over `DB` so it works with both MySQL and PostgreSQL pools.
pub struct UserRepository<DB: sqlx::Database> {
    pool: Pool<DB>,
}

impl UserRepository<MySql> {
    pub fn new_mysql(pool: Pool<MySql>) -> Self {
        Self { pool }
    }

    pub async fn find_by_id(&self, id: Uuid) -> AuthResult<Option<User>> {
        let row = sqlx::query(
            r#"
            SELECT id, phone, email, username, display_name, avatar_url,
                   status, user_type, created_at, updated_at
            FROM users WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_mysql_row_to_user).transpose()?)
    }

    pub async fn find_by_phone(&self, phone: &str) -> AuthResult<Option<User>> {
        let row = sqlx::query(
            r#"
            SELECT id, phone, email, username, display_name, avatar_url,
                   status, user_type, created_at, updated_at
            FROM users WHERE phone = ?
            "#,
        )
        .bind(phone)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_mysql_row_to_user).transpose()?)
    }

    pub async fn find_by_auth_method(
        &self,
        auth_type: AuthType,
        auth_id: &str,
    ) -> AuthResult<Option<User>> {
        let row = sqlx::query(
            r#"
            SELECT u.id, u.phone, u.email, u.username, u.display_name, u.avatar_url,
                   u.status, u.user_type, u.created_at, u.updated_at
            FROM users u
            JOIN user_auth_methods m ON u.id = m.user_id
            WHERE m.auth_type = ? AND m.auth_id = ?
            "#,
        )
        .bind(auth_type)
        .bind(auth_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_mysql_row_to_user).transpose()?)
    }

    pub async fn create(
        &self,
        phone: Option<&str>,
        email: Option<&str>,
        username: &str,
        display_name: Option<&str>,
        user_type: UserType,
    ) -> AuthResult<User> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO users (id, phone, email, username, display_name, status, user_type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?)
            "#,
        )
        .bind(id)
        .bind(phone)
        .bind(email)
        .bind(username)
        .bind(display_name)
        .bind(user_type)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(User {
            id,
            phone: phone.map(String::from),
            email: email.map(String::from),
            username: username.to_string(),
            display_name: display_name.map(String::from),
            avatar_url: None,
            status: UserStatus::Active,
            user_type,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn update_status(&self, id: Uuid, status: UserStatus) -> AuthResult<()> {
        let now = Utc::now();
        sqlx::query("UPDATE users SET status = ?, updated_at = ? WHERE id = ?")
            .bind(status)
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

impl UserRepository<Postgres> {
    pub fn new_postgres(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn find_by_id(&self, id: Uuid) -> AuthResult<Option<User>> {
        let row = sqlx::query(
            r#"
            SELECT id, phone, email, username, display_name, avatar_url,
                   status, user_type, created_at, updated_at
            FROM users WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_pg_row_to_user).transpose()?)
    }

    pub async fn find_by_phone(&self, phone: &str) -> AuthResult<Option<User>> {
        let row = sqlx::query(
            r#"
            SELECT id, phone, email, username, display_name, avatar_url,
                   status, user_type, created_at, updated_at
            FROM users WHERE phone = $1
            "#,
        )
        .bind(phone)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_pg_row_to_user).transpose()?)
    }

    pub async fn find_by_auth_method(
        &self,
        auth_type: AuthType,
        auth_id: &str,
    ) -> AuthResult<Option<User>> {
        let row = sqlx::query(
            r#"
            SELECT u.id, u.phone, u.email, u.username, u.display_name, u.avatar_url,
                   u.status, u.user_type, u.created_at, u.updated_at
            FROM users u
            JOIN user_auth_methods m ON u.id = m.user_id
            WHERE m.auth_type = $1 AND m.auth_id = $2
            "#,
        )
        .bind(auth_type)
        .bind(auth_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_pg_row_to_user).transpose()?)
    }

    pub async fn create(
        &self,
        phone: Option<&str>,
        email: Option<&str>,
        username: &str,
        display_name: Option<&str>,
        user_type: UserType,
    ) -> AuthResult<User> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO users (id, phone, email, username, display_name, status, user_type, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, 'active', $6, $7, $8)
            "#,
        )
        .bind(id)
        .bind(phone)
        .bind(email)
        .bind(username)
        .bind(display_name)
        .bind(user_type)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(User {
            id,
            phone: phone.map(String::from),
            email: email.map(String::from),
            username: username.to_string(),
            display_name: display_name.map(String::from),
            avatar_url: None,
            status: UserStatus::Active,
            user_type,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn update_status(&self, id: Uuid, status: UserStatus) -> AuthResult<()> {
        let now = Utc::now();
        sqlx::query("UPDATE users SET status = $1, updated_at = $2 WHERE id = $3")
            .bind(status)
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Row mapping helpers
// ---------------------------------------------------------------------------

fn map_mysql_row_to_user(row: sqlx::mysql::MySqlRow) -> Result<User, sqlx::Error> {
    Ok(User {
        id: row.try_get("id")?,
        phone: row.try_get("phone")?,
        email: row.try_get("email")?,
        username: row.try_get("username")?,
        display_name: row.try_get("display_name")?,
        avatar_url: row.try_get("avatar_url")?,
        status: row.try_get("status")?,
        user_type: row.try_get("user_type")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_pg_row_to_user(row: sqlx::postgres::PgRow) -> Result<User, sqlx::Error> {
    Ok(User {
        id: row.try_get("id")?,
        phone: row.try_get("phone")?,
        email: row.try_get("email")?,
        username: row.try_get("username")?,
        display_name: row.try_get("display_name")?,
        avatar_url: row.try_get("avatar_url")?,
        status: row.try_get("status")?,
        user_type: row.try_get("user_type")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}
