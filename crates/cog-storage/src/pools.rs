//! Opaque connection-pool handles published by [`crate::plugin::StoragePlugin`].
//!
//! These live in `cog-storage` rather than `cog-core` because they name
//! concrete driver types (`sqlx::PgPool`, `redis::Client`). Keeping them here
//! preserves the contract/implementation split: consumers that need a pool
//! already depend on the driver, and `cog-core` stays free of storage drivers.

use sqlx::PgPool;

/// PostgreSQL pool holding `users` / `user_auth_methods`.
#[derive(Debug, Clone)]
pub struct UsersPool(pub Option<PgPool>);

/// PostgreSQL pool holding chat `messages`.
#[derive(Debug, Clone)]
pub struct MessagesPool(pub Option<PgPool>);

/// PostgreSQL pool holding runtime configuration rows.
#[derive(Debug, Clone)]
pub struct ConfigPool(pub Option<PgPool>);

/// PostgreSQL pool holding explainability records.
#[derive(Debug, Clone)]
pub struct ExplainPool(pub Option<PgPool>);

/// Redis client wrapper so it can be published as a service in
/// `cog_core::contract::system_plugin::PluginContext`.
#[cfg(feature = "redis")]
#[derive(Debug, Clone)]
pub struct RedisClient(pub redis::Client);
