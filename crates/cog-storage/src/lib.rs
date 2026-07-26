//! Cogneva unified storage layer.
//! Implements all storage backend traits defined in `cog-core::storage`.
//! Backend selection is controlled via Cargo features and runtime config.
//! ## Module map
//! | Module | Backends | Features |
//! |--------|----------|----------|
//! | `postgres` | PostgreSQL state / checkpoint / metrics / observability / raw log | `postgres` |
//! | `redis` | Redis state / registry / trace store (hot) | `redis` |
//! | `qdrant` | Qdrant vector backend | `qdrant` |
//! | `s3` | S3 / SeaweedFS object + trace store (cold) | `s3` |
//! | `mem` | In-memory implementations for testing | `mem` |
//! | `migrate` | SQL migration runner (`cog-migrate` binary) | always |

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "qdrant")]
pub mod qdrant;
#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "qdrant")]
pub use qdrant::QdrantVectorBackend;
pub mod agent_id;
pub mod audit_stream;
pub mod backends;
pub mod dlq;
pub mod etcd;
pub mod event_filter;
pub mod raw;
#[cfg(feature = "s3")]
pub mod s3;
pub mod tier;
pub mod wal;

#[cfg(feature = "livekit")]
pub mod media;
#[cfg(feature = "meilisearch")]
pub mod meilisearch;
#[cfg(feature = "mem")]
pub mod mem;
pub mod migrate;

// ─── Crate-level re-exports (backward-compatible with old `cog-db` usage) ───

#[cfg(feature = "mem")]
pub use mem::{
    MemoryMetricsBackend, MemoryObjectBackend, MemoryObservabilityGateway, MemoryRawLogIndexStore,
    MemorySnapshotStore, MemoryStateBackend, MemoryTraceStore, MemoryVectorBackend,
};

#[cfg(feature = "postgres")]
pub use postgres::{
    HookArchiveRow, PostgresAuditRecorder, PostgresHookArchive, PostgresMetricsBackend,
    PostgresObservabilityGateway, PostgresRawLogIndexStore, PostgresSnapshotStore,
    PostgresStateBackend,
};

#[cfg(feature = "redis")]
pub use redis::{MemoryAgentRegistry, RedisAgentRegistry, RedisBackend, RedisTraceStore};

pub use agent_id::generate_agent_id;
pub use etcd::registry::EtcdAgentRegistry;

pub use raw::{FileRawLogger, MemoryRawLogger, NoopRawLogger};

pub use backends::file_snapshot::FileSnapshotStore;
pub use backends::file_trace_store::FileTraceStore;
pub use backends::object_backends::{FileObjectBackend, S3ObjectBackend};
pub use backends::redis_state::RedisStateBackend;

#[cfg(feature = "s3")]
pub use s3::S3TraceStore;

#[cfg(feature = "livekit")]
pub use media::LiveKitMediaBackend;

pub use tier::migrator::{is_compressed, parse_log_date, tier_policy_from_config, TierMigrator};

pub use migrate::{
    detect_driver, discover_up_migrations, Direction, Driver, Migration, MigrationStatus, Migrator,
};

pub use dlq::MemoryDeadLetterQueue;

pub mod plugin;
#[cfg(feature = "redis")]
pub use dlq::RedisDeadLetterQueue;
