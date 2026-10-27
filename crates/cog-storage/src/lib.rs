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
pub(crate) mod consumer_lag;
pub mod datasets;
pub mod dlq;
pub use datasets::{DatasetCheck, DatasetOutcome, DatasetStore, DatasetVerification, DatasetWrite};
pub mod etcd;
pub mod event_filter;
pub mod inbox_orphan_readings;
pub mod inbox_retirement;
pub mod pools;
pub mod raw;
#[cfg(feature = "s3")]
pub mod s3;
pub mod tier;
pub mod wal;

pub mod checkpoint_cap;
#[cfg(feature = "livekit")]
pub mod media;
#[cfg(feature = "meilisearch")]
pub mod meilisearch;
#[cfg(feature = "mem")]
pub mod mem;
pub mod metrics_retirement;
pub mod metrics_sample_cap;
pub mod migrate;
pub mod observability_event_cap;
pub mod owner_lease;
pub mod partition_maintainer;
pub mod relation_bytes;
pub mod relation_rows;
pub mod schema_ddl;
pub mod task_event_retention;

// ─── Crate-level re-exports (backward-compatible with old `cog-db` usage) ───

#[cfg(feature = "mem")]
pub use mem::{
    MemoryMetricsBackend, MemoryObjectBackend, MemoryObservabilityGateway, MemoryRawLogIndexStore,
    MemorySnapshotStore, MemoryStateBackend, MemoryTraceStore, MemoryVectorBackend,
};

#[cfg(feature = "postgres")]
pub use postgres::{
    gauge_latest_sql, sha256_of, AgentMessage, AgentMessageStore, ChatTurn, ChatTurnRecord,
    ChatTurnRole, ChatTurnStore, DatasetCatalog, DatasetEntry, HookArchiveRow, NewAgentMessage,
    NewDataset, PostgresAgentMessageStore, PostgresAuditRecorder, PostgresBackupRunStore,
    PostgresChangeLedgerStore, PostgresChatTurnStore, PostgresDatasetCatalog,
    PostgresDecisionStatsStore, PostgresGovernanceStore, PostgresHookArchive,
    PostgresMetricsBackend, PostgresNotificationStore, PostgresObservabilityGateway,
    PostgresPrincipleLedgerStore, PostgresRawLogIndexStore, PostgresResourceEventStore,
    PostgresSnapshotStore, PostgresStateBackend, PostgresTasteIntentStore, PostgresUserStore,
    SendOutcome, EVENTS_PAGE_SQL, ROWS_KEPT_PER_PEER, ROWS_KEPT_PER_SESSION,
};

#[cfg(feature = "redis")]
pub use redis::{
    MemoryAgentRegistry, RedisAgentRegistry, RedisBackend, RedisKeyValueStore, RedisTraceStore,
};

pub use agent_id::generate_agent_id;
pub use etcd::registry::EtcdAgentRegistry;

#[cfg(feature = "redis")]
pub use pools::RedisClient;
pub use pools::{ConfigPool, ExplainPool, MessagesPool, UsersPool};

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
    detect_driver, discover_up_migrations, migrations_dir, Direction, Driver, Migration,
    MigrationStatus, Migrator,
};

pub use checkpoint_cap::{CheckpointCap, CheckpointSweepOutcome};
pub use dlq::MemoryDeadLetterQueue;
pub use metrics_retirement::{
    MetricsRetirement, RetirementOutcome, RetirementPass, SUPERSEDED_QUIET_SECS, SUPERSEDED_SHAPES,
};
pub use metrics_sample_cap::{delete_surplus_sql, SampleLogCap, SweepOutcome};
pub use observability_event_cap::{EventLogCap, EventSweepOutcome};
pub use owner_lease::{lease_holder, LeaseRow, PgOwnerLeaseBroker, LEASE_TABLE};
pub use partition_maintainer::PartitionMaintainer;
pub use task_event_retention::{TaskEventRetention, TaskEventRetentionOutcome};

pub mod plugin;

/// The label naming where a reading's value came from.
///
/// The storage plugin publishes readings per source, and the panels that count
/// one source select by this label. Aliased to the core declaration rather than
/// spelled again: the word is the same one, and two names for it in the
/// declaring crates is how a rename lands on one site and misses the reader.
pub const LABEL_SOURCE: &str = cog_core::build_cache::SOURCE_LABEL;

/// 读数带上「为什么是这个值」时用的标签键。
pub const LABEL_REASON: &str = "reason";

/// 读数按「哪一档」归属时用的标签键。
pub const LABEL_TIER: &str = "tier";

/// 读数按「哪个连接池」归属时用的标签键。
pub const LABEL_POOL: &str = "pool";

/// 读数按「哪张表」归属时用的标签键。
pub const LABEL_TABLE: &str = "table";

/// 读数按「哪个消费方」归属时用的标签键。
pub const LABEL_CONSUMER: &str = "consumer";
#[cfg(feature = "redis")]
pub use dlq::RedisDeadLetterQueue;

/// The day key, declared where every crate can read it.
///
/// Re-exported rather than declared here: the same form also names the
/// day-scoped goal ids, and those are built by crates that do not depend on
/// storage. Two declarations would agree until one of them moved.
pub use cog_core::DAY_KEY_FORMAT;

/// The stem a raw log file written on `day` is named with.
///
/// The writer and the reader go through this one function rather than each
/// formatting the date themselves, so a change of form cannot land on one side
/// only.
pub fn log_file_stem(day: chrono::NaiveDate) -> String {
    cog_core::day_key(day)
}
