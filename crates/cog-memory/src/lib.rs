//! Cogneva 永久记忆子系统（Three-Layer Permanent Memory）
//! 实现 Agent 永久记忆的 Raw → Schema → Summary 三层架构。
//! ## 三层架构
//! - **Layer 0 — Raw**: 原始数据记录，immutable append-only，存储于对象存储/本地磁盘
//! - **Layer 1 — Schema**: 结构化事实（实体、关系、事件），存储于 PostgreSQL/TDSQL-PG
//! - **Layer 2 — Summary**: 语义摘要 + embedding 向量，存储于向量库（Qdrant）+ PostgreSQL
//! ## 核心组件
//! - [`cog_core::MemoryBackend`] trait: 统一后端接口
//! - [`cog_core::MemoryExtractor`] trait: 从 Raw 提取 Schema 和 Summary
//! - [`MemoryIngestor`]: 后台服务，监听 AgentEvent 自动触发摄取
//! - **CompositeMemoryBackend** — 组合后端
//! - **MetricsInstrumentedMemoryBackend** — 指标装饰器
//! - **记忆维护循环**（[`maintenance`]）— 周期衰减低价值记忆的驱动方

pub mod backend;
pub mod causal;
pub mod composite;
pub mod config;
pub mod consolidator;
pub mod embedding_provider;
pub mod entry_store;
pub mod extractor;
pub mod ingestor;
pub mod isolation;
pub mod maintenance;
pub mod metrics_instrumented;
pub mod noop_backends;
pub mod observable;
pub mod postgres_entry_store;
pub mod postgres_schema;
pub mod reranker;
pub mod schema_backend;
pub mod types;
pub mod vector_summary_backend;
pub use backend::MemoryMemoryBackend;
/// The reranker contract itself lives in `cog-core`, because its consumer
/// (`cog-wiki`) must not depend on this crate; only the local implementation is
/// here. Re-exported so a caller that already depends on `cog-memory` does not
/// have to reach for both crates to name one concept.
pub use cog_core::{RerankResult, RerankerProvider};
pub use composite::CompositeMemoryBackend;
pub use consolidator::{ConsolidationStrategy, MemoryConsolidator};
pub use embedding_provider::FastEmbedProvider;
pub use entry_store::{MemoryEntryStore, SummaryEntryStore};
pub use extractor::{IngestionPipeline, LlmMemoryExtractor, RuleBasedExtractor};
pub use ingestor::{MemoryIngestor, MemoryIngestorConfig};
pub use isolation::{BenchmarkIsolation, BenchmarkRefusal};
pub use metrics_instrumented::MetricsInstrumentedMemoryBackend;
pub use noop_backends::{NoopMetricsBackend, NoopVectorBackend};
pub use observable::MemoryObservable;
pub use postgres_entry_store::{PostgresEntryStore, SUMMARY_ENTRIES_DDL};
pub use postgres_schema::{PostgresSchemaBackend, SCHEMA_ENTRIES_DDL};
pub use reranker::FastEmbedRerankerProvider;
pub use schema_backend::MemorySchemaBackend;
pub use vector_summary_backend::{VectorSummaryBackend, DEFAULT_SUMMARY_COLLECTION};
pub mod plugin;

pub use config::{IngestConfig, MaintenanceConfig, MemoryConfig};
