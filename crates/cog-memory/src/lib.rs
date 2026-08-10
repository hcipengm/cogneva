//! Cogneva 永久记忆子系统（Three-Layer Permanent Memory）
//! 实现 Agent 永久记忆的 Raw → Schema → Summary 三层架构。
//! ## 三层架构
//! - **Layer 0 — Raw**: 原始数据记录，immutable append-only，存储于对象存储/本地磁盘
//! - **Layer 1 — Schema**: 结构化事实（实体、关系、事件），存储于 PostgreSQL/TDSQL-PG
//! - **Layer 2 — Summary**: 语义摘要 + embedding 向量，存储于 LanceDB/VectorDB
//! ## 核心组件
//! - [`MemoryBackend`] trait: 统一后端接口
//! - [`MemoryExtractor`] trait: 从 Raw 提取 Schema 和 Summary
//! - [`MemoryIngestor`]: 后台服务，监听 AgentEvent 自动触发摄取
//! - **CompositeMemoryBackend** — 组合后端
//! - **MetricsInstrumentedMemoryBackend** — 指标装饰器

pub mod backend;
pub mod causal;
pub mod composite;
pub mod config;
pub mod consolidator;
pub mod embedding_provider;
pub mod entry_store;
pub mod extractor;
pub mod ingestor;
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
pub use composite::CompositeMemoryBackend;
pub use consolidator::{ConsolidationStrategy, MemoryConsolidator};
pub use embedding_provider::FastEmbedProvider;
pub use entry_store::{MemoryEntryStore, SummaryEntryStore};
pub use extractor::{IngestionPipeline, LlmMemoryExtractor, RuleBasedExtractor};
pub use ingestor::{MemoryIngestor, MemoryIngestorConfig};
pub use metrics_instrumented::MetricsInstrumentedMemoryBackend;
pub use noop_backends::{NoopMetricsBackend, NoopVectorBackend};
pub use observable::MemoryObservable;
pub use postgres_entry_store::{PostgresEntryStore, SUMMARY_ENTRIES_DDL};
pub use postgres_schema::{PostgresSchemaBackend, SCHEMA_ENTRIES_DDL};
pub use reranker::{FastEmbedRerankerProvider, RerankResult, RerankerProvider};
pub use schema_backend::MemorySchemaBackend;
pub use vector_summary_backend::{VectorSummaryBackend, DEFAULT_SUMMARY_COLLECTION};
pub mod plugin;

pub use config::MemoryConfig;
