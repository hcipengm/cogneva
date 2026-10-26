/// Resolve the `<{prefix}:<word>}` slots a query template writes.
///
/// See [`cog_core::fill_slots`] for why a query may not spell a closed set's
/// word itself. Re-exported rather than reimplemented: the alert store fills
/// the same kind of slot from its own enum, and two copies of this rule would
/// be the very thing it exists to prevent.
pub(crate) use cog_core::fill_slots;

pub mod agent_message_store;
pub mod audit_recorder;
pub mod backup_run_store;
pub mod change_ledger_store;
pub mod chat_turn_store;
pub mod dataset_catalog;
pub mod decision_stats_store;
pub mod governance_store;
pub mod hook_archive;
pub mod metrics_backend;
pub mod notification_store;
pub mod observability_gateway;
pub mod principle_ledger_store;
pub mod raw_log_index_store;
pub mod resource_event_store;
pub mod snapshot_store;
pub mod state_backend;
pub mod taste_intent_store;
pub mod user_store;

pub use agent_message_store::{
    AgentMessage, AgentMessageStore, NewAgentMessage, PostgresAgentMessageStore, SendOutcome,
    ROWS_KEPT_PER_PEER,
};
pub use audit_recorder::PostgresAuditRecorder;
pub use backup_run_store::PostgresBackupRunStore;
pub use change_ledger_store::PostgresChangeLedgerStore;
pub use chat_turn_store::{
    ChatTurn, ChatTurnRecord, ChatTurnRole, ChatTurnStore, PostgresChatTurnStore,
    ROWS_KEPT_PER_SESSION,
};
pub use dataset_catalog::{
    sha256_of, DatasetCatalog, DatasetEntry, NewDataset, PostgresDatasetCatalog,
};
pub use decision_stats_store::PostgresDecisionStatsStore;
pub use governance_store::PostgresGovernanceStore;
pub use hook_archive::{HookArchiveRow, PostgresHookArchive};
pub use metrics_backend::{gauge_latest_sql, PostgresMetricsBackend};
pub use notification_store::PostgresNotificationStore;
pub use observability_gateway::PostgresObservabilityGateway;
pub use principle_ledger_store::PostgresPrincipleLedgerStore;
pub use raw_log_index_store::PostgresRawLogIndexStore;
pub use resource_event_store::PostgresResourceEventStore;
pub use snapshot_store::PostgresSnapshotStore;
pub use state_backend::{PostgresStateBackend, EVENTS_PAGE_SQL};
pub use taste_intent_store::PostgresTasteIntentStore;
pub use user_store::PostgresUserStore;
