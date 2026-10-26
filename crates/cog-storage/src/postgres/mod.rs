/// Resolve the `<{prefix}:<word>}` slots a query template writes.
///
/// A query that spells a word some enum's `as_str` already decides is a second
/// copy of that word, and the two agree only until one of them is renamed. The
/// failure is quiet in the way a store cannot afford: the predicate keeps
/// matching the old spelling, and a query that matches no rows answers exactly
/// like a table with nothing in it -- so "nothing is outstanding", "nothing was
/// retired" and "no work is bound to this face" all read as ordinary states.
///
/// The slot is named after the value it resolves (`{state:landed}`), so the
/// template says what will be written and the caller only has to offer the
/// enum's own words. One function, so a store that fixes a bug here does not
/// leave the other copy broken.
pub(crate) fn fill_slots(template: &str, slots: &[(&str, &str)]) -> String {
    let mut sql = template.to_string();
    for (prefix, word) in slots {
        sql = sql.replace(&format!("{{{prefix}:{word}}}"), word);
    }
    sql
}

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
pub use metrics_backend::{PostgresMetricsBackend, GAUGE_LATEST_SQL};
pub use notification_store::PostgresNotificationStore;
pub use observability_gateway::PostgresObservabilityGateway;
pub use principle_ledger_store::PostgresPrincipleLedgerStore;
pub use raw_log_index_store::PostgresRawLogIndexStore;
pub use resource_event_store::PostgresResourceEventStore;
pub use snapshot_store::PostgresSnapshotStore;
pub use state_backend::{PostgresStateBackend, EVENTS_PAGE_SQL};
pub use taste_intent_store::PostgresTasteIntentStore;
pub use user_store::PostgresUserStore;
