use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::page::WikiPage;
use cog_core::SFResult;

/// Report produced after ingesting a source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestReport {
    pub source_path: String,
    pub summary_page_id: String,
    pub entity_pages_created: Vec<String>,
    pub entity_pages_updated: Vec<String>,
    pub contradictions_flagged: Vec<ContradictionReport>,
    pub index_updated: bool,
    pub log_entry_id: String,
}

/// A detected contradiction between a new source and existing wiki pages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContradictionReport {
    pub existing_page_id: String,
    pub existing_claim: String,
    pub new_source_path: String,
    pub new_claim: String,
    pub severity: ContradictionSeverity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContradictionSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Result of a wiki query.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResult {
    pub answer: String,
    pub sources_consulted: Vec<String>,
    pub archived_page_id: Option<String>,
}

/// Health-check report produced by lint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LintReport {
    pub checked_at: DateTime<Utc>,
    pub orphan_pages: Vec<String>,
    pub outdated_pages: Vec<OutdatedPage>,
    pub missing_cross_references: Vec<MissingCrossRef>,
    pub data_gaps: Vec<DataGap>,
}

/// A page whose content may be outdated.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutdatedPage {
    pub page_id: String,
    pub last_updated: DateTime<Utc>,
    pub reason: String,
}

/// A missing cross-reference between two pages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MissingCrossRef {
    pub from_page: String,
    pub to_page: String,
    pub suggested_link_text: String,
}

/// A detected gap in wiki coverage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataGap {
    pub topic: String,
    pub missing_entity: String,
    pub suggestion: String,
}

/// LLM-driven wiki maintainer.
/// Responsible for the Ingest / Query / Lint workflows.
#[async_trait]
pub trait WikiMaintainer: Send + Sync {
    /// Ingest a new raw source into the wiki.
    /// Workflow: read source → write summary page → update entity/concept pages
    /// → flag contradictions → update index.md → write log.md.
    async fn ingest_source(
        &self,
        source_path: &str,
        source_content: &str,
    ) -> SFResult<IngestReport>;

    /// Query the wiki.
    /// Workflow: search wiki → read pages → synthesize answer → optionally archive as Synthesis page.
    async fn query(&self, question: &str, archive: bool) -> SFResult<QueryResult>;

    /// Lint the wiki for health issues.
    async fn lint(&self) -> SFResult<LintReport>;

    /// Update cross-references between pages (bidirectional [[links]]).
    async fn update_cross_references(&self) -> SFResult<()>;

    /// Archive an external answer as a Synthesis page.
    async fn archive_answer(
        &self,
        question: &str,
        answer: &str,
        sources: &[String],
    ) -> SFResult<WikiPage>;
}
