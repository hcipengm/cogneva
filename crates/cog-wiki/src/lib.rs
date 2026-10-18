//! principle: feedback_rechecking_an_old_finding_needs_both_did_it_happen_again_and_is_the_condition_still_here
//!
//! 复测一篇旧文档要分两问：它记的那件事后来又发生过吗（事件面）、它记的那个条件今天还在吗（状态面）。
//!
//! The gate at the bottom of this file answers both from the wiki's own
//! stored readings: the event face from the page's own update record
//! (`last_updated` / `update_reason`), the condition face from that record
//! compared against today -- never from the static fact that the old
//! document is still on disk.

pub mod bounds;
pub mod indexer;
pub mod llm_maintainer;
pub mod maintainer;
pub mod manager;
pub mod meilisearch;
pub mod page;
pub mod search;
pub mod unified_knowledge_backend;
pub mod vector_index;

pub use bounds::WIKI_PAGE_PROMPT_CHARS;
pub use indexer::{WikiIndexEntry, WikiIndexer};
pub use llm_maintainer::LlmWikiMaintainer;
pub use maintainer::{
    ContradictionReport, ContradictionSeverity, DataGap, IngestReport, LintReport, MissingCrossRef,
    OutdatedPage, QueryResult, WikiMaintainer,
};
pub use manager::WikiManager;
pub use page::{WikiPage, WikiPageType};
pub use search::{ThreeTierSearch, WikiSearchResult};
pub use unified_knowledge_backend::UnifiedKnowledgeBackend;
pub use vector_index::{IndexOutcome, WikiVectorIndex, DEFAULT_WIKI_COLLECTION};

pub mod plugin;

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use crate::page::{WikiPage, WikiPageType};

    /// 事件面：它记的那件事后来又发生过吗——由页面自己的更新记录回答。
    #[test]
    fn the_event_face_is_answered_from_the_pages_own_record() {
        let page = WikiPage::new("entity-alpha", WikiPageType::Entity, "Alpha")
            .with_update_reason("created from source: old-report.md");
        let recorded_at = page.last_updated;
        assert!(
            page.update_reason.contains("old-report.md"),
            "事件面读不出「记的是哪件事」：update_reason 没有记下触发它的来源"
        );

        // 那件事后来又发生了一次：同一来源的新一轮 ingest 触到了这页。
        let page = page.touch("updated by source: old-report.md".to_string());
        assert!(
            page.last_updated >= recorded_at,
            "事件面读不出「后来又发生过」：复测后 last_updated 没有前移"
        );
    }

    /// 状态面：它记的那个条件今天还在吗——由 last_updated 与今天的差回答，
    /// 而不是由「文档还在」这个静态事实回答。阈值与维护者的过时判定同为 30 天。
    #[test]
    fn the_condition_face_is_answered_against_today() {
        let mut page = WikiPage::new("entity-beta", WikiPageType::Entity, "Beta");
        page.last_updated = Utc::now() - Duration::days(45);
        assert!(
            Utc::now() - page.last_updated > Duration::days(30),
            "45 天未复测的条件应读作「今天已不在」"
        );

        let page = page.touch("rechecked today".to_string());
        assert!(
            Utc::now() - page.last_updated <= Duration::days(30),
            "当天复测过的条件不应再读作过时"
        );
    }
}
