use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Types of wiki pages in the LLM_WIKI architecture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WikiPageType {
    /// Summary of a raw source.
    Summary,
    /// Entity page (person, company, concept).
    Entity,
    /// Concept page.
    Concept,
    /// Comparison page (A vs B).
    Comparison,
    /// Synthesis /综合 conclusion.
    Synthesis,
    /// Operation log (append-only).
    Log,
    /// Directory index.
    Index,
    /// Raw source (immutable).
    Raw,
}

/// A rich wiki page model with backlinks, source refs, and provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WikiPage {
    pub id: String,
    pub page_type: WikiPageType,
    pub title: String,
    pub content: String,
    pub tags: Vec<String>,
    /// Source references (which raw sources this page is derived from).
    pub source_refs: Vec<String>,
    /// Pages that link to this page.
    pub backlinks: Vec<String>,
    /// Pages this page links to.
    pub outgoing_links: Vec<String>,
    pub last_updated: DateTime<Utc>,
    /// Reason for the last update (e.g. "new source: article.md").
    pub update_reason: String,
}

impl WikiPage {
    pub fn new(id: impl Into<String>, page_type: WikiPageType, title: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            page_type,
            title: title.into(),
            content: String::new(),
            tags: Vec::new(),
            source_refs: Vec::new(),
            backlinks: Vec::new(),
            outgoing_links: Vec::new(),
            last_updated: now,
            update_reason: "created".into(),
        }
    }

    pub fn with_content(mut self, content: impl Into<String>) -> Self {
        self.content = content.into();
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_source_refs(mut self, refs: Vec<String>) -> Self {
        self.source_refs = refs;
        self
    }

    pub fn with_backlinks(mut self, links: Vec<String>) -> Self {
        self.backlinks = links;
        self
    }

    pub fn with_outgoing_links(mut self, links: Vec<String>) -> Self {
        self.outgoing_links = links;
        self
    }

    pub fn with_update_reason(mut self, reason: impl Into<String>) -> Self {
        self.update_reason = reason.into();
        self
    }

    pub fn touch(mut self, reason: impl Into<String>) -> Self {
        self.last_updated = Utc::now();
        self.update_reason = reason.into();
        self
    }
}
