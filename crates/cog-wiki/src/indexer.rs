use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{ObjectBackend, SFResult};

/// A single entry in a generated `index.md`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WikiIndexEntry {
    pub title: String,
    pub path: String,
    pub summary: String,
    pub tags: Vec<String>,
}

/// BM25 index for keyword-based document ranking.
#[derive(Debug, Clone, Default)]
pub struct Bm25Index {
    /// term -> (doc_id, term_frequency)
    postings: HashMap<String, Vec<(String, f32)>>,
    /// doc_id -> total_terms
    doc_lengths: HashMap<String, usize>,
    /// doc_id -> document path
    doc_paths: HashMap<String, String>,
    avg_doc_length: f32,
    num_docs: usize,
}

impl Bm25Index {
    const K1: f32 = 1.5;
    const B: f32 = 0.75;

    pub fn new() -> Self {
        Self::default()
    }

    /// Tokenize text into lowercase terms.
    fn tokenize(text: &str) -> Vec<String> {
        let re = Regex::new(r"[a-zA-Z0-9一-鿿]+").unwrap();
        re.find_iter(text)
            .map(|m| m.as_str().to_lowercase())
            .collect()
    }

    /// Add a document to the BM25 index.
    pub fn add_document(&mut self, doc_id: &str, path: &str, content: &str) {
        let terms = Self::tokenize(content);
        let mut tf: HashMap<String, usize> = HashMap::new();
        for term in &terms {
            *tf.entry(term.clone()).or_insert(0) += 1;
        }

        for (term, count) in &tf {
            self.postings
                .entry(term.clone())
                .or_default()
                .push((doc_id.into(), *count as f32));
        }

        self.doc_lengths.insert(doc_id.into(), terms.len());
        self.doc_paths.insert(doc_id.into(), path.into());
        self.num_docs += 1;

        let total_len: usize = self.doc_lengths.values().sum();
        self.avg_doc_length = total_len as f32 / self.num_docs as f32;
    }

    /// Search the BM25 index for the given query.
    pub fn search(&self, query: &str, top_k: usize) -> Vec<(String, f32)> {
        let query_terms = Self::tokenize(query);
        let mut scores: HashMap<String, f32> = HashMap::new();

        for term in &query_terms {
            if let Some(postings) = self.postings.get(term) {
                let idf = self.idf(term);
                for (doc_id, tf) in postings {
                    let doc_len = self.doc_lengths.get(doc_id).copied().unwrap_or(0) as f32;
                    let tf_norm = tf * (Self::K1 + 1.0)
                        / (tf
                            + Self::K1
                                * (1.0 - Self::B
                                    + Self::B * doc_len / self.avg_doc_length.max(1.0)));
                    *scores.entry(doc_id.clone()).or_insert(0.0) += idf * tf_norm;
                }
            }
        }

        let mut results: Vec<(String, f32)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(top_k);
        results
    }

    fn idf(&self, term: &str) -> f32 {
        let postings = self.postings.get(term).map(|v| v.len()).unwrap_or(0);
        let n = self.num_docs as f32;
        ((n - postings as f32 + 0.5) / (postings as f32 + 0.5) + 1.0).ln()
    }

    /// Get the file path for a document ID.
    pub fn doc_path(&self, doc_id: &str) -> Option<&str> {
        self.doc_paths.get(doc_id).map(|s| s.as_str())
    }
}

/// Responsible for generating `index.md` files and maintaining keyword indexes.
pub struct WikiIndexer {
    object_backend: Arc<dyn ObjectBackend>,
    key_prefix: String,
    bm25: Bm25Index,
}

impl WikiIndexer {
    pub fn new(object_backend: Arc<dyn ObjectBackend>) -> Self {
        Self::with_prefix(object_backend, "wiki")
    }

    pub fn with_prefix(object_backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        Self {
            object_backend,
            key_prefix: prefix.into(),
            bm25: Bm25Index::new(),
        }
    }

    fn list_prefix(&self) -> Option<String> {
        if self.key_prefix.is_empty() {
            None
        } else {
            Some(format!("{}/", self.key_prefix))
        }
    }

    /// Scan the wiki directory tree and generate `index.md` for every directory.
    pub async fn generate_indices(&self) -> SFResult<()> {
        let keys = self
            .object_backend
            .list(self.list_prefix().as_deref())
            .await?;
        let md_keys: Vec<String> = keys
            .into_iter()
            .filter(|k| k.ends_with(".md") && !k.ends_with("index.md"))
            .collect();

        // Collect all directory prefixes (including intermediate ones)
        let mut dirs = std::collections::HashSet::new();
        dirs.insert("".to_string());
        for key in &md_keys {
            let mut remaining = key.as_str();
            while let Some(pos) = remaining.rfind('/') {
                dirs.insert(remaining[..pos + 1].to_string());
                remaining = &remaining[..pos];
            }
        }

        for dir in dirs {
            let mut entries = Vec::new();

            // Files in this directory
            for key in &md_keys {
                if key.starts_with(&dir) {
                    let remainder = &key[dir.len()..];
                    if !remainder.contains('/') {
                        let title = Self::extract_title(&*self.object_backend, key).await?;
                        let summary = Self::extract_summary(&*self.object_backend, key).await?;
                        let tags = Self::extract_tags(&*self.object_backend, key).await?;
                        entries.push(WikiIndexEntry {
                            title: title.unwrap_or_else(|| {
                                key.rfind('/')
                                    .map(|i| &key[i + 1..])
                                    .unwrap_or(key)
                                    .to_string()
                            }),
                            path: key.clone(),
                            summary,
                            tags,
                        });
                    }
                }
            }

            // Subdirectories
            let mut subdirs = std::collections::HashSet::new();
            for key in &md_keys {
                if key.starts_with(&dir) {
                    let remainder = &key[dir.len()..];
                    if let Some(pos) = remainder.find('/') {
                        subdirs.insert(remainder[..pos + 1].to_string());
                    }
                }
            }

            for subdir in subdirs {
                let sub_index_key = format!("{}{}index.md", dir, subdir);
                let summary = if self.object_backend.exists(&sub_index_key).await? {
                    Self::extract_summary(&*self.object_backend, &sub_index_key).await?
                } else {
                    format!("{}/", subdir.trim_end_matches('/'))
                };
                entries.push(WikiIndexEntry {
                    title: subdir.trim_end_matches('/').to_string(),
                    path: format!("{}{}", dir, subdir),
                    summary,
                    tags: vec![],
                });
            }

            if !entries.is_empty() {
                let index_key = if dir.is_empty() {
                    "index.md".to_string()
                } else {
                    format!("{}index.md", dir)
                };
                let content = self.render_index_md(&entries)?;
                self.object_backend
                    .put(&index_key, content.as_bytes())
                    .await?;
            }
        }

        Ok(())
    }

    async fn extract_title(
        object_backend: &dyn ObjectBackend,
        key: &str,
    ) -> SFResult<Option<String>> {
        match object_backend.get(key).await? {
            Some(data) => {
                let content = String::from_utf8_lossy(&data);
                for line in content.lines() {
                    let trimmed = line.trim();
                    if let Some(stripped) = trimmed.strip_prefix("# ") {
                        return Ok(Some(stripped.trim().to_string()));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn extract_summary(object_backend: &dyn ObjectBackend, key: &str) -> SFResult<String> {
        match object_backend.get(key).await? {
            Some(data) => {
                let content = String::from_utf8_lossy(&data);
                for line in content.lines() {
                    let trimmed = line.trim();
                    if !trimmed.is_empty()
                        && !trimmed.starts_with('#')
                        && !trimmed.starts_with("---")
                    {
                        return Ok(trimmed.chars().take(120).collect());
                    }
                }
                Ok("".into())
            }
            None => Ok("".into()),
        }
    }

    /// Extract YAML front-matter tags from a markdown document.
    pub async fn extract_tags(
        object_backend: &dyn ObjectBackend,
        key: &str,
    ) -> SFResult<Vec<String>> {
        match object_backend.get(key).await? {
            Some(data) => {
                let content = String::from_utf8_lossy(&data);
                let mut tags = Vec::new();
                if let Some(stripped) = content.strip_prefix("---") {
                    if let Some(end) = stripped.find("---") {
                        let fm = &stripped[..end];
                        let mut in_tags = false;
                        for line in fm.lines() {
                            let trimmed = line.trim();
                            if let Some(rest) = trimmed.strip_prefix("tags:") {
                                let rest = rest.trim();
                                if rest.starts_with('[') && rest.ends_with(']') {
                                    let inner = &rest[1..rest.len() - 1];
                                    for t in inner.split(',') {
                                        let tag = t.trim().trim_matches('"').trim_matches('\'');
                                        if !tag.is_empty() {
                                            tags.push(tag.to_string());
                                        }
                                    }
                                    in_tags = false;
                                }
                            } else if in_tags && trimmed.starts_with("- ") {
                                let tag = trimmed[2..].trim().trim_matches('"').trim_matches('\'');
                                if !tag.is_empty() {
                                    tags.push(tag.to_string());
                                }
                            }
                        }
                    }
                }
                Ok(tags)
            }
            None => Ok(Vec::new()),
        }
    }

    fn render_index_md(&self, entries: &[WikiIndexEntry]) -> SFResult<String> {
        let mut lines = vec!["# Index\n".to_string()];
        for entry in entries {
            lines.push(format!("## [{}]({})", entry.title, entry.path));
            if !entry.summary.is_empty() {
                lines.push(entry.summary.clone());
            }
            if !entry.tags.is_empty() {
                lines.push(format!("Tags: {}\n", entry.tags.join(", ")));
            } else {
                lines.push("".to_string());
            }
        }
        Ok(lines.join("\n"))
    }

    /// Build a BM25 keyword index from all markdown files in the wiki.
    pub async fn build_bm25_index(&mut self) -> SFResult<()> {
        self.bm25 = Bm25Index::new();
        let keys = self
            .object_backend
            .list(self.list_prefix().as_deref())
            .await?;
        for key in keys {
            if key.ends_with(".md") && !key.ends_with("index.md") {
                if let Some(data) = self.object_backend.get(&key).await? {
                    let content = String::from_utf8_lossy(&data);
                    let doc_id = key.replace('/', "-");
                    self.bm25.add_document(&doc_id, &key, &content);
                }
            }
        }
        Ok(())
    }

    /// Search the BM25 index for documents matching the query.
    pub fn search_bm25(&self, query: &str, top_k: usize) -> Vec<(String, f32)> {
        self.bm25.search(query, top_k)
    }

    /// Get the relative path for a BM25 document ID.
    pub fn doc_path(&self, doc_id: &str) -> Option<&str> {
        self.bm25.doc_path(doc_id)
    }
}
