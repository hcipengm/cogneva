use std::sync::Arc;

use async_trait::async_trait;

use cog_core::{
    EmbeddingProvider, FailurePattern, ImplementationExample, KnowledgeBackend, KnowledgeEntry,
    MemoryBackend, SFResult, SchemaEntry, SchemaKind, SourceRef, SummaryEntry, Task,
    TaskDecompositionPattern, TaskExecutionRecord, TaskResult, UnifiedSearchResult, WikiBackend,
};

/// Unified knowledge backend aggregating [`MemoryBackend`] (three-layer
/// execution memory) and [`WikiBackend`] (document knowledge base).
///
/// Lives in `cog-wiki` so that `cog-core` remains a pure contract crate and
/// the knowledge-aggregation concern stays close to the wiki implementation.
///
/// All three inner backends are optional — callers can wire only what they
/// need.  When a backend is `None` the corresponding queries return empty
/// results.
pub struct UnifiedKnowledgeBackend {
    memory: Option<Arc<dyn MemoryBackend>>,
    wiki: Option<Arc<dyn WikiBackend>>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
}

impl UnifiedKnowledgeBackend {
    pub fn new() -> Self {
        Self {
            memory: None,
            wiki: None,
            embedding: None,
        }
    }

    pub fn with_memory(mut self, memory: Arc<dyn MemoryBackend>) -> Self {
        self.memory = Some(memory);
        self
    }

    pub fn with_wiki(mut self, wiki: Arc<dyn WikiBackend>) -> Self {
        self.wiki = Some(wiki);
        self
    }

    pub fn with_embedding(mut self, embedding: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedding = Some(embedding);
        self
    }
}

impl Default for UnifiedKnowledgeBackend {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Namespace constants
// ---------------------------------------------------------------------------

const NS_DECOMPOSITION: &str = "task_decomposition";
const NS_IMPLEMENTATION: &str = "implementation";
const NS_FAILURE: &str = "failure_pattern";
const NS_EXECUTION: &str = "task_execution";
const NS_KNOWLEDGE: &str = "knowledge";

// ---------------------------------------------------------------------------
// KnowledgeBackend implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl KnowledgeBackend for UnifiedKnowledgeBackend {
    async fn retrieve_relevant(
        &self,
        _task: &Task,
        query: &str,
        top_k: usize,
    ) -> SFResult<Vec<KnowledgeEntry>> {
        let mut entries: Vec<KnowledgeEntry> = Vec::new();

        // --- Memory layer ---
        if let Some(ref memory) = self.memory {
            let embedding = if let Some(ref provider) = self.embedding {
                match provider.embed(vec![query.into()]).await {
                    Ok(mut vecs) if !vecs.is_empty() => Some(vecs.remove(0)),
                    Ok(_) => None,
                    Err(e) => {
                        tracing::warn!("embedding failed for knowledge query: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            match memory
                .search_all(NS_KNOWLEDGE, query, embedding.as_deref(), top_k, None)
                .await
            {
                Ok(results) => {
                    for r in results {
                        match r {
                            UnifiedSearchResult::Schema(s) => {
                                entries.push(KnowledgeEntry {
                                    id: s.entry.id.clone(),
                                    source: format!("memory:schema:{}", NS_KNOWLEDGE),
                                    title: s.entry.name.clone(),
                                    content: serde_json::to_string(&s.entry.properties)
                                        .unwrap_or_default(),
                                    relevance_score: s.score,
                                    metadata: Some(serde_json::json!({
                                        "kind": format!("{:?}", s.entry.kind),
                                        "namespace": s.entry.namespace,
                                    })),
                                });
                            }
                            UnifiedSearchResult::Summary(s) => {
                                entries.push(KnowledgeEntry {
                                    id: s.entry.id.clone(),
                                    source: format!("memory:summary:{}", NS_KNOWLEDGE),
                                    title: s.entry.namespace.clone(),
                                    content: s.entry.text.clone(),
                                    relevance_score: s.score,
                                    metadata: Some(serde_json::json!({
                                        "embedding_model": s.entry.embedding_model,
                                        "match_type": format!("{:?}", s.match_type),
                                    })),
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("memory search failed: {}", e);
                }
            }
        }

        // --- Wiki layer ---
        if let Some(ref wiki) = self.wiki {
            match wiki.search(query, top_k).await {
                Ok(results) => {
                    for r in results {
                        entries.push(KnowledgeEntry {
                            id: r.document.id.clone(),
                            source: "wiki".into(),
                            title: r.document.title.clone(),
                            content: r.document.content.clone(),
                            relevance_score: r.score,
                            metadata: Some(serde_json::json!({
                                "path": r.document.path,
                                "match_type": r.match_type,
                            })),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!("wiki search failed: {}", e);
                }
            }
        }

        // Sort by relevance descending.
        entries.sort_by(|a, b| b.relevance_score.partial_cmp(&a.relevance_score).unwrap());
        entries.truncate(top_k);

        Ok(entries)
    }

    async fn retrieve_similar_decompositions(
        &self,
        goal: &str,
        top_k: usize,
    ) -> SFResult<Vec<TaskDecompositionPattern>> {
        let Some(ref memory) = self.memory else {
            return Ok(Vec::new());
        };

        let results = memory.search_schema(NS_DECOMPOSITION, goal, top_k).await?;
        let patterns: Vec<TaskDecompositionPattern> = results
            .into_iter()
            .filter_map(|r| {
                serde_json::from_value::<TaskDecompositionPattern>(r.entry.properties.clone()).ok()
            })
            .collect();
        Ok(patterns)
    }

    async fn retrieve_similar_implementations(
        &self,
        task_type: &str,
        input_summary: &str,
        top_k: usize,
    ) -> SFResult<Vec<ImplementationExample>> {
        let Some(ref memory) = self.memory else {
            return Ok(Vec::new());
        };

        // Combine task_type and input_summary for broader matching.
        let query = format!("{} {}", task_type, input_summary);
        let results = memory
            .search_schema(NS_IMPLEMENTATION, &query, top_k)
            .await?;
        let examples: Vec<ImplementationExample> = results
            .into_iter()
            .filter_map(|r| {
                serde_json::from_value::<ImplementationExample>(r.entry.properties.clone()).ok()
            })
            .collect();
        Ok(examples)
    }

    async fn retrieve_failure_patterns(
        &self,
        task_type: &str,
        top_k: usize,
    ) -> SFResult<Vec<FailurePattern>> {
        let Some(ref memory) = self.memory else {
            return Ok(Vec::new());
        };

        let results = memory.search_schema(NS_FAILURE, task_type, top_k).await?;
        let patterns: Vec<FailurePattern> = results
            .into_iter()
            .filter_map(|r| {
                serde_json::from_value::<FailurePattern>(r.entry.properties.clone()).ok()
            })
            .collect();
        Ok(patterns)
    }

    async fn retrieve_task_history(&self, task_id: &str) -> SFResult<Vec<TaskExecutionRecord>> {
        let Some(ref memory) = self.memory else {
            return Ok(Vec::new());
        };

        let results = memory.search_schema(NS_EXECUTION, task_id, 100).await?;
        let records: Vec<TaskExecutionRecord> = results
            .into_iter()
            .filter_map(|r| {
                serde_json::from_value::<TaskExecutionRecord>(r.entry.properties.clone()).ok()
            })
            .collect();
        Ok(records)
    }

    async fn archive_execution(&self, task: &Task, result: &TaskResult) -> SFResult<()> {
        let Some(ref memory) = self.memory else {
            return Ok(());
        };

        let record_id = format!("exec:{}:{}", task.id, chrono::Utc::now().timestamp_millis());
        let result_summary = serde_json::to_string(&result.output)
            .map(|s| s.chars().take(500).collect::<String>())
            .unwrap_or_default();

        // --- Layer 1: Schema ---
        let schema_entry = SchemaEntry::new(
            &record_id,
            NS_EXECUTION,
            SchemaKind::Event,
            &task.id,
            &task.id,
            SourceRef::new(&record_id, "unified_knowledge_backend::archive_execution"),
        )
        .with_properties(serde_json::json!({
            "record_id": record_id,
            "task_id": task.id,
            "task_type": format!("{:?}", task.task_type),
            "status": if result.success { "success" } else { "failure" },
            "result_summary": result_summary,
            "executed_at": chrono::Utc::now(),
            "score": result.metadata.score,
        }));

        if let Err(e) = memory.store_schema(NS_EXECUTION, &schema_entry).await {
            tracing::warn!("failed to archive execution schema: {}", e);
        }

        // --- Layer 2: Summary ---
        let summary_text = format!(
            "Task {} (type: {:?}) executed with success={}. Output summary: {}",
            task.id, task.task_type, result.success, result_summary
        );

        let embedding = if let Some(ref provider) = self.embedding {
            match provider.embed(vec![summary_text.clone()]).await {
                Ok(mut vecs) if !vecs.is_empty() => vecs.remove(0),
                Ok(_) => Vec::new(),
                Err(e) => {
                    tracing::warn!("embedding failed for archive: {}", e);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        let summary_entry = SummaryEntry::new(
            &record_id,
            NS_EXECUTION,
            &summary_text,
            embedding,
            String::from("unified_knowledge_backend"),
            SourceRef::new(&record_id, "unified_knowledge_backend::archive_execution"),
        )
        .with_related_schema_ids(vec![schema_entry.id.clone()]);

        if let Err(e) = memory.store_summary(NS_EXECUTION, &summary_entry).await {
            tracing::warn!("failed to archive execution summary: {}", e);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{WikiDocument, WikiSearchResult};

    /// Wiki-only mock: returns three documents with distinct scores.
    struct MockWiki;

    #[async_trait]
    impl WikiBackend for MockWiki {
        async fn health_check(&self) -> bool {
            true
        }

        fn provider_name(&self) -> &str {
            "mock"
        }

        async fn ingest_document(&self, _relative_path: &str, _content: &str) -> SFResult<()> {
            Ok(())
        }

        async fn search(&self, _query: &str, _top_k: usize) -> SFResult<Vec<WikiSearchResult>> {
            Ok(vec![
                WikiSearchResult {
                    document: WikiDocument {
                        id: "doc-low".into(),
                        path: "low.md".into(),
                        title: "Low relevance".into(),
                        content: "low content".into(),
                        tags: None,
                        created_at: None,
                        updated_at: None,
                    },
                    score: 0.2,
                    match_type: None,
                    highlights: Vec::new(),
                },
                WikiSearchResult {
                    document: WikiDocument {
                        id: "doc-high".into(),
                        path: "high.md".into(),
                        title: "High relevance".into(),
                        content: "high content".into(),
                        tags: None,
                        created_at: None,
                        updated_at: None,
                    },
                    score: 0.9,
                    match_type: None,
                    highlights: Vec::new(),
                },
                WikiSearchResult {
                    document: WikiDocument {
                        id: "doc-mid".into(),
                        path: "mid.md".into(),
                        title: "Mid relevance".into(),
                        content: "mid content".into(),
                        tags: None,
                        created_at: None,
                        updated_at: None,
                    },
                    score: 0.5,
                    match_type: None,
                    highlights: Vec::new(),
                },
            ])
        }
    }

    #[tokio::test]
    async fn retrieve_relevant_sorts_by_score_and_truncates() {
        let backend = UnifiedKnowledgeBackend::new().with_wiki(Arc::new(MockWiki));
        let task = Task::new(
            "t1".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({}),
        );

        let entries = backend.retrieve_relevant(&task, "query", 2).await.unwrap();

        assert_eq!(entries.len(), 2, "top_k truncation should apply");
        assert_eq!(entries[0].id, "doc-high", "highest score first");
        assert_eq!(entries[1].id, "doc-mid");
        assert!(entries.iter().all(|e| e.source == "wiki"));
    }

    #[tokio::test]
    async fn empty_backends_return_empty_results() {
        let backend = UnifiedKnowledgeBackend::new();
        let task = Task::new(
            "t1".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({}),
        );

        assert!(backend
            .retrieve_relevant(&task, "q", 5)
            .await
            .unwrap()
            .is_empty());
        assert!(backend
            .retrieve_similar_decompositions("goal", 5)
            .await
            .unwrap()
            .is_empty());
        assert!(backend
            .retrieve_similar_implementations("t", "i", 5)
            .await
            .unwrap()
            .is_empty());
        assert!(backend
            .retrieve_failure_patterns("t", 5)
            .await
            .unwrap()
            .is_empty());
        assert!(backend
            .retrieve_task_history("t1")
            .await
            .unwrap()
            .is_empty());
        // archive without memory is a no-op, not an error.
        let result = cog_core::TaskResult {
            success: true,
            output: serde_json::json!({}),
            metadata: cog_core::TaskResultMetadata::new("test"),
        };
        assert!(backend.archive_execution(&task, &result).await.is_ok());
    }
}
