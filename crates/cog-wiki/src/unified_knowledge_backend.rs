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

/// How much of a record's free text is kept, in characters.
///
/// These summaries end up in a prompt, so the cap is on the producer: a task's
/// input is a serialized object of unbounded size, and clipping it at the
/// write side keeps the ceiling on what the namespace holds rather than on
/// what a reader is allowed to see.
const SUMMARY_MAX_CHARS: usize = 500;

/// How many rows a retrieval asks the store for before it ranks them.
///
/// Wider than any answer it can give, because ranking happens after the
/// store's own limit: asking for exactly `top_k` lets near-matching keys fill
/// the window and push out the row the caller wanted. Bounded rather than
/// open, so no single retrieval turns into a scan of the namespace.
fn scan_window(top_k: usize) -> usize {
    top_k.saturating_mul(4).clamp(8, 256)
}

/// How many distinct terms two summaries share.
///
/// Lexical on purpose: the store scores every match the same, so a caller
/// reordering results has only the text in front of it.
fn shared_terms(left: &str, right: &str) -> usize {
    fn terms(text: &str) -> std::collections::HashSet<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|term| !term.is_empty())
            .map(|term| term.to_lowercase())
            .collect()
    }
    let (left, right) = (terms(left), terms(right));
    left.intersection(&right).count()
}

/// The count a row reaches after one more observation, given what reading it
/// said.
///
/// Separated from the read so the decision can be exercised without a store:
/// `Err` means the count is unknown, and unknown is not zero.
fn advance_count(prior: SFResult<Option<SchemaEntry>>) -> Option<u64> {
    match prior {
        Ok(Some(entry)) => Some(entry.occurrences.saturating_add(1)),
        Ok(None) => Some(1),
        Err(_) => None,
    }
}

/// The leading `SUMMARY_MAX_CHARS` characters of `text`, never splitting one.
fn clip(text: &str) -> String {
    if text.chars().count() <= SUMMARY_MAX_CHARS {
        return text.to_string();
    }
    text.chars().take(SUMMARY_MAX_CHARS).collect()
}

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

        // The store matches a query as a substring of an entry's name or key
        // and has no scoring of its own, so the query has to be the dimension
        // the entries are keyed on — the task type. Asking with the summary
        // appended makes the query a string no entry contains, which answers
        // "no prior implementation" for every task and is indistinguishable
        // from a namespace nothing was ever stored in.
        let results = memory
            .search_schema(NS_IMPLEMENTATION, task_type, scan_window(top_k))
            .await?;
        let mut examples: Vec<ImplementationExample> = results
            .into_iter()
            .filter_map(|r| {
                serde_json::from_value::<ImplementationExample>(r.entry.properties.clone()).ok()
            })
            .collect();
        // Rank what came back by how much of the summary it shares. The task
        // type alone cannot separate two runs of the same type, and it is the
        // only signal available: no vector layer on a host without the weights,
        // and every stored match comes back at the same score.
        examples.sort_by(|a, b| {
            shared_terms(input_summary, &b.input_summary)
                .cmp(&shared_terms(input_summary, &a.input_summary))
        });
        examples.truncate(top_k);
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
        let task_type = format!("{:?}", task.task_type);
        let result_summary = serde_json::to_string(&result.output)
            .map(|output| clip(&output))
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
            "task_type": task_type,
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

        let embedding_model = if embedding.is_empty() {
            cog_core::NO_EMBEDDING_MODEL
        } else {
            "unified_knowledge_backend"
        };
        let summary_entry = SummaryEntry::new(
            &record_id,
            NS_EXECUTION,
            &summary_text,
            embedding,
            embedding_model,
            SourceRef::new(&record_id, "unified_knowledge_backend::archive_execution"),
        )
        .with_related_schema_ids(vec![schema_entry.id.clone()]);

        if let Err(e) = memory.store_summary(NS_EXECUTION, &summary_entry).await {
            tracing::warn!("failed to archive execution summary: {}", e);
        }

        // --- Retrieval namespaces ---
        // `retrieve_similar_implementations` and `retrieve_failure_patterns`
        // read namespaces that had no writer: the reads were in place, so an
        // empty answer was indistinguishable from a namespace nothing had ever
        // been stored in. The archive is the one point that already sees how a
        // task ended, and the key shape belongs to the namespace's owner —
        // a caller writing rows here directly would have to guess the query
        // they have to match.
        let input_summary = serde_json::to_string(&task.input)
            .map(|input| clip(&input))
            .unwrap_or_default();
        if result.success {
            self.archive_implementation(
                memory,
                &task_type,
                &input_summary,
                &result_summary,
                result.metadata.score.unwrap_or_default() as f32,
            )
            .await;
        } else {
            self.archive_failure(
                memory,
                &task_type,
                &result_summary,
                result.metadata.feedback.as_deref().unwrap_or_default(),
            )
            .await;
        }

        Ok(())
    }
}

impl UnifiedKnowledgeBackend {
    /// Record what a task of this type looks like when it works.
    ///
    /// One row per task type, holding the newest success and the count of
    /// them: the retrievals ask by task type, and a row per run would grow the
    /// namespace without bound while every query keeps returning the same
    /// shape of answer.
    async fn archive_implementation(
        &self,
        memory: &Arc<dyn MemoryBackend>,
        task_type: &str,
        input_summary: &str,
        output_summary: &str,
        score: f32,
    ) {
        // The key is also the entry's name: both are matched against the
        // retrieval's query, so the two have to carry the task type, and one
        // string carrying it once cannot drift from another carrying it again.
        let key = format!("implementation:{task_type}");
        let Some(observed) = self
            .next_count(memory, NS_IMPLEMENTATION, SchemaKind::Learning, &key)
            .await
        else {
            return;
        };
        let example = ImplementationExample {
            example_id: cog_core::schema_entry_id(NS_IMPLEMENTATION, SchemaKind::Learning, &key),
            task_type: task_type.to_string(),
            input_summary: input_summary.to_string(),
            output_summary: output_summary.to_string(),
            score,
            observed_count: observed,
        };
        let properties = match serde_json::to_value(&example) {
            Ok(properties) => properties,
            Err(e) => {
                tracing::warn!("failed to serialize implementation example: {}", e);
                return;
            }
        };
        self.upsert(
            memory,
            NS_IMPLEMENTATION,
            SchemaKind::Learning,
            &key,
            properties,
            observed,
        )
        .await;
    }

    /// Record how a task of this type tends to fail.
    async fn archive_failure(
        &self,
        memory: &Arc<dyn MemoryBackend>,
        task_type: &str,
        failure_summary: &str,
        root_cause: &str,
    ) {
        let key = format!("failure:{task_type}");
        let Some(occurrences) = self
            .next_count(memory, NS_FAILURE, SchemaKind::ErrorPattern, &key)
            .await
        else {
            return;
        };
        let pattern = FailurePattern {
            pattern_id: cog_core::schema_entry_id(NS_FAILURE, SchemaKind::ErrorPattern, &key),
            task_type: task_type.to_string(),
            failure_summary: failure_summary.to_string(),
            root_cause: clip(root_cause),
            occurrence_count: occurrences,
            last_occurrence: chrono::Utc::now(),
        };
        let properties = match serde_json::to_value(&pattern) {
            Ok(properties) => properties,
            Err(e) => {
                tracing::warn!("failed to serialize failure pattern: {}", e);
                return;
            }
        };
        self.upsert(
            memory,
            NS_FAILURE,
            SchemaKind::ErrorPattern,
            &key,
            properties,
            occurrences,
        )
        .await;
    }

    /// The count this observation brings the row for `key` to, or `None` when
    /// the count the row already holds could not be read.
    ///
    /// The count is part of what the row states, so it cannot be invented: a
    /// row holding seven observations that could not be read is not a row
    /// holding none, and writing 1 over it would replace a wrong number rather
    /// than a missing one. Nothing is lost by skipping the refresh — the run
    /// itself is already archived under its own record, and this row is the
    /// retrieval aid derived from it, so what is skipped is freshness.
    async fn next_count(
        &self,
        memory: &Arc<dyn MemoryBackend>,
        namespace: &str,
        kind: SchemaKind,
        key: &str,
    ) -> Option<u64> {
        let id = cog_core::schema_entry_id(namespace, kind, key);
        let prior = memory.get_schema(namespace, &id).await;
        if let Err(e) = &prior {
            tracing::warn!(
                "not refreshing {} for {}: its count could not be read: {}",
                namespace,
                key,
                e
            );
        }
        advance_count(prior)
    }

    async fn upsert(
        &self,
        memory: &Arc<dyn MemoryBackend>,
        namespace: &str,
        kind: SchemaKind,
        key: &str,
        properties: serde_json::Value,
        observations: u64,
    ) {
        let id = cog_core::schema_entry_id(namespace, kind, key);
        let mut entry = SchemaEntry::new(
            &id,
            namespace,
            kind,
            key,
            key,
            SourceRef::new(&id, "unified_knowledge_backend::archive_execution"),
        )
        .with_properties(properties);
        entry.occurrences = observations;
        if let Err(e) = memory.update_schema(namespace, &entry).await {
            tracing::warn!("failed to archive {} entry: {}", namespace, e);
        }
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

    /// A row that could not be read is not a row holding nothing: the count is
    /// part of what the record states, so a failure to read it has to stop the
    /// refresh rather than restart the count at one.
    #[test]
    fn an_unreadable_count_is_unknown_rather_than_zero() {
        assert_eq!(
            advance_count(Err(cog_core::SFError::Internal("store down".into()))),
            None
        );
        assert_eq!(advance_count(Ok(None)), Some(1), "第一次观察从 1 起算");

        let mut entry = SchemaEntry::new(
            "id",
            NS_IMPLEMENTATION,
            SchemaKind::Learning,
            "implementation:Generator",
            "implementation:Generator",
            SourceRef::new("raw", "test"),
        );
        entry.occurrences = 7;
        assert_eq!(
            advance_count(Ok(Some(entry))),
            Some(8),
            "已观察 7 次的行走一步是 8，不是 1"
        );
    }

    /// The scan window has to be wider than the answer, because ranking happens
    /// after the store's own limit — asking for exactly `top_k` lets a
    /// near-matching key fill the window and push out the row the caller wanted.
    /// It also has to stay finite: one retrieval must not become a namespace scan.
    #[test]
    fn a_retrieval_window_is_wider_than_the_answer_but_bounded() {
        for top_k in [1_usize, 3, 50] {
            let window = scan_window(top_k);
            assert!(window > top_k, "top_k={top_k} 时窗口不能等于答案数");
            assert!(window <= 256, "top_k={top_k} 时窗口要有界");
        }
        assert!(scan_window(0) > 0, "top_k=0 也要能取到候选再排序");
        assert_eq!(scan_window(usize::MAX), 256, "溢出不得变成无界扫描");
    }
}
