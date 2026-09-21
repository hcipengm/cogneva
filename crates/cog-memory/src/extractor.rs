use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{EmbeddingProvider, SFResult};
use cog_core::{RawSource, SchemaEntry, SchemaKind, SourceRef, SummaryEntry, NO_EMBEDDING_MODEL};

use cog_core::MemoryExtractor;

/// A rule-based extractor for testing and baseline behaviour.
/// - Schema extraction looks for simple `@entity:Name` and
///   `@relation:Name->Target` patterns in text payloads.
/// - Summary generation returns a truncated text preview and no embedding,
///   so that the pipeline can be exercised without an LLM.
#[derive(Debug, Clone, Default)]
pub struct RuleBasedExtractor;

impl RuleBasedExtractor {
    pub fn new() -> Self {
        Self
    }

    fn parse_entities(text: &str) -> Vec<(String, String)> {
        let mut results = Vec::new();
        for line in text.lines() {
            if let Some(stripped) = line.trim().strip_prefix("@entity:") {
                let name = stripped.trim().to_string();
                let key = name.to_lowercase().replace(' ', "_");
                results.push((name, key));
            }
        }
        results
    }

    fn parse_relations(text: &str) -> Vec<(String, String, String)> {
        let mut results = Vec::new();
        for line in text.lines() {
            if let Some(stripped) = line.trim().strip_prefix("@relation:") {
                let parts: Vec<&str> = stripped.split("->").collect();
                if parts.len() == 2 {
                    results.push((
                        parts[0].trim().to_string(),
                        parts[1].trim().to_string(),
                        format!("{}_to_{}", parts[0].trim(), parts[1].trim()),
                    ));
                }
            }
        }
        results
    }

    fn parse_events(text: &str) -> Vec<(String, String)> {
        let mut results = Vec::new();
        for line in text.lines() {
            if let Some(stripped) = line.trim().strip_prefix("@event:") {
                let name = stripped.trim().to_string();
                let key = name.to_lowercase().replace(' ', "_");
                results.push((name, key));
            }
        }
        results
    }
}

#[async_trait]
impl MemoryExtractor for RuleBasedExtractor {
    async fn extract_schema(&self, source: &RawSource) -> SFResult<Vec<SchemaEntry>> {
        let text = String::from_utf8_lossy(&source.payload);
        let source_ref = SourceRef::new(format!("memory://{}", source.id), "rule_based/v1");

        let mut entries = Vec::new();

        // Ids are scoped to the source. `store_schema` upserts on `id`, so a
        // position-only id (`schema-entity-0`) is the same primary key for
        // every source: the second source ingested overwrites the first
        // source's rows and rewrites their `raw_uri`, so the loss leaves no
        // trace to notice it by.
        for (idx, (name, key)) in Self::parse_entities(&text).into_iter().enumerate() {
            entries.push(
                SchemaEntry::new(
                    format!("schema-entity-{}-{}", source.id, idx),
                    &source.namespace,
                    SchemaKind::Entity,
                    name,
                    key,
                    source_ref.clone(),
                )
                .with_importance(0.6),
            );
        }

        for (idx, (from, to, key)) in Self::parse_relations(&text).into_iter().enumerate() {
            entries.push(
                SchemaEntry::new(
                    format!("schema-relation-{}-{}", source.id, idx),
                    &source.namespace,
                    SchemaKind::Relation,
                    format!("{} -> {}", from, to),
                    key,
                    source_ref.clone(),
                )
                .with_properties(serde_json::json!({
                    "from": from,
                    "to": to,
                }))
                .with_importance(0.7),
            );
        }

        for (idx, (name, key)) in Self::parse_events(&text).into_iter().enumerate() {
            entries.push(
                SchemaEntry::new(
                    format!("schema-event-{}-{}", source.id, idx),
                    &source.namespace,
                    SchemaKind::Event,
                    name,
                    key,
                    source_ref.clone(),
                )
                .with_importance(0.8),
            );
        }

        Ok(entries)
    }

    async fn generate_summary(&self, source: &RawSource) -> SFResult<SummaryEntry> {
        let text = String::from_utf8_lossy(&source.payload);
        let preview: String = text.chars().take(200).collect();
        let source_ref = SourceRef::new(format!("memory://{}", source.id), "rule_based/v1");

        Ok(SummaryEntry::new(
            format!("summary-{}", source.id),
            &source.namespace,
            preview,
            Vec::new(),
            NO_EMBEDDING_MODEL,
            source_ref,
        )
        .with_importance(0.5))
    }
}

/// High-level convenience wrapper that runs the full ingestion pipeline
/// for a single raw source.
#[derive(Debug, Clone)]
pub struct IngestionPipeline<E: MemoryExtractor> {
    extractor: E,
}

impl<E: MemoryExtractor> IngestionPipeline<E> {
    pub fn new(extractor: E) -> Self {
        Self { extractor }
    }

    /// Run the extractor against a raw source and return both layers.
    pub async fn ingest(&self, source: &RawSource) -> SFResult<(Vec<SchemaEntry>, SummaryEntry)> {
        let schema = self.extractor.extract_schema(source).await?;
        let summary = self.extractor.generate_summary(source).await?;
        Ok((schema, summary))
    }
}

#[async_trait]
impl cog_core::MemoryIngestor for IngestionPipeline<RuleBasedExtractor> {
    async fn ingest(
        &self,
        source: &RawSource,
    ) -> cog_core::SFResult<(Vec<SchemaEntry>, SummaryEntry)> {
        self.ingest(source).await
    }
}

// ---------------------------------------------------------------------------
// LLM-driven MemoryExtractor
// ---------------------------------------------------------------------------

use cog_core::{execute_structured, ChatOptions, LlmClient};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct ExtractedEntity {
    name: String,
    kind: String,
    #[serde(default)]
    properties: HashMap<String, String>,
    #[serde(default)]
    importance: u8, // 1-10
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct ExtractedRelation {
    source: String,
    target: String,
    relation_type: String,
    #[serde(default)]
    importance: u8, // 1-10
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct ExtractedEvent {
    name: String,
    timestamp: Option<String>,
    #[serde(default)]
    participants: Vec<String>,
    #[serde(default)]
    importance: u8, // 1-10
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct SchemaExtraction {
    #[serde(default)]
    entities: Vec<ExtractedEntity>,
    #[serde(default)]
    relations: Vec<ExtractedRelation>,
    #[serde(default)]
    events: Vec<ExtractedEvent>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct SummaryExtraction {
    text: String,
    #[serde(default)]
    importance: u8, // 1-10
}

/// LLM-driven [`MemoryExtractor`] implementation.
/// Uses a configured [`LlmClient`] to perform:
/// - Named-entity recognition (NER) + relation extraction + event extraction
/// - Semantic summarization with optional embedding generation
///
/// The extraction prompts are schema-driven via [`execute_structured`],
/// so the LLM is constrained to return valid JSON matching the expected
/// shapes.
#[derive(Clone)]
pub struct LlmMemoryExtractor {
    provider: Arc<dyn LlmClient>,
    options: ChatOptions,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
}

impl std::fmt::Debug for LlmMemoryExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmMemoryExtractor")
            .field("options", &self.options)
            .field("embedder", &self.embedder.is_some())
            .finish_non_exhaustive()
    }
}

impl LlmMemoryExtractor {
    pub fn new(provider: Arc<dyn LlmClient>) -> Self {
        Self {
            provider,
            options: ChatOptions::default().with_actor("memory"),
            embedder: None,
        }
    }

    pub fn with_options(mut self, options: ChatOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_embedder(mut self, embedder: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    fn build_schema_prompt(source: &RawSource) -> String {
        let text = String::from_utf8_lossy(&source.payload);
        format!(
            "Extract structured information from the following conversation or text. \
             Identify entities, relations between them, and any events mentioned. \
             Rate the importance of each extracted item on a scale of 1-10, \
             where 10 is critical information that will be valuable in future conversations, \
             and 1 is trivial.\n\n{}",
            text
        )
    }

    fn build_summary_prompt(source: &RawSource) -> String {
        let text = String::from_utf8_lossy(&source.payload);
        format!(
            "Summarize the following conversation or text, focusing on key decisions, \
             lessons learned, user preferences, and actionable insights. \
             Keep the summary concise (1-3 sentences). \
             Rate the importance of the summary on a scale of 1-10, \
             where 10 is critical information that will be valuable in future conversations, \
             and 1 is trivial.\n\n{}",
            text
        )
    }
}

#[async_trait]
impl MemoryExtractor for LlmMemoryExtractor {
    async fn extract_schema(&self, source: &RawSource) -> SFResult<Vec<SchemaEntry>> {
        let prompt = Self::build_schema_prompt(source);
        let extraction: SchemaExtraction = execute_structured(
            &*self.provider,
            &[cog_core::Message::user(prompt)],
            &self.options,
        )
        .await?;

        let source_ref = SourceRef::new(format!("memory://{}", source.id), "llm/v1");

        let mut entries = Vec::new();

        for (idx, entity) in extraction.entities.into_iter().enumerate() {
            let key = entity.name.to_lowercase().replace(' ', "_");
            let importance = (entity.importance as f32).clamp(1.0, 10.0) / 10.0;
            entries.push(
                SchemaEntry::new(
                    format!("schema-entity-{}-{}", source.id, idx),
                    &source.namespace,
                    SchemaKind::Entity,
                    entity.name,
                    key,
                    source_ref.clone(),
                )
                .with_properties(serde_json::to_value(entity.properties).unwrap_or_default())
                .with_importance(importance),
            );
        }

        for (idx, relation) in extraction.relations.into_iter().enumerate() {
            let key = format!(
                "{}_to_{}",
                relation.source.to_lowercase().replace(' ', "_"),
                relation.target.to_lowercase().replace(' ', "_")
            );
            let importance = (relation.importance as f32).clamp(1.0, 10.0) / 10.0;
            entries.push(
                SchemaEntry::new(
                    format!("schema-relation-{}-{}", source.id, idx),
                    &source.namespace,
                    SchemaKind::Relation,
                    format!("{} -> {}", relation.source, relation.target),
                    key,
                    source_ref.clone(),
                )
                .with_properties(serde_json::json!({
                    "relation_type": relation.relation_type,
                    "from": relation.source,
                    "to": relation.target,
                }))
                .with_importance(importance),
            );
        }

        for (idx, event) in extraction.events.into_iter().enumerate() {
            let key = event.name.to_lowercase().replace(' ', "_");
            let mut props = serde_json::json!({
                "participants": event.participants,
            });
            if let Some(ts) = event.timestamp {
                props["timestamp"] = serde_json::Value::String(ts);
            }
            let importance = (event.importance as f32).clamp(1.0, 10.0) / 10.0;
            entries.push(
                SchemaEntry::new(
                    format!("schema-event-{}-{}", source.id, idx),
                    &source.namespace,
                    SchemaKind::Event,
                    event.name,
                    key,
                    source_ref.clone(),
                )
                .with_properties(props)
                .with_importance(importance),
            );
        }

        Ok(entries)
    }

    async fn generate_summary(&self, source: &RawSource) -> SFResult<SummaryEntry> {
        let prompt = Self::build_summary_prompt(source);
        let extraction: SummaryExtraction = execute_structured(
            &*self.provider,
            &[cog_core::Message::user(prompt)],
            &self.options,
        )
        .await?;

        // An embedder that answers with nothing is a fault, not an absence: only
        // a missing embedder means "this host has no vector layer". Collapsing
        // the two would hide a broken embedder behind the same silent state.
        let embedding = match self.embedder.as_ref() {
            Some(embedder) => embedder
                .embed(vec![extraction.text.clone()])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    cog_core::SFError::Validation("embedder returned no vector".into())
                })?,
            None => Vec::new(),
        };
        let embedding_model = if embedding.is_empty() {
            NO_EMBEDDING_MODEL
        } else {
            "llm/v1"
        };

        let source_ref = SourceRef::new(format!("memory://{}", source.id), "llm/v1");

        let importance = (extraction.importance as f32).clamp(1.0, 10.0) / 10.0;
        Ok(SummaryEntry::new(
            format!("summary-{}", source.id),
            &source.namespace,
            extraction.text,
            embedding,
            embedding_model,
            source_ref,
        )
        .with_importance(importance))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{MemoryBackend, MemoryExtractor};

    fn raw(id: &str, body: &str) -> RawSource {
        RawSource::new(id, "default", "text/plain", body.as_bytes().to_vec())
    }

    /// Two raws carrying the same `@entity:` line are two entities observed
    /// twice, not one entity. Ids are the store's primary key, so a
    /// position-only id would make the second raw's rows overwrite the first
    /// raw's rows — and rewrite their `raw_uri` — leaving nothing behind that
    /// says a row went missing.
    #[tokio::test]
    async fn rule_based_schema_ids_are_scoped_to_their_source() {
        let extractor = RuleBasedExtractor::new();
        let first = extractor
            .extract_schema(&raw("raw-a", "@entity: security gateway\n"))
            .await
            .unwrap();
        let second = extractor
            .extract_schema(&raw("raw-b", "@entity: security gateway\n"))
            .await
            .unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_ne!(
            first[0].id, second[0].id,
            "the same name from two sources must not share a primary key"
        );

        let backend = crate::MemoryMemoryBackend::new();
        backend.store_schema("default", &first[0]).await.unwrap();
        backend.store_schema("default", &second[0]).await.unwrap();

        let rows = backend.list_schema("default").await.unwrap();
        assert_eq!(rows.len(), 2, "both sources' rows must survive the store");
        let uris: std::collections::HashSet<&str> =
            rows.iter().map(|r| r.source_ref.raw_uri.as_str()).collect();
        assert_eq!(uris.len(), 2, "each row must keep its own source");
    }

    /// Without an embedder there is no vector to store. A run of zeros would
    /// claim otherwise: every summary would carry a well-formed vector that
    /// scores 0.0 against every query, and the index would answer searches with
    /// arbitrary ties instead of with an empty set.
    #[tokio::test]
    async fn rule_based_summaries_carry_no_embedding() {
        let extractor = RuleBasedExtractor::new();
        let summary = extractor
            .generate_summary(&raw("raw-a", "some text"))
            .await
            .unwrap();

        assert!(
            summary.embedding.is_empty(),
            "a summary with no vector must store no vector, not zeros"
        );
        assert_eq!(summary.embedding_model, NO_EMBEDDING_MODEL);
    }

    /// Re-extracting the same raw must produce the same ids, so the store's
    /// upsert updates in place instead of appending a second copy.
    #[tokio::test]
    async fn rule_based_schema_ids_are_stable_across_extraction() {
        let extractor = RuleBasedExtractor::new();
        let source = raw(
            "raw-a",
            "@entity: alpha\n@relation: alpha->beta\n@event: deploy\n",
        );
        let once = extractor.extract_schema(&source).await.unwrap();
        let twice = extractor.extract_schema(&source).await.unwrap();

        let ids = |v: &[SchemaEntry]| v.iter().map(|e| e.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids(&once), ids(&twice));
        assert_eq!(once.len(), 3);
    }
}
