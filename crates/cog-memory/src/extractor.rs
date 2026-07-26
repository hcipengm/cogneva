use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{EmbeddingProvider, SFResult};
use cog_core::{RawSource, SchemaEntry, SchemaKind, SourceRef, SummaryEntry};

use cog_core::MemoryExtractor;

/// A rule-based extractor for testing and baseline behaviour.
/// - Schema extraction looks for simple `@entity:Name` and
///   `@relation:Name->Target` patterns in text payloads.
/// - Summary generation returns a truncated text preview and a dummy
///   embedding (all zeros) so that the pipeline can be exercised
///   without an LLM.
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

        for (name, key) in Self::parse_entities(&text) {
            entries.push(
                SchemaEntry::new(
                    format!("schema-entity-{}", entries.len()),
                    &source.namespace,
                    SchemaKind::Entity,
                    name,
                    key,
                    source_ref.clone(),
                )
                .with_importance(0.6),
            );
        }

        for (from, to, key) in Self::parse_relations(&text) {
            entries.push(
                SchemaEntry::new(
                    format!("schema-relation-{}", entries.len()),
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

        for (name, key) in Self::parse_events(&text) {
            entries.push(
                SchemaEntry::new(
                    format!("schema-event-{}", entries.len()),
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

        let embedding = vec![0.0f32; 128]; // dummy 128-dim embedding

        Ok(SummaryEntry::new(
            format!("summary-{}", source.id),
            &source.namespace,
            preview,
            embedding,
            "dummy/v1",
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
    embedding_dim: usize,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
}

impl std::fmt::Debug for LlmMemoryExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmMemoryExtractor")
            .field("options", &self.options)
            .field("embedding_dim", &self.embedding_dim)
            .field("embedder", &self.embedder.is_some())
            .finish_non_exhaustive()
    }
}

impl LlmMemoryExtractor {
    pub fn new(provider: Arc<dyn LlmClient>, embedding_dim: usize) -> Self {
        Self {
            provider,
            options: ChatOptions::default(),
            embedding_dim,
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

        let embedding = if let Some(ref embedder) = self.embedder {
            let embeddings = embedder.embed(vec![extraction.text.clone()]).await?;
            embeddings
                .into_iter()
                .next()
                .unwrap_or_else(|| vec![0.0f32; self.embedding_dim])
        } else {
            vec![0.0f32; self.embedding_dim]
        };

        let source_ref = SourceRef::new(format!("memory://{}", source.id), "llm/v1");

        let importance = (extraction.importance as f32).clamp(1.0, 10.0) / 10.0;
        Ok(SummaryEntry::new(
            format!("summary-{}", source.id),
            &source.namespace,
            extraction.text,
            embedding,
            "llm/v1",
            source_ref,
        )
        .with_importance(importance))
    }
}
