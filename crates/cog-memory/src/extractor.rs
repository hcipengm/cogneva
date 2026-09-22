use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{EmbeddingProvider, SFResult};
use cog_core::{RawSource, SchemaEntry, SchemaKind, SourceRef, SummaryEntry, NO_EMBEDDING_MODEL};

use cog_core::MemoryExtractor;

/// Fold one more mention of a fact into the pass's entries.
///
/// A pass that finds the same fact twice — two lines naming one event, an LLM
/// listing an entity it already listed — would otherwise hand the store two
/// entries carrying one identity, and the store would keep one of them. What
/// the store cannot recover on its own is how many times the pass saw it, so
/// the repeats are counted here, where they are still visible.
fn record(entries: &mut Vec<SchemaEntry>, entry: SchemaEntry) {
    match entries.iter_mut().find(|e| e.id == entry.id) {
        Some(existing) => existing.occurrences = existing.occurrences.saturating_add(1),
        None => entries.push(entry),
    }
}

/// A rule-based extractor for testing and baseline behaviour.
/// - Schema extraction looks for simple `@entity:Name` and
///   `@relation:Name->Target` patterns in text payloads.
/// - Summary generation returns a truncated text preview and no embedding,
///   so that the pipeline can be exercised without an LLM.
///
/// Importance here is a fixed prior per kind, not a measurement: a pattern
/// match says that something was written down, never how much it matters. The
/// prior is stated as a rating on the shared importance scale (see
/// [`cog_core::IMPORTANCE_RATING_MAX`]) so these entries sort against ones a
/// model rated, and it is deliberately undiscriminating — four kinds placed at
/// four adjacent ratings, nothing near the top. A rule extractor that claimed
/// a high rating would be asserting evidence it does not have, and a fallback
/// extractor runs precisely when no producer with better evidence is available.
#[derive(Debug, Clone, Default)]
pub struct RuleBasedExtractor;

/// Rating this extractor places a kind at, before conversion to the entry's
/// `importance`. See [`RuleBasedExtractor`] for why the numbers are a prior.
mod rating {
    /// An entity is named and nothing more is said about it.
    pub const ENTITY: u8 = 6;
    /// A relation carries the two ends it connects, so it says slightly more.
    pub const RELATION: u8 = 7;
    /// An event is a thing that happened, at a point in time.
    pub const EVENT: u8 = 8;
    /// A preview is a slice of the payload, not a claim about the payload.
    pub const SUMMARY: u8 = 5;
}

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
            record(
                &mut entries,
                SchemaEntry::identified(
                    &source.namespace,
                    SchemaKind::Entity,
                    name,
                    key,
                    source_ref.clone(),
                )
                .with_importance(cog_core::importance_from_rating(rating::ENTITY)),
            );
        }

        for (from, to, key) in Self::parse_relations(&text) {
            record(
                &mut entries,
                SchemaEntry::identified(
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
                .with_importance(cog_core::importance_from_rating(rating::RELATION)),
            );
        }

        for (name, key) in Self::parse_events(&text) {
            record(
                &mut entries,
                SchemaEntry::identified(
                    &source.namespace,
                    SchemaKind::Event,
                    name,
                    key,
                    source_ref.clone(),
                )
                .with_importance(cog_core::importance_from_rating(rating::EVENT)),
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
        .with_importance(cog_core::importance_from_rating(rating::SUMMARY)))
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

        for entity in extraction.entities {
            let key = entity.name.to_lowercase().replace(' ', "_");
            let importance = cog_core::importance_from_rating(entity.importance);
            record(
                &mut entries,
                SchemaEntry::identified(
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

        for relation in extraction.relations {
            let key = format!(
                "{}_to_{}",
                relation.source.to_lowercase().replace(' ', "_"),
                relation.target.to_lowercase().replace(' ', "_")
            );
            let importance = cog_core::importance_from_rating(relation.importance);
            record(
                &mut entries,
                SchemaEntry::identified(
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

        for event in extraction.events {
            let key = event.name.to_lowercase().replace(' ', "_");
            let mut props = serde_json::json!({
                "participants": event.participants,
            });
            if let Some(ts) = event.timestamp {
                props["timestamp"] = serde_json::Value::String(ts);
            }
            let importance = cog_core::importance_from_rating(event.importance);
            record(
                &mut entries,
                SchemaEntry::identified(
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

        let importance = cog_core::importance_from_rating(extraction.importance);
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

    /// The rule path's importance is a rating on the shared scale, not a bare
    /// float. Asserting the arithmetic (`importance * max` is a whole rating)
    /// rather than the four numbers themselves pins the property that matters:
    /// these entries sort against entries a model rated, and a later edit that
    /// reached for a hand-picked float would have to leave the scale to do it.
    #[tokio::test]
    async fn the_rule_path_places_importance_on_the_shared_scale() {
        let extractor = RuleBasedExtractor::new();
        let entries = extractor
            .extract_schema(&raw(
                "raw-scale",
                "@entity: gateway\n@relation: gateway -> cluster\n@event: deploy finished\n",
            ))
            .await
            .unwrap();

        let max = cog_core::IMPORTANCE_RATING_MAX as f32;
        for entry in &entries {
            let rating = entry.importance * max;
            assert!(
                (rating - rating.round()).abs() < f32::EPSILON,
                "{:?} importance {} is not on the rating scale",
                entry.kind,
                entry.importance
            );
        }

        // The prior only has to be a prior; asserting the relative order keeps
        // it from silently flattening, which is what a hand-edit to `0.5` for
        // every kind would do.
        let importance_of = |kind: SchemaKind| {
            entries
                .iter()
                .find(|e| e.kind == kind)
                .map(|e| e.importance)
                .expect("kind present in the fixture")
        };
        assert!(
            importance_of(SchemaKind::Event) > importance_of(SchemaKind::Relation)
                && importance_of(SchemaKind::Relation) > importance_of(SchemaKind::Entity),
            "the prior must keep its order: {:?}",
            entries
                .iter()
                .map(|e| (e.kind, e.importance))
                .collect::<Vec<_>>()
        );
    }

    /// Two raws carrying the same `@entity:` line are one entity observed
    /// twice. An id scoped to the raw that produced it would make them two
    /// rows holding one fact, and the row count would then measure ingestion
    /// volume instead of knowledge.
    #[tokio::test]
    async fn one_entity_reported_by_two_sources_is_one_row() {
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
        assert_eq!(
            first[0].id, second[0].id,
            "the same fact from two sources must be one identity"
        );

        let backend = crate::MemoryMemoryBackend::new();
        backend.store_schema("default", &first[0]).await.unwrap();
        backend.store_schema("default", &second[0]).await.unwrap();

        let rows = backend.list_schema("default").await.unwrap();
        assert_eq!(rows.len(), 1, "one fact, one row");
        assert_eq!(rows[0].occurrences, 2, "both observations must be counted");
        assert!(
            rows[0].observed_from("memory://raw-a") && rows[0].observed_from("memory://raw-b"),
            "both sources must stay readable as origins: {:?}",
            rows[0].observed_by
        );

        // Each source's own query still finds the shared row, which is what
        // keeps the reconciliation pass from re-extracting it forever.
        assert_eq!(
            backend
                .schema_for_raw("default", "raw-a")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            backend
                .schema_for_raw("default", "raw-b")
                .await
                .unwrap()
                .len(),
            1
        );

        // Forgetting one origin must not erase the other's fact.
        backend.forget("default", "raw-a").await.unwrap();
        let rows = backend.list_schema("default").await.unwrap();
        assert_eq!(rows.len(), 1, "the fact outlives the source it came from");
        assert_eq!(rows[0].observed_by, vec!["memory://raw-b".to_string()]);
        assert_eq!(
            backend
                .schema_for_raw("default", "raw-a")
                .await
                .unwrap()
                .len(),
            0
        );

        // The last origin gone leaves nothing behind.
        backend.forget("default", "raw-b").await.unwrap();
        assert!(backend.list_schema("default").await.unwrap().is_empty());
    }

    /// Repeated mentions inside one raw are counts, not rows: the store would
    /// collapse them onto one identity anyway, and the count is the part the
    /// store cannot recover once the pass is over.
    #[tokio::test]
    async fn repeated_mentions_in_one_raw_become_an_occurrence_count() {
        let extractor = RuleBasedExtractor::new();
        let entries = extractor
            .extract_schema(&raw(
                "raw-a",
                "@event: deploy\n@event: deploy\n@event: rollback\n",
            ))
            .await
            .unwrap();

        assert_eq!(entries.len(), 2, "two facts, not three rows: {entries:?}");
        let deploy = entries
            .iter()
            .find(|e| e.key == "deploy")
            .expect("deploy event");
        assert_eq!(deploy.occurrences, 2);

        // Re-extracting the same raw is the same observation seen again, so
        // the count must not inflate.
        let backend = crate::MemoryMemoryBackend::new();
        for entry in &entries {
            backend.store_schema("default", entry).await.unwrap();
        }
        for entry in &entries {
            backend.store_schema("default", entry).await.unwrap();
        }
        let rows = backend.list_schema("default").await.unwrap();
        let deploy = rows.iter().find(|e| e.key == "deploy").expect("deploy row");
        assert_eq!(
            deploy.occurrences, 2,
            "a re-extraction is not a new sighting"
        );
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
