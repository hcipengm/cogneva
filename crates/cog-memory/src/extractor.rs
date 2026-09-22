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
        self.extractor.extract_all(source).await
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

/// The shape one call answers both layers in. The schema half is the same three
/// lists [`SchemaExtraction`] declares, so [`Self::split`] hands each half to
/// the same builder the single-layer paths use.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct CombinedExtraction {
    #[serde(default)]
    entities: Vec<ExtractedEntity>,
    #[serde(default)]
    relations: Vec<ExtractedRelation>,
    #[serde(default)]
    events: Vec<ExtractedEvent>,
    summary: SummaryExtraction,
}

impl CombinedExtraction {
    fn split(self) -> (SchemaExtraction, SummaryExtraction) {
        (
            SchemaExtraction {
                entities: self.entities,
                relations: self.relations,
                events: self.events,
            },
            self.summary,
        )
    }
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

    /// What the model is asked to do for the schema layer.
    const SCHEMA_TASK: &'static str = "Extract structured information from the following \
         conversation or text. Identify entities, relations between them, and any events \
         mentioned.";

    /// What the model is asked to do for the summary layer.
    const SUMMARY_TASK: &'static str = "Summarize the following conversation or text, \
         focusing on key decisions, lessons learned, user preferences, and actionable \
         insights. Keep the summary concise (1-3 sentences).";

    /// Both layers are rated on the same 1-10 scale, so the scale is stated once
    /// instead of being repeated per task and drifting apart.
    const IMPORTANCE_TASK: &'static str = "Rate the importance of each extracted item on a \
         scale of 1-10, where 10 is critical information that will be valuable in future \
         conversations, and 1 is trivial.";

    /// The source is appended to the instructions exactly here, so every prompt
    /// path pays for the payload the same number of times — once.
    fn prompt(tasks: &str, source: &RawSource) -> String {
        format!("{tasks}\n\n{}", String::from_utf8_lossy(&source.payload))
    }

    fn build_schema_prompt(source: &RawSource) -> String {
        Self::prompt(
            &format!("{} {}", Self::SCHEMA_TASK, Self::IMPORTANCE_TASK),
            source,
        )
    }

    fn build_summary_prompt(source: &RawSource) -> String {
        Self::prompt(
            &format!("{} {}", Self::SUMMARY_TASK, Self::IMPORTANCE_TASK),
            source,
        )
    }

    /// One prompt carrying both tasks, so one call can answer both. The two
    /// single-layer prompts are what a caller falls back to when only one layer
    /// is still missing; this is what a caller with neither missing uses.
    fn build_combined_prompt(source: &RawSource) -> String {
        Self::prompt(
            &format!(
                "{} {}\n\n{} {}",
                Self::SCHEMA_TASK,
                Self::IMPORTANCE_TASK,
                Self::SUMMARY_TASK,
                Self::IMPORTANCE_TASK
            ),
            source,
        )
    }

    /// Turn a schema extraction into store entries. The schema-only call and
    /// the combined call both land here, so a merged call cannot produce
    /// different ids or importance for the same model output.
    fn schema_entries(source: &RawSource, extraction: SchemaExtraction) -> Vec<SchemaEntry> {
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

        entries
    }

    /// Turn a summary extraction into the store entry, embedding the text when
    /// this host has an embedder. Shared by the summary-only call and the
    /// combined call for the same reason as [`Self::schema_entries`].
    async fn summary_entry(
        &self,
        source: &RawSource,
        extraction: SummaryExtraction,
    ) -> SFResult<SummaryEntry> {
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

        Ok(Self::schema_entries(source, extraction))
    }

    async fn generate_summary(&self, source: &RawSource) -> SFResult<SummaryEntry> {
        let prompt = Self::build_summary_prompt(source);
        let extraction: SummaryExtraction = execute_structured(
            &*self.provider,
            &[cog_core::Message::user(prompt)],
            &self.options,
        )
        .await?;

        self.summary_entry(source, extraction).await
    }

    /// Both layers from one call. The source is a conversation transcript, so
    /// it dominates the prompt: asking for the layers in two calls sends all of
    /// it twice and pays for it twice. The two single-layer methods stay for
    /// the case where only one layer is still missing.
    async fn extract_all(&self, source: &RawSource) -> SFResult<(Vec<SchemaEntry>, SummaryEntry)> {
        let prompt = Self::build_combined_prompt(source);
        let extraction: CombinedExtraction = execute_structured(
            &*self.provider,
            &[cog_core::Message::user(prompt)],
            &self.options,
        )
        .await?;

        let (schema, summary) = extraction.split();
        let entries = Self::schema_entries(source, schema);
        let summary = self.summary_entry(source, summary).await?;
        Ok((entries, summary))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{MemoryBackend, MemoryExtractor};

    fn raw(id: &str, body: &str) -> RawSource {
        RawSource::new(id, "default", "text/plain", body.as_bytes().to_vec())
    }

    /// 抽取走的是 `execute_structured`，最终落在一次 `chat` 上，所以调用次数
    /// 就是"这段 payload 被发了几遍"。它同时记下每次收到的正文，供断言
    /// payload 出现次数用。
    struct ScriptedLlm {
        answer: String,
        calls: std::sync::atomic::AtomicUsize,
        prompts: std::sync::Mutex<Vec<String>>,
    }

    impl ScriptedLlm {
        fn new(answer: &str) -> Self {
            Self {
                answer: answer.to_string(),
                calls: std::sync::atomic::AtomicUsize::new(0),
                prompts: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn prompts(&self) -> Vec<String> {
            self.prompts.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl LlmClient for ScriptedLlm {
        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
            producer.end(cog_core::ChatResponse::default());
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            self.chat_stream(&[], &ChatOptions::default()).await
        }

        async fn chat(
            &self,
            messages: &[cog_core::Message],
            _options: &ChatOptions,
        ) -> SFResult<cog_core::ChatResponse> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.prompts.lock().unwrap().push(
                messages
                    .iter()
                    .map(|m| m.content())
                    .collect::<Vec<_>>()
                    .join("\n"),
            );

            Ok(cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::text(self.answer.clone())],
                ..Default::default()
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// The schema half alone, as the schema-only call would answer it.
    const SCHEMA_ANSWER: &str = r#"{
        "entities": [
            {"name": "gateway", "kind": "service", "properties": {"tier": "edge"}, "importance": 9}
        ],
        "relations": [
            {"source": "gateway", "target": "cluster", "relation_type": "routes_to", "importance": 7}
        ],
        "events": [
            {"name": "deploy finished", "participants": ["gateway"], "importance": 8}
        ]
    }"#;

    /// Both halves in one answer, as the merged call asks for them.
    const COMBINED_ANSWER: &str = r#"{
        "entities": [
            {"name": "gateway", "kind": "service", "properties": {"tier": "edge"}, "importance": 9}
        ],
        "relations": [
            {"source": "gateway", "target": "cluster", "relation_type": "routes_to", "importance": 7}
        ],
        "events": [
            {"name": "deploy finished", "participants": ["gateway"], "importance": 8}
        ],
        "summary": {
            "text": "The gateway routed a deploy to the cluster.",
            "importance": 6
        }
    }"#;

    /// 一段对话就是抽取调用的 payload，token 大头在它上面。分层调用把同一段
    /// payload 发两遍、按两遍计费；合并之后两层必须来自同一次请求。
    #[tokio::test]
    async fn both_layers_come_from_one_call() {
        let provider = Arc::new(ScriptedLlm::new(COMBINED_ANSWER));
        let extractor = LlmMemoryExtractor::new(provider.clone());

        let (schema, summary) = extractor
            .extract_all(&raw("raw-a", "gateway routed a deploy to the cluster"))
            .await
            .unwrap();

        assert_eq!(
            provider.calls(),
            1,
            "the source must be sent once, not once per layer"
        );
        // 那一次请求必须是合并提示词，否则"省一半"就退化成只问了其中一层。
        let sent = provider.prompts();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains(LlmMemoryExtractor::SCHEMA_TASK));
        assert!(sent[0].contains(LlmMemoryExtractor::SUMMARY_TASK));
        assert_eq!(
            schema.len(),
            3,
            "all three schema kinds survived: {schema:?}"
        );
        assert_eq!(summary.text, "The gateway routed a deploy to the cluster.");
        assert_eq!(summary.importance, cog_core::importance_from_rating(6));
    }

    /// 合并本身不许把 payload 塞两遍——那正是这次要消掉的那笔开销；也不许
    /// 用"少发点"的名义丢掉一层。
    #[tokio::test]
    async fn the_merged_prompt_carries_the_payload_once() {
        const SENTINEL: &str = "SENTINEL-4682f1";
        let prompt = LlmMemoryExtractor::build_combined_prompt(&raw("raw-a", SENTINEL));

        assert_eq!(
            prompt.matches(SENTINEL).count(),
            1,
            "the payload must appear exactly once: {prompt}"
        );
        assert!(prompt.contains(LlmMemoryExtractor::SCHEMA_TASK));
        assert!(prompt.contains(LlmMemoryExtractor::SUMMARY_TASK));
    }

    /// 合并只许省调用，不许改落库的内容：同一份模型输出，分层走与合并走
    /// 必须产出同样的 id、kind 与 importance，否则"优化"会静默改掉记忆。
    #[tokio::test]
    async fn the_merged_call_stores_what_the_layered_call_stores() {
        let layered = LlmMemoryExtractor::new(Arc::new(ScriptedLlm::new(SCHEMA_ANSWER)));
        let merged = LlmMemoryExtractor::new(Arc::new(ScriptedLlm::new(COMBINED_ANSWER)));
        let source = raw("raw-a", "gateway routed a deploy to the cluster");

        let mut alone = layered.extract_schema(&source).await.unwrap();
        let (mut together, _) = merged.extract_all(&source).await.unwrap();

        let shape = |entries: &mut Vec<SchemaEntry>| {
            entries.sort_by(|a, b| a.id.cmp(&b.id));
            entries
                .iter()
                .map(|e| (e.id.clone(), e.kind, e.importance))
                .collect::<Vec<_>>()
        };
        assert_eq!(shape(&mut alone), shape(&mut together));
    }

    /// 抽取器报错时两层一起失败（一次调用只有一个结局）。这不是缺陷，是合并
    /// 的代价：调用失败后什么都没落库，重驱动会把两层一起补上。
    #[tokio::test]
    async fn a_failed_merged_call_stores_nothing() {
        let provider = Arc::new(ScriptedLlm::new("{\"entities\": []}"));
        let extractor = LlmMemoryExtractor::new(provider.clone());

        assert!(extractor
            .extract_all(&raw("raw-a", "gateway routed a deploy"))
            .await
            .is_err());
        assert_eq!(provider.calls(), 1);
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
