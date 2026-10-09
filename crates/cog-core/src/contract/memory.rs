use crate::{storage::SparseEmbedding, SFResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ─── Types ───────────────────────────────────────────────────────────

/// The one scale importance is expressed on, system-wide: a rating in
/// `1..=IMPORTANCE_RATING_MAX`, divided by the maximum.
///
/// Two kinds of producer write this field. One measures it — the model rates an
/// item 1..=10 and that rating is the evidence. The other can only assert a
/// prior, because a rule match says nothing about how much an item matters. Both
/// land on this scale so their entries rank against each other; without a shared
/// scale each producer's numbers would be comparable only to its own, and a
/// reader comparing two entries could not tell which producer was speaking.
///
/// The `importance` field on each entry type stays a plain `f32` in
/// `0.0..=1.0`, because that is what consumers sort on, and a bounded float
/// needs no conversion at the point of use.
pub const IMPORTANCE_RATING_MAX: u8 = 10;

/// Place a rating on the shared importance scale, clamping it into range.
///
/// Clamping rather than rejecting: the ratings that arrive from a model are
/// frequently out of the advertised range, and the alternative to clamping is
/// losing the item over a number that is only ever used for ordering.
pub fn importance_from_rating(rating: u8) -> f32 {
    rating.clamp(1, IMPORTANCE_RATING_MAX) as f32 / IMPORTANCE_RATING_MAX as f32
}

/// The score a summary search ranks by: raw similarity scaled by the entry's
/// importance.
///
/// Importance is the one field both producers share (see
/// [`IMPORTANCE_RATING_MAX`]) and the field decay demotes over time. Folding it
/// into the ranking is what turns decay into a *fade*: a demoted entry sinks in
/// search as its importance drops, instead of holding its place until the
/// archive floor removes it in one step. Without it the only way a low-value
/// memory leaves the results is by disappearing entirely.
///
/// `similarity` is a cosine in `-1.0..=1.0`. A non-positive value says "not
/// related to this query", and importance must not lift such an entry by
/// scaling a negative number toward zero — the clamp maps the whole
/// anti-correlated half to `0.0`, so importance can only ever *demote* within
/// the related half, never promote the unrelated half. Within the related half
/// importance is a pure multiplier, so entries of equal importance keep their
/// similarity order; the unrelated half is a flat zero, and its order is
/// whatever [`rank_key`] decides.
///
/// Importance is a weight, so its floor is the one value that makes an entry
/// unrankable: everything it could score is multiplied by zero and it sinks
/// below every entry that is both related and rated. That follows from the
/// declared `0.0..=1.0` range rather than from a filter — an entry is ranked,
/// not withheld — and the paths that *rate* entries keep a floor of their own
/// (see [`importance_from_rating`]), so a zero is something a caller states
/// rather than something a producer drifts into.
pub fn importance_weighted_score(similarity: f32, importance: f32) -> f32 {
    // `f32::max` returns the non-NaN operand, so a NaN similarity becomes 0.0
    // rather than propagating a NaN score into every downstream comparison.
    similarity.max(0.0) * importance
}

/// The order a ranked result set sorts by, most significant first: the weighted
/// score, the raw similarity, then the entry id.
///
/// The last two keys are not conveniences. [`importance_weighted_score`] maps the
/// whole non-positive half onto one score, so the score alone cannot order the
/// unrelated entries at the end of a result set — they would come back in
/// whatever order the store handed them over in, and a map's iteration order is
/// per-process, so a stable sort preserves *that*. The same query then answers
/// differently between runs, and where a truncated list ends inside the flat
/// zero the returned *set* differs too, not just its order. Keeping the raw
/// similarity as the second key makes the ranking monotone in similarity
/// throughout, which is what "equal importance keeps its similarity order"
/// means once the clamp is in play.
///
/// A candidate set with no similarity of its own (a flat substring match) passes
/// `1.0` for every member, so the second key ties and the id decides.
pub fn rank_key(similarity: f32, importance: f32, id: &str) -> (f32, f32, &str) {
    (
        importance_weighted_score(similarity, importance),
        similarity,
        id,
    )
}

/// A pointer back to the raw source that produced a schema or summary entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceRef {
    pub raw_uri: String,
    pub range: Option<String>,
    pub extractor_version: String,
    pub extracted_at: DateTime<Utc>,
}

impl SourceRef {
    pub fn new(raw_uri: impl Into<String>, extractor_version: impl Into<String>) -> Self {
        Self {
            raw_uri: raw_uri.into(),
            range: None,
            extractor_version: extractor_version.into(),
            extracted_at: Utc::now(),
        }
    }

    pub fn with_range(mut self, range: impl Into<String>) -> Self {
        self.range = Some(range.into());
        self
    }
}

/// Namespace a memory read or write lands in when the caller carries none.
///
/// Every raw/schema/summary key is scoped by namespace, so a caller that has no
/// workspace of its own still needs one name to read and write under, and the
/// two sides must agree on it or a write becomes invisible to the read that was
/// meant to find it. Defined once here rather than spelled out at each entry
/// point: the gateway, the in-process tools and the ingest paths all address the
/// same store.
pub const DEFAULT_MEMORY_NAMESPACE: &str = "default";

/// The reference a caller carries to reach an archived raw source: the namespace
/// it landed in and its id.
///
/// One spelling for every producer of the reference, so a reader parsing it back
/// matches one grammar rather than one per writer.
pub fn artifact_uri(namespace: &str, id: &str) -> String {
    format!("artifact://{namespace}/{id}")
}

/// The raw id of one tool output: the task, the turn it was produced in, and its
/// position among that turn's tool calls.
///
/// Both numbers are zero-padded so lexicographic order is chronological -- a
/// prefix listing of a task's outputs reads back in the order they happened,
/// which is what "everything this task saw, in order" asks for. The two numbers
/// travel inside one segment rather than as two path segments because a
/// separator in a raw id turns into a directory in the object store and the
/// source silently vanishes from every listing that would have counted it (see
/// [`raw_id_key_error`]).
///
/// The task id is folded into that one segment too, and the fold has to be a
/// substitution rather than the id itself: a task id is free text by the time it
/// reaches here, and one carrying a separator would make the whole id
/// unaddressable -- the write would be refused at the API and the output would
/// be lost with only a failure count to show for it. The characters
/// [`raw_id_key_error`] refuses are replaced instead of the write being skipped,
/// because a lossy id is still a reachable archive and a refused one is not.
pub fn tool_output_raw_id(task_id: &str, turn: u32, frame: u32) -> String {
    let key: String = task_id
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' => '_',
            other => other,
        })
        .collect();
    let key = if key.is_empty() { "task" } else { &key };
    format!("{key}-{turn:04}-{frame:04}")
}

/// The env name a deployment sets to name the platform memory API base.
///
/// Business pods hold no credentials and reach the platform through the gateway;
/// this is where the gateway is, the memory-API analogue of the code-platform
/// base the landing channel reads. Named once here so the writer and the
/// deployment that sets it cannot drift apart.
pub const MEMORY_API_BASE_ENV: &str = "COGNEVA_MEMORY_API_BASE";

/// The path the memory API archives a raw source at, relative to its base.
pub const MEMORY_INGEST_PATH: &str = "/api/v1/memory/ingest";

/// Layer 0 — Raw Sources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawSource {
    pub id: String,
    pub namespace: String,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub archived_at: DateTime<Utc>,
}

/// Why `id` cannot be used as a raw key, or `None` when it can.
///
/// A raw id is not free text: every [`MemoryBackend`] turns it into an object
/// key, and the composite backend's file store maps each separator in that key
/// onto a directory. An id carrying a separator therefore does not fail — it
/// becomes a subtree. What the store then reports back is a different key than
/// the caller supplied, and where the separator lands exactly where a file was
/// expected the write leaves an empty directory and no object at all. Nothing
/// reports that: `list_raw` walks the subtree and returns files only, so the
/// raw disappears from every listing that would have counted it.
///
/// Only the silently-corrupting shapes are rejected here. An over-long id still
/// fails loudly at the store, which is a reportable error rather than a
/// vanished key.
pub fn raw_id_key_error(id: &str) -> Option<&'static str> {
    if id.is_empty() {
        return Some("must not be empty");
    }
    if id.contains('/') || id.contains('\\') {
        return Some("must not contain a path separator");
    }
    if id.contains('\0') {
        return Some("must not contain a NUL byte");
    }
    if id == "." || id == ".." {
        return Some("must not be a dot path segment");
    }
    None
}

impl RawSource {
    pub fn new(
        id: impl Into<String>,
        namespace: impl Into<String>,
        content_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            namespace: namespace.into(),
            content_type: content_type.into(),
            payload,
            tags: Vec::new(),
            created_at: now,
            archived_at: now,
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = created_at;
        self
    }
}

/// Direction of a graph relationship traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationDirection {
    From,
    To,
    Both,
}

/// Types of structured entries in the Schema layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaKind {
    Entity,
    Relation,
    Event,
    Sentiment,
    Learning,
    ErrorPattern,
    Custom,
    SkillEffectiveness,
    ModeDecision,
    DiscoveryResult,
}

impl SchemaKind {
    /// The canonical lowercase spelling, shared by the identity key and by
    /// every store's `kind` column.
    pub fn as_str(self) -> &'static str {
        match self {
            SchemaKind::Entity => "entity",
            SchemaKind::Relation => "relation",
            SchemaKind::Event => "event",
            SchemaKind::Sentiment => "sentiment",
            SchemaKind::Learning => "learning",
            SchemaKind::ErrorPattern => "error_pattern",
            SchemaKind::Custom => "custom",
            SchemaKind::SkillEffectiveness => "skill_effectiveness",
            SchemaKind::ModeDecision => "mode_decision",
            SchemaKind::DiscoveryResult => "discovery_result",
        }
    }
}

/// Layer 1 — Schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaEntry {
    pub id: String,
    pub namespace: String,
    pub kind: SchemaKind,
    pub name: String,
    pub key: String,
    pub properties: serde_json::Value,
    pub source_ref: SourceRef,
    /// Every raw source this entry has been extracted from, in first-seen
    /// order. `source_ref` names the most recent one; this list is what
    /// membership queries read.
    ///
    /// One entity outlives any single raw that mentions it: two raws reporting
    /// the same thing are two observations of one fact, not two facts. Keeping
    /// the whole list is what lets a row be attributed to all of its origins
    /// and lets removing one origin leave the row intact.
    #[serde(default)]
    pub observed_by: Vec<String>,
    /// How many times the fact has been observed — summed over the
    /// observations recorded in each raw, not over the number of raws.
    #[serde(default = "one")]
    pub occurrences: u64,
    /// Span of the observation history; `extracted_at` is when the newest
    /// observation was read, these two are when the fact was first and last
    /// seen at all.
    #[serde(default = "Utc::now")]
    pub first_seen: DateTime<Utc>,
    #[serde(default = "Utc::now")]
    pub last_seen: DateTime<Utc>,
    pub confidence: f32,
    pub importance: f32,
    pub extracted_at: DateTime<Utc>,
}

fn one() -> u64 {
    1
}

impl SchemaEntry {
    pub fn new(
        id: impl Into<String>,
        namespace: impl Into<String>,
        kind: SchemaKind,
        name: impl Into<String>,
        key: impl Into<String>,
        source_ref: SourceRef,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            namespace: namespace.into(),
            kind,
            name: name.into(),
            key: key.into(),
            properties: serde_json::Value::Object(Default::default()),
            observed_by: vec![source_ref.raw_uri.clone()],
            source_ref,
            occurrences: 1,
            first_seen: now,
            last_seen: now,
            confidence: 1.0,
            importance: 0.5,
            extracted_at: now,
        }
    }

    /// Build an entry whose identity comes from what it is rather than from
    /// where it was seen, so that the same fact reported by two sources names
    /// one row instead of two.
    pub fn identified(
        namespace: impl Into<String>,
        kind: SchemaKind,
        name: impl Into<String>,
        key: impl Into<String>,
        source_ref: SourceRef,
    ) -> Self {
        let namespace = namespace.into();
        let key = key.into();
        let id = schema_entry_id(&namespace, kind, &key);
        Self::new(id, namespace, kind, name, key, source_ref)
    }

    pub fn with_properties(mut self, properties: serde_json::Value) -> Self {
        self.properties = properties;
        self
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    pub fn with_importance(mut self, importance: f32) -> Self {
        self.importance = importance.clamp(0.0, 1.0);
        self
    }

    /// Whether `raw_uri` is one of the sources this entry was extracted from.
    ///
    /// `source_ref` is always one of them, so an entry denormalized before
    /// `observed_by` existed still answers for its own provenance.
    pub fn observed_from(&self, raw_uri: &str) -> bool {
        self.source_ref.raw_uri == raw_uri || self.observed_by.iter().any(|u| u == raw_uri)
    }

    /// Drop `raw_uri` from this entry's origins.
    ///
    /// `None` means it was the only origin: the entry then describes something
    /// no surviving raw carries, so the store should remove the row rather
    /// than keep a fact with nothing behind it. `source_ref` names one of the
    /// origins, so it moves to a surviving one instead of pointing at the raw
    /// that was just forgotten.
    pub fn without_observer(mut self, raw_uri: &str) -> Option<Self> {
        if !self.observed_from(raw_uri) {
            return Some(self);
        }
        if !self
            .observed_by
            .iter()
            .any(|u| u == &self.source_ref.raw_uri)
        {
            self.observed_by.push(self.source_ref.raw_uri.clone());
        }
        self.observed_by.retain(|u| u != raw_uri);
        self.source_ref.raw_uri = self.observed_by.first()?.clone();
        Some(self)
    }
}

/// The identity of a schema entry, derived from what it is.
///
/// Ids scoped to the raw that produced them make one fact as many rows as
/// there are raws mentioning it, so the row count tracks ingestion volume
/// rather than how much the system knows — the same entity observed by a
/// thousand conversations is a thousand rows holding the same fact, and every
/// per-entity query has to reconstruct the union itself.
///
/// The digest rather than the plain concatenation: namespace, kind and key are
/// not self-delimiting, so `("ab", "c")` and `("a", "bc")` would render to the
/// same key, and a fixed-width id keeps the primary key bounded whatever a key
/// grows to.
pub fn schema_entry_id(namespace: &str, kind: SchemaKind, key: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(namespace.as_bytes());
    hasher.update(&[0]);
    hasher.update(kind.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(key.as_bytes());
    format!("schema-{}", hasher.finalize().to_hex())
}

/// Fold a fresh observation of a fact into the row already held for it.
///
/// Two sources reporting the same entity derive the same id, so the second
/// store is a merge rather than a second row. What the newest observation
/// carries as a fact — name, provenance, confidence, importance — wins,
/// because it is the more recent reading of the same thing.
///
/// Properties are the exception: they are folded key by key, newest wins per
/// key, the way [`SchemaBackend::update_schema`] already folds them. Two raws
/// describing one entity usually describe different facets of it, and letting
/// the later writer's object stand alone would drop every facet only the
/// earlier one recorded.
///
/// What the row carries as history accumulates: the observing sources, the
/// occurrence count and the seen-at range. Dropping the earlier observers
/// would lose the provenance that decides whether the row may be deleted at
/// all, and would make the count drop when nothing about the world changed.
///
/// Merging an entry with itself is a no-op, which is what makes a
/// read-then-write store safe to run on a row it just inserted.
pub fn merge_schema_observation(stored: &SchemaEntry, observed: &SchemaEntry) -> SchemaEntry {
    let mut merged = observed.clone();

    if let (Some(mut into), Some(from)) = (
        stored.properties.as_object().cloned(),
        observed.properties.as_object(),
    ) {
        for (k, v) in from {
            into.insert(k.clone(), v.clone());
        }
        merged.properties = serde_json::Value::Object(into);
    }

    merged.observed_by = stored.observed_by.clone();
    // `source_ref` is an origin too, whether or not the list still names it.
    for uri in observed
        .observed_by
        .iter()
        .chain([&stored.source_ref.raw_uri, &observed.source_ref.raw_uri])
    {
        if !merged.observed_by.iter().any(|u| u == uri) {
            merged.observed_by.push(uri.clone());
        }
    }
    // Count once per observing source: a re-extraction of a raw the entry
    // already credits is the same observation seen again, not a new one.
    let new_source = !stored.observed_from(&observed.source_ref.raw_uri);
    merged.occurrences = if new_source {
        stored.occurrences.saturating_add(observed.occurrences)
    } else {
        stored.occurrences
    };
    merged.first_seen = stored.first_seen.min(observed.first_seen);
    merged.last_seen = stored.last_seen.max(observed.last_seen);
    merged
}

/// `embedding_model` carried by a summary whose vector is absent.
///
/// An absent vector is stored as an empty `Vec`, never as a run of zeros: a
/// zero vector is a well-formed vector that scores 0.0 against every query, so
/// a collection full of them answers every search with an arbitrary tie at
/// score 0.0 — a ranked-looking result set that carries no ranking. Absence has
/// to be representable, or the store ends up asserting something it cannot know.
pub const NO_EMBEDDING_MODEL: &str = "";

/// Layer 2 — Summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SummaryEntry {
    pub id: String,
    pub namespace: String,
    pub text: String,
    /// Empty when no embedder could produce a vector; see [`NO_EMBEDDING_MODEL`].
    pub embedding: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sparse_embedding: Option<SparseEmbedding>,
    pub embedding_model: String,
    pub source_ref: SourceRef,
    pub related_schema_ids: Vec<String>,
    pub confidence: f32,
    pub importance: f32,
    pub generated_at: DateTime<Utc>,
}

impl SummaryEntry {
    pub fn new(
        id: impl Into<String>,
        namespace: impl Into<String>,
        text: impl Into<String>,
        embedding: Vec<f32>,
        embedding_model: impl Into<String>,
        source_ref: SourceRef,
    ) -> Self {
        Self {
            id: id.into(),
            namespace: namespace.into(),
            text: text.into(),
            embedding,
            sparse_embedding: None,
            embedding_model: embedding_model.into(),
            source_ref,
            related_schema_ids: Vec::new(),
            confidence: 1.0,
            importance: 0.5,
            generated_at: Utc::now(),
        }
    }

    pub fn with_sparse_embedding(mut self, sparse: SparseEmbedding) -> Self {
        self.sparse_embedding = Some(sparse);
        self
    }

    pub fn with_related_schema_ids(mut self, ids: Vec<String>) -> Self {
        self.related_schema_ids = ids;
        self
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    pub fn with_importance(mut self, importance: f32) -> Self {
        self.importance = importance.clamp(0.0, 1.0);
        self
    }
}

/// The result of a schema search query.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaSearchResult {
    pub entry: SchemaEntry,
    pub score: f32,
}

/// The type of match that produced a search result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MatchType {
    #[default]
    Dense,
    Sparse,
    Hybrid,
    Rerank,
}

/// The result of a summary (semantic) search query.
#[derive(Debug, Clone, PartialEq)]
pub struct SummarySearchResult {
    pub entry: SummaryEntry,
    /// The ranking score, in `0.0..=1.0`: similarity scaled by the entry's
    /// importance via [`importance_weighted_score`]. Not the raw cosine — an
    /// entry's score is what it was *ranked* by, so the returned order and the
    /// scores always agree.
    pub score: f32,
    pub match_type: MatchType,
    pub highlights: Vec<String>,
}

impl SummarySearchResult {
    pub fn new(entry: SummaryEntry, score: f32) -> Self {
        Self {
            entry,
            score,
            match_type: MatchType::default(),
            highlights: Vec::new(),
        }
    }

    pub fn with_match_type(mut self, match_type: MatchType) -> Self {
        self.match_type = match_type;
        self
    }

    pub fn with_highlights(mut self, highlights: Vec<String>) -> Self {
        self.highlights = highlights;
        self
    }
}

/// Report produced by a memory-decay pass.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DecayReport {
    pub namespace: String,
    pub entries_decayed: usize,
    pub entries_archived: usize,
}

/// A unified search result that can represent any memory layer entry.
#[derive(Debug, Clone, PartialEq)]
pub enum UnifiedSearchResult {
    Schema(SchemaSearchResult),
    Summary(SummarySearchResult),
}

/// Counters for memory-backend operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MemoryMetrics {
    pub raw_archived: u64,
    pub raw_retrieved: u64,
    pub schema_stored: u64,
    pub schema_updated: u64,
    pub schema_searched: u64,
    pub summary_stored: u64,
    pub summary_updated: u64,
    pub summary_searched: u64,
}

// ─── MemoryBackend trait ─────────────────────────────────────────────

/// Unified backend for the three-layer permanent memory architecture.
#[async_trait]
pub trait MemoryBackend: Send + Sync {
    // ── Layer 0: Raw Sources ────────────────────────────────────────────
    async fn archive_raw(&self, source: &RawSource) -> SFResult<String>;
    async fn get_raw(&self, namespace: &str, id: &str) -> SFResult<Option<RawSource>>;
    async fn list_raw(
        &self,
        namespace: &str,
        content_type_prefix: Option<&str>,
    ) -> SFResult<Vec<String>>;
    async fn delete_raw(&self, namespace: &str, id: &str) -> SFResult<()>;

    // ── Layer 1: Schema ─────────────────────────────────────────────────
    async fn store_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;
    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>>;
    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>>;
    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>>;
    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>>;
    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()>;
    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>>;
    async fn update_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;

    // ── Layer 2: Summary ────────────────────────────────────────────────
    async fn store_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;
    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>>;
    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>>;
    async fn search_summary_hybrid(
        &self,
        namespace: &str,
        query_dense: &[f32],
        query_sparse: Option<&SparseEmbedding>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let _ = query_sparse;
        self.search_summary(namespace, query_dense, top_k, time_range)
            .await
    }
    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>>;
    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>>;
    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()>;
    async fn update_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;
    fn metrics(&self) -> MemoryMetrics;
    async fn health_check(&self) -> SFResult<()>;
    async fn search_all(
        &self,
        namespace: &str,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<UnifiedSearchResult>>;
    async fn ingest_explicit(
        &self,
        namespace: &str,
        text: &str,
        importance: f32,
        tags: Vec<String>,
    ) -> SFResult<()>;
    async fn forget(&self, namespace: &str, id: &str) -> SFResult<()>;
    async fn decay(
        &self,
        namespace: &str,
        age_threshold_secs: u64,
        importance_threshold: f32,
    ) -> SFResult<DecayReport>;
}

/// Trait for ingesting raw sources into structured schema and summary layers.
/// Implementations may use rule-based extraction, LLM-assisted parsing,
/// or a hybrid approach. The gateway consumes this via `PluginContext`
/// so it never depends directly on `cog-memory` concrete types.
#[async_trait]
pub trait MemoryIngestor: Send + Sync {
    /// Run extraction against a raw source and return both layers.
    async fn ingest(&self, source: &RawSource) -> SFResult<(Vec<SchemaEntry>, SummaryEntry)>;
}

/// Trait for extracting structured schema and summary from raw sources.
/// This is the lower-level trait used by `MemoryIngestor` implementations.
/// The gateway consumes this via `PluginContext`
/// so it never depends directly on `cog-memory` concrete types.
#[async_trait]
pub trait MemoryExtractor: Send + Sync {
    /// Extract structured schema entries from a raw source.
    async fn extract_schema(&self, source: &RawSource) -> SFResult<Vec<SchemaEntry>>;

    /// Generate a semantic summary entry from a raw source.
    async fn generate_summary(&self, source: &RawSource) -> SFResult<SummaryEntry>;

    /// Extract both layers in one pass.
    ///
    /// A caller that needs both layers calls this instead of the two methods
    /// above. The default runs them one after the other, which is correct but
    /// reads the source twice: when the extraction is a model call, the source
    /// — typically the bulk of the prompt — is paid for on every layer.
    /// Implementations whose two layers can share one call must override this
    /// so the source is sent once; the default keeps an extractor that has no
    /// such call (a rule matcher, a test double) working unchanged.
    async fn extract_all(&self, source: &RawSource) -> SFResult<(Vec<SchemaEntry>, SummaryEntry)> {
        let schema = self.extract_schema(source).await?;
        let summary = self.generate_summary(source).await?;
        Ok((schema, summary))
    }
}

// ─── SchemaBackend / SummaryBackend (migrated from cog-memory) ───────────

/// Pluggable backend for the Schema layer (Layer 1) of permanent memory.
/// Implementations may store entries in PostgreSQL, TDSQL-PG, an in-memory
/// HashMap, or any other relational/graph database.
#[async_trait]
pub trait SchemaBackend: Send + Sync {
    /// Store one observation of a schema entry.
    ///
    /// The entry's id names a fact, not a row per writer, so a store against
    /// an id that is already held folds the observation into it by
    /// [`merge_schema_observation`] rather than replacing what is there.
    async fn store_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;

    /// Retrieve a schema entry by id.
    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>>;

    /// Search schema entries by name/key substring.
    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>>;

    /// Find schema entries that point to a given raw source id.
    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>>;

    /// List all schema entries.
    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>>;

    /// Delete a schema entry by id.
    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()>;

    /// Stop attributing an entry to one of the raw sources it came from,
    /// deleting it only once no source observes it any more.
    ///
    /// A content-identified entry can be reached from every raw that mentions
    /// it, so deleting it because one of them was forgotten would erase a fact
    /// the other raws still carry — and the next reconciliation pass over those
    /// raws would extract it straight back. Remaining observers keep it alive;
    /// the last one out deletes it.
    ///
    /// The default read-edit-write is enough for a store whose entries only
    /// this process touches; a store fronting a shared table overrides it to
    /// make the removal atomic.
    async fn forget_schema_source(&self, namespace: &str, id: &str, raw_uri: &str) -> SFResult<()> {
        let Some(entry) = self.get_schema(namespace, id).await? else {
            return Ok(());
        };
        match entry.without_observer(raw_uri) {
            Some(entry) => self.update_schema(namespace, &entry).await,
            None => self.delete_schema(namespace, id).await,
        }
    }

    /// Query relation entries by direction and optional relation type.
    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>>;

    /// Update a schema entry, merging properties if the key already exists.
    async fn update_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;
}

/// Pluggable backend for the Summary layer (Layer 2) of permanent memory.
/// Implementations may store summaries (and their embeddings) in LanceDB,
/// Tencent VDB, or any [`crate::VectorBackend`] wrapped by a vector adapter.
#[async_trait]
pub trait SummaryBackend: Send + Sync {
    /// Store a summary entry, indexing its embedding for similarity search.
    async fn store_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;

    /// Retrieve a summary entry by id.
    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>>;

    /// Semantic search over summaries using a query embedding vector.
    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>>;

    /// Hybrid search over summaries using both dense and sparse vectors.
    /// Default implementation falls back to dense-only [`Self::search_summary`].
    async fn search_summary_hybrid(
        &self,
        namespace: &str,
        query_dense: &[f32],
        query_sparse: Option<&SparseEmbedding>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let _ = query_sparse;
        self.search_summary(namespace, query_dense, top_k, time_range)
            .await
    }

    /// Find summary entries that point to a given raw source id.
    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>>;

    /// List all summary entries.
    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>>;

    /// Delete a summary entry by id.
    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()>;

    /// Update a summary entry, overwriting if the id already exists.
    async fn update_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scale has to be usable at both ends and in the middle, because a
    /// model rating an item 1 and a producer asserting a low prior both go
    /// through here.
    #[test]
    fn a_rating_maps_onto_the_shared_importance_scale() {
        assert_eq!(importance_from_rating(1), 0.1);
        assert_eq!(importance_from_rating(10), 1.0);
        assert_eq!(importance_from_rating(5), 0.5);
    }

    /// Out-of-range ratings are clamped rather than dropped: the number is only
    /// ever used to order entries, so losing an entry over it would cost more
    /// than the ordering is worth.
    #[test]
    fn an_out_of_range_rating_is_clamped_into_the_scale() {
        assert_eq!(importance_from_rating(0), 0.1);
        assert_eq!(importance_from_rating(200), 1.0);
    }

    #[test]
    fn equal_importance_keeps_the_similarity_order() {
        // Importance is a pure multiplier, so at a fixed importance it can only
        // rescale — never reorder — the similarity ranking.
        assert!(importance_weighted_score(0.9, 0.5) > importance_weighted_score(0.8, 0.5));
        assert!(importance_weighted_score(0.8, 0.5) > importance_weighted_score(0.7, 0.5));
    }

    #[test]
    fn importance_breaks_a_similarity_tie_and_demotes_a_near_match() {
        // Same similarity, different importance: the higher one wins.
        assert!(importance_weighted_score(0.9, 0.9) > importance_weighted_score(0.9, 0.5));
        // The point of the weight: a more-similar but low-value entry sinks
        // below a less-similar but high-value one.
        assert!(importance_weighted_score(0.95, 0.1) < importance_weighted_score(0.80, 1.0));
    }

    #[test]
    fn importance_cannot_promote_an_anti_correlated_entry() {
        // A negative cosine says "unrelated"; scaling it by importance must not
        // lift it toward the related half. The clamp maps it to 0.0 for every
        // importance, so it can never outrank a genuinely related entry.
        assert_eq!(importance_weighted_score(-0.3, 1.0), 0.0);
        assert_eq!(importance_weighted_score(-0.3, 0.1), 0.0);
        assert!(importance_weighted_score(-0.3, 1.0) < importance_weighted_score(0.05, 0.1));
    }

    /// The score alone is not a total order: everything at or below zero scores
    /// the same, so a sort on it leaves that half in whatever order it arrived
    /// in. The key has to keep the similarity underneath it.
    #[test]
    fn the_rank_key_orders_the_half_the_score_flattens() {
        let q = rank_key(-0.1, 0.5, "b");
        let p = rank_key(-0.9, 0.5, "a");
        assert_eq!(q.0, p.0, "both are on the flat zero the clamp produces");
        assert!(q > p, "the less anti-correlated entry still ranks first");
    }

    /// Two entries the score *and* the similarity leave tied, which is the state
    /// every flat substring match is in, still come out in a fixed order.
    #[test]
    fn the_rank_key_breaks_a_full_tie_on_the_id() {
        assert!(rank_key(1.0, 0.5, "a") < rank_key(1.0, 0.5, "b"));
    }

    /// A zero weight is the bottom of the ranking, not a removal: the entry is
    /// still returned, still carrying its own similarity.
    #[test]
    fn a_zero_weight_scores_the_floor_and_still_ranks_below_nothing_else() {
        assert_eq!(importance_weighted_score(1.0, 0.0), 0.0);
        assert!(importance_weighted_score(0.05, 0.1) > importance_weighted_score(1.0, 0.0));
    }

    #[test]
    fn slug_ids_are_valid_raw_keys() {
        for id in [
            "e2e-conv-1",
            "agent-squad_self-signal-alert-134eb9f3ab",
            "agent-agent-squad_decompose-Fix_github_issue__9-1789375717930-089b53ac",
            " 摘要\n\n`",
            "任务：修复登录",
        ] {
            assert_eq!(raw_id_key_error(id), None, "rejected {id:?}");
        }
    }

    #[test]
    fn separators_are_rejected_because_they_fork_the_key() {
        for id in [
            "http://example.com/x",
            "a/b",
            "report/2026-09-13",
            "back\\slash",
        ] {
            assert_eq!(
                raw_id_key_error(id),
                Some("must not contain a path separator"),
                "accepted {id:?}"
            );
        }
    }

    #[test]
    fn empty_dot_and_nul_ids_are_rejected() {
        assert_eq!(raw_id_key_error(""), Some("must not be empty"));
        assert_eq!(
            raw_id_key_error("."),
            Some("must not be a dot path segment")
        );
        assert_eq!(
            raw_id_key_error(".."),
            Some("must not be a dot path segment")
        );
        assert_eq!(
            raw_id_key_error("a\0b"),
            Some("must not contain a NUL byte")
        );
    }

    #[test]
    fn a_long_id_is_left_to_the_store_to_reject_loudly() {
        let long = "x".repeat(400);
        assert_eq!(raw_id_key_error(&long), None);
    }

    fn entry(namespace: &str, raw: &str, key: &str) -> SchemaEntry {
        SchemaEntry::identified(
            namespace,
            SchemaKind::Entity,
            key,
            key,
            SourceRef::new(format!("memory://{raw}"), "test/v1"),
        )
    }

    /// The identity depends on what the entry is, so the row count measures
    /// knowledge rather than how much was ingested.
    #[test]
    fn identity_comes_from_the_content_not_the_source() {
        assert_eq!(
            entry("ns", "raw-a", "gateway").id,
            entry("ns", "raw-b", "gateway").id
        );
        assert_ne!(
            entry("ns", "raw-a", "gateway").id,
            entry("ns", "raw-a", "proxy").id
        );
        assert_ne!(
            entry("ns", "raw-a", "gateway").id,
            SchemaEntry::identified(
                "other",
                SchemaKind::Entity,
                "gateway",
                "gateway",
                SourceRef::new("memory://raw-a", "test/v1"),
            )
            .id
        );
        assert_ne!(
            entry("ns", "raw-a", "gateway").id,
            SchemaEntry::identified(
                "ns",
                SchemaKind::Relation,
                "gateway",
                "gateway",
                SourceRef::new("memory://raw-a", "test/v1"),
            )
            .id
        );
    }

    /// The parts are hashed with separators between them: concatenated raw, a
    /// namespace ending in the kind and a key starting with the rest of it
    /// would render to the same bytes as the pair that splits them elsewhere,
    /// and two unrelated facts would share a primary key.
    #[test]
    fn the_parts_cannot_trade_characters_across_the_boundary() {
        let split_late = SchemaEntry::identified(
            "x",
            SchemaKind::Entity,
            "k",
            "entityy",
            SourceRef::new("memory://raw-a", "test/v1"),
        );
        let split_early = SchemaEntry::identified(
            "xentity",
            SchemaKind::Entity,
            "k",
            "y",
            SourceRef::new("memory://raw-a", "test/v1"),
        );
        assert_ne!(split_late.id, split_early.id);
    }

    /// Two sources reporting one fact are two observations of it: both must
    /// stay readable as origins, and the count must say two.
    #[test]
    fn a_second_source_joins_the_row_instead_of_replacing_it() {
        let stored = entry("ns", "raw-a", "gateway");
        let observed = entry("ns", "raw-b", "gateway");

        let merged = merge_schema_observation(&stored, &observed);
        assert_eq!(merged.id, stored.id);
        assert_eq!(merged.observed_by.len(), 2);
        assert!(merged.observed_from("memory://raw-a"));
        assert!(merged.observed_from("memory://raw-b"));
        assert_eq!(merged.occurrences, 2);
    }

    /// A re-extraction of a raw the entry already credits is the same
    /// observation seen again, so neither the origin list nor the count moves
    /// — without this the reconciliation pass inflates both on every sweep.
    #[test]
    fn re_observing_from_a_known_source_changes_nothing() {
        let stored = entry("ns", "raw-a", "gateway");
        let merged = merge_schema_observation(&stored, &stored);
        assert_eq!(merged, stored);
    }

    /// The count is of observations, so repeats inside one raw survive the
    /// merge while the re-extraction above does not add to them.
    #[test]
    fn the_count_carries_repeats_within_one_source() {
        let mut stored = entry("ns", "raw-a", "deploy");
        stored.occurrences = 3;

        let re_extracted = entry("ns", "raw-a", "deploy");
        assert_eq!(
            merge_schema_observation(&stored, &re_extracted).occurrences,
            3
        );

        let from_elsewhere = entry("ns", "raw-b", "deploy");
        assert_eq!(
            merge_schema_observation(&stored, &from_elsewhere).occurrences,
            4
        );
    }

    /// Two sources describing one entity usually describe different facets of
    /// it, so the facets fold rather than the later writer's object replacing
    /// the earlier one's.
    #[test]
    fn properties_fold_key_by_key_across_sources() {
        let stored = entry("ns", "raw-a", "gateway")
            .with_properties(serde_json::json!({"category": "infra", "tier": "edge"}));
        let observed = entry("ns", "raw-b", "gateway")
            .with_properties(serde_json::json!({"tier": "core", "owner": "platform"}));

        let merged = merge_schema_observation(&stored, &observed);
        assert_eq!(merged.properties["category"], "infra");
        assert_eq!(merged.properties["tier"], "core", "the newer reading wins");
        assert_eq!(merged.properties["owner"], "platform");
    }

    /// Forgetting one origin withdraws it without erasing an entry the other
    /// origins still carry; the last origin out leaves nothing behind.
    #[test]
    fn an_entry_outlives_the_source_being_forgotten() {
        let mut merged = entry("ns", "raw-a", "gateway");
        merged = merge_schema_observation(&merged, &entry("ns", "raw-b", "gateway"));

        let kept = merged
            .clone()
            .without_observer("memory://raw-a")
            .expect("an entry another source still reports must survive");
        assert!(!kept.observed_from("memory://raw-a"));
        assert!(kept.observed_from("memory://raw-b"));
        assert_eq!(kept.source_ref.raw_uri, "memory://raw-b");

        let last = entry("ns", "raw-a", "gateway");
        assert!(last.without_observer("memory://raw-a").is_none());
    }

    /// An entry denormalized before the observer list existed names its origin
    /// only in `source_ref`; withdrawing that origin still has to be able to
    /// retire it.
    #[test]
    fn an_entry_with_only_a_source_ref_still_knows_when_it_is_orphaned() {
        let mut legacy = entry("ns", "raw-a", "gateway");
        legacy.observed_by.clear();
        assert!(legacy.observed_from("memory://raw-a"));
        assert!(legacy.without_observer("memory://raw-a").is_none());
    }
}
