//! Writing entries a caller has already judged.
//!
//! Extraction used to be one function that asked a model and stored what came
//! back. The judgment ("what here is worth remembering") and the mechanics
//! (ids, attribution, embeddings, dedupe) travelled together, so the only way
//! to get entries stored was to call that function — which is why memory looked
//! like an executor of its own rather than a place any agent could write to.
//!
//! This module keeps the mechanics and takes the judgment as input. Any caller
//! that can read the material and decide what matters — an agent on the normal
//! loop, a person through the API, a future pipeline — writes the same payload
//! the extraction prompt asks for, names itself in `judged_by`, and gets rows
//! built exactly as the model-driven path builds them.

use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{
    EmbeddingProvider, JudgedWriteReport, MemoryBackend, MemoryEntryWriter, SFError, SFResult,
    SourceRef,
};

use crate::extractor::{schema_entries, summary_entry, SchemaExtraction, SummaryExtraction};

/// Recorded as the extractor version when the caller names nobody.
///
/// A write with no judge is still a write, and saying so is more useful than
/// refusing it: the rows stay searchable and the absence is visible on them.
pub const JUDGED_BY_ANONYMOUS: &str = "judged/v1";

/// Stores judged entries with the same builders the model-driven path uses.
pub struct JudgedEntryWriter {
    backend: Arc<dyn MemoryBackend>,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
}

impl JudgedEntryWriter {
    pub fn new(backend: Arc<dyn MemoryBackend>) -> Self {
        Self {
            backend,
            embedder: None,
        }
    }

    /// Give the writer the vector layer, so a judged summary lands searchable
    /// instead of waiting for a repair pass to embed it later.
    pub fn with_embedder(mut self, embedder: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
    }
}

/// The payload a judge sends: the three schema lists, an optional summary, and
/// the two pieces of provenance the judge alone knows.
#[derive(Debug, serde::Deserialize)]
struct JudgedPayload {
    /// The material this judgment is about. Rows carry it as their source, so
    /// they can be attributed to it and forgotten with it.
    #[serde(default)]
    raw_id: Option<String>,
    /// Who made the judgment. Lands on every row as its extractor version,
    /// which is what lets a reader answer "who decided this was worth keeping".
    #[serde(default)]
    judged_by: Option<String>,
    #[serde(flatten)]
    schema: SchemaExtraction,
    #[serde(default)]
    summary: Option<SummaryExtraction>,
}

#[async_trait]
impl MemoryEntryWriter for JudgedEntryWriter {
    async fn write_judged(
        &self,
        namespace: &str,
        judged: &serde_json::Value,
    ) -> SFResult<JudgedWriteReport> {
        let payload: JudgedPayload = serde_json::from_value(judged.clone())
            .map_err(|e| SFError::Validation(format!("judged payload is not usable: {e}")))?;

        let judged_by = payload
            .judged_by
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(JUDGED_BY_ANONYMOUS)
            .to_string();

        let raw_id = payload
            .raw_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(id) = raw_id {
            if let Some(reason) = cog_core::raw_id_key_error(id) {
                return Err(SFError::Validation(format!(
                    "raw_id {id:?} is not usable: {reason}"
                )));
            }
        }

        let has_schema = !payload.schema.entities.is_empty()
            || !payload.schema.relations.is_empty()
            || !payload.schema.events.is_empty();
        if !has_schema && payload.summary.is_none() {
            // A write that stores nothing must not report success: the caller
            // would stop looking for the entries it thinks it stored.
            return Err(SFError::Validation(
                "judged payload carried no entities, relations, events or summary".into(),
            ));
        }

        let raw_uri = match raw_id {
            Some(id) => format!("memory://{id}"),
            // No source material named: the row still needs an origin, and a
            // fresh one per write is what keeps two unattributed judgments from
            // collapsing into each other's ids.
            None => format!("memory://judged/{}", uuid::Uuid::new_v4()),
        };
        let source_ref = SourceRef::new(raw_uri, judged_by);

        let entries = schema_entries(namespace, source_ref.clone(), payload.schema);
        for entry in &entries {
            self.backend.store_schema(namespace, entry).await?;
        }

        let summary_written = match payload.summary {
            Some(extraction) => {
                let entry =
                    summary_entry(self.embedder.as_ref(), namespace, source_ref, extraction)
                        .await?;
                self.backend.store_summary(namespace, &entry).await?;
                true
            }
            None => false,
        };

        Ok(JudgedWriteReport {
            schema_written: entries.len(),
            summary_written,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::MemoryBackend;
    use serde_json::json;
    use std::sync::Arc;

    /// The crate's own in-memory backend: the writer's job is what it asks of
    /// a backend, so the test asserts against the rows a real backend kept,
    /// not against a mock's call log.
    fn backend() -> Arc<crate::MemoryMemoryBackend> {
        Arc::new(crate::MemoryMemoryBackend::new())
    }

    #[tokio::test]
    async fn a_judged_write_lands_with_the_judge_named_on_it() {
        let backend = backend();
        let writer = JudgedEntryWriter::new(backend.clone());

        let report = writer
            .write_judged(
                "default",
                &json!({
                    "raw_id": "conv-1",
                    "judged_by": "agent:luna-1822ab0e",
                    "entities": [{"name": "Cogneva", "properties": {"role": "platform"}}],
                    "summary": {"text": "The team shipped the memory write-back API.", "importance": 7}
                }),
            )
            .await
            .unwrap();

        assert_eq!(report.schema_written, 1);
        assert!(report.summary_written);

        let rows = backend.list_schema("default").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_ref.extractor_version, "agent:luna-1822ab0e");
        assert_eq!(rows[0].source_ref.raw_uri, "memory://conv-1");

        let summaries = backend.list_summary("default").await.unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries[0].source_ref.extractor_version,
            "agent:luna-1822ab0e"
        );
    }

    #[tokio::test]
    async fn a_write_without_a_judge_is_marked_anonymous_rather_than_refused() {
        let backend = backend();
        let writer = JudgedEntryWriter::new(backend.clone());

        writer
            .write_judged(
                "default",
                &json!({"entities": [{"name": "Anonymous fact"}]}),
            )
            .await
            .unwrap();

        let rows = backend.list_schema("default").await.unwrap();
        assert_eq!(rows[0].source_ref.extractor_version, JUDGED_BY_ANONYMOUS);
    }

    #[tokio::test]
    async fn a_write_that_would_store_nothing_is_refused() {
        let backend = backend();
        let writer = JudgedEntryWriter::new(backend);

        let err = writer
            .write_judged("default", &json!({"judged_by": "agent:x"}))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no entities"), "{err}");
    }

    /// Answers every text with the same two-dimensional vector. The writer
    /// does not care what the numbers are, only that a vector reached the row.
    struct FixedEmbedder;

    #[async_trait]
    impl cog_core::EmbeddingProvider for FixedEmbedder {
        async fn embed(&self, texts: Vec<String>) -> SFResult<Vec<Vec<f32>>> {
            Ok(texts.into_iter().map(|_| vec![0.25, 0.75]).collect())
        }

        async fn embed_sparse(
            &self,
            texts: Vec<String>,
        ) -> SFResult<Vec<cog_core::SparseEmbedding>> {
            Ok(texts.into_iter().map(|_| Default::default()).collect())
        }

        fn supports_sparse(&self) -> bool {
            false
        }

        fn model_id(&self) -> &str {
            "test-embedder"
        }

        fn dimension(&self) -> usize {
            2
        }
    }

    #[tokio::test]
    async fn a_judged_summary_lands_with_its_vector_when_the_host_has_an_embedder() {
        let backend = backend();
        let writer = JudgedEntryWriter::new(backend.clone()).with_embedder(Arc::new(FixedEmbedder));

        writer
            .write_judged(
                "default",
                &json!({
                    "raw_id": "conv-2",
                    "summary": {"text": "A fact worth keeping.", "importance": 5}
                }),
            )
            .await
            .unwrap();

        let summaries = backend.list_summary("default").await.unwrap();
        assert_eq!(summaries[0].embedding, vec![0.25, 0.75]);
        assert!(
            !summaries[0].embedding_model.is_empty(),
            "the vector's model must be named on the row"
        );
    }

    #[tokio::test]
    async fn an_unusable_raw_id_is_refused_instead_of_becoming_a_made_up_uri() {
        let backend = backend();
        let writer = JudgedEntryWriter::new(backend);

        let err = writer
            .write_judged(
                "default",
                &json!({"raw_id": "../escape", "entities": [{"name": "x"}]}),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("raw_id"), "{err}");
    }
}
