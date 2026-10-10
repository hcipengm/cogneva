use cog_core::{RerankResult, RerankerProvider, SFResult};

/// Local reranker backed by [fastembed](https://crates.io/crates/fastembed).
/// Uses **BGE-Reranker-V2-M3** (`rozgo/bge-reranker-v2-m3`) by default:
/// - Cross-encoder architecture (query + doc jointly encoded)
/// - ONNX Runtime CPU inference
/// - Multilingual support
///
/// The model is downloaded automatically on first use and cached locally.
pub struct FastEmbedRerankerProvider {
    model: std::sync::Mutex<fastembed::TextRerank>,
}

impl FastEmbedRerankerProvider {
    /// Create a new reranker using BGE-Reranker-V2-M3, loading its weights from
    /// wherever fastembed's own cache setting points.
    pub fn try_new() -> Result<Self, String> {
        Self::try_new_with_cache_dir(None)
    }

    /// The same, with the cache directory named here instead of read from the
    /// environment.
    ///
    /// A caller that mounts the weights at a path of its own has to say so: the
    /// reranker's fetch path builds its hub client from the cache directory and
    /// fixes the endpoint, so on a network that cannot reach huggingface.co the
    /// only way to load them is to know where they are. The failure this avoids is
    /// the quiet one -- an environment variable set on one process and not on
    /// another, which shows up as a model that downloads in a test and hangs in a
    /// deployment.
    pub fn try_new_with_cache_dir(cache_dir: Option<std::path::PathBuf>) -> Result<Self, String> {
        let mut options =
            fastembed::RerankInitOptions::new(fastembed::RerankerModel::BGERerankerV2M3)
                .with_show_download_progress(true);
        if let Some(dir) = cache_dir {
            options = options.with_cache_dir(dir);
        }
        let model = fastembed::TextRerank::try_new(options)
            .map_err(|e| format!("failed to load BGE-Reranker-V2-M3: {e}"))?;

        Ok(Self {
            model: std::sync::Mutex::new(model),
        })
    }

    /// One score per document, in the order given, from the cross-encoder.
    ///
    /// `fastembed` returns every document scored and sorted by score, with the
    /// index it was given; this puts them back in the caller's order, because a
    /// caller pairing scores with candidates positionally needs the order it gave,
    /// and sorting is the caller's to do.
    pub fn scores_in_order(&self, query: &str, documents: &[String]) -> Result<Vec<f32>, String> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let mut model = self
            .model
            .lock()
            .map_err(|e| format!("reranker model lock poisoned: {e}"))?;
        let doc_refs: Vec<&str> = documents.iter().map(String::as_str).collect();
        let results = model
            .rerank(query, &doc_refs, false, None)
            .map_err(|e| format!("reranking failed: {e}"))?;

        let mut scores = vec![f32::NAN; documents.len()];
        let mut filled = vec![false; documents.len()];
        for result in results {
            if let Some(slot) = scores.get_mut(result.index) {
                *slot = result.score;
                filled[result.index] = true;
            }
        }
        // A partial answer is refused rather than passed on: the caller pairs these
        // with its candidates positionally, and a missing one left as `NaN` would
        // compare false against every boundary and read as "not relevant" instead of
        // as "the model did not answer about this candidate".
        if let Some(missing) = filled.iter().position(|f| !f) {
            return Err(format!(
                "the reranker scored {}/{len} documents; {missing} has no score",
                filled.iter().filter(|f| **f).count(),
                len = documents.len()
            ));
        }
        Ok(scores)
    }
}

#[async_trait::async_trait]
impl RerankerProvider for FastEmbedRerankerProvider {
    async fn rerank(
        &self,
        query: &str,
        documents: Vec<String>,
        top_n: usize,
    ) -> SFResult<Vec<RerankResult>> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        let mut model = self.model.lock().map_err(|e| {
            cog_core::SFError::Validation(format!("reranker model lock poisoned: {e}"))
        })?;

        let doc_refs: Vec<&str> = documents.iter().map(|s| s.as_str()).collect();
        let results = model
            .rerank(query, &doc_refs, true, None)
            .map_err(|e| cog_core::SFError::Validation(format!("reranking failed: {e}")))?;

        let mut ranked: Vec<RerankResult> = results
            .into_iter()
            .map(|r| RerankResult {
                document: r.document,
                score: r.score,
                index: r.index,
            })
            .collect();

        // The position the model returned is a total tie-break: scores come back
        // as floats and ties are common at the bottom of a candidate list, where
        // an undefined order would make the cut at `top_n` pick a different set
        // between runs.
        ranked.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.index.cmp(&b.index))
        });
        ranked.truncate(top_n);
        Ok(ranked)
    }
}
