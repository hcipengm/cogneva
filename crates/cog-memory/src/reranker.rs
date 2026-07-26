use cog_core::SFResult;

/// Result of a rerank operation.
#[derive(Debug, Clone)]
pub struct RerankResult {
    pub document: Option<String>,
    pub score: f32,
    pub index: usize,
}

/// Abstraction for cross-encoder reranking models.
/// Rerankers take a query and a list of candidate documents,
/// then score each pair for relevance.  They are typically used
/// as the second stage of a two-stage retrieval pipeline:
/// 1. Recall (dense + sparse hybrid) → Top-K candidates
/// 2. Rerank → Top-N most relevant documents.
#[async_trait::async_trait]
pub trait RerankerProvider: Send + Sync {
    /// Rerank candidate documents against a query.
    /// Returns results sorted by descending relevance score.
    async fn rerank(
        &self,
        query: &str,
        documents: Vec<String>,
        top_n: usize,
    ) -> SFResult<Vec<RerankResult>>;
}

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
    /// Create a new reranker using BGE-Reranker-V2-M3.
    pub fn try_new() -> Result<Self, String> {
        let model = fastembed::TextRerank::try_new(
            fastembed::RerankInitOptions::new(fastembed::RerankerModel::BGERerankerV2M3)
                .with_show_download_progress(true),
        )
        .map_err(|e| format!("failed to load BGE-Reranker-V2-M3: {e}"))?;

        Ok(Self {
            model: std::sync::Mutex::new(model),
        })
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

        ranked.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        ranked.truncate(top_n);
        Ok(ranked)
    }
}
