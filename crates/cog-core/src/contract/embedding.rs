//!Text embedding provider trait — 定义跨 crate 的 embedding 接口。
//!实现位于 `cog-memory` (`FastEmbedProvider`)，此处仅定义 trait 和工具函数，
//!符合 core = trait + struct/enum + SFError 的定位。

use crate::{storage::SparseEmbedding, SFResult};

/// Abstraction for text embedding models.
/// Implementations may be local (ONNX, fastembed) or remote (OpenAI, Ollama).
#[async_trait::async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Embed a batch of texts into dense vectors.
    /// Returns one `Vec<f32>` per input text.  All vectors share the same
    /// dimension (the model's output size).
    async fn embed(&self, texts: Vec<String>) -> SFResult<Vec<Vec<f32>>>;

    /// Embed a batch of texts into sparse vectors.
    /// Returns one [`SparseEmbedding`] per input text.
    async fn embed_sparse(&self, texts: Vec<String>) -> SFResult<Vec<SparseEmbedding>>;

    /// Return the expected dense vector dimension.
    fn dimension(&self) -> usize;
}

/// Compute cosine similarity between two dense vectors.
/// Returns a value in `[-1.0, 1.0]`.  For unit-length embeddings (BGE-M3
/// dense output) this is equivalent to the dot product.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum();
    let norm_a: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    let norm_b: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    (dot / (norm_a * norm_b)).clamp(-1.0, 1.0)
}

/// Batch compute pairwise cosine similarities between a query vector and
/// a list of candidate vectors.
pub fn batch_cosine_similarity(query: &[f32], candidates: &[Vec<f32>]) -> Vec<f64> {
    candidates
        .iter()
        .map(|c| cosine_similarity(query, c))
        .collect()
}
