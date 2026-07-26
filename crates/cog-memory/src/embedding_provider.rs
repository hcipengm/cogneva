use cog_core::{EmbeddingProvider, SFResult, SparseEmbedding};

/// Local embedding provider backed by [fastembed](https://crates.io/crates/fastembed).
/// Uses the **BGE-M3** model (`BAAI/bge-m3`) by default:
/// - 1024-dim dense vectors
/// - Sparse vectors (token-level keyword weights)
/// - 8192-token context window (long summaries are not truncated)
/// - ONNX Runtime CPU inference, no GPU required
///
/// The model is downloaded automatically on first use and cached locally.
pub struct FastEmbedProvider {
    dense_model: std::sync::Mutex<fastembed::TextEmbedding>,
    sparse_model: std::sync::Mutex<fastembed::SparseTextEmbedding>,
    dim: usize,
}

impl FastEmbedProvider {
    /// Create a new provider using BGE-M3 for both dense and sparse embeddings.
    pub fn try_new() -> Result<Self, String> {
        let dense_model = fastembed::TextEmbedding::try_new(
            fastembed::InitOptions::new(fastembed::EmbeddingModel::BGEM3)
                .with_show_download_progress(true),
        )
        .map_err(|e| format!("failed to load BGE-M3 dense embedding model: {e}"))?;

        let sparse_model = fastembed::SparseTextEmbedding::try_new(
            fastembed::SparseInitOptions::new(fastembed::SparseModel::BGEM3)
                .with_show_download_progress(true),
        )
        .map_err(|e| format!("failed to load BGE-M3 sparse embedding model: {e}"))?;

        Ok(Self {
            dense_model: std::sync::Mutex::new(dense_model),
            sparse_model: std::sync::Mutex::new(sparse_model),
            dim: 1024,
        })
    }
}

#[async_trait::async_trait]
impl EmbeddingProvider for FastEmbedProvider {
    async fn embed(&self, texts: Vec<String>) -> SFResult<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let mut model = self.dense_model.lock().map_err(|e| {
            cog_core::SFError::Validation(format!("dense embedding model lock poisoned: {e}"))
        })?;
        let embeddings = model
            .embed(refs, None)
            .map_err(|e| cog_core::SFError::Validation(format!("dense embedding failed: {e}")))?;

        Ok(embeddings)
    }

    async fn embed_sparse(&self, texts: Vec<String>) -> SFResult<Vec<SparseEmbedding>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let mut model = self.sparse_model.lock().map_err(|e| {
            cog_core::SFError::Validation(format!("sparse embedding model lock poisoned: {e}"))
        })?;
        let embeddings = model
            .embed(refs, None)
            .map_err(|e| cog_core::SFError::Validation(format!("sparse embedding failed: {e}")))?;

        Ok(embeddings
            .into_iter()
            .map(|e| SparseEmbedding {
                indices: e.indices.iter().map(|&i| i as u32).collect(),
                values: e.values,
            })
            .collect())
    }

    fn dimension(&self) -> usize {
        self.dim
    }
}
