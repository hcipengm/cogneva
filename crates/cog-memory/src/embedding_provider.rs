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
    /// Create a new provider using BGE-M3 for both dense and sparse embeddings,
    /// loading its weights from wherever fastembed's own cache setting points.
    pub fn try_new() -> Result<Self, String> {
        Self::try_new_with_cache_dir(None)
    }

    /// The same, with the cache directory named here instead of falling back to
    /// that setting.
    ///
    /// A caller that mounts the weights at a path of its own has to say so: with no
    /// route to a model hub, the loader can only read weights already on disk, and
    /// fastembed otherwise looks for them under `FASTEMBED_CACHE_DIR` or a
    /// `.fastembed_cache` beside the working directory. Naming the directory here
    /// takes that variable out of the question.
    ///
    /// It does not take `HF_HOME` out of the question: the hub client prefers that
    /// variable over any directory it was handed, so a process that has it set reads
    /// somewhere else and the failure reads as a bad weight directory. Whoever loads
    /// a mounted directory has to leave `HF_HOME` unset.
    ///
    /// Both sessions are built from the same directory: dense and sparse read the
    /// same repository, down to the same weight files.
    pub fn try_new_with_cache_dir(cache_dir: Option<std::path::PathBuf>) -> Result<Self, String> {
        let mut dense_options = fastembed::InitOptions::new(fastembed::EmbeddingModel::BGEM3)
            .with_show_download_progress(true);
        let mut sparse_options = fastembed::SparseInitOptions::new(fastembed::SparseModel::BGEM3)
            .with_show_download_progress(true);
        if let Some(dir) = cache_dir {
            dense_options = dense_options.with_cache_dir(dir.clone());
            sparse_options = sparse_options.with_cache_dir(dir);
        }

        let dense_model = fastembed::TextEmbedding::try_new(dense_options)
            .map_err(|e| format!("failed to load BGE-M3 dense embedding model: {e}"))?;

        let sparse_model = fastembed::SparseTextEmbedding::try_new(sparse_options)
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
