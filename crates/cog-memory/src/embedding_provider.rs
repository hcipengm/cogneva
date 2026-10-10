use cog_core::{EmbeddingProvider, SFError, SFResult, SparseEmbedding};

/// The name stored beside every vector this provider produces. It names the model, not
/// the caller, because that is what makes two vectors comparable: a row embedded by
/// another model has to be distinguishable from one embedded by this, and a row with no
/// vector at all is [`cog_core::NO_EMBEDDING_MODEL`].
pub const BGE_M3_MODEL_ID: &str = "bge-m3/v1";

/// Local embedding provider backed by [fastembed](https://crates.io/crates/fastembed),
/// running **BGE-M3** (`BAAI/bge-m3`): 1024-dim dense vectors, an 8192-token context
/// window (long summaries are not truncated), ONNX Runtime on CPU.
///
/// Two sessions over one set of weights. BGE-M3 emits both a dense vector and a sparse
/// (token-weight) one, and the summary collection carries a named sparse vector to hold
/// the second; hybrid retrieval is the reason this model was chosen over a dense-only
/// one, so the sparse session is part of what this provider is, not an option it might
/// grow later.
///
/// The two sessions load independently. The dense one is required -- a provider without
/// it has nothing to answer with -- so its failure is this constructor's error. The
/// sparse one is not: its failure leaves the provider able to embed densely (the text
/// path keeps working) and is reported by name when something asks for a sparse vector,
/// rather than silently swallowing the request or taking the dense session down with it.
///
/// Loading reads weights from disk and never fetches them: fastembed looks in the
/// directory it was given (see [`FastEmbedProvider::try_new_with_cache_dir`]) and,
/// only if they are missing there, at a model hub. In a deployment with no route to
/// one, that fetch neither succeeds nor fails, so the weights have to be on disk
/// before the process starts.
pub struct FastEmbedProvider {
    dense_model: std::sync::Mutex<fastembed::TextEmbedding>,
    /// Absent when the sparse session failed to load; the reason is kept so a
    /// request that needed it can say what actually went wrong instead of reporting
    /// a generic refusal.
    sparse_model: Option<std::sync::Mutex<fastembed::SparseTextEmbedding>>,
    sparse_unavailable: Option<String>,
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

        // Dense first, and unconditionally required: it is the session every caller
        // needs, so a failure here is the provider failing, reported as such.
        let dense_model = fastembed::TextEmbedding::try_new(dense_options)
            .map_err(|e| format!("failed to load BGE-M3 dense embedding model: {e}"))?;

        // Sparse second, and independently: it failing is a degraded provider, not a
        // broken one. The reason travels with the provider so the refusal a caller
        // gets names the load failure rather than just saying "unavailable".
        let (sparse_model, sparse_unavailable) =
            match fastembed::SparseTextEmbedding::try_new(sparse_options) {
                Ok(model) => (Some(std::sync::Mutex::new(model)), None),
                Err(e) => {
                    let reason = format!("failed to load BGE-M3 sparse embedding model: {e}");
                    tracing::warn!(
                        "{reason}; dense embedding stays available, sparse requests will be \
                         refused until this is fixed"
                    );
                    (None, Some(reason))
                }
            };

        Ok(Self {
            dense_model: std::sync::Mutex::new(dense_model),
            sparse_model,
            sparse_unavailable,
            dim: 1024,
        })
    }

    /// Whether this provider can produce sparse vectors, and if not, what went wrong.
    ///
    /// A caller about to wire a sparse path can ask before it starts writing rows: an
    /// ingest loop that discovers this per document pays the same failure once per
    /// document, and the answer cannot change while the process runs.
    pub fn sparse_status(&self) -> Result<(), &str> {
        match (&self.sparse_model, &self.sparse_unavailable) {
            (Some(_), _) => Ok(()),
            (None, Some(reason)) => Err(reason.as_str()),
            // The two fields are set together; an absent session always carries its
            // reason. Answering "unavailable, reason unknown" would be a lie a caller
            // could not act on, so say so plainly instead.
            (None, None) => Err("the sparse embedding session is not present"),
        }
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

        // An empty batch is answered as an empty batch -- asking for zero vectors of
        // anything is satisfied by zero vectors -- but a non-empty one with no session
        // is refused rather than answered with nothing, because a caller that stores
        // that nothing would be writing a row whose sparse column is empty for a
        // reason it cannot see.
        let Some(sparse_model) = self.sparse_model.as_ref() else {
            return Err(SFError::Config(format!(
                "this embedding provider has no sparse session; asked to embed {} text(s) as \
                 sparse vectors ({})",
                texts.len(),
                self.sparse_status().unwrap_err()
            )));
        };

        let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let mut model = sparse_model.lock().map_err(|e| {
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

    fn supports_sparse(&self) -> bool {
        self.sparse_model.is_some()
    }

    fn model_id(&self) -> &str {
        BGE_M3_MODEL_ID
    }

    fn dimension(&self) -> usize {
        self.dim
    }
}
