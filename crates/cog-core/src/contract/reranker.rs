//!Cross-encoder reranker contract — 定义跨 crate 的重排接口。
//!实现位于 `cog-memory` (`FastEmbedRerankerProvider`)，消费方在 `cog-wiki`
//!（检索路的第二段），两边都只认这里的契约。

use crate::SFResult;

/// Result of a rerank operation.
#[derive(Debug, Clone)]
pub struct RerankResult {
    /// The document this score belongs to, as it was handed in.
    pub document: Option<String>,
    pub score: f32,
    /// The position the document had in the input list, so a caller that keeps
    /// its own parallel array of candidates can map a score back to one.
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
