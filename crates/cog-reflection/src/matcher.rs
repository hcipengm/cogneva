//! Pattern matching and recurrence detection across learning entries.
//! The [`LearningMatcher`] trait finds semantically similar learnings so
//! that repeated issues can be grouped into [`Pattern`]s and promoted
//! once they cross the maturity threshold.

use std::sync::Arc;

use async_trait::async_trait;
use cog_core::SFResult;
use tracing::{debug, info};

use crate::recorder::LearningRecorder;
use cog_core::{Learning, Pattern};

/// Matches learnings against existing entries to detect repetition and
/// emerging patterns.
#[async_trait]
pub trait LearningMatcher: Send + Sync {
    /// Find learnings that are semantically similar to the given one.
    async fn find_similar(&self, learning: &Learning) -> SFResult<Vec<Learning>>;

    /// Update the recurrence count of `learning` based on matched similar entries.
    async fn update_recurrence(&self, learning: &mut Learning) -> SFResult<()>;

    /// Detect all patterns across the current learning corpus.
    async fn detect_patterns(&self) -> SFResult<Vec<Pattern>>;
}

/// Default matcher that combines:
/// 1. **Exact `pattern_key` match** (fast, deterministic).
/// 2. **Embedding cosine similarity** (primary, BGE-M3).
/// 3. **Keyword overlap heuristic** (fallback, lightweight Jaccard).
///
/// Phase 1 implemented (1) and (2). Phase 2 adds (3) via `cog-memory`.
pub struct DefaultLearningMatcher {
    recorder: Arc<dyn LearningRecorder>,
    /// Minimum similarity threshold (embedding cosine or Jaccard fallback).
    keyword_threshold: f32,
    embedder: Option<Arc<dyn cog_core::EmbeddingProvider>>,
}

impl std::fmt::Debug for DefaultLearningMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultLearningMatcher")
            .field("recorder", &"<dyn LearningRecorder>")
            .field("keyword_threshold", &self.keyword_threshold)
            .finish()
    }
}

impl Clone for DefaultLearningMatcher {
    fn clone(&self) -> Self {
        Self {
            recorder: Arc::clone(&self.recorder),
            keyword_threshold: self.keyword_threshold,
            embedder: self.embedder.clone(),
        }
    }
}

impl DefaultLearningMatcher {
    pub fn new(
        recorder: Arc<dyn LearningRecorder>,
        embedder: Option<Arc<dyn cog_core::EmbeddingProvider>>,
    ) -> Self {
        Self {
            recorder,
            keyword_threshold: 0.5,
            embedder,
        }
    }

    /// Set the keyword overlap threshold (0.0–1.0).
    pub fn with_keyword_threshold(mut self, threshold: f32) -> Self {
        self.keyword_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Attach an [`EmbeddingProvider`] for BGE-M3 semantic similarity.
    pub fn with_embedder(mut self, embedder: Arc<dyn cog_core::EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    fn tokenise(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|s| !s.is_empty() && s.len() > 2)
            .collect()
    }

    fn jaccard(a: &[String], b: &[String]) -> f32 {
        if a.is_empty() || b.is_empty() {
            return 0.0;
        }
        let set_a: std::collections::HashSet<&String> = a.iter().collect();
        let set_b: std::collections::HashSet<&String> = b.iter().collect();
        let intersection = set_a.intersection(&set_b).count();
        let union = set_a.union(&set_b).count();
        if union == 0 {
            0.0
        } else {
            intersection as f32 / union as f32
        }
    }

    fn compute_similarity(a: &Learning, b: &Learning, embed_sim: Option<f32>) -> f32 {
        // Pattern-key exact match is the strongest signal.
        if let (Some(pk_a), Some(pk_b)) = (&a.pattern_key, &b.pattern_key) {
            if pk_a == pk_b {
                return 1.0;
            }
        }

        // Category + area alignment boosts similarity.
        let mut score = 0.0f32;
        if std::mem::discriminant(&a.category) == std::mem::discriminant(&b.category) {
            score += 0.2;
        }
        if std::mem::discriminant(&a.area) == std::mem::discriminant(&b.area) {
            score += 0.1;
        }

        // Semantic similarity: embedding cosine (primary) or Jaccard (fallback).
        let semantic = match embed_sim {
            Some(cos) => {
                // When embedding is available, it dominates (90%) with Jaccard
                // as a lightweight fallback (10%).
                let tokens_a = Self::tokenise(&format!("{} {}", a.summary, a.details));
                let tokens_b = Self::tokenise(&format!("{} {}", b.summary, b.details));
                let jaccard = Self::jaccard(&tokens_a, &tokens_b);
                cos * 0.9 + jaccard * 0.1
            }
            None => {
                let tokens_a = Self::tokenise(&format!("{} {}", a.summary, a.details));
                let tokens_b = Self::tokenise(&format!("{} {}", b.summary, b.details));
                Self::jaccard(&tokens_a, &tokens_b)
            }
        };
        score += semantic * 0.7;

        score.min(1.0)
    }
}

#[async_trait]
impl LearningMatcher for DefaultLearningMatcher {
    async fn find_similar(&self, learning: &Learning) -> SFResult<Vec<Learning>> {
        let all = self.recorder.list_learnings(None).await?;

        // Pre-compute embeddings when an embedder is available.
        let embed_sims: std::collections::HashMap<String, f32> =
            if let Some(ref emb) = self.embedder {
                let texts: Vec<String> =
                    std::iter::once(format!("{} {}", learning.summary, learning.details))
                        .chain(all.iter().map(|l| format!("{} {}", l.summary, l.details)))
                        .collect();
                match emb.embed(texts).await {
                    Ok(vectors) if !vectors.is_empty() => {
                        let query_vec = &vectors[0];
                        all.iter()
                            .zip(vectors.iter().skip(1))
                            .map(|(l, v)| {
                                let sim = cog_core::cosine_similarity(query_vec, v) as f32;
                                (l.id.clone(), sim)
                            })
                            .collect()
                    }
                    _ => std::collections::HashMap::new(),
                }
            } else {
                std::collections::HashMap::new()
            };

        let mut similar: Vec<(Learning, f32)> = all
            .into_iter()
            .filter(|l| l.id != learning.id)
            .map(|l| {
                let embed_sim = embed_sims.get(&l.id).copied();
                let score = Self::compute_similarity(learning, &l, embed_sim);
                (l, score)
            })
            .filter(|(_, score)| *score >= self.keyword_threshold)
            .collect();

        similar.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        debug!(
            "found {} similar learnings for {} (threshold={})",
            similar.len(),
            learning.id,
            self.keyword_threshold
        );
        Ok(similar.into_iter().map(|(l, _)| l).collect())
    }

    async fn update_recurrence(&self, learning: &mut Learning) -> SFResult<()> {
        let similar = self.find_similar(learning).await?;
        if !similar.is_empty() {
            let max_recurrence = similar
                .iter()
                .map(|l| l.recurrence_count)
                .max()
                .unwrap_or(1);
            learning.recurrence_count = learning.recurrence_count.max(max_recurrence + 1);
            learning.last_seen = chrono::Utc::now();

            // Merge related tasks from similar learnings.
            for s in &similar {
                for task_id in &s.related_tasks {
                    if !learning.related_tasks.contains(task_id) {
                        learning.related_tasks.push(task_id.clone());
                    }
                }
            }

            info!(
                "bumped recurrence for {} to {} based on {} similar entries",
                learning.id,
                learning.recurrence_count,
                similar.len()
            );
        }
        Ok(())
    }

    async fn detect_patterns(&self) -> SFResult<Vec<Pattern>> {
        let learnings = self.recorder.list_learnings(None).await?;
        let mut patterns: std::collections::HashMap<String, Vec<Learning>> =
            std::collections::HashMap::new();

        // Pre-compute embeddings for all learnings when an embedder is available.
        let embeddings: std::collections::HashMap<String, Vec<f32>> =
            if let Some(ref emb) = self.embedder {
                let texts: Vec<String> = learnings
                    .iter()
                    .map(|l| format!("{} {}", l.summary, l.details))
                    .collect();
                match emb.embed(texts).await {
                    Ok(vectors) => learnings
                        .iter()
                        .zip(vectors)
                        .map(|(l, v)| (l.id.clone(), v))
                        .collect(),
                    _ => std::collections::HashMap::new(),
                }
            } else {
                std::collections::HashMap::new()
            };

        // Group by explicit pattern_key first.
        for l in &learnings {
            if let Some(pk) = &l.pattern_key {
                patterns.entry(pk.clone()).or_default().push(l.clone());
            }
        }

        // Auto-cluster learnings without explicit keys using semantic similarity.
        let unclustered: Vec<&Learning> = learnings
            .iter()
            .filter(|l| l.pattern_key.is_none())
            .collect();

        for l in &unclustered {
            let mut best_key: Option<String> = None;
            let mut best_score = 0.0f32;

            for (pk, group) in &patterns {
                if group.is_empty() {
                    continue;
                }
                let representative = &group[0];

                let score = match (embeddings.get(&l.id), embeddings.get(&representative.id)) {
                    (Some(a), Some(b)) => cog_core::cosine_similarity(a, b) as f32,
                    _ => {
                        let tokens = DefaultLearningMatcher::tokenise(&format!(
                            "{} {}",
                            l.summary, l.details
                        ));
                        let rep_tokens = DefaultLearningMatcher::tokenise(&format!(
                            "{} {}",
                            representative.summary, representative.details
                        ));
                        DefaultLearningMatcher::jaccard(&tokens, &rep_tokens)
                    }
                };

                if score > best_score && score >= self.keyword_threshold {
                    best_score = score;
                    best_key = Some(pk.clone());
                }
            }

            if let Some(pk) = best_key {
                patterns.entry(pk).or_default().push((*l).clone());
            } else {
                // Create a new auto-generated pattern key.
                let auto_key = format!("auto.{}", l.id.to_lowercase().replace('-', "_"));
                patterns.entry(auto_key).or_default().push((*l).clone());
            }
        }

        let mut result = Vec::new();
        for (key, group) in patterns {
            if group.len() < 2 {
                continue; // Ignore singletons as patterns.
            }
            let recurrence_count = group.iter().map(|l| l.recurrence_count).sum();
            let first_seen = group
                .iter()
                .map(|l| l.first_seen)
                .min()
                .unwrap_or_else(chrono::Utc::now);
            let last_seen = group
                .iter()
                .map(|l| l.last_seen)
                .max()
                .unwrap_or_else(chrono::Utc::now);

            result.push(Pattern {
                key: key.clone(),
                description: format!("Pattern '{}' with {} related learnings", key, group.len()),
                learning_ids: group.iter().map(|l| l.id.clone()).collect(),
                recurrence_count,
                first_seen,
                last_seen,
            });
        }

        info!(
            "detected {} patterns from {} learnings",
            result.len(),
            learnings.len()
        );
        Ok(result)
    }
}
