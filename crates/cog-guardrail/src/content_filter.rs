//! 内容安全过滤 — 深度版。
//! 双层防御：
//! 1. Regex 快速匹配（已知关键词）
//! 2. 语义相似度检测（基于预定义语义向量，检测同义词/近义词绕过）
//!
//! **Jaccard → Embedding 替换说明**：
//! - 旧方案：Jaccard 分词相似度，无法捕捉语义近义词（如"好"和"优秀"）
//! - 新方案：BGE-M3 dense embedding + 余弦相似度，支持语义层面的相似判断
//! - 回退：当 embedder 不可用时，自动回退到 Jaccard

use cog_core::GuardResult;
use cog_core::{cosine_similarity, EmbeddingProvider};
use regex::RegexSet;
use std::collections::HashSet;
use std::sync::Arc;

/// 内容过滤器配置。
#[derive(Debug, Clone)]
pub struct ContentFilterConfig {
    pub block_nsfw: bool,
    pub block_violence: bool,
    pub block_hate_speech: bool,
    pub block_self_harm: bool,
    pub block_illegal_acts: bool,
    pub semantic_threshold: f64, // 0.0 ~ 1.0, 语义检测阈值
    pub custom_blocked_patterns: Vec<String>,
}

impl Default for ContentFilterConfig {
    fn default() -> Self {
        Self {
            block_nsfw: true,
            block_violence: true,
            block_hate_speech: true,
            block_self_harm: true,
            block_illegal_acts: true,
            semantic_threshold: 0.6,
            custom_blocked_patterns: vec![],
        }
    }
}

/// 预计算的语义 embedding — 每个违规主题对应一个 dense vector。
struct SemanticEmbedding {
    category: String,
    embedding: Vec<f32>,
}

/// 预定义的语义向量（Jaccard 回退用）。
struct SemanticVector {
    category: String,
    terms: HashSet<String>,
}

/// 内容过滤器。
pub struct ContentFilter {
    config: ContentFilterConfig,
    patterns: RegexSet,
    /// 预计算的语义 embedding（embedding 方案）。
    semantic_embeddings: Vec<SemanticEmbedding>,
    /// Jaccard 语义向量（回退方案）。
    semantic_vectors: Vec<SemanticVector>,
    /// Embedding provider（可选，用于语义检测）。
    embedder: Option<Arc<dyn EmbeddingProvider>>,
}

impl ContentFilter {
    /// 同步构造函数 — 无 embedding，使用 Jaccard 回退。
    pub fn new(config: ContentFilterConfig) -> Self {
        let (_patterns, regex_set) = Self::build_regex_set(&config);
        let semantic_vectors = build_semantic_vectors();

        Self {
            config,
            patterns: regex_set,
            semantic_embeddings: vec![],
            semantic_vectors,
            embedder: None,
        }
    }

    /// 异步构造函数 — 预计算语义 embedding，使用 BGE-M3 + 余弦相似度。
    pub async fn new_with_embedder(
        config: ContentFilterConfig,
        embedder: Arc<dyn EmbeddingProvider>,
    ) -> Self {
        let (_patterns, regex_set) = Self::build_regex_set(&config);
        let semantic_vectors = build_semantic_vectors();

        // 预计算每个违规类别词组的平均 embedding
        let mut semantic_embeddings = vec![];
        for vector in &semantic_vectors {
            let texts: Vec<String> = vector.terms.iter().cloned().collect();
            match embedder.embed(texts).await {
                Ok(embeddings) if !embeddings.is_empty() => {
                    // 取该类别所有词的 embedding 平均值作为类别向量
                    let dim = embeddings[0].len();
                    let mut avg = vec![0.0f32; dim];
                    for emb in &embeddings {
                        for (i, v) in emb.iter().enumerate() {
                            avg[i] += v;
                        }
                    }
                    let count = embeddings.len() as f32;
                    for v in &mut avg {
                        *v /= count;
                    }
                    semantic_embeddings.push(SemanticEmbedding {
                        category: vector.category.clone(),
                        embedding: avg,
                    });
                }
                Ok(_) | Err(_) => {
                    tracing::warn!(
                        "Failed to pre-compute embedding for category {}, will fall back to Jaccard",
                        vector.category
                    );
                }
            }
        }

        Self {
            config,
            patterns: regex_set,
            semantic_embeddings,
            semantic_vectors,
            embedder: Some(embedder),
        }
    }

    fn build_regex_set(config: &ContentFilterConfig) -> (Vec<String>, RegexSet) {
        let mut patterns: Vec<String> = vec![
            // NSFW — explicit + euphemisms
            r"(?i)\b(nude|naked|porn|sexual|xxx|adult content|nsfw)\b".into(),
            r"(?i)\b(explicit|graphic|mature content|adult material)\b".into(),
            r"(?i)\b(nudity|intercourse|genital|erotic|obscene)\b".into(),
            // Violence
            r"(?i)\b(kill|murder|suicide|bomb|terrorist|attack|assassinate)\b".into(),
            r"(?i)\b(torture|maim|slaughter|massacre|genocide|lynch)\b".into(),
            r"(?i)\b(shoot|stab|strangle|drown|burn alive)\b".into(),
            // Hate speech
            r"(?i)\b(racist|nazi|supremacist|slur|hate|xenophobic|antisemitic)\b".into(),
            r"(?i)\b(holocaust denial|race war|ethnic cleansing|inferior race)\b".into(),
            r"(?i)\b(homophobic|transphobic|misogynist|sexist|ableist)\b".into(),
            // Self-harm
            r"(?i)\b(self.?harm|self.?injury|cutting myself|end my life)\b".into(),
            r"(?i)\b(i want to die|suicidal ideation|no reason to live)\b".into(),
            // Illegal acts
            r"(?i)\b(how to make|recipe for|instructions for).{0,20}(bomb|meth|explosive)\b".into(),
            r"(?i)\b(steal|hack|break into|bypass security|social engineer)\b".into(),
        ];
        patterns.extend(config.custom_blocked_patterns.iter().cloned());
        let regex_set = RegexSet::new(&patterns).unwrap_or_else(|_| RegexSet::empty());
        (patterns, regex_set)
    }

    pub async fn check(&self, text: &str) -> GuardResult {
        // Layer 1: Regex pattern matching
        let regex_matches: Vec<usize> = self.patterns.matches(text).into_iter().collect();
        let regex_reasons: Vec<String> = regex_matches
            .iter()
            .filter_map(|&i| match i {
                0..=2 if self.config.block_nsfw => Some("NSFW content detected".into()),
                3..=5 if self.config.block_violence => Some("Violence content detected".into()),
                6..=8 if self.config.block_hate_speech => Some("Hate speech detected".into()),
                9 | 10 if self.config.block_self_harm => Some("Self-harm content detected".into()),
                11 | 12 if self.config.block_illegal_acts => {
                    Some("Illegal acts content detected".into())
                }
                _ if i >= 13 => Some(format!("Custom pattern matched: index {}", i)),
                _ => None,
            })
            .collect();

        // Layer 2: Semantic similarity detection (embedding first, Jaccard fallback)
        let mut semantic_reasons: Vec<String> = vec![];

        if !self.semantic_embeddings.is_empty() {
            // Embedding 方案：BGE-M3 + 余弦相似度
            if let Some(ref embedder) = self.embedder {
                match embedder.embed(vec![text.to_string()]).await {
                    Ok(mut embeddings) if !embeddings.is_empty() => {
                        let input_emb = embeddings.remove(0);
                        for sem in &self.semantic_embeddings {
                            let score = cosine_similarity(&input_emb, &sem.embedding);
                            if score >= self.config.semantic_threshold {
                                semantic_reasons.push(format!(
                                    "Semantic {} detected (score: {:.2})",
                                    sem.category, score
                                ));
                            }
                        }
                    }
                    Ok(_) | Err(_) => {
                        tracing::warn!(
                            "Embedding failed for guardrail check, falling back to Jaccard"
                        );
                        semantic_reasons.extend(self.check_jaccard(text));
                    }
                }
            } else {
                semantic_reasons.extend(self.check_jaccard(text));
            }
        } else {
            // 无预计算 embedding，回退到 Jaccard
            semantic_reasons.extend(self.check_jaccard(text));
        }

        let all_reasons: Vec<String> = regex_reasons.into_iter().chain(semantic_reasons).collect();

        if all_reasons.is_empty() {
            GuardResult::Pass
        } else {
            GuardResult::Block {
                reason: all_reasons.join("; "),
                rule: "content_filter".into(),
            }
        }
    }

    /// Jaccard 回退检测。
    fn check_jaccard(&self, text: &str) -> Vec<String> {
        let tokens: HashSet<String> = tokenize(text);
        let mut reasons = vec![];
        for vector in &self.semantic_vectors {
            let score = jaccard_similarity(&tokens, &vector.terms);
            if score >= self.config.semantic_threshold {
                reasons.push(format!(
                    "Semantic {} detected (score: {:.2})",
                    vector.category, score
                ));
            }
        }
        reasons
    }
}

fn tokenize(text: &str) -> HashSet<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '-')
        .filter(|s| !s.is_empty() && s.len() > 2)
        .map(String::from)
        .collect()
}

fn jaccard_similarity(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let intersection: HashSet<_> = a.intersection(b).cloned().collect();
    let union: HashSet<_> = a.union(b).cloned().collect();
    intersection.len() as f64 / union.len() as f64
}

fn build_semantic_vectors() -> Vec<SemanticVector> {
    vec![
        SemanticVector {
            category: "nsfw".into(),
            terms: [
                "sexual",
                "explicit",
                "pornographic",
                "adult",
                "mature",
                "erotic",
                "nude",
                "naked",
                "undress",
                "intimate",
                "sensual",
                "provocative",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        },
        SemanticVector {
            category: "violence".into(),
            terms: [
                "violence",
                "aggressive",
                "brutal",
                "savage",
                "cruel",
                "vicious",
                "attack",
                "assault",
                "harm",
                "injure",
                "wound",
                "damage",
                "destroy",
                "fight",
                "beat",
                "hit",
                "punch",
                "kick",
                "strangle",
                "torture",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        },
        SemanticVector {
            category: "hate_speech".into(),
            terms: [
                "hate",
                "discriminate",
                "prejudice",
                "bigotry",
                "intolerance",
                "supremacy",
                "superior",
                "inferior",
                "subhuman",
                "degenerate",
                "ethnic",
                "racial",
                "religious",
                "homophobic",
                "transphobic",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        },
        SemanticVector {
            category: "self_harm".into(),
            terms: [
                "suicide",
                "self-harm",
                "self-injury",
                "cutting",
                "overdose",
                "die",
                "death",
                "end life",
                "no point",
                "hopeless",
                "worthless",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        },
        SemanticVector {
            category: "illegal".into(),
            terms: [
                "illegal",
                "criminal",
                "fraud",
                "theft",
                "steal",
                "hack",
                "exploit",
                "weapon",
                "bomb",
                "explosive",
                "drug",
                "meth",
                "forgery",
                "counterfeit",
                "trafficking",
                "launder",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        },
    ]
}

#[async_trait::async_trait]
impl cog_core::Guardrail for ContentFilter {
    async fn check_input(&self, messages: &[cog_core::Message]) -> GuardResult {
        let text: String = messages
            .iter()
            .map(|m| m.content())
            .collect::<Vec<_>>()
            .join("\n");
        self.check(&text).await
    }

    async fn check_output(&self, response: &str) -> GuardResult {
        self.check(response).await
    }

    async fn check_tool_call(&self, _tool: &cog_core::ToolCall) -> GuardResult {
        GuardResult::Pass
    }
}
