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

/// The rule name this detector stamps on the blocks it raises.
///
/// Named here because this is where the block is produced, and recognized
/// through [`is_harmful_content_rule`] rather than by a second copy of the
/// string: the layer that counts harmful-content detections has to recognize
/// the same name the block carries, and two literals that are meant to be one
/// name is how they stop being one.
pub const HARMFUL_CONTENT_RULE: &str = "content_filter";

/// Whether a block carrying this rule came from the harmful-content detector.
///
/// Matched by equality rather than by prefix. A prefix test would admit any
/// later rule name that merely begins with this one — a second content rule
/// added alongside it would start counting as harmful content without anyone
/// deciding that — and the name being recognized is one detector's whole rule
/// name, not a family of them.
pub fn is_harmful_content_rule(rule: &str) -> bool {
    rule == HARMFUL_CONTENT_RULE
}

/// 本门印在判词里的那几句话，一条规则一句。
///
/// 这些字面量有两个用途，而它们必须是同一份：`check` 用它们生成判词，
/// [`own_verdict_phrases`] 用它们在判之前剥掉。写两遍的那天，「打印的句子」与
/// 「剥掉的句子」会各自漂移，而漂移的后果不是多拦一次——是剥不掉，于是解释判词的
/// 回执被自己的判词再拦一次，重试又转述上一轮，每轮多一处匹配，整轮判死。
pub const NSFW_DETECTED: &str = "NSFW content detected";
pub const VIOLENCE_DETECTED: &str = "Violence content detected";
pub const HATE_SPEECH_DETECTED: &str = "Hate speech detected";
pub const SELF_HARM_DETECTED: &str = "Self-harm content detected";
pub const ILLEGAL_ACTS_DETECTED: &str = "Illegal acts content detected";

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
            // `nsfw` 是这一类内容的名字，不是一个在描述内容的词：智能体解释自己
            // 为什么被判过时，句子里必然出现它（「NSFW 误判」「NSFW 判定」）。把它
            // 当裸词拦，解释一次误判的回执自己就成了误判。与下面 `explicit` /
            // `graphic` 同一条规矩：只有它点名描述的东西时才算证据。
            r"(?i)\b(nude|naked|porn|sexual|xxx|adult content)\b".into(),
            // `explicit` and `graphic` are ordinary technical English on their own
            // ("an explicit verdict", "the graphic on the panel"), and this filter
            // judges what an agent says about its own work. They are evidence only
            // when they name the thing being described.
            r"(?i)\b(?:(?:explicit|graphic|nsfw)\s+(?:content|material|images?|scenes?|language|violence)|mature content|adult material)\b".into(),
            r"(?i)\b(nudity|intercourse|genital|erotic|obscene)\b".into(),
            // Violence — 判「动词加什么宾语」，不判「出现过哪个动词」。
            // `kill` 是这棵树里结束进程的说法（`kill -9`、`kill the process
            // group`、`SIGKILL`），`attack` 是安全工程的名词（`attack surface`、
            // `replay attack`）：当裸词拦，每一段讲后台任务、超时、网关的文字都
            // 会被判成暴力（实测 2026-11-07 05:01–05:06：三条讲 `read_file` 配
            // `run_command_background` 的能力发现任务判死于 `Violence content
            // detected`，它们的回执里没有一个字在讲人）。所以这几个动词只在宾语
            // 是人的时候算证据；不带宾语就足够有害的那几个词（自杀、恐怖、暗杀、
            // 人群枪击、安放爆炸物）单独成一支，形状自己就说明了对象。
            r"(?i)(?:\b(?:kill|murder|assassinate|attack|strangle|shoot|stab)(?:s|ed|ing)?\s+(?:the\s+|a\s+|an\s+|my\s+|his\s+|her\s+|their\s+)?(?:me|you|him|her|them|us|people|someone|somebody|everybody|everyone|anybody|children|civilians?|crowd|women|men|family)\b|\b(?:suicide|terrorist|terrorism|assassination|mass\s+shooting)\b|\b(?:plant|detonate|set\s+off)\w*\s+(?:a\s+|the\s+)?(?:bomb|explosive)\b)".into(),
            r"(?i)\b(torture|maim|slaughter|massacre|genocide|lynch)\b".into(),
            r"(?i)\b(drown|burn alive)\b".into(),
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
        // 判词不是内容。一条解释「它为什么被拦」的回执必然转述本门印的那几短语；
        // 不先剥掉，这条回执自己就会被同一条规则再拦一次，而重试时它又会转述上一
        // 轮的判词，于是每一轮都比上一轮多一处匹配，直到整轮判死（实测 2026-11-04：
        // WebUI 里两轮之后就变成 `terminal_env_failure`，分解再也跑不完）。
        let stripped = own_verdict_phrases().replace_all(text, " ");
        let text: &str = &stripped;

        // Layer 1: Regex pattern matching
        let regex_matches: Vec<usize> = self.patterns.matches(text).into_iter().collect();
        let regex_reasons: Vec<String> = regex_matches
            .iter()
            .filter_map(|&i| match i {
                0..=2 if self.config.block_nsfw => Some(NSFW_DETECTED.into()),
                3..=5 if self.config.block_violence => Some(VIOLENCE_DETECTED.into()),
                6..=8 if self.config.block_hate_speech => Some(HATE_SPEECH_DETECTED.into()),
                9 | 10 if self.config.block_self_harm => Some(SELF_HARM_DETECTED.into()),
                11 | 12 if self.config.block_illegal_acts => Some(ILLEGAL_ACTS_DETECTED.into()),
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
                rule: HARMFUL_CONTENT_RULE.into(),
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

/// 本门印在判词里的那几短语，供 [`ContentFilter::check`] 判之前剥掉。
///
/// 只认本门自己产出的写法（见 `check` 里那几行 `Some("… content detected")`），
/// 不碰别的：同样一件事换个说法讲，仍要照旧命中规则。剥掉它们不会让真实内容漏网，
/// 因为真实内容靠的是别的词，而这几个短语本身就是「有人在报告一次判定」的形状。
fn own_verdict_phrases() -> &'static regex::Regex {
    static PHRASES: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    PHRASES.get_or_init(|| {
        let alternatives: Vec<String> = [
            NSFW_DETECTED,
            VIOLENCE_DETECTED,
            HATE_SPEECH_DETECTED,
            SELF_HARM_DETECTED,
            ILLEGAL_ACTS_DETECTED,
        ]
        .iter()
        .map(|phrase| regex::escape(phrase))
        .collect();
        regex::Regex::new(&format!("(?i){}", alternatives.join("|"))).expect("本门判词的固定写法")
    })
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
