use async_trait::async_trait;
use cog_core::SFResult;
use std::sync::Arc;

/// One round of argument from a debate participant.
#[derive(Debug, Clone)]
pub struct DebateRound {
    pub participant_id: String,
    pub argument: String,
}

/// A participant in the roundtable debate.
#[async_trait]
pub trait Debater: Send + Sync {
    async fn debate(&self, goal: &str, previous_arguments: &[DebateRound]) -> SFResult<String>;
}

/// Configuration for [`Roundtable`] execution.
#[derive(Debug, Clone)]
pub struct RoundtableConfig {
    pub max_rounds: u32,
    pub consensus_threshold: f32,
}

// 三轮：第一轮各说各的，第二轮回应对方的论据，第三轮之后新的一轮已经不再改变
// 谁被说服——再辩只是在同一组分歧上换措辞。
// cap: bound — 三轮：之后新的一轮不再改变谁被说服
const DEBATE_MAX_ROUNDS: u32 = 3;

// 八成：多数即收口。五五开说明没有形成结论；要求全体一致则被一条少数意见钉死。
// cap: bound — 0.8：多数即收口；全体一致会被少数意见钉死
const DEBATE_CONSENSUS: f32 = 0.8;

impl Default for RoundtableConfig {
    fn default() -> Self {
        Self {
            max_rounds: DEBATE_MAX_ROUNDS,
            consensus_threshold: DEBATE_CONSENSUS,
        }
    }
}

/// Iterative debate loop with multiple participants and consensus checking.
pub struct Roundtable {
    participants: Vec<Arc<dyn Debater>>,
    config: RoundtableConfig,
}

impl Roundtable {
    pub fn new(participants: Vec<Arc<dyn Debater>>) -> Self {
        Self {
            participants,
            config: RoundtableConfig::default(),
        }
    }

    pub fn with_config(mut self, config: RoundtableConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn execute(&self, goal: &str) -> SFResult<String> {
        let mut rounds: Vec<DebateRound> = Vec::new();

        for _ in 0..self.config.max_rounds {
            let mut round_arguments = Vec::new();
            for (idx, participant) in self.participants.iter().enumerate() {
                let argument = participant.debate(goal, &rounds).await?;
                round_arguments.push(DebateRound {
                    participant_id: format!("participant-{}", idx),
                    argument,
                });
            }

            // Simple consensus check: if last 2 participants agree
            // (string similarity > threshold)
            if round_arguments.len() >= 2 {
                let last = &round_arguments[round_arguments.len() - 1].argument;
                let prev = &round_arguments[round_arguments.len() - 2].argument;
                let similarity = simple_similarity(last, prev);
                if similarity >= self.config.consensus_threshold {
                    return Ok(last.clone());
                }
            }

            rounds.extend(round_arguments);
        }

        // No consensus: return the last argument
        Ok(rounds
            .last()
            .map(|r| r.argument.clone())
            .unwrap_or_default())
    }
}

/// Simple Jaccard similarity on words.
fn simple_similarity(a: &str, b: &str) -> f32 {
    let a_words: std::collections::HashSet<String> = a
        .to_lowercase()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    let b_words: std::collections::HashSet<String> = b
        .to_lowercase()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    let intersection: std::collections::HashSet<_> = a_words.intersection(&b_words).collect();
    let union: std::collections::HashSet<_> = a_words.union(&b_words).collect();
    if union.is_empty() {
        0.0
    } else {
        intersection.len() as f32 / union.len() as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 辩论轮次与共识线各只有一处声明：默认值读常量，源码面 `impl Default` 的
    /// 字段里不再有数字（下一笔写回裸字面量就红）。
    #[test]
    fn the_debate_default_reads_its_rounds_and_consensus_line() {
        let cfg = RoundtableConfig::default();
        assert_eq!(cfg.max_rounds, DEBATE_MAX_ROUNDS);
        assert_eq!(cfg.consensus_threshold, DEBATE_CONSENSUS);

        let body = include_str!("roundtable.rs")
            .split("impl Default for RoundtableConfig")
            .nth(1)
            .expect("the impl is in this file")
            .split("\n}\n")
            .next()
            .unwrap();
        let bare: Vec<&str> = body
            .lines()
            .filter(|line| {
                line.split_once(':').is_some_and(|(field, value)| {
                    let value = value.trim().trim_end_matches(',');
                    !field.is_empty()
                        && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                        && value.starts_with(|c: char| c.is_ascii_digit())
                        && value != "0"
                })
            })
            .map(str::trim)
            .collect();
        assert!(bare.is_empty(), "默认值要读具名常量：{bare:?}");
    }
    use async_trait::async_trait;

    struct EchoDebater {
        response: String,
    }

    #[async_trait]
    impl Debater for EchoDebater {
        async fn debate(
            &self,
            _goal: &str,
            _previous_arguments: &[DebateRound],
        ) -> SFResult<String> {
            Ok(self.response.clone())
        }
    }

    #[tokio::test]
    async fn test_roundtable_consensus() {
        let participants: Vec<Arc<dyn Debater>> = vec![
            Arc::new(EchoDebater {
                response: "agreed solution alpha".into(),
            }),
            Arc::new(EchoDebater {
                response: "agreed solution alpha".into(),
            }),
        ];
        let roundtable = Roundtable::new(participants);
        let result = roundtable.execute("solve x").await.unwrap();
        assert_eq!(result, "agreed solution alpha");
    }

    #[tokio::test]
    async fn test_roundtable_no_consensus() {
        let participants: Vec<Arc<dyn Debater>> = vec![
            Arc::new(EchoDebater {
                response: "solution A is best".into(),
            }),
            Arc::new(EchoDebater {
                response: "solution B is best".into(),
            }),
        ];
        let roundtable = Roundtable::new(participants).with_config(RoundtableConfig {
            max_rounds: 2,
            consensus_threshold: 0.8,
        });
        let result = roundtable.execute("solve x").await.unwrap();
        // No consensus, returns last argument
        assert_eq!(result, "solution B is best");
    }

    #[test]
    fn test_simple_similarity_identical() {
        assert_eq!(simple_similarity("hello world", "hello world"), 1.0);
    }

    #[test]
    fn test_simple_similarity_disjoint() {
        assert_eq!(simple_similarity("abc def", "ghi jkl"), 0.0);
    }

    #[test]
    fn test_simple_similarity_partial() {
        let sim = simple_similarity("hello world foo", "hello world bar");
        // intersection = {hello, world} = 2, union = {hello, world, foo, bar} = 4
        assert!((sim - 0.5).abs() < f32::EPSILON);
    }
}
