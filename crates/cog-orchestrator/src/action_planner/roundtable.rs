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

impl Default for RoundtableConfig {
    fn default() -> Self {
        Self {
            max_rounds: 3,
            consensus_threshold: 0.8,
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
