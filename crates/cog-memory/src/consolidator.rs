use cog_core::SFResult;
use cog_core::{SchemaEntry, SummaryEntry};
use std::collections::HashMap;

/// Strategy for resolving conflicts between two versions of the same fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationStrategy {
    /// Keep the newer entry (based on extracted_at / generated_at).
    KeepNewer,
    /// Keep the entry with the higher confidence score.
    KeepHigherConfidence,
    /// Preserve both entries and let downstream consumers decide.
    PreserveBoth,
}

/// Resolves conflicts when the same entity, relation, or summary is
/// extracted from multiple raw sources or at different times.
/// The consolidator compares entries that refer to the same conceptual
/// fact (e.g. two extractions of the same entity) and applies a
/// [`ConsolidationStrategy`] to produce a canonical result.
#[derive(Debug, Clone)]
pub struct MemoryConsolidator {
    strategy: ConsolidationStrategy,
}

impl MemoryConsolidator {
    pub fn new(strategy: ConsolidationStrategy) -> Self {
        Self { strategy }
    }

    /// Merge two schema entries that represent the same fact.
    /// Returns `Some(entry)` if a single winner is chosen, or `None` when
    /// the strategy is `PreserveBoth`.
    pub fn merge_schema(&self, a: &SchemaEntry, b: &SchemaEntry) -> SFResult<Option<SchemaEntry>> {
        match self.strategy {
            ConsolidationStrategy::KeepNewer => {
                let winner = if b.extracted_at > a.extracted_at {
                    b.clone()
                } else {
                    a.clone()
                };
                Ok(Some(winner))
            }
            ConsolidationStrategy::KeepHigherConfidence => {
                let winner = if b.confidence > a.confidence {
                    b.clone()
                } else {
                    a.clone()
                };
                Ok(Some(winner))
            }
            ConsolidationStrategy::PreserveBoth => Ok(None),
        }
    }

    /// Merge two summary entries that represent the same raw source.
    /// Returns `Some(entry)` if a single winner is chosen, or `None` when
    /// the strategy is `PreserveBoth`.
    pub fn merge_summary(
        &self,
        a: &SummaryEntry,
        b: &SummaryEntry,
    ) -> SFResult<Option<SummaryEntry>> {
        match self.strategy {
            ConsolidationStrategy::KeepNewer => {
                let winner = if b.generated_at > a.generated_at {
                    b.clone()
                } else {
                    a.clone()
                };
                Ok(Some(winner))
            }
            ConsolidationStrategy::KeepHigherConfidence => {
                let winner = if b.confidence > a.confidence {
                    b.clone()
                } else {
                    a.clone()
                };
                Ok(Some(winner))
            }
            ConsolidationStrategy::PreserveBoth => Ok(None),
        }
    }

    /// Deduplicate a list of schema entries by `key`, applying the
    /// configured strategy for each collision.
    pub fn deduplicate_schema(&self, entries: Vec<SchemaEntry>) -> SFResult<Vec<SchemaEntry>> {
        let mut by_key: HashMap<String, Vec<SchemaEntry>> = HashMap::new();
        for e in entries {
            by_key.entry(e.key.clone()).or_default().push(e);
        }

        let mut result = Vec::new();
        for (_key, mut group) in by_key {
            if group.len() == 1 {
                result.push(group.into_iter().next().unwrap());
                continue;
            }

            match self.strategy {
                ConsolidationStrategy::PreserveBoth => result.extend(group),
                _ => {
                    let mut winner = group.remove(0);
                    for other in group {
                        if let Some(new_winner) = self.merge_schema(&winner, &other)? {
                            winner = new_winner;
                        }
                    }
                    result.push(winner);
                }
            }
        }
        Ok(result)
    }
}

impl Default for MemoryConsolidator {
    fn default() -> Self {
        Self::new(ConsolidationStrategy::KeepHigherConfidence)
    }
}
