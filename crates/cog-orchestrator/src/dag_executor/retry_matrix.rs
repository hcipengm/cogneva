use cog_core::TaskType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Backoff strategy for retries.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries.
    Fixed { interval_ms: u64 },
    /// Linear backoff: delay = base + increment * attempt.
    Linear { base_ms: u64, increment_ms: u64 },
    /// Exponential backoff: delay = min(base * multiplier^attempt, max).
    Exponential {
        base_ms: u64,
        max_ms: u64,
        multiplier: f64,
    },
}

impl BackoffStrategy {
    /// Compute the delay for a given retry attempt (0-indexed).
    pub fn delay(&self, attempt: u32) -> Duration {
        match *self {
            BackoffStrategy::Fixed { interval_ms } => Duration::from_millis(interval_ms),
            BackoffStrategy::Linear {
                base_ms,
                increment_ms,
            } => Duration::from_millis(base_ms.saturating_add(increment_ms * attempt as u64)),
            BackoffStrategy::Exponential {
                base_ms,
                max_ms,
                multiplier,
            } => {
                let delay =
                    (base_ms as f64 * multiplier.powi(attempt as i32)).min(max_ms as f64) as u64;
                Duration::from_millis(delay)
            }
        }
    }
}

/// Circuit breaker configuration per task type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Duration the circuit stays open before transitioning to half-open.
    pub recovery_timeout_ms: u64,
    /// Max calls allowed in half-open state.
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout_ms: 30000,
            half_open_max_calls: 3,
        }
    }
}

/// Retry configuration for a specific task type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub backoff: BackoffStrategy,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: BackoffStrategy::Exponential {
                base_ms: 1000,
                max_ms: 30000,
                multiplier: 2.0,
            },
            circuit_breaker: Some(CircuitBreakerConfig::default()),
        }
    }
}

/// Per-task-type retry policy matrix.
/// Provides differentiated retry + backoff + circuit-breaker policies
/// based on the operational type of the task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryMatrix {
    default: RetryConfig,
    by_task_type: HashMap<TaskType, RetryConfig>,
}

impl Default for RetryMatrix {
    fn default() -> Self {
        Self::defaults()
    }
}

impl RetryMatrix {
    /// Build a [`RetryMatrix`] pre-populated with the standard
    pub fn defaults() -> Self {
        let mut by_task_type = HashMap::new();

        // LLM call: 3 retries, exponential [1s, 2s, 4s], CB: 5 failures → 60s
        by_task_type.insert(
            TaskType::LlmCall,
            RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential {
                    base_ms: 1000,
                    max_ms: 8000,
                    multiplier: 2.0,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 5,
                    recovery_timeout_ms: 60000,
                    half_open_max_calls: 3,
                }),
            },
        );

        // Tool call: 2 retries, fixed 3s, CB: 3 failures → 30s
        by_task_type.insert(
            TaskType::ToolCall,
            RetryConfig {
                max_retries: 2,
                backoff: BackoffStrategy::Fixed { interval_ms: 3000 },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 3,
                    recovery_timeout_ms: 30000,
                    half_open_max_calls: 2,
                }),
            },
        );

        // File op: 1 retry, immediate (0s), no CB
        by_task_type.insert(
            TaskType::FileOp,
            RetryConfig {
                max_retries: 1,
                backoff: BackoffStrategy::Fixed { interval_ms: 0 },
                circuit_breaker: None,
            },
        );

        // DB transaction: 3 retries, linear [100ms, 200ms, 300ms], CB: 10 failures → 120s
        by_task_type.insert(
            TaskType::DbTransaction,
            RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Linear {
                    base_ms: 100,
                    increment_ms: 100,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 10,
                    recovery_timeout_ms: 120000,
                    half_open_max_calls: 3,
                }),
            },
        );

        // Network request: 3 retries, exponential [500ms, 1s, 2s], CB: 5 failures → 30s
        by_task_type.insert(
            TaskType::NetworkRequest,
            RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential {
                    base_ms: 500,
                    max_ms: 2000,
                    multiplier: 2.0,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 5,
                    recovery_timeout_ms: 30000,
                    half_open_max_calls: 3,
                }),
            },
        );

        // WASM skill: 2 retries, fixed 1s, no CB
        by_task_type.insert(
            TaskType::WasmSkill,
            RetryConfig {
                max_retries: 2,
                backoff: BackoffStrategy::Fixed { interval_ms: 1000 },
                circuit_breaker: None,
            },
        );

        // Skill: 1 retry, immediate, no CB
        by_task_type.insert(
            TaskType::Skill,
            RetryConfig {
                max_retries: 1,
                backoff: BackoffStrategy::Fixed { interval_ms: 0 },
                circuit_breaker: None,
            },
        );

        // DAG node: 3 retries, exponential [5s, 10s, 20s], CB: 3 failures → DLQ
        by_task_type.insert(
            TaskType::DagNode,
            RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential {
                    base_ms: 5000,
                    max_ms: 20000,
                    multiplier: 2.0,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 3,
                    recovery_timeout_ms: 30000,
                    half_open_max_calls: 1,
                }),
            },
        );

        Self {
            default: RetryConfig::default(),
            by_task_type,
        }
    }

    /// Look up the retry configuration for a given task type.
    pub fn lookup(&self, task_type: &TaskType) -> &RetryConfig {
        self.by_task_type.get(task_type).unwrap_or(&self.default)
    }

    /// Register or override a retry configuration for a task type.
    pub fn set(&mut self, task_type: TaskType, config: RetryConfig) {
        self.by_task_type.insert(task_type, config);
    }

    /// Compute the backoff delay for a given task type and attempt.
    pub fn delay(&self, task_type: &TaskType, attempt: u32) -> Duration {
        self.lookup(task_type).backoff.delay(attempt)
    }

    /// Get the max retries for a given task type.
    pub fn max_retries(&self, task_type: &TaskType) -> u32 {
        self.lookup(task_type).max_retries
    }

    /// Get the circuit breaker config for a given task type (if any).
    pub fn circuit_breaker(&self, task_type: &TaskType) -> Option<&CircuitBreakerConfig> {
        self.lookup(task_type).circuit_breaker.as_ref()
    }

    /// Check if a task type has a circuit breaker configured.
    pub fn has_circuit_breaker(&self, task_type: &TaskType) -> bool {
        self.lookup(task_type).circuit_breaker.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_matrix_defaults_llm_call() {
        let matrix = RetryMatrix::defaults();
        let cfg = matrix.lookup(&TaskType::LlmCall);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.backoff.delay(0).as_secs(), 1);
        assert_eq!(cfg.backoff.delay(1).as_secs(), 2);
        assert_eq!(cfg.backoff.delay(2).as_secs(), 4);
        assert_eq!(cfg.backoff.delay(10).as_secs(), 8); // capped
        assert!(cfg.circuit_breaker.is_some());
        let cb = cfg.circuit_breaker.unwrap();
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.recovery_timeout_ms, 60000);
    }

    #[test]
    fn test_retry_matrix_defaults_db_transaction() {
        let matrix = RetryMatrix::defaults();
        let cfg = matrix.lookup(&TaskType::DbTransaction);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.backoff.delay(0).as_millis(), 100);
        assert_eq!(cfg.backoff.delay(1).as_millis(), 200);
        assert_eq!(cfg.backoff.delay(2).as_millis(), 300);
        assert_eq!(cfg.backoff.delay(5).as_millis(), 600); // linear, no cap needed
        assert!(cfg.circuit_breaker.is_some());
    }

    #[test]
    fn test_retry_matrix_defaults_file_op() {
        let matrix = RetryMatrix::defaults();
        let cfg = matrix.lookup(&TaskType::FileOp);
        assert_eq!(cfg.max_retries, 1);
        assert_eq!(cfg.backoff.delay(0).as_millis(), 0);
        assert!(cfg.circuit_breaker.is_none());
    }

    #[test]
    fn test_retry_matrix_lookup_unknown_uses_default() {
        let matrix = RetryMatrix::defaults();
        // Planner is not explicitly configured in defaults → falls back
        let cfg = matrix.lookup(&TaskType::Planner);
        assert_eq!(cfg, &RetryConfig::default());
    }

    #[test]
    fn test_retry_matrix_set_override() {
        let mut matrix = RetryMatrix::defaults();
        let custom = RetryConfig {
            max_retries: 5,
            backoff: BackoffStrategy::Fixed { interval_ms: 500 },
            circuit_breaker: None,
        };
        matrix.set(TaskType::Planner, custom.clone());
        let cfg = matrix.lookup(&TaskType::Planner);
        assert_eq!(cfg.max_retries, 5);
    }

    #[test]
    fn test_retry_matrix_delay_wrapper() {
        let matrix = RetryMatrix::defaults();
        assert_eq!(matrix.delay(&TaskType::LlmCall, 1).as_secs(), 2);
        assert_eq!(matrix.max_retries(&TaskType::ToolCall), 2);
    }
}
