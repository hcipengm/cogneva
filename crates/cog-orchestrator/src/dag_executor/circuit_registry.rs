use cog_core::resilience::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use cog_core::{SFError, SFResult, TaskType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::dag_executor::retry_matrix::RetryMatrix;

/// A registry that maintains a [`CircuitBreaker`] per task type.
/// The registry looks up the circuit-breaker configuration from the
/// [`RetryMatrix`] and lazily constructs one when a type is first
/// referenced.
pub struct CircuitBreakerRegistry {
    breakers: Mutex<HashMap<TaskType, Arc<CircuitBreaker>>>,
    matrix: Arc<RetryMatrix>,
}

impl CircuitBreakerRegistry {
    pub fn new(matrix: Arc<RetryMatrix>) -> Self {
        Self {
            breakers: Mutex::new(HashMap::new()),
            matrix,
        }
    }

    /// Get (or create) the circuit breaker for a task type.
    fn get_or_create(&self, task_type: &TaskType) -> SFResult<Option<Arc<CircuitBreaker>>> {
        let mut map = self
            .breakers
            .lock()
            .map_err(|_| SFError::Agent("circuit registry lock poisoned".into()))?;

        if let Some(cb) = map.get(task_type) {
            return Ok(Some(cb.clone()));
        }

        let cfg = self.matrix.circuit_breaker(task_type);
        let cb = match cfg {
            Some(cfg) => {
                let breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
                    failure_threshold: cfg.failure_threshold,
                    success_threshold: cfg.half_open_max_calls,
                    timeout_ms: cfg.recovery_timeout_ms,
                }));
                map.insert(task_type.clone(), breaker.clone());
                Some(breaker)
            }
            None => None,
        };
        Ok(cb)
    }

    /// Check if the circuit breaker allows a call for the given task type.
    /// Returns `Ok(())` when there is no circuit breaker or it is closed.
    /// Returns `Err` when the circuit is open.
    pub fn allow(&self, task_type: &TaskType) -> SFResult<()> {
        match self.get_or_create(task_type)? {
            Some(cb) => cb.allow(),
            None => Ok(()),
        }
    }

    /// Record a successful call for the given task type.
    pub fn record_success(&self, task_type: &TaskType) -> SFResult<()> {
        if let Some(cb) = self.get_or_create(task_type)? {
            cb.record_success();
        }
        Ok(())
    }

    /// Record a failed call for the given task type.
    pub fn record_failure(&self, task_type: &TaskType) -> SFResult<()> {
        if let Some(cb) = self.get_or_create(task_type)? {
            cb.record_failure();
        }
        Ok(())
    }

    /// Get the current circuit state for a task type.
    pub fn state(&self, task_type: &TaskType) -> SFResult<Option<CircuitState>> {
        match self.get_or_create(task_type)? {
            Some(cb) => Ok(Some(cb.state())),
            None => Ok(None),
        }
    }

    /// Reset all circuit breakers (useful for admin operations or tests).
    pub fn reset_all(&self) -> SFResult<()> {
        let mut map = self
            .breakers
            .lock()
            .map_err(|_| SFError::Agent("circuit registry lock poisoned".into()))?;
        map.clear();
        Ok(())
    }

    /// Reset a specific circuit breaker.
    pub fn reset(&self, task_type: &TaskType) -> SFResult<()> {
        let mut map = self
            .breakers
            .lock()
            .map_err(|_| SFError::Agent("circuit registry lock poisoned".into()))?;
        map.remove(task_type);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_registry() -> CircuitBreakerRegistry {
        CircuitBreakerRegistry::new(Arc::new(RetryMatrix::defaults()))
    }

    #[test]
    fn test_circuit_registry_allow_no_cb() {
        let reg = make_registry();
        // FileOp has no circuit breaker in defaults
        assert!(reg.allow(&TaskType::FileOp).is_ok());
    }

    #[test]
    fn test_circuit_registry_opens_after_threshold() {
        let reg = make_registry();
        // LlmCall has threshold 5 in defaults
        for _ in 0..5 {
            reg.record_success(&TaskType::LlmCall).unwrap();
        }
        assert!(reg.allow(&TaskType::LlmCall).is_ok());

        for _ in 0..5 {
            reg.record_failure(&TaskType::LlmCall).unwrap();
        }

        let state = reg.state(&TaskType::LlmCall).unwrap();
        assert_eq!(state, Some(CircuitState::Open));

        // Should deny
        assert!(reg.allow(&TaskType::LlmCall).is_err());
    }

    #[test]
    fn test_circuit_registry_half_open_recovery() {
        let _reg = make_registry();
        // Use a type with low thresholds via a custom matrix
        let mut matrix = RetryMatrix::defaults();
        matrix.set(
            TaskType::ToolCall,
            crate::dag_executor::retry_matrix::RetryConfig {
                max_retries: 2,
                backoff: crate::dag_executor::retry_matrix::BackoffStrategy::Fixed {
                    interval_ms: 0,
                },
                circuit_breaker: Some(crate::dag_executor::retry_matrix::CircuitBreakerConfig {
                    failure_threshold: 1,
                    recovery_timeout_ms: 10,
                    half_open_max_calls: 1,
                }),
            },
        );
        let reg = CircuitBreakerRegistry::new(Arc::new(matrix));

        reg.record_failure(&TaskType::ToolCall).unwrap();
        assert_eq!(
            reg.state(&TaskType::ToolCall).unwrap(),
            Some(CircuitState::Open)
        );

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(reg.allow(&TaskType::ToolCall).is_ok()); // HalfOpen
        assert_eq!(
            reg.state(&TaskType::ToolCall).unwrap(),
            Some(CircuitState::HalfOpen)
        );

        reg.record_success(&TaskType::ToolCall).unwrap();
        assert_eq!(
            reg.state(&TaskType::ToolCall).unwrap(),
            Some(CircuitState::Closed)
        );
    }

    #[test]
    fn test_circuit_registry_reset() {
        let reg = make_registry();
        reg.record_failure(&TaskType::LlmCall).unwrap();
        reg.record_failure(&TaskType::LlmCall).unwrap();
        reg.record_failure(&TaskType::LlmCall).unwrap();
        reg.record_failure(&TaskType::LlmCall).unwrap();
        reg.record_failure(&TaskType::LlmCall).unwrap();

        assert!(reg.allow(&TaskType::LlmCall).is_err());

        reg.reset(&TaskType::LlmCall).unwrap();
        assert!(reg.allow(&TaskType::LlmCall).is_ok());
    }
}
