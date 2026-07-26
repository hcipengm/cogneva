use crate::{SFError, SFResult};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default high watermark: when the buffer reaches this level, the producer pauses.
/// Zero in core — real defaults live in the assembly-layer config loader.
pub const DEFAULT_HIGH_WATERMARK: usize = 0;
/// Default low watermark: when the buffer drops below this level, the producer resumes.
/// Zero in core — real defaults live in the assembly-layer config loader.
pub const DEFAULT_LOW_WATERMARK: usize = 0;

/// Configuration for backpressure watermarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BackpressureConfig {
    pub high_watermark: usize,
    pub low_watermark: usize,
}

impl BackpressureConfig {
    /// Create a new config with the given watermarks.
    pub const fn new(high_watermark: usize, low_watermark: usize) -> Self {
        Self {
            high_watermark,
            low_watermark,
        }
    }
}

/// Error type for backpressure-aware push operations.
#[derive(Debug, thiserror::Error)]
pub enum BackpressureError<T> {
    #[error("High watermark reached")]
    HighWatermark(T),
    #[error("Send error: {0}")]
    Send(#[from] tokio::sync::mpsc::error::SendError<T>),
}

// ─── Circuit Breaker ───

/// State of a circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Configuration for a circuit breaker.
#[derive(Debug, Clone, Default)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Consecutive successes in half-open before closing.
    pub success_threshold: u32,
    /// Duration the circuit stays open before transitioning to half-open.
    pub timeout_ms: u64,
}

/// Thread-safe circuit breaker for protecting downstream calls from cascading failures.
pub struct CircuitBreaker {
    cfg: CircuitBreakerConfig,
    state: Mutex<CircuitState>,
    consecutive_failures: AtomicU32,
    consecutive_successes: AtomicU32,
    last_failure_time: Mutex<Option<Instant>>,
}

impl CircuitBreaker {
    pub fn new(cfg: CircuitBreakerConfig) -> Self {
        Self {
            cfg,
            state: Mutex::new(CircuitState::Closed),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            last_failure_time: Mutex::new(None),
        }
    }

    pub fn state(&self) -> CircuitState {
        *self.state.lock().unwrap()
    }

    /// Check if the circuit allows a request through.
    /// Returns Err if the circuit is open.
    pub fn allow(&self) -> SFResult<()> {
        let mut state = self.state.lock().unwrap();
        match *state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open => {
                let last = *self.last_failure_time.lock().unwrap();
                let elapsed = last.map(|t| t.elapsed()).unwrap_or(Duration::MAX);
                if elapsed >= Duration::from_millis(self.cfg.timeout_ms) {
                    *state = CircuitState::HalfOpen;
                    self.consecutive_successes.store(0, Ordering::SeqCst);
                    Ok(())
                } else {
                    Err(SFError::Agent("circuit breaker is open".into()))
                }
            }
            CircuitState::HalfOpen => Ok(()),
        }
    }

    /// Record a successful call.
    pub fn record_success(&self) {
        let mut state = self.state.lock().unwrap();
        self.consecutive_failures.store(0, Ordering::SeqCst);
        match *state {
            CircuitState::HalfOpen => {
                let successes = self.consecutive_successes.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.cfg.success_threshold {
                    *state = CircuitState::Closed;
                    self.consecutive_successes.store(0, Ordering::SeqCst);
                }
            }
            _ => {
                self.consecutive_successes.store(0, Ordering::SeqCst);
            }
        }
    }

    /// Record a failed call.
    pub fn record_failure(&self) {
        let mut state = self.state.lock().unwrap();
        self.consecutive_successes.store(0, Ordering::SeqCst);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        *self.last_failure_time.lock().unwrap() = Some(Instant::now());
        if failures >= self.cfg.failure_threshold {
            *state = CircuitState::Open;
        }
    }
}
