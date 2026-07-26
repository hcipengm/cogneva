use std::time::{Duration, Instant};

/// Simple token-bucket rate limiter.
/// `capacity` tokens are refilled at `refill_per_sec` per second.  Each call
/// to [`TokenBucket::try_acquire`] consumes one token if available.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_per_sec: u32) -> Self {
        // Both values are forced to a minimum of 1 so a misconfigured hook
        // (e.g. `burst: 0`) does not silently break — it just runs slowly.
        let capacity = capacity.max(1) as f64;
        let refill_per_sec = refill_per_sec.max(1) as f64;
        Self {
            capacity,
            refill_per_sec,
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last_refill = now;
        }
    }

    /// Try to consume a single token.  Returns `true` on success.
    pub fn try_acquire(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Available token count after performing a refill (mainly for tests).
    pub fn available(&mut self) -> f64 {
        self.refill();
        self.tokens
    }
}

/// Internal helper used by the dedup cache: returns true if `since` is older
/// than `window`.
pub fn is_expired(since: Instant, window: Duration) -> bool {
    Instant::now().duration_since(since) >= window
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn fresh_bucket_is_full() {
        let mut bucket = TokenBucket::new(5, 5);
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire(), "bucket should be empty after burst");
    }

    #[test]
    fn bucket_refills_over_time() {
        let mut bucket = TokenBucket::new(2, 1000); // 1000/s = 1ms per token
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire());
        std::thread::sleep(Duration::from_millis(20));
        assert!(bucket.try_acquire(), "bucket should refill within 20ms");
    }

    #[test]
    fn bucket_zero_inputs_are_clamped() {
        // capacity: 0 / refill: 0 should still work (clamped to 1).
        let mut bucket = TokenBucket::new(0, 0);
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn is_expired_works() {
        let then = Instant::now() - Duration::from_secs(2);
        assert!(is_expired(then, Duration::from_secs(1)));
        assert!(!is_expired(Instant::now(), Duration::from_secs(60)));
    }
}
