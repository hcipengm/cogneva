use std::sync::atomic::{AtomicBool, Ordering};

/// Cooperative pause signal for the autonomous scheduler.
/// The Supervisor flips this gate when a quota threshold is breached or
/// when a manual operator pause is requested.  The autonomous executor
/// loop in `cogneva` checks the gate at the top of every tick and
/// skips task scheduling while it is paused.
/// The gate is intentionally a single shared atomic — we want zero
/// allocation cost on the hot path of the scheduler.
#[derive(Debug, Default)]
pub struct SchedulerGate {
    paused: AtomicBool,
}

impl SchedulerGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` while the scheduler is paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Pause the scheduler.  Returns the previous state.
    pub fn pause(&self) -> bool {
        self.paused.swap(true, Ordering::SeqCst)
    }

    /// Resume the scheduler.  Returns the previous state.
    pub fn resume(&self) -> bool {
        self.paused.swap(false, Ordering::SeqCst)
    }
}

impl cog_core::SchedulerGate for SchedulerGate {
    fn is_paused(&self) -> bool {
        self.is_paused()
    }

    fn pause(&self) -> bool {
        self.pause()
    }

    fn resume(&self) -> bool {
        self.resume()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_running() {
        let gate = SchedulerGate::new();
        assert!(!gate.is_paused());
    }

    #[test]
    fn pause_and_resume_round_trip() {
        let gate = SchedulerGate::new();
        let was = gate.pause();
        assert!(!was);
        assert!(gate.is_paused());
        let was = gate.resume();
        assert!(was);
        assert!(!gate.is_paused());
    }
}
