use std::sync::atomic::{AtomicBool, Ordering};

use cog_core::TaskClass;

/// Cooperative pause signal for the autonomous scheduler.
///
/// Two independent levels:
/// - the **global** switch (`pause`/`resume`) is the operator/emergency stop
///   and halts every task class;
/// - the **per-class** switch (`pause_kind`/`resume_kind`) pauses one class
///   only, so an unavailable LLM upstream pool stops LLM-dependent work while
///   builds, deployments and metric collection keep running.
///
/// Both levels are single atomics: the scheduler hot path must stay
/// allocation-free.
#[derive(Debug, Default)]
pub struct SchedulerGate {
    paused: AtomicBool,
    llm_dependent_paused: AtomicBool,
}

impl SchedulerGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` while the global switch is set.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Set the global switch. Returns the previous state.
    pub fn pause(&self) -> bool {
        self.paused.swap(true, Ordering::SeqCst)
    }

    /// Clear the global switch. Returns the previous state.
    pub fn resume(&self) -> bool {
        self.paused.swap(false, Ordering::SeqCst)
    }

    /// Returns `true` when `class` must not run right now — either the global
    /// switch is set or the class's own switch is.
    pub fn is_paused_kind(&self, class: TaskClass) -> bool {
        self.is_paused()
            || self
                .class_flag(class)
                .is_some_and(|f| f.load(Ordering::SeqCst))
    }

    /// Pause one task class. Returns the class's previous state. Classes with
    /// no switch of their own (mechanical work) are unaffected.
    pub fn pause_kind(&self, class: TaskClass) -> bool {
        self.class_flag(class)
            .is_some_and(|f| f.swap(true, Ordering::SeqCst))
    }

    /// Resume one task class. Returns the class's previous state.
    pub fn resume_kind(&self, class: TaskClass) -> bool {
        self.class_flag(class)
            .is_some_and(|f| f.swap(false, Ordering::SeqCst))
    }

    /// The class's own switch. Mechanical work has none: it is only ever
    /// stopped by the global operator pause.
    fn class_flag(&self, class: TaskClass) -> Option<&AtomicBool> {
        match class {
            TaskClass::LlmDependent => Some(&self.llm_dependent_paused),
            TaskClass::Mechanical => None,
        }
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

    fn is_paused_kind(&self, class: TaskClass) -> bool {
        self.is_paused_kind(class)
    }

    fn pause_kind(&self, class: TaskClass) -> bool {
        self.pause_kind(class)
    }

    fn resume_kind(&self, class: TaskClass) -> bool {
        self.resume_kind(class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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

    #[test]
    fn class_pause_leaves_mechanical_running() {
        let gate = SchedulerGate::new();
        assert!(!gate.pause_kind(TaskClass::LlmDependent));
        assert!(gate.is_paused_kind(TaskClass::LlmDependent));
        assert!(!gate.is_paused_kind(TaskClass::Mechanical));
        assert!(
            !gate.is_paused(),
            "class pause must not set the global flag"
        );

        assert!(gate.resume_kind(TaskClass::LlmDependent));
        assert!(!gate.is_paused_kind(TaskClass::LlmDependent));
    }

    #[test]
    fn global_pause_stops_every_class() {
        let gate = SchedulerGate::new();
        gate.pause();
        assert!(gate.is_paused_kind(TaskClass::LlmDependent));
        assert!(gate.is_paused_kind(TaskClass::Mechanical));
    }

    #[test]
    fn mechanical_has_no_class_switch() {
        let gate = SchedulerGate::new();
        assert!(!gate.pause_kind(TaskClass::Mechanical));
        assert!(!gate.is_paused_kind(TaskClass::Mechanical));
        assert!(!gate.resume_kind(TaskClass::Mechanical));
    }

    /// The gate is consumed as `Arc<dyn cog_core::SchedulerGate>`; a class pause
    /// that only works through the concrete type would silently no-op in
    /// production. Exercise it through dynamic dispatch.
    #[test]
    fn class_pause_through_dyn_trait() {
        let gate: Arc<dyn cog_core::SchedulerGate> = Arc::new(SchedulerGate::new());
        gate.pause_kind(TaskClass::LlmDependent);
        assert!(gate.is_paused_kind(TaskClass::LlmDependent));
        assert!(!gate.is_paused_kind(TaskClass::Mechanical));
        gate.resume_kind(TaskClass::LlmDependent);
        assert!(!gate.is_paused_kind(TaskClass::LlmDependent));
    }
}
