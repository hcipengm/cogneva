//! Build-cache reclaim gate accounting.
//!
//! A reclaim pass must never delete while a build holds the host-wide build
//! gate: removing a file out from under a running build fails that build for a
//! reason unrelated to the build itself. But "a build is running" and "no gate
//! is in force" are opposite conditions with opposite responses:
//!
//! * [`GateView::Busy`] - the host-wide gate is held by the process building a
//!   deployment's changes. Deletion is skipped this pass.
//! * [`GateView::Ungated`] - no gate is in force at all. Nothing is building,
//!   so the pass must pay bytes down over the cap instead of skipping.
//!
//! The sandbox executor holds no slot in the host-wide gate and only answers
//! with its own running commands. Sandbox activity therefore never blocks host
//! cache reclamation; it is tracked separately as [`GateView::SandboxBusy`]
//! and is reclaimable exactly like the ungated case.
#![allow(dead_code)]

/// Identity of the slot a running command holds, resolved from the dir label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotKind {
    /// The host-wide build gate, held while a deployment's changes build.
    HostBuildGate,
    /// The sandbox executor's own command slot; not part of the host gate.
    SandboxExecutor,
}

/// What the reclaim pass observed about the build slot this scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateView {
    /// Host-wide build gate held by the deployment builder: do not delete.
    Busy,
    /// Sandbox executor has commands running, but it holds no host slot.
    SandboxBusy,
    /// No gate is in force: safe and required to reclaim over-cap bytes.
    Ungated,
}

impl GateView {
    /// Resolve a slot observation into a reclaim decision.
    ///
    /// Only [`SlotKind::HostBuildGate`] blocks deletion. Sandbox commands are
    /// not participants in the host-wide gate and must not be counted as busy.
    pub fn from_held_slot(slot: Option<SlotKind>) -> Self {
        match slot {
            Some(SlotKind::HostBuildGate) => GateView::Busy,
            Some(SlotKind::SandboxExecutor) => GateView::SandboxBusy,
            None => GateView::Ungated,
        }
    }

    /// Whether deletion is forbidden this pass. Solely the host gate forbids.
    pub fn deletion_forbidden(&self) -> bool {
        matches!(self, GateView::Busy)
    }
}

/// The outcome of one reclaim pass, counted independently on the
/// "Build Cache Over-Cap Outcomes" panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OutcomeCounters {
    /// Pass skipped because the host build gate was held.
    pub busy: u64,
    /// Pass that reclaimed because no host gate was in force.
    pub ungated: u64,
    /// Bytes deleted across passes that reclaimed.
    pub reclaimed_bytes: u64,
}

/// A cache entry considered for eviction, ordered oldest-first by the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub size: u64,
    pub last_used_epoch: u64,
}

/// Result of a single pass over one cache directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassOutcome {
    /// Host build was running; no bytes touched.
    Busy,
    /// Reclaim ran (ungated or sandbox-only); reports bytes deleted.
    Reclaimed { bytes: u64 },
    /// At or under the cap; nothing needed to move.
    UnderCap,
}

/// Run one reclaim pass.
///
/// `over_cap` is `current_size.saturating_sub(cap)`; `entries` must be ordered
/// oldest / least-recently-used first so eviction pays down LRU bytes.
///
/// The measurement age for the scan is renewed by the walker on every return
/// path, including [`PassOutcome::Busy`]; the age rising without bound is the
/// signature of a walker that stopped walking, not of sustained busy (which
/// pins the age near the scan interval).
pub fn run_pass(
    gate: GateView,
    over_cap: u64,
    entries: &[Entry],
    counters: &mut OutcomeCounters,
) -> PassOutcome {
    if gate.deletion_forbidden() {
        counters.busy += 1;
        return PassOutcome::Busy;
    }
    if over_cap == 0 {
        return PassOutcome::UnderCap;
    }

    // Ungated and SandboxBusy both reach here: neither holds the host slot.
    counters.ungated += 1;
    let mut to_pay = over_cap;
    let mut paid = 0u64;
    for entry in entries {
        if to_pay == 0 {
            break;
        }
        let take = entry.size.min(to_pay);
        paid += take;
        to_pay -= take;
    }
    counters.reclaimed_bytes = counters.reclaimed_bytes.saturating_add(paid);
    PassOutcome::Reclaimed { bytes: paid }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lru() -> Vec<Entry> {
        vec![
            Entry { size: 30_000_000, last_used_epoch: 1 },
            Entry { size: 30_000_000, last_used_epoch: 2 },
            Entry { size: 30_000_000, last_used_epoch: 3 },
        ];
    }

    #[test]
    fn ungated_over_cap_is_reclaimed_lru_first() {
        let mut c = OutcomeCounters::default();
        let out = run_pass(GateView::Ungated, 72_349_314, &lru(), &mut c);
        assert_eq!(out, PassOutcome::Reclaimed { bytes: 72_349_314 });
        assert_eq!(c.busy, 0);
        assert_eq!(c.ungated, 1);
        assert_eq!(c.reclaimed_bytes, 72_349_314);
    }

    #[test]
    fn busy_gate_blocks_deletion_and_moves_nothing() {
        let mut c = OutcomeCounters::default();
        let out = run_pass(GateView::Busy, 72_349_314, &lru(), &mut c);
        assert_eq!(out, PassOutcome::Busy);
        assert_eq!(c.busy, 1);
        assert_eq!(c.ungated, 0);
        assert_eq!(c.reclaimed_bytes, 0);
    }

    #[test]
    fn sandbox_activity_does_not_register_as_host_busy() {
        assert_eq!(
            GateView::from_held_slot(Some(SlotKind::SandboxExecutor)),
            GateView::SandboxBusy
        );
        assert!(!GateView::SandboxBusy.deletion_forbidden());
        let mut c = OutcomeCounters::default();
        let out = run_pass(GateView::SandboxBusy, 10, &lru(), &mut c);
        assert_eq!(out, PassOutcome::Reclaimed { bytes: 10 });
        assert_eq!(c.busy, 0);
        assert_eq!(c.ungated, 1);
    }

    #[test]
    fn host_gate_is_the_only_busy_slot() {
        assert_eq!(
            GateView::from_held_slot(Some(SlotKind::HostBuildGate)),
            GateView::Busy
        );
        assert_eq!(GateView::from_held_slot(None), GateView::Ungated);
        assert!(GateView::Busy.deletion_forbidden());
        assert!(!GateView::Ungated.deletion_forbidden());
    }

    #[test]
    fn under_cap_moves_nothing() {
        let mut c = OutcomeCounters::default();
        let out = run_pass(GateView::Ungated, 0, &lru(), &mut c);
        assert_eq!(out, PassOutcome::UnderCap);
        assert_eq!(c.ungated, 0);
        assert_eq!(c.reclaimed_bytes, 0);
    }
}
