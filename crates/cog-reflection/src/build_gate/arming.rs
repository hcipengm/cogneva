//! How the executor arms the host-wide build gate (`crate::build_gate`)
//! from the configuration it was handed.
//!
//! The gate itself — slot files, flocks, counters — lives in
//! `crate::build_gate`. What lives here is the one place the whole block is
//! taken, so every reader of the gate arms the same object: the gate that
//! refuses, the wall a flight in progress is judged against, and the wall a
//! cycle's queue is planned against.
//!
//! It used to do more. The wait budget was floored at `build_timeout_secs` plus
//! a release margin, because a build was killed at that timeout and a waiter
//! queued behind a legal holder would otherwise be refused before the holder was
//! even out of bounds. Builds are no longer killed at a time — they are stopped
//! when they stop moving (`cog_core::progress`), and a build that is still
//! working gets as long as it needs. There is therefore no "longest legal hold"
//! to floor against, and a floor computed from a number nothing enforces would
//! only be a wall wearing a derivation's clothes.
//!
//! The wait budget itself stays: waiting for a slot is a queue, not a unit of
//! work, and a one-shot change verification that is told "the build never ran"
//! is being told something true rather than having its work thrown away.

use cog_core::config::BuildGateConfig;
use cog_core::SelfEvolutionConfig;

/// The gate configuration to arm for a self-evolution block.
///
/// Takes the whole block rather than one field because the readers must take
/// the same object: a caller that fetched `wait_secs` from one place and
/// `max_concurrent` from another could arm a gate different from the one the
/// readings describe.
pub fn effective_gate_for(self_evolution: &SelfEvolutionConfig) -> BuildGateConfig {
    self_evolution.build_gate.clone()
}
