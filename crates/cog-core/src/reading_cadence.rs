//! How often a reading whose value has not changed is written again anyway.
//!
//! A gauge that only writes when its value moves is, from the reader's side,
//! indistinguishable from a writer that is gone: both leave a series whose last
//! sample is old. What separates them is the age of that sample, and the age is
//! only an answer while the writer keeps stamping a value that is not moving —
//! so the writer owns the cadence, and every alert window that reads such a
//! series has to be strictly longer than it. A window equal to the cadence is
//! satisfied by a healthy writer sitting between two of its own stamps.
//!
//! There is one cadence for the deployment rather than one per loop. Two would
//! turn "how long may this series be silent" into a per-series fact that every
//! rule has to remember, and the rule that forgot would fire on a working
//! system. The value lives here because the loops that stamp under it are in
//! different crates — the worktree index sampler, the change funnel census and
//! the durable pool gauges — and a constant copied into each of them cannot be
//! compared to the others, so a fourth copy with a third value would be
//! invisible.
//!
//! This is a declared reference point, not a dial. It is not read from
//! configuration on purpose: what has to hold is the *relation* between this
//! cadence and each reader's window, and a relation between two settings cannot
//! be checked by anything when both can move independently.

use std::time::Duration;

/// The period at which a reading that has not changed is written again.
///
/// 1800 is the point where a permanently silent series is still told apart from
/// a writer that stopped: long enough that the write cost of an idle deployment
/// stays irrelevant, short enough that an alert window twice it — the smallest
/// window one can reasonably hand to a rule — still sees a stamp from a writer
/// that is alive.
pub const UNCHANGED_READING_HEARTBEAT: Duration = Duration::from_secs(1800);
