//! The UTC day boundary, and the wait until the next one.
//!
//! Every counter in this crate resets at UTC midnight, and every refusal made
//! against one of them has to name that reset. The arithmetic lives in
//! [`cog_core::day`] because the process that answers a refused caller lives in
//! another crate and has to name the same instant: two spellings of the
//! boundary agree only until one of them is edited, and the way they disagree is
//! a caller told to come back one second before the counter actually resets.

pub use cog_core::day::{day_start_unix, seconds_to_next_day, SECONDS_PER_DAY};
