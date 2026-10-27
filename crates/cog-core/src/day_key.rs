//! The date form a day goes into a name with.
//!
//! Two kinds of name carry the day and are read back later: the rotating raw
//! log file, whose name the tiered migrator parses, and the goal ids that carry
//! the day so the same finding is only dispatched once. In both, one component
//! writes the name and a different one reads it, and neither read fails loudly
//! when the two stop agreeing — a migrator that finds no file reports a day with
//! no traffic, and a goal id that no longer matches yesterday's dispatches
//! yesterday's finding again, every pass, while every reading looks normal.
//!
//! So the form is declared once, here, where every crate that has to build such
//! a name can read it.
//!
//! `gate`: `crates/cogneva/tests/core_declared_strings_are_not_retyped_test.rs`
//! reddens when a crate outside the declaring one spells the value again.

use chrono::NaiveDate;

/// The date form a day is written in when it becomes part of a name.
pub const DAY_KEY_FORMAT: &str = "%Y-%m-%d";

/// The day key for `day`: what a dated file or a day-scoped id is named with.
pub fn day_key(day: NaiveDate) -> String {
    day.format(DAY_KEY_FORMAT).to_string()
}
