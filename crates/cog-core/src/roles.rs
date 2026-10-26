//! The names the platform gives its own workers.
//!
//! A role name is a string three parties have to agree on: the configuration
//! that declares a squad's actors, the code that picks a prompt schema or a
//! checkpoint prefix by role, and the identifiers written into storage
//! (`squad:{task}-{role}`). Before this module the name had three homes — an
//! open `AgentRole` newtype in `cog-agent`, the `Display` arms of [`PgeRole`]
//! here, and the task-type table in the gateway — plus roughly seventy sites
//! that spelled the literal again.
//!
//! The failure is quiet in the way that matters: `schema_for` answers an
//! unknown actor with `None`, and every caller is written `if let Some(schema)
//! = ...` and simply proceeds without a schema. Rename the actor in the
//! configuration and the run still works — with a prompt that no longer has the
//! structure the role was built around, and nothing anywhere saying so.
//!
//! Only names that more than one crate has to agree on belong here. A role a
//! single crate invents for itself is that crate's business.
//!
//! `gate`: `crates/cogneva/tests/role_names_are_declared_once_test.rs` counts
//! how many production lines spell each of these names and fails on any second
//! one.

/// The actor that turns a goal into a plan.
pub const PLANNER: &str = "planner";

/// The actor that turns a plan into a change.
pub const GENERATOR: &str = "generator";

/// The actor that judges a change.
pub const EVALUATOR: &str = "evaluator";

/// The actor that runs a round table between the others.
pub const MODERATOR: &str = "moderator";

/// The actor that merges a branched plan back together.
pub const MERGER: &str = "merger";

/// The actor that routes an incoming request to the work that suits it.
pub const MODE_SELECTOR: &str = "mode_selector";

/// Every role this module declares, in the order a run meets them.
///
/// Consumers that need "a valid role" read this rather than writing their own
/// list; a list kept separately is a fourth home that agrees only by luck.
pub const ALL: &[&str] = &[
    PLANNER,
    GENERATOR,
    EVALUATOR,
    MODERATOR,
    MERGER,
    MODE_SELECTOR,
];
