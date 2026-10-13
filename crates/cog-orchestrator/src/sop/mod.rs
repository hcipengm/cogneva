//! SOP-driven task injection: compile a face's declared SOP into DAG tasks.
//!
//! A task face keeps its plan in a human-readable `SOP.md`; this module turns
//! that document into the verified tasks the planner already knows how to inject
//! directly, so the plan a person edits is the plan the platform runs. The split
//! is deliberate: [`compile`] is a pure function over the document text — easy to
//! test with fixtures and free of any I/O — while [`dispatch`] owns the resident
//! loop that fetches the document, decides when to re-read it, and hands the
//! result to the planner. Everything face-specific (which repository, which path,
//! how often) is runtime configuration, never a literal here.

mod compile;
mod dispatch;

pub use compile::{compile, SopCompileError, SopDoc, SopTask, TaskFaceSource};
pub use dispatch::{
    spawn_sop_dispatch_loop, SopDispatchConfig, SOP_DISPATCH_LOOP, SOP_DISPATCH_ROLE,
};
