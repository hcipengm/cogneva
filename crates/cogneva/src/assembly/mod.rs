//! Assembly layer — shared infrastructure helpers for the `cogneva` binary.
//! NOTE: All first-party crate plugins now self-assemble via their
//! `SystemPlugin::init()` implementations. This directory is retained
//! only for shared infrastructure helpers (`infra.rs`).

pub mod infra;
