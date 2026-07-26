//! Concrete backend implementations that depend on external I/O or storage.

pub mod file_snapshot;
pub mod file_trace_store;
pub mod object_backends;
pub mod redis_state;
