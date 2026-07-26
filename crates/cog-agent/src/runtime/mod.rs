//! Multi-agent runtime — global agent manager, task dispatch, and coordination.
//! This module provides production-grade infrastructure for running multiple
//! global agents in parallel, with automatic registry enrollment, inbox
//! consumption, and shared state board access.

pub mod manager;

pub use manager::{GlobalAgentManager, WorkerHandle};
