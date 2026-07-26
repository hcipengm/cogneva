//! Meta-bootstrap layer — pure infrastructure initialization.
//! Design principle: `bootstrap` must NEVER contain cross-crate business
//! assembly logic.  All business-crate wiring happens through the plugin
//! system (`SystemPlugin` + `PluginContext`).
//! Responsibilities retained here:
//! - None (config loading, PID file, and daemon control moved to
//!   `assembly::infra`).
//!
//! This module is kept as a placeholder to preserve import paths.  Future
//! meta-bootstrap utilities (e.g. runtime feature probing) may live here.
