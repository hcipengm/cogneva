//! Security policies for sandbox execution.
//! Applies capabilities, seccomp, and network policies.
//! This module is a placeholder for the full security hardening layer.

use cog_core::ResourceLimits;

/// Security context applied to a sandbox execution.
#[derive(Debug, Clone, Default)]
pub struct SecurityContext {
    pub limits: ResourceLimits,
    pub seccomp_profile: Option<String>,
    pub capabilities_drop: Vec<String>,
}

impl SecurityContext {
    pub fn from_limits(limits: &ResourceLimits) -> Self {
        Self {
            limits: limits.clone(),
            seccomp_profile: None,
            capabilities_drop: vec![
                "CAP_NET_RAW".into(),
                "CAP_SYS_ADMIN".into(),
            ],
        }
    }

    /// Enforce the security context on a WASM runtime configuration.
    #[cfg(feature = "wasm")]
    pub fn apply_wasm(&self, config: &mut wasmtime::Config) {
        // WASMtime fuel metering for CPU limiting.
        config.consume_fuel(true);
    }
}
