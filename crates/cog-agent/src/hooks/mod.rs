//! HookEngine — observability and notification fan-out for `cog-agents`.
//! engine handles fan-out to webhooks, Redis Streams, and notifications.
//! Key properties:
//! - **Async, non-blocking**: each hook fires on a fresh `tokio::spawn` task.
//! - **Rate limited**: per-hook token bucket (default `100/s` burst `100`).
//! - **Timeout protected**: 30s default, configurable per hook.
//! - **Error isolated**: a single failing hook does not block the others.
//! - **Deduplicated**: identical events fired inside a 1s window are coalesced.
//!
//! See [`HookEngine`] for the entry point.

pub mod engine;
pub mod lifecycle;
pub mod loader;
pub mod rate_limit;
pub mod tiered;
pub mod types;
pub mod wal_buffer;

pub use cog_core::{
    HookAction, HookDef, HookEvent, HookExecution, HookOutcome, HookScope, HookTrigger, LogLevel,
    RateLimitConfig,
};
pub use engine::{DefaultHookPublisher, HookEngine, HookEngineConfig, HookPublisher};
pub use lifecycle::{
    HookHandler, HookType, LifecycleHookEngine, LifecycleHookEvent,
    DEFAULT_LIFECYCLE_CHANNEL_BUFFER,
};
pub use loader::{apply_runtime_overrides, load_and_apply, HookConfig};
pub use rate_limit::TokenBucket;
pub use tiered::TieredHookPublisher;
pub use types::{DEFAULT_DEDUP_WINDOW, DEFAULT_HOOK_TIMEOUT};
pub use wal_buffer::WalHookPublisher;
