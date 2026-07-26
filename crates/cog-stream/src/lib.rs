//! Stream messaging backends — Redis Streams, NATS JetStream, and in-memory channels.
//! This crate provides implementations of [`cog_core::MessageBackend`] for
//! various message-bus backends.  It does not define the trait itself;
//! the trait lives in `cog-core` so that business crates can depend only on
//! `cog-core` and receive the backend via `Arc<dyn MessageBackend>` injection.

#[cfg(feature = "mem")]
pub mod memory;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "mem")]
pub use memory::MemoryMessageBackend;
#[cfg(feature = "nats")]
pub use nats::NatsMessageBackend;
#[cfg(feature = "redis")]
pub use redis::RedisMessageBackend;

pub mod event_publisher;
pub use event_publisher::MqEventPublisher;

pub mod plugin;
