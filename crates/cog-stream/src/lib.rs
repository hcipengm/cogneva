//! Stream messaging backends — Redis Streams, NATS JetStream, and in-memory channels.
//! This crate provides implementations of [`cog_core::MessageBackend`] for
//! various message-bus backends.  It does not define the trait itself;
//! the trait lives in `cog-core` so that business crates can depend only on
//! `cog-core` and receive the backend via `Arc<dyn MessageBackend>` injection.

#[cfg(feature = "mem")]
pub mod memory;
#[cfg(feature = "nats")]
pub mod nats;
pub mod observable;
pub mod read_health;
#[cfg(feature = "redis")]
pub mod redis;

/// 读数按「哪条流」归属时用的标签键。
///
/// 部署里的面板按它分组，代码在这里发射它；两处各拼一遍时，一处改名不会让
/// 另一处报错，分组结果为空读起来与「这条流一直没动静」是同一个读数。
pub const LABEL_STREAM: &str = "stream";

/// 读数按「哪个消费组」归属时用的标签键。
pub const LABEL_CONSUMER_GROUP: &str = "consumer_group";

pub use observable::StreamReadObservable;
pub use read_health::{ReadFamily, ReadGuard, ReadHealth, ReadRefusal};

#[cfg(feature = "mem")]
pub use memory::MemoryMessageBackend;
#[cfg(feature = "nats")]
pub use nats::NatsMessageBackend;
#[cfg(feature = "redis")]
pub use redis::RedisMessageBackend;

pub mod event_publisher;
pub use event_publisher::MqEventPublisher;

pub mod plugin;

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;
