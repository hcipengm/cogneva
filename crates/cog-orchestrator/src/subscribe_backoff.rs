//! 订阅失败后隔多久再订一次：读数只有一处，在 `cog_core`。

pub use cog_core::contract::subscribe_backoff::SUBSCRIBE_RETRY_BACKOFF;
