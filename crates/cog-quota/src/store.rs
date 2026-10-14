//! Helpers shared by the managers that keep counters in a key-value store.
//!
//! Both [`crate::quota::QuotaManager`] and
//! [`crate::hierarchy::HierarchyManager`] read the same shapes out of the same
//! contract, so the two conversions live here once rather than once per
//! manager — two copies would be two answers to "what does an unreadable
//! counter mean".

use crate::error::QuotaError;
use cog_core::KeyValueStore;

/// Map a store failure into the quota error type.
///
/// The store reports a cache failure as `SFError::Redis`; keeping that as
/// `QuotaError::RedisError` preserves the `redis_error` code the API already
/// returns for a cache outage.
pub(crate) fn store_error(e: cog_core::SFError) -> QuotaError {
    QuotaError::RedisError(e.to_string())
}

/// Read an integer counter from the store.
///
/// A missing key and a value that is not a number both read as absent, which is
/// what the call sites already assumed: they took the driver's integer
/// conversion and fell back to a default on `None` or an error.
pub(crate) async fn read_int(store: &dyn KeyValueStore, key: &str) -> Option<i64> {
    store
        .get(key)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.parse().ok())
}
