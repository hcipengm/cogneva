//! The key-value store contract.
//!
//! `cog-core` already publishes a contract for every other storage shape the
//! business crates reach for: streams ([`crate::contract::stream::MessageBackend`]),
//! objects, vectors and metrics. The plain key-value operations were the gap,
//! and because no contract existed the crates that needed them linked the driver
//! directly (`cog-redis`, and `cog-storage`'s client handle). An edge to a
//! neighbour's implementation is not a capability anyone chose: it is what is
//! left when the capability has no contract, and it costs the dependent crate a
//! rebuild of its neighbour whenever the neighbour's implementation moves.
//!
//! The surface is not invented here — it is the union of what the call sites
//! actually do, so that each method has a caller today rather than a hoped-for
//! one. Four groups, in the order they appear below:
//!
//! - **strings with a TTL**: [`get`](KeyValueStore::get),
//!   [`set`](KeyValueStore::set), [`set_ex`](KeyValueStore::set_ex),
//!   [`del`](KeyValueStore::del), [`expire`](KeyValueStore::expire),
//!   [`ttl`](KeyValueStore::ttl);
//! - **atomic counters**: [`incr`](KeyValueStore::incr),
//!   [`decr`](KeyValueStore::decr), [`incr_ex`](KeyValueStore::incr_ex);
//! - **sets**: [`sadd`](KeyValueStore::sadd), [`srem`](KeyValueStore::srem),
//!   [`smembers`](KeyValueStore::smembers);
//! - **hash fields**: [`hset`](KeyValueStore::hset),
//!   [`hget`](KeyValueStore::hget), [`hdel`](KeyValueStore::hdel),
//!   [`hvals`](KeyValueStore::hvals), [`hgetall`](KeyValueStore::hgetall),
//!   [`hlen`](KeyValueStore::hlen).
//!
//! Deliberately **not** here: pub/sub and Streams. Those are a different
//! capability with a different contract
//! ([`crate::contract::stream::MessageBackend`]), and folding them in would make
//! one trait that a subscriber has to implement a hash for. A single-key
//! compare-and-swap is likewise absent: the one caller that needed it
//! ([`crate::contract::storage::StateBackend::cas_agent_state`]) already states
//! it as its own operation, where the value is a serialized document rather than
//! a string, and a generic CAS on this trait would be a second way to spell the
//! same thing.

use crate::SFResult;
use async_trait::async_trait;

/// A handle to a string-keyed key-value store with TTLs, atomic counters, sets
/// and hash fields.
///
/// Implemented by the crate that owns the driver, published as a service, and
/// consumed as `Arc<dyn KeyValueStore>` — so a manager that needs a counter does
/// not link the cache implementation, and a test can supply an in-memory
/// implementation without the driver being compiled in.
///
/// Values are strings. Callers that store a number or a serialized document
/// spell it as one, which is what the underlying stores do anyway; a trait
/// generic over the value type would put the encoding choice in every call site
/// instead of at the edge that owns the key.
#[async_trait]
pub trait KeyValueStore: Send + Sync {
    /// Read the value at `key`, or `None` if it is absent or has expired.
    async fn get(&self, key: &str) -> SFResult<Option<String>>;

    /// Overwrite the value at `key`, leaving any TTL already set in place.
    async fn set(&self, key: &str, value: &str) -> SFResult<()>;

    /// Write `value` at `key` with a time-to-live of `ttl_secs` seconds.
    ///
    /// One operation rather than a `set` followed by an `expire`: the two-call
    /// form leaves a window in which the value exists with no expiry, and a
    /// process that dies inside that window leaves it there permanently.
    async fn set_ex(&self, key: &str, value: &str, ttl_secs: u64) -> SFResult<()>;

    /// Delete `key`. A key that is already absent is not an error.
    async fn del(&self, key: &str) -> SFResult<()>;

    /// Set the time-to-live of an existing `key` without touching its value.
    async fn expire(&self, key: &str, ttl_secs: i64) -> SFResult<()>;

    /// The remaining time-to-live of `key` in seconds, `-1` if it has none and
    /// `-2` if the key does not exist.
    ///
    /// The read side of [`expire`](KeyValueStore::expire): a TTL that is only
    /// ever written cannot be checked, and a write with no reader is how a
    /// wrong TTL goes unnoticed.
    async fn ttl(&self, key: &str) -> SFResult<i64>;

    /// Add `by` to the integer stored at `key`, atomically, and return the new
    /// value.
    ///
    /// Atomic because its callers use it for quota accounting: a read followed
    /// by a write in the caller lets two concurrent requests each read the same
    /// total and each write back its own increment, so one of them is lost.
    async fn incr(&self, key: &str, by: i64) -> SFResult<i64>;

    /// Subtract `by` from the integer stored at `key`, atomically, and return
    /// the new value.
    async fn decr(&self, key: &str, by: i64) -> SFResult<i64>;

    /// Add `by` to the integer at `key` and set a time-to-live of `ttl_secs`
    /// seconds **only if this call created the key**, then return the new value.
    ///
    /// One operation rather than [`incr`](KeyValueStore::incr) followed by
    /// [`expire`](KeyValueStore::expire): the two-call form leaves a window in
    /// which the key exists with no expiry, and a process that dies inside that
    /// window leaves a counter that never resets. Its caller counts failed login
    /// attempts by identifier, where a counter that never resets is that
    /// identifier locked out permanently.
    ///
    /// The TTL is **not** refreshed by later increments. A caller that wants a
    /// window ending N seconds after the first attempt rather than after the
    /// most recent one depends on that: refreshing on every write lets the
    /// counter be kept alive indefinitely by the very attempts it is counting.
    async fn incr_ex(&self, key: &str, by: i64, ttl_secs: u64) -> SFResult<i64>;

    /// Add `member` to the set at `key`, creating the set if it is absent.
    async fn sadd(&self, key: &str, member: &str) -> SFResult<()>;

    /// Remove `member` from the set at `key`. A member that is not there is not
    /// an error.
    async fn srem(&self, key: &str, member: &str) -> SFResult<()>;

    /// Every member of the set at `key`, empty if the key is absent.
    async fn smembers(&self, key: &str) -> SFResult<Vec<String>>;

    /// Set `field` of the hash at `key` to `value`, creating the hash if it is
    /// absent.
    async fn hset(&self, key: &str, field: &str, value: &str) -> SFResult<()>;

    /// Read `field` of the hash at `key`, or `None` if the field or the hash is
    /// absent.
    async fn hget(&self, key: &str, field: &str) -> SFResult<Option<String>>;

    /// Remove `field` from the hash at `key`. A field that is not there is not
    /// an error.
    async fn hdel(&self, key: &str, field: &str) -> SFResult<()>;

    /// Every value of the hash at `key`, empty if the key is absent.
    async fn hvals(&self, key: &str) -> SFResult<Vec<String>>;

    /// Every field and value of the hash at `key` as pairs, empty if the key is
    /// absent.
    ///
    /// Distinct from [`hvals`](KeyValueStore::hvals) because the caller here
    /// reassembles a structured document and the field names are the keys of
    /// that document: values alone lose which field each one came from.
    async fn hgetall(&self, key: &str) -> SFResult<Vec<(String, String)>>;

    /// How many fields the hash at `key` has, zero if the key is absent.
    async fn hlen(&self, key: &str) -> SFResult<u64>;
}
