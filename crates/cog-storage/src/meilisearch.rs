//! Meilisearch full-text search backend re-exports.
//! Enabled via the `meilisearch` Cargo feature on `cog-storage`.

pub use meilisearch_sdk::*;

/// Create a Meilisearch client from host URL and optional API key.
pub fn connect_meilisearch(host: &str, api_key: Option<&str>) -> meilisearch_sdk::client::Client {
    meilisearch_sdk::client::Client::new(host, api_key)
}
