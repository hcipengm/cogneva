//! HTTP client factory for unified reqwest configuration.
//! All HTTP clients should be created via [`build_client`] or
//! [`ReqwestHttpClient`] so that timeouts, connection pools, and proxy settings
//! are consistent across the system.

pub mod config;
pub mod factory;
pub mod plugin;
pub mod vault;
pub mod websocket;

pub use factory::build_client;
pub use factory::ReqwestHttpClient;
pub use vault::VaultSecretProvider;
pub use websocket::TungsteniteWebSocketClient;

pub use config::HttpClientConfig;
