//! Helpers shared by this crate's integration tests.
//!
//! A binary that declares `mod common;` and uses only part of it would warn on
//! the rest, and the fixtures here are written for whichever test needs them
//! rather than trimmed per binary.
#![allow(dead_code)]

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use cog_core::MessageBackend;
use cog_stream::RedisMessageBackend;
use cog_test_support::redis_url;
use tokio::net::TcpStream;

/// Address of the redis the tests may use, or None when there is none — the
/// same skip-rather-than-fail contract the other live-redis tests follow.
/// Only the address is ever printed: the URL may carry a password.
pub async fn upstream_addr() -> Option<SocketAddr> {
    let url = redis_url();
    let client = redis::Client::open(url.as_str()).ok()?;
    let addr = match client.get_connection_info().addr.clone() {
        redis::ConnectionAddr::Tcp(host, port) => {
            SocketAddr::new(host.parse::<IpAddr>().ok()?, port)
        }
        other => {
            eprintln!("SKIP: redis url is not a plain TCP address: {other:?}");
            return None;
        }
    };
    match TcpStream::connect(addr).await {
        Ok(_) => Some(addr),
        Err(_) => {
            eprintln!("SKIP: no redis at {addr}");
            None
        }
    }
}

/// Publish, retrying for a bounded time.
///
/// A connection that has just died fails one command — the reconnect happens
/// underneath and that error is surfaced to the caller — so a caller that only
/// publishes gets the same retry the subscription loop already has. Two tests
/// need it (a cut socket and a socket that keeps its wire open but stops
/// answering); the body is the same in both, so it lives once.
pub async fn publish_until_ok(
    backend: &RedisMessageBackend,
    subject: &str,
    payload: &[u8],
    within: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + within;
    let mut last = String::from("no attempt was made");
    while Instant::now() < deadline {
        match backend.publish(subject, payload).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(last)
}
