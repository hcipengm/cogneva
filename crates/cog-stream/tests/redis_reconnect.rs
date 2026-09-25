//! A redis connection that dies must be replaced, not reused.
//!
//! The backend holds long-lived connections: one shared by every publisher and
//! one per subscription. When the server side of such a socket goes away (redis
//! restarted, the peer closed it, a network blip) a bare multiplexed connection
//! keeps handing out the same dead socket. Every later command then fails with
//! the same I/O error, the read loop retries it forever, and the consumer group
//! stops draining while lag piles up behind the caller that is still waiting on
//! it. Observed in the cluster on 2026-09-25: an evolution pod repeating
//! "broken pipe" for hours, with zero clients left on the server, until it was
//! restarted by hand.
//!
//! This test puts a TCP proxy between the backend and redis, cuts every
//! connection through it, and asserts the backend recovers on its own — the
//! subscription delivers a message published after the cut, without a restart.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cog_core::MessageBackend;
use cog_stream::RedisMessageBackend;
use futures::StreamExt;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// TCP proxy in front of redis whose live connections can be cut on demand.
struct CuttableProxy {
    addr: SocketAddr,
    cuts: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
}

impl CuttableProxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy address");
        let cuts: Arc<Mutex<Vec<oneshot::Sender<()>>>> = Arc::default();
        let cuts_task = Arc::clone(&cuts);
        tokio::spawn(async move {
            while let Ok((mut downstream, _)) = listener.accept().await {
                let (cut_tx, cut_rx) = oneshot::channel();
                cuts_task.lock().expect("cuts lock").push(cut_tx);
                tokio::spawn(async move {
                    let Ok(mut upstream) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    // Either side closing ends the copy, which drops both
                    // sockets; a cut drops them the same way, so the backend
                    // sees exactly what a server-side close looks like.
                    tokio::select! {
                        _ = copy_bidirectional(&mut downstream, &mut upstream) => {}
                        _ = cut_rx => {}
                    }
                });
            }
        });
        Self { addr, cuts }
    }

    fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    /// Drop every connection currently open through the proxy.
    fn cut(&self) {
        for cut in self.cuts.lock().expect("cuts lock").drain(..) {
            let _ = cut.send(());
        }
    }
}

fn redis_url() -> String {
    std::env::var("COGNEVA_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

/// Address of the redis the tests may use, or None when there is none — the
/// same skip-rather-than-fail contract the other live-redis tests follow.
/// Only the address is ever printed: the URL may carry a password.
async fn upstream_addr() -> Option<SocketAddr> {
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

/// Publish, retrying for a bounded time. A connection that has just died fails
/// one command — the reconnect happens underneath and that error is surfaced to
/// the caller — so a caller that only publishes gets the same retry the
/// subscription loop already has.
async fn publish_until_ok(
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

#[tokio::test]
async fn a_cut_connection_is_replaced_without_restarting_the_process() {
    let Some(upstream) = upstream_addr().await else {
        return;
    };
    let proxy = CuttableProxy::start(upstream).await;
    let backend = RedisMessageBackend::new(&proxy.url())
        .await
        .expect("backend through the proxy");

    // A stream of its own per run, so repeated runs cannot read each other's
    // leftovers even when a previous run died mid-test.
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos();
    let subject = format!("cog-test:reconnect:{}:{run}", std::process::id());
    let group = "reconnect-group";

    // Subscribe first: the group is created against the empty stream, so the
    // message published next is the one delivered to it.
    let mut stream = backend
        .subscribe(&subject, group)
        .await
        .expect("subscribe through the proxy");
    publish_until_ok(&backend, &subject, b"before", Duration::from_secs(10))
        .await
        .expect("baseline publish");

    let first = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("the proxied path must deliver a baseline message")
        .expect("stream ended early")
        .expect("baseline delivery failed");
    assert_eq!(first.1, b"before");

    proxy.cut();

    publish_until_ok(&backend, &subject, b"after", Duration::from_secs(20))
        .await
        .unwrap_or_else(|e| {
            panic!("publishing after the connection was cut never succeeded, so the dead connection was never replaced: {e}")
        });

    let second = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the subscription never recovered after its connection was cut: \
                 a connection that is only multiplexed is never replaced"
            )
        })
        .expect("stream ended early")
        .expect("post-cut delivery failed");
    assert_eq!(second.1, b"after");
}
