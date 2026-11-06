//! A read connection that stops answering must be replaced, not retried.
//!
//! A cut socket is not the only way a consumer dies. The other way is a socket
//! that stays open and stops answering: the command goes out, the reply never
//! comes back, and every later command queued on that request-reply connection
//! waits behind it. Nothing errors, so nothing is counted, and the consumer
//! reads as idle while the group it belongs to falls behind.
//!
//! Measured in the cluster on 2026-11-06: the process-local ready stream had
//! delivered nothing for nineteen hours with 1,369 entries queued behind it, and
//! a consumer group created on the same key read the head of the stream at once
//! — the stream and the group were fine, and only that one socket was not
//! answering. The lane it serves is the one every self-evolution task queues
//! on, so the platform's own change pipeline stopped at the read side while the
//! queue filled from the write side.
//!
//! This test puts a TCP proxy in front of redis that can silence the server side
//! of the connections already open without closing them, then asserts a message
//! published after the silence is delivered anyway.

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cog_core::MessageBackend;
use cog_stream::{ReadHealth, RedisMessageBackend};
use common::{publish_until_ok, upstream_addr};
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One proxied connection: whether the server's answers still reach the client.
type Darkness = Arc<AtomicBool>;

/// TCP proxy that can silence the server side of the connections it already
/// carries, without closing either end.
///
/// Closing would be a different incident — the client sees EOF and reconnects on
/// its own — so the interesting case is the one where both ends stay open and
/// the answers simply stop arriving.
struct SilencingProxy {
    addr: SocketAddr,
    live: Arc<Mutex<Vec<Darkness>>>,
    /// How many connections have been accepted, in order. A reader that gives up
    /// on its socket opens another one, and that is the only outward sign that
    /// it recovered — the stream itself looks the same before and after.
    accepted: Arc<AtomicUsize>,
}

impl SilencingProxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy address");
        let live: Arc<Mutex<Vec<Darkness>>> = Arc::default();
        let tracked = Arc::clone(&live);
        let accepted: Arc<AtomicUsize> = Arc::default();
        let counted = Arc::clone(&accepted);
        tokio::spawn(async move {
            while let Ok((downstream, _)) = listener.accept().await {
                let Ok(upstream) = TcpStream::connect(upstream).await else {
                    continue;
                };
                counted.fetch_add(1, Ordering::Relaxed);
                let dark: Darkness = Arc::new(AtomicBool::new(false));
                tracked.lock().expect("live lock").push(Arc::clone(&dark));
                tokio::spawn(proxy_pair(downstream, upstream, dark));
            }
        });
        Self {
            addr,
            live,
            accepted,
        }
    }

    fn connections_accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    /// Silence the server side of every connection open right now except the
    /// first one.
    ///
    /// The backend's ordinary connection is the first through the proxy and the
    /// read loop's is the second, so this silences the reader while leaving
    /// publishing able to answer — otherwise the test would be waiting on a
    /// publish that cannot complete either, and would prove nothing about the
    /// reader. Connections opened afterwards start clear, which is what lets the
    /// replacement a recovering reader opens carry traffic again.
    fn silence_open_connections_except_the_first(&self) {
        for dark in self.live.lock().expect("live lock").iter().skip(1) {
            dark.store(true, Ordering::Relaxed);
        }
    }
}

async fn proxy_pair(mut downstream: TcpStream, mut upstream: TcpStream, dark: Darkness) {
    let (mut client_read, mut client_write) = downstream.split();
    let (mut server_read, mut server_write) = upstream.split();

    let to_server = async {
        let mut buf = vec![0u8; 8192];
        loop {
            match client_read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if server_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    let to_client = async {
        let mut buf = vec![0u8; 8192];
        loop {
            match server_read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    // While dark the bytes are read and thrown away: the server
                    // believes it answered, and the client waits for a reply
                    // that is never going to arrive.
                    if dark.load(Ordering::Relaxed) {
                        continue;
                    }
                    if client_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    tokio::select! {
        _ = to_server => {}
        _ = to_client => {}
    }
}

#[tokio::test]
async fn a_read_connection_that_stops_answering_is_replaced() {
    let Some(upstream) = upstream_addr().await else {
        return;
    };
    let proxy = SilencingProxy::start(upstream).await;

    // Short block periods so the unanswered read is given up on in seconds
    // rather than in the deployment's five. The grace the loop adds is the
    // production one, so the deadline here is what a five-second block would
    // get, minus four seconds.
    let backend = RedisMessageBackend::with_block_periods(
        &proxy.url(),
        1000,
        1000,
        Arc::new(ReadHealth::new(1000)),
    )
    .await
    .expect("backend through the proxy");

    // A stream of its own per run, so repeated runs cannot read each other's
    // leftovers even when a previous run died mid-test.
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos();
    let subject = format!("cog-test:silent-read:{}:{run}", std::process::id());
    let group = "silent-read-group";

    let mut stream = backend
        .subscribe(&subject, group)
        .await
        .expect("subscribe through the proxy");
    publish_until_ok(&backend, &subject, b"before", Duration::from_secs(10))
        .await
        .expect("baseline publish");

    let first = tokio::time::timeout(Duration::from_secs(20), stream.next())
        .await
        .expect("the proxied path must deliver a baseline message")
        .expect("stream ended early")
        .expect("baseline delivery failed");
    assert_eq!(first.1, b"before");

    // From here the read connection keeps its socket and stops being answered.
    // Nothing is published yet: a message handed to the socket before the read
    // is given up on would be delivered into it and stay in the pending list,
    // which is the queue's own recovery and not the read loop's.
    let before_silence = proxy.connections_accepted();
    proxy.silence_open_connections_except_the_first();

    // Polling the stream is what makes the read loop run at all: between calls
    // to `next` the unfold body is dropped, so waiting for the replacement
    // without reading would be waiting for something that cannot happen.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut published = false;
    let delivered = loop {
        assert!(
            Instant::now() < deadline,
            "the read loop never recovered from a connection that stopped answering: \
             {}, and no message published after it was ever read",
            if published {
                "it did reopen"
            } else {
                "it never opened another connection"
            }
        );
        tokio::select! {
            msg = stream.next() => {
                break msg;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Published only once the replacement exists: a message handed
                // to the silenced socket would be delivered into it and left in
                // the pending list, which is the queue's own recovery rather
                // than the read loop's.
                if !published && proxy.connections_accepted() > before_silence {
                    publish_until_ok(&backend, &subject, b"after", Duration::from_secs(10))
                        .await
                        .expect("the publish connection was not the one silenced");
                    published = true;
                }
            }
        }
    };

    let second = delivered
        .expect("stream ended early")
        .expect("delivery failed");
    assert_eq!(second.1, b"after");
    assert!(
        published,
        "the message was delivered without ever being published"
    );
}
