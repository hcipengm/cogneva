//! Standalone host ledger: the host's memory, split by cgroup slice, served on
//! `/metrics` from one process per node.
//!
//! The reading itself lives in `cog_observability::host_ledger`; this mode is
//! only how a node runs it. It is a separate mode rather than a loop inside the
//! main application because the main application is not on every node and does
//! not see the host's cgroup root -- a pod that mounts it and reports it has to
//! be the DaemonSet, and a DaemonSet container is one program.
//!
//! Both probes answer with the health of the listener, and stay green when the
//! root is unreadable. That is deliberate. This pod is one container with no
//! data plane behind it, so a failing probe would not restart it into a working
//! state -- the mount is either there or it is not -- and an endpoint whose
//! readiness gates it out is dropped from the Service's endpoint list, which
//! removes the scrape rather than reporting the misconfiguration. A missing
//! reading is a fact about the deployment, and it is reported where deployments
//! are read: the unreachable slice is named in a log line on every scrape.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use cog_core::observability_text::render_raw_metrics;
use cog_core::Observable;
use cog_observability::host_ledger::{self, HostLedgerObservable};

/// Port this mode serves on.
pub const PORT_ENV: &str = "COGNEVA_HOST_LEDGER_PORT";

pub use crate::scraper_endpoint::DEFAULT_PORT;

/// Port this mode should listen on, from the environment.
pub fn port_from_env() -> u16 {
    crate::scraper_endpoint::port_from_env(PORT_ENV)
}

/// The host cgroup root this process reads, from the environment.
pub fn root_from_env() -> PathBuf {
    parse_root(std::env::var(host_ledger::ROOT_ENV).ok().as_deref())
}

/// Pure half of [`root_from_env`]: an unset or blank path is the deployment
/// saying nothing, not the deployment saying the working directory.
fn parse_root(raw: Option<&str>) -> PathBuf {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(host_ledger::DEFAULT_ROOT),
    }
}

/// The node this process is on, from the environment.
///
/// The deployment sets this from the pod's own `spec.nodeName` rather than
/// leaving it to the scraper: the scraper's label for a DaemonSet target is the
/// pod's address, which changes when the pod is recreated and does not match
/// the node name the node exporter carries -- so nothing downstream could
/// attribute a sample to a node. An unset variable is reported as an empty
/// label, not as a missing one: the rule that groups by node puts a blank name
/// in its own group instead of folding it into a real node's.
pub fn node_from_env() -> String {
    parse_node(std::env::var(host_ledger::NODE_ENV).ok().as_deref())
}

/// Pure half of [`node_from_env`]: nothing here can invent a node name, so an
/// unset one is the empty string.
fn parse_node(raw: Option<&str>) -> String {
    raw.map(str::trim).unwrap_or_default().to_string()
}

#[derive(Clone)]
struct LedgerState {
    observable: Arc<HostLedgerObservable>,
}

async fn health() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("ok"))
        .expect("static response")
}

/// The reading is taken from the filesystem while this handler runs, so there is
/// no stored measurement that could go stale and no background loop whose death
/// would be silent: a scrape that reaches the handler carries a reading taken
/// now, and one that does not reach it reports the target as down.
async fn metrics(State(state): State<LedgerState>) -> Response {
    let body = match state.observable.collect_metrics("").await {
        Ok(readings) => render_raw_metrics(&readings),
        Err(e) => {
            tracing::warn!(error = %e, "host slice readings unavailable this scrape");
            String::new()
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("static response")
}

/// Router over an already-built observable.
pub fn router(observable: Arc<HostLedgerObservable>) -> Router {
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/metrics", get(metrics))
        .with_state(LedgerState { observable })
}

/// Entry point for the `host-ledger` subcommand.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    // Standalone modes do not go through `run_app`, so they own their logging
    // setup; without it the lines below (the only place an unreachable slice is
    // visible) would go nowhere.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let observable = Arc::new(HostLedgerObservable::new(root_from_env(), node_from_env()));
    let listening = observable.readable_slices();
    if listening == 0 {
        let root = observable.root();
        tracing::error!(
            env = host_ledger::ROOT_ENV,
            node = observable.node(),
            root = %root.display(),
            "no host slice readable; this process will serve an empty /metrics"
        );
    } else {
        tracing::info!(
            root = %observable.root().display(),
            node = observable.node(),
            slices = listening,
            "host ledger reading the host cgroup tree"
        );
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], port_from_env()));
    tracing::info!(addr = %addr, "host ledger listening");
    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        router(observable),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::scrape;
    use std::path::Path;

    fn scratch(name: &str) -> cog_test_support::ScratchDir {
        cog_test_support::ScratchDir::new(&format!("ledger-mode-{name}"))
    }

    fn write_slice(root: &Path, slice: &str, stat: &str) {
        let dir = root.join(slice);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.stat"), stat).unwrap();
    }

    /// A blank root must not resolve to the working directory: the process would
    /// then read a tree that has nothing to do with a host and report a host
    /// with no slices, which reads exactly like an unreadable mount.
    #[test]
    fn a_blank_root_falls_back_to_the_deployment_mount_point() {
        assert_eq!(
            parse_root(Some("/sys/fs/cgroup")),
            PathBuf::from("/sys/fs/cgroup")
        );
        assert_eq!(
            parse_root(Some("  ")),
            PathBuf::from(host_ledger::DEFAULT_ROOT)
        );
        assert_eq!(parse_root(None), PathBuf::from(host_ledger::DEFAULT_ROOT));
    }

    /// The point of the mode: the number it publishes is the host's, split by
    /// the label the kernel already keeps it in -- and every line says which
    /// node it came from, because the scraper's own label cannot say it.
    #[tokio::test]
    async fn the_scrape_carries_each_slice_under_its_own_node_and_label() {
        let root = scratch("scrape");
        write_slice(&root, "user.slice", "anon 1024\nfile 2048\n");
        write_slice(&root, "system.slice", "anon 4096\nfile 8192\n");

        let app = router(Arc::new(HostLedgerObservable::new(root.path(), "node-a")));
        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains(
                "cogneva_host_slice_anon_bytes{node=\"node-a\",slice=\"user.slice\"} 1024"
            ),
            "user.slice anon missing from the scrape:\n{body}"
        );
        assert!(
            body.contains(
                "cogneva_host_slice_file_bytes{node=\"node-a\",slice=\"system.slice\"} 8192"
            ),
            "system.slice file missing from the scrape:\n{body}"
        );
        // The presence series is what makes a node with no ledger a missing
        // series rather than an unread one, so it has to leave the endpoint
        // even when both slices were readable.
        assert!(
            body.contains("cogneva_host_ledger_node_info{node=\"node-a\"} 1"),
            "the node's presence series is not in the scrape:\n{body}"
        );
        assert!(
            body.contains("cogneva_host_ledger_slices_readable{node=\"node-a\"} 2"),
            "the coverage reading is not in the scrape:\n{body}"
        );
    }

    /// A host whose cgroup root was not mounted still leaves a live endpoint:
    /// the pod is not taken out of service over a missing reading, so the
    /// endpoint answering with no readings is the shape the deployment sees.
    /// What it must not answer with is silence about itself: the coverage
    /// reading says "this node read nothing", which is a different fact from
    /// three zeroes and a different one from the node being gone.
    #[tokio::test]
    async fn an_unmounted_root_serves_a_live_but_empty_endpoint() {
        let root = scratch("unmounted");
        let app = router(Arc::new(HostLedgerObservable::new(
            root.join("not-a-cgroup-root"),
            "node-a",
        )));
        for path in ["/health/live", "/health/ready"] {
            let (status, _) = scrape(app.clone(), path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
        }
        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !body.contains("cogneva_host_slice_anon_bytes"),
            "no slice was readable but one was reported:\n{body}"
        );
        assert!(
            body.contains("cogneva_host_ledger_slices_readable{node=\"node-a\"} 0"),
            "a node that read nothing did not say so:\n{body}"
        );
    }

    /// The node name is what the deployment provides; nothing else in the
    /// process can know it, so a name is taken as given and an unset one is the
    /// empty string rather than a fabricated hostname.
    #[test]
    fn a_node_name_is_taken_verbatim_and_an_absent_one_is_empty() {
        assert_eq!(parse_node(Some("node-a")), "node-a");
        assert_eq!(parse_node(Some("  node-a\n")), "node-a");
        assert_eq!(parse_node(Some("")), "");
        assert_eq!(parse_node(Some("   ")), "");
        assert_eq!(parse_node(None), "");
    }
}
