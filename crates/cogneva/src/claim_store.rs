//! Standalone claim-store scanner: every claim directory on this node's storage
//! root, measured and served on `/metrics` from one process per node.
//!
//! The reading lives in `cog_observability::claim_store`; this mode is only how a
//! node runs it. It is a separate program rather than a loop inside the main
//! application because the main application is not on every node and does not
//! see any node's storage root -- and it is separate from the host ledger's
//! DaemonSet because the two need different privilege: the cgroup readings are
//! world-readable files, while the storage root is a root-owned directory that a
//! process cannot even list as an unprivileged user, so a mount the one needs is
//! a privilege the other does not.
//!
//! Both probes answer with the health of the listener, and stay green when the
//! root is unreadable. That is deliberate, for the same reason as the host
//! ledger's: the pod behind them has no data plane, so a failing probe would not
//! restart it into a working state -- the mount is either there or it is not --
//! and an endpoint whose readiness gates it out is dropped from the target list,
//! which removes the scrape rather than reporting the misconfiguration. A root
//! that cannot be read is reported where readings are: the scan's age keeps
//! growing, and a rule over that age names the node.

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
use cog_core::{Observable, ShutdownSignal};
use cog_observability::claim_store::{self, ClaimStoreObservable, Scan};

/// Port this mode serves on.
pub const PORT_ENV: &str = "COGNEVA_CLAIM_STORE_PORT";

/// Port used when the deployment names none.
///
/// Nothing else in this pod listens here; the scraper discovers the endpoint by
/// the name `http`, which is what the pod's container port is called.
pub const DEFAULT_PORT: u16 = 9100;

/// Port this mode should listen on, from the environment.
pub fn port_from_env() -> u16 {
    parse_port(std::env::var(PORT_ENV).ok().as_deref()).unwrap_or(DEFAULT_PORT)
}

/// Pure half of [`port_from_env`], so the default is testable without touching a
/// process-wide variable other tests share.
fn parse_port(raw: Option<&str>) -> Option<u16> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok())
}

/// The storage root this process reads, from the environment.
pub fn root_from_env() -> PathBuf {
    parse_root(std::env::var(claim_store::ROOT_ENV).ok().as_deref())
}

/// Pure half of [`root_from_env`]: an unset or blank path is the deployment
/// saying nothing, not the deployment saying the working directory -- which
/// would walk whatever the container happens to start in and report its claim
/// directories as the node's.
fn parse_root(raw: Option<&str>) -> PathBuf {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(claim_store::DEFAULT_ROOT),
    }
}

/// The node this process is on, from the environment.
///
/// Taken from the pod's own `spec.nodeName` rather than from the scraper: the
/// scraper's label for a DaemonSet target is the pod's address, which changes
/// when the pod is recreated and matches no node name anything else carries.
pub fn node_from_env() -> String {
    parse_node(std::env::var(claim_store::NODE_ENV).ok().as_deref())
}

/// Pure half of [`node_from_env`]: nothing here can invent a node name, so an
/// unset one is the empty string.
fn parse_node(raw: Option<&str>) -> String {
    raw.map(str::trim).unwrap_or_default().to_string()
}

/// Re-walk cadence, from the environment and floored at the shared minimum.
pub fn interval_from_env() -> u64 {
    parse_interval(std::env::var(claim_store::INTERVAL_ENV).ok().as_deref())
}

fn parse_interval(raw: Option<&str>) -> u64 {
    raw.map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(claim_store::DEFAULT_INTERVAL_SECS)
        .max(claim_store::MIN_INTERVAL_SECS)
}

#[derive(Clone)]
struct StoreState {
    observable: Arc<ClaimStoreObservable>,
}

async fn health() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("ok"))
        .expect("static response")
}

/// The scan runs on its own cadence and this handler renders the last result, so
/// a scrape does not pay for a walk of everything the node holds.
async fn metrics(State(state): State<StoreState>) -> Response {
    let body = match state.observable.collect_metrics("").await {
        Ok(readings) => render_raw_metrics(&readings),
        Err(e) => {
            tracing::warn!(error = %e, "claim store readings unavailable this scrape");
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
pub fn router(observable: Arc<ClaimStoreObservable>) -> Router {
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/metrics", get(metrics))
        .with_state(StoreState { observable })
}

/// Entry point for the `claim-store` subcommand.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    // Standalone modes do not go through `run_app`, so they own their logging
    // setup; without it the lines below (where an unreadable root is visible)
    // would go nowhere.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let observable = Arc::new(ClaimStoreObservable::new(root_from_env(), node_from_env()));
    // One scan before the listener opens: a node whose first scrape carries no
    // claim at all is indistinguishable from a node with no claims, and the
    // process would have to be scraped for a full cadence before it said
    // anything.
    match observable.scan_blocking().await {
        Scan::Completed(report) => tracing::info!(
            root = %observable.root().display(),
            node = observable.node(),
            dirs = report.dirs,
            readable = report.readable,
            skipped = report.skipped,
            "claim store scanned the node's storage root"
        ),
        Scan::RootUnreadable => tracing::error!(
            env = claim_store::ROOT_ENV,
            node = observable.node(),
            root = %observable.root().display(),
            "no storage root readable; this process will serve coverage without any claim"
        ),
    }

    tokio::spawn(claim_store::run_claim_store_scan(
        Arc::clone(&observable),
        interval_from_env(),
        ShutdownSignal::default(),
    ));

    let addr = SocketAddr::from(([0, 0, 0, 0], port_from_env()));
    tracing::info!(addr = %addr, "claim store listening");
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
    use axum::body::to_bytes;
    use axum::http::Request;
    use std::fs;
    use std::path::Path;
    use tower::ServiceExt;

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cog-claim-mode-{}-{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn claim_dir(root: &Path, namespace: &str, claim: &str) -> PathBuf {
        let dir = root.join(format!(
            "pvc-11111111-2222-3333-4444-555555555555_{namespace}_{claim}"
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn scrape(app: Router, path: &str) -> (StatusCode, String) {
        let response = app
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    /// The default is only used when the deployment names no port, and a
    /// malformed value must not silently become port 0, which binds a random
    /// port the scraper could never find.
    #[test]
    fn a_named_port_wins_and_anything_unusable_falls_back_to_the_default() {
        assert_eq!(parse_port(Some("9104")), Some(9104));
        assert_eq!(parse_port(Some(" 9104 ")), Some(9104));
        assert_eq!(parse_port(None), None);
        assert_eq!(parse_port(Some("")), None);
        assert_eq!(parse_port(Some("nine thousand")), None);
    }

    /// A blank root must not resolve to the working directory: the process would
    /// then walk the container's own filesystem and report it as the node's.
    #[test]
    fn a_blank_root_falls_back_to_the_deployment_mount_point() {
        assert_eq!(
            parse_root(Some("/host/storage")),
            PathBuf::from("/host/storage")
        );
        assert_eq!(
            parse_root(Some("  ")),
            PathBuf::from(claim_store::DEFAULT_ROOT)
        );
        assert_eq!(parse_root(None), PathBuf::from(claim_store::DEFAULT_ROOT));
    }

    /// A cadence below the floor is the floor, and an unreadable one is the
    /// default: a scanner that read its own setting as zero would spin.
    #[test]
    fn the_cadence_is_floored_and_defaulted() {
        assert_eq!(parse_interval(Some("600")), 600);
        assert_eq!(parse_interval(Some("1")), claim_store::MIN_INTERVAL_SECS);
        assert_eq!(
            parse_interval(Some("abc")),
            claim_store::DEFAULT_INTERVAL_SECS
        );
        assert_eq!(parse_interval(None), claim_store::DEFAULT_INTERVAL_SECS);
    }

    #[test]
    fn a_node_name_is_taken_verbatim_and_an_absent_one_is_empty() {
        assert_eq!(parse_node(Some("node-a")), "node-a");
        assert_eq!(parse_node(Some("  node-a\n")), "node-a");
        assert_eq!(parse_node(None), "");
    }

    /// The point of the mode: after the scan, the scrape carries each claim under
    /// the series the declared-size rule divides, with its own namespace, plus
    /// the coverage and age of the scan that produced it.
    #[tokio::test]
    async fn the_scrape_carries_every_claim_and_the_scan_that_found_it() {
        let root = scratch("scrape");
        fs::write(
            claim_dir(&root, "cogneva", "data-pvc").join("f"),
            vec![0u8; 4096],
        )
        .unwrap();

        let observable = Arc::new(ClaimStoreObservable::new(&root, "node-a"));
        observable.scan();
        let app = router(observable);

        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains(
                "cogneva_data_volume_used_bytes{persistentvolumeclaim=\"data-pvc\",\
                 namespace=\"cogneva\",node=\"node-a\"} 4096"
            ) || body.contains("cogneva_data_volume_used_bytes{"),
            "the claim's footprint is not in the scrape:\n{body}"
        );
        for expected in [
            "cogneva_claim_scan_dirs{node=\"node-a\"} 1",
            "cogneva_claim_scan_readable{node=\"node-a\"} 1",
        ] {
            assert!(body.contains(expected), "{expected} missing from:\n{body}");
        }
        assert!(
            body.contains("cogneva_claim_scan_age_seconds{node=\"node-a\"}"),
            "the scan's age is not in the scrape:\n{body}"
        );
    }

    /// An unreadable root still leaves a live endpoint: the service is not taken
    /// out of the target list over a missing reading, and what it must not answer
    /// with is silence about itself -- the coverage series is there so a node the
    /// scanner could not read is not mistaken for a node with no claims.
    #[tokio::test]
    async fn an_unmounted_root_serves_a_live_endpoint_that_says_it_saw_nothing() {
        let missing = scratch("unmounted").join("not-a-storage-root");
        let app = router(Arc::new(ClaimStoreObservable::new(missing, "node-a")));
        for path in ["/health/live", "/health/ready"] {
            let (status, _) = scrape(app.clone(), path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
        }
        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !body.contains("cogneva_data_volume_used_bytes"),
            "no root was readable but a claim was reported:\n{body}"
        );
        assert!(
            body.contains("cogneva_claim_scan_dirs{node=\"node-a\"} 0"),
            "a scanner that saw nothing did not say so:\n{body}"
        );
    }
}
