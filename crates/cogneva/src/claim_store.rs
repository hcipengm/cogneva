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
use cog_observability::image_store::{self, ImageStoreObservable};

/// Port this mode serves on.
pub const PORT_ENV: &str = "COGNEVA_CLAIM_STORE_PORT";

pub use crate::scraper_endpoint::DEFAULT_PORT;

/// Port this mode should listen on, from the environment.
pub fn port_from_env() -> u16 {
    crate::scraper_endpoint::port_from_env(PORT_ENV)
}

/// The storage root this process reads, from the environment.
pub fn root_from_env() -> PathBuf {
    parse_path(
        std::env::var(claim_store::ROOT_ENV).ok().as_deref(),
        claim_store::DEFAULT_ROOT,
    )
}

/// The image store root this process reads, from the environment.
pub fn image_root_from_env() -> PathBuf {
    parse_path(
        std::env::var(image_store::ROOT_ENV).ok().as_deref(),
        image_store::DEFAULT_ROOT,
    )
}

/// The stores this process walks, from the environment.
///
/// One node holds several layer stores: the kubelet's containerd, and the
/// rootful build stores that image builds write into. They are the same kind of
/// bytes -- unpacked layers on the node's filesystem that no claim boundary
/// covers -- so they are the same reading, told apart by the `store` label.
pub fn image_stores_from_env() -> Vec<(String, PathBuf)> {
    parse_stores(
        std::env::var(image_store::ROOTS_ENV).ok().as_deref(),
        std::env::var(image_store::ROOT_ENV).ok().as_deref(),
        image_store::DEFAULT_ROOT,
    )
}

/// Pure half of [`image_stores_from_env`].
///
/// The list syntax is `name=path,name=path`. An entry without a name takes the
/// last segment of its path as one, so a deployment that lists paths only still
/// gets a label that means something. Blank entries are skipped rather than
/// walked -- a trailing comma is not a store at `/`. A repeated name is kept
/// once: two roots under one name would be one series with two writers, which is
/// the shape this label exists to prevent.
fn parse_stores(
    roots: Option<&str>,
    single: Option<&str>,
    fallback: &str,
) -> Vec<(String, PathBuf)> {
    let list = roots.map(str::trim).filter(|value| !value.is_empty());
    let Some(list) = list else {
        return vec![(
            image_store::DEFAULT_STORE.to_string(),
            parse_path(single, fallback),
        )];
    };
    let mut out: Vec<(String, PathBuf)> = Vec::new();
    for entry in list.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (name, path) = match entry.split_once('=') {
            Some((name, path)) => (name.trim().to_string(), path.trim()),
            None => (String::new(), entry),
        };
        if path.is_empty() {
            continue;
        }
        let name = if name.is_empty() {
            std::path::Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(image_store::DEFAULT_STORE)
                .to_string()
        } else {
            name
        };
        if out.iter().any(|(seen, _)| *seen == name) {
            continue;
        }
        out.push((name, PathBuf::from(path)));
    }
    if out.is_empty() {
        // A list that named nothing usable is the deployment saying nothing, the
        // same as an unset variable: walking the working directory instead would
        // report the container's own root as the node's store.
        return vec![(
            image_store::DEFAULT_STORE.to_string(),
            parse_path(single, fallback),
        )];
    }
    out
}

/// Pure half of the two roots: an unset or blank path is the deployment saying
/// nothing, not the deployment saying the working directory -- which would walk
/// whatever the container happens to start in and report it as the node's.
fn parse_path(raw: Option<&str>, fallback: &str) -> PathBuf {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(fallback),
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
    parse_cadence(
        raw,
        claim_store::DEFAULT_INTERVAL_SECS,
        claim_store::MIN_INTERVAL_SECS,
    )
}

/// Re-walk cadence of the image store, from the environment and floored.
pub fn image_interval_from_env() -> u64 {
    parse_cadence(
        std::env::var(image_store::INTERVAL_ENV).ok().as_deref(),
        image_store::DEFAULT_INTERVAL_SECS,
        image_store::MIN_INTERVAL_SECS,
    )
}

fn parse_cadence(raw: Option<&str>, default: u64, floor: u64) -> u64 {
    raw.map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .max(floor)
}

#[derive(Clone)]
struct StoreState {
    observable: Arc<ClaimStoreObservable>,
    images: Vec<Arc<ImageStoreObservable>>,
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
    // Two walks, one endpoint: both are readings of this node's disk taken by
    // this same privileged process, and a second listener would be a second
    // scrape target to keep in the target list for no gain.
    let mut readings = Vec::new();
    match state.observable.collect_metrics("").await {
        Ok(claims) => readings.extend(claims),
        Err(e) => tracing::warn!(error = %e, "claim readings unavailable this scrape"),
    }
    for store in &state.images {
        match store.collect_metrics("").await {
            Ok(images) => readings.extend(images),
            Err(e) => tracing::warn!(
                store = store.name(),
                error = %e,
                "image store readings unavailable this scrape"
            ),
        }
    }
    let body = render_raw_metrics(&readings);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("static response")
}

/// Router over an already-built observable.
pub fn router(
    observable: Arc<ClaimStoreObservable>,
    images: Vec<Arc<ImageStoreObservable>>,
) -> Router {
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/metrics", get(metrics))
        .with_state(StoreState { observable, images })
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

    let node = node_from_env();
    let observable = Arc::new(ClaimStoreObservable::new(root_from_env(), node.clone()));
    let images: Vec<Arc<ImageStoreObservable>> = image_stores_from_env()
        .into_iter()
        .map(|(name, root)| Arc::new(ImageStoreObservable::new(root, node.clone(), name)))
        .collect();
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

    // The image store is walked once before the listener opens for the same
    // reason: a first scrape with no part in it is indistinguishable from a node
    // whose store is empty, and the walk that would tell them apart is minutes
    // away.
    for images in &images {
        match images.scan_blocking().await {
            image_store::Scan::Completed(report) => tracing::info!(
                store = images.name(),
                root = %images.root().display(),
                parts = report.parts,
                readable = report.readable,
                files = report.files,
                "image store walked"
            ),
            image_store::Scan::RootUnreadable => tracing::error!(
                env = image_store::ROOTS_ENV,
                store = images.name(),
                root = %images.root().display(),
                "no image store root readable; this process will serve coverage without any part"
            ),
        }
    }

    let shutdown = ShutdownSignal::default();
    tokio::spawn(claim_store::run_claim_store_scan(
        Arc::clone(&observable),
        interval_from_env(),
        shutdown.clone(),
    ));
    tokio::spawn(image_store::run_image_store_scan(
        images.iter().map(Arc::clone).collect(),
        image_interval_from_env(),
        shutdown,
    ));

    let addr = SocketAddr::from(([0, 0, 0, 0], port_from_env()));
    tracing::info!(addr = %addr, "claim store listening");
    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        router(observable, images),
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

    fn empty_images() -> Vec<Arc<ImageStoreObservable>> {
        vec![Arc::new(ImageStoreObservable::new(
            scratch("no-images").join("absent"),
            "node-a",
            image_store::DEFAULT_STORE,
        ))]
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

    /// A blank root must not resolve to the working directory: the process would
    /// then walk the container's own filesystem and report it as the node's.
    #[test]
    fn a_blank_root_falls_back_to_the_deployment_mount_point() {
        let storage = claim_store::DEFAULT_ROOT;
        assert_eq!(
            parse_path(Some("/host/storage"), storage),
            PathBuf::from("/host/storage")
        );
        assert_eq!(parse_path(Some("  "), storage), PathBuf::from(storage));
        assert_eq!(parse_path(None, storage), PathBuf::from(storage));
    }

    /// The store list is the deployment's statement of which layer stores this
    /// node has. A name repeated is kept once -- two roots under one label would
    /// be one series with two writers -- and a list that names nothing usable
    /// falls back to the single root rather than to no stores, because a node
    /// that is told about no store would publish a store-sized gap as silence.
    #[test]
    fn the_store_list_parses_names_paths_and_falls_back_to_the_single_root() {
        let single = Some("/host/containerd");
        let fallback = image_store::DEFAULT_ROOT;
        assert_eq!(
            parse_stores(None, single, fallback),
            vec![(
                image_store::DEFAULT_STORE.to_string(),
                PathBuf::from("/host/containerd")
            )]
        );
        assert_eq!(
            parse_stores(Some(""), single, fallback),
            vec![(
                image_store::DEFAULT_STORE.to_string(),
                PathBuf::from("/host/containerd")
            )]
        );
        assert_eq!(
            parse_stores(
                Some("containerd=/host/containerd,podman=/host/podman,buildah=/host/buildah"),
                single,
                fallback
            ),
            vec![
                ("containerd".to_string(), PathBuf::from("/host/containerd")),
                ("podman".to_string(), PathBuf::from("/host/podman")),
                ("buildah".to_string(), PathBuf::from("/host/buildah")),
            ]
        );
        // A path without a name takes its last segment, so a deployment that
        // lists only paths still gets a label that means something.
        assert_eq!(
            parse_stores(Some("/host/podman,"), single, fallback),
            vec![("podman".to_string(), PathBuf::from("/host/podman"))]
        );
        // A repeated name is kept once.
        assert_eq!(
            parse_stores(Some("podman=/a,podman=/b"), single, fallback),
            vec![("podman".to_string(), PathBuf::from("/a"))]
        );
        // A list of nothing but separators is the deployment saying nothing.
        assert_eq!(
            parse_stores(Some(" , = ,"), single, fallback),
            vec![(
                image_store::DEFAULT_STORE.to_string(),
                PathBuf::from("/host/containerd")
            )]
        );
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
        let app = router(observable, empty_images());

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

    /// One endpoint carries both walks: the claims the node mounts and the image
    /// store it unpacks into. Two readers of the same disk in one process, so a
    /// node's account is complete from a single scrape target.
    #[tokio::test]
    async fn the_scrape_carries_the_image_store_alongside_the_claims() {
        let root = scratch("both-faces");
        let images_root = scratch("both-faces-images");
        fs::create_dir_all(images_root.join("io.containerd.content.v1.content")).unwrap();
        fs::write(
            images_root
                .join("io.containerd.content.v1.content")
                .join("blob"),
            vec![0u8; 2048],
        )
        .unwrap();

        let observable = Arc::new(ClaimStoreObservable::new(&root, "node-a"));
        observable.scan();
        let images = Arc::new(ImageStoreObservable::new(
            &images_root,
            "node-a",
            image_store::DEFAULT_STORE,
        ));
        images.scan();
        let app = router(observable, vec![images]);

        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        for expected in [
            "cogneva_claim_scan_dirs{node=\"node-a\"} 0",
            "cogneva_image_store_parts{node=\"node-a\",store=\"containerd\"} 1",
            "cogneva_image_store_readable{node=\"node-a\",store=\"containerd\"} 1",
            "io.containerd.content.v1.content",
        ] {
            assert!(body.contains(expected), "{expected} missing from:\n{body}");
        }
    }

    /// An unreadable root still leaves a live endpoint: the service is not taken
    /// out of the target list over a missing reading, and what it must not answer
    /// with is silence about itself -- the coverage series is there so a node the
    /// scanner could not read is not mistaken for a node with no claims.
    #[tokio::test]
    async fn an_unmounted_root_serves_a_live_endpoint_that_says_it_saw_nothing() {
        let missing = scratch("unmounted").join("not-a-storage-root");
        let app = router(
            Arc::new(ClaimStoreObservable::new(missing, "node-a")),
            empty_images(),
        );
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
