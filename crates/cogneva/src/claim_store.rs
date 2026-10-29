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
//! Liveness answers as soon as the listener exists, and readiness answers once
//! the first sweep has finished trying. Readiness is deliberately not "the
//! readings are good": the pod behind these probes has no data plane, so a
//! probe that failed over an unreadable root would restart it into the same
//! failure -- the mount is either there or it is not -- and an endpoint held
//! out of the target list removes the scrape instead of reporting the
//! misconfiguration. What readiness waits for is only that a walk has been
//! *attempted*; a node whose root could not be read is ready and says so
//! through `claim_scan_readable`, and the scan's age keeps growing.
//!
//! The listener opens before that walk rather than after it. The walk measures
//! this node's whole storage root and every image store it holds, which is
//! minutes on a node with a real image history (2026-10-26: four minutes and
//! twelve seconds), and an unbounded walk that had to finish before `bind`
//! could not be told apart from a process that never started: the only answer
//! the platform has for that is a restart, which begins the same walk again.
//!
//! Readiness stops short of the image stores' walks for the same reason, and
//! it is the image stores that make it matter: the node's podman store holds
//! 2.57 million entries and took ten of the twelve minutes one rollout spent
//! waiting on this pod. What readiness waits for there is the *listing* -- one
//! `read_dir` per store, which is what separates a store with no parts from
//! one not yet listed -- and the sizes follow it from the cadence loop. Until
//! the first walk completes, a store publishes only the age of that walk,
//! which counts from process start: sizes and the coverage pair (parts and
//! readable) arrive together with the walk that measured them, because a
//! coverage pair written before one would accuse every part of being
//! unreadable. A deployment that waits for readiness is waiting for this
//! process to have looked, not for the disk to have been walked, and until
//! this split it waited for the disk.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Whether the first sweep has finished *trying*. Kept out of the
    /// observables on purpose: it is a fact about this process's start-up, not
    /// about the node's disk, and a reading that has not been taken yet is not
    /// a zero.
    first_sweep: Arc<AtomicBool>,
}

fn text(status: StatusCode, body: &str) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(body.to_string()))
        .expect("static response")
}

const SWEEP_PENDING: &str =
    "the first sweep of this node's stores is still running; readings taken now would be zeros \
     that mean not measured rather than nothing there\n";

/// The process is up and serving. This says nothing about the readings: before
/// the first sweep they are not there yet, and that is what readiness is for.
async fn live() -> Response {
    text(StatusCode::OK, "ok")
}

/// Ready once every store has been listed and this node's claims walked --
/// attempted, not necessarily successfully. The image stores' byte walks are
/// not part of it: their parts are named by the listing, and a probe that
/// waited for the sizes would keep this pod out of its own target list for the
/// minutes the walk costs on every start.
async fn ready(State(state): State<StoreState>) -> Response {
    if state.first_sweep.load(Ordering::Relaxed) {
        text(StatusCode::OK, "ok")
    } else {
        text(StatusCode::SERVICE_UNAVAILABLE, SWEEP_PENDING)
    }
}

/// The scan runs on its own cadence and this handler renders the last result, so
/// a scrape does not pay for a walk of everything the node holds.
async fn metrics(State(state): State<StoreState>) -> Response {
    // Before the first sweep this process holds an empty store, and an empty
    // store renders as zeros: a reader cannot tell "nothing is mounted here"
    // from "this node has not looked yet". Answering nothing at all is the one
    // answer that does not lie, and the scrape target does not exist until
    // readiness turns, so this only reaches a reader who came looking directly.
    //
    // Past that point the image stores may still be on their first walk, and
    // what they publish in that state is the walk's age per store and nothing
    // else: the sizes and the coverage pair arrive together with the first
    // completed walk, because a coverage pair written before one would say
    // every part was unreadable. The age is the reading that says the store
    // exists and its first walk has not finished.
    if !state.first_sweep.load(Ordering::Relaxed) {
        return text(StatusCode::SERVICE_UNAVAILABLE, SWEEP_PENDING);
    }
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
    first_sweep: Arc<AtomicBool>,
) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(StoreState {
            observable,
            images,
            first_sweep,
        })
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
    // The listener opens before the sweep below, not after it. The sweep walks
    // this node's storage root and every image store on it, which is minutes
    // once a node has a real image history, and for that whole time a listener
    // that waited for it would answer nothing -- to a liveness probe that is
    // indistinguishable from a process that never started, and the only answer
    // the platform has for that is a restart, which begins the same walk again.
    // Readiness carries the wait instead: it stays unavailable until a walk has
    // been attempted, and the scrape target does not exist before then either.
    let first_sweep = Arc::new(AtomicBool::new(false));
    let addr = SocketAddr::from(([0, 0, 0, 0], port_from_env()));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "claim store listening; the first sweep runs before readings");

    // One scan before the readings open: a node whose first scrape carries no
    // claim at all is indistinguishable from a node with no claims, and the
    // process would have to be scraped for a full cadence before it said
    // anything.
    let sweeping = Arc::clone(&observable);
    let sweeping_images = images.clone();
    let done = Arc::clone(&first_sweep);
    tokio::spawn(async move {
        match sweeping.scan_blocking().await {
            Scan::Completed(report) => tracing::info!(
                root = %sweeping.root().display(),
                node = sweeping.node(),
                dirs = report.dirs,
                readable = report.readable,
                skipped = report.skipped,
                "claim store scanned the node's storage root"
            ),
            Scan::RootUnreadable => tracing::error!(
                env = claim_store::ROOT_ENV,
                node = sweeping.node(),
                root = %sweeping.root().display(),
                "no storage root readable; this process will serve coverage without any claim"
            ),
        }

        // Each image store is *listed* once, not walked: a first scrape with no
        // part in it is indistinguishable from a node whose store is empty, and
        // the read that tells them apart is one `read_dir` of the root. The
        // walk that measures those parts is minutes on a node with a real image
        // history -- 2.57 million entries and ten of those minutes on the
        // podman store alone -- and it is not what a probe should wait for: the
        // parts' names are the whole reading until the sizes arrive, and the
        // cadence loop started below fills them in on its first tick.
        for store in &sweeping_images {
            match store.enumerate_blocking().await {
                image_store::Listing::Listed(parts) => tracing::info!(
                    store = store.name(),
                    root = %store.root().display(),
                    parts,
                    "image store listed; the walk runs behind readiness"
                ),
                image_store::Listing::RootUnreadable => tracing::error!(
                    env = image_store::ROOTS_ENV,
                    store = store.name(),
                    root = %store.root().display(),
                    "no image store root readable; this process will serve coverage without any part"
                ),
            }
        }
        // Every store has been listed, readable or not. That is what readiness
        // was waiting for: the readings that follow are measurements, and a
        // node whose root could not be read says so through its coverage
        // series rather than by withholding the target.
        done.store(true, Ordering::Relaxed);

        let shutdown = ShutdownSignal::default();
        tokio::spawn(claim_store::run_claim_store_scan(
            Arc::clone(&sweeping),
            interval_from_env(),
            shutdown.clone(),
        ));
        tokio::spawn(image_store::run_image_store_scan(
            sweeping_images.iter().map(Arc::clone).collect(),
            image_interval_from_env(),
            shutdown,
        ));
    });

    axum::serve(listener, router(observable, images, first_sweep)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::scrape;
    use std::fs;
    use std::path::Path;

    fn scratch(name: &str) -> cog_test_support::ScratchDir {
        cog_test_support::ScratchDir::new(&format!("claim-mode-{name}"))
    }

    fn claim_dir(root: &Path, namespace: &str, claim: &str) -> PathBuf {
        let dir = root.join(format!(
            "pvc-11111111-2222-3333-4444-555555555555_{namespace}_{claim}"
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn empty_images() -> Vec<Arc<ImageStoreObservable>> {
        // The path names a child that must not exist; whether the scratch parent
        // outlives this call makes no difference to the reading it produces.
        let root = scratch("no-images");
        vec![Arc::new(ImageStoreObservable::new(
            root.join("absent"),
            "node-a",
            image_store::DEFAULT_STORE,
        ))]
    }

    /// A router whose first sweep has already finished -- the state every
    /// reading below is taken in, and the one `run_from_env` reaches once its
    /// own walk is done.
    fn served(
        observable: Arc<ClaimStoreObservable>,
        images: Vec<Arc<ImageStoreObservable>>,
    ) -> Router {
        router(observable, images, Arc::new(AtomicBool::new(true)))
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

        let observable = Arc::new(ClaimStoreObservable::new(root.path(), "node-a"));
        observable.scan();
        let app = served(observable, empty_images());

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

        let observable = Arc::new(ClaimStoreObservable::new(root.path(), "node-a"));
        observable.scan();
        let images = Arc::new(ImageStoreObservable::new(
            images_root.path(),
            "node-a",
            image_store::DEFAULT_STORE,
        ));
        images.scan();
        let app = served(observable, vec![images]);

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

    /// Readiness is about the listing, not the measurement: a store that has
    /// been listed and not yet walked is on a scrape that answers OK.
    ///
    /// The walk behind it is what a deployment was actually waiting for --
    /// 2.57 million entries and ten minutes on this node's podman store, which
    /// one rollout spent 749 of its 900 seconds inside. What the scrape
    /// carries in that state is the walk's age and nothing else from the
    /// store: a size published before a walk would be a zero claiming the part
    /// is empty, and the coverage pair would say every part was one the walker
    /// could not read.
    #[tokio::test]
    async fn readiness_turns_on_the_listing_and_the_sizes_arrive_behind_it() {
        let root = scratch("listed-not-walked");
        let images_root = scratch("listed-not-walked-images");
        fs::create_dir_all(images_root.join("snap")).unwrap();
        fs::write(images_root.join("snap").join("blob"), vec![0u8; 512]).unwrap();

        let images = Arc::new(ImageStoreObservable::new(
            images_root.path(),
            "node-a",
            image_store::DEFAULT_STORE,
        ));
        assert_eq!(
            images.enumerate(),
            image_store::Listing::Listed(1),
            "the listing must name the store's parts"
        );
        let app = served(
            Arc::new(ClaimStoreObservable::new(root.path(), "node-a")),
            vec![images],
        );

        let (status, body) = scrape(app.clone(), "/health/ready").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "readiness waited for a walk it does not need: {body}"
        );
        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("cogneva_image_store_age_seconds{node=\"node-a\",store=\"containerd\"}"),
            "the walk that has not finished is not visible at all:\n{body}"
        );
        for absent in [
            "cogneva_image_store_bytes",
            "cogneva_image_store_parts",
            "cogneva_image_store_readable",
        ] {
            assert!(
                !body.contains(absent),
                "{absent} was published before any walk ran, so it says a part is empty \
                 or unreadable when the walk simply has not reached it:\n{body}"
            );
        }
    }

    /// Before the first sweep the listener is up and nothing else is. Liveness
    /// has to answer there: the walk takes minutes on a node with a real image
    /// history, and a listener that waited for it looked exactly like a process
    /// that never started -- which the platform answers with a restart, and a
    /// restart begins the same walk. Readiness and the readings carry the wait
    /// instead, because an empty store renders as zeros, which a reader cannot
    /// tell from "nothing is mounted here".
    #[tokio::test]
    async fn liveness_answers_before_the_first_sweep_while_readings_wait_for_it() {
        let root = scratch("before-first-sweep");
        let first_sweep = Arc::new(AtomicBool::new(false));
        let app = router(
            Arc::new(ClaimStoreObservable::new(root.path(), "node-a")),
            empty_images(),
            Arc::clone(&first_sweep),
        );

        let (status, body) = scrape(app.clone(), "/health/live").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "liveness must answer with the listener alone, not wait for a walk: {body}"
        );
        for path in ["/health/ready", "/metrics"] {
            let (status, body) = scrape(app.clone(), path).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{path} answered before this node had looked at anything: {body}"
            );
            assert!(
                body.contains("still running"),
                "{path} said the wrong thing about why it is unavailable: {body}"
            );
        }

        first_sweep.store(true, Ordering::Relaxed);
        for path in ["/health/ready", "/metrics"] {
            let (status, body) = scrape(app.clone(), path).await;
            assert_eq!(status, StatusCode::OK, "{path} stayed unavailable: {body}");
        }
    }

    /// An unreadable root still leaves a live endpoint: the service is not taken
    /// out of the target list over a missing reading, and what it must not answer
    /// with is silence about itself -- the coverage series is there so a node the
    /// scanner could not read is not mistaken for a node with no claims.
    #[tokio::test]
    async fn an_unmounted_root_serves_a_live_endpoint_that_says_it_saw_nothing() {
        let root = scratch("unmounted");
        let missing = root.join("not-a-storage-root");
        let app = served(
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
