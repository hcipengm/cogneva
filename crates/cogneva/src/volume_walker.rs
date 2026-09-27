//! Standalone volume footprint walker: measures a volume whose writer is not
//! this repository's code.
//!
//! Two ways to answer "how much of this volume is in use" exist, and they do not
//! agree. A process that has the volume mounted can walk it and add up what is
//! on the disk. A process that can only reach the writer over its API gets what
//! the writer says it holds — for a store, the bytes its live tags reference,
//! which is *not* what occupies the disk: a blob no tag points at any more stays
//! there, and the API cannot see it. Neither is wrong; they are different
//! quantities, and only the first one is what a claim's size means.
//!
//! This mode exists because the volume that fills up fastest here is written by
//! a third-party registry that publishes no metrics and cannot be told to, so
//! there is no process of ours in that pod to walk the mount — the walker is a
//! separate program, run as a second container that mounts the same claim. It
//! reports `cogneva_data_volume_used_bytes`, the same series every other
//! workload reports for its own mounts, because the rule that compares a volume
//! against its declared size divides that series and means exactly this
//! quantity. The API's narrower answer keeps its own name
//! (`cogneva_registry_referenced_bytes`), published from the process that asks
//! it: what the store references is what a retention policy can still reclaim,
//! and the gap between the two numbers is the mass no deletion has freed yet.
//!
//! The declaration is the same one the in-process walkers read
//! (`COGNEVA_DATA_VOLUME_MOUNTS`, `claim=path` per line), so a deployment that
//! says which volume to measure says it the same way here.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use cog_core::observability_text::render_raw_metrics;
use cog_core::{Observable, ShutdownSignal};
use cog_observability::data_volume::{self, DataVolumeObservable};

/// Port this mode serves on.
pub const PORT_ENV: &str = "COGNEVA_VOLUME_WALKER_PORT";

/// Port used when the deployment names none.
///
/// Nothing else in the pod this runs in listens here; the Service that exposes
/// it names the endpoint `http`, which is the name the scraper discovers by.
pub const DEFAULT_PORT: u16 = 9100;

/// Port this mode should listen on, from the environment.
pub fn port_from_env() -> u16 {
    parse_port(std::env::var(PORT_ENV).ok().as_deref()).unwrap_or(DEFAULT_PORT)
}

/// Pure half of [`port_from_env`], so the default is testable without touching
/// a process-wide variable other tests share.
fn parse_port(raw: Option<&str>) -> Option<u16> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok())
}

#[derive(Clone)]
struct WalkerState {
    observables: Arc<Vec<Arc<DataVolumeObservable>>>,
}

async fn health() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("ok"))
        .expect("static response")
}

/// Every reading this process holds: the volumes it walks, plus its own
/// background loops.
///
/// A walker that stopped would leave its last measurement in place, and a volume
/// that is not growing produces the same line, so the loop census is rendered
/// next to the walk for the same reason the walk exists.
async fn metrics(State(state): State<WalkerState>) -> Response {
    let mut body = String::new();
    for observable in state.observables.iter() {
        match observable.collect_metrics("").await {
            Ok(readings) => body.push_str(&render_raw_metrics(&readings)),
            Err(e) => tracing::warn!(
                claim = %observable.claim(),
                error = %e,
                "volume footprint unavailable this scrape"
            ),
        }
    }
    let loops = cog_core::loop_health::observable();
    match loops.collect_metrics("").await {
        Ok(readings) => body.push_str(&render_raw_metrics(&readings)),
        Err(e) => tracing::warn!(error = %e, "background loop readings unavailable this scrape"),
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("static response")
}

/// Router over a set of already-built observables.
pub fn router(observables: Vec<Arc<DataVolumeObservable>>) -> Router {
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/metrics", get(metrics))
        .with_state(WalkerState {
            observables: Arc::new(observables),
        })
}

/// Entry point for the `volume-walker` subcommand.
///
/// A declaration this process cannot act on is logged and then served anyway:
/// the probes below answer with the health of the *listener*, and both of them
/// stay green when the walker has nothing to walk. That is deliberate. This
/// container is not the only one in its pod — the process distributing images
/// cluster-wide is the other one — and a container that fails a probe takes the
/// whole pod out of service, so a missing environment variable here would stop
/// every node's image pulls to report a missing reading. The reading's absence is
/// a fact about the deployment, and it is reported where deployments are read:
/// the delivery gate asserts that a pod which declares a volume also runs a
/// walker for it, and the log line below says which declaration became nothing.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    // Standalone modes do not go through `run_app`, so they own their logging
    // setup; without it the lines below (the only place a bad declaration is
    // visible) would go nowhere.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let (observables, problems) = data_volume::observables_from_env();
    for problem in &problems {
        tracing::error!(problem = %problem, "volume footprint declaration became no reading");
    }
    let marked: Vec<String> = observables.iter().map(|o| o.claim().to_string()).collect();
    if marked.is_empty() {
        tracing::error!(
            env = data_volume::MOUNTS_ENV,
            "no volume declared; this process will serve an empty /metrics"
        );
    } else {
        tracing::info!(volumes = ?marked, "volume footprint walker measuring declared volumes");
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], port_from_env()));
    // The reading belongs to the pod, so the walkers are handed no stop signal:
    // a walker that stopped would leave its last measurement in place.
    data_volume::spawn_watchers(
        &observables,
        data_volume::scan_interval_from_env(),
        ShutdownSignal::default(),
    );
    tracing::info!(addr = %addr, "volume walker listening");
    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        router(observables),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use std::path::PathBuf;
    use tower::ServiceExt;

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cog-volwalker-{}-{}", name, std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The default is only used when the deployment names no port, so it must be
    /// reachable without a name and a malformed value must not silently become
    /// port 0 (which binds a random port the Service could never target).
    #[test]
    fn a_named_port_wins_and_anything_unusable_falls_back_to_the_default() {
        assert_eq!(parse_port(Some("9101")), Some(9101));
        assert_eq!(parse_port(Some(" 9101 ")), Some(9101));
        assert_eq!(parse_port(None), None);
        assert_eq!(parse_port(Some("")), None);
        assert_eq!(parse_port(Some("nine thousand")), None);
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

    /// The point of the mode: the number it publishes is the bytes on the disk,
    /// under the series name the declared-size rule divides.
    ///
    /// Both halves are asserted. A walk is skipped when the volume is unreadable,
    /// and a scrape that never reached the walk would also report nothing — so
    /// the exact byte count is what says the walk happened, and the file it
    /// counted is what says the count is this volume's.
    #[tokio::test]
    async fn the_walk_publishes_what_occupies_the_volume() {
        let dir = scratch("walk");
        // 4096 bytes of content in one file. Sizes are what the walk adds up
        // (directory entries are not), so the expected reading is exact.
        std::fs::write(dir.join("blob"), vec![0u8; 4096]).unwrap();

        let (observables, problems) = data_volume::observables_from_declaration(
            None,
            Some(&format!("cogneva-registry-pvc={}", dir.display())),
        );
        assert!(problems.is_empty(), "{problems:?}");
        assert_eq!(observables.len(), 1);

        let app = router(observables.clone());
        let (status, body) = scrape(app.clone(), "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !body.contains("cogneva_data_volume_used_bytes"),
            "a reading before the first walk claims the volume is empty:\n{body}"
        );

        observables[0].measure_blocking().await.unwrap();
        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains(
                "cogneva_data_volume_used_bytes{persistentvolumeclaim=\"cogneva-registry-pvc\"} \
                 4096"
            ),
            "walk reading missing from the scrape:\n{body}"
        );
        // The same byte count must not also be reachable under the store's own
        // series name: those two numbers differ, and one name cannot carry both.
        assert!(
            !body.contains("cogneva_registry_referenced_bytes"),
            "the walker published the store's narrower number:\n{body}"
        );
    }

    /// A declaration that yields nothing still leaves a live endpoint: the pod
    /// this runs in is not taken out of service over a missing reading, so the
    /// endpoint answering with no readings is the shape the deployment sees.
    #[tokio::test]
    async fn an_empty_declaration_serves_a_live_but_empty_endpoint() {
        let app = router(Vec::new());
        for path in ["/health/live", "/health/ready"] {
            let (status, _) = scrape(app.clone(), path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
        }
        let (status, body) = scrape(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !body.contains("cogneva_data_volume_used_bytes"),
            "no volume was declared but one was reported:\n{body}"
        );
    }
}
