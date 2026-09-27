//! What the in-cluster registry's volume is holding, read from the registry.
//!
//! The registry is the one volume in this deployment whose writer is not our
//! process: it runs `registry:2`, its store is its own filesystem, and nothing
//! in that pod can be asked to walk it. The reading a volume normally gets from
//! its own writer is therefore out of reach here, and the kubelet's per-volume
//! series cannot stand in for it -- for a directory-backed claim it reports the
//! node's filesystem, which reads 689 GB for every volume in this cluster,
//! a 10 GiB claim among them.
//!
//! The party that does know is the store itself. Every tag the registry serves
//! resolves to a manifest naming the config and layer blobs it references, each
//! with its size, and the bytes those blobs occupy are what the volume is
//! holding. So this reading is taken from the owner through the registry's own
//! API rather than from a neighbouring filesystem, and it is published under the
//! volume family's series and label: one wall per claim, compared against the
//! declared size by the same rule as every other volume.
//!
//! Two quantities it deliberately leaves out, both because the API does not
//! expose them: the store's own metadata (manifests and upload bookkeeping --
//! megabytes against the gigabytes that matter) and blobs no tag references any
//! more. The second is why reclaiming bytes belongs to the code that knows which
//! tags it removed rather than to a watcher of this number: after a deletion the
//! blobs stay on disk until something unlinks them, and this reading would go on
//! counting the volume as full, which is right.
//!
//! Who measures, and why it is the same process that pushes: the deployer's
//! `buildah push` is the registry's only writer. One writer reading its own
//! store keeps the tag list this walks stable while it walks it, and keeps the
//! bytes a deletion frees attributable to the process that deleted them.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cog_core::claim_footprint::{
    CLAIM_LABEL, DEFAULT_SCAN_INTERVAL_SECS, INTERVAL_ENV, MIN_SCAN_INTERVAL_SECS,
};
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFError, SFResult, ShutdownSignal};
use tracing::{info, warn};

use crate::mainline_deployer::{endpoint_host_port, parse_http_response, IMAGE_REPOSITORY};

pub use cog_core::claim_footprint::USED_METRIC;

/// How many tags the repository serves, published beside the bytes.
///
/// A number that only ever grows is what an unreclaimed registry looks like when
/// it is still far from its declared size: the bytes answer "how full", this
/// answers "is anything being removed".
pub const TAG_COUNT_METRIC: &str = "cogneva_registry_tag_count";

/// Seconds since the store was last measured successfully.
///
/// The bytes keep their last reading when a walk fails, which is the right thing
/// to publish and is also invisible: a walker that stopped leaves behind a store
/// that reads as unchanging rather than as unmeasured.
pub const SCAN_AGE_METRIC: &str = "cogneva_registry_scan_age_seconds";

/// Deployment variable naming the claim behind the registry's store.
///
/// The declaration is what gates the reading, exactly as it does for the
/// directory walkers: the name is written in the deployment and read here, and
/// the two are different files. Empty means this process measures no registry --
/// a deployment pointed at an external registry, or one that runs no deployer.
pub const CLAIM_ENV: &str = "COGNEVA_REGISTRY_CLAIM";

/// This loop's name in the liveness census.
pub const WATCH_LOOP: &str = "registry_footprint_watch";

/// Media types a manifest request accepts, one per tag and per index child.
///
/// The registry refuses to serve an OCI manifest to a client that did not say it
/// understands one (`MANIFEST_UNKNOWN: OCI manifest found, but accept header
/// does not support OCI manifests`), and these images are OCI.
const MANIFEST_ACCEPTS: &[&str] = &[
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
];

/// How deep an index may nest before this walk gives up on the shape.
///
/// An index whose children are indexes is legal and never produced here; a walk
/// that followed that unboundedly would be a loop, and one that stopped quietly
/// would under-count. So the limit is a bound on what may be read, and a manifest
/// beyond it fails the walk rather than contributing nothing.
const MAX_INDEX_DEPTH: usize = 2;

/// The read side of the registry's HTTP API, over the plain-HTTP path `buildah`
/// already uses for the same store.
#[derive(Debug, Clone)]
pub struct RegistryClient {
    endpoint: String,
}

impl RegistryClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    /// GET one path, 200 only.
    ///
    /// Unreachable, non-200 and malformed are all errors: a caller deciding
    /// whether the store is full has to tell "this path was not there" from "the
    /// size was zero", and a body it could not parse from an empty one.
    pub async fn get(&self, path: &str, accept: &[&str]) -> SFResult<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (host, port) = endpoint_host_port(&self.endpoint).ok_or_else(|| {
            SFError::Config(format!(
                "registry endpoint {:?} is not host:port",
                self.endpoint
            ))
        })?;
        let accepted = if accept.is_empty() {
            String::new()
        } else {
            format!("Accept: {}\r\n", accept.join(", "))
        };
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n{accepted}Connection: close\r\n\r\n");
        let mut stream = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::net::TcpStream::connect((host, port)),
        )
        .await
        .map_err(|_| SFError::IO(format!("registry {} connect timed out", self.endpoint)))?
        .map_err(|e| SFError::IO(format!("registry {} connect failed: {e}", self.endpoint)))?;
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("registry request write failed: {e}")))?;
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(60), stream.read_to_end(&mut raw))
            .await
            .map_err(|_| SFError::IO("registry read timed out".into()))?
            .map_err(|e| SFError::IO(format!("registry read failed: {e}")))?;
        let (status, body) = parse_http_response(&raw)
            .ok_or_else(|| SFError::IO("registry returned a malformed HTTP response".into()))?;
        if status != 200 {
            return Err(SFError::IO(format!("registry GET {path} -> {status}")));
        }
        Ok(body)
    }
}

/// The registry store's footprint, as of the last walk that succeeded.
///
/// The measurement is written by the scanning task and read by the metrics pull,
/// which are different tasks, hence the atomics rather than a lock. Cloning the
/// handle shares the measurement: the two sides see one number, not a copy each.
#[derive(Debug)]
pub struct RegistryFootprint {
    client: RegistryClient,
    claim: String,
    used_bytes: AtomicU64,
    tag_count: AtomicU64,
    measured: AtomicBool,
    /// Unix seconds of the last walk that succeeded, 0 before the first.
    last_success_secs: AtomicU64,
    /// Unix seconds this handle was built, the age's floor before a first walk.
    started_secs: u64,
}

impl RegistryFootprint {
    pub fn new(endpoint: impl Into<String>, claim: impl Into<String>) -> Self {
        Self {
            client: RegistryClient::new(endpoint),
            claim: claim.into(),
            used_bytes: AtomicU64::new(0),
            tag_count: AtomicU64::new(0),
            measured: AtomicBool::new(false),
            last_success_secs: AtomicU64::new(0),
            started_secs: unix_now(),
        }
    }

    /// The claim this reading is attributed to, which is the label the rule
    /// joins the declared size on.
    pub fn claim(&self) -> &str {
        &self.claim
    }

    pub fn endpoint(&self) -> &str {
        self.client.endpoint.as_str()
    }

    /// The last successful measurement, or `None` before one has happened.
    ///
    /// Not zero: a reading that exists before anything was walked says the store
    /// is empty, which is a claim about the store rather than an absence of
    /// evidence.
    pub fn value(&self) -> Option<u64> {
        self.measured
            .load(Ordering::Acquire)
            .then(|| self.used_bytes.load(Ordering::Relaxed))
    }

    /// The tag count of the last successful walk, published with the bytes so the
    /// two cannot be read apart.
    pub fn tags(&self) -> Option<u64> {
        self.measured
            .load(Ordering::Acquire)
            .then(|| self.tag_count.load(Ordering::Relaxed))
    }

    /// Seconds since the last successful walk, or since this handle was built if
    /// no walk has ever succeeded.
    pub fn scan_age_secs(&self, now: u64) -> u64 {
        let last = self.last_success_secs.load(Ordering::Relaxed);
        let from = if last == 0 { self.started_secs } else { last };
        now.saturating_sub(from)
    }

    /// Walk the store and record what it is holding.
    ///
    /// Every failure is the whole walk's: a tag whose manifest could not be read
    /// leaves its layers out of the sum, and a sum that is too small can only
    /// silence the comparison this feeds. So a partial walk changes nothing and
    /// is reported instead.
    pub async fn measure(&self) -> SFResult<u64> {
        let tags = self.tags_list().await?;
        // Deduplicated by digest: the layers of rev N are also the base of rev
        // N+1, so summing tag by tag would count the same bytes once per rev that
        // inherits them, and the reading would grow with the age of the store
        // rather than with its contents.
        let mut blobs: BTreeMap<String, u64> = BTreeMap::new();
        for tag in &tags {
            self.collect_manifest(&tag_manifest_path(tag), &mut blobs)
                .await?;
        }
        let total: u64 = blobs.values().sum();
        self.used_bytes.store(total, Ordering::Relaxed);
        self.tag_count.store(tags.len() as u64, Ordering::Relaxed);
        self.last_success_secs.store(unix_now(), Ordering::Relaxed);
        self.measured.store(true, Ordering::Release);
        Ok(total)
    }

    /// The tags the repository serves, in the order the registry lists them.
    async fn tags_list(&self) -> SFResult<Vec<String>> {
        let body = self.client.get(&tags_path(), &[]).await?;
        let doc: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| SFError::IO(format!("registry tag list is not JSON: {e}")))?;
        // A repository with no tags answers `{"name":...,"tags":null}`, which is
        // an empty store rather than a failed read.
        let Some(tags) = doc.get("tags") else {
            return Ok(Vec::new());
        };
        if tags.is_null() {
            return Ok(Vec::new());
        }
        let listed = tags
            .as_array()
            .ok_or_else(|| SFError::IO("registry tag list is not an array".into()))?;
        let mut out = Vec::with_capacity(listed.len());
        for tag in listed {
            let tag = tag
                .as_str()
                .ok_or_else(|| SFError::IO("registry tag list holds a non-string".into()))?;
            out.push(tag.to_string());
        }
        Ok(out)
    }

    /// Add one manifest's blobs to `blobs`, following an index into its children.
    ///
    /// Iterative rather than recursive, with the depth carried on the stack: an
    /// index of indexes is legal, and the bound has to be enforced somewhere that
    /// does not grow a future per level.
    async fn collect_manifest(
        &self,
        first: &str,
        blobs: &mut BTreeMap<String, u64>,
    ) -> SFResult<()> {
        let mut pending = vec![(first.to_string(), 0usize)];
        while let Some((path, depth)) = pending.pop() {
            if depth > MAX_INDEX_DEPTH {
                return Err(SFError::IO(format!(
                    "registry manifest {path} nests indexes deeper than {MAX_INDEX_DEPTH}"
                )));
            }
            let body = self.client.get(&path, MANIFEST_ACCEPTS).await?;
            let doc: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| SFError::IO(format!("registry manifest {path} is not JSON: {e}")))?;
            if let Some(children) = doc.get("manifests").and_then(|c| c.as_array()) {
                for child in children {
                    let digest = child
                        .get("digest")
                        .and_then(|d| d.as_str())
                        .ok_or_else(|| {
                            SFError::IO(format!("index {path} names a child without a digest"))
                        })?;
                    pending.push((manifest_path(digest), depth + 1));
                }
                continue;
            }
            for key in ["config", "layers"] {
                let present = doc
                    .get(key)
                    .ok_or_else(|| SFError::IO(format!("manifest {path} has no {key}")))?;
                let entries = match key {
                    "config" => std::slice::from_ref(present),
                    _ => present.as_array().ok_or_else(|| {
                        SFError::IO(format!("manifest {path} {key} is not an array"))
                    })?,
                };
                for entry in entries {
                    let digest = entry
                        .get("digest")
                        .and_then(|d| d.as_str())
                        .ok_or_else(|| {
                            SFError::IO(format!("manifest {path} {key} has no digest"))
                        })?;
                    // A size that is absent is not a size of zero: it would
                    // under-count silently, which is the direction that hides a
                    // full volume.
                    let size = entry
                        .get("size")
                        .and_then(|s| s.as_u64())
                        .ok_or_else(|| SFError::IO(format!("manifest {path} {key} has no size")))?;
                    blobs.insert(digest.to_string(), size);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Observable for RegistryFootprint {
    /// Nothing at all before the first walk completes, so the series the rule
    /// compares against the declared size does not exist yet rather than existing
    /// with a value nothing measured. A process that runs no deployer publishes
    /// none of these, which is what "this one does not measure the registry"
    /// looks like from outside.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let Some(bytes) = self.value() else {
            return Ok(Vec::new());
        };
        let mut out =
            vec![RawMetric::new(USED_METRIC, bytes as f64).with_label(CLAIM_LABEL, self.claim())];
        if let Some(tags) = self.tags() {
            out.push(RawMetric::new(TAG_COUNT_METRIC, tags as f64));
        }
        out.push(RawMetric::new(
            SCAN_AGE_METRIC,
            self.scan_age_secs(unix_now()) as f64,
        ));
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The footprint is not a per-dimension metric: every dimension reads the one
    /// store, so declaring no dimension is what tells the collector to pull this
    /// observable once rather than once per question.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// Re-measure the store on a timer and publish the result.
pub fn spawn_watch(
    footprint: Arc<RegistryFootprint>,
    interval_secs: u64,
    shutdown: ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let interval = Duration::from_secs(interval_secs.max(MIN_SCAN_INTERVAL_SECS));
    // Every series this watcher publishes is its own measurement, so a task that
    // died leaves a store that reads as unchanged rather than as unmeasured. Its
    // liveness therefore cannot come from itself.
    cog_core::loop_health::spawn(
        WATCH_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let footprint = Arc::clone(&footprint);
            let shutdown = shutdown.clone();
            async move {
                info!(
                    endpoint = %footprint.endpoint(),
                    claim = %footprint.claim(),
                    interval_secs = interval.as_secs(),
                    metric = USED_METRIC,
                    "registry footprint watcher started"
                );
                let mut ticker = tokio::time::interval(interval);
                loop {
                    // One stamp per pass, whatever the pass measured: a store that
                    // did not grow is not a watcher that stopped.
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => break,
                        _ = ticker.tick() => {
                            match footprint.measure().await {
                                Ok(bytes) => info!(
                                    bytes,
                                    tags = footprint.tags().unwrap_or(0),
                                    metric = USED_METRIC,
                                    "registry footprint measured"
                                ),
                                // The last reading stands and its age grows, which is
                                // what tells this apart from a store that stopped
                                // growing.
                                Err(e) => warn!(
                                    error = %e,
                                    age_secs = footprint.scan_age_secs(unix_now()),
                                    "registry footprint walk failed; the last reading stands"
                                ),
                            }
                        }
                    }
                }
            }
        },
    )
}

/// Build the watcher from the deployment's declaration.
///
/// A claim that was never declared is not an error: a deployment with an external
/// registry, or one that runs no deployer, has no store here to measure, and the
/// reading is absent rather than reported as zero. A declaration that cannot be
/// used, on the other hand, is returned as a problem: a claim name that reaches
/// the series and a pod that measures nothing look identical from outside.
pub fn from_env(
    endpoint: &str,
    namespace: &str,
    claim: Option<&str>,
) -> (Option<Arc<RegistryFootprint>>, Vec<String>) {
    let mut problems = Vec::new();
    let Some(claim) = claim.map(str::trim).filter(|c| !c.is_empty()) else {
        return (None, problems);
    };
    let endpoint = if endpoint.trim().is_empty() {
        format!("cogneva-registry.{namespace}.svc.cluster.local:5000")
    } else {
        endpoint.trim_end_matches('/').to_string()
    };
    if endpoint_host_port(&endpoint).is_none() {
        problems.push(format!(
            "registry endpoint {endpoint:?} is not host:port; the store behind claim {claim} will not be measured"
        ));
        return (None, problems);
    }
    (
        Some(Arc::new(RegistryFootprint::new(endpoint, claim))),
        problems,
    )
}

/// The cadence this process re-walks the store at, from the volume family's own
/// variable so that every footprint on one process is as fresh as the others.
pub fn scan_interval_from_env() -> u64 {
    std::env::var(INTERVAL_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_SCAN_INTERVAL_SECS)
        .max(MIN_SCAN_INTERVAL_SECS)
}

/// Unix seconds now, saturating at the epoch.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn tags_path() -> String {
    format!("/v2/{IMAGE_REPOSITORY}/tags/list")
}

fn tag_manifest_path(tag: &str) -> String {
    format!("/v2/{IMAGE_REPOSITORY}/manifests/{tag}")
}

fn manifest_path(digest: &str) -> String {
    format!("/v2/{IMAGE_REPOSITORY}/manifests/{digest}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A registry that routes by path, so a walk's request order is not part of
    /// what the test asserts. The table is shared so a test can change what the
    /// store serves between two walks of the same handle.
    async fn stub_registry(
        routes: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let mut seen = Vec::new();
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    continue;
                }
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let target = req
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("")
                    .to_string();
                let resp = routes
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(path, _)| *path == target)
                    .map(|(_, body)| body.clone())
                    .unwrap_or_else(|| {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".into()
                    });
                seen.push(target);
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
            seen
        });
        (format!("127.0.0.1:{port}"), handle)
    }

    fn routes(entries: Vec<(String, String)>) -> Arc<std::sync::Mutex<Vec<(String, String)>>> {
        Arc::new(std::sync::Mutex::new(entries))
    }

    fn http_200(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    fn layer(digest: &str, size: u64) -> String {
        format!(
            r#"{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{digest}","size":{size}}}"#
        )
    }

    fn manifest(layers: &[String]) -> String {
        format!(
            r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:cfg","size":100}},"layers":[{}]}}"#,
            layers.join(",")
        )
    }

    fn tags_body(tags: &[&str]) -> String {
        let listed: Vec<String> = tags.iter().map(|t| format!("\"{t}\"")).collect();
        format!(r#"{{"name":"cogneva","tags":[{}]}}"#, listed.join(","))
    }

    /// The point of the reading: the bytes are per blob, not per tag. Every rev
    /// inherits its base's layers, so a sum over tags would count them once per
    /// rev that inherits them and grow with the age of the store instead of with
    /// what it holds.
    #[tokio::test]
    async fn shared_layers_are_counted_once_and_tags_are_counted() {
        let shared = layer("sha256:base", 5_000);
        let (endpoint, _handle) = stub_registry(routes(vec![
            (tags_path(), http_200(&tags_body(&["local", "main-a"]))),
            (
                tag_manifest_path("local"),
                http_200(&manifest(&[shared.clone(), layer("sha256:new", 700)])),
            ),
            (
                tag_manifest_path("main-a"),
                http_200(&manifest(std::slice::from_ref(&shared))),
            ),
        ]))
        .await;

        let footprint = RegistryFootprint::new(endpoint, "cogneva-registry-pvc");
        assert_eq!(footprint.measure().await.unwrap(), 5_800);
        assert_eq!(footprint.value(), Some(5_800));
        assert_eq!(footprint.tags(), Some(2));
    }

    /// A tag pointing at an index is the shape a multi-platform image has; the
    /// bytes are the children's.
    #[tokio::test]
    async fn an_index_is_followed_to_its_children() {
        let (endpoint, _handle) = stub_registry(routes(vec![
            (tags_path(), http_200(&tags_body(&["local"]))),
            (
                tag_manifest_path("local"),
                http_200(
                    r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"sha256:plat","size":400}]}"#,
                ),
            ),
            (
                manifest_path("sha256:plat"),
                http_200(&manifest(&[layer("sha256:bin", 2_000)])),
            ),
        ]))
        .await;

        let footprint = RegistryFootprint::new(endpoint, "claim");
        assert_eq!(footprint.measure().await.unwrap(), 2_100);
    }

    /// A partial walk is not a smaller reading. The tag whose manifest could not
    /// be read has layers too, so counting the rest would report a store as
    /// emptier than it is -- the direction that hides a full volume.
    #[tokio::test]
    async fn a_missing_manifest_fails_the_walk_and_keeps_the_last_reading() {
        let table = routes(vec![
            (tags_path(), http_200(&tags_body(&["local"]))),
            (
                tag_manifest_path("local"),
                http_200(&manifest(&[layer("sha256:bin", 1_000)])),
            ),
        ]);
        let (endpoint, _handle) = stub_registry(Arc::clone(&table)).await;
        let footprint = RegistryFootprint::new(endpoint, "claim");
        assert_eq!(footprint.measure().await.unwrap(), 1_100);
        let measured_at = unix_now();

        // The same repository, now listing a tag the server does not serve: the
        // walk fails as a whole rather than adding up the tags it could read.
        table.lock().unwrap()[0].1 = http_200(&tags_body(&["local", "main-gone"]));
        assert!(footprint.measure().await.is_err());
        assert_eq!(footprint.value(), Some(1_100), "the last reading stands");
        assert!(
            footprint.scan_age_secs(measured_at + 300) >= 300,
            "and the age says how old it is"
        );
    }

    /// An absent size is not a size of zero: reading it as one would under-count
    /// silently, and this reading exists to be compared against a declaration.
    #[tokio::test]
    async fn a_blob_without_a_size_fails_the_walk() {
        let (endpoint, _handle) = stub_registry(routes(vec![
            (tags_path(), http_200(&tags_body(&["local"]))),
            (
                tag_manifest_path("local"),
                http_200(
                    r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"sha256:cfg","size":100},"layers":[{"digest":"sha256:bin"}]}"#,
                ),
            ),
        ]))
        .await;
        let footprint = RegistryFootprint::new(endpoint, "claim");
        assert!(footprint.measure().await.is_err());
        assert_eq!(footprint.value(), None);
    }

    /// A repository with no tags is an empty store, not a failed read.
    #[tokio::test]
    async fn an_empty_repository_measures_zero() {
        let (endpoint, _handle) = stub_registry(routes(vec![(
            tags_path(),
            http_200(r#"{"name":"cogneva","tags":null}"#),
        )]))
        .await;
        let footprint = RegistryFootprint::new(endpoint, "claim");
        assert_eq!(footprint.measure().await.unwrap(), 0);
        assert_eq!(footprint.tags(), Some(0));
    }

    /// The reading appears only once something has been measured: a zero would be
    /// a claim that the store is empty.
    #[tokio::test]
    async fn nothing_is_published_before_a_walk_succeeds() {
        let footprint = RegistryFootprint::new("127.0.0.1:1", "claim");
        assert!(footprint.collect_metrics("").await.unwrap().is_empty());
    }

    /// The claim and the age are what make the bytes usable: one names the
    /// declaration they are compared against, the other tells a store that
    /// stopped growing from a walker that stopped walking.
    #[tokio::test]
    async fn the_reading_carries_the_claim_and_the_age() {
        let (endpoint, _handle) = stub_registry(routes(vec![
            (tags_path(), http_200(&tags_body(&["local"]))),
            (
                tag_manifest_path("local"),
                http_200(&manifest(&[layer("sha256:bin", 4_096)])),
            ),
        ]))
        .await;
        let footprint = RegistryFootprint::new(endpoint, "cogneva-registry-pvc");
        footprint.measure().await.unwrap();
        let metrics = footprint.collect_metrics("").await.unwrap();
        let used = metrics
            .iter()
            .find(|m| m.name == USED_METRIC)
            .expect("the footprint series");
        assert_eq!(
            used.labels.get(CLAIM_LABEL).map(String::as_str),
            Some("cogneva-registry-pvc")
        );
        assert_eq!(used.value, 4_196.0);
        assert!(metrics.iter().any(|m| m.name == TAG_COUNT_METRIC));
        let age = metrics
            .iter()
            .find(|m| m.name == SCAN_AGE_METRIC)
            .expect("the age series");
        assert!(age.value < 60.0, "a fresh walk is not an old one");
        // The age is what stands in for a walk that stopped: the same handle,
        // read 300s later, must say so.
        assert!(footprint.scan_age_secs(unix_now() + 300) >= 300);
    }

    /// The declaration gates the reading, and a claim that cannot be reached is
    /// reported rather than left looking like a store nobody measures.
    #[test]
    fn the_declaration_gates_the_reading() {
        let (none, problems) = from_env("reg:5000", "cogneva", None);
        assert!(none.is_none());
        assert!(problems.is_empty(), "no declaration is not a problem");
        let (none, problems) = from_env("reg:5000", "cogneva", Some("  "));
        assert!(none.is_none());
        assert!(problems.is_empty());

        let (some, problems) = from_env("reg:5000", "cogneva", Some("cogneva-registry-pvc"));
        assert!(problems.is_empty());
        let footprint = some.expect("a declared claim is measured");
        assert_eq!(footprint.claim(), "cogneva-registry-pvc");
        assert_eq!(footprint.endpoint(), "reg:5000");

        // An empty endpoint falls back to the service name the deployer pushes to.
        let (some, _) = from_env("", "cogneva", Some("claim"));
        assert_eq!(
            some.unwrap().endpoint(),
            "cogneva-registry.cogneva.svc.cluster.local:5000"
        );

        // An endpoint that is not host:port would fail every walk; saying so now
        // is the difference between a broken declaration and an unfilled one.
        let (none, problems) = from_env("https://reg/", "cogneva", Some("claim"));
        assert!(none.is_none());
        assert_eq!(problems.len(), 1);
    }
}
