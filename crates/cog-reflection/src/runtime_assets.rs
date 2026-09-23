//! What the overlay recorded copying into this image, against what is live now.
//!
//! The mainline overlay is built by the deployer binary that is *currently
//! running*, and that binary is one revision behind the revision it is
//! deploying: the checkout it copies from is the target rev's, but the list of
//! what to copy is compiled into it. A list that gains a destination therefore
//! takes effect one rollout late, and if no further rollout arrives the pod
//! keeps whatever the image already had at that path, indefinitely, with
//! nothing on the runtime side that can tell the difference between "this
//! revision's asset" and "some earlier build's asset".
//!
//! Two things follow, and this module is both of them.
//!
//! The list itself is a file in the repository ([`OVERLAY_ASSETS_JSON`],
//! `deploy/overlay-assets.json`), so the deployer reads the *target* rev's list
//! out of the target rev's checkout, and only falls back to the copy embedded
//! in itself when that checkout predates the file. A revision that adds an
//! asset is then deployed with that asset, on the rollout that carries the
//! revision.
//!
//! And the overlay records what it actually copied, inside the image, at
//! [`RUNTIME_ASSET_MANIFEST_DEST`]; the application reads that record at
//! startup and judges it against the destinations its own revision declares.
//! That judgement belongs on this side because this side is the only one that
//! runs the latest code — the deployer is always the older binary, so a check
//! written there would answer the previous revision's question. This is what
//! turns "did this rollout carry the assets" from an inference into a reading.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Where the overlay records what it copied, inside the image.
///
/// One file at the application root rather than a marker inside each
/// destination: writing into an asset directory would change the very digest
/// being recorded, and the asset directories are read back by name elsewhere.
pub const RUNTIME_ASSET_MANIFEST_DEST: &str = "/opt/cogneva/runtime-assets.json";

/// The repository's asset list, embedded at build time.
///
/// This is what the deployer falls back to when the checkout it is deploying
/// carries no list. It is the same file the build reads, so there is no second
/// copy to drift; the preference for the checkout is what makes the list
/// version-appropriate.
pub const OVERLAY_ASSETS_JSON: &str = include_str!("../../../deploy/overlay-assets.json");

/// One asset the overlay re-copies: `from` is a path in the rev's checkout,
/// `to` the path inside the image.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AssetEntry {
    pub from: String,
    pub to: String,
}

#[derive(serde::Deserialize)]
struct AssetListFile {
    assets: Vec<AssetEntry>,
}

/// Parse an overlay asset list.
///
/// Rejecting an entry the overlay could not act on is the point: a destination
/// that is not an absolute image path would be copied relative to whatever the
/// builder's working directory happened to be, and the runtime check — which
/// reads destinations as absolute paths — could never find it, so the mistake
/// would surface as an asset that is silently never refreshed.
pub fn parse_asset_list(text: &str) -> Result<Vec<AssetEntry>, String> {
    let file: AssetListFile = serde_json::from_str(text)
        .map_err(|e| format!("overlay asset list is not valid JSON: {e}"))?;
    if file.assets.is_empty() {
        return Err("overlay asset list declares no assets".to_string());
    }
    for entry in &file.assets {
        if entry.from.is_empty() || entry.to.is_empty() {
            return Err(format!("overlay asset entry has an empty path: {entry:?}"));
        }
        if !entry.to.starts_with('/') {
            return Err(format!(
                "overlay asset destination must be an absolute image path: {entry:?}"
            ));
        }
    }
    Ok(file.assets)
}

/// The list this binary was built with, for a checkout that has none.
pub fn embedded_asset_list() -> Vec<AssetEntry> {
    parse_asset_list(OVERLAY_ASSETS_JSON)
        .expect("the embedded overlay asset list parses; it is compiled into this binary")
}

/// What the overlay recorded about one image it built.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AssetManifest {
    /// The revision whose checkout the copies came from.
    pub rev: String,
    /// Image destination -> digest of the checkout tree copied there.
    pub assets: BTreeMap<String, String>,
}

/// Parse what the overlay wrote into an image.
pub fn parse_manifest(text: &str) -> Result<AssetManifest, String> {
    serde_json::from_str(text).map_err(|e| format!("runtime asset manifest is not valid JSON: {e}"))
}

/// Render what the overlay writes into an image.
pub fn manifest_json(rev: &str, assets: &BTreeMap<String, String>) -> Result<String, String> {
    let manifest = AssetManifest {
        rev: rev.to_string(),
        assets: assets.clone(),
    };
    serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("serialize runtime asset manifest: {e}"))
}

/// A tree's content identity.
pub struct TreeDigest {
    pub digest: String,
    pub files: u64,
}

/// Digest every file under `root` as its checkout-relative path together with
/// its bytes, in sorted order.
///
/// Paths are hashed as well as contents, so a rename is a different identity;
/// the order is fixed so the digest does not depend on the order the filesystem
/// happens to hand entries back in. Nothing about ownership, mode or mtime is
/// hashed: those legitimately differ between the checkout a file was copied
/// from and the image it landed in, and hashing them would report drift on
/// every asset.
pub async fn digest_tree(root: &Path) -> std::io::Result<TreeDigest> {
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if entry.file_type().await?.is_dir() {
                stack.push(path);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                files.push((rel, path));
            }
        }
    }
    files.sort();

    let mut hasher = blake3::Hasher::new();
    for (rel, path) in &files {
        let bytes = tokio::fs::read(path).await?;
        hasher.update(rel.as_bytes());
        hasher.update(&[0]);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    Ok(TreeDigest {
        digest: hasher.finalize().to_hex().to_string(),
        files: files.len() as u64,
    })
}

/// What the running image's copy of one asset is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetVerdict {
    /// The overlay recorded copying this destination, and what is there now is
    /// what it copied.
    Refreshed,
    /// The overlay that built this image recorded nothing about this
    /// destination. Whatever is there is the base image's, from whenever that
    /// was built — the asset this revision declares is not what is running.
    NotRefreshed,
    /// Recorded, but the destination does not hold what was recorded: the copy
    /// did not land, landed elsewhere, or was merged onto a directory that
    /// still carries files the source no longer has.
    ContentDrift {
        recorded: String,
        live: Option<String>,
    },
}

impl AssetVerdict {
    /// The `state` label this verdict is published under, which the alert rules
    /// also select on. Kept here so the two cannot be written down twice.
    pub fn state(&self) -> &'static str {
        match self {
            AssetVerdict::Refreshed => "refreshed",
            AssetVerdict::NotRefreshed => "not_refreshed",
            AssetVerdict::ContentDrift { .. } => "content_drift",
        }
    }

    pub fn is_problem(&self) -> bool {
        !matches!(self, AssetVerdict::Refreshed)
    }
}

/// Every value [`AssetVerdict::state`] can produce, so a rule that selects one
/// can be checked against this list instead of trusted to match it.
pub const ASSET_STATE_VALUES: &[&str] = &["refreshed", "not_refreshed", "content_drift"];

/// Judge one destination.
///
/// `None` on either side is a different fact from a digest that merely
/// differs, and the two need different sentences: nothing recorded means the
/// list that built this image did not include the asset, while a recorded
/// digest that does not match means the copy was meant to happen and did not
/// land as recorded.
pub fn judge_one(recorded: Option<&str>, live: Option<&str>) -> AssetVerdict {
    match (recorded, live) {
        (None, _) => AssetVerdict::NotRefreshed,
        (Some(recorded), Some(live)) if recorded == live => AssetVerdict::Refreshed,
        (Some(recorded), live) => AssetVerdict::ContentDrift {
            recorded: recorded.to_string(),
            live: live.map(str::to_string),
        },
    }
}

/// Judge every declared destination against one image's manifest.
///
/// `live` carries one entry per declared destination; `None` for a destination
/// that could not be read. Callers pass the declared set rather than the
/// manifest's own keys, because the question is "is what this revision needs
/// there", and the manifest's keys are the previous revision's answer to it.
pub fn judge(
    declared: &[AssetEntry],
    manifest: &AssetManifest,
    live: &BTreeMap<String, Option<String>>,
) -> BTreeMap<String, AssetVerdict> {
    declared
        .iter()
        .map(|entry| {
            let recorded = manifest.assets.get(&entry.to).map(String::as_str);
            let live = live.get(&entry.to).and_then(|d| d.as_deref());
            (entry.to.clone(), judge_one(recorded, live))
        })
        .collect()
}

/// What the startup check could read of the image's manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRead {
    /// No manifest at all: the image was built by an overlay that recorded
    /// nothing. This is the absence of evidence, not evidence of correctness.
    Absent,
    /// Present but unusable, with the reason.
    Unusable(String),
    /// Present and parsed; carries the revision it recorded.
    Ok(String),
}

/// What the startup check found about this process's assets.
#[derive(Debug, Clone)]
pub struct AssetIdentityReport {
    pub manifest: ManifestRead,
    /// One verdict per declared destination, by image path. Empty unless the
    /// manifest was readable: with nothing recorded there is nothing to judge
    /// against, and reporting every asset as a problem would be reporting the
    /// recorder's absence as each asset's fault.
    pub verdicts: BTreeMap<String, AssetVerdict>,
    /// Destinations that could not be read, with why. Such a destination is
    /// judged as drift; the reason is kept for the log, where it is actionable,
    /// rather than folded into the verdict, where it would not be.
    pub unreadable: Vec<(String, String)>,
    pub checked_at_seconds: u64,
}

impl AssetIdentityReport {
    pub fn problems(&self) -> impl Iterator<Item = (&String, &AssetVerdict)> {
        self.verdicts.iter().filter(|(_, v)| v.is_problem())
    }

    /// Say what was found in the terms the reading is made of: which asset,
    /// which state, and for drift, both digests — a verdict that names what it
    /// compared is actionable; "assets are wrong" is not.
    pub fn log(&self) {
        match &self.manifest {
            ManifestRead::Absent => tracing::warn!(
                path = RUNTIME_ASSET_MANIFEST_DEST,
                "this image carries no runtime asset record, so nothing here says which \
                 revision's assets are live"
            ),
            ManifestRead::Unusable(reason) => tracing::warn!(
                path = RUNTIME_ASSET_MANIFEST_DEST,
                reason = %reason,
                "the image's runtime asset record cannot be read, so the assets are unjudged"
            ),
            ManifestRead::Ok(rev) => tracing::info!(
                rev = %rev,
                assets = self.verdicts.len(),
                "runtime asset record read from the image"
            ),
        }
        for (asset, reason) in &self.unreadable {
            tracing::warn!(
                asset = %asset,
                error = %reason,
                "live runtime asset could not be read, and is judged as drift"
            );
        }
        for (asset, verdict) in self.problems() {
            match verdict {
                AssetVerdict::NotRefreshed => tracing::error!(
                    asset = %asset,
                    "the overlay that built this image never recorded refreshing this asset: \
                     what is live is the base image's copy, not this revision's"
                ),
                AssetVerdict::ContentDrift { recorded, live } => tracing::error!(
                    asset = %asset,
                    recorded = %recorded,
                    live = %live.as_deref().unwrap_or("<unreadable>"),
                    "the live asset is not what the overlay recorded writing"
                ),
                AssetVerdict::Refreshed => {}
            }
        }
    }
}

/// Read the manifest at `manifest_path` and judge `declared` against what is
/// live at each destination.
///
/// Every failure to read is a distinct finding rather than a false clean: a
/// missing manifest, a corrupt one, and an unreadable destination all leave
/// something unjudged, and each says so in its own terms.
pub async fn verify(manifest_path: &Path, declared: &[AssetEntry]) -> AssetIdentityReport {
    let checked_at_seconds = chrono::Utc::now().timestamp().max(0) as u64;
    let text = match tokio::fs::read_to_string(manifest_path).await {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return AssetIdentityReport {
                manifest: ManifestRead::Absent,
                verdicts: BTreeMap::new(),
                unreadable: Vec::new(),
                checked_at_seconds,
            };
        }
        Err(e) => {
            return AssetIdentityReport {
                manifest: ManifestRead::Unusable(format!("{}: {e}", manifest_path.display())),
                verdicts: BTreeMap::new(),
                unreadable: Vec::new(),
                checked_at_seconds,
            };
        }
    };
    let manifest = match parse_manifest(&text) {
        Ok(manifest) => manifest,
        Err(e) => {
            return AssetIdentityReport {
                manifest: ManifestRead::Unusable(format!("{}: {e}", manifest_path.display())),
                verdicts: BTreeMap::new(),
                unreadable: Vec::new(),
                checked_at_seconds,
            };
        }
    };

    let mut live: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut unreadable = Vec::new();
    for entry in declared {
        match digest_tree(Path::new(&entry.to)).await {
            Ok(tree) => {
                tracing::info!(
                    asset = %entry.to,
                    files = tree.files,
                    "live runtime asset digested"
                );
                live.insert(entry.to.clone(), Some(tree.digest));
            }
            Err(e) => {
                unreadable.push((entry.to.clone(), e.to_string()));
                live.insert(entry.to.clone(), None);
            }
        }
    }

    let verdicts = judge(declared, &manifest, &live);
    AssetIdentityReport {
        manifest: ManifestRead::Ok(manifest.rev),
        verdicts,
        unreadable,
        checked_at_seconds,
    }
}

/// The gauges the startup check publishes.
///
/// Only the problem states are exported, and only for destinations that have
/// one: an alert reads this series as "> 0", so a destination that is fine is
/// absent rather than zero, and a process that never ran the check exports
/// none of it — which is why the check's own timestamp is a separate series.
pub const RUNTIME_ASSET_STATE_METRIC: &str = "cogneva_runtime_asset_state";
pub const RUNTIME_ASSET_MANIFEST_ABSENT_METRIC: &str = "cogneva_runtime_asset_manifest_absent";
pub const RUNTIME_ASSET_MANIFEST_UNUSABLE_METRIC: &str = "cogneva_runtime_asset_manifest_unusable";
pub const RUNTIME_ASSET_CHECK_SECONDS_METRIC: &str = "cogneva_runtime_asset_check_seconds";

/// The rules that consume these gauges, named here so the pairing is asserted
/// by a test rather than assumed.
pub const RUNTIME_ASSET_NOT_REFRESHED_RULE: &str = "runtime_asset_not_refreshed";
pub const RUNTIME_ASSET_CONTENT_DRIFT_RULE: &str = "runtime_asset_content_drift";

/// Holds the result of the one startup check so the scrape can ask for it.
///
/// One check per process is all there is to do — an image's assets are fixed
/// when it is built, and nothing at runtime changes them — so this records the
/// single answer instead of measuring on a cadence.
#[derive(Default)]
pub struct AssetIdentityObservable {
    report: tokio::sync::Mutex<Option<AssetIdentityReport>>,
}

impl AssetIdentityObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn record(&self, report: AssetIdentityReport) {
        *self.report.lock().await = Some(report);
    }
}

#[async_trait::async_trait]
impl Observable for AssetIdentityObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let guard = self.report.lock().await;
        let Some(report) = guard.as_ref() else {
            return Ok(Vec::new());
        };
        let mut out = vec![
            RawMetric::new(
                RUNTIME_ASSET_CHECK_SECONDS_METRIC,
                report.checked_at_seconds as f64,
            ),
            RawMetric::new(
                RUNTIME_ASSET_MANIFEST_ABSENT_METRIC,
                if matches!(report.manifest, ManifestRead::Absent) {
                    1.0
                } else {
                    0.0
                },
            ),
            RawMetric::new(
                RUNTIME_ASSET_MANIFEST_UNUSABLE_METRIC,
                if matches!(report.manifest, ManifestRead::Unusable(_)) {
                    1.0
                } else {
                    0.0
                },
            ),
        ];
        for (asset, verdict) in report.problems() {
            out.push(
                RawMetric::new(RUNTIME_ASSET_STATE_METRIC, 1.0)
                    .with_label("asset", asset.as_str())
                    .with_label("state", verdict.state()),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(from: &str, to: &str) -> AssetEntry {
        AssetEntry {
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    fn manifest(rev: &str, pairs: &[(&str, &str)]) -> AssetManifest {
        AssetManifest {
            rev: rev.to_string(),
            assets: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn live(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
            .collect()
    }

    #[test]
    fn the_embedded_list_is_the_repository_file() {
        let list = embedded_asset_list();
        assert!(
            list.iter().any(|e| e.to == "/opt/cogneva/skills"),
            "the asset list must hand the application its runtime assets: {list:?}"
        );
        for entry in &list {
            assert!(
                entry.to.starts_with("/opt/cogneva/"),
                "an asset must land inside the application root: {entry:?}"
            );
        }
    }

    #[test]
    fn a_list_the_overlay_could_not_act_on_is_rejected() {
        for (text, why) in [
            ("{", "not JSON"),
            ("{\"assets\": []}", "no assets"),
            (
                "{\"assets\": [{\"from\": \"skills\", \"to\": \"relative/skills\"}]}",
                "destination is not an absolute image path",
            ),
            (
                "{\"assets\": [{\"from\": \"\", \"to\": \"/opt/cogneva/skills\"}]}",
                "empty source",
            ),
        ] {
            assert!(
                parse_asset_list(text).is_err(),
                "a list entry ({why}) must be refused, not copied somewhere the check cannot \
                 find it: {text}"
            );
        }
        assert!(parse_asset_list(OVERLAY_ASSETS_JSON).is_ok());
    }

    /// The three verdicts differ in the direction that matters: what the
    /// overlay recorded, and what is actually there. Testing only the bad
    /// samples would leave a checker that calls everything broken looking
    /// correct.
    #[test]
    fn a_fresh_asset_and_a_stale_one_are_told_apart() {
        let declared = [entry("skills", "/opt/cogneva/skills")];

        let recorded = manifest("rev-new", &[("/opt/cogneva/skills", "aaa")]);

        let verdicts = judge(
            &declared,
            &recorded,
            &live(&[("/opt/cogneva/skills", Some("aaa"))]),
        );
        assert_eq!(verdicts["/opt/cogneva/skills"], AssetVerdict::Refreshed);

        // The overlay that built this image did not know about the destination
        // this revision declares: the list came from the revision before.
        let older_list = manifest("rev-old", &[]);
        let verdicts = judge(
            &declared,
            &older_list,
            &live(&[("/opt/cogneva/skills", Some("bbb"))]),
        );
        assert_eq!(verdicts["/opt/cogneva/skills"], AssetVerdict::NotRefreshed);

        // Recorded, and the bytes there now are not the ones it wrote.
        let verdicts = judge(
            &declared,
            &recorded,
            &live(&[("/opt/cogneva/skills", Some("bbb"))]),
        );
        assert_eq!(
            verdicts["/opt/cogneva/skills"],
            AssetVerdict::ContentDrift {
                recorded: "aaa".to_string(),
                live: Some("bbb".to_string()),
            }
        );

        // Recorded, and nothing is there at all.
        let verdicts = judge(
            &declared,
            &recorded,
            &live(&[("/opt/cogneva/skills", None)]),
        );
        assert_eq!(
            verdicts["/opt/cogneva/skills"],
            AssetVerdict::ContentDrift {
                recorded: "aaa".to_string(),
                live: None,
            }
        );
    }

    /// The digest has to be an identity, not a description: the same tree twice
    /// must agree, and any difference in content, in a file's name, or in the
    /// set of files must show up as a different digest.
    #[tokio::test]
    async fn the_digest_changes_with_content_and_with_the_set_of_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("skills");
        tokio::fs::create_dir_all(root.join("nested"))
            .await
            .unwrap();
        tokio::fs::write(root.join("generator.json"), b"[\"read_file\"]")
            .await
            .unwrap();
        tokio::fs::write(root.join("nested/evaluator.json"), b"{}")
            .await
            .unwrap();

        let first = digest_tree(&root).await.unwrap();
        assert_eq!(first.files, 2);
        assert_eq!(digest_tree(&root).await.unwrap().digest, first.digest);

        tokio::fs::write(
            root.join("generator.json"),
            b"[\"read_file\",\"write_file\"]",
        )
        .await
        .unwrap();
        assert_ne!(
            digest_tree(&root).await.unwrap().digest,
            first.digest,
            "a file whose contents changed must not digest the same"
        );

        // A rename with identical contents is a different tree.
        let renamed = dir.path().join("renamed");
        tokio::fs::create_dir_all(renamed.join("nested"))
            .await
            .unwrap();
        tokio::fs::write(
            renamed.join("generator.json"),
            b"[\"read_file\",\"write_file\"]",
        )
        .await
        .unwrap();
        tokio::fs::write(renamed.join("nested/evaluator.json"), b"{}")
            .await
            .unwrap();
        tokio::fs::write(renamed.join("extra.json"), b"{}")
            .await
            .unwrap();
        assert_ne!(
            digest_tree(&renamed).await.unwrap().digest,
            digest_tree(&root).await.unwrap().digest,
            "a file the source gained must not digest the same"
        );
    }

    /// The manifest is the only thing standing between the overlay and the
    /// application, so it must survive the round trip it actually makes: a JSON
    /// file written into an image and read back out.
    #[test]
    fn the_manifest_round_trips() {
        let mut assets = BTreeMap::new();
        assets.insert("/opt/cogneva/skills".to_string(), "abc".to_string());
        let text = manifest_json("rev-1", &assets).unwrap();
        let parsed = parse_manifest(&text).unwrap();
        assert_eq!(parsed.rev, "rev-1");
        assert_eq!(parsed.assets, assets);
    }

    #[tokio::test]
    async fn an_image_without_a_manifest_is_not_reported_as_clean() {
        let dir = tempfile::tempdir().unwrap();
        let declared = [entry("skills", "/opt/cogneva/skills")];
        let report = verify(&dir.path().join("runtime-assets.json"), &declared).await;
        assert_eq!(report.manifest, ManifestRead::Absent);
        assert!(
            report.verdicts.is_empty(),
            "nothing was recorded, so nothing can be judged: {:?}",
            report.verdicts
        );
    }

    #[tokio::test]
    async fn a_corrupt_manifest_is_its_own_finding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime-assets.json");
        tokio::fs::write(&path, b"{not json").await.unwrap();
        let declared = [entry("skills", "/opt/cogneva/skills")];
        let report = verify(&path, &declared).await;
        assert!(
            matches!(report.manifest, ManifestRead::Unusable(_)),
            "a manifest that cannot be read is not the same as one that is missing: {:?}",
            report.manifest
        );
        assert!(report.verdicts.is_empty());
    }

    /// End to end over a real tree: the overlay's own digest of the source, the
    /// manifest it would write, and the judgement the application makes when it
    /// reads that manifest against the same bytes on disk.
    #[tokio::test]
    async fn the_application_clears_an_asset_the_overlay_copied() {
        let dir = tempfile::tempdir().unwrap();
        // 检出里的那一份和镜像落点上的那一份是两个不同的绝对路径、同一份内容。
        // 部署器据前者算摘要，应用据后者算，两边要对得上才说明改写没漂。
        let checkout = dir.path().join("checkout/skills");
        let live = dir.path().join("image/opt/cogneva/skills");
        for root in [&checkout, &live] {
            tokio::fs::create_dir_all(root).await.unwrap();
            tokio::fs::write(root.join("generator.json"), b"[\"read_file\"]")
                .await
                .unwrap();
        }
        let copied = digest_tree(&checkout).await.unwrap().digest;
        assert_eq!(
            copied,
            digest_tree(&live).await.unwrap().digest,
            "摘要按检出内相对路径算：换一个绝对根，同一份内容必须同值"
        );

        let manifest_path = dir.path().join("runtime-assets.json");
        let mut assets = BTreeMap::new();
        assets.insert(live.to_str().unwrap().to_string(), copied);
        tokio::fs::write(&manifest_path, manifest_json("rev-1", &assets).unwrap())
            .await
            .unwrap();

        let declared = [entry("skills", live.to_str().unwrap())];
        let report = verify(&manifest_path, &declared).await;
        assert_eq!(report.manifest, ManifestRead::Ok("rev-1".to_string()));
        assert_eq!(
            report.problems().count(),
            0,
            "an asset copied from this very tree must judge clean: {:?}",
            report.verdicts
        );

        // Now what is live is no longer what was recorded — the case a copy that
        // landed elsewhere, or a merge onto a directory the source has moved
        // past, would produce.
        tokio::fs::write(live.join("generator.json"), b"[\"write_file\"]")
            .await
            .unwrap();
        let report = verify(&manifest_path, &declared).await;
        assert_eq!(
            report.problems().count(),
            1,
            "a destination that drifted from the record must be a problem: {:?}",
            report.verdicts
        );
        assert!(
            matches!(
                report.verdicts.get(live.to_str().unwrap()),
                Some(AssetVerdict::ContentDrift { .. })
            ),
            "漂移要与「从未重拷过」分开报，否则两种病因在同一个标量里分不出：{:?}",
            report.verdicts
        );
    }

    /// The alert rules select on label values this module writes. A rule that
    /// names a state nothing produces never fires, and never says so.
    #[test]
    fn the_rules_select_on_states_this_module_produces() {
        let config = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../deploy/helm/cogneva/files/cogneva.json"),
        )
        .expect("read the chart's cogneva.json");
        let parsed: serde_json::Value = serde_json::from_str(&config).expect("chart JSON parses");
        let rules = parsed["observability"]["infra_watch"]["rules"]
            .as_array()
            .expect("rules array");

        let mut seen_rules = 0;
        for rule in rules {
            let promql = rule["promql"].as_str().unwrap_or_default();
            if !promql.contains(RUNTIME_ASSET_STATE_METRIC) {
                continue;
            }
            seen_rules += 1;
            for value in state_label_values(promql) {
                assert!(
                    ASSET_STATE_VALUES.contains(&value.as_str()),
                    "rule {} selects state=\"{value}\", which no verdict produces: {ASSET_STATE_VALUES:?}",
                    rule["name"].as_str().unwrap_or_default()
                );
            }
        }
        assert_eq!(
            seen_rules, 2,
            "both asset rules must exist and select on the state metric"
        );

        let names: Vec<&str> = rules.iter().filter_map(|r| r["name"].as_str()).collect();
        for rule in [
            RUNTIME_ASSET_NOT_REFRESHED_RULE,
            RUNTIME_ASSET_CONTENT_DRIFT_RULE,
        ] {
            assert!(names.contains(&rule), "chart is missing rule {rule}");
        }
    }

    /// Every `state="..."` value in a rule expression.
    fn state_label_values(promql: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = promql;
        while let Some(at) = rest.find("state=\"") {
            rest = &rest[at + "state=\"".len()..];
            if let Some(end) = rest.find('"') {
                out.push(rest[..end].to_string());
                rest = &rest[end..];
            }
        }
        out
    }

    #[test]
    fn the_state_label_reader_finds_every_selector() {
        assert_eq!(
            state_label_values("cogneva_runtime_asset_state{state=\"not_refreshed\"} > 0"),
            vec!["not_refreshed".to_string()]
        );
        assert_eq!(
            state_label_values(
                "cogneva_runtime_asset_state{state=\"content_drift\", asset=\"/x\"} > 0"
            ),
            vec!["content_drift".to_string()]
        );
        assert!(state_label_values("up > 0").is_empty());
    }
}
