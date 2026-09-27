//! Gate: a volume a deployment declares must be measured by the process in it.
//!
//! The footprint reading has one producer per pod that writes a volume, and the
//! declaration lives in the delivery manifests while the producer lives in code.
//! Nothing connects them: the deployment sets a variable, the pod starts, and a
//! process that never reads the variable looks exactly like one that measures
//! its volume and finds it small. Measured on this repository: the security
//! gateway's mirror volume was declared in every profile and published by
//! nothing, so `data_volume_over_declared_size` could never see the one volume
//! that gateway alone can measure.
//!
//! Three things are asserted over the delivered manifests and the sources:
//!
//! - every declaring container belongs to a workload in [`PRODUCERS`], so a new
//!   workload that declares a volume without a producer fails here instead of
//!   reading as covered (unclassified fails: the failure mode is silence);
//! - every `PRODUCERS` row is still exercised by a delivered manifest, so a row
//!   whose workload was renamed or stopped declaring is deleted rather than left
//!   describing a layout that no longer exists;
//! - the row's marker, a call that constructs the producer, is still in the file
//!   it names — a name mentioned in a module's prose does not count, which is
//!   why the markers are call shapes rather than the constants they call.
//!
//! The declaration itself is checked against the same manifest: a `claim=path`
//! entry has to be a mount of that very claim inside the container that declares
//! it, because a reading walks a directory, and a directory that is not the
//! claim's measures something else and publishes it under the claim's name.
//!
//! A claim whose store belongs to someone else's process is also measured
//! through that store's own API: the declaring container does not mount it at
//! all, so this reading comes from outside the pod that writes it. Those
//! declarations are keyed separately ([`API_MEASURED`]) and get their own
//! judgement, which is written to be at least as strong as the mount pairing it
//! replaces: the claim such a declaration names has to be a claim the delivery
//! declares as an object, that some container in that delivery mounts, and that
//! is **also walked** by a producer here. Asking the store is not an alternative
//! to walking it: the store answers what its live tags reference, which is not
//! what occupies the disk — a blob no tag points at any more is invisible to it.
//! So a claim whose only reading comes from an API has a number for a different
//! quantity, and the rule that divides that number by the declared size reads a
//! volume that cannot fill up. The claim also may not be walked *twice*, which
//! would publish two different byte counts under one claim name.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde::Deserialize;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every directory whose manifests can be delivered to a cluster.
///
/// The same set the rollout pinning is judged on: the static k3s tree the
/// in-cluster deployer applies from the repository, and the three pre-rendered
/// profiles the bootstrap applies without helm.
const DELIVERED: [&str; 4] = [
    "deploy/k3s",
    "deploy/rendered/k3s-single",
    "deploy/rendered/k3s-multi",
    "deploy/rendered/k8s-standard",
];

/// Workloads whose process measures the volumes it declares, and the evidence.
///
/// The workload name is the key because it is the only machine-readable
/// identity a manifest carries: container commands are shell wrappers or absent
/// entirely, so a table keyed on them would need a shell parser and would
/// silently skip the workloads it cannot read. The marker is the call that
/// constructs the producer on that workload's own entry path, not the name of
/// the constant it passes.
const PRODUCERS: [(&str, &str, &str); 5] = [
    (
        // The full application: the observability plugin builds one observable
        // per declared volume and starts its watcher.
        "cogneva",
        "crates/cog-observability/src/plugin.rs",
        "data_volume::run_data_volume_watch(",
    ),
    (
        // The evolution deployment runs the same entry as the application, on a
        // pod whose volume is its own.
        "cogneva-evolution",
        "crates/cog-observability/src/plugin.rs",
        "data_volume::run_data_volume_watch(",
    ),
    (
        // The standalone executor builds its observables in the workdir router.
        "cogneva-sandbox-executor",
        "crates/cog-extension/src/workdir.rs",
        "ClaimFootprint::for_mounted_volume(",
    ),
    (
        // The standalone gateway has no configuration document of its own; it
        // reads the declaration from the environment and starts the watchers.
        "cogneva-security-gateway",
        "crates/cog-gateway/src/security_gateway.rs",
        "data_volume::spawn_watchers(",
    ),
    (
        // The registry's store is written by an image from outside this
        // repository, so its pod gets a second container that does nothing but
        // walk the mounted claim — same standalone entry shape as the gateway,
        // mounted on the volume the registry writes.
        "cogneva-registry",
        "crates/cogneva/src/volume_walker.rs",
        "data_volume::spawn_watchers(",
    ),
];

/// A claim measured through its store's API, whose writer is not this code.
///
/// Same shape as [`PRODUCERS`] — workload, the file whose call builds the
/// producer, the call — because the evidence for "something measures it" is the
/// same kind of evidence. What differs is the quantity: the store is written by
/// an image from outside this repository, and the bytes it can report are the
/// ones it still references, not the ones on the disk. That is a reading in its
/// own right (it is what a retention policy can still reclaim), and it is
/// deliberately *not* the volume family's series — which is why a row here does
/// not stand in for a walker and the judgement below requires both.
const API_MEASURED: [(&str, &str, &str); 1] = [(
    // The cluster registry's store is written by the registry image; the only
    // writer from this repository is the mainline deployer in this pod, which
    // measures it through the registry's own API. The volume's occupancy comes
    // from the walker the registry's own deployment runs.
    "cogneva-evolution",
    "crates/cog-reflection/src/registry_footprint.rs",
    "RegistryFootprint::new(",
)];

const MOUNTS_ENV: &str = "COGNEVA_DATA_VOLUME_MOUNTS";
const CLAIM_ENV: &str = "COGNEVA_DATA_VOLUME_CLAIM";
const API_CLAIM_ENV: &str = "COGNEVA_REGISTRY_CLAIM";

/// One container that declares volumes to be measured.
#[derive(Debug)]
struct Declaration {
    dir: &'static str,
    workload: String,
    container: String,
    /// `claim=path` entries from the mounts variable, in the order declared.
    mounts: Vec<(String, String)>,
    /// Claim named by the claim variable, which names no path of its own.
    claim: Option<String>,
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// Walk a YAML document for every mapping that is a pod spec.
///
/// A pod spec is recognised by its shape — a `containers` sequence beside the
/// `volumes` those containers mount — rather than by the object kind wrapping
/// it, so Deployment, StatefulSet, DaemonSet, Job and a bare Pod are all reached
/// without listing them.
fn pod_specs<'a>(value: &'a serde_yaml::Value, out: &mut Vec<&'a serde_yaml::Mapping>) {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            if mapping
                .get("containers")
                .and_then(|c| c.as_sequence())
                .is_some()
            {
                out.push(mapping);
            }
            for child in mapping.values() {
                pod_specs(child, out);
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                pod_specs(item, out);
            }
        }
        _ => {}
    }
}

/// The claim behind every mount path a container has, keyed by the path.
///
/// A path can be mounted from something other than a claim (configMap, secret,
/// emptyDir); those map to nothing, which is what makes a declaration naming one
/// of them fail the pairing check.
fn mounted_claims(
    pod: &serde_yaml::Mapping,
    container: &serde_yaml::Mapping,
) -> BTreeMap<String, String> {
    let mut claims_by_volume = BTreeMap::new();
    if let Some(volumes) = pod.get("volumes").and_then(|v| v.as_sequence()) {
        for volume in volumes {
            let Some(volume) = volume.as_mapping() else {
                continue;
            };
            let name = volume.get("name").and_then(|n| n.as_str());
            let claim = volume
                .get("persistentVolumeClaim")
                .and_then(|p| p.as_mapping())
                .and_then(|p| p.get("claimName"))
                .and_then(|c| c.as_str());
            if let (Some(name), Some(claim)) = (name, claim) {
                claims_by_volume.insert(name.to_string(), claim.to_string());
            }
        }
    }

    let mut claims = BTreeMap::new();
    if let Some(mounts) = container.get("volumeMounts").and_then(|m| m.as_sequence()) {
        for mount in mounts {
            let Some(mount) = mount.as_mapping() else {
                continue;
            };
            let name = mount.get("name").and_then(|n| n.as_str());
            let path = mount.get("mountPath").and_then(|p| p.as_str());
            if let (Some(name), Some(path)) = (name, path) {
                if let Some(claim) = claims_by_volume.get(name) {
                    claims.insert(path.trim_end_matches('/').to_string(), claim.clone());
                }
            }
        }
    }
    claims
}

fn env_value(container: &serde_yaml::Mapping, name: &str) -> Option<String> {
    let env = container.get("env").and_then(|e| e.as_sequence())?;
    for entry in env {
        let entry = entry.as_mapping()?;
        if entry.get("name").and_then(|n| n.as_str()) == Some(name) {
            return entry
                .get("value")
                .and_then(|v| v.as_str())
                .map(str::to_string);
        }
    }
    None
}

/// Every declaration the delivered manifests carry, beside the claim behind each
/// mount of the container that made it.
fn declarations() -> Vec<(Declaration, BTreeMap<String, String>)> {
    let mut found = Vec::new();
    for dir in DELIVERED {
        let manifest_dir = repo_root().join(dir);
        let entries = match std::fs::read_dir(&manifest_dir) {
            Ok(entries) => entries,
            Err(e) => panic!("{} unreadable: {e}", manifest_dir.display()),
        };
        for entry in entries {
            let file = entry.expect("delivered manifest entry").path();
            if !matches!(
                file.extension().and_then(|e| e.to_str()),
                Some("yaml") | Some("yml")
            ) {
                continue;
            }
            let text = std::fs::read_to_string(&file).expect("delivered manifest readable");
            for document in serde_yaml::Deserializer::from_str(&text) {
                let value = serde_yaml::Value::deserialize(document)
                    .unwrap_or_else(|e| panic!("{} is not YAML: {e}", file.display()));
                let Some(workload) = value
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                else {
                    continue;
                };
                let mut pods = Vec::new();
                pod_specs(&value, &mut pods);
                for pod in pods {
                    for list in ["containers", "initContainers"] {
                        let Some(containers) = pod.get(list).and_then(|c| c.as_sequence()) else {
                            continue;
                        };
                        for container in containers {
                            let Some(container) = container.as_mapping() else {
                                continue;
                            };
                            let mounts_raw = env_value(container, MOUNTS_ENV);
                            let claim = env_value(container, CLAIM_ENV)
                                .map(|c| c.trim().to_string())
                                .filter(|c| !c.is_empty());
                            if mounts_raw.is_none() && claim.is_none() {
                                continue;
                            }
                            let mut mounts = Vec::new();
                            for line in mounts_raw.unwrap_or_default().lines() {
                                let line = line.trim();
                                if line.is_empty() {
                                    continue;
                                }
                                let Some((claim, path)) = line.split_once('=') else {
                                    panic!(
                                        "{dir}/{workload}: {MOUNTS_ENV} entry {line:?} is not \
                                         claim=path, so nothing can check what it measures"
                                    )
                                };
                                mounts.push((
                                    claim.trim().to_string(),
                                    path.trim().trim_end_matches('/').to_string(),
                                ));
                            }
                            found.push((
                                Declaration {
                                    dir,
                                    workload: workload.to_string(),
                                    container: container
                                        .get("name")
                                        .and_then(|n| n.as_str())
                                        .unwrap_or("<unnamed>")
                                        .to_string(),
                                    mounts,
                                    claim,
                                },
                                mounted_claims(pod, container),
                            ));
                        }
                    }
                }
            }
        }
    }
    found
}

/// Unclassified fails, and so does a row nobody exercises.
///
/// The two halves are one assertion on purpose: the table has to describe
/// exactly the declaring workloads that are delivered, so a rename, a removal,
/// or a new declaration cannot pass by editing the manifests alone.
#[test]
fn every_declaring_workload_is_one_a_producer_measures() {
    let declared = declarations();
    assert!(
        !declared.is_empty(),
        "no volume footprint is declared anywhere in the delivered manifests, so this gate read \
         nothing"
    );

    let known: BTreeSet<&str> = PRODUCERS.iter().map(|(name, ..)| *name).collect();
    let seen: BTreeSet<&str> = declared.iter().map(|(d, _)| d.workload.as_str()).collect();

    for (declaration, _) in &declared {
        assert!(
            known.contains(declaration.workload.as_str()),
            "{}/{} (container {}): this workload declares a volume footprint and no process \
             measures it, so the series it names can never exist — wire a producer into the entry \
             that pod runs, or add the workload to PRODUCERS",
            declaration.dir,
            declaration.workload,
            declaration.container
        );
    }
    for row in &known {
        assert!(
            seen.contains(row),
            "PRODUCERS lists {row}, which no delivered manifest declares any more: the row is \
             describing a layout that is gone and would otherwise licence the next gap"
        );
    }
}

/// A row's evidence must still be in the file it names.
#[test]
fn every_producer_row_still_names_a_call_that_builds_the_producer() {
    for (workload, file, marker) in PRODUCERS {
        let source = read(file);
        assert!(
            source.contains(marker),
            "{workload}: {file} no longer contains {marker:?}, so the producer this row stands \
             for was renamed, moved, or dropped"
        );
    }
}

/// The declared pairing must be a real mount of that claim, in that container.
///
/// A path that is not the claim's measures something else and is published under
/// the claim's name: the reading then divides the wrong bytes by the declared
/// size, which invents an overrun on one volume and hides the one on another.
#[test]
fn every_declaration_names_a_volume_that_container_actually_mounts() {
    for (declaration, mounted) in declarations() {
        for (claim, path) in &declaration.mounts {
            match mounted.get(path) {
                Some(mounted_claim) => assert_eq!(
                    mounted_claim, claim,
                    "{}/{} (container {}): {path} is mounted from {mounted_claim} and declared as \
                     {claim}",
                    declaration.dir, declaration.workload, declaration.container
                ),
                None => panic!(
                    "{}/{} (container {}): declares {claim}={path}, and that container mounts \
                     nothing at {path}, so the walk would measure some other directory",
                    declaration.dir, declaration.workload, declaration.container
                ),
            }
        }
        if let Some(claim) = &declaration.claim {
            assert!(
                mounted.values().any(|mounted_claim| mounted_claim == claim),
                "{}/{} (container {}): {CLAIM_ENV} names {claim}, which that container does not \
                 mount",
                declaration.dir,
                declaration.workload,
                declaration.container
            );
        }
    }
}

/// The scan reads what it claims to read: every delivered directory is present
/// and at least one manifest in it declares a volume.
#[test]
fn the_scan_covers_every_delivered_directory() {
    let declared = declarations();
    for dir in DELIVERED {
        let path = repo_root().join(dir);
        assert!(path.is_dir(), "{} is not a directory", path.display());
        assert!(
            declared.iter().any(|(d, _)| d.dir == dir),
            "{dir} declares no volume footprint anywhere, so this gate read nothing there"
        );
    }
}

/// What one delivered directory says about claims, gathered in one pass.
#[derive(Default)]
struct DeliveryFacts {
    /// Claims the delivery declares as objects, so a name in a variable has
    /// something to point at.
    declared: BTreeSet<String>,
    /// Claims some container mounts, whoever declared them.
    mounted: BTreeSet<String>,
    /// Claims declared as walkable mounts anywhere in the delivery, each mapped
    /// to the `<workload>/<container>` that walks it.
    walked: BTreeMap<String, BTreeSet<String>>,
    /// `(workload, container, claim)` for every API-measured declaration.
    measured: Vec<(String, String, String)>,
}

fn delivery_facts() -> BTreeMap<&'static str, DeliveryFacts> {
    let mut facts = BTreeMap::new();
    for dir in DELIVERED {
        let mut entry = DeliveryFacts::default();
        let manifest_dir = repo_root().join(dir);
        let entries = match std::fs::read_dir(&manifest_dir) {
            Ok(entries) => entries,
            Err(e) => panic!("{} unreadable: {e}", manifest_dir.display()),
        };
        for file_entry in entries {
            let file = file_entry.expect("delivered manifest entry").path();
            if !matches!(
                file.extension().and_then(|e| e.to_str()),
                Some("yaml") | Some("yml")
            ) {
                continue;
            }
            let text = std::fs::read_to_string(&file).expect("delivered manifest readable");
            for document in serde_yaml::Deserializer::from_str(&text) {
                let value = serde_yaml::Value::deserialize(document)
                    .unwrap_or_else(|e| panic!("{} is not YAML: {e}", file.display()));
                if value.get("kind").and_then(|k| k.as_str()) == Some("PersistentVolumeClaim") {
                    if let Some(name) = value
                        .get("metadata")
                        .and_then(|m| m.get("name"))
                        .and_then(|n| n.as_str())
                    {
                        entry.declared.insert(name.to_string());
                    }
                }
                let Some(workload) = value
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                else {
                    continue;
                };
                let mut pods = Vec::new();
                pod_specs(&value, &mut pods);
                for pod in pods {
                    for list in ["containers", "initContainers"] {
                        let Some(containers) = pod.get(list).and_then(|c| c.as_sequence()) else {
                            continue;
                        };
                        for container in containers {
                            let Some(container) = container.as_mapping() else {
                                continue;
                            };
                            entry
                                .mounted
                                .extend(mounted_claims(pod, container).into_values());
                            let walker = format!(
                                "{}/{}",
                                workload,
                                container
                                    .get("name")
                                    .and_then(|n| n.as_str())
                                    .unwrap_or("<unnamed>")
                            );
                            for line in env_value(container, MOUNTS_ENV).unwrap_or_default().lines()
                            {
                                if let Some((claim, _)) = line.split_once('=') {
                                    entry
                                        .walked
                                        .entry(claim.trim().to_string())
                                        .or_default()
                                        .insert(walker.clone());
                                }
                            }
                            let measured = env_value(container, API_CLAIM_ENV)
                                .map(|c| c.trim().to_string())
                                .filter(|c| !c.is_empty());
                            if let Some(claim) = measured {
                                entry.measured.push((
                                    workload.to_string(),
                                    container
                                        .get("name")
                                        .and_then(|n| n.as_str())
                                        .unwrap_or("<unnamed>")
                                        .to_string(),
                                    claim,
                                ));
                            }
                        }
                    }
                }
            }
        }
        facts.insert(dir, entry);
    }
    facts
}

/// An API-measured claim has to be a real volume of that delivery, and one that
/// is walked as well.
///
/// The mount pairing cannot apply here — the declaring container deliberately
/// does not mount the claim — so this judgement stands in for it against the
/// same manifest: the name has to match a claim object the delivery declares and
/// that some container in it mounts. On top of that the claim has to be walked
/// by exactly one container of this delivery: the API answers a different
/// quantity (what the store still references, not what occupies the disk), so a
/// claim measured only that way has no reading of the size it is compared
/// against, and two walkers of one claim publish two byte counts under one name.
/// Every row is also required to be exercised somewhere, so a row cannot outlive
/// the layout it describes.
#[test]
fn every_api_measured_claim_is_a_volume_that_delivery_declares_mounts_and_walks() {
    let facts = delivery_facts();
    let known: BTreeSet<&str> = API_MEASURED.iter().map(|(name, ..)| *name).collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for dir in DELIVERED {
        let Some(facts) = facts.get(dir) else {
            panic!("{dir} was not scanned");
        };
        assert!(
            !facts.measured.is_empty(),
            "{dir} declares no {API_CLAIM_ENV} anywhere, so this gate read nothing there: either \
             the declaration was dropped from that profile's pod, or the store it names is no \
             longer delivered and the reading it feeds is unproduced"
        );
        for (workload, container, claim) in &facts.measured {
            assert!(
                known.contains(workload.as_str()),
                "{dir}/{workload} (container {container}): this workload declares an API-measured \
                 claim and no producer row covers it — add it to API_MEASURED with the call that \
                 builds its producer"
            );
            assert!(
                facts.declared.contains(claim),
                "{dir}/{workload} (container {container}): {API_CLAIM_ENV} names {claim}, and this \
                 delivery declares no PersistentVolumeClaim with that name, so the reading is \
                 published against a claim nothing backs"
            );
            assert!(
                facts.mounted.contains(claim),
                "{dir}/{workload} (container {container}): {API_CLAIM_ENV} names {claim}, and no \
                 container in this delivery mounts it: the declaration outlived the volume it \
                 names"
            );
            let walkers = facts.walked.get(claim).cloned().unwrap_or_default();
            assert_eq!(
                walkers.len(),
                1,
                "{dir}/{workload} (container {container}): {claim} is measured through its store's \
                 API and walked by {walkers:?} — the API reports the bytes the store still \
                 references, not the ones on the disk, so a claim asking its writer is not a \
                 reading of its own size; exactly one container has to walk the mount (and two \
                 would publish two byte counts under one claim name)"
            );
            seen.insert(workload.clone());
        }
    }

    for row in &known {
        assert!(
            seen.contains(*row),
            "API_MEASURED lists {row}, which no delivered manifest declares any more: the row \
             describes a layout that is gone and would otherwise licence the next gap"
        );
    }
}

/// The API-measured row's evidence must still be in the file it names.
///
/// Both ends are pinned to one spelling: the manifests this gate reads carry the
/// name in [`API_CLAIM_ENV`], and the producer reads the same name from its own
/// constant, so a rename on either side has to break something here rather than
/// silently stop the reading.
#[test]
fn every_api_measured_row_still_names_the_call_that_builds_the_producer() {
    for (workload, file, marker) in API_MEASURED {
        let source = read(file);
        assert!(
            source.contains(marker),
            "{workload}: {file} no longer contains {marker:?}, so the producer this row stands for \
             was renamed, moved, or dropped"
        );
        assert!(
            source.contains(API_CLAIM_ENV),
            "{workload}: {file} no longer reads {API_CLAIM_ENV}, so the delivered declaration \
             reaches nothing"
        );
        // The other half of "one name means one thing": this producer answers a
        // narrower question than the volume family asks, and the rule that divides
        // a claim's bytes by its declared size reads only the family's name. A
        // series name is a claim about what the number is, so the narrower number
        // published under it makes that rule read a volume that cannot fill up.
        let family = cog_core::claim_footprint::USED_METRIC;
        assert!(
            !source.contains(family),
            "{workload}: {file} publishes {family}, which is the volume family's series — this \
             producer reports what the store still references, not what occupies the disk, and the \
             declared-size rule divides the family's name"
        );
    }
}
