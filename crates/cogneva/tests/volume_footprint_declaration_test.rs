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
const PRODUCERS: [(&str, &str, &str); 4] = [
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
];

const MOUNTS_ENV: &str = "COGNEVA_DATA_VOLUME_MOUNTS";
const CLAIM_ENV: &str = "COGNEVA_DATA_VOLUME_CLAIM";

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
