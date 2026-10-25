//! The resource ceiling is a function of the workload set, not an environment
//! parameter. Every container already declares its limits, every Deployment
//! already declares its replicas and its surge strategy, every PVC already
//! declares its size -- the namespace total is the sum of those, and a number
//! written by hand next to declarations it is supposed to bound is a second
//! source that drifts the first time a workload grows. It drifts silently: the
//! reading on the cluster is "this revision will not roll", because the refused
//! pod is the new one while the old one keeps serving.
//!
//! So the ceiling lives in exactly one place: the projection in
//! `deploy/scripts/project-resource-governance.py`, written out per profile as
//! `values.resource-governance.<profile>.yaml` and rendered by
//! `deploy/scripts/render-deploy.sh`. These tests hold the artifacts to that:
//!
//! - every profile has a generated ceiling file, and it says so in its first
//!   line -- an artifact nobody can tell from a hand-written one is the thing
//!   this whole arrangement exists to prevent.
//! - `values.yaml` carries no ceiling numbers. A hand-set `quota:` block would
//!   win over the generated file and be the second source again.
//! - the static manifest the GitOps loop applies carries the same numbers as the
//!   single-node projection, so the applied copy and the rendered copy are the
//!   same derivation rather than two that happen to agree today.

use std::path::{Path, PathBuf};

use serde::Deserialize;

const PROJECTOR: &str = "deploy/scripts/project-resource-governance.py";
const CHART: &str = "deploy/helm/cogneva";
const VALUES: &str = "deploy/helm/cogneva/values.yaml";
const PROFILES: &str = "deploy/helm/cogneva/profiles";
const STATIC_QUOTA: &str = "deploy/k3s/resource-quota.yaml";

/// The keys the projection writes into a per-profile file. The ceiling is only
/// complete when all of them are there: a missing one renders as an empty
/// constraint, which is a quota that does not bound anything.
const QUOTA_KEYS: [&str; 6] = [
    "requestsCpu",
    "requestsMemory",
    "limitsCpu",
    "limitsMemory",
    "pods",
    "requestsStorage",
];

const LIMIT_RANGE_KEYS: [&str; 3] = ["maxCpu", "maxMemory", "pvcMaxStorage"];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The generated file for a profile, by the same rule `render-deploy.sh` and
/// `bootstrap` use: the profile's own file stem.
fn generated_path(profile: &Path) -> PathBuf {
    let stem = profile
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    repo_root()
        .join(CHART)
        .join(format!("values.resource-governance.{stem}.yaml"))
}

fn profiles() -> Vec<PathBuf> {
    let dir = repo_root().join(PROFILES);
    let mut out: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
        .collect();
    out.sort();
    assert!(!out.is_empty(), "{}: no profiles", dir.display());
    out
}

fn parse_documents(text: &str, what: &str) -> Vec<serde_yaml::Value> {
    let mut out = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(text) {
        let value = serde_yaml::Value::deserialize(doc)
            .unwrap_or_else(|e| panic!("{what}: does not parse: {e}"));
        if !value.is_null() {
            out.push(value);
        }
    }
    out
}

fn get<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    value
        .as_mapping()?
        .get(serde_yaml::Value::String(key.to_string()))
}

fn scalar(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::String(s) => s.clone(),
        other => serde_yaml::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_string(),
    }
}

/// The projection is an artifact, and an artifact nobody can tell from a
/// hand-written file will be edited by hand on the first Friday it is in the
/// way. Each profile gets its own file -- profiles declare different volumes,
/// so one shared file cannot be a projection of all three.
#[test]
fn every_profile_carries_a_generated_ceiling() {
    for profile in profiles() {
        let path = generated_path(&profile);
        let name = profile
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let text = read(&path);
        let first = text.lines().next().unwrap_or_default();
        assert!(
            first.contains("GENERATED") && first.contains(PROJECTOR),
            "{}: first line does not name the projector, so nothing distinguishes this \
             file from a hand-written one: {first}",
            path.display()
        );

        let docs = parse_documents(&text, &path.display().to_string());
        assert_eq!(docs.len(), 1, "{}: one document", path.display());
        let governance = get(&docs[0], "resourceGovernance")
            .unwrap_or_else(|| panic!("{}: no resourceGovernance", path.display()));

        let quota = get(governance, "quota").unwrap_or_else(|| panic!("{name}: no quota block"));
        for key in QUOTA_KEYS {
            let value = get(quota, key).unwrap_or_else(|| panic!("{name}: quota.{key} is missing"));
            assert!(
                !scalar(value).is_empty(),
                "{name}: quota.{key} is empty, which renders as a constraint that \
                 bounds nothing"
            );
        }

        let limits =
            get(governance, "limitRange").unwrap_or_else(|| panic!("{name}: no limitRange block"));
        for key in LIMIT_RANGE_KEYS {
            let value =
                get(limits, key).unwrap_or_else(|| panic!("{name}: limitRange.{key} is missing"));
            assert!(
                !scalar(value).is_empty(),
                "{name}: limitRange.{key} is empty"
            );
        }
    }
}

/// The failure this arrangement exists to prevent: a ceiling set by hand next to
/// the declarations it is supposed to bound. A `quota:` key under
/// `resourceGovernance` in `values.yaml` would be merged over the generated file
/// by Helm, and the generated file would stop being the answer to "what bounds
/// this namespace". The policy values (`default*`, `min*`) stay -- they say what
/// an undeclared container is charged, which no rendered declaration can say.
#[test]
fn the_ceiling_is_not_written_twice() {
    let values: serde_yaml::Value =
        serde_yaml::from_str(&read(&repo_root().join(VALUES))).expect("values.yaml parses");
    let governance = get(&values, "resourceGovernance")
        .expect("values.yaml: resourceGovernance is how the projection is switched on");

    assert!(
        get(governance, "quota").is_none(),
        "values.yaml: resourceGovernance.quota is a second home for a number the \
         projection derives; the generated file already carries it and Helm would \
         let this one win"
    );

    if let Some(limits) = get(governance, "limitRange") {
        for key in LIMIT_RANGE_KEYS {
            assert!(
                get(limits, key).is_none(),
                "values.yaml: resourceGovernance.limitRange.{key} is a ceiling -- it has \
                 to be at least the largest declaration in the set, which only the \
                 projection knows"
            );
        }
    }
}

/// The static manifest is the copy the in-cluster GitOps loop applies and the
/// parity baseline the chart is held to. It has to be the same derivation as the
/// single-node render, not a parallel one that agrees the day it was typed.
#[test]
fn the_static_quota_is_the_single_node_projection() {
    let static_text = read(&repo_root().join(STATIC_QUOTA));
    let first = static_text.lines().next().unwrap_or_default();
    assert!(
        first.contains("GENERATED") && first.contains(PROJECTOR),
        "{STATIC_QUOTA}: first line does not name the projector, so the applied copy \
         cannot be told from a hand-written one: {first}"
    );

    let projected_text = read(&generated_path(
        &repo_root().join(PROFILES).join("k3s-single.yaml"),
    ));
    let projected = parse_documents(&projected_text, "k3s-single projection")
        .into_iter()
        .next()
        .expect("k3s-single projection has one document");
    let projected = get(&projected, "resourceGovernance").expect("resourceGovernance");
    let projected_quota = get(projected, "quota").expect("projected quota");
    let projected_limits = get(projected, "limitRange").expect("projected limitRange");

    let docs = parse_documents(&static_text, STATIC_QUOTA);
    let quota = docs
        .iter()
        .find(|d| get(d, "kind").map(scalar).as_deref() == Some("ResourceQuota"))
        .unwrap_or_else(|| panic!("{STATIC_QUOTA}: no ResourceQuota"));
    let hard = get(quota, "spec")
        .and_then(|spec| get(spec, "hard"))
        .unwrap_or_else(|| panic!("{STATIC_QUOTA}: quota has no spec.hard"));

    // k8s spells the same quantities differently in the two files (the
    // projection carries chart-value names, the manifest carries the API's).
    let pairs: [(&str, &str); 6] = [
        ("requestsCpu", "requests.cpu"),
        ("requestsMemory", "requests.memory"),
        ("limitsCpu", "limits.cpu"),
        ("limitsMemory", "limits.memory"),
        ("pods", "pods"),
        ("requestsStorage", "requests.storage"),
    ];
    for (projected_key, api_key) in pairs {
        let want = scalar(get(projected_quota, projected_key).expect(projected_key));
        let got = scalar(
            get(hard, api_key)
                .unwrap_or_else(|| panic!("{STATIC_QUOTA}: hard.{api_key} is missing")),
        );
        assert_eq!(
            got, want,
            "{STATIC_QUOTA}: hard.{api_key} is {got}, the k3s-single projection derives \
             {want}; the applied copy has to come from the same derivation"
        );
    }

    let limits = docs
        .iter()
        .find(|d| get(d, "kind").map(scalar).as_deref() == Some("LimitRange"))
        .unwrap_or_else(|| panic!("{STATIC_QUOTA}: no LimitRange"));
    let container = get(limits, "spec")
        .and_then(|spec| get(spec, "limits"))
        .and_then(|limits| limits.as_sequence())
        .and_then(|items| {
            items
                .iter()
                .find(|i| get(i, "type").map(scalar).as_deref() == Some("Container"))
        })
        .expect("a Container limit entry");
    let max = get(container, "max").expect("max");
    assert_eq!(
        scalar(get(max, "cpu").expect("max.cpu")),
        scalar(get(projected_limits, "maxCpu").expect("maxCpu")),
        "{STATIC_QUOTA}: the container ceiling has to be the projection's, or a \
         declaration that fits the chart is refused by the applied manifest"
    );
    assert_eq!(
        scalar(get(max, "memory").expect("max.memory")),
        scalar(get(projected_limits, "maxMemory").expect("maxMemory")),
        "{STATIC_QUOTA}: the container ceiling has to be the projection's"
    );
}
