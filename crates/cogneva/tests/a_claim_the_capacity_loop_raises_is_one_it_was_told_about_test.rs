//! principle: feedback_no_hardcoding_config_surface
//!
//! The loop that raises a claim's declaration acts only on the claims its own
//! process was told about, and it is told through one variable,
//! `COGNEVA_DATA_VOLUME_MOUNTS`, written as `claim=path` lines. A claim mounted
//! by the pod but missing from that list is not merely unmeasured: it has no
//! repairer at all. Its usage still appears on the readings face — a separate
//! walker publishes it — so the deployment looks accounted for while the one
//! action that keeps a volume from filling up is never taken for it, and the
//! declaration behind it is raised by hand, in `values.yaml`, or not at all.
//!
//! That is how `cogneva-evolution-data-pvc` sat at the declaration the chart
//! ships with nothing able to
//! raise it: the pod mounts it at `/var/lib/cogneva-data`, and the only
//! declaration naming it was `COGNEVA_DATA_VOLUME_CLAIM`, which the loop reads
//! for a different question (whether the data directory's claim is walkable)
//! and can only warn about.
//!
//! This test draws the line on the other side of the same seam: every claim a
//! pod mounts has to be one that pod's mounts variable names. It reads the
//! rendered manifests rather than the chart, because what the process is handed
//! is the rendered value; the freshness gate is what keeps the chart and these
//! files the same document.
//!
//! Pods whose containers declare no mounts variable are out of scope and are
//! counted, not passed: a third-party workload (postgres, redis, qdrant and the
//! rest) mounts a claim and cannot declare anything, so the reading that covers
//! those is the fill-level rule, not this loop.

use crate::common;

use common::routes::repo_root;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::Path;

const MOUNTS_ENV: &str = "COGNEVA_DATA_VOLUME_MOUNTS";

/// The rendered workloads the check runs over. One profile is enough: the
/// workload shapes do not vary by profile, only their numbers do, and the
/// freshness gate keeps this directory equal to the chart.
fn rendered_workloads() -> Vec<serde_yaml::Value> {
    let dir = repo_root().join("deploy/rendered/k3s-single");
    let entries = std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    let mut out = Vec::new();
    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .collect();
    paths.sort();
    for path in paths {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for doc in serde_yaml::Deserializer::from_str(&text) {
            let value = serde_yaml::Value::deserialize(doc)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let kind = value
                .get("kind")
                .and_then(|k| k.as_str())
                .unwrap_or_default();
            if matches!(kind, "Deployment" | "DaemonSet" | "StatefulSet") {
                out.push(value);
            }
        }
    }
    out
}

/// `claim=path` pairs a container declares.
fn declared_mounts(container: &serde_yaml::Value) -> BTreeSet<(String, String)> {
    let Some(env) = container.get("env").and_then(|e| e.as_sequence()) else {
        return BTreeSet::new();
    };
    let mut out = BTreeSet::new();
    for entry in env {
        if entry.get("name").and_then(|n| n.as_str()) != Some(MOUNTS_ENV) {
            continue;
        }
        let Some(value) = entry.get("value").and_then(|v| v.as_str()) else {
            continue;
        };
        for line in value.lines() {
            let line = line.trim();
            if let Some((claim, path)) = line.split_once('=') {
                out.insert((claim.trim().to_string(), path.trim().to_string()));
            }
        }
    }
    out
}

/// Volume name → the claim it mounts, for the volumes the pod declares.
fn claim_of_volume(pod_spec: &serde_yaml::Value) -> Vec<(String, String)> {
    let Some(volumes) = pod_spec.get("volumes").and_then(|v| v.as_sequence()) else {
        return Vec::new();
    };
    volumes
        .iter()
        .filter_map(|v| {
            let name = v.get("name")?.as_str()?.to_string();
            let claim = v
                .get("persistentVolumeClaim")?
                .get("claimName")?
                .as_str()?
                .to_string();
            Some((name, claim))
        })
        .collect()
}

fn container_name(container: &serde_yaml::Value) -> String {
    container
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("<unnamed>")
        .to_string()
}

#[test]
fn every_claim_a_pod_mounts_is_one_its_mounts_variable_names() {
    let workloads = rendered_workloads();
    assert!(
        !workloads.is_empty(),
        "deploy/rendered/k3s-single 里一个工作负载都没读到：这条判据的分母是它"
    );

    let mut complaints: Vec<String> = Vec::new();
    let mut pods_judged = 0usize;
    let mut claims_judged = 0usize;
    let mut pods_out_of_scope = 0usize;

    for workload in &workloads {
        let name = workload
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>");
        let Some(pod_spec) = workload
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
        else {
            continue;
        };
        let Some(containers) = pod_spec.get("containers").and_then(|c| c.as_sequence()) else {
            continue;
        };

        let declared: BTreeSet<(String, String)> =
            containers.iter().flat_map(declared_mounts).collect();
        if declared.is_empty() {
            pods_out_of_scope += 1;
            continue;
        }
        pods_judged += 1;

        let volumes = claim_of_volume(pod_spec);
        for container in containers {
            let Some(mounts) = container.get("volumeMounts").and_then(|m| m.as_sequence()) else {
                continue;
            };
            for mount in mounts {
                let Some(volume) = mount.get("name").and_then(|n| n.as_str()) else {
                    continue;
                };
                let Some((_, claim)) = volumes.iter().find(|(v, _)| v == volume) else {
                    continue;
                };
                let Some(path) = mount.get("mountPath").and_then(|p| p.as_str()) else {
                    continue;
                };
                claims_judged += 1;
                if !declared.contains(&(claim.clone(), path.to_string())) {
                    complaints.push(format!(
                        "{name}/{} 挂着卷 {claim} 在 {path}，而 {MOUNTS_ENV} 里没有这一条：\
                         这条卷只被量、没有抬声明的人，涨到头来要靠人改 values",
                        container_name(container),
                    ));
                }
            }
        }
    }

    // 分母：判过的 Pod 数与卷挂载点。塌了就说明这条判据在空跑。
    assert!(
        pods_judged >= 3 && claims_judged >= 4,
        "判过 {pods_judged} 个 Pod / {claims_judged} 个卷挂载点（超出名单 {pods_out_of_scope} 个），\
         分母塌了，这条判据已形同虚设"
    );
    assert!(
        complaints.is_empty(),
        "有 Pod 挂着一张没人替它抬声明的卷:\n{}",
        complaints.join("\n")
    );
}

/// The specific gap this was written for: the evolution pod's data directory
/// holds a claim of its own, large enough to matter and previously named to the
/// loop by nothing that could act on it.
#[test]
fn the_evolution_pods_own_data_claim_is_one_the_loop_can_raise() {
    let workloads = rendered_workloads();
    let evolution = workloads
        .iter()
        .find(|w| {
            w.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                == Some("cogneva-evolution")
        })
        .expect("deploy/rendered/k3s-single 里没有 cogneva-evolution 工作负载");

    let pod_spec = evolution
        .get("spec")
        .and_then(|s| s.get("template"))
        .and_then(|t| t.get("spec"))
        .expect("cogneva-evolution 没有 Pod 模板");
    let volumes = claim_of_volume(pod_spec);
    let data_claim = volumes
        .iter()
        .find(|(name, _)| name == "app-data")
        .map(|(_, claim)| claim.clone())
        .expect("cogneva-evolution 不再挂 app-data 卷");

    let declared: BTreeSet<(String, String)> = pod_spec
        .get("containers")
        .and_then(|c| c.as_sequence())
        .map(|cs| cs.iter().flat_map(declared_mounts).collect())
        .unwrap_or_default();

    assert!(
        declared
            .iter()
            .any(|(claim, path)| claim == &data_claim && path == "/var/lib/cogneva-data"),
        "进化 Pod 的数据卷 {data_claim} 挂在 /var/lib/cogneva-data 上，{MOUNTS_ENV} 却没点名它：\
         这张卷只有占用读数、没有抬声明的人"
    );
}

/// Keeps the constant in this file honest against the code that reads it, so a
/// rename on the consumer side reds here rather than silently exempting every
/// pod from the check.
#[test]
fn the_variable_name_checked_here_is_the_one_the_loop_reads() {
    let path = repo_root().join("crates/cog-core/src/claim_footprint.rs");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let exposed = cog_core::claim_footprint::MOUNTS_ENV;
    assert_eq!(exposed, MOUNTS_ENV);
    assert!(
        text.contains("MOUNTS_ENV"),
        "{} 里找不到 MOUNTS_ENV：消费者改名了，这条判据读的就不是它了",
        Path::new("crates/cog-core/src/claim_footprint.rs").display()
    );
}
