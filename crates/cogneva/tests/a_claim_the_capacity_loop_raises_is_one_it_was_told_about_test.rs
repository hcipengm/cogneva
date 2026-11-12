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

const OCCUPANCY_URL_ENV: &str = "COGNEVA_CLAIM_STORE_METRICS_URL";

/// Every manifest in a rendered profile, keyed by nothing — the checks below ask
/// for kinds, so the documents are all kept and filtered at the use.
fn rendered_documents() -> Vec<serde_yaml::Value> {
    let dir = repo_root().join("deploy/rendered/k3s-single");
    let entries = std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for doc in serde_yaml::Deserializer::from_str(&text) {
            let value = serde_yaml::Value::deserialize(doc)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            if !value.is_null() {
                out.push(value);
            }
        }
    }
    out
}

fn kind(value: &serde_yaml::Value) -> &str {
    value
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or_default()
}

fn pod_template_labels(workload: &serde_yaml::Value) -> std::collections::BTreeMap<String, String> {
    workload
        .get("spec")
        .and_then(|s| s.get("template"))
        .and_then(|t| t.get("metadata"))
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_mapping())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn selector_labels(
    selector: Option<&serde_yaml::Value>,
) -> Option<std::collections::BTreeMap<String, String>> {
    let m = selector?.get("matchLabels")?.as_mapping()?;
    Some(
        m.iter()
            .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
            .collect(),
    )
}

/// A selector matches when every label it names is held with that value; an
/// empty selector matches everything it is asked about.
fn selects(
    selector: &std::collections::BTreeMap<String, String>,
    labels: &std::collections::BTreeMap<String, String>,
) -> bool {
    selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

/// A plain label map, which is the shape a Service selects pods with.
fn plain_labels(
    value: Option<&serde_yaml::Value>,
) -> Option<std::collections::BTreeMap<String, String>> {
    let m = value?.as_mapping()?;
    Some(
        m.iter()
            .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
            .collect(),
    )
}

/// Whether this container publishes a reading rather than raising declarations.
///
/// The volume walker declares the claim it walks so its figure lands on the
/// right volume; it holds no write access to a declaration and runs none of
/// these decisions. The parties below are the processes that build.
fn is_volume_walker(container: &serde_yaml::Value) -> bool {
    let mut words: Vec<String> = Vec::new();
    for key in ["command", "args"] {
        if let Some(seq) = container.get(key).and_then(|c| c.as_sequence()) {
            words.extend(seq.iter().filter_map(|w| w.as_str()).map(str::to_string));
        }
    }
    words.iter().any(|w| w == "volume-walker")
}

/// `claim=path` pairs a container declares for the purpose of raising them.
fn raising_mounts(container: &serde_yaml::Value) -> BTreeSet<(String, String)> {
    if is_volume_walker(container) {
        return BTreeSet::new();
    }
    declared_mounts(container)
}

/// `http://host:port/path` into the two parts an allowlist is written with.
fn split_url(url: &str) -> Option<(String, u16)> {
    let rest = url.strip_prefix("http://")?;
    let authority = rest.split('/').next()?;
    let (host, port) = authority.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

/// The container env by name, first value wins.
fn env_value(containers: &[serde_yaml::Value], name: &str) -> Option<String> {
    containers.iter().find_map(|c| {
        c.get("env")?.as_sequence()?.iter().find_map(|e| {
            (e.get("name")?.as_str()? == name)
                .then(|| e.get("value").and_then(|v| v.as_str()).map(str::to_string))
                .flatten()
        })
    })
}

/// The rendered workloads the check runs over. One profile is enough: the
/// workload shapes do not vary by profile, only their numbers do, and the
/// freshness gate keeps this directory equal to the chart.
fn rendered_workloads() -> Vec<serde_yaml::Value> {
    rendered_documents()
        .into_iter()
        .filter(|d| matches!(kind(d), "Deployment" | "DaemonSet" | "StatefulSet"))
        .collect()
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
        "{} 里找不到 MOUNTS_ENV：消费者改名了，这条判据读的不是它了",
        Path::new("crates/cog-core/src/claim_footprint.rs").display()
    );
    assert_eq!(
        cog_reflection::volume_capacity::OCCUPANCY_URL_ENV,
        OCCUPANCY_URL_ENV,
        "容量循环读的变量名改了，这条判据读的不是它了"
    );
}

/// The guard is only worth having if the raise actually goes through it.
///
/// A class that will not carry an expansion out is named by a claim it already
/// stranded, and a request written on such a class can never be lowered again:
/// the claim then announces more than it will ever be given for the rest of its
/// life. The decision itself is unit-tested where it lives; this is the wiring
/// -- the round has to derive the blocked set from the namespace before it
/// reconciles anything, and the write has to sit behind the verdict.
#[test]
fn a_size_nothing_can_carry_out_is_not_written_and_the_write_sits_behind_that_verdict() {
    let path = repo_root().join("crates/cog-reflection/src/volume_capacity.rs");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let round = text
        .find("let blocked_classes = match kubectl(")
        .unwrap_or_else(|| {
            panic!(
                "容量循环不再从命名空间的卷列表推出「哪些类抬不动」：\
                 这条判据读的那一步没了"
            )
        });
    let first_reconcile = text
        .find("reconcile(target, state, &published, &blocked_classes)")
        .unwrap_or_else(|| panic!("容量循环不再把 blocked_classes 交给单卷决策"));
    assert!(
        round < first_reconcile,
        "blocked_classes 必须在任何一卷被决策之前算出来：晚一拍就是先写后判"
    );

    // Inside the single-claim decision, the patch has to come after the guard.
    let reconcile = text
        .find("async fn reconcile(")
        .expect("找不到单卷决策函数 reconcile");
    let body = &text[reconcile..];
    let guard = body
        .find("if !may_raise(")
        .expect("单卷决策里没有 may_raise 这一判：白名单算出来了却没人用");
    // The verb and the resource it is sent to, found by what they say rather
    // than by how they are laid out: wrapping the call in the guard above
    // changes the indentation, and a judge that reads the indent would go red
    // on a decision that still sits in the right place.
    let patch = body
        .match_indices("\"patch\",")
        .map(|(at, _)| at)
        .find(|at| {
            body[*at..]
                .chars()
                .take(120)
                .collect::<String>()
                .contains("\"pvc\",")
        })
        .expect("单卷决策里找不到改卷声明的 patch");
    assert!(
        guard < patch,
        "must not raise 的判定在写之后：这条 guard 挡不住任何一次抬高"
    );
}

/// The workloads that may change a claim's declaration.
///
/// A pod can raise a declaration only if its ServiceAccount is bound to a Role
/// that grants a write verb on `persistentvolumeclaims`, and the binding in this
/// namespace is a RoleBinding. That is the whole of the permission, so it is also
/// the whole of the set this check has to cover: a workload that gains the write
/// permission by any other route is a widening nobody here would see anyway.
fn workloads_that_may_raise(documents: &[serde_yaml::Value]) -> BTreeSet<String> {
    let sequence_of = |value: Option<&serde_yaml::Value>| -> Vec<String> {
        value
            .and_then(|v| v.as_sequence())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|i| i.as_str())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };

    let mut writing_roles: BTreeSet<String> = BTreeSet::new();
    for role in documents.iter().filter(|d| kind(d) == "Role") {
        let Some(name) = role
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
        else {
            continue;
        };
        let writes = role
            .get("rules")
            .and_then(|r| r.as_sequence())
            .map(|rules| {
                rules.iter().any(|rule| {
                    let core_group = {
                        let groups = sequence_of(rule.get("apiGroups"));
                        groups.iter().any(|g| g.is_empty() || g == "*")
                    };
                    let claims = {
                        let resources = sequence_of(rule.get("resources"));
                        resources
                            .iter()
                            .any(|r| r == "persistentvolumeclaims" || r == "*")
                    };
                    let writes = {
                        let verbs = sequence_of(rule.get("verbs"));
                        verbs
                            .iter()
                            .any(|v| matches!(v.as_str(), "patch" | "update" | "*"))
                    };
                    core_group && claims && writes
                })
            })
            .unwrap_or(false);
        if writes {
            writing_roles.insert(name.to_string());
        }
    }

    let mut writers: BTreeSet<String> = BTreeSet::new();
    for binding in documents.iter().filter(|d| kind(d) == "RoleBinding") {
        let role = binding
            .get("roleRef")
            .and_then(|r| r.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or_default();
        if !writing_roles.contains(role) {
            continue;
        }
        let Some(subjects) = binding.get("subjects").and_then(|s| s.as_sequence()) else {
            continue;
        };
        for subject in subjects {
            if subject.get("kind").and_then(|k| k.as_str()) != Some("ServiceAccount") {
                continue;
            }
            if let Some(name) = subject.get("name").and_then(|n| n.as_str()) {
                writers.insert(name.to_string());
            }
        }
    }

    documents
        .iter()
        .filter(|d| matches!(kind(d), "Deployment" | "DaemonSet" | "StatefulSet"))
        .filter(|d| {
            let sa = d
                .get("spec")
                .and_then(|s| s.get("template"))
                .and_then(|t| t.get("spec"))
                .and_then(|s| s.get("serviceAccountName"))
                .and_then(|n| n.as_str())
                .unwrap_or("default");
            writers.contains(sa)
        })
        .filter_map(|d| {
            d.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(str::to_string)
        })
        .collect()
}

fn containers_of(workload: &serde_yaml::Value) -> Vec<serde_yaml::Value> {
    workload
        .get("spec")
        .and_then(|s| s.get("template"))
        .and_then(|t| t.get("spec"))
        .and_then(|s| s.get("containers"))
        .and_then(|c| c.as_sequence())
        .map(|cs| cs.to_vec())
        .unwrap_or_default()
}

/// The pod that raises declarations is given an address for the claims it
/// cannot measure.
///
/// It mounts a handful of claims; the namespace holds more than a dozen, and
/// every other one belongs to a workload whose image this platform does not
/// write. The figure for those exists — the process that walks the node's
/// storage root publishes it — but a figure nothing reads is not a channel.
/// Without this variable the loop decides nothing about them every round, and
/// `unmeasured` reads the same from outside as a volume that stopped growing.
///
/// The address is checked against the objects it has to resolve to, in the same
/// render: a Service of that name, carrying that port, whose selector is met by
/// a workload that exists. A URL nobody answers is worse than none — it reads
/// as a wired channel.
///
/// The set of pods is derived from the permission to raise, not from a list of
/// names: any workload that gains the write permission has to be given the
/// channel in the same change, or this reds.
#[test]
fn the_pod_that_raises_claims_is_given_the_endpoint_the_others_publish_on() {
    let documents = rendered_documents();
    let raisers = workloads_that_may_raise(&documents);
    assert!(
        !raisers.is_empty(),
        "渲染集里没有任何工作负载持有改卷声明的权限：这条判据的分母塌了"
    );

    for workload in documents
        .iter()
        .filter(|d| matches!(kind(d), "Deployment" | "DaemonSet" | "StatefulSet"))
    {
        let name = workload
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>");
        if !raisers.contains(name) {
            continue;
        }
        let containers = containers_of(workload);
        let declared: BTreeSet<(String, String)> =
            containers.iter().flat_map(raising_mounts).collect();
        assert!(
            !declared.is_empty(),
            "{name} 持有改卷声明的权限，却没声明 {MOUNTS_ENV}：它一张卷都不量"
        );

        let url = env_value(&containers, OCCUPANCY_URL_ENV).unwrap_or_else(|| {
            panic!(
                "{name} 跑容量循环（声明了 {MOUNTS_ENV}），却没拿到 {OCCUPANCY_URL_ENV}：\
                 别的 Pod 写的那些卷它每一拍都读成 unmeasured，声明永远不抬"
            )
        });
        assert!(
            !url.trim().is_empty(),
            "{name} 的 {OCCUPANCY_URL_ENV} 是空串：读起来就是这条通道关着"
        );
        let (host, port) = split_url(&url)
            .unwrap_or_else(|| panic!("{name} 的地址 {url} 不是 http://host:port/…"));
        let service_name = host.split('.').next().unwrap_or_default().to_string();

        let service = documents
            .iter()
            .find(|d| {
                kind(d) == "Service"
                    && d.get("metadata")
                        .and_then(|m| m.get("name"))
                        .and_then(|n| n.as_str())
                        == Some(service_name.as_str())
            })
            .unwrap_or_else(|| panic!("{name} 的地址指向 Service {service_name}，渲染集里没有它"));
        let ports: Vec<u16> = service
            .get("spec")
            .and_then(|s| s.get("ports"))
            .and_then(|p| p.as_sequence())
            .map(|ps| {
                ps.iter()
                    .filter_map(|p| p.get("port").and_then(|n| n.as_u64()))
                    .filter_map(|n| u16::try_from(n).ok())
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            ports.contains(&port),
            "{name} 的地址用的是 {port}，而 Service {service_name} 只提供 {ports:?}"
        );

        // The Service's selector has to reach the pods that publish the figure.
        let service_selector = plain_labels(service.get("spec").and_then(|s| s.get("selector")))
            .unwrap_or_else(|| panic!("Service {service_name} 没有选择器，没有人会接住它"));
        let publishers: Vec<&serde_yaml::Value> = documents
            .iter()
            .filter(|d| matches!(kind(d), "Deployment" | "DaemonSet" | "StatefulSet"))
            .filter(|d| selects(&service_selector, &pod_template_labels(d)))
            .collect();
        assert!(
            !publishers.is_empty(),
            "Service {service_name} 的选择器没有任何工作负载接住：那个地址上没有人发布读数"
        );
    }
}

/// The egress allowlist the raising pod is under admits the address it was
/// given.
///
/// These pods have a default-deny egress policy; an address that is not on the
/// allowlist is dropped, and a dropped scrape is a warn in the log beside a
/// reading that says volumes are unmeasured. The env derives its port from
/// values while the policy names one by hand, which is the shape that breaks
/// silently, so both are read out of the same render and compared.
#[test]
fn the_egress_that_governs_the_raising_pod_admits_the_endpoint_it_was_given() {
    let documents = rendered_documents();
    let raisers = workloads_that_may_raise(&documents);
    assert!(
        !raisers.is_empty(),
        "渲染集里没有任何工作负载持有改卷声明的权限：这条判据的分母塌了"
    );

    for workload in documents
        .iter()
        .filter(|d| matches!(kind(d), "Deployment" | "DaemonSet" | "StatefulSet"))
    {
        let name = workload
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>");
        if !raisers.contains(name) {
            continue;
        }
        let containers = containers_of(workload);
        let labels = pod_template_labels(workload);
        let url = env_value(&containers, OCCUPANCY_URL_ENV)
            .unwrap_or_else(|| panic!("{name} 没拿到 {OCCUPANCY_URL_ENV}"));
        let (host, port) =
            split_url(&url).unwrap_or_else(|| panic!("{name} 的地址 {url} 不可解析"));
        let service_name = host.split('.').next().unwrap_or_default().to_string();

        // The pods the address resolves to, found the way the Service finds
        // them: by the selector it publishes, not by guessing the name.
        let service_selector = documents
            .iter()
            .find(|d| {
                kind(d) == "Service"
                    && d.get("metadata")
                        .and_then(|m| m.get("name"))
                        .and_then(|n| n.as_str())
                        == Some(service_name.as_str())
            })
            .and_then(|s| plain_labels(s.get("spec").and_then(|spec| spec.get("selector"))))
            .unwrap_or_else(|| panic!("渲染集里没有可用的 Service {service_name}"));
        let publisher_labels = documents
            .iter()
            .filter(|d| matches!(kind(d), "Deployment" | "DaemonSet" | "StatefulSet"))
            .map(pod_template_labels)
            .find(|labels| selects(&service_selector, labels))
            .unwrap_or_else(|| panic!("Service {service_name} 的选择器没有任何工作负载接住"));

        let admitted = documents
            .iter()
            .filter(|d| kind(d) == "NetworkPolicy")
            .any(|policy| {
                let governs = policy
                    .get("spec")
                    .and_then(|s| s.get("policyTypes"))
                    .and_then(|p| p.as_sequence())
                    .map(|types| types.iter().any(|t| t.as_str() == Some("Egress")))
                    .unwrap_or(false)
                    && selects(
                        &selector_labels(policy.get("spec").and_then(|s| s.get("podSelector")))
                            .unwrap_or_default(),
                        &labels,
                    );
                if !governs {
                    return false;
                }
                let Some(rules) = policy
                    .get("spec")
                    .and_then(|s| s.get("egress"))
                    .and_then(|e| e.as_sequence())
                else {
                    return false;
                };
                rules.iter().any(|rule| {
                    // A named peer, not the namespace-wide `podSelector: {}`
                    // rule beside it. That one opens this port for every pod in
                    // the namespace on the ground of a different consumer, so a
                    // channel admitted only by it is admitted by coincidence:
                    // change either side's number and the scrape is dropped
                    // with nothing in this render saying so.
                    let reaches_publisher = rule
                        .get("to")
                        .and_then(|t| t.as_sequence())
                        .map(|to| {
                            to.iter().any(|peer| {
                                selector_labels(peer.get("podSelector"))
                                    .filter(|sel| !sel.is_empty())
                                    .is_some_and(|sel| selects(&sel, &publisher_labels))
                            })
                        })
                        .unwrap_or(false);
                    let opens_port = rule
                        .get("ports")
                        .and_then(|p| p.as_sequence())
                        .map(|ports| {
                            ports.iter().any(|p| {
                                p.get("port").and_then(|n| n.as_u64()) == Some(port as u64)
                            })
                        })
                        .unwrap_or(false);
                    reaches_publisher && opens_port
                })
            });
        assert!(
            admitted,
            "{name} 的出口是白名单，而没有任何一条策略放行到 {service_name} 的 {port}：\
             那一抓被丢弃，那些卷每一拍都读成 unmeasured"
        );
    }
}
