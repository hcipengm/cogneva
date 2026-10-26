//! principle: feedback_no_hardcoding_config_surface
//! The objects have names the chart gives them (`deploy/helm/cogneva/templates/`)
//! and names in-cluster code asks for: a deployer rolling `deployment/cogneva`,
//! the gateway reading the security gateway's replicas, the evolution loop
//! tagging all four base workloads, the installer waiting on its own image
//! server and distributor, a pod mounting `cogneva-json`,
//! a deployer writing the prompts back into `cogneva-prompts`. Every one of
//! those is one string written twice.
//!
//! The failure is quiet. A rename in the chart does not break a build and does
//! not fail a test: `kubectl get deployment <old name>` exits 1 with NotFound,
//! a rollout watches a workload that is not there, and the reading of "which
//! revision is live" comes back empty -- which reads as "not rolled yet", not as
//! "nobody asked the right object". Same shape as the namespace and the image
//! root: the declaration lives in the artifact, the copy lives in the code, and
//! nothing compares them until one of them moves.
//!
//! Here the code's copy is `cog_core::topology`, once. The predicates are:
//! every declared name is a workload the manifests actually create, and no other
//! production source writes it.
//!
//! The main workload's name is also the product name, so it appears all over the
//! tree for reasons that have nothing to do with workloads -- a tracing `service`
//! label, a NATS subject, a JWT issuer, an MCP client name. Reddening on those
//! would get the gate switched off in a day, so for that name the predicate is
//! the *shape* of a workload reference, and only that shape.
//!
//! Test modules are not scanned: a fixture may name whatever it likes.

use std::path::PathBuf;

mod common;

use common::rust_production::production_text;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The one place an object's name may be written.
const DECLARATION: &str = "crates/cog-core/src/topology.rs";

/// The names, each with the kind the manifests give it.
fn declared() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Deployment", cog_core::topology::MAIN_DEPLOYMENT),
        ("Deployment", cog_core::topology::SECURITY_GATEWAY),
        ("Deployment", cog_core::topology::SANDBOX_EXECUTOR),
        ("Deployment", cog_core::topology::EVOLUTION),
        ("Deployment", cog_core::topology::IMAGE_SERVER),
        ("DaemonSet", cog_core::topology::IMAGE_DISTRIBUTOR),
        ("ConfigMap", cog_core::topology::CONFIG_CONFIGMAP),
        ("ConfigMap", cog_core::topology::PROMPTS_CONFIGMAP),
        ("ConfigMap", cog_core::topology::EVOLUTION_CONFIG_CONFIGMAP),
        ("ConfigMap", cog_core::topology::ENV_CONFIGMAP),
        ("Secret", cog_core::topology::SECRET_INTERNAL),
        ("Secret", cog_core::topology::SECRET_OOB),
    ]
}

/// The object names the code is allowed to ask for by name: everything whose
/// name carries the product prefix. A manifest-declared `cogneva-*`
/// ConfigMap/Secret that is *not* here is a name the code would have to write a
/// second copy of to reach.
fn product_objects(created: &[(String, String)]) -> Vec<(String, String)> {
    created
        .iter()
        .filter(|(kind, name)| {
            (kind == "ConfigMap" || kind == "Secret") && name.starts_with("cogneva-")
        })
        .cloned()
        .collect()
}

/// The manifest files that create workloads: the chart's templates plus the
/// manifests the installer applies itself.
fn workload_manifests() -> Vec<PathBuf> {
    let mut out = Vec::new();
    let templates = repo_root().join("deploy/helm/cogneva/templates");
    for entry in std::fs::read_dir(&templates).expect("chart templates/ 不在仓库里") {
        let path = entry.expect("读 templates/ 失败").path();
        if path.extension().and_then(|e| e.to_str()) == Some("yaml") {
            out.push(path);
        }
    }
    let k8s = repo_root().join("deploy/k8s");
    for entry in std::fs::read_dir(&k8s).expect("deploy/k8s 不在仓库里") {
        let path = entry.expect("读 deploy/k8s 失败").path();
        if path.extension().and_then(|e| e.to_str()) == Some("yaml") {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// Every `(kind, name)` declared in a manifest, read off the text.
///
/// Deliberately not a YAML parse: the chart's templates are Go templates and do
/// not parse, and `deploy/rendered/` is a projection of them. Reading
/// `kind:`-then-`  name:` at two-space indent is what both files have in
/// common, and a manifest that stops having it is a manifest this gate should
/// notice rather than silently skip.
fn workloads_in(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let kinds = [
        "Deployment",
        "StatefulSet",
        "DaemonSet",
        "Job",
        "CronJob",
        "Service",
        "ConfigMap",
        "Secret",
    ];
    let lines: Vec<&str> = text.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let Some(kind) = line.strip_prefix("kind: ") else {
            continue;
        };
        let kind = kind.trim();
        if !kinds.contains(&kind) {
            continue;
        }
        let name = lines
            .iter()
            .skip(i + 1)
            .take(12)
            .find_map(|l| l.strip_prefix("  name: ").map(|n| n.trim().to_string()));
        if let Some(name) = name {
            out.push((kind.to_string(), name));
        }
    }
    out
}

#[test]
fn every_declared_name_is_one_the_manifests_create() {
    let mut created: Vec<(String, String)> = Vec::new();
    for path in workload_manifests() {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for pair in workloads_in(&text) {
            created.push(pair);
        }
    }
    let missing: Vec<(&str, &str)> = declared()
        .into_iter()
        .filter(|(kind, name)| !created.iter().any(|(k, n)| k == kind && n == *name))
        .collect();
    assert!(
        missing.is_empty(),
        "cog_core::topology 声明了清单里不存在的对象。代码会给一个不存在的对象发命令：\
         `kubectl get` 读回空、rollout 等的是别处的对象，而失败读起来像「还没滚到」：\
         {missing:#?}\n清单里实际有：{created:#?}"
    );
}

/// The other direction: a product-named ConfigMap or Secret the manifests
/// create but the declaration does not carry. Nothing in the code could reach
/// it without writing the name a second time, which is the copy this whole file
/// exists to prevent -- so the declaration has to be complete, not just
/// correct.
#[test]
fn every_product_object_the_manifests_create_is_declared() {
    let mut created: Vec<(String, String)> = Vec::new();
    for path in workload_manifests() {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        created.extend(workloads_in(&text));
    }
    let declared = declared();
    let undeclared: Vec<(String, String)> = product_objects(&created)
        .into_iter()
        .filter(|(_, name)| !declared.iter().any(|(_, d)| d == name))
        .collect();
    assert!(
        undeclared.is_empty(),
        "清单里有代码尚未声明的对象名。任何一处要按名找到它，都得再抄一遍字面量：\
         {undeclared:#?}"
    );
}

/// Every `*.rs` file under `crates/*/src`, relative to the repo root.
fn rust_sources() -> Vec<PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    let mut crates: Vec<PathBuf> = std::fs::read_dir(repo_root().join("crates"))
        .expect("crates/ 不在仓库里")
        .flatten()
        .map(|entry| entry.path())
        .collect();
    crates.sort();
    let mut out = Vec::new();
    for krate in crates {
        let src = krate.join("src");
        if src.is_dir() {
            walk(&src, &mut out);
        }
    }
    out.sort();
    out
}

/// The kinds that make a bare product name a *workload* reference. `service` is
/// not in the list on purpose: `service="cogneva"` is the tracing resource
/// label, not a Service, and a gate that cannot tell them apart reddens on
/// logging code.
const WORKLOAD_KINDS: [&str; 7] = [
    "deployment",
    "deployments",
    "deploy",
    "daemonset",
    "statefulset",
    "job",
    "svc",
];

/// Does the name at `at` in the flattened text sit where a workload reference
/// puts it -- `deployment/cogneva`, `"deployment", "cogneva"`,
/// `deployment: "cogneva"`, `svc/cogneva`?
fn is_workload_reference(flat: &str, at: usize) -> bool {
    let back = &flat[at.saturating_sub(18)..at];
    let stripped = back.trim_end_matches(['"', '[', '/', ',', ':', '=', '(']);
    WORKLOAD_KINDS.iter().any(|kind| stripped.ends_with(kind))
}

/// Positions of `name` as a whole token -- not the prefix of a longer workload
/// name (`cogneva-security-gateway` is a different workload, not this one).
fn token_positions(flat: &str, name: &str) -> Vec<usize> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = flat[from..].find(name) {
        let at = from + rel;
        let end = at + name.len();
        let before_ok = at == 0
            || !flat[..at]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || c == '-' || c == '_');
        let after_ok = !flat[end..]
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '-' || c == '_');
        if before_ok && after_ok {
            out.push(at);
        }
        from = end;
    }
    out
}

#[test]
fn only_the_declaration_names_an_object() {
    let main = cog_core::topology::MAIN_DEPLOYMENT;
    let unambiguous: Vec<&str> = declared()
        .into_iter()
        .map(|(_, name)| name)
        .filter(|name| *name != main)
        .collect();
    let mut offenders: Vec<(String, String)> = Vec::new();
    for path in rust_sources() {
        let rel = path
            .strip_prefix(repo_root())
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        if rel == DECLARATION {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        // Comment lines are prose, not calls: a comment that names a workload
        // is describing one, and a stale one does not fail at run time. Only
        // lines that *start* with `//` are dropped -- a line-internal `//` is
        // usually inside a URL (`http://<name>:8081`), and cutting from there
        // would hide the very occurrences this gate is for.
        let code: String = production_text(&text)
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let flat: String = code.split_whitespace().collect();
        for name in &unambiguous {
            if flat.contains(name) {
                offenders.push((rel.clone(), (*name).to_string()));
            }
        }
        for at in token_positions(&flat, main) {
            if is_workload_reference(&flat, at) {
                let start = at.saturating_sub(50);
                let end = (at + main.len() + 30).min(flat.len());
                offenders.push((rel.clone(), flat[start..end].to_string()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "对象名只能写在 {DECLARATION} 里（清单那边的声明在 deploy/）。这里又抄了一份：\
         chart 一改名，这些命令就去问一个不存在的对象，而报错读起来像「还没滚到」：\n\
         {offenders:#?}"
    );
}

/// A reading is only a reading if the scanner can see the shapes it is looking
/// for. These are the forms that were actually in the tree, plus the ones that
/// look similar and must not count.
#[test]
fn the_scanner_recognises_the_shapes_and_only_those() {
    let main = cog_core::topology::MAIN_DEPLOYMENT;
    let flat = |s: &str| -> String { s.split_whitespace().collect() };

    for positive in [
        format!("\"deployment\", \"{main}\""),
        format!("deployment: \"{main}\".into(),"),
        format!("deployment/{main}"),
        format!("deploy/{main}"),
        format!("svc/{main}"),
        format!("[\"-n\", namespace, \"rollout\", \"status\", \"deployment/{main}\"]"),
    ] {
        let text = flat(&positive);
        let hits: Vec<usize> = token_positions(&text, main)
            .into_iter()
            .filter(|at| is_workload_reference(&text, *at))
            .collect();
        assert!(!hits.is_empty(), "判据没认出负载引用：{positive}");
    }

    for negative in [
        // 观测面 resource label，不是 Service。
        format!("service = \"{main}\""),
        // 另一个负载，前缀相同但不是这一个。
        "deployment/cogneva-security-gateway".to_string(),
        // 镜像仓库名、NATS subject、JWT issuer 都不是负载引用。
        format!("localhost:30500/{main}:local"),
        "channel: \"cogneva-events\"".to_string(),
        format!("issuer: \"{main}\".into()"),
        // 已经走常量的写法。
        "deployment: cog_core::topology::MAIN_DEPLOYMENT.into()".to_string(),
    ] {
        let text = flat(&negative);
        let hits: Vec<usize> = token_positions(&text, main)
            .into_iter()
            .filter(|at| is_workload_reference(&text, *at))
            .collect();
        assert!(hits.is_empty(), "判据在不是负载引用的地方变红：{negative}");
    }
}

/// The four base workloads are a set the evolution loop compares revisions
/// across. A name in that list that the manifests do not create makes the
/// comparison read "not all on the same revision" -- a false negative that
/// looks exactly like a roll in progress.
#[test]
fn the_base_workload_set_is_four_deployments_the_chart_creates() {
    let base = cog_reflection::live_revision::BASE_DEPLOYMENTS;
    assert_eq!(base.len(), 4);
    let mut created: Vec<String> = Vec::new();
    for path in workload_manifests() {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for (kind, name) in workloads_in(&text) {
            if kind == "Deployment" {
                created.push(name);
            }
        }
    }
    for name in base {
        assert!(
            created.iter().any(|n| n == name),
            "主负载名单里的 {name} 没有对应的 Deployment：这一格读数会把它读成「不可读」\
             或「没在同一版上」，两种都不是真的"
        );
    }
}
