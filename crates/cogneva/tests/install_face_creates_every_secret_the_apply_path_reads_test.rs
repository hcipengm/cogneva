//! The Secrets the apply-path workloads read, against the ones the install
//! face creates.
//!
//! A Secret never travels through the apply path: a manifest that carries one
//! is a hard error (credentials do not ride the manifest channel), and the
//! deployer's Role grants no Secret verbs -- so the rollout side can neither
//! deliver one nor *read* whether one exists. Nothing on that side can
//! therefore catch "a workload now reads a Secret nobody creates", which is the
//! shape of the 2026-10-11 incident: the render pointed the security gateway at
//! `cogneva-secrets-oob` while the object that creates it for the apply path,
//! `init-secrets.sh`, was not yet creating it. The gateway came up on zero
//! credentials and every readiness gate stayed green.
//!
//! So the comparison lives here, beside the tree it reads. The apply-path tree
//! the in-cluster deployer consumes is `deploy/k3s/` (the parity gate holds it
//! equal to the k3s-single chart render), and the object that creates these
//! Secrets there is `deploy/scripts/init-secrets.sh`.
//!
//! The script names them through variables, so this reads the script the way
//! the shell does: an assignment binds the variable, and `ensure_object "$VAR"`
//! is the call that creates the object. A shape this cannot resolve is a
//! failure, not a skip -- a check that cannot run must not report "nothing to
//! see".

mod common;

use common::routes::{read_repo, repo_root};
use common::yaml::docs;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cog_reflection::workload_kind;

fn at_path<'a>(v: &'a serde_yaml::Value, path: &str) -> Option<&'a serde_yaml::Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn str_at<'a>(v: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a str> {
    let mut cur = v;
    for seg in path {
        cur = cur.get(seg)?;
    }
    cur.as_str()
}

fn seq(v: Option<&serde_yaml::Value>) -> Vec<&serde_yaml::Value> {
    v.and_then(|v| v.as_sequence())
        .map(|s| s.iter().collect())
        .unwrap_or_default()
}

/// Every Secret name one document's workload reads by name: the same field set
/// the deployer's lookup reads off a different key, because these are the reads
/// that go dangling silently when the object is missing.
fn secret_references(doc: &serde_yaml::Value) -> Vec<String> {
    let kind = doc.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    let Some(entry) = workload_kind(kind) else {
        return Vec::new();
    };
    let Some(pod) = at_path(doc, entry.pod_template) else {
        return Vec::new();
    };
    let mut out: Vec<String> = Vec::new();
    let mut push = |name: Option<&str>| {
        if let Some(n) = name.filter(|n| !n.is_empty()) {
            out.push(n.to_string());
        }
    };
    for field in ["containers", "initContainers"] {
        for c in seq(pod.get(field)) {
            for e in seq(c.get("env")) {
                push(
                    e.get("valueFrom")
                        .and_then(|v| str_at(v, &["secretKeyRef", "name"])),
                );
            }
            for ef in seq(c.get("envFrom")) {
                push(str_at(ef, &["secretRef", "name"]));
            }
        }
    }
    for vol in seq(pod.get("volumes")) {
        push(str_at(vol, &["secret", "secretName"]));
        for src in seq(vol.get("projected").and_then(|v| v.get("sources"))) {
            push(str_at(src, &["secret", "name"]));
        }
    }
    out
}

/// The Secret names `init-secrets.sh` creates: the variables it binds, resolved
/// at the `ensure_object` calls. An `ensure_object` argument this cannot resolve
/// is a failure, and so is resolving none at all -- a name that slips out of the
/// set would turn a real gap green.
fn created_secret_names(script: &str) -> BTreeSet<String> {
    let mut vars: BTreeMap<String, String> = BTreeMap::new();
    let mut ensured: BTreeSet<String> = BTreeSet::new();
    let mut unresolved: Vec<String> = Vec::new();
    for line in script.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("ensure_object ") {
            let arg = rest.trim().trim_matches('"').trim_start_matches('$');
            match vars.get(arg) {
                Some(name) => {
                    ensured.insert(name.clone());
                }
                None => unresolved.push(arg.to_string()),
            }
            continue;
        }
        if let Some((lhs, rhs)) = line.split_once('=') {
            let is_bare_assignment = !lhs.is_empty()
                && lhs.chars().all(|c| c.is_ascii_uppercase() || c == '_')
                && !rhs.is_empty()
                && rhs
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
            if is_bare_assignment {
                vars.insert(lhs.to_string(), rhs.to_string());
            }
        }
    }
    assert!(
        unresolved.is_empty(),
        "init-secrets.sh 里 ensure_object 的参数解析不出变量：{unresolved:?}"
    );
    assert!(
        !ensured.is_empty(),
        "没有从 init-secrets.sh 里解析出任何被创建的 Secret：判据的形态变了，不是这里没有东西可判"
    );
    ensured
}

#[test]
fn the_walk_and_the_script_parse_both_name_the_reads_and_the_creates() {
    // Every by-name Secret read a pod template can carry, on a CronJob so the
    // template is reached through jobTemplate.
    let doc = "kind: CronJob\nmetadata:\n  name: c\nspec:\n  jobTemplate:\n    spec:\n      template:\n        spec:\n          containers:\n            - name: x\n              env:\n                - valueFrom:\n                    secretKeyRef:\n                      name: a\n              envFrom:\n                - secretRef:\n                    name: b\n          volumes:\n            - secret:\n                secretName: d\n            - projected:\n                sources:\n                  - secret:\n                      name: e\n";
    let mut names = secret_references(&docs(doc, "fixture")[0]);
    names.sort();
    assert_eq!(names, vec!["a", "b", "d", "e"]);

    // The script shape the parser reads: bind the variable, ensure the object.
    let script = "SECRET=cogneva-secrets\nOOB_SECRET=cogneva-secrets-oob\nensure_object \"$SECRET\"\nensure_object \"$OOB_SECRET\"\n";
    let created: Vec<String> = created_secret_names(script).into_iter().collect();
    assert_eq!(created, vec!["cogneva-secrets", "cogneva-secrets-oob"]);
}

/// 判据读不懂的形态要**响**，不是当它不存在：一篇解析不出来的文档被丢掉，它带的
/// Secret 引用就一起消失，差集照样绿——单文件畸形是聚合层的 `!referenced.is_empty()`
/// 咬不住的。
#[test]
#[should_panic(expected = "解析不出来")]
fn a_document_that_does_not_parse_is_a_failure_not_a_skip() {
    // 未闭合的 flow mapping：一处真实的畸形形态（比如没渲染完的模板落到这里）。
    let _ = docs("kind: Deployment\nmetadata: {name: torn\n", "torn.yaml");
}

#[test]
fn every_secret_an_apply_path_workload_reads_is_created_by_the_install_face() {
    let created = created_secret_names(&read_repo("deploy/scripts/init-secrets.sh"));

    let dir = repo_root().join("deploy/k3s");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "deploy/k3s 里一个清单都没有：路径变了，这是判据起不来的错，不是它通过了"
    );

    let mut referenced: BTreeSet<String> = BTreeSet::new();
    for file in &files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        for doc in docs(&text, &file.display().to_string()) {
            referenced.extend(secret_references(&doc));
        }
    }
    assert!(
        !referenced.is_empty(),
        "deploy/k3s 里没有一个工作负载按名读 Secret：判据没读到东西，不是它通过了"
    );

    let orphans: Vec<&String> = referenced.difference(&created).collect();
    assert!(
        orphans.is_empty(),
        "这些 Secret 被 deploy/k3s 的工作负载引用，但 init-secrets.sh 不建它们：{orphans:?}；\
         引出它们的那次改动里，建它的一方必须同步跟上"
    );
}
