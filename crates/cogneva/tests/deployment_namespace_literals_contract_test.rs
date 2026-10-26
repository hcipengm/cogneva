//! principle: feedback_no_hardcoding_config_surface
//! The namespace is the deployment's to declare: `deploy/helm/cogneva/values.yaml`
//! carries `global.namespace`, the chart renders every object into it, and
//! bootstrap reads that name once (`declared_namespace`) and hands it to every
//! `kubectl`/`helm` call it makes.
//!
//! A second copy in Rust is a copy no `--set` can move. It is also one that never
//! reports being wrong: point the deployment at another namespace and every
//! `kubectl -n <old name>` still exits 0 — `get` reads an empty list, `delete`
//! says NotFound after `--ignore-not-found`, `create namespace` quietly rebuilds
//! the old one. The install then reports success while the workloads it waits on
//! were never in the namespace it looked at.
//!
//! The predicate is the *shape* of a namespace argument, not the bare name:
//! `cogneva` is also the release name, the workload name and half the image
//! references, and a gate that reddened on those would be turned off before it
//! caught anything. The name itself is read from the chart, so moving
//! `global.namespace` moves this gate with it instead of leaving it scanning for
//! a name nobody uses.
//!
//! Test modules are not scanned: a fixture may name whatever it likes.

use std::path::PathBuf;

mod common;

use common::rust_production::production_text;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The namespace the chart declares. Read, not written down here — that is the
/// whole point of the gate.
fn declared_namespace() -> String {
    let path = repo_root().join("deploy/helm/cogneva/values.yaml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let parsed: serde_json::Value =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    parsed
        .pointer("/global/namespace")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            panic!(
                "{} 没有声明 global.namespace：这条判据无从判起，也不该猜一个名字",
                path.display()
            )
        })
        .to_string()
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

/// The shapes a namespace literal takes. `-n <name>` inside a shell string and
/// `"-n", "<name>"` in an argument list are the same fact written two ways, so
/// both are asked for; whitespace is stripped before the search, because the
/// argument pair is usually split across lines.
fn shapes(namespace: &str) -> Vec<String> {
    vec![
        format!("\"-n\",\"{namespace}\""),
        format!("\"--namespace\",\"{namespace}\""),
        format!("-n{namespace}"),
        format!("--namespace={namespace}"),
        format!("namespace={namespace}"),
        format!("\"namespace/{namespace}\""),
        format!("namespace:{namespace}"),
        format!("namespace{namespace}"),
        format!("namespace\":\"{namespace}\""),
    ]
}

/// One hit: where, and the shape that matched. Read only through `Debug` when
/// the assertion prints them, which is enough for a test-only type.
#[derive(Debug)]
#[allow(dead_code)]
struct Hit {
    file: String,
    shape: String,
    around: String,
}

fn census() -> Vec<Hit> {
    let namespace = declared_namespace();
    let wanted = shapes(&namespace);
    let mut hits = Vec::new();
    for path in rust_sources() {
        let rel = path
            .strip_prefix(repo_root())
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        // Whitespace out: `"-n",` and the name it belongs to are usually on
        // different lines, and a line-at-a-time search would miss exactly the
        // call form `cargo fmt` produces.
        let flat: String = production_text(&text).split_whitespace().collect();
        for shape in &wanted {
            let Some(at) = flat.find(shape.as_str()) else {
                continue;
            };
            let start = at.saturating_sub(60);
            let end = (at + shape.len() + 60).min(flat.len());
            hits.push(Hit {
                file: rel.clone(),
                shape: shape.clone(),
                around: flat[start..end].to_string(),
            });
        }
    }
    hits
}

#[test]
fn no_source_writes_the_declared_namespace_into_a_command() {
    let hits = census();
    assert!(
        hits.is_empty(),
        "代码里把命名空间写进了命令。这个名字由 chart 声明、由 bootstrap 读一次\
         （crates/bootstrap 里的 declared_namespace），其他地方再抄一份就是第二份声明：\n{:#?}",
        hits
    );
}

/// A reading is only a reading if the scanner can see the shape it is looking
/// for. These are the three forms that were actually in the tree — an argument
/// pair split across lines, a shell pipeline inside a `format!`, and a resource
/// id — plus the two that are not a namespace argument at all.
#[test]
fn the_scanner_recognises_the_shapes_and_only_those() {
    let namespace = declared_namespace();
    let wanted = shapes(&namespace);
    let flat = |s: &str| -> String { s.split_whitespace().collect() };
    // An argument pair split across lines.
    let pair = flat(&format!("\"-n\",\n                \"{namespace}\","));
    assert!(wanted.iter().any(|s| pair.contains(s.as_str())), "{pair}");
    // A shell pipeline inside a string that is formatted at run time.
    let shell = flat(&format!(
        "format!(\"kubectl create namespace {namespace} --dry-run=client -o yaml | kubectl apply -f -\")"
    ));
    assert!(wanted.iter().any(|s| shell.contains(s.as_str())), "{shell}");
    // A resource id handed to `kubectl patch`.
    let id = flat(&format!("vec![\"namespace/{namespace}\".to_string()]"));
    assert!(wanted.iter().any(|s| id.contains(s.as_str())), "{id}");
    // Not shapes: the release name, the workload name, an image reference, and
    // the same name carried in a variable.
    for innocent in [
        format!("\"{namespace}\".into()"),
        format!("deployments/{namespace}"),
        format!("localhost:30500/{namespace}:local"),
        "-n\",\n            namespace,".to_string(),
    ] {
        let flat = flat(&innocent);
        assert!(
            !wanted.iter().any(|s| flat.contains(s.as_str())),
            "{innocent} 不是命名空间参数，判据不该在它上面变红"
        );
    }
}

/// The one manifest bootstrap renders at run time must leave the name to the
/// installer. A literal there would send the distributor into a namespace the
/// `-n <declared>` calls around it never look at.
#[test]
fn the_manifest_bootstrap_renders_leaves_the_namespace_to_the_installer() {
    let namespace = declared_namespace();
    let path = repo_root().join("deploy/k8s/image-distributor.yaml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let literal = format!("namespace: {namespace}");
    let offenders: Vec<&str> = text
        .lines()
        .filter(|line| line.trim().starts_with(&literal))
        .collect();
    assert!(
        offenders.is_empty(),
        "{} 里写着 `{literal}`：这份模板由引导器渲染，命名空间要留占位符；实得 {offenders:?}",
        path.display()
    );
    assert!(
        text.contains("namespace: __NAMESPACE__"),
        "{} 没有 __NAMESPACE__ 占位符，引导器渲染时没有地方写它读到的名字",
        path.display()
    );
}
