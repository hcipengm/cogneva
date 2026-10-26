//! principle: feedback_no_hardcoding_config_surface
//! The cluster's service addresses are the deployment's to declare: the chart
//! renders them from `values.yaml` into the process environment, and
//! `files/cogneva.json` gets them substituted by `templates/app-config.yaml`.
//! A literal in Rust is none of those paths -- it is a second copy that no
//! `--set` can move.
//!
//! The failure this guards is quiet in a particular way. An address written
//! into the code is correct on the day it is written; it goes wrong only when
//! the deployment moves, and then it points at a port nobody listens on. What
//! comes back is a connection error, which every caller in this tree reports as
//! "the other side is down" -- so the incident starts on the wrong machine.
//!
//! The criterion is the cluster-DNS form, `<host>.svc.cluster.local:<port>`.
//! Nothing else qualifies: short names (`cogneva-security-gateway:8081`) and
//! `localhost:8080` are also what a local run and a port-forward use, so a
//! predicate that caught them would be red on legitimate code before it caught
//! anything real. The DNS form only resolves inside a cluster, so writing one
//! down is always a deployment fact.
//!
//! Every hit has to be a row in
//! [`data/in-cluster-address-literals.tsv`](data/in-cluster-address-literals.tsv)
//! with the reason it stays, and the row count is capped, so the set cannot grow
//! back. Test modules are not scanned: a fixture naming an address is a specimen,
//! not a fact the process runs on.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The register this census is read against.
const REGISTER: &str = "crates/cogneva/tests/data/in-cluster-address-literals.tsv";

/// How many registered literals the code may carry. Zero: the last one, the
/// `base_image` default in `cog-core`, was removed rather than registered --
/// the reason it carried ("the chart overrides it") turned out to name an
/// environment variable nothing in `deploy/` sets, so it was the only source.
/// `ImageRollout` now refuses before it writes a Containerfile.
const DEBT_CEILING: usize = 0;

/// The one file allowed to name the in-cluster registry without registering:
/// it *is* the convention, and a convention with copies is not one.
const CONVENTION_HOME: &str = "crates/cog-reflection/src/registry_endpoint.rs";

/// The suffix every cluster-DNS address ends with before its port.
const SUFFIX: &str = ".svc.cluster.local";

/// One literal found in the tree: the file it is in and the address as written.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Found {
    file: String,
    address: String,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Every `*.rs` file under `crates/*/src`, relative to the repo root.
fn rust_sources() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
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
    let root = repo_root().join("crates");
    let mut crates: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("{}: {e}", root.display()))
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

/// Whether this line ends the production half of a file: everything after
/// `#[cfg(test)]` or an inline `mod tests {` is a specimen, not a deployment
/// fact. The tree puts its test modules last, so the first marker is the
/// boundary.
fn opens_test_module(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("#[cfg(test)]")
        || trimmed.starts_with("mod tests")
        || trimmed.starts_with("pub mod tests")
}

/// The cluster-DNS addresses written on one line.
fn addresses_on(line: &str) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i + SUFFIX.len() <= chars.len() {
        let window: String = chars[i..i + SUFFIX.len()].iter().collect();
        if window != SUFFIX {
            i += 1;
            continue;
        }
        // The host runs left from the dot: letters, digits, dashes and dots, plus
        // the braces of a `format!` hole. A host assembled at runtime is still a
        // cluster address written down -- `cogneva-registry.{ns}.svc...` names one
        // deployment convention, and skipping it would leave the most repeated
        // shape in the tree uncounted.
        let mut start = i;
        while start > 0
            && (chars[start - 1].is_ascii_alphanumeric()
                || chars[start - 1] == '-'
                || chars[start - 1] == '.'
                || chars[start - 1] == '{'
                || chars[start - 1] == '}')
        {
            start -= 1;
        }
        // The port runs right from the suffix, if there is one.
        let mut end = i + SUFFIX.len();
        if chars.get(end) == Some(&':') {
            end += 1;
            while end < chars.len() && chars[end].is_ascii_digit() {
                end += 1;
            }
        }
        if start < i {
            out.push(chars[start..end].iter().collect());
        }
        i = end;
    }
    out
}

/// Every in-cluster address the production half of the tree writes down.
fn census() -> BTreeSet<Found> {
    let mut out = BTreeSet::new();
    for path in rust_sources() {
        let rel = path
            .strip_prefix(repo_root())
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for line in text.lines() {
            if opens_test_module(line) {
                break;
            }
            if line.trim_start().starts_with("//") {
                continue;
            }
            for address in addresses_on(line) {
                out.insert(Found {
                    file: rel.clone(),
                    address,
                });
            }
        }
    }
    out
}

/// The register's rows, as `(file, address)`.
fn register() -> Vec<(String, String)> {
    read(REGISTER)
        .lines()
        .filter(|line| !line.trim_start().starts_with('#') && !line.trim().is_empty())
        .filter(|line| !line.starts_with("file\t"))
        .map(|line| {
            let mut columns = line.split('\t');
            let file = columns.next().unwrap_or_default().trim().to_string();
            let address = columns.next().unwrap_or_default().trim().to_string();
            let reason = columns.next().unwrap_or_default().trim();
            assert!(
                !reason.is_empty(),
                "{REGISTER} 的 {file} / {address} 没写理由：没有理由的行就是一张白名单"
            );
            (file, address)
        })
        .collect()
}

// The cap below is a ratchet and a paid-up one reads zero, which folds the
// comparison to `len() == 0` and makes clippy call the `<=` half absurd. It is
// written against the constant anyway: raising the cap is how a literal that the
// manifest genuinely cannot declare gets registered, and an `==` would silently
// turn "at most" into "exactly".
#[allow(clippy::absurd_extreme_comparisons)]
#[test]
fn every_cluster_address_in_the_code_is_a_registered_decision() {
    let found = census();
    let rows = register();
    let registered: BTreeSet<(String, String)> = rows.iter().cloned().collect();

    assert!(
        rows.len() <= DEBT_CEILING,
        "登记表有 {} 行，上限是 {DEBT_CEILING}：这一轮的消除面就是把代码里的集群地址交还给\
         清单。新增一行要同时抬 DEBT_CEILING 并写清为什么清单声明不了它",
        rows.len()
    );

    let mut unregistered = Vec::new();
    for hit in &found {
        if hit.file == CONVENTION_HOME {
            continue;
        }
        if !registered.contains(&(hit.file.clone(), hit.address.clone())) {
            unregistered.push(hit);
        }
    }

    // A row whose literal is gone is dead weight: it inflates the cap and reads
    // as a fact that is still carried.
    let stale: Vec<&(String, String)> = rows
        .iter()
        .filter(|(file, address)| {
            !found.contains(&Found {
                file: file.clone(),
                address: address.clone(),
            })
        })
        .collect();

    assert!(
        unregistered.is_empty() && stale.is_empty(),
        "代码里的集群地址与登记表对不上。\n\
         没登记的（要么改成从清单读，要么登记并写清理由）：{unregistered:#?}\n\
         登记了但代码里已经没有的（删掉那一行，别让上限替不存在的事实说话）：{stale:#?}"
    );
}

#[test]
fn the_scanner_reads_the_dns_form_and_nothing_else() {
    assert_eq!(
        addresses_on(r#"let url = "http://cogneva-security-gateway:8081/v1";"#),
        Vec::<String>::new(),
        "短名是本地跑与 port-forward 也在用的形状，不做判据"
    );
    assert_eq!(
        addresses_on(r#"const A: &str = "http://prom.monitoring.svc.cluster.local:9090";"#),
        vec!["prom.monitoring.svc.cluster.local:9090".to_string()]
    );
    // A `format!` hole is part of the host: the whole dotted name is what gets
    // registered, so the same address written two ways cannot count as two rows.
    assert_eq!(
        addresses_on(r#"format!("redis-0.redis.{ns}.svc.cluster.local:6379")"#),
        vec!["redis-0.redis.{ns}.svc.cluster.local:6379".to_string()]
    );
    assert!(
        opens_test_module("    #[cfg(test)]"),
        "测试模块往后都不算生产面"
    );
}
