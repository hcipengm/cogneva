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

mod common;

use common::rust_production::{brace_delta, opens_test_module, production_lines};

/// The register this census is read against.
const REGISTER: &str = "crates/cogneva/tests/data/in-cluster-address-literals.tsv";

/// How many registered literals the code may carry. Zero, and nothing is
/// exempt: the two that used to live here are gone rather than registered.
/// The `base_image` default in `cog-core` claimed the chart overrode it, and
/// the variable it named is set nowhere -- so it was the only source, and
/// `ImageRollout` now refuses before it writes a Containerfile. The registry
/// convention that had a home in `cog-reflection` was a fifth copy of
/// `clusterRegistry.port`; the endpoint is now read from the declaration too.
const DEBT_CEILING: usize = 0;

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

/// Every hit of one predicate in the production half of the tree.
fn census_with(scan: fn(&str) -> Vec<String>) -> BTreeSet<Found> {
    let mut out = BTreeSet::new();
    for path in rust_sources() {
        let rel = path
            .strip_prefix(repo_root())
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for line in production_lines(&text) {
            for address in scan(line) {
                out.insert(Found {
                    file: rel.clone(),
                    address,
                });
            }
        }
    }
    out
}

/// Every in-cluster address the production half of the tree writes down.
fn census() -> BTreeSet<Found> {
    census_with(addresses_on)
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

/// 判据的边界必须按条目切，不能按「第一个测试标记」切。
///
/// 这一段是照着树里真实出现的形状写的：一个 `#[cfg(test)] fn`（测试夹具）夹在
/// 两段生产代码中间。按第一个标记截断的扫法会把后面那段生产代码整段跳过，而读数
/// 仍然是绿的——「没看」和「看了没有」读出来一模一样，这是门最不能有的失效方式。
#[test]
fn the_census_reads_on_after_a_test_helper_that_sits_before_production() {
    let text = r#"
fn before() {
    let a = "cogneva-registry.cogneva.svc.cluster.local:5000";
}

#[cfg(test)]
fn a_fixture() -> String {
    let t = "localhost:30500/cogneva:local";
    format!("{t}{}")
}

fn after() {
    let b = "prom.monitoring.svc.cluster.local:9090";
}
"#;
    let read: Vec<&str> = production_lines(text)
        .into_iter()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(
        read,
        vec![
            "fn before() {",
            r#"let a = "cogneva-registry.cogneva.svc.cluster.local:5000";"#,
            "}",
            "fn after() {",
            r#"let b = "prom.monitoring.svc.cluster.local:9090";"#,
            "}",
        ],
        "夹具那一项要跳过，它后面的生产代码要接着读"
    );
}

/// 同一个判据：整块 `#[cfg(test)] mod tests { … }` 跳过，模块关掉之后生产代码
/// 重新算数；`format!` 里的花括号是文本，不能把跳过的地方算错。
#[test]
fn a_test_module_is_skipped_and_production_after_it_is_not() {
    let text = r#"
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_case() {
        let s = format!("{}", "{");
        assert_eq!(s, "{");
    }
}

fn production() {
    let a = "prom.monitoring.svc.cluster.local:9090";
}
"#;
    let read: Vec<&str> = production_lines(text)
        .into_iter()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(
        read,
        vec![
            "fn production() {",
            r#"let a = "prom.monitoring.svc.cluster.local:9090";"#,
            "}",
        ],
        "测试模块整块不算生产面，它后面那段算"
    );
}

/// 夹具字符串跨行时，它里面的 `}` 不能把测试模块提前收掉。
///
/// 这一段是照 `cog-agent/src/web.rs` 的真实形状抄的：`mod tests` 里一个
/// `r#"…"#` 跨了四行，正文里有个 `}`。按行数括号会在那里把模块判成结束、
/// 把后面整段测试读成生产面——反过来也一样容易：漏读一段生产代码，读数还是绿的。
#[test]
fn a_fixture_string_that_spans_lines_does_not_end_the_module() {
    let text = r##"
#[cfg(test)]
mod tests {
    #[test]
    fn a_case() {
        let body = r#"[
            {"answers":[],"suggestions":["rust async"]}"#;
        assert!(!body.is_empty());
    }
}

fn production() {
    let a = "prom.monitoring.svc.cluster.local:9090";
}
"##;
    let read: Vec<&str> = production_lines(text)
        .into_iter()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(
        read,
        vec![
            "fn production() {",
            r#"let a = "prom.monitoring.svc.cluster.local:9090";"#,
            "}",
        ],
        "跨行夹具里的 `}}` 是文本，模块在那里没结束"
    );
}

/// 花括号只在代码里配对：字面量里的 `{` `}` 是文本，字符字面量 `'{'` 也不是嵌套。
#[test]
fn braces_inside_literals_do_not_nest() {
    assert_eq!(brace_delta(r##"let s = format!("{rev}");"##), 0);
    assert_eq!(brace_delta(r#"let c = '{';"#), 0);
    assert_eq!(brace_delta(r#"let c = '}';"#), 0);
    assert_eq!(brace_delta("fn f(&'a self) -> &'a str {"), 1);
    assert_eq!(brace_delta("// }{}{"), 0);
    assert_eq!(brace_delta("}"), -1);
}

/// 第二张登记表：以节点端口当 registry 的镜像引用。
///
/// 判据与 DNS 那一条是两回事。`localhost:8080` 是本地跑与 port-forward 都在用的
/// 形状，做判据会先红在正当代码上；`localhost:<port>/<repo>` 是镜像引用，那个端口
/// 只可能是 `clusterRegistry.nodePort` 在这一套部署上渲染出来的数——把它写进代码，
/// 就是把这个集群的端口抄进二进制。带协议的行不算：那是 URL，不是镜像。
const NODE_PORT_REGISTER: &str = "crates/cogneva/tests/data/node-port-registry-literals.tsv";

/// 这一行里以节点端口当 registry 的镜像引用，端点原样（`localhost:30500`）。
fn node_port_registries_on(line: &str) -> Vec<String> {
    // 一条 URL 里也有同样的子串，而 registry 端点从来不带协议。
    if line.contains("http://") || line.contains("https://") {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(at) = rest.find("localhost:") {
        let after = &rest[at + "localhost:".len()..];
        let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        let tail = &after[digits.len()..];
        // 端口后面紧跟 `/` 才是镜像引用；`localhost:8080"` 这类是别的地址。
        if !digits.is_empty() && tail.starts_with('/') {
            out.push(format!("localhost:{digits}"));
        }
        rest = &after[digits.len()..];
    }
    out
}

/// 生产半边里所有以节点端口当 registry 的镜像引用。
fn node_port_census() -> BTreeSet<Found> {
    census_with(node_port_registries_on)
}

// Same ratchet arithmetic as the DNS census above, and the same reason for
// writing it against the constant: raising the cap is how a literal that the
// manifest genuinely cannot declare gets registered.
#[allow(clippy::absurd_extreme_comparisons)]
#[test]
fn every_image_pulled_from_a_node_port_is_a_registered_decision() {
    let found = node_port_census();
    let rows: Vec<(String, String)> = read(NODE_PORT_REGISTER)
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
                "{NODE_PORT_REGISTER} 的 {file} / {address} 没写理由：没有理由的行就是一张白名单"
            );
            (file, address)
        })
        .collect();
    let registered: BTreeSet<(String, String)> = rows.iter().cloned().collect();

    assert!(
        rows.len() <= DEBT_CEILING,
        "登记表有 {} 行，上限是 {DEBT_CEILING}：镜像的 registry 端点由部署面声明，\
         代码读它，不抄它",
        rows.len()
    );

    let unregistered: Vec<&Found> = found
        .iter()
        .filter(|hit| !registered.contains(&(hit.file.clone(), hit.address.clone())))
        .collect();
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
        "代码里以节点端口当 registry 的引用与登记表对不上。\n\
         没登记的（改成从 Service 读那个端口，或登记并写清理由）：{unregistered:#?}\n\
         登记了但代码里已经没有的（删掉那一行）：{stale:#?}"
    );
}

#[test]
fn the_node_port_scanner_reads_image_references_and_nothing_else() {
    assert_eq!(
        node_port_registries_on(r#"let remote = format!("localhost:{port}/cogneva:local");"#),
        Vec::<String>::new(),
        "端口是运行时读出来的，代码里没有写死的数"
    );
    assert_eq!(
        node_port_registries_on(r#"const I: &str = "localhost:30500/cogneva:local";"#),
        vec!["localhost:30500".to_string()]
    );
    // A URL's port is not a registry endpoint: the scheme is the tell.
    assert_eq!(
        node_port_registries_on(r#"let u = "http://localhost:8080/api/v1";"#),
        Vec::<String>::new()
    );
    // A bare port with nothing after it is some other address.
    assert_eq!(
        node_port_registries_on(r#"let h = "localhost:8080";"#),
        Vec::<String>::new()
    );
}
