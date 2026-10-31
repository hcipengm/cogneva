//! principle: feedback_no_hardcoding_config_surface
//!
//! Gate: the three faces that answer "what is running in this cluster" must read
//! the whole cluster, not the one namespace the serving process happens to live in.
//!
//! On 2026-10-31 an operator asked for the full topology in the resource view and
//! found that two of its halves had never been cluster-wide at all: the workload
//! list and the quota/limit report each read
//! `/api/v1/namespaces/<own>/…`, so the Prometheus and Grafana deployments in
//! `monitoring`, and the control-plane workloads in `kube-system`, were simply
//! not on the page. The topology tree above them *was* cluster-wide — the pod,
//! event and PVC reads have no namespace segment — which is what made the gap
//! invisible: the page showed pods from every namespace while the tables beside
//! them, drawn from the same page, showed one.
//!
//! Nothing fails when a face narrows its scope. A namespace-scoped read answers
//! 200 with a shorter list, and a shorter list is exactly what "this cluster has
//! fewer workloads" looks like. So the scope is pinned here, in both directions:
//!
//! * the handler must ask for the cluster-wide path, and
//! * the path it asks for must be the one `WorkloadKind` builds for `None`,
//!   because a handler that spells its own URL can drift from the table the
//!   probe uses and then one of the two reports on a read nobody makes.

use crate::common;

use common::routes::{read, repo_root};
use common::rust_production::production_text;
use common::rust_text::rust_function_body;

const CLUSTER: &str = "crates/cog-gateway/src/cluster.rs";

fn source() -> String {
    production_text(&read(&repo_root().join(CLUSTER)))
}

fn body(name: &str) -> String {
    let text = source();
    rust_function_body(&text, name)
        .unwrap_or_else(|| panic!("{CLUSTER} no longer defines a function named `{name}`"))
}

/// Every `"/api/v1/…"` and `"/apis/…"` literal in a function body.
///
/// Read as literals rather than by searching for a substring: the same path
/// appears in doc comments and in the probe table, and a search that cannot tell
/// a path the handler asks for from one it merely mentions would stay green on a
/// handler that stopped asking.
fn paths(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (index, _) in body.match_indices("\"/") {
        // `rest` 从开引号开始；`rest[1..]` 是引号之后的字节，所以收尾位置要
        // 再加回那一个引号——少加一格取到的是缺了最后一个字符的路径，而
        // `starts_with` 照样认它。
        let rest = &body[index..];
        let Some(end) = rest[1..].find('"') else {
            continue;
        };
        let literal = &rest[1..1 + end];
        if literal.starts_with("/api/") || literal.starts_with("/apis/") {
            out.push(literal.to_string());
        }
    }
    out
}

/// A path that names one namespace cannot be the one a whole-cluster face reads.
fn names_a_namespace(path: &str) -> bool {
    path.contains("/namespaces/")
}

#[test]
fn the_quota_face_reads_every_namespace() {
    let body = body("cluster_quota_handler");
    let paths = paths(&body);
    assert!(!paths.is_empty(), "配额那一面一条路径都没读到：{body}");
    for path in &paths {
        assert!(
            !names_a_namespace(path),
            "配额/限额仍在读单命名空间路径 {path}：monitoring 与 kube-system 的\
             ResourceQuota / LimitRange 不会出现在资源页上，而少了几个命名空间的账\
             与一张完整的账在屏上同形"
        );
    }
    for want in ["/api/v1/resourcequotas", "/api/v1/limitranges"] {
        assert!(
            paths.iter().any(|path| path == want),
            "配额那一面没有直接读 {want}（读到的是 {paths:?}）：这两个对象的集群范围\
             路径就长这样，读别的路径意味着它只覆盖了一部分命名空间"
        );
    }
}

#[test]
fn the_workload_face_asks_for_the_cluster_wide_path() {
    let body = body("cluster_deployments_handler");
    assert!(
        body.contains("list_path(None)"),
        "工作负载那一面没有用 `list_path(None)` 取集群范围的路径：用 `Some(..)` 时\
         它只读本命名空间，monitoring 的 Prometheus / Grafana 与 kube-system 的\
         控制面组件都不会出现在「工作负载」表里"
    );
    assert!(
        !body.contains("list_path(Some("),
        "工作负载那一面又按命名空间取了路径：`list_path(Some(..))` 的每一条都带\
         `/namespaces/`，那是这次修掉的形状"
    );
}

/// The probe must walk the paths the handlers walk. A probe that checks
/// `/namespaces/<ns>/deployments` while the handler reads `/apis/apps/v1/deployments`
/// reports `ok` for a permission that the real read does not have.
#[test]
fn the_probe_asks_for_the_same_paths_as_the_faces() {
    let body = body("read_targets");
    for path in paths(&body) {
        assert!(
            !names_a_namespace(&path),
            "集群读探针仍在探单命名空间路径 {path}：读数读的是集群范围，探针探的却是\
             另一条——只授权了单命名空间的账号会让探针全绿而每条读数都空"
        );
    }
    assert!(
        body.contains("list_path(None)"),
        "探针没有用 `list_path(None)`：它探的是另一条路径"
    );
}

/// Both reports say how many namespaces they covered. Without it, "no quotas
/// anywhere in this cluster" and "the read covered nothing" print the same.
#[test]
fn both_reports_say_which_namespaces_they_covered() {
    let text = source();
    for report in ["ResourceReport", "WorkloadsReport"] {
        let at = text
            .find(&format!("pub struct {report}"))
            .unwrap_or_else(|| panic!("{CLUSTER} no longer declares {report}"));
        let block = &text[at..at + text[at..].find("\n}").unwrap_or(0)];
        assert!(
            block.contains("pub namespaces: Vec<String>"),
            "{report} 没有 `namespaces` 这一格：读者数不出这份读数覆盖了几个命名空间，\
             「整个集群都没有」与「一个都没读到」就分不开"
        );
    }
}

/// The web client's types carry the namespace of every row. Without it the page
/// cannot group by it, and the same workload name in two namespaces becomes one
/// row key.
#[test]
fn the_web_client_carries_the_namespace_of_every_row() {
    let client = read(&repo_root().join("web/src/api/client.ts"));
    for interface in ["QuotaView", "LimitView", "WorkloadView"] {
        let at = client
            .find(&format!("export interface {interface} {{"))
            .unwrap_or_else(|| panic!("client.ts no longer declares {interface}"));
        let block = &client[at..at + client[at..].find("\n}").unwrap_or(0)];
        assert!(
            block.contains("namespace: string"),
            "{interface} 没有 `namespace` 字段：两条不同命名空间的对象在表里并成一条，\
             屏上看不出归属"
        );
    }
}

/// The resource view groups by the namespace it now reads. A page that renders
/// a flat list of two `default` quotas side by side has the data and throws away
/// the half that says whose they are.
#[test]
fn the_resource_view_groups_its_quotas_by_namespace() {
    let view = read(&repo_root().join("web/src/views/ResourcesView.tsx"));
    assert!(
        view.contains("byNamespace("),
        "资源页没有按命名空间分组：集群范围的配额与限额直接摊平，两条同名的 \
         ResourceQuota 在屏上并排，没人知道哪条属于谁"
    );
    assert!(
        view.contains("item.namespace"),
        "资源页的分组函数没有读 `namespace`：分组键来自别处，就与对象的归属不是同一个数"
    );
}
