//! principle: feedback_no_hardcoding_config_surface
//!
//! 抓取目标在两处各写一遍：Pod 里的容器按名字声明端口（`ports[].name: metrics`），
//! 监视器按名字去抓（`endpoints[].port: metrics`），路径写在第三处
//! （`endpoints[].path: /metrics` 对 `app.route("/metrics", …)`）。三处之间没有任何
//! 东西比较。
//!
//! 漂移的后果是**没有读数**，而这个读数与「一切正常」同形：ServiceMonitor 的
//! `port` 名字对不上它选中的 Service，Prometheus Operator 既不产生目标也不报错
//! ——面板上是一条零线，告警规则里没有一条会响，因为「目标不存在」不是任何一条
//! 规则的输入。树里已经有一处手工修过的标本（traefik 的 PodMonitor：chart 的
//! Service 只转 `web`/`websecure`，`metrics` 只在 Pod 上），那份修法由
//! `observability_stack_convergence_test.rs` 里的单条判据钉着；这道门管的是其余全部。
//!
//! 判什么（三处各一句）：
//!
//! * **选中谁**：监视器的 `namespaceSelector` 加选择器，在这棵树里至少命中一个
//!   对象。一个命中不了任何东西的监视器是纯静的：装上了、应用了、什么都不抓。
//! * **端口名**：`endpoints[].port` 必须是被选中对象真的声明过的那个名字——对
//!   ServiceMonitor 是命中 Service 的 `ports[].name`，对 PodMonitor 是命中工作负载
//!   容器端口的名字。至少有一个被选中对象提供它；匹配不到任何提供者的端点就是
//!   那条零线的来源。
//! * **路径**：顺着 Service 的 `selector` 与 `targetPort` 走到容器上（具名端口按
//!   名字比、数字端口按端口号比），凡是跑本项目镜像的容器，`path` 必须是**这个
//!   容器起的那份代码**注册过的路由。第三方容器不判路径——它的路由不在这棵树里。
//!
//! 判不了什么：
//!
//! * 第三方命名空间（`kube-system`、`ingress-nginx`）里的对象：这棵树不声明它们
//!   （[`common::service_topology::declared_namespaces`] 只认本树的工作负载与
//!   Service 落在哪），抓它们的监视器归别人的栈；traefik 那一份的端口语义另有
//!   单条判据钉着。
//! * 处置表里登记为 `exempt` 的清单：那些文件永不交付，它里面的监视器不产生目标，
//!   判它等于判一段不上集群的字节。表由
//!   `cog_reflection::observability_stack::read_dispositions_from` 读——与运行时
//!   收敛循环、安装脚本读的是同一张表，这道门不另立一份名单。
//! * 抓取间隔、超时、`honorLabels` 这类端点参数；
//! * 目标真的能不能被抓到（网络策略、TLS、认证）：这是运行期读数，静态清单里
//!   读不出来。
//!
//! 普查要有下限：读不到 chart 的 `image.repository`、这条判据在树里一个本项目的
//! 被抓容器都没走到、或者一个在范围内的监视器都没有时，一律按红处理——「没读到」
//! 与「没有漂移」在结果上分不开。处置表**不存在**时按「全部交付」判（多判不会
//! 漏，少判才会），读了但解不出来时按红。
//!
//! 消融（真实树，两次）：
//!
//! * 把 `deploy/k3s/observability/manifests/18-servicemonitor-storage.yaml` 的
//!   `endpoints[].port` 从 `http` 改成 `httpz`，本门红，点名该监视器、它选中的
//!   qdrant Service 与那些 Service 实际声明的端口名；
//! * 把 `08-servicemonitor-gateway.yaml` 的 `path` 从 `/metrics` 改成 `/metricz`，
//!   本门红，点名 `gateway-deployment.yaml` 的 `security-gateway` 容器与
//!   `security_gateway.rs` 实际注册的那二十来条路径。
//!
//! 两次都改回绿。第三方命名空间、处置表豁免、第三方镜像不判路径这三个分支由夹具
//! 守着——真实树上「另一个命名空间的监视器」与「豁免的监视器」同时存在，把它们
//! 改红只证明它们会被判，不证明它们会被放过。

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cog_reflection::observability_stack::{read_dispositions_from, Disposition};

use common::routes::repo_root;
use common::rust_production::{production_sources, sources_of_tree};
use common::service_topology::{
    container_routes, declared_namespaces, monitors, our_image, routes_of, selector_matches,
    services, workloads, ContainerSpec, Monitor,
};

const MANIFESTS: &str = "deploy/k3s/observability/manifests";
const DISPOSITIONS: &str = "delivery-dispositions.txt";

/// 处置表里登记为永不交付的清单文件名。
///
/// 表不存在时返回空集而不是报错：缺席的方向是「全部照判」，多判不会漏掉漂移。
/// 表在但解不出来时要报出去，那说明有人改了格式而没人跟。
fn exempt_files(root: &Path) -> Result<BTreeSet<String>, String> {
    let dir = root.join(MANIFESTS);
    if !dir.join(DISPOSITIONS).is_file() {
        return Ok(BTreeSet::new());
    }
    let entries = read_dispositions_from(&dir)?;
    Ok(entries
        .into_iter()
        .filter(|entry| entry.disposition == Disposition::Exempt)
        .map(|entry| entry.file)
        .collect())
}

/// 监视器管得着的命名空间。
fn scope_of(monitor: &Monitor, declared: &BTreeSet<String>) -> BTreeSet<String> {
    if monitor.any_namespace {
        return declared.clone();
    }
    if monitor.namespaces.is_empty() {
        return [monitor.namespace.clone()].into_iter().collect();
    }
    monitor.namespaces.iter().cloned().collect()
}

fn is_our_image(image: Option<&String>, container: &ContainerSpec) -> bool {
    image.is_some_and(|our| container.image.starts_with(our))
}

fn complaints(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut out = Vec::new();
    let workloads = workloads(root);
    let services = services(root);
    let declared = declared_namespaces(root);
    let every = routes_of(root, files);
    let image = our_image(root);
    let exempt = match exempt_files(root) {
        Ok(exempt) => exempt,
        Err(reason) => {
            out.push(format!("  `{MANIFESTS}/{DISPOSITIONS}` 读不了：{reason}"));
            return out;
        }
    };

    if image.is_none() {
        out.push(format!(
            "  读不到 `{}` 里 `image.repository` 的取值：分不出哪些被抓容器跑的是本项目\
             的镜像，路径那一半判不了",
            common::service_topology::CHART_VALUES
        ));
    }
    if every.is_empty() {
        out.push(
            "  这棵树的 `crates/*/src` 里一条 `.route(\"…\")` 都没读到：读不出来按红处理"
                .to_string(),
        );
    }

    let mut judged: BTreeSet<String> = BTreeSet::new();
    let mut in_scope = 0usize;
    for monitor in monitors(root) {
        let file = monitor
            .manifest
            .rsplit('/')
            .next()
            .unwrap_or(&monitor.manifest)
            .to_string();
        if monitor.manifest.starts_with(MANIFESTS) && exempt.contains(&file) {
            continue;
        }
        let scope = scope_of(&monitor, &declared);
        if scope.is_disjoint(&declared) {
            // 别人的命名空间：这棵树对它一无所知，判不了。
            continue;
        }
        in_scope += 1;
        let label = format!(
            "`{}` 里的 {} `{}`",
            monitor.manifest, monitor.kind, monitor.name
        );

        // 端点 → (路径, 顺着端口名走到的容器)。按端点分开记：一个 Service 的两个
        // 端口指向两个不同容器时，把端点混在一起判，会给一个容器安上另一个容器
        // 才该服务的路径。
        let mut reached: Vec<(String, Vec<&ContainerSpec>)> = Vec::new();
        if monitor.kind == "ServiceMonitor" {
            let selected: Vec<_> = services
                .iter()
                .filter(|service| scope.contains(&service.namespace))
                .filter(|service| selector_matches(&service.labels, &monitor.selector))
                .collect();
            if selected.is_empty() {
                out.push(format!(
                    "  {label} 的选择器在这棵树里一个 Service 都没命中：不产生目标，\
                     也不报错——抓不到与「没有请求」同形"
                ));
                continue;
            }
            for endpoint in &monitor.endpoints {
                let Some(port) = endpoint.port.as_deref() else {
                    out.push(format!(
                        "  {label} 有一个端点没写 `port` 名：ServiceMonitor 的端口只能\
                         按名字指到 Service 的 `ports[].name` 上"
                    ));
                    continue;
                };
                let providing: Vec<_> = selected
                    .iter()
                    .filter(|service| service.ports.iter().any(|p| p.name == port))
                    .collect();
                if providing.is_empty() {
                    out.push(format!(
                        "  {label} 的端点端口 `{port}` 在它选中的 Service 里没人提供：\
                         命中 {}，而它们的端口名是 {}——Prometheus Operator 对不上的\
                         端口名既不产生目标也不报错",
                        names_of(selected.iter().map(|service| format!("`{}`", service.name))),
                        names_of(selected.iter().flat_map(|service| {
                            service.ports.iter().map(|port| format!("`{}`", port.name))
                        }))
                    ));
                    continue;
                }
                let mut containers: Vec<&ContainerSpec> = Vec::new();
                for service in providing {
                    let targets: Vec<&str> = service
                        .ports
                        .iter()
                        .filter(|p| p.name == port)
                        .map(|p| p.target.as_str())
                        .collect();
                    for target in targets {
                        for workload in workloads.iter().filter(|workload| {
                            workload.namespace == service.namespace
                                && selector_matches(&workload.labels, &service.selector)
                        }) {
                            for container in workload.containers.iter().filter(|container| {
                                container.ports.iter().any(|(name, number)| {
                                    name == target || number.to_string() == target
                                })
                            }) {
                                containers.push(container);
                            }
                        }
                    }
                }
                reached.push((endpoint.path.clone(), containers));
            }
        } else {
            let selected: Vec<_> = workloads
                .iter()
                .filter(|workload| scope.contains(&workload.namespace))
                .filter(|workload| selector_matches(&workload.labels, &monitor.selector))
                .collect();
            if selected.is_empty() {
                out.push(format!(
                    "  {label} 的选择器在这棵树里一个工作负载都没命中：不产生目标，\
                     也不报错——抓不到与「没有这组指标」同形"
                ));
                continue;
            }
            for endpoint in &monitor.endpoints {
                let Some(port) = endpoint.port.as_deref() else {
                    out.push(format!(
                        "  {label} 有一个端点没写 `port` 名：PodMonitor 按容器端口名抓，\
                         缺了名字没有目标"
                    ));
                    continue;
                };
                let mut containers: Vec<&ContainerSpec> = Vec::new();
                for workload in &selected {
                    for container in &workload.containers {
                        if container
                            .ports
                            .iter()
                            .any(|(name, number)| name == port || number.to_string() == port)
                        {
                            containers.push(container);
                        }
                    }
                }
                if containers.is_empty() {
                    out.push(format!(
                        "  {label} 的端点端口 `{port}` 在它选中的 Pod 里没人声明：\
                         命中 {}，而它们容器声明的端口名是 {}——抓一个没人提供的端口，\
                         这份读数在每条查询里都不存在",
                        names_of(
                            selected
                                .iter()
                                .map(|workload| format!("`{}`", workload.name))
                        ),
                        names_of(selected.iter().flat_map(|workload| {
                            workload.containers.iter().flat_map(|container| {
                                container.ports.iter().map(|(name, _)| format!("`{name}`"))
                            })
                        }))
                    ));
                    continue;
                }
                reached.push((endpoint.path.clone(), containers));
            }
        }

        // 路径：只判跑本项目镜像的容器。
        let mut seen: Vec<String> = Vec::new();
        for (path, containers) in &reached {
            for container in containers {
                if !is_our_image(image.as_ref(), container) {
                    continue;
                }
                let identity = format!("{}/{}", container.workload, container.name);
                let key = format!("{identity} {path}");
                if seen.contains(&key) {
                    continue;
                }
                seen.push(key);
                judged.insert(identity);
                let routes = match container_routes(root, container.subcommand.as_deref(), &every) {
                    Ok(routes) => routes,
                    Err(reason) => {
                        out.push(format!(
                            "  `{}` 的 `{}` 里容器 `{}` {reason}",
                            container.manifest, container.workload, container.name
                        ));
                        continue;
                    }
                };
                if routes.contains(path) {
                    continue;
                }
                out.push(format!(
                    "  {label} 抓到 `{}` 的 `{}` 容器（{}）的 `{path}`，而那份代码注册的\
                     路径是 {}：抓取会一直 404，面板上是一条零线，与「没有请求」同形",
                    container.manifest,
                    container.workload,
                    container.name,
                    if routes.is_empty() {
                        "（一条都没有）".to_string()
                    } else {
                        routes
                            .iter()
                            .map(|route| format!("`{route}`"))
                            .collect::<Vec<_>>()
                            .join("、")
                    }
                ));
            }
        }
    }

    if in_scope == 0 {
        out.push(format!(
            "  这棵树里一个落在本命名空间内的监视器都没读到：要么清单搬了家，要么全是\
             第三方命名空间的目标，两种都按红处理（读的是 `{MANIFESTS}` 下每份 yaml 里\
             kind 为 ServiceMonitor / PodMonitor 的文档，不按文件名认）"
        ));
    }
    if judged.is_empty() {
        out.push(format!(
            "  顺着本树监视器的选择器走过去，一个跑本项目镜像的容器都没走到：\
             这条判据此刻是空转的，按红处理（镜像名取自 `{}`）",
            common::service_topology::CHART_VALUES
        ));
    }
    out
}

fn names_of<I>(names: I) -> String
where
    I: Iterator<Item = String>,
{
    let names: BTreeSet<String> = names.collect();
    if names.is_empty() {
        "（一个都没声明）".to_string()
    } else {
        names.into_iter().collect::<Vec<_>>().join("、")
    }
}

#[test]
fn a_scrape_target_names_a_port_and_path_the_workload_provides() {
    let root = repo_root();
    let found = complaints(&root, &production_sources(&root));
    assert!(
        found.is_empty(),
        "监视器指向的端口名与路径必须是工作负载真的提供的那一个；这三处各自写一遍，\
         编译器不比较，而抓不到在面板上与「没有请求」同形。\n{}",
        found.join("\n")
    );
}

// ── 夹具：临时树里放一份 chart values、一张处置表、若干监视器与工作负载 ──

fn fixture(files: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    complaints(dir.path(), &sources_of_tree(dir.path()))
}

const VALUES: &str = "image:\n  repository: registry.example/cogneva\n  tag: local\n";

const CLI_TABLE: &str = "const SUBCOMMANDS: [(&str, Command); 1] = [\n    \
(\"claim-store\", Command::ClaimStore),\n];\n";

const MAIN_ARMS: &str =
    "    match command {\n        Command::ClaimStore => cogneva::claim_store::run_from_env().await,\n    }\n";

const CLAIM_STORE: &str = "pub fn run_from_env() {\n    let app = Router::new()\n        \
.route(\"/health/live\", get(live))\n        .route(\"/metrics\", get(metrics));\n}\n";

const WORKLOAD: &str = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: claim-store\n  \
namespace: cogneva\nspec:\n  template:\n    metadata:\n      labels:\n        \
app.kubernetes.io/name: cogneva\n        app.kubernetes.io/component: claim-store\n    spec:\n      \
containers:\n        - name: claim-store\n          image: registry.example/cogneva:local\n          \
args: [\"claim-store\"]\n          ports:\n            - name: http\n              \
containerPort: 9100\n";

const SERVICE: &str = "apiVersion: v1\nkind: Service\nmetadata:\n  name: cogneva-claim-store\n  \
namespace: cogneva\n  labels:\n    app.kubernetes.io/name: cogneva\n    \
app.kubernetes.io/component: claim-store\nspec:\n  selector:\n    app.kubernetes.io/name: cogneva\n    \
app.kubernetes.io/component: claim-store\n  ports:\n    - name: http\n      port: 9100\n      \
targetPort: http\n";

fn service_monitor(port: &str, path: &str) -> String {
    format!(
        "apiVersion: monitoring.coreos.com/v1\nkind: ServiceMonitor\nmetadata:\n  name: \
cogneva-metrics\n  namespace: monitoring\nspec:\n  namespaceSelector:\n    matchNames:\n      - \
cogneva\n  selector:\n    matchLabels:\n      app.kubernetes.io/name: cogneva\n  endpoints:\n    - \
port: {port}\n      path: {path}\n"
    )
}

fn pod_monitor(port: &str, path: &str) -> String {
    format!(
        "apiVersion: monitoring.coreos.com/v1\nkind: PodMonitor\nmetadata:\n  name: cogneva-pods\n  \
namespace: monitoring\nspec:\n  namespaceSelector:\n    matchNames:\n      - cogneva\n  selector:\n    \
matchLabels:\n      app.kubernetes.io/name: cogneva\n  podMetricsEndpoints:\n    - port: {port}\n      \
path: {path}\n"
    )
}

const MONITOR_FILE: &str = "deploy/k3s/observability/manifests/04-servicemonitor-x.yaml";

fn base(monitor: &(&'static str, String)) -> Vec<(&'static str, String)> {
    vec![
        ("deploy/helm/cogneva/values.yaml", VALUES.to_string()),
        ("crates/cogneva/src/cli.rs", CLI_TABLE.to_string()),
        ("crates/cogneva/src/main.rs", MAIN_ARMS.to_string()),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE.to_string()),
        ("deploy/k3s/claim-store.yaml", WORKLOAD.to_string()),
        ("deploy/k3s/claim-store-service.yaml", SERVICE.to_string()),
        (monitor.0, monitor.1.clone()),
    ]
}

fn run(files: &[(&'static str, String)]) -> Vec<String> {
    let borrowed: Vec<(&str, &str)> = files
        .iter()
        .map(|(path, body)| (*path, body.as_str()))
        .collect();
    fixture(&borrowed)
}

#[test]
fn the_fixture_is_green_when_the_service_monitor_names_a_declared_port_and_served_path() {
    let found = run(&base(&(MONITOR_FILE, service_monitor("http", "/metrics"))));
    assert_eq!(found, Vec::<String>::new());
}

/// 选择器命中不了任何对象：装上了、应用了、什么都不抓，这是纯静的。
#[test]
fn a_monitor_whose_selector_matches_nothing_is_reported() {
    let monitor = service_monitor("http", "/metrics").replace("name: cogneva\n", "name: nobody\n");
    let found = run(&base(&(MONITOR_FILE, monitor)));
    assert!(
        found.iter().any(|c| c.contains("一个 Service 都没命中")),
        "{found:?}"
    );
}

/// 端口名漂移：这条零线的本体。报出监视器、选中的 Service 与实际端口名。
#[test]
fn a_port_name_no_selected_service_declares_is_reported() {
    let found = run(&base(&(MONITOR_FILE, service_monitor("httpz", "/metrics"))));
    assert!(
        found.iter().any(|c| c.contains("httpz")
            && c.contains("cogneva-claim-store")
            && c.contains("`http`")),
        "{found:?}"
    );
}

/// PodMonitor 走容器端口，不经 Service：同一类漂移换一个读取面。
#[test]
fn a_pod_monitor_port_the_pod_does_not_declare_is_reported() {
    let found = run(&base(&(
        "deploy/k3s/observability/manifests/11-podmonitor-x.yaml",
        pod_monitor("aof-metrics", "/metrics"),
    )));
    assert!(
        found
            .iter()
            .any(|c| c.contains("aof-metrics") && c.contains("claim-store")),
        "{found:?}"
    );
}

#[test]
fn a_pod_monitor_port_the_pod_declares_is_green() {
    let found = run(&base(&(
        "deploy/k3s/observability/manifests/11-podmonitor-x.yaml",
        pod_monitor("http", "/metrics"),
    )));
    assert_eq!(found, Vec::<String>::new());
}

/// 端口名对了、路径错了：抓取一直 404，而目标照样被算成「在抓」。
#[test]
fn a_scrape_path_the_container_does_not_serve_is_reported() {
    let found = run(&base(&(MONITOR_FILE, service_monitor("http", "/stats"))));
    assert!(
        found
            .iter()
            .any(|c| c.contains("/stats") && c.contains("/metrics")),
        "{found:?}"
    );
}

/// 第三方镜像不判路径：它的路由不在这棵树里，按红处理只会逼出一条豁免表。
///
/// 夹具里同时有一个本项目的被抓容器，否则这条测试会因为「一个本项目的被抓容器都
/// 没走到」那条下限而绿，而不是因为它没判第三方。
#[test]
fn a_third_party_target_is_not_judged_for_its_path() {
    let third_party = WORKLOAD
        .replace("name: claim-store\n  namespace", "name: nginx\n  namespace")
        .replace(
            "registry.example/cogneva:local",
            "docker.io/library/nginx:1.27",
        )
        .replace("          args: [\"claim-store\"]\n", "")
        .replace(
            "name: http\n              containerPort: 9100",
            "name: stats\n              containerPort: 9113",
        );
    let two_endpoints = "apiVersion: monitoring.coreos.com/v1\nkind: ServiceMonitor\nmetadata:\n  \
name: cogneva-metrics\n  namespace: monitoring\nspec:\n  namespaceSelector:\n    matchNames:\n      - \
cogneva\n  selector:\n    matchLabels:\n      app.kubernetes.io/name: cogneva\n  endpoints:\n    - \
port: http\n      path: /metrics\n    - port: stats\n      path: /nginx-stats\n";
    let two_ports = SERVICE.replace(
        "      targetPort: http\n",
        "      targetPort: http\n    - name: stats\n      port: 9113\n      targetPort: stats\n",
    );
    let mut files = base(&(MONITOR_FILE, two_endpoints.to_string()));
    files.retain(|(path, _)| *path != "deploy/k3s/claim-store-service.yaml");
    files.push(("deploy/k3s/claim-store-service.yaml", two_ports));
    files.push(("deploy/k3s/nginx.yaml", third_party));
    let found = run(&files);
    assert_eq!(found, Vec::<String>::new());
}

/// 处置表说这份永不交付：它里面的监视器不产生目标，判它等于判一段不上集群的字节。
#[test]
fn an_exempt_monitor_is_not_judged() {
    let mut files = base(&(MONITOR_FILE, service_monitor("httpz", "/metrics")));
    files.push((
        "deploy/k3s/observability/manifests/delivery-dispositions.txt",
        "04-servicemonitor-x.yaml exempt # 二选一的替代方案，两个都应用就是双份抓取\n\
         11-podmonitor-x.yaml deliver # 另一份抓取目标\n"
            .to_string(),
    ));
    let found = run(&files);
    // 豁免之后一个在范围内的监视器都没有了：这正是必须有下限的原因。
    assert!(
        found
            .iter()
            .any(|c| c.contains("一个落在本命名空间内的监视器都没读到")),
        "{found:?}"
    );
    assert!(
        !found.iter().any(|c| c.contains("httpz")),
        "豁免的监视器被判了：{found:?}"
    );
}

/// 第三方命名空间：这棵树不声明它，判不了。
#[test]
fn a_monitor_pointing_at_a_foreign_namespace_is_out_of_scope() {
    let monitor = service_monitor("http", "/metrics").replace(
        "    matchNames:\n      - cogneva\n",
        "    matchNames:\n      - kube-system\n",
    );
    let found = run(&base(&(MONITOR_FILE, monitor)));
    assert!(
        found
            .iter()
            .any(|c| c.contains("一个落在本命名空间内的监视器都没读到")),
        "{found:?}"
    );
    assert!(
        !found.iter().any(|c| c.contains("一个 Service 都没命中")),
        "第三方命名空间的监视器被判了：{found:?}"
    );
}

/// 读不到镜像名：分不出哪些容器跑的是本项目，路径那一半判不了。
#[test]
fn a_chart_that_names_no_image_is_not_green() {
    let mut files = base(&(MONITOR_FILE, service_monitor("http", "/metrics")));
    files.retain(|(path, _)| *path != "deploy/helm/cogneva/values.yaml");
    let found = run(&files);
    assert!(
        found.iter().any(|c| c.contains("image.repository")),
        "{found:?}"
    );
}
