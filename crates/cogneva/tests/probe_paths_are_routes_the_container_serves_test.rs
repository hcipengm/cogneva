//! principle: feedback_no_hardcoding_config_surface
//!
//! 探针路径在两侧各写一遍：进程用 `.route("/health/live", …)` 注册它，Deployment
//! 的 `livenessProbe.httpGet.path` 按字面量去访问它。没有任何东西比较这两处。
//!
//! 漂移的后果与「Pod 还没起来」是同一个读数：探针打到一个没人服务的路径，kubelet
//! 收到 404，容器一直不 ready，滚动端把这一轮判成启动失败。而两侧各自都「对」——
//! 代码编译通过，清单也 apply 成功。`deploy/scripts` 下那份控制面可达性门的头注里
//! 明写了这件事在它判据之外：「探针的路径对不对」不在它判的六族里，它判的是对象与
//! 对象之间的关系，不是路径与应用之间的关系。
//!
//! 判什么：
//!
//! * 每个跑本项目镜像的容器，它的 `httpGet` 探针路径必须是**这个容器起的那份代码**
//!   注册过的路由；
//! * 读的清单是**会被投递出去的每一棵树**：`deploy/k3s`（元启动 apply 的字节）与
//!   `deploy/rendered` 下的每个 profile（`helm install` 的字节）。profile 目录由
//!   目录枚举出来而不是在这里点名，新加一个 profile 不需要改这道门；
//! * 「这个容器起的是哪份代码」由 [`common::service_topology`] 读一次（子命令词表
//!   来自 `cli.rs`、变体 → 模块来自 `main.rs`），本文件不另写一份；抓取目标那道门
//!   读的是同一份。
//!
//! 判不了什么：
//!
//! * 第三方镜像容器的探针（crawl4ai 的 `/health`、registry 的 `/v2/`、nats 的
//!   `/healthz`）：它们的路由不在这棵树里，读不到就不判；
//! * 主应用（不带子命令、走镜像入口的那一个）注册了什么：它的路由由插件在运行时
//!   装配，静态读不到「哪个插件挂了哪条路由」，所以对这份代码只要求「这棵树里
//!   某处注册过它」——比旁的分支宽一档，宽在哪儿写在这里；
//! * 探针的端口对不对（那是 reachability 门第 4 族的事）、探针的时机与阈值；
//! * chart 模板本身（带 `{{ }}` 的不是合法 YAML）：模板里写错要到重新渲染之后才在
//!   这道门上出现，而「渲染产物是不是最新」是新鲜度门的事；parity 只比对
//!   卷/挂载/端口/env/SA，不比探针，所以两棵树之间的探针分叉同样由这道门接住。
//!
//! 普查要有下限：读不到 chart 的 `image.repository`、一个跑本项目镜像且带
//! `httpGet` 探针的容器都没找到、或这棵树里一条路由都没读到时，一律按红处理——
//! 「没读到」与「没有漂移」在结果上分不开。
//!
//! 消融（真实树）：把 `deploy/k3s/claim-store-daemonset.yaml` 的 readinessProbe
//! 路径 `/health/ready` 改成 `/health/readyz`，本门红，点名该文件、claim-store
//! 容器与 `claim_store.rs` 实际注册过的路径；改回绿。子命令解析、「只认生产字节」
//! 与预渲染 profile 这三个分支由夹具守着
//! （`a_subcommand_the_cli_does_not_declare_is_reported`、
//! `a_route_registered_only_in_a_test_block_is_not_served`、
//! `a_rendered_profile_with_a_stale_probe_path_is_reported`）——真实树上四棵树
//! 此刻彼此一致，把它们轮流改红只证明同一件事。

mod common;

use std::path::{Path, PathBuf};

use common::routes::repo_root;
use common::rust_production::{production_sources, sources_of_tree};
use common::service_topology::{container_routes, our_image, routes_of, workloads};

fn complaints(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut out = Vec::new();
    let Some(image) = our_image(root) else {
        out.push(format!(
            "  读不到 `{}` 里 `image.repository` 的取值：读不出来按红处理",
            common::service_topology::CHART_VALUES
        ));
        return out;
    };
    let every = routes_of(root, files);
    if every.is_empty() {
        out.push(
            "  这棵树的 `crates/*/src` 里一条 `.route(\"…\")` 都没读到：读不出来按红处理"
                .to_string(),
        );
    }

    let judged: Vec<_> = workloads(root)
        .into_iter()
        .flat_map(|workload| workload.containers)
        .filter(|container| container.image.starts_with(&image))
        .filter(|container| !container.probes.is_empty())
        .collect();
    if judged.is_empty() {
        out.push(format!(
            "  `{}` 与 `{}` 下每个 profile 里，一个跑本项目镜像（`{image}`）且带 \
             `httpGet` 探针的容器都没读到：要么清单搬了家，要么探针换了形状，两种都按红处理",
            common::service_topology::APPLY_TREE,
            common::service_topology::RENDERED
        ));
        return out;
    }

    for container in judged {
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
        for (probe, path) in &container.probes {
            if routes.contains(path) {
                continue;
            }
            out.push(format!(
                "  `{}` 的 `{}` 里容器 `{}`{} 的 `{probe}` 访问 `{path}`，而那份代码注册的\
                 路径是 {}：探针会一直收到 404，容器永远不 ready，而两侧各自都是「对」的\
                 ——代码编译通过，清单也 apply 成功",
                container.manifest,
                container.workload,
                container.name,
                match &container.subcommand {
                    Some(word) => format!("（子命令 `{word}`）"),
                    None => String::new(),
                },
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
    out
}

#[test]
fn a_probe_path_is_a_route_the_container_serves() {
    let root = repo_root();
    let found = complaints(&root, &production_sources(&root));
    assert!(
        found.is_empty(),
        "静态清单里每个探针路径都必须是那个容器起的代码注册过的路由；这两处各自写一遍，\
         编译器不比较。\n{}",
        found.join("\n")
    );
}

// ── 夹具：临时树里放一份 chart values、一张子命令表、一份分发表与一份清单 ──

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
.route(\"/health/live\", get(live))\n        .route(\"/health/ready\", get(ready));\n}\n";

fn workload(image: &str, args: &str, probes: &[(&str, &str)]) -> String {
    let mut body = format!(
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: claim-store\nspec:\n  \
         template:\n    spec:\n      containers:\n        - name: claim-store\n          \
         image: {image}\n"
    );
    if !args.is_empty() {
        body.push_str(&format!("          args: [\"{args}\"]\n"));
    }
    for (probe, path) in probes {
        body.push_str(&format!(
            "          {probe}:\n            httpGet:\n              path: {path}\n              \
             port: http\n"
        ));
    }
    body
}

#[test]
fn the_fixture_is_green_when_the_probe_asks_for_a_route_the_component_registers() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[
            ("livenessProbe", "/health/live"),
            ("readinessProbe", "/health/ready"),
        ],
    );
    assert_eq!(
        fixture(&[
            ("deploy/helm/cogneva/values.yaml", VALUES),
            ("crates/cogneva/src/cli.rs", CLI_TABLE),
            ("crates/cogneva/src/main.rs", MAIN_ARMS),
            ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
            ("deploy/k3s/claim-store.yaml", &manifest),
        ]),
        Vec::<String>::new()
    );
}

/// 判据落在**这个容器**起的那份代码上，不是「这棵树里有没有人服务它」：另一个
/// 组件的路由不该让这条探针变绿。
#[test]
fn a_probe_asking_for_another_components_route_is_reported() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/other")],
    );
    let elsewhere = "pub fn run() {\n    let app = Router::new().route(\"/health/other\", \
                     get(other));\n}\n";
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("crates/cogneva/src/other.rs", elsewhere),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(
        found
            .iter()
            .any(|c| c.contains("/health/other") && c.contains("claim-store")),
        "{found:?}"
    );
}

/// 预渲染 profile 是 `helm install` 投递的那份字节：它里面的探针路径同样要判。
/// 只读 `deploy/k3s` 时，改 chart 模板后重新渲染出来的那一份没人比。
#[test]
fn a_rendered_profile_with_a_stale_probe_path_is_reported() {
    let applied = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/ready")],
    );
    let rendered = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/readyz")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/claim-store-daemonset.yaml", &applied),
        ("deploy/rendered/k3s-single/claim-store.yaml", &rendered),
    ]);
    assert!(
        found
            .iter()
            .any(|c| c.contains("/health/readyz") && c.contains("k3s-single")),
        "{found:?}"
    );
    assert!(
        !found.iter().any(|c| c.contains("claim-store-daemonset")),
        "apply 路径那一份是对的，不该被牵连：{found:?}"
    );
}

/// `#[cfg(test)]` 里注册的路由没有进程会起：读进来就是假绿。
#[test]
fn a_route_registered_only_in_a_test_block_is_not_served() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/ready")],
    );
    let with_tests = "pub fn run_from_env() {\n    let app = Router::new()\n        \
                      .route(\"/health/live\", get(live));\n}\n\n#[cfg(test)]\nmod tests {\n    \
                      fn app() -> Router {\n        Router::new().route(\"/health/ready\", \
                      get(ready))\n    }\n}\n";
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", with_tests),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(
        found
            .iter()
            .any(|c| c.contains("/health/ready") && c.contains("claim-store")),
        "{found:?}"
    );
}

/// 第三方镜像的探针不判：它的路由不在这棵树里，按红处理只会逼出一条豁免表。
#[test]
fn a_container_running_another_image_is_not_judged() {
    let manifest = workload(
        "docker.io/library/nginx:1.27",
        "",
        &[("livenessProbe", "/health")],
    );
    let ours = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/nginx.yaml", &manifest),
        ("deploy/k3s/claim-store.yaml", &ours),
    ]);
    assert!(found.is_empty(), "{found:?}");
}

/// 一个本项目镜像的容器都没找到时不能算绿：清单搬了家、镜像名对不上，都会读成
/// 「没有容器」，而「没有容器」与「没有漂移」在结果上分不开。
#[test]
fn a_tree_with_no_container_running_the_project_image_is_not_green() {
    let manifest = workload(
        "docker.io/library/nginx:1.27",
        "",
        &[("livenessProbe", "/health")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/nginx.yaml", &manifest),
    ]);
    assert!(found.iter().any(|c| c.contains("都没读到")), "{found:?}");
}

/// 子命令词不在 `SUBCOMMANDS` 里：进程启动即 exit 2，容器起不来。
#[test]
fn a_subcommand_the_cli_does_not_declare_is_reported() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store-typo",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(found.iter().any(|c| c.contains("SUBCOMMANDS")), "{found:?}");
}

/// 读不到镜像名时按红：此时每个容器都会被判成「不是本项目的」，结果是空集。
#[test]
fn a_chart_that_names_no_image_is_not_green() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(
        found.iter().any(|c| c.contains("image.repository")),
        "{found:?}"
    );
}

/// 主应用没有子命令：判据放宽到「这棵树里某处注册过」，宽在明处。
#[test]
fn the_main_application_is_judged_against_every_route_in_the_tree() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/deployment.yaml", &manifest),
    ]);
    assert!(found.is_empty(), "{found:?}");
}
