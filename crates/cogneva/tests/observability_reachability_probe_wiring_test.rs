//! 可达性探针的接线：探测地址指向谁、模块名有没有人定义、这台机器该不该有探针。
//!
//! 这一族的坏法全都只表现为「一条序列都没有」，而空白与「一切正常」在面板上同形：
//! - `__address__` 指到一个不存在的 Service 名（`cog-observability-blackbox-exporter`
//!   对着清单里的 `blackbox-exporter`）：每次抓取 DNS 失败，`probe_success` 一条都
//!   不产生；
//! - 作业里点名的 `module` 在 exporter 的配置里没有：exporter 回 400，同样只有失败、
//!   没有序列；
//! - 探测作业写在基础档里而不是写在按入口形态二选一的那两份里：安装在认不出入口的
//!   机器上时它照样生效，于是「这台机器没有按形态选目标」被读成「这台机器什么都不通」。
//!
//! 所以判据按对象读，不按裸子串读：地址要解析到清单里真有的 Service 与端口，模块名
//! 要解析到 Deployment 真挂上的那份 ConfigMap 里真定义的模块，基础档里一条探测作业
//! 都不许有。任何一条不成立，装的这套探针在每个集群上都只是「安静」。

use crate::common;

use common::routes::{read_repo, repo_root};

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

const HELM_DIR: &str = "deploy/k3s/observability/helm";
const BASE_VALUES: &str = "kube-prometheus-stack-values.yaml";
const SMALL_VALUES: &str = "kube-prometheus-stack-values-small.yaml";
const PROFILE_PREFIX: &str = "kube-prometheus-stack-values-reachability-";
const MANIFEST_DIR: &str = "deploy/k3s/observability/manifests";
const INSTALL_SH: &str = "deploy/k3s/observability/scripts/install.sh";
const CLUSTER_DOMAIN: &str = ".svc.cluster.local";
const PROMETHEUS_PATH: [&str; 3] = ["prometheus", "prometheusSpec", "additionalScrapeConfigs"];

fn yaml_docs(text: &str, what: &str) -> Vec<serde_yaml::Value> {
    serde_yaml::Deserializer::from_str(text)
        .enumerate()
        .map(|(i, doc)| {
            serde_yaml::Value::deserialize(doc)
                .unwrap_or_else(|e| panic!("{what} 第 {} 份文档解析失败: {e}", i + 1))
        })
        .collect()
}

/// 可达性目标文件（按入口形态二选一的那两份），文件名排序。
fn profiles() -> Vec<String> {
    let dir = repo_root().join(HELM_DIR);
    let mut out: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(PROFILE_PREFIX) && n.ends_with(".yaml"))
        .collect();
    out.sort();
    assert!(
        !out.is_empty(),
        "{} 下一份可达性目标文件都没有：探针无处安放",
        HELM_DIR
    );
    out
}

/// values 里的 `prometheus.prometheusSpec.additionalScrapeConfigs`（没有就是空）。
fn scrape_configs(doc: &serde_yaml::Value) -> Vec<&serde_yaml::Value> {
    let mut node = doc;
    for key in PROMETHEUS_PATH {
        node = match node.get(key) {
            Some(next) => next,
            None => return Vec::new(),
        };
    }
    node.as_sequence()
        .map(|seq| seq.iter().collect())
        .unwrap_or_default()
}

fn job_name(job: &serde_yaml::Value) -> String {
    job.get("job_name")
        .and_then(|v| v.as_str())
        .unwrap_or("<无 job_name>")
        .to_string()
}

/// `blackbox-exporter.monitoring.svc.cluster.local:9115` → (name, namespace, port)。
fn parse_cluster_address(addr: &str) -> Option<(String, String, u64)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port: u64 = port.parse().ok()?;
    let host = host.strip_suffix(CLUSTER_DOMAIN)?;
    let (name, namespace) = host.rsplit_once('.')?;
    if name.is_empty() || namespace.is_empty() || name.contains('.') || namespace.contains('.') {
        return None;
    }
    Some((name.to_string(), namespace.to_string(), port))
}

/// 清单里声明的 Service：(命名空间, 名字) → 端口集合。
fn declared_services() -> BTreeMap<(String, String), BTreeSet<u64>> {
    let dir = repo_root().join(MANIFEST_DIR);
    let mut out: BTreeMap<(String, String), BTreeSet<u64>> = BTreeMap::new();
    for entry in std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", dir.display()))
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !path.to_string_lossy().ends_with(".yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
        for doc in yaml_docs(&text, &path.display().to_string()) {
            if doc.get("kind").and_then(|k| k.as_str()) != Some("Service") {
                continue;
            }
            let meta = match doc.get("metadata") {
                Some(m) => m,
                None => continue,
            };
            let name = meta
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let namespace = meta
                .get("namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            let mut ports = BTreeSet::new();
            if let Some(list) = doc
                .get("spec")
                .and_then(|s| s.get("ports"))
                .and_then(|p| p.as_sequence())
            {
                for port in list {
                    if let Some(n) = port.get("port").and_then(|v| v.as_u64()) {
                        ports.insert(n);
                    }
                }
            }
            if !name.is_empty() {
                out.entry((namespace, name)).or_default().extend(ports);
            }
        }
    }
    out
}

/// exporter 实际挂上的那份 ConfigMap，以及它定义了的模块名。
///
/// 模块名不能只从 `19-blackbox-exporter.yaml` 里随便找一份 ConfigMap 读：真正生效的
/// 是 Deployment 卷上点名的那个名字。两者对不上时，exporter 用默认配置起来，探什么
/// 都回 400——模块名全对也没用。
fn exporter_modules() -> BTreeSet<String> {
    let text = read_repo(&format!("{MANIFEST_DIR}/19-blackbox-exporter.yaml"));
    let docs = yaml_docs(&text, "19-blackbox-exporter.yaml");

    let mut configmaps: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut mounted: Vec<String> = Vec::new();
    for doc in &docs {
        let meta = match doc.get("metadata") {
            Some(m) => m,
            None => continue,
        };
        let name = meta
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let namespace = meta
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        if doc.get("kind").and_then(|k| k.as_str()) == Some("ConfigMap") {
            let body = doc
                .get("data")
                .and_then(|d| d.get("config.yml"))
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| {
                    panic!("ConfigMap {namespace}/{name} 里没有 config.yml，模块定义无处可读")
                })
                .to_string();
            configmaps.insert((namespace.clone(), name.clone()), body);
            continue;
        }

        if doc.get("kind").and_then(|k| k.as_str()) != Some("Deployment") {
            continue;
        }
        let volumes = doc
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("volumes"))
            .and_then(|v| v.as_sequence());
        let volumes = match volumes {
            Some(v) => v,
            None => panic!("Deployment {namespace}/{name} 没有卷：exporter 的配置从哪来"),
        };
        for volume in volumes {
            if let Some(cm) = volume
                .get("configMap")
                .and_then(|c| c.get("name"))
                .and_then(|v| v.as_str())
            {
                mounted.push(cm.to_string());
            }
        }
    }

    assert_eq!(
        mounted.len(),
        1,
        "exporter 应当只挂一份配置 ConfigMap，实际挂了 {mounted:?}"
    );
    let mounted_name = mounted.pop().unwrap();

    let mut parsed: BTreeSet<String> = BTreeSet::new();
    for ((_, name), body) in &configmaps {
        if *name != mounted_name {
            continue;
        }
        let cfg = yaml_docs(body, "blackbox config.yml");
        let modules = cfg
            .first()
            .and_then(|doc| doc.get("modules"))
            .and_then(|m| m.as_mapping())
            .unwrap_or_else(|| panic!("{mounted_name} 的 config.yml 里没有 modules"));
        for key in modules.keys() {
            parsed.insert(key.as_str().unwrap_or_default().to_string());
        }
    }
    assert!(
        !parsed.is_empty(),
        "exporter 挂的 {mounted_name} 里一个模块都没解析出来"
    );
    parsed
}

/// 一个探测作业点名的模块名：`static_configs[].labels.module`，或者把某枚标签写成
/// `__param_module` 的赋值。
fn job_modules(job: &serde_yaml::Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    if let Some(groups) = job.get("static_configs").and_then(|v| v.as_sequence()) {
        for group in groups {
            if let Some(module) = group
                .get("labels")
                .and_then(|l| l.get("module"))
                .and_then(|v| v.as_str())
            {
                out.insert(module.to_string());
            }
        }
    }
    if let Some(relabels) = job.get("relabel_configs").and_then(|v| v.as_sequence()) {
        for relabel in relabels {
            if relabel.get("target_label").and_then(|v| v.as_str()) != Some("__param_module") {
                continue;
            }
            if let Some(module) = relabel.get("replacement").and_then(|v| v.as_str()) {
                out.insert(module.to_string());
            }
        }
    }
    out
}

/// 一个作业把抓取地址改写成哪里（`target_label: __address__` 的 `replacement`）。
fn address_replacements(job: &serde_yaml::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(relabels) = job.get("relabel_configs").and_then(|v| v.as_sequence()) {
        for relabel in relabels {
            if relabel.get("target_label").and_then(|v| v.as_str()) != Some("__address__") {
                continue;
            }
            if let Some(replacement) = relabel.get("replacement").and_then(|v| v.as_str()) {
                out.push(replacement.to_string());
            }
        }
    }
    out
}

/// 基础档与缩配档里不许有探测作业。有第二份的下场是它只在「安装面认不出入口形态」
/// 时生效——也就是最没人盯着的那台机器上——而它指向的 Service 名字已经不在清单里了。
#[test]
fn the_base_and_small_values_carry_no_probe_job() {
    for file in [BASE_VALUES, SMALL_VALUES] {
        let text = read_repo(&format!("{HELM_DIR}/{file}"));
        let docs = yaml_docs(&text, file);
        let jobs: Vec<&serde_yaml::Value> = docs.iter().flat_map(scrape_configs).collect();
        assert!(
            jobs.is_empty(),
            "{file} 里还有 {} 条 additionalScrapeConfigs 作业（{}）：探测目标只能来自按入口形态二选一的那两份，\
             否则装到认不出入口的机器上时，它探的是别处的服务，而读数看起来像本机不通",
            jobs.len(),
            jobs.iter().map(|j| job_name(j)).collect::<Vec<_>>().join(", ")
        );
        for doc in &docs {
            assert!(
                doc.get("prometheus")
                    .and_then(|p| p.get("prometheusSpec"))
                    .and_then(|s| s.get("additionalScrapeConfigs"))
                    .is_none(),
                "{file} 的 prometheusSpec 下仍有 additionalScrapeConfigs 键"
            );
        }
    }
}

/// 每条探测作业拨的地址，必须是清单里真的声明了的 Service 与端口。
///
/// 这一类错在集群里只印一行抓取错误，`probe_success` 一条都不产生——而「没有序列」
/// 与「没有断线」在面板上同形。
#[test]
fn every_probe_job_dials_a_service_the_manifests_declare() {
    let services = declared_services();
    assert!(
        !services.is_empty(),
        "{MANIFEST_DIR} 里一个 Service 都没解析到"
    );

    let mut checked = 0usize;
    for file in profiles() {
        let text = read_repo(&format!("{HELM_DIR}/{file}"));
        let docs = yaml_docs(&text, &file);
        let jobs: Vec<&serde_yaml::Value> = docs.iter().flat_map(scrape_configs).collect();
        assert!(!jobs.is_empty(), "{file} 里一条探测作业都没有");

        for job in jobs {
            let name = job_name(job);
            let where_ = format!("{file} 的作业 {name}");
            assert_eq!(
                job.get("metrics_path").and_then(|v| v.as_str()),
                Some("/probe"),
                "{where_} 不走 /probe：探的就不是可达性"
            );
            let replacements = address_replacements(job);
            assert_eq!(
                replacements.len(),
                1,
                "{where_} 应当恰好把抓取地址改写一次（改写成 exporter），实际 {} 次: {replacements:?}",
                replacements.len()
            );
            let (svc, ns, port) = parse_cluster_address(&replacements[0]).unwrap_or_else(|| {
                panic!(
                    "{where_} 的抓取地址 {} 不是 <service>.<namespace>.svc.cluster.local:<port> 形式",
                    replacements[0]
                )
            });
            let ports = services.get(&(ns.clone(), svc.clone())).unwrap_or_else(|| {
                panic!(
                    "{where_} 把抓取地址指到 {ns}/{svc}，而清单里没有这个 Service（已有的：{:?}）",
                    services.keys().collect::<Vec<_>>()
                )
            });
            assert!(
                ports.contains(&port),
                "{where_} 拨 {ns}/{svc}:{port}，而那个 Service 只声明了端口 {ports:?}"
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "一份可达性目标文件都没检查到");
}

/// 每条探测作业点名的模块，必须在 exporter 真正挂上的那份配置里定义。
#[test]
fn every_probe_module_is_defined_by_the_exporter_it_dials() {
    let defined = exporter_modules();

    for file in profiles() {
        let text = read_repo(&format!("{HELM_DIR}/{file}"));
        let docs = yaml_docs(&text, &file);
        for job in docs.iter().flat_map(scrape_configs) {
            let name = job_name(job);
            let used = job_modules(job);
            assert!(
                !used.is_empty(),
                "{file} 的作业 {name} 没点名任何模块：exporter 会退回默认的 http_2xx，\
                 探入口时那是一个必然失败的模块"
            );
            for module in &used {
                assert!(
                    defined.contains(module),
                    "{file} 的作业 {name} 点名模块 {module}，而 exporter 的配置里只有 {defined:?}"
                );
            }
        }
    }
}

/// 每一份可达性目标文件都必须被安装面认识，且「一份都对不上」时必须明说。
///
/// 第三条坏法的另一半：新增一份入口形态的目标文件却没接到安装面上，那份文件在每台
/// 机器上都是死文本；而认不出入口时如果只是安静地不加，读数面上就是「入口与代码托管
/// 都不可达」——把配置缺席读成了事故。
#[test]
fn every_reachability_profile_is_wired_into_the_installer() {
    let install = read_repo(INSTALL_SH);
    for file in profiles() {
        assert!(
            install.contains(&file),
            "{INSTALL_SH} 不认识 {file}：这份目标在安装时永远不会生效"
        );
    }
    assert!(
        install.contains("本次不装可达性探针"),
        "{INSTALL_SH} 在认不出入口形态时必须明说「不装可达性探针」，\
         否则配置缺席会被读成入口不可达"
    );
}
