//! 通知出口的可达性：插件能注册的每个出口，部署面都得有一条给它地址的通路。
//!
//! 要修的坏法是「dispatcher 在代码里、配置面没有它的名字」——钉钉／飞书／企微三个
//! 平台机器人曾经就是这样：四个出口都能挂上，只有通用 webhook 有一条配置路径，另外
//! 三个永远挂不上。这种坏法在运行期没有读数：插件只会报「没有出口」，而那是运维没配
//! 地址的合法状态，两者长得一模一样。
//!
//! 判据从**生产面**取：出口清单来自 `cog-notification` 自己的表，不是本文件再抄一份。
//! 每个出口要同时满足三件事，缺任何一环通路就断在不同的地方：
//!
//! 1. 配置文档的 `env` 映射里有且只有一个键指向它的地址路径——这是部署面给这个出口
//!    起的名字；
//! 2. chart 的 ConfigMap 把这个名字送进 Pod，且取值来自 `.Values`——写死的名字送不进
//!    地址，只有名字没有值等于没有；
//! 3. `values.yaml` 里真的有那个键——引用一个不存在的 `.Values` 键渲染成空串而不报错。
//!
//! 反向（部署面写了插件不读的路径）不在这里判：`gateway` 是具体结构体，落到不存在的
//! 字段上的映射由 `config_loader` 的映射门禁判，重造一条判据只会多一处会分叉的名单。

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cog_notification::plugin::OUTLET_ADDRESS_PATHS;

const CHART_DOC: &str = "deploy/helm/cogneva/files/cogneva.json";
const CHART_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";
const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const VALUES_BLOCK: &str = "notification:";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// 配置文档自己的 `env` 映射：环境变量名 -> 点分路径。
fn env_map() -> BTreeMap<String, String> {
    let doc: serde_json::Value =
        serde_json::from_str(&read(CHART_DOC)).expect("chart 的 cogneva.json 不是合法 JSON");
    doc["env"]
        .as_object()
        .expect("配置文档带 env 映射")
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str()
                    .unwrap_or_else(|| panic!("{k} 映射到非字符串目标：{v}"))
                    .to_string(),
            )
        })
        .collect()
}

/// 缩进块里的标量键（chart values 的手写风格）：块头之后、缩进更深的行为成员。
fn yaml_block(text: &str, header: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut header_indent = None;
    for line in text.lines() {
        if line.trim_end() == header {
            header_indent = Some(line.len() - line.trim_start().len());
            continue;
        }
        let Some(indent) = header_indent else {
            continue;
        };
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let line_indent = line.len() - line.trim_start().len();
        if line_indent <= indent {
            break;
        }
        if let Some((k, v)) = line.trim().split_once(':') {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    out
}

/// 从 ConfigMap 模板的一行里取出它引用的 values 键。
fn values_key_of(line: &str) -> Option<&str> {
    let tail = line.split_once(".Values.notification.")?.1;
    let end = tail
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(tail.len());
    (end > 0).then(|| &tail[..end])
}

/// 判据自身的下限：表里没有出口、或者两个出口共用一条路径，后面的检查会一起空转。
#[test]
fn the_outlet_table_is_a_usable_premise() {
    assert!(
        !OUTLET_ADDRESS_PATHS.is_empty(),
        "出口表为空，可达性检查会静默通过"
    );
    let outlets: BTreeSet<&str> = OUTLET_ADDRESS_PATHS.iter().map(|(o, _)| *o).collect();
    let paths: BTreeSet<&str> = OUTLET_ADDRESS_PATHS.iter().map(|(_, p)| *p).collect();
    assert_eq!(
        outlets.len(),
        OUTLET_ADDRESS_PATHS.len(),
        "出口名有重复，其中一个平台的告警会被另一个顶掉"
    );
    assert_eq!(
        paths.len(),
        OUTLET_ADDRESS_PATHS.len(),
        "两个出口声明了同一条地址路径，其中一个永远读不到地址"
    );
}

/// 每个出口都得从部署面拿到地址：配置文档给名字、ConfigMap 送名字、values 存值。
#[test]
fn every_outlet_the_plugin_can_register_is_reachable_from_the_deploy_surface() {
    let env = env_map();
    let template = read(CHART_CONFIGMAP);
    let values = yaml_block(&read(VALUES_YAML), VALUES_BLOCK);

    for (outlet, path) in OUTLET_ADDRESS_PATHS {
        let names: Vec<&String> = env
            .iter()
            .filter(|(_, target)| target.as_str() == path)
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            names.len(),
            1,
            "出口 {outlet} 的地址路径 {path} 在 {CHART_DOC} 的 env 映射里出现 {} 次（应为 1 次）",
            names.len()
        );
        let name = names[0];

        let line = template
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{name}:")))
            .unwrap_or_else(|| {
                panic!("{CHART_CONFIGMAP} 没有把 {name} 送进 Pod：出口 {outlet} 挂不上")
            });
        let key = values_key_of(line).unwrap_or_else(|| {
            panic!("{CHART_CONFIGMAP} 里 {name} 不取自 .Values：地址没有可设的入口")
        });
        assert!(
            values.contains_key(key),
            "{VALUES_YAML} 的 {VALUES_BLOCK} 块里没有 {key}：{name} 渲染成空串，出口 {outlet} 永远挂不上"
        );
    }
}
