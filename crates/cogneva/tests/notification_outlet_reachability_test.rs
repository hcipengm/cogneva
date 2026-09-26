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
//!
//! 地址配得上还有第四环，它是**同一个症状的另一条路**：进化 Pod 的 egress 是白名单，
//! 地址落在集群外时没有放行，通知照样发不出去，而现象与前三环断掉时一模一样（一行
//! warn、看起来像没有告警）。这一环由 `notification.egress` 承载，键就是宿主的名字，
//! 判据把「配了地址的宿主」与「放行的宿主」两个方向都对一遍——单值旋钮表达不了多个
//! 宿主，而四个出口可以落在四个宿主上。
//!
//! 这条判据的上界要说清：模板只认地址，仓库里没有 DNS，所以判不了那段 `cidr` 是不是
//! 这个宿主真实的地址段。那一半的兜底是投递读数——放行写错时
//! `cogneva_notification_delivery_total{result="unreachable"}` 会说这个出口根本没通。

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cog_notification::plugin::OUTLET_ADDRESS_PATHS;

const CHART_DOC: &str = "deploy/helm/cogneva/files/cogneva.json";
const CHART_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";
const CHART_NETPOL: &str = "deploy/helm/cogneva/templates/network-policy.yaml";
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

/// 一个出口在部署面上走到地址的那条链，走完的产物。
struct OutletWiring {
    outlet: &'static str,
    env_name: String,
    values_key: String,
    /// values 里写的地址，原样（空串 = 这个出口没配）。
    address: String,
}

/// 走完 env 名 → ConfigMap → values 键这条链，逐环点名。四个出口共用一次走查，
/// 因为「配得上地址」与「地址被放行」判的是同一件事的两个面，各自抄一遍必然分叉。
fn outlet_wiring() -> Vec<OutletWiring> {
    let env = env_map();
    let template = read(CHART_CONFIGMAP);
    let values = yaml_block(&read(VALUES_YAML), VALUES_BLOCK);

    OUTLET_ADDRESS_PATHS
        .iter()
        .map(|(outlet, path)| {
            let names: Vec<&String> = env
                .iter()
                .filter(|(_, target)| target.as_str() == *path)
                .map(|(name, _)| name)
                .collect();
            assert_eq!(
                names.len(),
                1,
                "出口 {outlet} 的地址路径 {path} 在 {CHART_DOC} 的 env 映射里出现 {} 次（应为 1 次）",
                names.len()
            );
            let env_name = names[0].clone();

            let line = template
                .lines()
                .find(|l| l.trim_start().starts_with(&format!("{env_name}:")))
                .unwrap_or_else(|| {
                    panic!("{CHART_CONFIGMAP} 没有把 {env_name} 送进 Pod：出口 {outlet} 挂不上")
                });
            let key = values_key_of(line)
                .unwrap_or_else(|| {
                    panic!("{CHART_CONFIGMAP} 里 {env_name} 不取自 .Values：地址没有可设的入口")
                })
                .to_string();
            let address = values.get(&key).unwrap_or_else(|| {
                panic!(
                    "{VALUES_YAML} 的 {VALUES_BLOCK} 块里没有 {key}：{env_name} 渲染成空串，\
                     出口 {outlet} 永远挂不上"
                )
            });
            OutletWiring {
                outlet,
                env_name,
                values_key: key,
                address: address.trim_matches('"').to_string(),
            }
        })
        .collect()
}

/// `notification.egress`：宿主名 → 该项的字段。键就是宿主，这一项放行的是谁没有第二个
/// 说法；注释行跳过，`{}` 与 `[]` 都按空表算。
///
/// 项与字段的分界取自各自的缩进相对第一项的层次，不写死步长：一份缩进四格的 values
/// 不该让这条判据静默读空。
fn egress_entries(values: &str) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut block_indent: Option<usize> = None;
    let mut egress_indent: Option<usize> = None;
    let mut item_indent: Option<usize> = None;
    let mut current: Option<String> = None;

    for line in values.lines() {
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        let body = line.trim_end().trim_start();

        let Some(block) = block_indent else {
            if line.trim_end() == VALUES_BLOCK {
                block_indent = Some(indent);
            }
            continue;
        };
        let Some(egress) = egress_indent else {
            if indent <= block {
                // The block ended before the allow list was found.
                break;
            }
            if let Some(rest) = body.strip_prefix("egress:") {
                let rest = rest.trim();
                assert!(
                    rest == "{}" || rest == "[]" || rest.is_empty(),
                    "{VALUES_YAML} 的 egress 既不是映射也不是空表：{rest}"
                );
                egress_indent = Some(indent);
            }
            continue;
        };
        if indent <= egress {
            break;
        }
        let items = *item_indent.get_or_insert(indent);
        assert!(
            indent >= items,
            "放行项 {body} 的缩进比前一项浅：这一项会被读成上一项的字段"
        );
        if indent == items {
            let host = body
                .strip_suffix(':')
                .unwrap_or_else(|| panic!("放行项 {body} 不是 `宿主:` 形状"))
                .trim()
                .to_string();
            assert!(
                out.insert(host.clone(), BTreeMap::new()).is_none(),
                "放行表里 {host} 出现了两次：策略里多一条同宿主的规则，读的人分不清哪条是准的"
            );
            current = Some(host);
            continue;
        }
        let (key, value) = body
            .split_once(':')
            .unwrap_or_else(|| panic!("放行项字段 {body} 不是 `键: 值` 形状"));
        let host = current.clone().expect("a field before any host");
        out.get_mut(&host).expect("host inserted above").insert(
            key.trim().to_string(),
            value.trim().trim_matches('"').to_string(),
        );
    }
    out
}

/// 地址 URL 的宿主与端口：方案默认端口在没写端口时生效，宿主按 DNS 规则小写化。
fn host_and_port(url: &str) -> (String, u16) {
    let (scheme, rest) = url.split_once("://").unwrap_or(("https", url));
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let port_of = |scheme: &str| {
        if scheme.eq_ignore_ascii_case("http") {
            80
        } else {
            443
        }
    };
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        // An IPv6 literal: the colons inside the brackets are not a port separator.
        match rest.split_once(']') {
            Some((host, tail)) => (
                host.to_string(),
                tail.trim_start_matches(':')
                    .parse::<u16>()
                    .unwrap_or_else(|_| port_of(scheme)),
            ),
            None => (authority.to_string(), port_of(scheme)),
        }
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => (
                host.to_string(),
                port.parse::<u16>().unwrap_or_else(|_| port_of(scheme)),
            ),
            _ => (authority.to_string(), port_of(scheme)),
        }
    };
    (host.to_ascii_lowercase(), port)
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
    let wiring = outlet_wiring();
    assert_eq!(
        wiring.len(),
        OUTLET_ADDRESS_PATHS.len(),
        "走查覆盖的出口数与生产面的表不一致"
    );
    for w in &wiring {
        assert!(
            !w.values_key.is_empty() && !w.env_name.is_empty(),
            "出口 {} 的名字链是空的",
            w.outlet
        );
    }
}

/// 配了地址的宿主与放行的宿主必须是同一组，端口也要一致——两个方向都判。
///
/// 记下的病史是「四个出口共用一个单值旋钮」：出口从一个涨到四个之后，多个不同宿主的
/// 放行表达不出来，而症状与地址没配时一样（一行 warn、看起来像没有告警）。反向也要
/// 判：放行了一个没人用的宿主是一份看不懂的授权，读的人分不清它是不是还有用。
#[test]
fn the_egress_allow_list_pairs_one_to_one_with_the_configured_addresses() {
    let allowed = egress_entries(&read(VALUES_YAML));

    let mut configured: BTreeMap<String, (&'static str, u16)> = BTreeMap::new();
    for w in outlet_wiring() {
        if w.address.trim().is_empty() {
            continue;
        }
        let (host, port) = host_and_port(&w.address);
        assert!(
            configured.insert(host.clone(), (w.outlet, port)).is_none(),
            "两个出口配了同一个宿主 {host}：放行是按宿主配的，一个出口会被另一个顶掉"
        );
    }

    let configured_hosts: BTreeSet<&String> = configured.keys().collect();
    let allowed_hosts: BTreeSet<&String> = allowed.keys().collect();
    assert_eq!(
        configured_hosts,
        allowed_hosts,
        "配了地址的宿主与放行的宿主不是同一组。有地址没放行 = 通知被 egress 白名单拦掉、\
         只有一行 warn；放行了没人用 = 一份读不出用途的授权。差集：有地址没放行 {:?} / \
         放行了没人用 {:?}",
        configured_hosts
            .difference(&allowed_hosts)
            .collect::<Vec<_>>(),
        allowed_hosts
            .difference(&configured_hosts)
            .collect::<Vec<_>>()
    );

    for (host, (outlet, port)) in &configured {
        let entry = &allowed[host];
        assert_eq!(
            entry.get("port").and_then(|p| p.parse::<u16>().ok()),
            Some(*port),
            "出口 {outlet}：宿主 {host} 的放行端口与地址里的端口不一致——策略按端口放行，\
             端口写错与没放行是同一件事"
        );
        assert!(
            entry.get("cidr").is_some_and(|c| !c.trim().is_empty()),
            "宿主 {host} 的放行没有 cidr：这一项渲染出一条空地址的规则"
        );
    }
}

/// 判据自身的下限：解析器要读得出一份填好的放行表。它读不出来时上面那条配对判据会在
/// 「两个集合都空」的情况下恒真，而那正是默认部署的样子。
#[test]
fn the_egress_parser_reads_a_filled_table() {
    let empty = "notification:\n  webhookUrl: \"\"\n  egress: {}\n  ingress:\n    enabled: true\n";
    assert!(egress_entries(empty).is_empty());

    let filled = concat!(
        "notification:\n",
        "  webhookUrl: \"https://hook.example.invalid/x\"\n",
        "  egress:\n",
        "    #  oapi.dingtalk.com:\n",
        "    #    cidr: \"1.2.3.0/24\"\n",
        "    oapi.dingtalk.com:\n",
        "      cidr: \"1.2.3.0/24\"\n",
        "      port: 443\n",
        "    hook.example.invalid:\n",
        "      cidr: \"203.0.113.7/32\"\n",
        "      port: 8443\n",
        "  ingress:\n",
        "    enabled: true\n"
    );
    let parsed = egress_entries(filled);
    assert_eq!(
        parsed.keys().collect::<Vec<_>>(),
        vec!["hook.example.invalid", "oapi.dingtalk.com"],
        "注释掉的那一项不该进来，块外的键也不该进来"
    );
    assert_eq!(parsed["oapi.dingtalk.com"]["cidr"], "1.2.3.0/24");
    assert_eq!(parsed["oapi.dingtalk.com"]["port"], "443");
    assert_eq!(parsed["hook.example.invalid"]["port"], "8443");
}

/// 地址里没写端口时按方案取默认端口，宿主按 DNS 规则小写——判据两侧拿到的名字要能对上。
#[test]
fn a_url_without_a_port_uses_its_scheme_default() {
    assert_eq!(
        host_and_port("https://oapi.dingtalk.com/robot/send?access_token=x"),
        ("oapi.dingtalk.com".to_string(), 443)
    );
    assert_eq!(
        host_and_port("http://hook.example.invalid:8080/path"),
        ("hook.example.invalid".to_string(), 8080)
    );
    assert_eq!(
        host_and_port("https://Hook.Example.INVALID/hook"),
        ("hook.example.invalid".to_string(), 443)
    );
}

/// values 里的放行要真进得去策略，而且进的是**进化 Pod** 那条——只有它的 egress 是
/// 白名单，放行落到别的策略上等于没放。
#[test]
fn the_egress_entries_reach_the_evolution_egress_policy() {
    let template = read(CHART_NETPOL);
    let evolution = template
        .split("name: cogneva-evolution-deny-egress")
        .nth(1)
        .and_then(|rest| rest.split("\n---").next())
        .unwrap_or_else(|| panic!("{CHART_NETPOL} 里找不到进化 Pod 的 egress 策略"));

    let (_, rest) = evolution
        .split_once("range $host, $allow := .Values.notification.egress")
        .unwrap_or_else(|| {
            panic!(
                "{CHART_NETPOL} 的进化 Pod 策略没有遍历 notification.egress：\
                 values 里配的放行进不了策略"
            )
        });
    let body = rest.split("{{- end }}").next().unwrap_or(rest);
    for field in ["$host", "$allow.cidr", "$allow.port"] {
        assert!(
            body.contains(field),
            "放行块的规则体里没有用到 {field}：那一项渲染出来是空的"
        );
    }
}
