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
//!
//! **告警出口（`cog-observability` 的 alertmanager 段）与通知出口同判。** 它们是
//! 同一个坏法的两个实例：dispatcher 在代码里、配置面没有它的名字（Email/Slack 两条
//! 分派臂就是这样挂了很久）。两处共用这一份走查，因为要判的都是"部署面能不能写到
//! 这个出口的开关"，而且**共用同一条 egress 白名单**——进化 Pod 上两套出口走同一个
//! 策略。放行表只解析一份、只配对一个方向，是为了不让两个消费者各自抄一份配对逻辑
//! 再各自漂移。
//!
//! 告警出口与通知出口的链路长度不同，这一点不掩盖：通知的地址要经配置文档的 `env`
//! 映射（`gateway.notification_*`）再进 ConfigMap；告警的地址由进程自己读
//! `COGNEVA_ALERTMANAGER_*`（`OBS_ENV`），链路是"env 名 → ConfigMap/Secret → values"，
//! 少一跳。缺的那一跳由 `every_alertmanager_env_key_reaches_a_deployment` 按生产面的
//! 表反向兜住：`OBS_ENV` 里的每个键都得有一个部署面来源。

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cog_notification::plugin::OUTLET_ADDRESS_PATHS;
use cog_observability::config::{
    ALERT_CHANNEL_CONFIG_PATHS, ALERT_CHANNEL_CREDENTIAL_PATH, OBS_ENV,
};

const CHART_DOC: &str = "deploy/helm/cogneva/files/cogneva.json";
const CHART_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";
const CHART_NETPOL: &str = "deploy/helm/cogneva/templates/network-policy.yaml";
const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const VALUES_BLOCK: &str = "notification:";

/// 告警出口的 values 段与它的键前缀。
const ALERT_VALUES_BLOCK: &str = "alertmanager:";
const ALERT_VALUES_PREFIX: &str = ".Values.alertmanager.";

/// 跑告警桥的 Pod：主应用与进化 Pod（两处都发告警，各自的 env 要齐）。
const ALERT_POD_TEMPLATES: [&str; 2] = [
    "deploy/helm/cogneva/templates/gateway.yaml",
    "deploy/helm/cogneva/templates/evolution.yaml",
];

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
    values_key_after(line, ".Values.notification.")
}

/// 同上，但前缀由调用方给：告警出口的键在 `alertmanager` 段下。
fn values_key_after<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    let tail = line.split_once(prefix)?.1;
    let end = tail
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
        .unwrap_or(tail.len());
    (end > 0).then(|| &tail[..end])
}

/// 嵌套段里的标量键，展开成点分路径（`email.smtpHost`）。
///
/// 通知出口那几个键是平的一层，告警出口的键是两层的（`email` / `slack` 各一
/// 小组），所以这一份按缩进跟栈展开，不写死层数：一份多一层缩进的 values 不该
/// 让判据静默读空。
fn nested_values(text: &str, header: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut header_indent = None;
    let mut stack: Vec<(usize, String)> = Vec::new();
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
        let body = line.trim_end().trim_start();
        let Some((key, value)) = body.split_once(':') else {
            continue;
        };
        while stack.last().is_some_and(|(i, _)| *i >= line_indent) {
            stack.pop();
        }
        let value = value.trim();
        if value.is_empty() {
            stack.push((line_indent, key.trim().to_string()));
            continue;
        }
        let mut path: Vec<&str> = stack.iter().map(|(_, k)| k.as_str()).collect();
        path.push(key.trim());
        out.insert(path.join("."), value.trim_matches('"').to_string());
    }
    out
}

/// Pod 模板里某个 env 名从 Secret 的哪个键取值。
fn secret_key_ref(template: &str, env_name: &str) -> Option<String> {
    let lines: Vec<&str> = template.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.trim_start().starts_with(&format!("- name: {env_name}")))?;
    for line in lines.iter().skip(start).take(6) {
        if let Some(rest) = line.trim().strip_prefix("key: ") {
            return Some(rest.trim().to_string());
        }
    }
    None
}

/// 写某个配置路径的 env 名。生产面的表说了每个键写哪儿，这里按路径反查——
/// 反查不到或查到多个时判据会空转，所以查空要在调用处响亮报错。
fn env_key_for(path: &str) -> Vec<&'static str> {
    OBS_ENV
        .iter()
        .filter(|(_, target)| *target == path)
        .map(|(name, _)| *name)
        .collect()
}

/// 一个告警出口的某个配置路径，在部署面上取值的那一环。
#[derive(Debug)]
enum AlertSource {
    /// chart ConfigMap 里的一行，取自 values 的这个键。
    Value(String),
    /// Pod env 里的一条 `secretKeyRef`，取自 Secret 的这个键。
    Secret(String),
}

/// 告警出口的走查产物：一个配置路径 + 给它赋值的 env 名 + 这一环的取值处。
#[derive(Debug)]
struct AlertWiring {
    channel: &'static str,
    path: &'static str,
    source: AlertSource,
}

/// 走完"配置路径 → OBS_ENV 的 env 名 → ConfigMap/Secret → values"这条链。
///
/// 与通知出口不同，告警出口的地址不经配置文档的 `env` 映射（进程自己读
/// `COGNEVA_ALERTMANAGER_*`），所以这一条少一跳，形状也少一环：有 ConfigMap 行就
/// 一路查到 values 键，没有就得是 Pod 上的 Secret 取值——两者都没有，这个键谁都不送。
fn alert_wiring() -> Vec<AlertWiring> {
    let template = read(CHART_CONFIGMAP);
    let values = nested_values(&read(VALUES_YAML), ALERT_VALUES_BLOCK);

    ALERT_CHANNEL_CONFIG_PATHS
        .iter()
        .map(|(channel, path)| {
            let names: Vec<&str> = OBS_ENV
                .iter()
                .filter(|(_, target)| target == path)
                .map(|(name, _)| *name)
                .collect();
            assert_eq!(
                names.len(),
                1,
                "告警出口 {channel} 的配置路径 {path} 在 cog-observability 的 env 表里出现 {} 次（应为 1 次）",
                names.len()
            );
            let env_name = names[0].to_string();
            assert!(
                env_name.starts_with("COGNEVA_"),
                "出口 {channel} 的 {path} 由 {env_name} 赋值：进程读的是 COGNEVA_ 前缀的表，\
                 别的名字写进去没人读"
            );

            let source = match template
                .lines()
                .find(|l| l.trim_start().starts_with(&format!("{env_name}:")))
            {
                Some(line) => {
                    let key = values_key_after(line, ALERT_VALUES_PREFIX)
                        .unwrap_or_else(|| {
                            panic!(
                                "{CHART_CONFIGMAP} 里 {env_name} 不取自 .Values.alertmanager：\
                                 地址没有可设的入口"
                            )
                        })
                        .to_string();
                    assert!(
                        values.contains_key(&key),
                        "{VALUES_YAML} 的 {ALERT_VALUES_BLOCK} 段里没有 {key}：{env_name} \
                         渲染成空串，出口 {channel} 永远挂不上"
                    );
                    AlertSource::Value(key)
                }
                None => {
                    let keys: Vec<String> = ALERT_POD_TEMPLATES
                        .iter()
                        .filter_map(|t| secret_key_ref(&read(t), &env_name))
                        .collect();
                    assert!(
                        !keys.is_empty(),
                        "{env_name} 既不在 {CHART_CONFIGMAP} 里，也不在任何 Pod 模板的 \
                         secretKeyRef 里：这个键没人送进 Pod，出口 {channel} 的 {path} 永远读不到"
                    );
                    assert_eq!(
                        *path, ALERT_CHANNEL_CREDENTIAL_PATH,
                        "{env_name} 从 Secret 取值，但它不是凭证（{path}）——凭据以外的东西\
                         走 Secret 会让运维在一个看不见的地方找它的开关"
                    );
                    AlertSource::Secret(keys[0].clone())
                }
            };
            AlertWiring {
                channel,
                path,
                source,
            }
        })
        .collect()
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
fn the_alert_channel_table_is_a_usable_premise() {
    assert!(
        !ALERT_CHANNEL_CONFIG_PATHS.is_empty(),
        "告警出口表为空，可达性检查会静默通过"
    );
    let paths: BTreeSet<&str> = ALERT_CHANNEL_CONFIG_PATHS.iter().map(|(_, p)| *p).collect();
    assert_eq!(
        paths.len(),
        ALERT_CHANNEL_CONFIG_PATHS.len(),
        "两个受判项声明了同一条配置路径，其中一个读到的不是自己的值"
    );
    assert!(
        !paths.contains(ALERT_CHANNEL_CREDENTIAL_PATH),
        "凭证路径也在开关表里：开关是「地址即开关」的那几个，凭证不是开关"
    );
    assert_eq!(
        env_key_for(ALERT_CHANNEL_CREDENTIAL_PATH).len(),
        1,
        "凭证路径在 env 表里没有唯一的键：下面那条「凭证必须走 Secret」的判据会空转"
    );
}

/// 告警出口的每个开关都得从部署面拿得到值——缺失的那一环就是"dispatch 在代码里、
/// 配置面没有它的名字"。开关必须是 values 里的一个键：只有凭证才允许走 Secret，
/// 否则运维得在一个看不见的地方找它的开关。
#[test]
fn every_alert_channel_switch_is_reachable_from_the_deploy_surface() {
    let wiring = alert_wiring();
    assert_eq!(
        wiring.len(),
        ALERT_CHANNEL_CONFIG_PATHS.len(),
        "走查覆盖的受判项数与生产面的表不一致"
    );
    let declared: BTreeSet<&str> = ALERT_CHANNEL_CONFIG_PATHS.iter().map(|(c, _)| *c).collect();
    let covered: BTreeSet<&str> = wiring.iter().map(|w| w.channel).collect();
    assert_eq!(declared, covered, "表里的出口与走查覆盖的出口不是同一组");

    for w in &wiring {
        match &w.source {
            AlertSource::Value(key) => {
                assert!(!key.is_empty(), "出口 {} 的 values 键是空的", w.channel)
            }
            AlertSource::Secret(key) => panic!(
                "出口 {} 的 {} 从 Secret 的 {key} 取值：它不是凭证，运维会在 values 里\
                 找不到这个出口的开关",
                w.channel, w.path
            ),
        }
    }
}

/// 凭证那一环：两个跑告警桥的 Pod 都要拿到口令，且它只能经 Secret——
/// values 键或 ConfigMap 行都等于把口令写进仓库的清单。
#[test]
fn the_smtp_credential_arrives_through_a_secret_in_every_pod_that_sends_alerts() {
    let names = env_key_for(ALERT_CHANNEL_CREDENTIAL_PATH);
    assert_eq!(
        names.len(),
        1,
        "{ALERT_CHANNEL_CREDENTIAL_PATH} 在 env 表里出现 {} 次（应为 1 次）",
        names.len()
    );
    let env_name = names[0];
    let template = read(CHART_CONFIGMAP);
    assert!(
        !template.contains(&format!("{env_name}:")),
        "{CHART_CONFIGMAP} 里出现了 {env_name}：ConfigMap 装的是所有人都读得到的值，口令不该在这"
    );
    let keys: Vec<String> = ALERT_POD_TEMPLATES
        .iter()
        .filter_map(|t| secret_key_ref(&read(t), env_name))
        .collect();
    assert!(
        !keys.is_empty(),
        "没有任何 Pod 模板把 {env_name} 从 Secret 送进去：口令永远读不到"
    );
    let secret_key = keys[0].clone();
    assert!(
        keys.iter().all(|k| *k == secret_key),
        "两个 Pod 从不同的 Secret 键取同一个口令：{keys:?}"
    );
    assert!(
        !nested_values(&read(VALUES_YAML), ALERT_VALUES_BLOCK)
            .iter()
            .any(|(_, v)| v == &secret_key),
        "{VALUES_YAML} 里写着口令（Secret 键名 {secret_key}）：它只能由集群侧写入 Secret"
    );

    for template in ALERT_POD_TEMPLATES {
        assert_eq!(
            secret_key_ref(&read(template), env_name).as_deref(),
            Some(secret_key.as_str()),
            "{template} 没有把 {env_name} 从 Secret 的 {secret_key} 送进 Pod：这个 Pod 上的告警\
             邮件出口发不出去，而它与没有出口长得一样"
        );
    }
}

/// 反向：`OBS_ENV` 里每个告警相关的键都得有一个部署面来源。
///
/// 上面那条只判出口的开关；这一条判**全部**告警键（中继端口、发件人、TLS 开关、
/// Slack 频道……）。少一个键的后果与少一条 dispatcher 一样安静：进程读到默认值，
/// 部署里写的那一行没人认。
#[test]
fn every_alertmanager_env_key_reaches_a_deployment() {
    let template = read(CHART_CONFIGMAP);
    let values = nested_values(&read(VALUES_YAML), ALERT_VALUES_BLOCK);
    let mut checked = 0;

    for (env_name, path) in OBS_ENV {
        let Some(relative) = path.strip_prefix("alertmanager.") else {
            continue;
        };
        checked += 1;

        if let Some(line) = template
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{env_name}:")))
        {
            let key = values_key_after(line, ALERT_VALUES_PREFIX)
                .unwrap_or_else(|| {
                    panic!("{CHART_CONFIGMAP} 里 {env_name} 不取自 .Values.alertmanager")
                })
                .to_string();
            assert!(
                values.contains_key(&key),
                "{VALUES_YAML} 的 {ALERT_VALUES_BLOCK} 段里没有 {key}：{env_name} 渲染成空串，\
                 {path} 永远读不到运维设的值"
            );
            continue;
        }

        let delivered = ALERT_POD_TEMPLATES
            .iter()
            .any(|t| secret_key_ref(&read(t), env_name).is_some());
        assert!(
            delivered,
            "{env_name}（{path}）没有部署面来源：既不在 {CHART_CONFIGMAP} 里，也不在任何 \
             Pod 模板的 secretKeyRef 里——这个键没人送进 Pod"
        );
        assert_eq!(
            *path, ALERT_CHANNEL_CREDENTIAL_PATH,
            "{env_name} 走 Secret 取值，但它不是凭证（{path}）"
        );
        let _ = relative;
    }

    assert!(
        checked >= 8,
        "只判到 {checked} 个告警键，OBS_ENV 的解析可能读空了"
    );
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
///
/// 告警出口与通知出口共用这一条白名单（进化 Pod 上两套出口走同一个策略），所以它们
/// 一起进来配：拆成两份配对逻辑就会有一份先漂移，而症状只是"某个出口悄悄不通"。
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

    let alert_values = nested_values(&read(VALUES_YAML), ALERT_VALUES_BLOCK);
    let alert_configured = |key: &str| -> Option<String> {
        alert_values
            .get(key)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    for (key, outlet) in [
        ("webhookUrl", "alertmanager-webhook"),
        ("slack.webhookUrl", "alertmanager-slack"),
    ] {
        let Some(url) = alert_configured(key) else {
            continue;
        };
        let (host, port) = host_and_port(&url);
        assert!(
            configured.insert(host.clone(), (outlet, port)).is_none(),
            "两个出口配了同一个宿主 {host}：放行是按宿主配的，一个出口会被另一个顶掉"
        );
    }
    // 邮件的宿主是个裸主机名（端口是另一个键），不走 URL 解析。
    if let Some(host) = alert_configured("email.smtpHost") {
        let port = alert_configured("email.smtpPort")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or_else(|| {
                panic!("{VALUES_YAML} 的 email.smtpHost 配了，smtpPort 却不是端口号")
            });
        assert!(
            configured
                .insert(host.to_ascii_lowercase(), ("alertmanager-email", port))
                .is_none(),
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
