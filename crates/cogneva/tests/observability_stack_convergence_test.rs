//! 可观测性栈清单的收敛面：判断只有一张表，授权面等于交付面。
//!
//! 这条路要修的缺陷是「清单只有一条人工交付路径，装完一次之后仓库与现场各走
//! 各的，而清单没生效这件事没有判据」。修法是把交付判断收到一份两个消费面共用
//! 的处置表里，再让集群内的循环按仓库 rev 周期 apply 回声明态。于是有四个必须
//! 同时成立的东西，任何一个单独看都像对的：
//!
//! 1. 那张表必须是**清单目录的盘点**——目录里有几个 yaml 就有几行，多一行少一
//!    行都失败。缺行的方向（漏登记）本身是安全的（按交付处理），但没有盘点就
//!    没人知道新文件算什么。
//! 2. 安装脚本必须**读同一张表**，不能自己留一份名单——两份判断一定会分叉，
//!    而分叉的样子是「装的时候跳过、收敛的时候照做」。
//! 3. 循环必须真的被挂上，并且把结论送进持久化告警面：只在日志里的结论和「没人
//!    报」长得一样。
//! 4. 授给循环的权必须**等于它真会 apply 的那一面**。少了就是"要交付却没权"（每
//!    轮报权限被拒），多了就是"授了权却永不交付"。
//!
//! 第 4 条是这里唯一不能靠读字符串验的：它要按清单声明的 kind、经与循环同一个
//! 归置函数算出交付面，再与 Role 比对。所以 [`cog_reflection::classify_doc`] 是
//! 公开的，本文件用它而不是自己复制一份 kind 表。

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cog_reflection::observability_stack::{
    dispositions_path, parse_dispositions, plan_delivery, Disposition, DispositionEntry,
    DISPOSITIONS_FILE, STACK_NOT_CONVERGED_RULE,
};
use cog_reflection::{classify_doc, DocFate};

const MANIFEST_DIR: &str = "deploy/k3s/observability/manifests";
const INSTALL_SH: &str = "deploy/k3s/observability/scripts/install.sh";
const RBAC_FILE: &str = "12-stack-convergence-rbac.yaml";
const K3S_CONFIGMAP: &str = "deploy/k3s/evolution-configmap.yaml";
const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const PLUGIN_RS: &str = "crates/cog-reflection/src/plugin.rs";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

fn manifest_dir() -> PathBuf {
    repo_root().join(MANIFEST_DIR)
}

/// 清单目录里的 `*.yaml`，排序后返回（比对用，不依赖 readdir 顺序）。
fn yaml_files() -> Vec<String> {
    let dir = manifest_dir();
    let mut out: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".yaml"))
        .collect();
    out.sort();
    out
}

fn entries() -> Vec<DispositionEntry> {
    let path = dispositions_path(&manifest_dir());
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
    parse_dispositions(&text).unwrap_or_else(|e| panic!("{} 不符合格式: {e}", path.display()))
}

/// 一层文档的 kind：顶层（无缩进）的 `kind:` 行。
fn declared_kinds(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| l.strip_prefix("kind: "))
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty())
        .collect()
}

/// kind 落在哪个 API 组、哪个资源名。没有登记 = 清单里出现了授权面还没考虑过的
/// 对象，两条路必须一起改。
fn api_of(kind: &str) -> Option<(&'static str, &'static str)> {
    Some(match kind {
        "ConfigMap" => ("", "configmaps"),
        "Service" => ("", "services"),
        "StatefulSet" => ("apps", "statefulsets"),
        "Ingress" => ("networking.k8s.io", "ingresses"),
        "ServiceMonitor" => ("monitoring.coreos.com", "servicemonitors"),
        "PodMonitor" => ("monitoring.coreos.com", "podmonitors"),
        _ => return None,
    })
}

fn parse_bracket(line: &str) -> Vec<String> {
    let (Some(start), Some(end)) = (line.find('['), line.rfind(']')) else {
        panic!("授权面里有一行没有列表: {line}");
    };
    serde_json::from_str(&line[start..=end])
        .unwrap_or_else(|e| panic!("授权面的列表不是合法 JSON 数组（{line}）: {e}"))
}

#[derive(Default, Debug)]
struct Grant {
    groups: Vec<String>,
    resources: Vec<String>,
    verbs: Vec<String>,
}

fn role_grants(text: &str) -> Vec<Grant> {
    let mut grants: Vec<Grant> = Vec::new();
    let mut cur: Option<Grant> = None;
    for line in text.lines() {
        let t = line.trim_start();
        if t.starts_with("- apiGroups:") {
            if let Some(g) = cur.take() {
                grants.push(g);
            }
            cur = Some(Grant {
                groups: parse_bracket(t),
                ..Grant::default()
            });
        } else if t.starts_with("resources:") {
            if let Some(g) = cur.as_mut() {
                g.resources = parse_bracket(t);
            }
        } else if t.starts_with("verbs:") {
            if let Some(g) = cur.as_mut() {
                g.verbs = parse_bracket(t);
            }
        }
    }
    if let Some(g) = cur {
        grants.push(g);
    }
    grants
}

/// 那张表必须就是清单目录的盘点：每个 yaml 一行，每行指向一个真存在的文件。
#[test]
fn the_disposition_table_is_the_manifest_directory_inventory() {
    let files = yaml_files();
    let table = entries();

    assert!(
        !files.is_empty(),
        "{MANIFEST_DIR} 里一个清单都没有，这个门禁是空的"
    );
    // 处置表自己不是清单（`*.txt`），但必须躺在同一个目录里：两个消费面按目录
    // 相对路径读它，换个地方就得两边一起改。
    assert!(
        manifest_dir().join(DISPOSITIONS_FILE).is_file(),
        "{DISPOSITIONS_FILE} 不在 {MANIFEST_DIR} 里"
    );

    let listed: BTreeSet<&str> = table.iter().map(|e| e.file.as_str()).collect();
    let present: BTreeSet<&str> = files.iter().map(|f| f.as_str()).collect();
    let missing: Vec<&&str> = present.difference(&listed).collect();
    let dangling: Vec<&&str> = listed.difference(&present).collect();
    assert!(
        missing.is_empty(),
        "这些清单没有登记处置（新加的文件必须写下它算什么）: {missing:?}"
    );
    assert!(
        dangling.is_empty(),
        "处置表指向了不存在的文件（改名或删除后留下的话已经不是它说的那件事了）: {dangling:?}"
    );

    // 理由：一条没有理由的处置就是下一个没人知道的洞。
    for e in &table {
        assert!(!e.reason.trim().is_empty(), "{} 的处置没有理由", e.file);
    }
}

/// 安装脚本读同一张表，不留自己那份名单。
///
/// 名单只允许出现在**顺序**上说的通的两个地方：命名空间要先于依赖它的凭证准备，
/// ClickHouse 凭证要先于它的清单。名字出现在这里是为了排序，不是为了决定交付。
#[test]
fn the_install_script_consults_the_table_instead_of_carrying_its_own_list() {
    let script = read(INSTALL_SH);
    assert!(
        script.contains(DISPOSITIONS_FILE),
        "{INSTALL_SH} 没有读 {DISPOSITIONS_FILE}"
    );
    assert!(
        script.contains("skip_reason"),
        "{INSTALL_SH} 的 apply 路径没有经过处置判断"
    );

    let mut named: BTreeSet<String> = BTreeSet::new();
    for f in yaml_files() {
        if script.contains(&f) {
            named.insert(f);
        }
    }
    let ordering_anchors: BTreeSet<String> = ["01-namespace.yaml", "09-clickhouse.yaml"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let extra: Vec<&String> = named.difference(&ordering_anchors).collect();
    assert!(
        extra.is_empty(),
        "安装脚本里点了这些清单的名，等于自己留了一份交付判断（它与处置表分叉时，\n\
         分叉的样子是「装的时候跳过、收敛的时候照做」）: {extra:?}"
    );
}

/// 循环得真被挂上，结论得进持久化告警面。
#[test]
fn the_convergence_loop_is_wired_and_persists_its_verdict() {
    let lib = read("crates/cog-reflection/src/lib.rs");
    assert!(
        lib.contains("pub mod observability_stack;"),
        "收敛模块没有在 lib.rs 里挂出来"
    );

    let plugin = read(PLUGIN_RS);
    let start = plugin
        .find("observability_stack.enabled")
        .expect("plugin.rs 里没有按配置开关起收敛循环");
    let end = plugin[start..]
        .find("run_mainline_loop")
        .map(|i| start + i)
        .expect("收敛循环没有与主线循环并列起在部署器旁边");
    let block = &plugin[start..end];
    assert!(
        block.contains("StackConvergence::new("),
        "开关后面没有构造收敛面"
    );
    assert!(block.contains("stack.run("), "构造了收敛面但没有起循环");
    assert!(
        block.contains("PersistentAlertSink"),
        "收敛面没有把结论送进持久化告警面：只在日志里的结论和「没人报」长得一样"
    );
    assert!(
        block.contains("deployer.clone()"),
        "收敛面与主线部署器不是同一个部署器实例（读的是同一份 bare 仓库，必须是）"
    );
}

/// 授权面必须恰好等于交付面。
///
/// 交付面按与循环同一个归置函数算：清单声明的 kind 里，`classify_doc` 判为交付
/// 的那些。少授 = 每轮报权限被拒；多授 = 授了权却永不交付，两条都是没人看得见
/// 的状态。
#[test]
fn the_grant_covers_exactly_the_documents_the_loop_will_apply() {
    let files = yaml_files();
    let table = entries();
    let plan = plan_delivery(&files, &table, true);

    // 交付面：交付文件里所有会被 apply 的文档。
    let mut needed: BTreeMap<(&str, &str), BTreeSet<String>> = BTreeMap::new();
    for file in &plan.deliver {
        let text = read(&format!("{MANIFEST_DIR}/{file}"));
        for kind in declared_kinds(&text) {
            if classify_doc(&kind) != DocFate::Deliver {
                continue;
            }
            let (group, resource) =
                api_of(&kind).unwrap_or_else(|| panic!("{file} 里的 {kind} 没有映射到授权面"));
            needed
                .entry((group, resource))
                .or_default()
                .insert(file.clone());
        }
    }
    assert!(
        !needed.is_empty(),
        "交付面里没有一个需要授权的对象，这个门禁是空的"
    );

    let rbac_text = read(&format!("{MANIFEST_DIR}/{RBAC_FILE}"));
    let grants = role_grants(&rbac_text);
    let api_group_lines = rbac_text
        .lines()
        .filter(|l| l.trim_start().starts_with("- apiGroups:"))
        .count();
    assert_eq!(
        grants.len(),
        api_group_lines,
        "授权面的规则条数解析后对不上，比对结果不可信"
    );

    let mut missing: Vec<String> = Vec::new();
    for ((group, resource), sources) in &needed {
        let ok = grants.iter().any(|g| {
            g.groups.iter().any(|x| x == group)
                && g.resources.iter().any(|x| x == resource)
                && ["get", "create", "patch"]
                    .iter()
                    .all(|v| g.verbs.iter().any(|x| x == v))
        });
        if !ok {
            missing.push(format!("{group}/{resource}（{sources:?} 需要）"));
        }
    }
    assert!(
        missing.is_empty(),
        "{RBAC_FILE} 没有覆盖循环会 apply 的对象，缺了就是每轮报一次权限被拒: {missing:?}"
    );

    // 循环没有删除路径：授 delete 等于把一个能力面挂在没人用的地方。
    for g in &grants {
        assert!(
            !g.verbs.iter().any(|v| v == "delete"),
            "授权面给了 delete：收敛面没有删除路径"
        );
        assert!(
            !g.groups
                .iter()
                .any(|x| x.contains("rbac.authorization.k8s.io")),
            "授权面把授权面自己授了出去（一份清单能给自己长权限）"
        );
    }
}

/// 权限与凭证永远不由循环交付：这条不变式是授权面能保持有界的前提。
#[test]
fn the_loop_never_delivers_the_authority_or_secret_surface() {
    for kind in ["Role", "RoleBinding", "ClusterRole", "Namespace", "Secret"] {
        assert_ne!(
            classify_doc(kind),
            DocFate::Deliver,
            "{kind} 会被循环 apply：一份清单能给自己长权限/把密钥搬进集群"
        );
    }
    // 反向：正常的支撑对象仍要交付，否则这条不变式是拿"什么都不交付"换来的。
    assert_eq!(classify_doc("ConfigMap"), DocFate::Deliver);
    assert_eq!(classify_doc("ServiceMonitor"), DocFate::Deliver);

    // 授权面文件自己就得落在这个被挡住的面上（它只由安装期交付）。
    let rbac_text = read(&format!("{MANIFEST_DIR}/{RBAC_FILE}"));
    for kind in declared_kinds(&rbac_text) {
        assert_ne!(
            classify_doc(&kind),
            DocFate::Deliver,
            "{RBAC_FILE} 里的 {kind} 会被循环自己 apply"
        );
    }
    // 但它在安装期是要交付的（那正是它唯一的交付路径）。
    let entry = entries()
        .into_iter()
        .find(|e| e.file == RBAC_FILE)
        .expect("授权面文件没有登记处置");
    assert_eq!(entry.disposition, Disposition::Deliver);
}

/// 部署面三处写的路径/命名空间必须是同一处，且命名空间得是清单自己声明的那个。
#[test]
fn the_deploy_surfaces_and_the_code_default_agree_on_where_the_stack_lives() {
    let default = cog_reflection::ObservabilityStackConfig::default();
    assert_eq!(default.manifest_dir, MANIFEST_DIR);
    assert!(
        manifest_dir().is_dir(),
        "代码默认的清单目录在仓库里不存在: {}",
        default.manifest_dir
    );
    // 安装脚本用 SCRIPT_DIR/../manifests 定位同一处：两个消费面读的必须是同一批
    // 文件，不能靠"我记得改了两边"。
    let install_dir =
        std::fs::canonicalize(repo_root().join("deploy/k3s/observability/scripts/../manifests"))
            .expect("安装脚本的清单目录不存在");
    let code_dir = std::fs::canonicalize(manifest_dir()).expect("清单目录不存在");
    assert_eq!(install_dir, code_dir);

    // 命名空间：清单自己声明的那个（01-namespace.yaml），不是另写一份。
    let ns = read(&format!("{MANIFEST_DIR}/01-namespace.yaml"));
    let declared = ns
        .lines()
        .skip_while(|l| !l.starts_with("kind: Namespace"))
        .find_map(|l| l.strip_prefix("  name: "))
        .map(|s| s.trim().to_string())
        .expect("01-namespace.yaml 里读不到命名空间名");
    assert_eq!(
        default.namespace, declared,
        "代码默认的命名空间与清单声明的不一致"
    );

    // k3s profile 的部署清单：路径与命名空间都得与代码默认同源。
    let cm = read(K3S_CONFIGMAP);
    assert!(
        cm.contains(&format!(
            "COGNEVA_MAINLINE_DEPLOYER_STACK_MANIFEST_DIR: \"{MANIFEST_DIR}\""
        )),
        "{K3S_CONFIGMAP} 里的清单目录与代码默认不一致"
    );
    assert!(
        cm.contains(&format!(
            "COGNEVA_MAINLINE_DEPLOYER_STACK_NAMESPACE: \"{declared}\""
        )),
        "{K3S_CONFIGMAP} 里的命名空间与清单声明的不一致"
    );

    // chart 是拓扑的权威源：values.yaml 里也要有这两项，且指向同一处。
    let values = read(VALUES_YAML);
    let block = yaml_block(&values, "  observabilityStack:");
    let expected_dir = format!("\"{MANIFEST_DIR}\"");
    assert_eq!(
        block.get("manifestDir").map(String::as_str),
        Some(expected_dir.as_str()),
        "{VALUES_YAML} 的 mainlineDeployer.observabilityStack.manifestDir 与代码默认不一致"
    );
    assert_eq!(
        block.get("namespace").map(String::as_str),
        Some(declared.as_str()),
        "{VALUES_YAML} 的 mainlineDeployer.observabilityStack.namespace 与清单声明不一致"
    );

    // 自我告警的规则名不能落在配置的规则集里：否则另一个消费者会把它当自己的
    // 行 adopt/resolve（同 observability_alert_rule_contract_test.rs 的判据）。
    assert!(!STACK_NOT_CONVERGED_RULE.is_empty());
}

/// 读一个两空格缩进的 YAML 块（chart values 的手写风格），返回块内的标量。
fn yaml_block(text: &str, header: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut inside = false;
    for line in text.lines() {
        if line.trim_end() == header.trim_end() {
            inside = true;
            continue;
        }
        if !inside {
            continue;
        }
        // 回到同级或更外层，块结束。
        if !line.starts_with("    ") {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            break;
        }
        if line.trim_start().starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.trim().split_once(':') {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    out
}
