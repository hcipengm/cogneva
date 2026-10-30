//! 清单里的 env 从字面量 `value` 改成 `valueFrom` 时，**已存在的对象上那份
//! 残留的 value 必须显式清掉**——客户端 apply 清不掉它。
//!
//! 为什么清不掉：`kubectl apply` 的三方合并以"上次应用的清单"为基线，而
//! `env` 是按 `name` 合并的列表。清单把某个 env 从 `value` 改成 `valueFrom`
//! 时，合并结果里该条目**同时**留着旧的 `value` 和新的 `valueFrom`，而
//! `valueFrom` 与 `value` 互斥——对象被准入拒绝。没有 last-applied 注解的对象
//! （用服务端 apply 或 helm 创建的都是这样）更没有"删掉旧字段"的依据：合并把
//! 所有 live 独有字段都当"别人写的"原样保留，于是这个对象**永远 apply 不进去**。
//!
//! 症状还特别难查：报错指向清单（"valueFrom 在 value 非空时不允许"），而清单
//! 本身是正确的——错在 live 对象上那份没人负责的残留。更糟的是残留物往往正是
//! 我们要从清单里拿掉的那个明文凭证。
//!
//! 所以这里给出一条确定性判据：**清单声明了 `valueFrom` 而 live 上同名的 env 还
//! 带着 `value`** ⇒ 把 live 上那个 `value` 删掉。判据只看这两侧的事实，不猜。
//!
//! 反向（清单给 `value`、live 有 `valueFrom`）不在这里处理：那种情况合并结果同样
//! 非法，但"该听谁的"取决于清单意图（是有意写回字面量，还是漏了改动），猜错会
//! 把凭证写回对象，所以留给调用方显式决定。

use serde_json::Value;

/// 一处需要清除的残留：`<pod 模板>.<container 字段>[i].env[j].value`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersededEnvValue {
    /// `containers` 或 `initContainers`——patch 路径要按它拼。
    pub container_field: &'static str,
    /// 容器在数组里的下标。
    pub container: usize,
    /// env 条目在数组里的下标。
    pub env: usize,
    /// env 名（日志与核对用）。
    pub name: String,
}

impl SupersededEnvValue {
    /// 该残留字段在对象里的 JSON 指针（`kubectl patch --type=json` 用）。
    pub fn patch_path(&self) -> String {
        self.patch_path_under(POD_TEMPLATE)
    }

    /// 同上，但 pod 模板不在默认位置时（CronJob 的模板在 `jobTemplate` 之下）
    /// 由调用方给前缀。
    pub fn patch_path_under(&self, prefix: &str) -> String {
        format!(
            "{prefix}/{}/{}/env/{}/value",
            self.container_field, self.container, self.env
        )
    }

    /// 整条 env 删掉时的 JSON 指针（没有结尾的 `/value`）。
    pub fn entry_path_under(&self, prefix: &str) -> String {
        format!(
            "{prefix}/{}/{}/env/{}",
            self.container_field, self.container, self.env
        )
    }
}

/// 两类清理合成一次 `kubectl patch --type=json` 的操作序列：`whole` 整条删，
/// `value` 只删条目里的 `value`。
///
/// 为什么必须合成的**一次**：同一容器里删掉一条 env 会让它后面条目的下标整体
/// 前移。分成两次 patch，第二次那批下标是按原始对象算出来的，第一次删完就已经
/// 指向别人了。两类在同一条目上不会撞（整条删的那条清单里根本没有，被 valueFrom
/// 取代的那条清单里有），所以按（字段、容器、倒序下标）排一遍就够。
pub fn env_cleanup_patch_ops(
    whole: &[SupersededEnvValue],
    value: &[SupersededEnvValue],
    prefix: &str,
) -> Vec<Value> {
    struct Op {
        head: (&'static str, usize, std::cmp::Reverse<usize>),
        /// 同一条目上只会有一种；这一位只为排序确定
        rank: u8,
        path: String,
    }
    let mut ops: Vec<Op> = Vec::new();
    for r in whole {
        ops.push(Op {
            head: (r.container_field, r.container, std::cmp::Reverse(r.env)),
            rank: 0,
            path: r.entry_path_under(prefix),
        });
    }
    for r in value {
        ops.push(Op {
            head: (r.container_field, r.container, std::cmp::Reverse(r.env)),
            rank: 1,
            path: r.patch_path_under(prefix),
        });
    }
    ops.sort_by(|a, b| a.head.cmp(&b.head).then(a.rank.cmp(&b.rank)));
    ops.into_iter()
        .map(|o| serde_json::json!({"op": "remove", "path": o.path}))
        .collect()
}

/// pod 模板在文档里的指针。CronJob 的模板比别的工作负载多两层（`jobTemplate`），
/// 这层差异曾让它整个落在下面的判据之外。
pub const POD_TEMPLATE: &str = "/spec/template/spec";
pub const CRONJOB_POD_TEMPLATE: &str = "/spec/jobTemplate/spec/template/spec";

/// 带 pod 模板的工作负载，连同模板在文档里的位置；其余文档返回 `None`。
///
/// 与 `workload_identity` 的区别只有一个：CronJob 也算。它是真有 pod 模板的
/// 对象，路径也是确定的，不认它只会让它的 env 没人判。
pub fn pod_workload(doc: &Value) -> Option<(WorkloadRef, &'static str)> {
    let prefix = match doc.get("kind")?.as_str()? {
        "Deployment" | "StatefulSet" | "DaemonSet" | "Job" => POD_TEMPLATE,
        "CronJob" => CRONJOB_POD_TEMPLATE,
        _ => return None,
    };
    doc.pointer(prefix)?;
    let kind = doc.get("kind")?.as_str()?.to_string();
    let metadata = doc.get("metadata")?;
    let name = metadata.get("name")?.as_str()?.to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .map(str::to_string);
    Some((
        WorkloadRef {
            kind,
            name,
            namespace,
        },
        prefix,
    ))
}

/// 带 pod 模板的工作负载身份。清除残留之前得先知道要查哪个对象。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadRef {
    pub kind: String,
    pub name: String,
    /// 清单里写的命名空间；清单通常不写（由调用方的 `-n` 决定），此时为 None。
    pub namespace: Option<String>,
}

impl WorkloadRef {
    /// `kubectl` 命令里用的资源写法（kind 小写）。
    pub fn kind_arg(&self) -> String {
        self.kind.to_lowercase()
    }
}

/// 带 pod 模板的工作负载身份（kind、name、namespace）。其余文档返回 None——
/// 判据只认 pod 模板在 `/spec/template/spec` 的对象，不为别的资源猜路径。
pub fn workload_identity(doc: &Value) -> Option<WorkloadRef> {
    let kind = doc.get("kind")?.as_str()?;
    if !matches!(kind, "Deployment" | "StatefulSet" | "DaemonSet" | "Job") {
        return None;
    }
    doc.pointer("/spec/template/spec")?;
    let metadata = doc.get("metadata")?;
    let name = metadata.get("name")?.as_str()?.to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .map(str::to_string);
    Some(WorkloadRef {
        kind: kind.to_string(),
        name,
        namespace,
    })
}

/// `kubectl patch --type=json` 的删除操作序列。
///
/// **同一容器内按 env 下标倒序**：正向删会让后面条目的下标整体前移，patch 打偏
/// 到别人身上。这条规则是判据的一部分，所以和判据放在一起——分散到各交付路径
/// 手写一遍，迟早有一处写成正序。
pub fn removal_patch_ops(removals: &[SupersededEnvValue]) -> Vec<Value> {
    let mut ordered = removals.to_vec();
    ordered.sort_by(|a, b| {
        (a.container_field, a.container, std::cmp::Reverse(a.env)).cmp(&(
            b.container_field,
            b.container,
            std::cmp::Reverse(b.env),
        ))
    });
    ordered
        .iter()
        .map(|r| serde_json::json!({"op": "remove", "path": r.patch_path()}))
        .collect()
}

const CONTAINER_FIELDS: [&str; 2] = ["containers", "initContainers"];

/// 找出 live 对象上"已被清单的 `valueFrom` 取代"的 env `value`。
///
/// 输入是**同一对象**的期望态（清单反序列化后的文档）与现状（`kubectl get -o json`）。
/// 任一侧读不出 pod 模板就返回空——判不出来就不动，绝不"顺手清一遍"。
pub fn superseded_env_values(desired: &Value, live: &Value) -> Vec<SupersededEnvValue> {
    superseded_env_values_at(desired, live, POD_TEMPLATE)
}

/// 同上，pod 模板不在默认位置时（CronJob）由调用方给前缀。
pub fn superseded_env_values_at(
    desired: &Value,
    live: &Value,
    prefix: &str,
) -> Vec<SupersededEnvValue> {
    let mut out = Vec::new();
    for field in CONTAINER_FIELDS {
        let desired_containers = containers_at(desired, field, prefix);
        let live_containers = containers_at(live, field, prefix);
        for (ci, desired_container) in desired_containers.iter().enumerate() {
            let Some(live_container) = live_containers.get(ci) else {
                continue;
            };
            let Some(desired_env) = env_entries(desired_container) else {
                continue;
            };
            let Some(live_env) = env_entries(live_container) else {
                continue;
            };
            for entry in desired_env {
                // 清单这一条是 valueFrom 且没同时写字面量，才是"取代"关系
                let (Some(name), Some(_)) = (env_name(entry), entry.get("valueFrom")) else {
                    continue;
                };
                if entry.get("value").is_some() {
                    continue;
                }
                // live 上同名的条目还带着 value ⇒ 该删
                let Some((env_index, _)) = live_env
                    .iter()
                    .enumerate()
                    .find(|(_, e)| env_name(e) == Some(name))
                else {
                    continue;
                };
                let Some(live_entry) = live_env.get(env_index) else {
                    continue;
                };
                if live_entry.get("value").is_none() {
                    continue;
                }
                out.push(SupersededEnvValue {
                    container_field: field,
                    container: ci,
                    env: env_index,
                    name: name.to_string(),
                });
            }
        }
    }
    out
}

/// live 上还从 Secret 取值、而清单已经完全不再声明的那几条 env。
///
/// 为什么只认 Secret 来源：清单里拿掉一条 env，`kubectl apply` 按名字合并，那条
/// 在已有对象上**原样留着**——不报错、不留标记，也没有任何路径会去删它。普通
/// env 不动它（可能是带外调试留下的，删错比留着贵）；但一条凭证不同：它读的是
/// 别人名下的密钥，而凭证面的读法只认声明，现网多一个读者就是一次越权，不是一次
/// 配置差异。2026-10-31 的现场正是这样：`6b99311b` 把业务进程换成 `cog_app` 并
/// 拿掉了 `pg-password`，三个 Deployment 与备份 CronJob 的 env 里那份超级用户
/// 口令却一直挂着，凭证面读数连红一天，而没有任何东西会去删它。
///
/// 容器按名字对（两边都写了名字时），名字缺失才退回下标——按下标对会为一次
/// 容器重排删掉另一个容器的 env。
pub fn secret_env_dropped_by_the_manifest(
    desired: &Value,
    live: &Value,
) -> Vec<SupersededEnvValue> {
    let Some((_, prefix)) = pod_workload(desired) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for field in CONTAINER_FIELDS {
        let desired_containers = containers_at(desired, field, prefix);
        let live_containers = containers_at(live, field, prefix);
        for (ci, live_container) in live_containers.iter().enumerate() {
            let Some(desired_container) = counterpart(desired_containers, live_container, ci)
            else {
                continue;
            };
            let declared: Vec<&str> = env_entries(desired_container)
                .into_iter()
                .flatten()
                .filter_map(|e| env_name(e))
                .collect();
            let Some(live_env) = env_entries(live_container) else {
                continue;
            };
            for (ei, entry) in live_env.iter().enumerate() {
                let Some(name) = env_name(entry) else {
                    continue;
                };
                if entry
                    .get("valueFrom")
                    .and_then(|v| v.get("secretKeyRef"))
                    .is_none()
                {
                    continue;
                }
                if declared.contains(&name) {
                    continue;
                }
                out.push(SupersededEnvValue {
                    container_field: field,
                    container: ci,
                    env: ei,
                    name: name.to_string(),
                });
            }
        }
    }
    out
}

/// 一次清理的完整方案：一次 patch 的删除操作、被清掉的 env 名（日志用）、其中整条
/// 删除了几条。
///
/// 两类清理必须合进**同一次** patch：它们的位置都按同一份现状算下标，分开算、分开
/// 打，第二份下标已经被第一份删动过了。合成一处也让调用方不必自己记住这个顺序。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvCleanupPlan {
    pub ops: Vec<Value>,
    pub names: Vec<String>,
    pub dropped: usize,
}

/// 把两类清理与它们的 patch 序列合成一份方案；没什么可清返回 `None`。
pub fn env_cleanup_plan(desired: &Value, live: &Value) -> Option<EnvCleanupPlan> {
    let (_, prefix) = pod_workload(desired)?;
    let superseded = superseded_env_values_at(desired, live, prefix);
    let dropped = secret_env_dropped_by_the_manifest(desired, live);
    if superseded.is_empty() && dropped.is_empty() {
        return None;
    }
    let ops = env_cleanup_patch_ops(&dropped, &superseded, prefix);
    let names = dropped
        .iter()
        .chain(superseded.iter())
        .map(|r| r.name.clone())
        .collect();
    Some(EnvCleanupPlan {
        ops,
        names,
        dropped: dropped.len(),
    })
}

/// live 的某个容器在清单里的对应项：先按名字，两边都没有名字才按下标。
fn counterpart<'a>(declared: &'a [Value], live: &Value, index: usize) -> Option<&'a Value> {
    if let Some(name) = live.get("name").and_then(Value::as_str) {
        if let Some(found) = declared
            .iter()
            .find(|d| d.get("name").and_then(Value::as_str) == Some(name))
        {
            return Some(found);
        }
        // 清单里没有这个容器：它的 env 没有声明可对照，不猜
        return None;
    }
    declared.get(index)
}

fn containers_at<'a>(doc: &'a Value, field: &str, prefix: &str) -> &'a [Value] {
    doc.pointer(&format!("{prefix}/{field}"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn env_entries(container: &Value) -> Option<&Vec<Value>> {
    container.get("env").and_then(Value::as_array)
}

fn env_name(entry: &Value) -> Option<&str> {
    entry.get("name").and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn live(env: Value) -> Value {
        json!({"spec": {"template": {"spec": {"containers": [{"name": "c", "env": env}]}}}})
    }

    fn desired(env: Value) -> Value {
        json!({"spec": {"template": {"spec": {"containers": [{"name": "c", "env": env}]}}}})
    }

    #[test]
    fn a_value_superseded_by_a_secret_ref_is_removed() {
        // 真实事故形态：清单把某个 env 从明文改成从 Secret 注入，live 上那份明文
        // 没有任何路径能删掉，于是对象永久不可 apply。
        let d = desired(json!([
            {"name": "A", "value": "1"},
            {"name": "B", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}}
        ]));
        let l = live(json!([
            {"name": "A", "value": "1"},
            {"name": "B", "value": "leaked-secret"}
        ]));
        let found = superseded_env_values(&d, &l);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "B");
        assert_eq!(found[0].env, 1);
        assert_eq!(
            found[0].patch_path(),
            "/spec/template/spec/containers/0/env/1/value"
        );
    }

    #[test]
    fn a_clean_object_yields_no_patch() {
        let d = desired(json!([{"name": "B", "valueFrom": {"secretKeyRef": {}}}]));
        let l = live(json!([{"name": "B", "valueFrom": {"secretKeyRef": {}}}]));
        assert!(superseded_env_values(&d, &l).is_empty());
    }

    #[test]
    fn a_manifest_that_still_writes_a_literal_is_left_alone() {
        // 清单自己给了 value：这不是"残留"，有没有 valueFrom 该由清单作者说清
        let d = desired(json!([{"name": "B", "value": "new"}]));
        let l = live(json!([{"name": "B", "value": "old"}]));
        assert!(superseded_env_values(&d, &l).is_empty());
    }

    #[test]
    fn an_env_the_manifest_no_longer_declares_is_not_touched() {
        // 清单里根本没有这一条：删不删是"清单删条目"的另一回事（可能是有意保留
        // 给带外写入的），判据不该顺手扩大
        let d = desired(json!([{"name": "A", "value": "1"}]));
        let l = live(json!([
            {"name": "A", "value": "1"},
            {"name": "GONE", "value": "x"}
        ]));
        assert!(superseded_env_values(&d, &l).is_empty());
    }

    #[test]
    fn init_containers_are_covered_too() {
        let d = json!({"spec": {"template": {"spec": {"initContainers": [
            {"name": "i", "env": [{"name": "B", "valueFrom": {"secretKeyRef": {}}}]}
        ]}}}});
        let l = json!({"spec": {"template": {"spec": {"initContainers": [
            {"name": "i", "env": [{"name": "B", "value": "leaked"}]}
        ]}}}});
        let found = superseded_env_values(&d, &l);
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].patch_path(),
            "/spec/template/spec/initContainers/0/env/0/value"
        );
    }

    #[test]
    fn a_document_without_a_pod_template_is_not_guessed_at() {
        let d = json!({"kind": "ConfigMap", "data": {"a": "b"}});
        let l = json!({"kind": "ConfigMap", "data": {"a": "b"}});
        assert!(superseded_env_values(&d, &l).is_empty());
    }

    #[test]
    fn only_kinds_with_a_pod_template_get_an_identity() {
        for (kind, expected) in [
            ("Deployment", true),
            ("StatefulSet", true),
            ("DaemonSet", true),
            ("Job", true),
            ("ConfigMap", false),
            ("Service", false),
        ] {
            let doc = json!({
                "kind": kind,
                "metadata": {"name": "w", "namespace": "ns"},
                "spec": {"template": {"spec": {"containers": []}}}
            });
            assert_eq!(workload_identity(&doc).is_some(), expected, "{kind}");
        }
        // pod 模板不在 /spec/template/spec 上的（如 CronJob）不猜路径
        let cron = json!({
            "kind": "CronJob",
            "metadata": {"name": "c"},
            "spec": {"jobTemplate": {"spec": {"template": {"spec": {}}}}}
        });
        assert!(workload_identity(&cron).is_none());
        // 命名空间是可选的：清单不写时由调用方的 -n 决定
        let plain = json!({"kind": "Deployment", "metadata": {"name": "w"},
            "spec": {"template": {"spec": {}}}});
        let r = workload_identity(&plain).unwrap();
        assert_eq!(r.namespace, None);
        assert_eq!(r.kind_arg(), "deployment");
    }

    #[test]
    fn removals_are_emitted_highest_index_first() {
        // 同一容器里删两条时，先删大的下标：反过来的话第一条删掉之后，第二条的
        // 下标已经指向别处了。
        let found = vec![
            SupersededEnvValue {
                container_field: "containers",
                container: 0,
                env: 1,
                name: "A".into(),
            },
            SupersededEnvValue {
                container_field: "containers",
                container: 0,
                env: 3,
                name: "B".into(),
            },
        ];
        let ops = removal_patch_ops(&found);
        assert_eq!(
            ops,
            vec![
                json!({"op": "remove", "path": "/spec/template/spec/containers/0/env/3/value"}),
                json!({"op": "remove", "path": "/spec/template/spec/containers/0/env/1/value"}),
            ]
        );
    }

    #[test]
    fn a_secret_env_the_manifest_no_longer_declares_is_dropped_whole() {
        // 2026-10-31 的形态：清单把 PG_PASSWORD 整条拿掉了，live 上那条还在从
        // Secret 取超级用户口令。要删的是整条（带 `/value` 的只是它的一半）。
        let d = json!({"kind": "Deployment", "metadata": {"name": "cogneva"},
        "spec": {"template": {"spec": {"containers": [
            {"name": "cogneva", "env": [{"name": "APP_PASSWORD",
                "valueFrom": {"secretKeyRef": {"name": "s", "key": "app-password"}}}]}
        ]}}}});
        let l = json!({"kind": "Deployment", "metadata": {"name": "cogneva"},
        "spec": {"template": {"spec": {"containers": [
            {"name": "cogneva", "env": [
                {"name": "PG_PASSWORD",
                 "valueFrom": {"secretKeyRef": {"name": "s", "key": "pg-password"}}},
                {"name": "APP_PASSWORD",
                 "valueFrom": {"secretKeyRef": {"name": "s", "key": "app-password"}}}]}
        ]}}}});
        let dropped = secret_env_dropped_by_the_manifest(&d, &l);
        assert_eq!(dropped.len(), 1, "{dropped:?}");
        assert_eq!(dropped[0].name, "PG_PASSWORD");
        assert_eq!(dropped[0].env, 0);
        let ops = env_cleanup_patch_ops(&dropped, &[], POD_TEMPLATE);
        assert_eq!(
            ops,
            vec![json!({"op": "remove",
                "path": "/spec/template/spec/containers/0/env/0"})]
        );
    }

    #[test]
    fn a_plain_env_the_manifest_no_longer_declares_is_left_alone() {
        // 不带 Secret 的条目不动：可能是带外调试留下来的，删错比留着贵。
        // 反面：这条判据若扩大成"清单没有就删"，一次带外排障留下的 env 会在
        // 下一次滚动里被静默拿掉。
        let d = json!({"kind": "Deployment", "metadata": {"name": "w"},
            "spec": {"template": {"spec": {"containers": [{"name": "c", "env": []}]}}}});
        let l = json!({"kind": "Deployment", "metadata": {"name": "w"},
            "spec": {"template": {"spec": {"containers": [
                {"name": "c", "env": [{"name": "RUST_LOG", "value": "debug"}]}]}}}});
        assert!(secret_env_dropped_by_the_manifest(&d, &l).is_empty());
    }

    #[test]
    fn containers_are_matched_by_name_not_by_index() {
        // 容器重排过一次之后，按下标对会把另一个容器声明的同名 env 当成"清单
        // 已经没有"而删掉——那是在删一条清单明确要的读取。
        let d = json!({"kind": "Deployment", "metadata": {"name": "w"},
            "spec": {"template": {"spec": {"containers": [
                {"name": "b", "env": [{"name": "PG_PASSWORD",
                    "valueFrom": {"secretKeyRef": {}}}]},
                {"name": "a", "env": []}]}}}});
        let l = json!({"kind": "Deployment", "metadata": {"name": "w"},
            "spec": {"template": {"spec": {"containers": [
                {"name": "a", "env": [{"name": "PG_PASSWORD",
                    "valueFrom": {"secretKeyRef": {}}}]},
                {"name": "b", "env": [{"name": "PG_PASSWORD",
                    "valueFrom": {"secretKeyRef": {}}}]}]}}}});
        let dropped = secret_env_dropped_by_the_manifest(&d, &l);
        assert_eq!(
            dropped
                .iter()
                .map(|d| (dropped_container(&l, d), d.name.as_str()))
                .collect::<Vec<_>>(),
            vec![("a", "PG_PASSWORD")],
            "只该删 a 上那一条：b 的名字清单里有"
        );
    }

    fn dropped_container<'a>(live: &'a Value, r: &SupersededEnvValue) -> &'a str {
        live.pointer(&format!(
            "/spec/template/spec/{}/{}/name",
            r.container_field, r.container
        ))
        .and_then(Value::as_str)
        .unwrap_or("")
    }

    #[test]
    fn a_cronjob_pod_template_is_found_two_levels_deeper() {
        // 备份 CronJob 的 env 一直不在这些判据里：它的模板在 jobTemplate 下面。
        let d = json!({"kind": "CronJob", "metadata": {"name": "cogneva-backup"},
            "spec": {"jobTemplate": {"spec": {"template": {"spec": {"containers": [
                {"name": "backup", "env": []}]}}}}}});
        let l = json!({"kind": "CronJob", "metadata": {"name": "cogneva-backup"},
            "spec": {"jobTemplate": {"spec": {"template": {"spec": {"containers": [
                {"name": "backup", "env": [{"name": "PG_PASSWORD",
                    "valueFrom": {"secretKeyRef": {}}}]}]}}}}}});
        let (workload, prefix) = pod_workload(&d).expect("CronJob 有 pod 模板");
        assert_eq!(workload.kind_arg(), "cronjob");
        assert_eq!(prefix, CRONJOB_POD_TEMPLATE);
        let dropped = secret_env_dropped_by_the_manifest(&d, &l);
        assert_eq!(dropped.len(), 1);
        let ops = env_cleanup_patch_ops(&dropped, &[], prefix);
        assert_eq!(
            ops,
            vec![json!({"op": "remove",
                "path": "/spec/jobTemplate/spec/template/spec/containers/0/env/0"})]
        );
    }

    /// 端到端：清单不再声明的一条 Secret env 与清单改成 `valueFrom` 的一条，落进
    /// 同一次 patch，且下标都按原始对象倒序——正向删会让后面条目的下标前移。
    #[test]
    fn the_plan_puts_both_cleanups_in_one_patch_and_nothing_else() {
        let desired = json!({
            "kind": "Deployment",
            "metadata": {"name": "app"},
            "spec": {"template": {"spec": {"containers": [{"name": "app", "env": [
                {"name": "PG_HOST", "value": "pg"},
                {"name": "TOKEN", "valueFrom": {"secretKeyRef": {"name": "s", "key": "t"}}},
            ]}]}}},
        });
        let live = json!({
            "spec": {"template": {"spec": {"containers": [{"name": "app", "env": [
                {"name": "PG_HOST", "value": "pg"},
                {"name": "TOKEN", "value": "stale"},
                {"name": "PG_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "s", "key": "p"}}},
            ]}]}}},
        });
        let plan = env_cleanup_plan(&desired, &live).expect("there is something to clean");
        assert_eq!(plan.dropped, 1);
        assert_eq!(
            plan.names,
            vec!["PG_PASSWORD".to_string(), "TOKEN".to_string()]
        );
        assert_eq!(
            plan.ops,
            vec![
                json!({"op": "remove", "path": "/spec/template/spec/containers/0/env/2"}),
                json!({"op": "remove", "path": "/spec/template/spec/containers/0/env/1/value"}),
            ]
        );
        // 已经对齐的对象不给任何操作：这条在 apply 之前跑，空 patch 也是白跑一趟。
        assert!(env_cleanup_plan(&desired, &desired).is_none());
    }

    #[test]
    fn both_cleanups_land_in_one_ordered_patch() {
        // 整条删与删 `.value` 各自的下标都按原始对象算；合成一次、倒序应用。
        let whole = vec![SupersededEnvValue {
            container_field: "containers",
            container: 0,
            env: 0,
            name: "GONE".into(),
        }];
        let value = vec![SupersededEnvValue {
            container_field: "containers",
            container: 0,
            env: 2,
            name: "B".into(),
        }];
        let ops = env_cleanup_patch_ops(&whole, &value, POD_TEMPLATE);
        assert_eq!(
            ops,
            vec![
                json!({"op": "remove", "path": "/spec/template/spec/containers/0/env/2/value"}),
                json!({"op": "remove", "path": "/spec/template/spec/containers/0/env/0"}),
            ]
        );
    }

    #[test]
    fn the_live_index_is_the_one_reported() {
        // 清单顺序和 live 顺序可以不一样，patch 必须打 live 的下标
        let d = desired(json!([
            {"name": "A", "valueFrom": {"secretKeyRef": {}}},
            {"name": "B", "valueFrom": {"secretKeyRef": {}}}
        ]));
        let l = live(json!([
            {"name": "B", "value": "x"},
            {"name": "A", "value": "y"}
        ]));
        let mut found = superseded_env_values(&d, &l);
        found.sort_by_key(|f| f.env);
        assert_eq!(
            found
                .iter()
                .map(|f| (f.name.as_str(), f.env))
                .collect::<Vec<_>>(),
            vec![("B", 0), ("A", 1)]
        );
    }
}
