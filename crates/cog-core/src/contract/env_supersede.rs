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
        format!(
            "/spec/template/spec/{}/{}/env/{}/value",
            self.container_field, self.container, self.env
        )
    }
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
    let mut out = Vec::new();
    for field in CONTAINER_FIELDS {
        let desired_containers = containers(desired, field);
        let live_containers = containers(live, field);
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

fn containers<'a>(doc: &'a Value, field: &str) -> &'a [Value] {
    doc.pointer(&format!("/spec/template/spec/{field}"))
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
