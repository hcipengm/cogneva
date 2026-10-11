//! 两个 Secret 对象的键位表：哪个键住哪一片，以及按片读写的原语。
//!
//! 实例的凭证分成两个对象，分家的依据是**读者面**而不是凭证种类：一片只被安全
//! 网关读（`cogneva-secrets-oob`），一片被业务 Pod 读（`cogneva-secrets`）。
//! 这条依据是可核的——两片的读者集合与切分前逐键相同——而"像不像凭证"不是：
//! 内部那片是**整对象** get，把只该网关读的键放进去，等于把它铺给持那一份的每
//! 一个工作负载；反过来把业务 Pod 要读的键放去带外片，那些 Pod 就读不到。
//!
//! 这里的表是**单一真源**：写路径按它分派目标对象，读路径按它拼合并视图，所以
//! 调用点不必各自知道键住哪片。查不到的键**不猜**——写进错误的对象不会报错，
//! 只会在读它的那个进程那里变成空值，而那要等到功能静默 fail-closed 才暴露。

use base64::Engine as _;
use serde_json::{Map, Value};

use crate::llm_admin::KubeClient;

/// 内部片：业务 Pod 读的键，加上本进程写的非凭证状态键。
pub(crate) const SECRET_NAME_INTERNAL: &str = "cogneva-secrets";
/// 带外片：只被安全网关读的键（另见 backup.yaml 的整体导出卷）。
pub(crate) const SECRET_NAME_OOB: &str = "cogneva-secrets-oob";

/// 带外片装这些键。与 `deploy/scripts/init-secrets.sh` 的 `OOB_KEYS` 同一张表；
/// 两边都要有是因为一侧在编译期分派、一侧在安装期搬迁，而**安装期那一侧是搬迁
/// 的唯一执行者**——少列一个键，老安装里就会留一份副本在被业务 Pod 读得到的地方。
pub(crate) const OOB_SECRET_KEYS: &[&str] = &[
    "llm-upstreams",
    "github-token",
    "gitee-token",
    "github-webhook-secret",
    "gitee-webhook-token",
    "gitee-oauth-client-secret",
    "github-oauth-client-secret",
    "notification-dingtalk-secret",
    "notification-feishu-secret",
    "github-app-id",
    "github-app-installation-id",
    "github-app-private-key",
    "git-ssh-private-key",
    "git-ssh-private-key-pending",
    "gitee-refresh-token",
    "clickhouse-password",
];

/// 内部片里**本进程会读写的键**。表不必穷举该对象里的每一个键（数据库口令那
/// 些由 chart/安装脚本写，本进程不碰），但凡是本进程要写进内部片的键都必须在这里：
/// 查不到就写不进去，那正是要的——失败当场可见，而不是写进一片没人读的地方。
pub(crate) const INTERNAL_SECRET_KEYS: &[&str] = &[
    "contribution-config",
    "git-identity-state",
    "git-identity-note",
    "git-ssh-public-key",
];

/// 键属于哪个对象；未声明的键返回 `None`，由调用点当成错误处理。
pub(crate) fn secret_object_for_key(key: &str) -> Option<&'static str> {
    if OOB_SECRET_KEYS.contains(&key) {
        Some(SECRET_NAME_OOB)
    } else if INTERNAL_SECRET_KEYS.contains(&key) {
        Some(SECRET_NAME_INTERNAL)
    } else {
        None
    }
}

pub(crate) fn secret_api_path(namespace: &str, object: &str) -> String {
    format!("/api/v1/namespaces/{namespace}/secrets/{object}")
}

/// 内部片的路径：`contribution-config` 这类非凭证状态一直住在这里，调用点不必
/// 为了一个新对象改名字。
pub(crate) fn internal_secret_api_path(namespace: &str) -> String {
    secret_api_path(namespace, SECRET_NAME_INTERNAL)
}

/// 带外片的路径：属主探测与整对象搬迁用。
pub(crate) fn oob_secret_api_path(namespace: &str) -> String {
    secret_api_path(namespace, SECRET_NAME_OOB)
}

/// 读两片并合成 `{"data": {...}}`：调用点仍按 key 取，取哪片由键表决定。形状与
/// apiserver 返回的 Secret 一致，所以原来"读一个对象再按 key 取"的调用点不必改。
///
/// 两片都必须读得到。带外那份在正常安装里由 init-secrets.sh 或 chart 建出来，
/// 读不到就是真故障——把它当成"这些键不存在"会让带外凭证被读成空值，功能静默
/// fail-closed，而日志里只有一句成功。
pub(crate) async fn load_data_view(kube: &KubeClient) -> Result<Value, String> {
    let mut data = Map::new();
    for object in [SECRET_NAME_INTERNAL, SECRET_NAME_OOB] {
        let path = secret_api_path(kube.namespace(), object);
        let secret = kube
            .get_json(&path)
            .await
            .map_err(|e| format!("读取 {object} 失败（{e}）"))?;
        if let Some(map) = secret.get("data").and_then(Value::as_object) {
            for (k, v) in map {
                data.insert(k.clone(), v.clone());
            }
        }
    }
    Ok(serde_json::json!({ "data": data }))
}

/// 把一组待写字段按键所属的对象分组，供 `kube.patch` 分别写。字段名未声明时
/// 返回错误并报出键名——见模块开头为什么这里不设默认片。
pub(crate) fn group_fields_by_object(
    fields: &Value,
) -> Result<Vec<(&'static str, Value)>, String> {
    let map = fields
        .as_object()
        .ok_or_else(|| "待写字段不是一个对象".to_string())?;
    let mut internal = Map::new();
    let mut oob = Map::new();
    for (key, value) in map {
        match secret_object_for_key(key) {
            Some(SECRET_NAME_OOB) => {
                oob.insert(key.clone(), value.clone());
            }
            Some(_) => {
                internal.insert(key.clone(), value.clone());
            }
            None => {
                return Err(format!(
                    "键 {key} 没有声明属于哪一片 Secret：写进错误的那一片不会报错，\
                     只会在读它的进程那里变成空值"
                ))
            }
        }
    }
    let mut out = Vec::new();
    if !internal.is_empty() {
        out.push((SECRET_NAME_INTERNAL, Value::Object(internal)));
    }
    if !oob.is_empty() {
        out.push((SECRET_NAME_OOB, Value::Object(oob)));
    }
    Ok(out)
}

/// 按键分派写入两片（明文 `stringData`，由 apiserver 编码）；全部成功才算成功。
/// 任一片失败即返回错误——此时另一片可能已经落盘，所以文案说"其余字段可能已落"，
/// 而不是"没写"。
pub(crate) async fn patch_fields(kube: &KubeClient, fields: Value) -> Result<(), String> {
    patch_groups(kube, group_fields_by_object(&fields)?, false).await
}

/// 同上，但写 `data`（本进程 base64）而不是 `stringData`。`null` 值在两边都是
/// "删掉这个键"。两种编码都存在是因为它们各自的调用点还兼着别的事：明文那侧更
/// 好读，base64 那侧要与同一次写入里的"置空待确认键位"保持一种编码，否则
/// "删掉的那个到底生效没有"就不可读。
pub(crate) async fn patch_fields_b64(kube: &KubeClient, fields: Value) -> Result<(), String> {
    patch_groups(kube, group_fields_by_object(&fields)?, true).await
}

async fn patch_groups(
    kube: &KubeClient,
    groups: Vec<(&'static str, Value)>,
    encode_b64: bool,
) -> Result<(), String> {
    for (object, group) in groups {
        let body = if encode_b64 {
            let engine = base64::engine::general_purpose::STANDARD;
            let mut data = Map::new();
            if let Some(map) = group.as_object() {
                for (key, value) in map {
                    match value {
                        Value::String(text) => {
                            data.insert(key.clone(), Value::String(engine.encode(text)));
                        }
                        _ => {
                            data.insert(key.clone(), Value::Null);
                        }
                    }
                }
            }
            serde_json::json!({ "data": Value::Object(data) })
        } else {
            serde_json::json!({ "stringData": group })
        };
        let path = secret_api_path(kube.namespace(), object);
        kube.patch(&path, body)
            .await
            .map_err(|e| format!("写入 {object} 失败（同一次写入里其余字段可能已落）：{e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_tables_partition_without_overlap() {
        for key in OOB_SECRET_KEYS {
            assert_eq!(secret_object_for_key(key), Some(SECRET_NAME_OOB));
            assert!(
                !INTERNAL_SECRET_KEYS.contains(key),
                "{key} 同时出现在两片里"
            );
        }
        for key in INTERNAL_SECRET_KEYS {
            assert_eq!(secret_object_for_key(key), Some(SECRET_NAME_INTERNAL));
        }
        assert_eq!(secret_object_for_key("pg-password"), None, "未声明的键不许有默认片");
    }

    #[test]
    fn groups_land_in_their_own_object() {
        let groups = group_fields_by_object(&serde_json::json!({
            "contribution-config": "{}",
            "gitee-token": "tok",
            "git-ssh-public-key": "ssh-ed25519 AAAA",
        }))
        .unwrap();
        let names: Vec<&str> = groups.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, vec![SECRET_NAME_INTERNAL, SECRET_NAME_OOB]);
        assert!(groups[0].1.get("contribution-config").is_some());
        assert!(groups[1].1.get("gitee-token").is_some());
        assert!(groups[1].1.get("git-ssh-public-key").is_none());
    }

    #[test]
    fn an_undeclared_key_is_an_error_not_a_default() {
        let err = group_fields_by_object(&serde_json::json!({"who-knows": "v"})).unwrap_err();
        assert!(err.contains("who-knows"), "报错要带上键名: {err}");
    }
}
