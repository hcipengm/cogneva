//! 导出备份包：页面上点一次，跑一趟 `cogneva backup`，包落在页面上能下载的地方。
//!
//! 为什么不是「页面自己打包」：包的权威写法在 `cogneva backup` 里——它知道该按什么
//! 顺序导出哪些面、清单怎么写、口令怎么决定凭证进不进包。页面这边只做三件事：起一个
//! 跑那趟命令的 Job、把跑出来的包列出来、把选中的那份流回去。重写一遍打包逻辑就是
//! 第二份规格，而两份规格的第一处分歧会出现在「包里到底有没有凭证」这种没人看得见
//! 的地方。
//!
//! Job 的形状不在这里另写一份：它从**每日备份那个 CronJob 的 jobTemplate 克隆**。
//! 镜像、env、卷、亲和都由那一份承载；克隆时只改三处——名字、发布目录、以及（当调用
//! 方给了口令时）那把口令。另写一份 Pod 模板意味着今天加一个卷要记得改两处，而漏改
//! 的那一处表现是「手动导出的包缺一段数据」。
//!
//! 口令写在 Job 的 env 里，这一条写明白：它能被任何能读这个命名空间 Job 的人看到。
//! 它是**用户为这一份包选的口令**，不是平台凭证——平台凭证仍然只在安全网关。选这条
//! 路的代价是这份口令在 Job 被 TTL 清掉之前留在 etcd 里；换来的是导出这条路不需要
//! 新建 Secret、不需要清理孤儿，也不会有「口令还在、Job 没了」的半截状态。
//!
//! 默认不加密：包里不带凭证（凭证只在安全网关，且只有密文包才导出那两片），剩下的
//! 是整库数据本身。页面把这句话印在密码框旁边，而不是让用户猜。

use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::path::PathBuf;

use crate::llm_admin::KubeClient;

/// 包名前后缀。声明在 cog-core，出包那侧读的是同一份：这里认不出来的名字，
/// 页面就不会把它列出来，而「一个包也没有」正是第一次备份还没跑完时的读数。
pub const PACKAGE_PREFIX: &str = cog_core::BACKUP_PACKAGE_PREFIX;
pub const PACKAGE_SUFFIX: &str = cog_core::BACKUP_PACKAGE_SUFFIX;

/// 页面上要的口令最短长度。
///
/// 下限不是安全强度，是「别让 `123456` 变成一份密文包的钥匙」：密文包里有整库数据，
/// 而它会被复制到别处、被邮件带走。口令太短时整单拒绝，页面照原话显示这条下限。
pub const MIN_PASSPHRASE_CHARS: usize = 12;

/// 下载时一次读多少字节。
///
/// 定成常量而不是写在读循环里：这个数决定一个管理员下一次下载最多在 Pod 里
/// 占多少内存，而它乘上并发下载数就是这一面可能吃掉的峰值——一个写在循环里的
/// 字面量没人能沿着这条线去核对。
pub const DOWNLOAD_CHUNK_BYTES: usize = 64 * 1024;

/// 发布目录在数据卷上的相对位置。备份 Job 与应用 Pod 挂的是同一块卷，路径相对
/// [`cog_core::paths::DATA_DIR`] 取，两边都不写死一个绝对路径。
pub const EXPORTS_SUBDIR: &str = "exports";

/// 克隆出来的 Job 带这个标签。列表与「有没有在跑」都按它筛，不按名字前缀——
/// 名字里含时刻，按名字前缀筛等于每次都要重新猜一次命名规则。
pub const EXPORT_LABEL_KEY: &str = "cogneva.io/backup-export";
pub const EXPORT_LABEL_VALUE: &str = "manual";

/// 这一面要的那份权限清单。它不是随手写的路径：建 Job 被拒时给出的修法就是
/// `kubectl apply` 这一份，所以必须与树里的文件名是同一个字符串（有测试按它去读文件）。
pub const EXPORT_RBAC_MANIFEST: &str = "deploy/k3s/backup-export-rbac.yaml";

/// 出包那一侧读的口令变量的名字。声明在 cog-core，写端读的是同一份：两处各写
/// 一遍时改一处不会报错，写端会去找一个再没人设置的名字，而「没给口令」正是它
/// 读不到时给出的说法——结果是「看起来没要求加密」，而不是「口令的变量名换了」。
pub const PACKAGE_KEY_ENV: &str = cog_core::env::BACKUP_PACKAGE_KEY;

/// 每日备份那个 CronJob 的名字。它由 chart 渲染，是三份 profile 共用的一份事实。
const CRONJOB_ENV: &str = "COGNEVA_BACKUP_CRONJOB";
const DEFAULT_CRONJOB: &str = "cogneva-backup";

/// 发布目录在容器里的位置。与 [`EXPORTS_SUBDIR`] 是同一处事实的两个写法。
fn publish_dir() -> PathBuf {
    cog_core::paths::data_path(EXPORTS_SUBDIR)
}

fn cronjob_name() -> String {
    std::env::var(CRONJOB_ENV)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_CRONJOB.to_string())
}

/// 这个词是不是一份能列出来、能下载的包：前缀后缀对得上，且只有一层文件名。
///
/// 判整串而不是判子串：`../cogneva-backup-x.tar.zst` 含前缀和后缀，但它指的是
/// 发布目录外面。路径里出现分隔符就不是这一层的名字。
pub fn is_package_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && name != "."
        && name != ".."
        && name.starts_with(PACKAGE_PREFIX)
        && name.ends_with(PACKAGE_SUFFIX)
        && name.len() > PACKAGE_PREFIX.len() + PACKAGE_SUFFIX.len()
}

#[derive(Debug, serde::Deserialize)]
pub struct ExportRequest {
    /// 给这一份包用的口令。缺席 = 不加密：包里是明文数据，不含凭证。
    #[serde(default)]
    pub passphrase: Option<String>,
}

/// 从 CronJob 克隆出这一趟要跑的 Job。
///
/// 纯函数：给进 CronJob 的 JSON、这次的名字与口令，出 Job 的 JSON。三处改动各自
/// 都能单独说清为什么，所以它们各自留一句话在代码里，而不是一句「调整一下模板」。
fn export_job(cronjob: &Value, name: &str, passphrase: Option<&str>) -> Result<Value, String> {
    let mut spec = cronjob
        .pointer("/spec/jobTemplate/spec")
        .cloned()
        .ok_or("备份 CronJob 里没有 jobTemplate.spec，导不出一个跑得起来的 Job")?;

    // 1. 数据卷要**可写**：克隆来的是每日备份的模板，它把数据卷挂成只读（备份绝不
    //    写数据面）。发布目录就在这块卷上，只读挂载会让这一趟在最后一步失败——而
    //    那时包已经写完，失败看起来像「备份坏了」而不是「挂载是只读的」。
    let mounts = spec
        .pointer_mut("/template/spec/containers/0/volumeMounts")
        .and_then(Value::as_array_mut)
        .ok_or("备份 Job 模板里没有容器的 volumeMounts，无法把发布目录写进去")?;
    let data_mount_path = cog_core::paths::DATA_DIR;
    let Some(data_mount) = mounts
        .iter_mut()
        .find(|mount| mount.get("mountPath").and_then(Value::as_str) == Some(data_mount_path))
    else {
        return Err(format!(
            "备份 Job 模板没有把 {data_mount_path} 挂进来，导出的包没有地方落"
        ));
    };
    data_mount["readOnly"] = Value::Bool(false);

    // 2. 发布目录 + 口令。发布目录无条件写：不给它，这一趟的包就只落在备份卷上，
    //    页面永远列不出来，而「导出成功」会是假的。
    let env = spec
        .pointer_mut("/template/spec/containers/0/env")
        .and_then(Value::as_array_mut)
        .ok_or("备份 Job 模板里没有容器的 env，无法把发布目录写进去")?;
    env.retain(|entry| {
        entry.get("name").and_then(Value::as_str) != Some(cog_core::env::BACKUP_PUBLISH_DIR)
            && entry.get("name").and_then(Value::as_str) != Some(PACKAGE_KEY_ENV)
    });
    env.push(json!({
        "name": cog_core::env::BACKUP_PUBLISH_DIR,
        "value": publish_dir().to_string_lossy(),
    }));
    if let Some(passphrase) = passphrase {
        env.push(json!({
            "name": PACKAGE_KEY_ENV,
            "value": passphrase,
        }));
    }

    // 3. 名字与标签。名字里带时刻：同一个名字建两次会被 apiserver 拒（409），而那
    //    恰好是对的——第二趟导出是另一份包，不是同一份。
    //    标签两面都打：Job 自己用来列表，Pod 用来在日志与事件里认领。
    let labels = json!({ EXPORT_LABEL_KEY: EXPORT_LABEL_VALUE });
    Ok(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": labels,
        },
        "spec": {
            "backoffLimit": spec.get("backoffLimit").cloned().unwrap_or(json!(0)),
            "activeDeadlineSeconds": spec.get("activeDeadlineSeconds").cloned(),
            // 这一趟是一次性的、有人等着的翻页动作：一小时之后把 Job 连同它的
            // env（含口令）一起从 etcd 里清掉。包在卷上，不受这条影响。
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": { "labels": labels },
                "spec": spec.pointer("/template/spec").cloned().unwrap_or(Value::Null),
            },
        },
    }))
}

/// POST /api/v1/admin/backup/export — 起一趟导出。
pub async fn export_handler(Json(request): Json<ExportRequest>) -> Response {
    let passphrase = request
        .passphrase
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(passphrase) = &passphrase {
        if passphrase.chars().count() < MIN_PASSPHRASE_CHARS {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "code": "passphrase_too_short",
                    "message": format!(
                        "口令至少 {MIN_PASSPHRASE_CHARS} 个字符。这份包里是整库数据，\
                         它会被复制到别处；短口令让加密只剩一个仪式。"
                    ),
                })),
            )
                .into_response();
        }
    }

    let client = match KubeClient::in_cluster() {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "code": "not_in_cluster",
                    "message": format!("导出要起一个 Job，而这个进程不在集群里：{e}"),
                })),
            )
                .into_response();
        }
    };

    match start_export(&client, passphrase.as_deref()).await {
        Ok((job, encrypted)) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "job": job,
                "encrypted": encrypted,
                "publish_dir": publish_dir().to_string_lossy(),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({ "code": "export_refused", "message": e })),
        )
            .into_response(),
    }
}

/// 把 apiserver 的 403 翻成本面的病因与修法。
///
/// 建 Job 被拒几乎从来不是这次请求写错了：这一面要的那份 Role 只有**安装面**会写。
/// 滚动面按设计不下发 Role/RoleBinding——K8s 的反提权规则要求 apply 一个 Role 的
/// 发起者已经持有其中全部权限，部署器不该持有发布集里管理类 Role 的权限，所以一份
/// 新增的权限清单在有人重跑安装面之前**到不了集群**，而应用侧一切正常。
///
/// 2026-10-27 实测：`deploy/k3s/backup-export-rbac.yaml` 在 kustomization 里、
/// 三份渲染产物也都有它，但集群里没有这个 Role——那份清单是之后才加的，安装面没有
/// 再跑过。原文的 403 读起来像程序 bug，实际是集群少了一份权限；把这句话说出来，
/// 下一个人不必再沿 "jobs.batch is forbidden" 往回追一遍。
fn explain_forbidden_job_creation(error: &str) -> Option<String> {
    let start = error.find('{')?;
    // 只取第一个 JSON 值：apiserver 的 Status 后面可能还跟着调用方加的注脚。
    let status: Value = serde_json::Deserializer::from_str(&error[start..])
        .into_iter::<Value>()
        .next()?
        .ok()?;
    let forbidden = status.get("reason").and_then(Value::as_str) == Some("Forbidden")
        || status.get("code").and_then(Value::as_u64) == Some(403);
    let group = status
        .pointer("/details/group")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let kind = status
        .pointer("/details/kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !(forbidden && group == "batch" && kind == "jobs") {
        return None;
    }
    Some(format!(
        "集群里没有这一趟导出要的权限：建 Job 被拒。滚动面按设计不下发 Role/RoleBinding\
         （K8s 的反提权规则要求 apply 一个 Role 的发起者已持有其中全部权限），所以这份\
         权限只有在安装面跑过之后才在集群里。修法：kubectl apply -f {EXPORT_RBAC_MANIFEST}\
         （或重跑安装）。apiserver 原文：{error}"
    ))
}

async fn start_export(
    client: &KubeClient,
    passphrase: Option<&str>,
) -> Result<(String, bool), String> {
    let namespace = client.namespace();
    // 一次只跑一趟。两趟同时跑时它们会各自对备份卷做保留裁剪，先跑完的那趟会把
    // 后跑完那趟刚写下的包当成「超出保留数」删掉——包没了，而两趟都自报成功。
    if let Ok(jobs) = client
        .get_json(&format!(
            "/apis/batch/v1/namespaces/{namespace}/jobs?labelSelector={EXPORT_LABEL_KEY}%3D{EXPORT_LABEL_VALUE}"
        ))
        .await
    {
        if jobs
            .pointer("/items")
            .and_then(Value::as_array)
            .map(|items| {
                items.iter().any(|job| {
                    job.pointer("/status/active")
                        .and_then(Value::as_i64)
                        .unwrap_or(0)
                        > 0
                })
            })
            .unwrap_or(false)
        {
            return Err("已经有一趟导出在跑；等它结束再起下一趟".into());
        }
    }

    let cronjob = client
        .get_json(&format!(
            "/apis/batch/v1/namespaces/{namespace}/cronjobs/{}",
            cronjob_name()
        ))
        .await
        .map_err(|e| format!("读不到备份 CronJob：{e}。没有它就没有可克隆的 Pod 模板"))?;

    // 毫秒：秒级在「上一趟刚跑完、TTL 还没把它清掉」的窗口里会撞名，而 apiserver 对
    // 重名答 409——那次失败读起来像导出坏了，其实只是名字撞了。
    let name = format!(
        "cogneva-backup-export-{}",
        chrono::Utc::now().timestamp_millis()
    );
    let job = export_job(&cronjob, &name, passphrase)?;
    client
        .post_json(&format!("/apis/batch/v1/namespaces/{namespace}/jobs"), job)
        .await
        .map_err(|e| {
            let text = e.to_string();
            explain_forbidden_job_creation(&text)
                .unwrap_or_else(|| format!("起不来这一趟导出：{text}"))
        })?;
    Ok((name, passphrase.is_some()))
}

/// GET /api/v1/admin/backup/packages — 已经导出的包，以及现在有没有在跑一趟。
pub async fn list_handler() -> Response {
    let dir = publish_dir();
    let mut packages: Vec<Value> = Vec::new();
    match std::fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !is_package_name(&name) {
                    continue;
                }
                let Ok(metadata) = entry.metadata() else {
                    continue;
                };
                let modified = metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|since| since.as_secs())
                    .unwrap_or(0);
                packages.push(json!({
                    "name": name,
                    "bytes": metadata.len(),
                    "created_at": modified,
                }));
            }
        }
        // 目录还不存在 = 一份都没导出过。这与「读不到」是两件事，但在这个面上
        // 给出的答案相同：没有东西可以下载。区别留在日志里。
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "code": "read_failed", "message": e.to_string() })),
            )
                .into_response();
        }
    }
    // 新的在前：包名里的时间戳是定长 UTC，字典序就是时间序。
    packages.sort_by(|a, b| b["name"].as_str().cmp(&a["name"].as_str()));

    let running = match KubeClient::in_cluster() {
        Ok(client) => export_running(&client).await,
        // 不在集群里时列表照样可用（包的权威在卷上），只是没有可问的 apiserver：
        // 这里按「不在跑」作答，页面因此不会再轮询——而这个进程本来也起不了 Job。
        Err(_) => false,
    };
    (
        StatusCode::OK,
        Json(json!({ "running": running, "packages": packages })),
    )
        .into_response()
}

async fn export_running(client: &KubeClient) -> bool {
    let namespace = client.namespace();
    let Ok(jobs) = client
        .get_json(&format!(
            "/apis/batch/v1/namespaces/{namespace}/jobs?labelSelector={EXPORT_LABEL_KEY}%3D{EXPORT_LABEL_VALUE}"
        ))
        .await
    else {
        return false;
    };
    jobs.pointer("/items")
        .and_then(Value::as_array)
        .map(|items| {
            items.iter().any(|job| {
                job.pointer("/status/active")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    > 0
            })
        })
        .unwrap_or(false)
}

/// GET /api/v1/admin/backup/packages/{name} — 把一份包流回浏览器。
pub async fn download_handler(Path(name): Path<String>) -> Response {
    if !is_package_name(&name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "code": "bad_name", "message": "这不是一个包的名字" })),
        )
            .into_response();
    }
    let path = publish_dir().join(&name);
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "code": "not_found", "message": format!("{name} 不在发布目录里") })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "code": "open_failed", "message": e.to_string() })),
            )
                .into_response();
        }
    };
    // 分块读而不是整份读进内存：一份包是几十到几百 MB，而下载是这一面上唯一
    // 可能同时有几个人在点的动作。把包读成 Vec 会把「几个管理员同时下载」变成
    // 「Pod 内存涨到 limit」。
    let stream = futures::stream::try_unfold(file, |mut file| async move {
        use tokio::io::AsyncReadExt;
        let mut chunk = vec![0u8; DOWNLOAD_CHUNK_BYTES];
        let read = file.read(&mut chunk).await?;
        if read == 0 {
            return Ok::<Option<(axum::body::Bytes, tokio::fs::File)>, std::io::Error>(None);
        }
        chunk.truncate(read);
        Ok(Some((axum::body::Bytes::from(chunk), file)))
    });
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/zstd"),
            (
                header::CONTENT_DISPOSITION,
                &format!("attachment; filename=\"{name}\""),
            ),
        ],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

/// DELETE /api/v1/admin/backup/packages/{name} — 删掉一份已经下载走的包。
///
/// 删除是这一面唯一的破坏性动作，所以它必须由人来点：导出目录不受备份卷的保留策略
/// 管，一份包会一直躺着，而「自动删掉最旧的」在导出这个面上就是「删掉用户还没取走
/// 的东西」。
pub async fn delete_handler(Path(name): Path<String>) -> Response {
    if !is_package_name(&name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "code": "bad_name", "message": "这不是一个包的名字" })),
        )
            .into_response();
    }
    let path = publish_dir().join(&name);
    match tokio::fs::remove_file(&path).await {
        Ok(()) => (StatusCode::OK, Json(json!({ "deleted": name }))).into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({ "code": "not_found", "message": format!("{name} 不在发布目录里") })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "code": "delete_failed", "message": e.to_string() })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cronjob() -> Value {
        json!({
            "spec": {
                "jobTemplate": {
                    "spec": {
                        "backoffLimit": 1,
                        "activeDeadlineSeconds": 3600,
                        "template": {
                            "spec": {
                                "containers": [{
                                    "name": "backup",
                                    "image": "cogneva:test",
                                    "args": ["backup"],
                                    "env": [
                                        {"name": "COGNEVA_BACKUP_KEEP", "value": "7"}
                                    ],
                                    "volumeMounts": [
                                        {"name": "data", "mountPath": "/var/lib/cogneva-data", "readOnly": true},
                                        {"name": "backup", "mountPath": "/backups"}
                                    ]
                                }],
                                "volumes": [
                                    {"name": "data", "persistentVolumeClaim": {"claimName": "cogneva-data-pvc"}},
                                    {"name": "backup", "persistentVolumeClaim": {"claimName": "cogneva-backup-pvc"}}
                                ]
                            }
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn a_package_name_is_one_flat_file_with_the_prefix_and_suffix() {
        assert!(is_package_name("cogneva-backup-20261027T031700Z.tar.zst"));
        assert!(
            !is_package_name("cogneva-backup-.tar.zst"),
            "空时间戳不是包"
        );
        assert!(!is_package_name(
            "../cogneva-backup-20261027T031700Z.tar.zst"
        ));
        assert!(!is_package_name(
            "cogneva-backup-20261027T031700Z.tar.zst/extra"
        ));
        assert!(!is_package_name("readme.txt"));
        assert!(!is_package_name(""));
    }

    #[test]
    fn a_plain_export_carries_the_publish_dir_and_no_passphrase() {
        let job = export_job(&cronjob(), "cogneva-backup-export-1", None).unwrap();
        let env = job.pointer("/spec/template/spec/containers/0/env").unwrap();
        let names: Vec<&str> = env
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|entry| entry["name"].as_str())
            .collect();
        assert!(names.contains(&cog_core::env::BACKUP_PUBLISH_DIR));
        assert!(
            !names.contains(&PACKAGE_KEY_ENV),
            "没给口令就不该有口令那一格：加密由它自己的存在与否决定"
        );
        assert!(
            names.contains(&"COGNEVA_BACKUP_KEEP"),
            "模板其余 env 原样保留"
        );
    }

    #[test]
    fn an_encrypted_export_passes_the_passphrase_through() {
        let job = export_job(
            &cronjob(),
            "cogneva-backup-export-2",
            Some("a long enough passphrase"),
        )
        .unwrap();
        let env = job.pointer("/spec/template/spec/containers/0/env").unwrap();
        let key = env
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == PACKAGE_KEY_ENV)
            .expect("给了口令就要有那一格");
        assert_eq!(key["value"], "a long enough passphrase");
    }

    #[test]
    fn the_data_volume_is_writable_so_the_package_can_be_published() {
        let job = export_job(&cronjob(), "cogneva-backup-export-3", None).unwrap();
        let mount = job
            .pointer("/spec/template/spec/containers/0/volumeMounts")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .find(|mount| mount["mountPath"] == cog_core::paths::DATA_DIR)
            .unwrap();
        assert_eq!(mount["readOnly"], false);
        // 其它挂载不被顺手改掉：只读是每日备份的保护，克隆不是放宽它的机会。
        let backup = job
            .pointer("/spec/template/spec/containers/0/volumeMounts")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .find(|mount| mount["mountPath"] == "/backups")
            .unwrap();
        assert!(backup.get("readOnly").is_none());
    }

    #[test]
    fn the_job_is_labelled_so_a_second_export_can_see_the_first() {
        let job = export_job(&cronjob(), "cogneva-backup-export-4", None).unwrap();
        assert_eq!(
            job["metadata"]["labels"][EXPORT_LABEL_KEY],
            EXPORT_LABEL_VALUE
        );
        assert_eq!(
            job.pointer("/spec/template/metadata/labels").unwrap()[EXPORT_LABEL_KEY],
            EXPORT_LABEL_VALUE,
            "Pod 上也要有：日志与事件是按 Pod 标签找的"
        );
    }

    #[test]
    fn a_template_that_does_not_mount_the_data_volume_is_refused() {
        let mut cronjob = cronjob();
        cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
            ["volumeMounts"] = json!([{"name": "backup", "mountPath": "/backups"}]);
        let err = export_job(&cronjob, "cogneva-backup-export-5", None).unwrap_err();
        assert!(err.contains(cog_core::paths::DATA_DIR), "{err}");
    }

    /// apiserver 对「建 Job 被拒」的原文。逐字取自 2026-10-27 线上那一次：
    /// 那份权限清单进了仓库、进了 kustomization、进了三份渲染产物，就是没进集群。
    const FORBIDDEN_JOB_BODY: &str = r#"apiserver 返回 403 Forbidden: {"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"jobs.batch is forbidden: User \"system:serviceaccount:cogneva:cogneva\" cannot create resource \"jobs\" in API group \"batch\" in the namespace \"cogneva\"","reason":"Forbidden","details":{"group":"batch","kind":"jobs"},"code":403}"#;

    #[test]
    fn a_forbidden_job_creation_names_the_manifest_that_grants_it() {
        let message = explain_forbidden_job_creation(FORBIDDEN_JOB_BODY)
            .expect("建 Job 被拒要有自己的病因，不能被当成一条普通失败");
        assert!(
            message.contains(EXPORT_RBAC_MANIFEST),
            "修法要点名那份清单，否则读者只能从 403 往回猜：{message}"
        );
        assert!(
            message.contains("jobs.batch is forbidden"),
            "原文要留着：判词是这一面拼的，出错时两边的说法都得在手：{message}"
        );
    }

    /// 判词给的修法必须在树里成立：那份清单读不到、或者它不再给这个 SA 建 Job 的权限，
    /// 上面那条消息就变成一句谎话——而它正好是在人最没空核对的时候被读到。
    #[test]
    fn the_manifest_the_message_names_still_grants_this_workload_the_right() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let manifest = std::fs::read_to_string(root.join(EXPORT_RBAC_MANIFEST))
            .unwrap_or_else(|e| panic!("判词点名的清单读不到（{EXPORT_RBAC_MANIFEST}）：{e}"));
        let deployment = std::fs::read_to_string(root.join("deploy/k3s/deployment.yaml"))
            .expect("应用 Deployment 应当可读");
        // 跑这段代码的是哪一份 SA，从清单里读，不在这里写死：改 SA 名的那天，
        // 判词指的绑定会落空，而这段代码自己不会有任何感觉。
        let service_account = deployment
            .lines()
            .find_map(|line| line.trim().strip_prefix("serviceAccountName:"))
            .map(|value| value.trim().to_string())
            .expect("应用 Deployment 里应有 serviceAccountName");

        let docs = manifest
            .split("\n---")
            .map(|doc| serde_yaml_value(doc))
            .collect::<Vec<Value>>();
        let role = docs
            .iter()
            .find(|doc| doc["kind"] == "Role")
            .expect("清单里要有 Role");
        let grants_create_jobs = role["rules"]
            .as_array()
            .expect("Role.rules 是列表")
            .iter()
            .any(|rule| {
                rule["apiGroups"] == json!(["batch"])
                    && rule["resources"] == json!(["jobs"])
                    && rule["verbs"]
                        .as_array()
                        .map(|verbs| verbs.contains(&json!("create")))
                        .unwrap_or(false)
            });
        assert!(
            grants_create_jobs,
            "判词说这条路能修好，而 {EXPORT_RBAC_MANIFEST} 已经不给 batch/jobs 的 create 了"
        );

        let bound = docs
            .iter()
            .find(|doc| doc["kind"] == "RoleBinding")
            .expect("清单里要有 RoleBinding");
        assert_eq!(bound["roleRef"]["name"], role["metadata"]["name"]);
        let subjects = bound["subjects"].as_array().expect("subjects 是列表");
        assert!(
            subjects.iter().any(|subject| {
                subject["kind"] == "ServiceAccount"
                    && subject["name"].as_str() == Some(service_account.as_str())
            }),
            "{EXPORT_RBAC_MANIFEST} 绑的不是跑这段代码的那份 SA（{service_account}）：判词指的修法落空"
        );
    }

    #[test]
    fn other_refusals_are_not_dressed_up_as_a_missing_role() {
        // 404：CronJob 不在。那是另一件事，不能也被说成缺权限。
        let not_found = r#"apiserver 返回 404 Not Found: {"kind":"Status","reason":"NotFound","code":404,"details":{"group":"batch","kind":"cronjobs"}}"#;
        assert!(explain_forbidden_job_creation(not_found).is_none());
        // 403 但拒的不是 jobs：同样不该套这句话。
        let other = r#"apiserver 返回 403 Forbidden: {"kind":"Status","reason":"Forbidden","code":403,"details":{"group":"batch","kind":"cronjobs"}}"#;
        assert!(explain_forbidden_job_creation(other).is_none());
        // 根本不是 apiserver 的 Status（连不上、超时、连接被拒）。
        assert!(explain_forbidden_job_creation("connection refused").is_none());
    }

    /// 清单是 YAML，读它的判据也只能是 YAML；这里用 `serde_json` 的解析器会把
    /// `kubectl apply` 认得的东西读成另一份。
    fn serde_yaml_value(doc: &str) -> Value {
        serde_json::to_value(
            serde_yaml::from_str::<serde_yaml::Value>(doc).expect("清单里每一段都应当是合法 YAML"),
        )
        .expect("YAML 应当能转成 JSON 形状")
    }

    #[test]
    fn a_short_passphrase_is_refused_before_anything_is_created() {
        // 判据在 handler 里，之所以也在这里写一条：长度下限是这一面的约定，
        // 调短它的时候应该有一条测试红，而不是悄悄放宽。条件全是常量，所以写成
        // 常量块——编译期就能红，不必等到有人跑这条测试。
        const { assert!(MIN_PASSPHRASE_CHARS >= 12) };
    }
}
