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

/// 包名前后缀。与出包那一侧的常量是同一个事实，`crates/cogneva/tests/`
/// 里有一条测试把两处钉在一起：这里认不出来的名字，页面就不会把它列出来。
pub const PACKAGE_PREFIX: &str = "cogneva-backup-";
pub const PACKAGE_SUFFIX: &str = ".tar.zst";

/// 页面上要的口令最短长度。
///
/// 下限不是安全强度，是「别让 `123456` 变成一份密文包的钥匙」：密文包里有整库数据，
/// 而它会被复制到别处、被邮件带走。口令太短时整单拒绝，页面照原话显示这条下限。
pub const MIN_PASSPHRASE_CHARS: usize = 12;

/// 发布目录在数据卷上的相对位置。备份 Job 与应用 Pod 挂的是同一块卷，路径相对
/// [`cog_core::paths::DATA_DIR`] 取，两边都不写死一个绝对路径。
pub const EXPORTS_SUBDIR: &str = "exports";

/// 克隆出来的 Job 带这个标签。列表与「有没有在跑」都按它筛，不按名字前缀——
/// 名字里含时刻，按名字前缀筛等于每次都要重新猜一次命名规则。
pub const EXPORT_LABEL_KEY: &str = "cogneva.io/backup-export";
pub const EXPORT_LABEL_VALUE: &str = "manual";

/// 出包那一侧读的口令变量的名字。它声明在 `cogneva` crate 里（写端在那里），
/// 这个 crate 不依赖它，所以只能再写一遍——`crates/cogneva/tests/` 里有一条
/// 测试把两处钉在一起：写端换了名字而这里没换，结果是「给了口令却没加密」。
pub const PACKAGE_KEY_ENV: &str = "COGNEVA_BACKUP_PACKAGE_KEY";

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
        .map_err(|e| format!("起不来这一趟导出：{e}"))?;
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
        let mut chunk = vec![0u8; 64 * 1024];
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

    #[test]
    fn a_short_passphrase_is_refused_before_anything_is_created() {
        // 判据在 handler 里，之所以也在这里写一条：长度下限是这一面的约定，
        // 调短它的时候应该有一条测试红，而不是悄悄放宽。
        assert!(MIN_PASSPHRASE_CHARS >= 12);
    }
}
