//! 评测报告的落盘与列举。
//!
//! 一次评测跑完，报告此前只跟着那一次 HTTP 响应走掉。读面 `/api/v1/eval/report/{id}`
//! 找的是 `{data_dir}/eval/reports/{id}.json`，却没有任何一处在写这份文件——这条读面
//! 永远 404，而「读不到」与「没跑过」在界面上长得一样。
//!
//! 这里只做两件事：把跑完的报告写进那个目录，把目录里的报告列出来。写与列都按目录
//! 说话，不引入第二份账本——盘上有什么就是有什么，不会与某个内存索引各说一套。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::GatewayState;

/// 一次列举最多返回多少份。
///
/// 列举要按修改时间排序，并且为了给出数据集名要读每一份的正文——每个返回项一次
/// 读。不设上限时，「列一下目录」会退化成按文件数读盘，所以上限落在这里，同时把
/// 未截断的总数报出去：调用方必须能看出自己拿到的是前缀。
pub const DEFAULT_LIST_LIMIT: usize = 200;

/// 报告目录：`{data_dir}/eval/reports`。
pub fn reports_dir(data_dir: &str) -> PathBuf {
    Path::new(data_dir)
        .join(crate::eval_datasets::EVAL_SUBDIR)
        .join("reports")
}

/// 一次评测的报告 id：数据集名 + 跑完的时刻。
///
/// 数据集名要进文件名，所以路径分隔符、冒号与控制字符都换成下划线：id 随后会被
/// 拼进路径，一个带 `/` 的名字会把报告写到目录外面去。
pub fn new_report_id(dataset_name: &str, at: DateTime<Utc>) -> String {
    let stem: String = dataset_name
        .chars()
        .map(|c| {
            if c == '/' || c == '\\' || c == ':' || c.is_control() || c.is_ascii_whitespace() {
                '_'
            } else {
                c
            }
        })
        .collect();
    let stem = if stem.trim_matches('_').is_empty() {
        "eval".to_string()
    } else {
        stem
    };
    format!("{}_{}", stem, at.format("%Y%m%d_%H%M%S"))
}

/// 把一份报告写进目录，返回最终的 id 与路径。
///
/// 先写临时文件再改名：列举是直接读目录，读者不该有机会看见一个写了一半的 JSON。
/// 同一秒内同一个数据集连跑两次时加序号，不覆盖前一份——覆盖会让「上一次跑成什么样」
/// 永远消失，而那正是对比两次评测时要看的东西。
pub fn store_report(dir: &Path, id: &str, report_json: &str) -> std::io::Result<(String, PathBuf)> {
    std::fs::create_dir_all(dir)?;
    let mut final_id = id.to_string();
    let mut attempt = 1usize;
    let path = loop {
        let candidate = dir.join(format!("{final_id}.json"));
        if !candidate.exists() {
            break candidate;
        }
        attempt += 1;
        final_id = format!("{id}-{attempt}");
    };
    let tmp = dir.join(format!(".{final_id}.json.tmp"));
    std::fs::write(&tmp, report_json)?;
    std::fs::rename(&tmp, &path)?;
    Ok((final_id, path))
}

/// 目录里的一份报告，只带选择下一份读所需的元数据。
#[derive(Debug, Serialize)]
pub struct StoredReport {
    pub id: String,
    pub dataset_name: String,
    pub created_at: String,
    pub size_bytes: u64,
}

/// 一次列举的结果：**返回的**与前缀**之前的总数**分开给。
pub struct ReportListing {
    pub reports: Vec<StoredReport>,
    pub total: usize,
}

/// 列出目录里的报告，新的在前。
///
/// 目录不存在时回一份空列举而不是错误：还没跑过评测的部署本来就是空目录，把它报成
/// 读失败，会让「没跑过」和「读不通」看起来一样。正文读不出来时才放弃那一份——目录
/// 里有别的进程写坏的字节，不该让整张列表消失。
pub fn list_reports(dir: &Path, limit: usize) -> std::io::Result<ReportListing> {
    if !dir.is_dir() {
        return Ok(ReportListing {
            reports: Vec::new(),
            total: 0,
        });
    }

    let mut files: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        files.push((path, modified));
    }

    let total = files.len();
    files.sort_by_key(|(_, modified)| std::cmp::Reverse(*modified));

    let mut reports = Vec::new();
    for (path, modified) in files.into_iter().take(limit) {
        let Some(id) = path.file_stem().and_then(|s| s.to_str()).map(String::from) else {
            continue;
        };
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let dataset_name = serde_json::from_str::<serde_json::Value>(&content)
            .ok()
            .and_then(|v| {
                v.get("dataset_name")
                    .and_then(|d| d.as_str())
                    .map(String::from)
            })
            .unwrap_or_default();
        let created_at = Utc
            .timestamp_opt(
                modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or_default(),
                0,
            )
            .single()
            .unwrap_or_else(Utc::now)
            .to_rfc3339();
        reports.push(StoredReport {
            id,
            dataset_name,
            created_at,
            size_bytes: content.len() as u64,
        });
    }

    Ok(ReportListing { reports, total })
}

#[derive(Debug, Deserialize)]
pub struct ListReportsQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ListReportsResponse {
    /// 这次读的目录。读的人要能看出「空」说的是哪个目录空。
    pub dir: String,
    pub reports: Vec<StoredReport>,
    /// 应用 `limit` **之前**目录里有几份。等于 `reports.len()` 时这次列全了。
    pub total: usize,
}

/// 列举评测报告。目录与总数都回报，读者不必猜自己看到的是不是全部。
pub async fn list_eval_reports_handler(
    State(state): State<Arc<GatewayState>>,
    Query(params): Query<ListReportsQuery>,
) -> axum::response::Response {
    let dir = reports_dir(&state.data_dir);
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, DEFAULT_LIST_LIMIT);

    match list_reports(&dir, limit) {
        Ok(listing) => (
            StatusCode::OK,
            Json(ListReportsResponse {
                dir: dir.display().to_string(),
                reports: listing.reports,
                total: listing.total,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "list_failed", "message": e.to_string()})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cog-eval-reports-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // 数据集名会拼进路径。一个带分隔符的名字不许把报告写到目录外面去，也不许把
    // id 变成一个找不到的路径。
    #[test]
    fn an_id_never_carries_a_path_separator() {
        let at = Utc.with_ymd_and_hms(2026, 10, 25, 4, 5, 6).unwrap();
        let id = new_report_id("suite/one:nightly run", at);
        assert!(!id.contains('/'), "id 里还有分隔符：{id}");
        assert!(!id.contains(':'), "id 里还有冒号：{id}");
        assert!(!id.contains(' '), "id 里还有空格：{id}");
        assert!(
            id.starts_with("suite_one_nightly_run_"),
            "id 丢了可读的来路：{id}"
        );
        assert!(id.ends_with("20261025_040506"), "id 里没有时刻：{id}");
    }

    // 同一秒内连跑两次，后一次不许盖掉前一次：两份都要在盘上，id 不同。
    #[test]
    fn a_second_run_in_the_same_second_does_not_overwrite_the_first() {
        let dir = scratch("no-overwrite");
        let at = Utc.with_ymd_and_hms(2026, 10, 25, 4, 5, 6).unwrap();
        let id = new_report_id("probe", at);
        let (first_id, first_path) =
            store_report(&dir, &id, r#"{"dataset_name":"probe","n":1}"#).unwrap();
        let (second_id, second_path) =
            store_report(&dir, &id, r#"{"dataset_name":"probe","n":2}"#).unwrap();

        assert_eq!(first_id, id);
        assert_ne!(first_id, second_id);
        assert!(first_path.exists() && second_path.exists());
        assert_eq!(
            std::fs::read_to_string(&first_path).unwrap(),
            r#"{"dataset_name":"probe","n":1}"#
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // 列举要新的在前，并在截断时报出截断前的总数——否则一页列表与一份完整的列表
    // 在读者眼里长得一样。
    #[test]
    fn a_listing_is_newest_first_and_reports_the_total_before_the_cut() {
        let dir = scratch("listing");
        for n in 0..3 {
            let name = format!("data-{n}.json");
            std::fs::write(dir.join(&name), format!(r#"{{"dataset_name":"set-{n}"}}"#)).unwrap();
            // 修改时间按秒粒度，测试里显式拉开，免得同秒内排序不稳。
            let stamp = std::time::SystemTime::now() + std::time::Duration::from_secs(n + 1);
            set_mtime(&dir.join(&name), stamp);
        }

        let listing = list_reports(&dir, 2).unwrap();
        assert_eq!(listing.total, 3, "总数不许被 limit 截掉");
        assert_eq!(listing.reports.len(), 2);
        assert_eq!(listing.reports[0].dataset_name, "set-2", "新的要在前");
        assert_eq!(listing.reports[1].dataset_name, "set-1");
        std::fs::remove_dir_all(&dir).ok();
    }

    // 目录不存在（还没跑过评测）与读失败是两件事：前者是一份空列举。
    #[test]
    fn a_directory_that_was_never_written_is_an_empty_listing_not_an_error() {
        let dir = std::env::temp_dir().join(format!("cog-eval-never-{}", uuid::Uuid::new_v4()));
        let listing = list_reports(&dir, 10).unwrap();
        assert_eq!(listing.total, 0);
        assert!(listing.reports.is_empty());
    }

    fn set_mtime(path: &Path, stamp: std::time::SystemTime) {
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_modified(stamp).unwrap();
    }
}
