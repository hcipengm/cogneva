//! 晋级判定跨进程替换的交接。
//!
//! 晋级判定跑在「变更已在沙盒里部署并 soak 过」之后。而 `self_exec` 切换模式下
//! **那次部署就是本进程的 `execve`**：切换成功即不返回，挂在它之后的任何回调
//! （`tokio::spawn` 也一样，任务随进程映像一起消失）永远不会执行。
//! 所以判定不能长在部署进程的栈上——它在切换**之前**落成数据目录里的一条记录，
//! 由 soak 期满后**当时活着的那个进程**取走（通常就是本进程换上新二进制后的样子）。
//!
//! 一个 change id 一个文件：同一条变更重复交接是重写，不是第二条。

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use cog_core::{SFError, SFResult};

use crate::types::EvolutionResult;
use crate::PromotionSource;

/// 一条已交出的待晋级判定。
pub struct HandedOff {
    /// 承载这条记录的文件；判定做完后由调用方撤回。
    pub path: PathBuf,
    pub change: EvolutionResult,
    pub source: PromotionSource,
    pub staged_at: DateTime<Utc>,
}

/// 交接记录的落盘结构。`PromotionSource` 的 `repo` 是 `PathBuf`，落成字符串
/// 是为了让这份文件在换机/换挂载点后仍然可读。
#[derive(serde::Serialize, serde::Deserialize)]
struct Record {
    change: EvolutionResult,
    repo: String,
    rev: String,
    staged_at: DateTime<Utc>,
}

/// `COGNEVA_DATA_DIR` 是进程级环境变量：凡是把它指到临时目录的测试都要拿
/// 这把锁串行，否则两条测试会把对方的目录换掉，失败还是随机的。用 tokio 的锁
/// 是因为持锁期间要 `.await` 落盘。
#[cfg(test)]
pub(crate) static DATA_DIR_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 交接目录。与其它运行时数据同卷：切换会换掉进程映像，换不掉挂载。
pub fn dir() -> PathBuf {
    let root = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
    PathBuf::from(root).join("pending-promotions")
}

/// 文件系统安全的 change id。
fn slug(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn path_for(change_id: &str) -> PathBuf {
    dir().join(format!("{}.json", slug(change_id)))
}

/// 判定是否已过 soak。纯函数：soak 的起点是**交出这条记录的时刻**，不是
/// 取走它的时刻——否则每重启一次 soak 就重新开始，等于没有 soak。
fn is_due(staged_at: DateTime<Utc>, soak_secs: u64, now: DateTime<Utc>) -> bool {
    let elapsed = now.signed_duration_since(staged_at).num_seconds();
    elapsed >= 0 && (elapsed as u64) >= soak_secs
}

/// 在切换之前把判定交出去（同一 change 重复交是重写）。
pub async fn hand_off(change: &EvolutionResult, source: &PromotionSource) -> SFResult<PathBuf> {
    let dir = dir();
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| SFError::IO(format!("create pending-promotions dir: {e}")))?;
    let rec = Record {
        change: change.clone(),
        repo: source.repo.to_string_lossy().into_owned(),
        rev: source.rev.clone(),
        staged_at: Utc::now(),
    };
    let path = path_for(&change.artifact_id);
    let json = serde_json::to_string_pretty(&rec)
        .map_err(|e| SFError::Internal(format!("serialize handed-off promotion: {e}")))?;
    tokio::fs::write(&path, json)
        .await
        .map_err(|e| SFError::IO(format!("write handed-off promotion: {e}")))?;
    Ok(path)
}

/// 撤回一条交接（切换没成功，就没有可晋级的沙盒部署）。失败只记日志：
/// 撤回不了最多多判一次，判定自己是幂等的（台账已有记录就跳过）。
pub async fn withdraw(path: &Path) {
    if let Err(e) = tokio::fs::remove_file(path).await {
        tracing::warn!(path = %path.display(), error = %e, "cannot withdraw a handed-off promotion");
    }
}

/// soak 期满、可以判定的那些交接。读不动的条目跳过并留在盘上（下一轮再来），
/// 一条坏文件不阻塞其余。
pub async fn load_due(soak_secs: u64, now: DateTime<Utc>) -> Vec<HandedOff> {
    let mut out = Vec::new();
    let Ok(mut it) = tokio::fs::read_dir(dir()).await else {
        return out;
    };
    while let Ok(Some(entry)) = it.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(text) = tokio::fs::read_to_string(&path).await else {
            continue;
        };
        let Ok(rec) = serde_json::from_str::<Record>(&text) else {
            continue;
        };
        if !is_due(rec.staged_at, soak_secs, now) {
            continue;
        }
        out.push(HandedOff {
            path,
            change: rec.change,
            source: PromotionSource {
                repo: PathBuf::from(rec.repo),
                rev: rec.rev,
            },
            staged_at: rec.staged_at,
        });
    }
    out.sort_by_key(|h| h.staged_at);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria_face::{Tier, TierReason, Tiering};

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn the_soak_runs_from_the_hand_off_not_from_the_pickup() {
        let staged = at("2026-10-04T00:00:00Z");
        assert!(!is_due(staged, 600, at("2026-10-04T00:09:59Z")));
        assert!(is_due(staged, 600, at("2026-10-04T00:10:00Z")));
        // 取走它的是重启后的进程，进程多新都不影响已流逝的 soak。
        assert!(is_due(staged, 600, at("2026-10-05T00:00:00Z")));
    }

    #[test]
    fn a_record_from_the_future_is_not_due() {
        // 时钟回拨或时间戳写错时不能立刻放行：判定要等它自己的起点。
        assert!(!is_due(
            at("2026-10-04T01:00:00Z"),
            0,
            at("2026-10-04T00:00:00Z")
        ));
    }

    #[test]
    fn zero_soak_is_immediately_due() {
        let staged = at("2026-10-04T00:00:00Z");
        assert!(is_due(staged, 0, staged));
    }

    fn handed_off_change(id: &str) -> EvolutionResult {
        EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: id.into(),
            description: "hand-off round trip".into(),
            content: "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -1 +1,2 @@\n x\n+y\n".into(),
            status: crate::types::EvolutionStatus::Active,
            created_at: Utc::now(),
            eval_summary: Some(cog_core::EvalReport {
                verdict: cog_core::EvalVerdict::Adopt,
                summary: "Adopt z=2.31 uplift +18%".into(),
            }),
            tiering: Some(Tiering {
                tier: Tier::RealGate,
                reasons: vec![TierReason::CriteriaCarrier],
                touches_criteria_code: false,
            }),
        }
    }

    #[tokio::test]
    async fn a_hand_off_survives_the_trip_to_disk_and_back() {
        let _guard = DATA_DIR_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        let change = handed_off_change("chg-round-trip");
        assert!(change.tiering.is_some());
        let source = PromotionSource {
            repo: PathBuf::from("/host-git"),
            rev: "abc123".into(),
        };
        let path = hand_off(&change, &source).await.unwrap();
        assert!(
            path.exists(),
            "the hand-off must land on disk, not in memory"
        );

        // soak 没满不取：等于判定又跑在部署进程自己身上了。
        assert!(load_due(600, Utc::now()).await.is_empty());

        let due = load_due(0, Utc::now()).await;
        assert_eq!(due.len(), 1, "soak 期满后应当正好取走这一条");
        assert_eq!(due[0].change.artifact_id, "chg-round-trip");
        assert_eq!(due[0].change.content, change.content);
        assert_eq!(due[0].change.eval_summary, change.eval_summary);
        assert_eq!(
            due[0].change.tiering, change.tiering,
            "档位要随变更记录过盘：判定在 soak 之后、常常在另一个进程里做，\
             那时工作树已经移动，重算就是第二次读数"
        );
        assert_eq!(due[0].source.repo, PathBuf::from("/host-git"));
        assert_eq!(due[0].source.rev, "abc123");

        withdraw(&due[0].path).await;
        assert!(
            load_due(0, Utc::now()).await.is_empty(),
            "撤回后不该再被取走一次"
        );

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[tokio::test]
    async fn a_corrupt_hand_off_does_not_block_the_readable_ones() {
        let _guard = DATA_DIR_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", tmp.path());

        let source = PromotionSource {
            repo: PathBuf::from("/host-git"),
            rev: "abc123".into(),
        };
        hand_off(&handed_off_change("chg-good"), &source)
            .await
            .unwrap();
        tokio::fs::write(dir().join("chg-bad.json"), b"{ not json")
            .await
            .unwrap();

        let due = load_due(0, Utc::now()).await;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].change.artifact_id, "chg-good");

        std::env::remove_var("COGNEVA_DATA_DIR");
    }
}
