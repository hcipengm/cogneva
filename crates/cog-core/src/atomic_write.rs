//! 一份文件的整份替换：先写同目录下的临时文件，再 rename 过去。
//!
//! 同一条规则在树里写过五遍——主线的状态、元学习的快照、策略库的版本、变更执行
//! 的记录、评测报告——每一遍都自己拼临时文件名（`json.tmp`、`tmp-<pid>`、
//! `.<name>.json.tmp`、`.<name>.<pid>.<seq>.tmp`），也各自决定 fsync 与否。
//! 五份里任意一份改错（临时文件放到别的目录、先删后写），另外四份不会红：read 端
//! 读到半份 JSON 时，报的是「解析失败」还是「这份读数不存在」取决于它自己那一支，
//! 而两者在运维面上都读作「这里没有东西」。
//!
//! 五份分属两个 crate，而这两个 crate 互不依赖（`cog-gateway` 只依赖 core，
//! `cog-reflection` 的反思引擎不该被网关拉进来），所以规则落在 core：这是它唯一的
//! 位置，不是又一次往 core 堆实现。
//!
//! 于是规则只写一处，差异只剩一个显式的选择：[`Durability`]。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use crate::{SFError, SFResult};

/// 这次写要在什么级别的故障下还成立。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Durability {
    /// 同一个内核里的另一个读者、下一次打开，都能看到整份新的。
    ///
    /// 不 fsync：写端与读端在同一个内核上（共享卷是同节点的一个目录），rename
    /// 当场对另一个进程可见，耐用性边界不是断电。**能重算的东西**用这一档。
    SameKernel,
    /// 断电之后重启，盘上要么是整份旧的、要么是整份新的。
    ///
    /// 写临时文件之后 fsync 它，rename 之后再 fsync 目录：少了目录那一次，
    /// rename 本身可能还留在页缓存里。**不能重算的东西**（策略、账）用这一档。
    SurvivesPowerLoss,
}

/// 临时文件名里区分写者的那一段。
///
/// 一个进程内的序号不够：两个进程写同一个目标时，固定名会让它们互相覆盖一半的
/// 字节，而两次 rename 都成功——后写的那份悄悄赢了，谁也没报错。
static SEQ: AtomicU64 = AtomicU64::new(0);

/// 目标旁边的临时文件名：隐藏，带 pid 与序号，`.tmp` 结尾。
fn sibling_tmp(path: &Path) -> SFResult<PathBuf> {
    let dir = path
        .parent()
        .ok_or_else(|| SFError::IO(format!("no parent directory for {}", path.display())))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unnamed");
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    Ok(dir.join(format!(".{name}.{}.{seq}.tmp", std::process::id())))
}

/// 把 `bytes` 写成 `path` 的完整内容。
pub fn write_bytes(path: &Path, bytes: &[u8], durability: Durability) -> SFResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| SFError::IO(format!("create {}: {e}", parent.display())))?;
    }
    let tmp = sibling_tmp(path)?;
    let written = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        if durability == Durability::SurvivesPowerLoss {
            file.sync_all()?;
        }
        drop(file);
        std::fs::rename(&tmp, path)?;
        if durability == Durability::SurvivesPowerLoss {
            if let Some(dir) = path.parent() {
                std::fs::File::open(dir)?.sync_all()?;
            }
        }
        Ok(())
    })();
    match written {
        Ok(()) => Ok(()),
        Err(e) => {
            // 失败的那一份留在原地只会在下一次写时被另一个序号绕过，成为一块
            // 永远不会有人读的字节。删失败不算错——它已经不影响正确性。
            let _ = std::fs::remove_file(&tmp);
            Err(SFError::IO(format!("write {}: {e}", path.display())))
        }
    }
}

/// 同 [`write_bytes`]，给异步调用点。
pub async fn write_bytes_async(path: &Path, bytes: &[u8], durability: Durability) -> SFResult<()> {
    use tokio::io::AsyncWriteExt;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| SFError::IO(format!("create {}: {e}", parent.display())))?;
    }
    let tmp = sibling_tmp(path)?;
    let written = async {
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(bytes).await?;
        if durability == Durability::SurvivesPowerLoss {
            file.sync_all().await?;
        }
        drop(file);
        tokio::fs::rename(&tmp, path).await?;
        if durability == Durability::SurvivesPowerLoss {
            if let Some(dir) = path.parent() {
                tokio::fs::File::open(dir).await?.sync_all().await?;
            }
        }
        Ok::<(), std::io::Error>(())
    }
    .await;
    match written {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            Err(SFError::IO(format!("write {}: {e}", path.display())))
        }
    }
}

/// 把 `value` 的 JSON 写成 `path` 的完整内容。
pub fn write_json<T: Serialize>(path: &Path, value: &T, durability: Durability) -> SFResult<()> {
    let body = serde_json::to_vec_pretty(value)
        .map_err(|e| SFError::IO(format!("serialize {}: {e}", path.display())))?;
    write_bytes(path, &body, durability)
}

/// 同 [`write_json`]，给异步调用点。
pub async fn write_json_async<T: Serialize>(
    path: &Path,
    value: &T,
    durability: Durability,
) -> SFResult<()> {
    let body = serde_json::to_vec_pretty(value)
        .map_err(|e| SFError::IO(format!("serialize {}: {e}", path.display())))?;
    write_bytes_async(path, &body, durability).await
}
