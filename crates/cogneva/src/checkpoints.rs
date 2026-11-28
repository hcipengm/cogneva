//! 对话里的检查点：每一轮跑完之后把那棵工作树记一份，之后能显式退回去。
//!
//! 记在**跑完**之后而不是开跑之前，是为了不挡第一句话：拍快照要走一趟执行器、
//! 读一遍树，放在开跑之前，人感到的是「说完话以后它先愣一下」。记在跑完之后，
//! 这一次快照就是「这一轮结束时的树」，而「退回到第 N 轮之前」要的是第 N−1 轮
//! 结束时的样子——映射差一轮，规则只有这一条，写在 [`rewind_target`] 里。
//!
//! 每段会话的第一轮额外记一份 `turn 0`：会话开头的那棵树不在任何一轮结束时，
//! 没有它，「退回到第一轮之前」就没有落脚的地方。
//!
//! 记的是**整棵树**而不是工具写过的那几个文件：一轮里改文件的不只有文件工具，
//! 一条 `sed -i` 也改。只记工具碰过的路径，那种轮次会退不干净，而读数上看不出
//! 来。快照本身由执行器那侧落到 git 上（见 `cog_extension::checkpoints`）。

use std::sync::Arc;

use cog_core::{ResourceLimits, SandboxBackend, SandboxPayload, SandboxRequest};

/// 一段会话最多在界面上列多少个检查点。
///
/// 这是钉住的值（`cap: bound`）：列出来的是给人挑的入口，不是存储的上限；更早的
/// 那些仍然在树上（对象与别的快照共用），只是不再逐个列出，列表尾上说明「更早的
/// 没列」。
// cap: fixed — 给人挑的入口个数：这是列表的选择约定，不据此删任何快照，也不是存储的上限
// keep: 删掉后一次列出全部快照，人挑不完；由「给人挑的入口个数」兜住
pub const MAX_LISTED_CHECKPOINTS: usize = 20;

/// 检查点在存储里的那一行。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Row {
    /// 名字。同一个会话里唯一，跨会话不保证。
    pub key: String,
    /// 这份快照是哪一轮**结束时**的样子。`0` 是会话开头。
    pub turn: usize,
    /// 什么时候记下的，给界面排序与显示。
    pub at: String,
}

/// 一段会话记下来的东西：列得出来的行，以及「为什么记不了」。
///
/// 两格分开是有意的：一个部署没接沙箱时 `rows` 是空的、`unavailable` 有话说；
/// 记下了但被上限截掉时 `rows` 非空、`unavailable` 仍是空的。把「做不了」与
/// 「还没做过」画成同一句空列表，人会以为退不回去是因为自己没跑过。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Rows {
    pub rows: Vec<Row>,
    #[serde(default)]
    pub unavailable: Option<String>,
}

/// 一段会话、第 `turn` 轮的检查点名字。
///
/// 会话号直接拼进来：它是这段对话自己的标识，名字要在执行器那侧也能独立认出
/// 一段会话，哈希过的名字在日志与 git 引用里就只剩一串十六进制了。
pub fn key(session_id: &str, turn: usize) -> String {
    format!("{session_id}:{turn}")
}

/// 「退回到第 `turn` 轮之前」该退到哪一份快照。
///
/// 第 1 轮之前是会话开头（`0`）；往后每一轮之前是上一轮结束时的样子。
pub fn rewind_target(turn: usize) -> Option<usize> {
    turn.checked_sub(1)
}

/// 这一段对话的检查点记录落在存储里的键。没有登录身份时没有「谁的这一段」，
/// 也就没有键——那时的行存不下来，而这一点要能读出来，不能静默当成空。
fn storage_key(owner: &str, session_id: &str) -> Option<String> {
    let owner = owner.trim();
    if owner.is_empty() {
        return None;
    }
    Some(format!("checkpoints:{owner}:{session_id}"))
}

/// 这一段会话已经记下来的那些行。
pub async fn load(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    owner: &str,
    session_id: &str,
) -> Result<Rows, String> {
    let Some(key) = storage_key(owner, session_id) else {
        return Err("这段对话没有登录身份，检查点记不到任何人的名下。".into());
    };
    let raw = kv
        .get(&key)
        .await
        .map_err(|e| format!("读不到这一段会话的检查点：{e}"))?;
    let Some(raw) = raw else {
        return Ok(Rows::default());
    };
    serde_json::from_str(&raw).map_err(|e| format!("这一段会话的检查点记录读不懂：{e}"))
}

/// 记下一行。返回记完之后的样子。
pub async fn remember(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    owner: &str,
    session_id: &str,
    row: Row,
) -> Result<Rows, String> {
    let mut current = load(kv, owner, session_id).await?;
    if let Some(existing) = current.rows.iter_mut().find(|r| r.turn == row.turn) {
        // 同一轮跑了两遍（重试、重发）：以最后那份为准，不叠两行。
        *existing = row;
    } else {
        current.rows.push(row);
        current.rows.sort_by_key(|r| r.turn);
    }
    save(kv, owner, session_id, &current).await?;
    Ok(current)
}

/// 记下「这一段会话做不了检查点，原因是这个」。
pub async fn mark_unavailable(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    owner: &str,
    session_id: &str,
    reason: String,
) -> Result<(), String> {
    let mut current = load(kv, owner, session_id).await?;
    current.unavailable = Some(reason);
    save(kv, owner, session_id, &current).await
}

/// 退回到某一轮之后，把后面那几轮从记录里去掉。
///
/// 那些轮次的改动已经被丢掉了：它们的快照指向一段不再存在的历史，留着只会在
/// 下一次退回时让人挑到一份「退到这里也一样」。留下的是「退到的那一轮及之前」。
pub async fn forget_after(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    owner: &str,
    session_id: &str,
    turn: usize,
) -> Result<Rows, String> {
    let mut current = load(kv, owner, session_id).await?;
    current.rows.retain(|row| row.turn <= turn);
    save(kv, owner, session_id, &current).await?;
    Ok(current)
}

async fn save(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    owner: &str,
    session_id: &str,
    rows: &Rows,
) -> Result<(), String> {
    let Some(key) = storage_key(owner, session_id) else {
        return Err("这段对话没有登录身份，检查点记不到任何人的名下。".into());
    };
    let body = serde_json::to_string(rows).map_err(|e| format!("检查点记录写不出来：{e}"))?;
    kv.set(&key, &body)
        .await
        .map_err(|e| format!("检查点记录存不下去：{e}"))
}

/// 一次对执行器的检查点请求。
async fn ask(
    sandbox: &Arc<dyn SandboxBackend>,
    agent_id: &str,
    task_id: &str,
    payload: SandboxPayload,
) -> Result<serde_json::Value, String> {
    let request = SandboxRequest {
        task_id: task_id.to_string(),
        agent_id: agent_id.to_string(),
        payload,
        input: serde_json::Value::Null,
        limits: ResourceLimits::default(),
    };
    let result = sandbox
        .execute(&request)
        .await
        .map_err(|e| format!("检查点这一步没跑成：{e}"))?;
    if result.exit_code != 0 {
        let why = if result.stderr.trim().is_empty() {
            format!("退出码 {}", result.exit_code)
        } else {
            result.stderr.trim().to_string()
        };
        return Err(why);
    }
    serde_json::from_str(&result.stdout).map_err(|e| {
        format!(
            "执行器答回来的检查点读不懂（{e}）：{}",
            result.stdout.trim()
        )
    })
}

/// 把这棵树现在这个样子记下来。
pub async fn capture(
    sandbox: &Arc<dyn SandboxBackend>,
    agent_id: &str,
    task_id: &str,
    key: &str,
) -> Result<serde_json::Value, String> {
    ask(
        sandbox,
        agent_id,
        task_id,
        SandboxPayload::CaptureCheckpoint {
            key: key.to_string(),
        },
    )
    .await
}

/// 退回这一份快照会改动什么。
pub async fn plan(
    sandbox: &Arc<dyn SandboxBackend>,
    agent_id: &str,
    task_id: &str,
    key: &str,
) -> Result<serde_json::Value, String> {
    ask(
        sandbox,
        agent_id,
        task_id,
        SandboxPayload::PlanCheckpoint {
            key: key.to_string(),
        },
    )
    .await
}

/// 把树退回去。
pub async fn restore(
    sandbox: &Arc<dyn SandboxBackend>,
    agent_id: &str,
    task_id: &str,
    key: &str,
) -> Result<serde_json::Value, String> {
    ask(
        sandbox,
        agent_id,
        task_id,
        SandboxPayload::RestoreCheckpoint {
            key: key.to_string(),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_snapshot_before_a_turn_is_the_one_it_ended_on() {
        assert_eq!(rewind_target(1), Some(0), "第一轮之前是会话开头");
        assert_eq!(rewind_target(3), Some(2), "第三轮之前是第二轮结束时");
        assert_eq!(rewind_target(0), None, "会话开头之前没有可退的");
    }

    #[test]
    fn a_key_names_the_turn_it_was_taken_after() {
        assert_eq!(key("s-1", 2), "s-1:2");
        assert_ne!(key("s-1", 2), key("s-1", 3));
        assert_ne!(key("s-1", 2), key("s-2", 2));
    }
}
