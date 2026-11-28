//! 待办的五格住在任务定义里：`task_defined` 的载荷带整条 `Task`，三格是它的
//! 一等字段（`terminal`／`now`／`gap`）；旧行回退到 `input.ledger_item`。
//!
//! 这里没有第二条 `item_defined`：带内只有任务这一族正本，同一件事记两处，
//! 两处一漂开读的人就分不清哪一份是正本。定义写下一版，五格同时有了落点；
//! 判完/读树的人更新它时写下一版定义（rev+1），最后一条折出现在与差距。
//! 状态不看定义那一刻的快照：取最后一条 `task_transition` 的 `to` 与差距现算。

use sqlx::PgPool;

use cog_core::contract::ledger_item::{LedgerItem, LedgerState, ITEM_INPUT_KEY};
use cog_core::SFError;

/// 一条待办最近一次被定义的样子；`None` 是流上没有它，或那条任务不带待办。
///
/// 终态那一格叠上最后一条 `terminal_revised`：改终态是另一条事实（带理由），
/// 只读定义会把修订前的判据答给读者——而 A5 要的正是「换过没有、为什么换」看得见。
pub async fn item_definition(pool: &PgPool, item_id: &str) -> Result<Option<LedgerItem>, SFError> {
    // 按**族**读，不按细词：旧行写 `task_defined`，聚合词写 `task`＋`step`；
    // 只认一个拼写的读者会在另一套写法落下的那一刻读成「没有发生过」。
    let task_kinds: Vec<String> = cog_core::kinds_of("task")
        .unwrap_or(&["task"])
        .iter()
        .map(|kind| (*kind).to_string())
        .collect();
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT attributes FROM cogneva_event \
          WHERE kind = ANY($1) AND step = 'defined' AND subject_id = $2 \
          ORDER BY seq DESC LIMIT 1",
    )
    .bind(&task_kinds)
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| SFError::Database(format!("read definition of {item_id}: {e}")))?;
    let Some((attributes,)) = row else {
        return Ok(None);
    };
    let Some(mut item) = ledger_item_of_task_attributes(&attributes)? else {
        return Ok(None);
    };
    let revisions =
        crate::postgres::terminal_revisions::terminal_revisions_for(pool, &[item_id.to_string()])
            .await?;
    if let Some(revision) = revisions.into_iter().next() {
        item.terminal = revision.terminal;
    }
    // 状态取最后一条转移的目标状态，不取定义那一刻的快照：定义写下去时多半还是
    // pending，完成发生在后面的 `task_transition` 里。
    let transition_states: Vec<String> = cog_core::TaskStatus::ALL
        .iter()
        .map(|state| state.as_str().to_string())
        .collect();
    let transition: Option<(String,)> = sqlx::query_as(
        "SELECT attributes FROM cogneva_event \
          WHERE kind = ANY($1) AND step = ANY($2) AND subject_id = $3 \
          ORDER BY seq DESC LIMIT 1",
    )
    .bind(&task_kinds)
    .bind(&transition_states)
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| SFError::Database(format!("read the last transition of {item_id}: {e}")))?;
    if let Some((attributes,)) = transition {
        let value: serde_json::Value = cog_core::logged_default!(serde_json::from_str(&attributes));
        item.state = state_from_transition(value.get("to").and_then(|to| to.as_str()), &item.gap);
    }
    Ok(Some(item))
}

/// 第五格：最后一条转移的目标状态与差距现算，只有「完成且差距为空」才是 `已落`。
pub fn state_from_transition(to: Option<&str>, gap: &str) -> LedgerState {
    LedgerState::from_outcome(to == Some("completed"), gap)
}

/// 从一条任务定义事件的载荷里读五格。载荷形状与
/// `EventPayload::TaskDefined` 的信封一致：`{rev, workspace_id, task: {...}}`。
///
/// 三格先读任务的一等字段；字段都空时退回 `input.ledger_item`（过渡期的旧入口
/// 与旧行）。状态不读存下来的那一格：由任务 `status + gap` 现算，只有「完成且
/// 差距为空」才是 `已落`。没有三格、也不是待办的任务答 `None`。
pub fn ledger_item_of_task_attributes(attributes: &str) -> Result<Option<LedgerItem>, SFError> {
    let value: serde_json::Value = serde_json::from_str(attributes)
        .map_err(|e| SFError::Database(format!("task_defined payload: {e}")))?;
    let Some(task) = value.get("task") else {
        return Ok(None);
    };
    let text = |key: &str| {
        task.get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    let terminal = text("terminal");
    let now = text("now");
    let gap = text("gap");
    let mut item = if terminal.is_empty() && now.is_empty() && gap.is_empty() {
        let Some(raw) = task
            .get("input")
            .and_then(|input| input.get(ITEM_INPUT_KEY))
            .cloned()
        else {
            return Ok(None);
        };
        serde_json::from_value::<LedgerItem>(raw).map_err(|e| {
            SFError::Database(format!(
                "the five cells on the task definition do not parse: {e}"
            ))
        })?
    } else {
        LedgerItem {
            id: text("id"),
            terminal,
            now,
            gap,
            state: LedgerState::Open,
        }
    };
    // 状态现算：完成且差距为空／破折号才是已落；规则只在 `LedgerState::from_outcome` 一处。
    item.state = LedgerState::from_outcome(text("status") == "completed", &item.gap);
    Ok(Some(item))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attributes_with(item: serde_json::Value) -> String {
        serde_json::json!({
            "rev": 1,
            "workspace_id": "ws-1",
            "task": { "id": "L1", "input": { ITEM_INPUT_KEY: item } },
        })
        .to_string()
    }

    /// 五格从任务定义事件里读得回来：没有第二条 `item_defined` 可读。
    #[test]
    fn the_five_cells_read_back_from_the_task_definition() {
        let attributes = attributes_with(serde_json::json!({
            "id": "L1",
            "terminal": "到终态",
            "now": "此刻",
            "gap": "缺的是：接线",
            "state": "未落",
        }));
        let item = ledger_item_of_task_attributes(&attributes)
            .expect("payload parses")
            .expect("the task carries the five cells");
        assert_eq!(item.id, "L1");
        assert_eq!(item.terminal, "到终态");
        assert_eq!(item.now, "此刻");
        assert_eq!(item.gap, "缺的是：接线");
        assert_eq!(item.state, LedgerState::Open);
    }

    /// 三格在任务的一等字段上时直接读它，不用再解析输入。
    #[test]
    fn the_five_cells_read_from_the_task_fields() {
        let attributes = serde_json::json!({
            "rev": 1,
            "workspace_id": "ws-1",
            "task": {
                "id": "L1",
                "terminal": "到终态",
                "now": "此刻",
                "gap": "",
                "status": "completed",
            },
        })
        .to_string();
        let item = ledger_item_of_task_attributes(&attributes)
            .expect("payload parses")
            .expect("the task carries the five cells");
        assert_eq!(item.id, "L1");
        assert_eq!(item.terminal, "到终态");
        assert!(item.gap_is_empty());
        assert_eq!(item.state, LedgerState::Reached);
    }

    /// 完成却还写着差距：仍读未落——一次运行报告成功不等于那件事在树上成立。
    #[test]
    fn a_completed_task_that_still_carries_a_gap_is_not_reached() {
        let attributes = serde_json::json!({
            "rev": 1,
            "workspace_id": "ws-1",
            "task": { "id": "L1", "terminal": "到终态", "now": "此刻",
                      "gap": "缺的是：接线", "status": "completed" },
        })
        .to_string();
        let item = ledger_item_of_task_attributes(&attributes)
            .expect("payload parses")
            .expect("the task carries the five cells");
        assert_eq!(item.state, LedgerState::Open);
    }

    /// 第五格看最后一条转移，不看定义那一刻的快照。
    #[test]
    fn the_state_follows_the_last_transition_and_the_gap() {
        assert_eq!(
            state_from_transition(Some("completed"), ""),
            LedgerState::Reached
        );
        assert_eq!(
            state_from_transition(Some("completed"), "缺的是：最后一处"),
            LedgerState::Open,
            "完成却还写着差距，不许冒充终态"
        );
        assert_eq!(
            state_from_transition(Some("running"), ""),
            LedgerState::Open
        );
        assert_eq!(state_from_transition(None, ""), LedgerState::Open);
    }

    /// 不带待办的任务定义（普通任务）读作「没有」，不是一条空白的待办。
    #[test]
    fn a_task_without_the_cells_reads_as_none() {
        let attributes = serde_json::json!({
            "rev": 1,
            "workspace_id": "ws-1",
            "task": { "id": "t-1", "input": {} },
        })
        .to_string();
        assert!(ledger_item_of_task_attributes(&attributes)
            .expect("payload parses")
            .is_none());
    }

    /// 闭集里的词还在，读法与写者不靠旧 kind 存在。
    #[test]
    fn the_task_definition_is_a_known_kind() {
        assert!(cog_core::is_known_kind("task_defined"));
        assert!(!cog_core::is_known_kind("item_defined"));
    }
}
