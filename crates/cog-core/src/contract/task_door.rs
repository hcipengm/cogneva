//! 交活的那个门：三个入口交出去的形状只在这里拼一次。
//!
//! `/api/v1/tasks` 是唯一收活的地方——对话侧、智能体侧的 `start_task`、A2A 都
//! 该打到它。形状若各拼一份，某一天有一边少一个键，门照样收下，读的人却按入口
//! 分出两套语义。这里只做拼装，不做投递。

/// 一行活：身份、种类、输入、声明的时长，以及「调用方已经把活想清楚了」那个标记。
///
/// `planner_verified` 为真时门不再为这一行花一次分解——调用方已经替它写了终态与
/// 验收。字段名取 planner 角色自己的名字（[`crate::roles::PLANNER`]），另一处拼写
/// 是一扇门从不看的字段，行会照旧被规划。
pub fn task_door_row(
    task_id: &str,
    task_type: &str,
    input: serde_json::Value,
    cells: Option<(&str, &str, &str)>,
    declared_duration_seconds: u64,
    planner_verified: bool,
) -> serde_json::Value {
    let mut row = serde_json::json!({
        "id": task_id,
        "task_type": task_type,
        "input": input,
        "declared_duration_seconds": declared_duration_seconds,
    });
    // 三格是任务的一等字段，随行一起交；不塞进 `input`（那是开集载荷）。
    if let Some((terminal, now, gap)) = cells {
        row["terminal"] = serde_json::json!(terminal);
        row["now"] = serde_json::json!(now);
        row["gap"] = serde_json::json!(gap);
    }
    if planner_verified {
        row[crate::roles::PLANNER] = serde_json::json!({
            "verified": true,
            "source": "user_provided",
        });
    }
    row
}

/// 一份交活的请求体：目标号、目标原文、工作区，与已经建好的那几行。
pub fn task_door_body(
    goal_id: &str,
    goal: &str,
    workspace_id: &str,
    tasks: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "goal_id": goal_id,
        "goal": goal,
        "workspace_id": workspace_id,
        "intent_kind": "task",
        "tasks": tasks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_verified_row_says_so_under_the_planners_own_name() {
        let row = task_door_row(
            "t-1",
            "platform_agent_task",
            serde_json::json!({"q": 1}),
            Some(("到终态", "此刻", "缺的是：接线")),
            60,
            true,
        );
        assert_eq!(row["id"], "t-1");
        assert_eq!(row["terminal"], "到终态");
        assert_eq!(row["gap"], "缺的是：接线");
        assert_eq!(row[crate::roles::PLANNER]["verified"], true);
        let plain = task_door_row(
            "t-2",
            "platform_agent_task",
            serde_json::json!({}),
            None,
            60,
            false,
        );
        assert!(plain.get(crate::roles::PLANNER).is_none());
        let body = task_door_body("g-1", "做那件事", "default", vec![row]);
        assert_eq!(body["goal_id"], "g-1");
        assert_eq!(body["intent_kind"], "task");
        assert_eq!(body["tasks"].as_array().unwrap().len(), 1);
    }
}
