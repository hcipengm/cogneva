//! 「负责人」这一列填着名字，不等于那个人还在。
//!
//! 现场：任务图里 2413 行未结，其中 3448 行连 `owner_unit` 都没有，而剩下的那些
//! 名字里有的早已退场。按状态数行数的读面把这两件事都读成一片安静——那一列填着
//! 名字，看起来有人。进度读数存在的理由就是把这一列拆成两个问题：谁名下有什么，
//! 以及那个人还答不答话。
//!
//! 判据（每一格都是「没有这条读数就会红」）：
//!   1. 有未结活、在岗账里查无此人 → `absent`，不是一个空名字；
//!   2. 有未结活、这一拍根本没问过在岗 → `unreadable`。它与 `absent` 指往相反的
//!      处置（前者去查在岗账，后者派人接手），压成一档会把一次读不动的在岗账画成
//!      「所有人都走了」；
//!   3. 没有 `owner_unit` 的未结活单独计数，不摊进任何一个 unit，也不进「其他」；
//!   4. 已经结束的无主行不算欠账；
//!   5. `returned_to_line` 只数「还是 pending、行上却带着错」的那一批：它在状态上
//!      与排得好好的活逐字同形，而这个数说的正是「被放回队里、从未开工」；
//!   6. 各状态之和等于总行数——摊开之后不许漏行，也不许一行被数两遍；
//!   7. 活已经交完的 unit 不列进责任人表：名单是「谁该动手」，不是一张历史。

use std::collections::BTreeMap;

use cog_core::{Task, TaskStatus, TaskType};
use cog_gateway::progress::{task_progress, Presence};

fn task(id: &str, status: TaskStatus, owner: Option<&str>) -> Task {
    let mut task = Task::new(id, TaskType::Custom("work".into()), serde_json::json!({}));
    task.status = status;
    task.owner_unit = owner.map(str::to_string);
    task
}

fn owner<'a>(
    progress: &'a cog_gateway::progress::TaskProgress,
    unit: &str,
) -> &'a cog_gateway::progress::OwnerProgress {
    progress
        .owners
        .iter()
        .find(|row| row.unit == unit)
        .unwrap_or_else(|| panic!("{unit} is missing from the owner table"))
}

fn rows(progress: &cog_gateway::progress::TaskProgress) -> u64 {
    progress.by_state.iter().map(|cell| cell.rows).sum()
}

#[test]
fn a_named_owner_that_no_longer_answers_is_reported_absent() {
    let tasks = vec![
        task("a", TaskStatus::Pending, Some("f5")),
        task("b", TaskStatus::Completed, Some("f5")),
    ];
    let mut presence = BTreeMap::new();
    presence.insert("f5".to_string(), Presence::Absent);

    let progress = task_progress(&tasks, &presence);
    let f5 = owner(&progress, "f5");
    assert_eq!(f5.presence, Presence::Absent);
    assert_eq!(f5.open, 1);
    assert_eq!(f5.completed, 1);
}

#[test]
fn an_unasked_owner_is_unreadable_rather_than_absent() {
    let tasks = vec![task("a", TaskStatus::Running, Some("orphan"))];
    let progress = task_progress(&tasks, &BTreeMap::new());
    assert_eq!(owner(&progress, "orphan").presence, Presence::Unreadable);
}

#[test]
fn work_nobody_owns_is_counted_on_its_own() {
    let tasks = vec![
        task("a", TaskStatus::Pending, None),
        task("b", TaskStatus::Scheduled, None),
        task("c", TaskStatus::Completed, None),
        task("d", TaskStatus::Pending, Some("f5")),
    ];
    let progress = task_progress(&tasks, &BTreeMap::new());

    assert_eq!(progress.unowned_open, 2, "只数还开着的那两行");
    assert_eq!(progress.open, 3);
    // 无主的行不进任何一条 owners，所以责任人名下的未结合计加上无主的那两批，
    // 要正好等于开着的活——这条等式是「没有活被藏起来」唯一读得出来的形状。
    let owned_open: u64 = progress.owners.iter().map(|row| row.open).sum();
    assert_eq!(
        owned_open + progress.unowned_open + progress.unowned_goals,
        progress.open
    );
}

#[test]
fn a_goal_row_nobody_owns_is_a_goal_not_unclaimed_work() {
    let mut goal = task("g", TaskStatus::Pending, None);
    goal.is_executable = false;
    let tasks = vec![goal, task("w", TaskStatus::Pending, None)];

    let progress = task_progress(&tasks, &BTreeMap::new());
    assert_eq!(
        progress.unowned_goals, 1,
        "占位行是没闭的目标，不是没人领的活"
    );
    assert_eq!(progress.unowned_open, 1);
}

#[test]
fn returned_to_line_counts_rows_that_are_pending_and_still_name_an_error() {
    let mut put_back = task("a", TaskStatus::Pending, Some("f5"));
    put_back.error = Some("never started: sat in Scheduled".into());
    let mut failed = task("b", TaskStatus::Failed, Some("f5"));
    failed.error = Some("boom".into());
    let clean = task("c", TaskStatus::Pending, Some("f5"));

    let tasks = vec![put_back, failed, clean];
    let progress = task_progress(&tasks, &BTreeMap::new());
    assert_eq!(
        progress.returned_to_line, 1,
        "失败的行已经结束，它不是「被放回队里」"
    );
}

#[test]
fn every_row_lands_in_exactly_one_state_cell() {
    let tasks = vec![
        task("a", TaskStatus::Pending, None),
        task("b", TaskStatus::Running, Some("f5")),
        task("c", TaskStatus::Cancelled, Some("f5")),
        task("d", TaskStatus::Failed, None),
    ];
    let progress = task_progress(&tasks, &BTreeMap::new());
    assert_eq!(rows(&progress), progress.rows);
    assert_eq!(progress.rows, 4);
    assert_eq!(progress.open, 2);
}

#[test]
fn a_unit_that_finished_its_work_leaves_the_owner_table() {
    let tasks = vec![
        task("done", TaskStatus::Completed, Some("retired-unit")),
        task("open", TaskStatus::Pending, Some("f5")),
    ];
    let progress = task_progress(&tasks, &BTreeMap::new());
    assert!(
        !progress.owners.iter().any(|row| row.unit == "retired-unit"),
        "交完活的 unit 不该继续占着责任人表"
    );
    assert_eq!(progress.owners.len(), 1);
}
