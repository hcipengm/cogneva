//! A ready message that names an unfinished task must run that task, not be
//! discarded because the row has moved since the message was published.
//!
//! Publishing is the moment a ready message gets its permission to run, and the
//! consumer runs the task long after — at the queue depths this deployment
//! admits, minutes after. A row that has waited too long in `Scheduled` is
//! handed back to `Pending` by the stall sweep, so a message older than that
//! window arrives naming a `Pending` row. `start_task` only accepts
//! `Scheduled`, refuses it, and the reader reads the refusal as "this is a
//! stale copy" and acks the message away.
//!
//! Measured on 2026-11-06: of the 145 ready messages read in ten minutes, 144
//! were refused this way and dropped, while three claim slots sat idle and 291
//! ready tasks waited in that same stream to be re-queued and dropped in turn.
//! So admission is decided once more where it is used: a `Pending` row is
//! scheduled again and then started. Terminal rows still refuse — that is what
//! keeps a second copy of a finished task from running.
//!
//! The control comes from `common::orchestrator` rather than being built here:
//! a fixture that constructs its own describes a different object the moment
//! the shared one gains a piece, and both stay green while they disagree.

use crate::common;

use cog_core::{Task, TaskStatus, TaskType};
use common::orchestrator::control;

#[tokio::test]
async fn admitting_a_row_the_sweep_handed_back_to_pending() {
    let control = control("ws-readmit");
    control
        .submit_goal(
            "goal-readmit",
            vec![Task::new(
                "task-a",
                TaskType::DagNode,
                serde_json::json!({}),
            )],
        )
        .await
        .unwrap();

    // 看门狗退回之后的形状：行在 Pending，而只有 Scheduled 能起。
    assert_eq!(
        control.get_task("task-a").await.unwrap().status,
        TaskStatus::Pending
    );
    assert!(
        control.start_task("task-a").await.is_err(),
        "start_task 只认 Scheduled；这一条拒绝就是就绪消息被丢的来源"
    );

    control.admit_and_start("task-a").await.unwrap();
    assert_eq!(
        control.get_task("task-a").await.unwrap().status,
        TaskStatus::Running,
        "被这条消息点名的 Pending 行，就该照这条消息起"
    );
}

#[tokio::test]
async fn a_finished_row_is_not_started_again_by_a_late_message() {
    let control = control("ws-readmit-done");
    control
        .submit_goal(
            "goal-readmit-done",
            vec![Task::new(
                "task-a",
                TaskType::DagNode,
                serde_json::json!({}),
            )],
        )
        .await
        .unwrap();
    control.admit_and_start("task-a").await.unwrap();
    control
        .complete_task("task-a", serde_json::json!({}))
        .await
        .unwrap();

    assert!(
        control.admit_and_start("task-a").await.is_err(),
        "跑完的行不许因为一条旧消息再起一次"
    );
    assert_eq!(
        control.get_task("task-a").await.unwrap().status,
        TaskStatus::Completed,
        "被拒之后行要留在原状态，不能因为拒了反而被动过"
    );
}
