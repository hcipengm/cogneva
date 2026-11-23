//! 尝试行记下那一段活由哪把座位、哪个角色跑的——人（工号）写在任务行上。
//!
//! R17 的判据是「同一个人、两段座位、两段尝试」：换一把椅子再做同一件活，
//! 任务行上的工号不变，尝试行上的执行地址与角色变了。执行地址是这份账里唯一
//! 写着角色的地方（`squad:<任务>-<座位>`），所以尝试表自己不记角色的话，读的人
//! 只能从地址里现猜，而地址一旦换拼法就谁也答不出来。
//!
//! 这条用例对着真的 PostgreSQL 跑，因为它判的正是那两条语句写出来的列：迁移
//! 与启动 schema 都把 `role` 加上了（另有一道纯文本的门判两者逐条相同），
//! 真正写进去的只有 `open_attempt`。
//!
//! 默认忽略，需要一个一次性的库：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test task_attempt_records_the_seat -- --ignored
//! ```

use chrono::Utc;
use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

fn task(id: &str, owner_unit: &str) -> Task {
    let mut task = Task::new(id, TaskType::DagNode, serde_json::json!({}));
    task.owner_unit = Some(owner_unit.to_string());
    task.acceptance = vec!["the row is done when it says so".to_string()];
    task
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn two_attempts_by_two_seats_of_one_person_keep_the_number_and_name_the_role() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");

    let marker = format!(
        "attempt-seat-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let workspace = format!("{marker}:ws");
    let id = format!("{marker}:attempted");
    let person = "E0412";
    let first = cog_core::agent_id_for(&id, cog_core::roles::PLANNER);
    let second = cog_core::agent_id_for(&id, cog_core::roles::GENERATOR);
    let lease = chrono::Duration::seconds(300);

    backend
        .dag_set_task(&workspace, &id, &task(&id, person))
        .await
        .expect("seed the task");

    // 第一段：plan 座位接走、跑起来（尝试在这一步开出来）、失败、回队。
    let mut claimed = task(&id, person);
    claimed.take_claim(&first, lease, Utc::now());
    backend
        .dag_claim_task(&workspace, &id, &[TaskStatus::Pending], &claimed)
        .await
        .expect("the first seat takes the row");
    let mut running = claimed.clone();
    running.status = TaskStatus::Running;
    running.started_at = Some(Utc::now());
    backend
        .dag_transition_owned_task(&workspace, &id, &[TaskStatus::Scheduled], &running, person)
        .await
        .expect("the first attempt opens");
    let mut failed = running.clone();
    failed.status = TaskStatus::Failed;
    backend
        .dag_transition_owned_task(&workspace, &id, &[TaskStatus::Running], &failed, person)
        .await
        .expect("the first attempt closes");
    let mut pending = failed.clone();
    pending.status = TaskStatus::Pending;
    pending.clear_run();
    backend
        .dag_transition_owned_task(&workspace, &id, &[TaskStatus::Failed], &pending, person)
        .await
        .expect("the row goes back to the queue");

    // 第二段：同一个人换一把椅子接走、跑起来。
    let mut claimed = pending.clone();
    claimed.take_claim(&second, lease, Utc::now());
    backend
        .dag_claim_task(&workspace, &id, &[TaskStatus::Pending], &claimed)
        .await
        .expect("the same person's other seat takes the row");
    let mut running = claimed.clone();
    running.status = TaskStatus::Running;
    running.started_at = Some(Utc::now());
    backend
        .dag_transition_owned_task(&workspace, &id, &[TaskStatus::Scheduled], &running, person)
        .await
        .expect("the second attempt opens");

    let attempts: Vec<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT owner, role FROM task_attempt WHERE task_id = $1 ORDER BY attempt_no",
    )
    .bind(&id)
    .fetch_all(&pool)
    .await
    .expect("read the attempts back");
    assert_eq!(
        attempts,
        vec![
            (Some(first.clone()), Some("planner".to_string())),
            (Some(second.clone()), Some("generator".to_string())),
        ],
        "两段尝试各记自己那把座位与它的角色；角色只写在执行地址里，落账时要反解出来"
    );

    let numbers: Vec<String> = sqlx::query_as::<_, (String,)>(
        "SELECT DISTINCT owner_unit FROM task_defined WHERE task_id = $1",
    )
    .bind(&id)
    .fetch_all(&pool)
    .await
    .expect("read the definitions back")
    .into_iter()
    .map(|(number,)| number)
    .collect();
    assert_eq!(
        numbers,
        vec![person.to_string()],
        "换座位不是换人：两段尝试之间，任务行上的工号始终是同一个"
    );
    assert_ne!(first, second, "这条用例的前提是两把椅子不是一个名字");
}
