//! 完成 / 失败 / 取消一条任务前，那条把整个 workspace 锁住的语句，必须让两个
//! 进程按**同一个顺序**取行。
//!
//! 生产上 2026-11-05 一天里 Postgres 报出 13 次死锁，每次等待链的两端都是同一条语句：
//!
//! ```text
//! SELECT task_id, task FROM cog_dag_tasks WHERE workspace_id = $1 FOR UPDATE
//! ```
//!
//! 同一条语句、同一张表，两个进程却互相等对方的行——因为「哪一行先被取到」这件事
//! 它没有规定过：`cog_dag_tasks` 上有 `(workspace_id, status)`、`(workspace_id,
//! face_id, status)`、`(task_id, updated_at)` 几条索引，几次执行完全可以走不同
//! 的计划、给出不同的行序；行本身还在被不停地更新、搬到别的页上去，物理顺序也不是
//! 个稳定的东西。行序不定，两个进程就可能各握着对方要的那一行等下去，Postgres 挑
//! 一个杀掉。被杀的那个事务是一次**已经算完、结果没落库**的完成——下一位持锁者只
//! 看得到「租约过期」，于是把整条任务重新派发、重新花一遍钱。
//!
//! 判据不是「SQL 里有没有写 ORDER BY」，是那条语句交给两个进程的是不是同一个顺序。
//! 所以这里让两个会话**故意走不同的计划**（一个只许顺序扫描、一个不许顺序扫描），
//! 各开一只游标按行锁（`FOR UPDATE` 在取到那一行时才加锁，所以 `FETCH 1` 就停在第
//! 一行上），然后一起把剩下的取完：
//!
//! - 两边第一行相同 → 第二个会话在第一次取行时就等住了，等先来的那个提交，然后自己
//!   走完——今天的语句就是这个样子；
//! - 两边第一行不同 → 各握着对方的行等下去，Postgres 一秒后杀掉一个，那条连接报
//!   40P01。这正是生产日志的形状。
//!
//! 另有一处走完整路径的用例：同一个 workspace 上并发完成多条任务，每条都成功、每条
//! 都落在 `Completed`。它不保证能复现死锁（死锁取决于计划与时序），但它守的是「这
//! 条路还能用」——死锁在这里出现时是 `dag_complete_task` 返回错误，不是静默。
//!
//! 需要活库，默认忽略：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test workspace_lock_takes_rows_in_one_order -- --ignored
//! ```

use std::sync::Arc;

use sqlx::{PgPool, Row};

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::postgres::state_backend::DAG_LOCK_WORKSPACE_SQL;
use cog_storage::PostgresStateBackend;
use cog_test_support::{database_url, drop_schema, own_schema, schema_pool_with};

/// 一次用例的那个 workspace。用例各建各的 schema，所以名字不用再带前缀。
const WORKSPACE: &str = "ws-lock-order";

/// 这批任务写进库里的顺序，故意与 `task_id` 的字典序相反、与 `status` 的分组也不
/// 一致：这样的话「物理顺序」「按 status 的索引顺序」「按 task_id 的顺序」三者两两
/// 不同，两个会话只要各自走了不同的计划，第一行就不可能是同一行。
///
/// 每一对是（task_id，status），按写入顺序给。
const ROWS: [(&str, &str); 8] = [
    ("t-08", "completed"),
    ("t-07", "pending"),
    ("t-06", "running"),
    ("t-05", "completed"),
    ("t-04", "pending"),
    ("t-03", "running"),
    ("t-02", "completed"),
    ("t-01", "pending"),
];

async fn setup() -> (String, PgPool) {
    let url = database_url();
    let schema = own_schema(&url, "probe_workspace_lock_order").await;
    let pool = schema_pool_with(&url, &schema, 4).await;
    sqlx::raw_sql(cog_storage::postgres::state_backend::DDL)
        .execute(&pool)
        .await
        .expect("apply the startup schema");
    (schema, pool)
}

/// 一行任务，`task` 那一列是可以反序列化成 [`Task`] 的完整载荷——走完整路径的那个
/// 用例要把它读回来判断状态，一个只有 id 的载荷在那里会先炸在反序列化上。
fn task(id: &str, status: &str) -> serde_json::Value {
    let mut t = Task::new(
        id,
        TaskType::Custom("lock-order-probe".to_string()),
        serde_json::json!({ "description": "lock order probe" }),
    );
    t.workspace_id = Some(WORKSPACE.to_string());
    t.status = match status {
        "completed" => TaskStatus::Completed,
        "running" => TaskStatus::Running,
        _ => TaskStatus::Pending,
    };
    serde_json::to_value(t).expect("a task serializes")
}

/// 按 [`ROWS`] 的顺序把行写进去。
async fn seed(pool: &PgPool) {
    for (id, status) in ROWS {
        sqlx::query(
            "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(WORKSPACE)
        .bind(id)
        .bind(task(id, status))
        .bind(status)
        .execute(pool)
        .await
        .expect("write a task");
    }
    // 计划要靠统计信息才稳：不 ANALYZE，两个会话在这么小的表上可能都挑顺序扫描，
    // 那样差别就被抹平了，用例量的是「两个计划」而不是「两行数据」。
    sqlx::query("ANALYZE cog_dag_tasks")
        .execute(pool)
        .await
        .expect("refresh the planner statistics");
}

/// 在 `conn` 上开一只游标，读的正是生产那条语句。
///
/// `order` 决定这个会话走哪个计划：`false` 关掉索引与位图扫描，只剩顺序扫描；
/// `true` 关掉顺序扫描，只能走索引。两者取到的行序在生产上确实不同——这就是那 13
/// 次死锁的来路。
async fn open_cursor(conn: &mut sqlx::PgConnection, cursor: &str, use_index: bool) {
    let plan = if use_index {
        "SET enable_seqscan = off"
    } else {
        "SET enable_indexscan = off; SET enable_bitmapscan = off"
    };
    for statement in plan.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("set the session's plan preferences");
    }
    // 卡住就报错，别让用例一直等下去：等锁超过三秒就够说明它被挡住了。
    sqlx::query("SET statement_timeout = '3s'")
        .execute(&mut *conn)
        .await
        .expect("bound the wait");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("open the transaction the cursor lives in");
    sqlx::query(&format!(
        "DECLARE {cursor} CURSOR FOR {DAG_LOCK_WORKSPACE_SQL}"
    ))
    .bind(WORKSPACE)
    .execute(&mut *conn)
    .await
    .expect("declare the cursor");
}

/// 这条语句在这个会话上的计划，拼成一段文本。
async fn explain(conn: &mut sqlx::PgConnection, use_index: bool) -> String {
    let side = if use_index { "index" } else { "seq" };
    sqlx::query(&format!(
        "EXPLAIN (COSTS OFF) /* {side} */ {DAG_LOCK_WORKSPACE_SQL}"
    ))
    .bind(WORKSPACE)
    .fetch_all(&mut *conn)
    .await
    .expect("explain the lock statement")
    .iter()
    .map(|row| row.get::<String, _>("QUERY PLAN"))
    .collect::<Vec<_>>()
    .join("\n")
}

/// 按 `task_id` 排在最前的那一行——全序定了之后，两个会话都只能从它开始。
fn expected_first() -> String {
    ROWS.iter()
        .map(|(id, _)| id.to_string())
        .min()
        .expect("the probe workspace is seeded")
}

/// 取一行，返回它的 `task_id`；没有行了就是 `None`。
async fn fetch_one(conn: &mut sqlx::PgConnection, cursor: &str) -> Result<Option<String>, String> {
    let row = sqlx::query(&format!("FETCH 1 FROM {cursor}"))
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| e.to_string())?;
    Ok(row.map(|r| r.get::<String, _>("task_id")))
}

/// 把游标剩下的行取完；返回取到的 `task_id`，按取到的顺序。
async fn drain(conn: &mut sqlx::PgConnection, cursor: &str) -> Result<Vec<String>, String> {
    let mut ids = Vec::new();
    loop {
        match fetch_one(conn, cursor).await? {
            Some(id) => ids.push(id),
            None => return Ok(ids),
        }
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_rows_come_back_in_one_order_whatever_plan_the_session_took() {
    let (schema, pool) = setup().await;
    let url = database_url();
    seed(&pool).await;

    let mut expected: Vec<String> = ROWS.iter().map(|(id, _)| id.to_string()).collect();
    expected.sort();

    // 同一个 workspace、两条不同的计划，各读一遍。读回来的顺序必须一样，而且必须是
    // `task_id` 的升序：顺序一旦由计划或行的物理位置决定，两个进程就可能各握着一头。
    let mut seen = Vec::new();
    for (cursor, use_index) in [("slow_scan", false), ("fast_scan", true)] {
        let mut conn = pool.acquire().await.expect("take a connection");
        open_cursor(&mut conn, cursor, use_index).await;
        let plan = explain(&mut conn, use_index).await;
        seen.push((
            drain(&mut conn, cursor).await.expect("drain the cursor"),
            plan,
        ));
        sqlx::query("ROLLBACK")
            .execute(&mut *conn)
            .await
            .expect("release the locks");
        drop(conn);
    }

    // 先确认这两个会话真的走了不同的计划：都落到顺序扫描上的话，下面那条判断量的是
    // 同一件事，用例会一直绿，而它以为自己证了「计划无关」。
    let (seq_plan, index_plan) = (&seen[0].1, &seen[1].1);
    assert!(
        seq_plan.contains("Seq Scan") && !index_plan.contains("Seq Scan"),
        "两个会话没有走成不同的计划，这条用例量不出「行序与计划无关」：\n\
         只许顺序扫描的那个：\n{seq_plan}\n只许索引的那个：\n{index_plan}"
    );

    for (rows, plan) in &seen {
        assert_eq!(
            rows, &expected,
            "锁住整个 workspace 的那条语句要按 task_id 给行一个全序，这个会话读回来的\
             却是另一个顺序（计划：\n{plan}）"
        );
    }

    drop(pool);
    drop_schema(&url, &schema).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn only_one_session_can_hold_the_first_row_at_a_time() {
    let (schema, pool) = setup().await;
    let url = database_url();
    seed(&pool).await;

    // 两个会话各开一只游标（`FOR UPDATE` 在取到那一行时才加锁），同时去取第一行。
    // 行序相同的时候它们抢的是同一行——后来的那个只能在第一次取行上等住，等前一个
    // 提交；行序不同的时候两边各拿到一行，于是各自往下走、各自去要对方手里那一行，
    // Postgres 一秒后杀掉一个。生产上那 13 次死锁就是后一种。
    //
    // 所以这里的判据是：**同一时刻只有一个会话能握住这个 workspace 的第一行**。
    const SESSIONS: [(&str, bool); 2] = [("slow_scan", false), ("fast_scan", true)];
    let barrier = Arc::new(tokio::sync::Barrier::new(SESSIONS.len()));
    let mut handles = Vec::new();
    for (i, (cursor, use_index)) in SESSIONS.iter().enumerate() {
        let mut conn = pool.acquire().await.expect("take a connection");
        open_cursor(&mut conn, cursor, *use_index).await;
        let barrier = Arc::clone(&barrier);
        let cursor = *cursor;
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let first = fetch_one(&mut conn, cursor).await;
            (i, conn, first)
        }));
    }

    let mut held = Vec::new();
    let mut blocked = Vec::new();
    for handle in handles {
        let (i, mut conn, first) = handle.await.expect("the fetch task did not panic");
        match first {
            Ok(id) => held.push((i, conn, id)),
            Err(e) => {
                // 等锁等到超时是这条用例**期望**的那种结果；其他错误（连不上、语句写错）
                // 不是，别让它们扮成「挡住了」。
                assert!(
                    e.contains("statement timeout") || e.contains("deadlock detected"),
                    "取第一行时报了一个与等锁无关的错：{e}"
                );
                // 语句超时会把事务置为已中止，这里只能回滚，不能再往下走。
                sqlx::query("ROLLBACK").execute(&mut *conn).await.ok();
                blocked.push(conn);
            }
        }
    }

    assert_eq!(
        held.len(),
        1,
        "同一时刻有 {} 个会话握住了这个 workspace 的第一行：行序不是由数据定的，\
         于是两个进程可以各握着对方要的那一行等下去。锁住整个 workspace 的语句必须\
         按 task_id 给这批行一个全序",
        held.len()
    );
    let (winner_index, mut winner, first) = held.pop().expect("the session that held a row");
    assert_eq!(
        first.as_deref(),
        Some(expected_first().as_str()),
        "握住的那一行不是 task_id 最小的那一行"
    );

    // 挡住不等于锁死：先来的那个取完整个集合、提交，后来的那个接着就能走完。
    let cursor = SESSIONS[winner_index].0;
    let rest = drain(&mut winner, cursor).await.expect("drain the cursor");
    let mut whole = vec![first.expect("the first row")];
    whole.extend(rest);
    let mut expected: Vec<String> = ROWS.iter().map(|(id, _)| id.to_string()).collect();
    expected.sort();
    assert_eq!(
        whole, expected,
        "握住第一行的会话读回来的不是整个 workspace 按 task_id 排好的那份"
    );
    sqlx::query("COMMIT")
        .execute(&mut *winner)
        .await
        .expect("release the locks");

    drop(blocked);
    drop(winner);
    drop(pool);
    drop_schema(&url, &schema).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn concurrent_completions_in_one_workspace_all_land() {
    let (schema, pool) = setup().await;
    let url = database_url();
    seed(&pool).await;

    // 每一行都是 `running`：`dag_complete_task` 只接受正在跑的任务。用独立的
    // workspace 把这两处用例的数据分开，免得一起跑时互相数到对方的行。
    for (id, _) in ROWS {
        sqlx::query(
            "UPDATE cog_dag_tasks SET status = 'running',
                    task = jsonb_set(task, '{status}', '\"running\"')
              WHERE workspace_id = $1 AND task_id = $2",
        )
        .bind(WORKSPACE)
        .bind(id)
        .execute(&pool)
        .await
        .expect("mark the task running");
    }

    let backend = Arc::new(PostgresStateBackend::new(pool.clone()));
    let mut handles = Vec::new();
    for (id, _) in ROWS {
        let backend = Arc::clone(&backend);
        handles.push(tokio::spawn(async move {
            let outcome = backend
                .dag_complete_task(WORKSPACE, id, serde_json::json!({ "done": id }))
                .await;
            (id, outcome)
        }));
    }

    let mut failed = Vec::new();
    for handle in handles {
        let (id, outcome) = handle.await.expect("the completion task did not panic");
        if let Err(e) = outcome {
            failed.push(format!("{id}: {e}"));
        }
    }
    assert!(
        failed.is_empty(),
        "同一个 workspace 上并发完成的几条任务里，有 {} 条没能完成——两条语句抢同一个\
         全序时就该出这个错，不该让它们撞上：\n{}",
        failed.len(),
        failed.join("\n")
    );

    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cog_dag_tasks WHERE workspace_id = $1 AND status = 'completed'",
    )
    .bind(WORKSPACE)
    .fetch_one(&pool)
    .await
    .expect("count the completed rows");
    assert_eq!(
        completed,
        ROWS.len() as i64,
        "完成调用都返回了成功，落在库里的却不是 {} 条 completed",
        ROWS.len()
    );

    // 结果要真的写进了行里：只把状态翻过去的那种「完成」，读的人拿不到任何结果。
    for (id, _) in ROWS {
        let stored: serde_json::Value = sqlx::query_scalar(
            "SELECT task FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id = $2",
        )
        .bind(WORKSPACE)
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("read the completed row");
        assert_eq!(
            stored["result"],
            serde_json::json!({ "done": id }),
            "{id} 的状态是 completed，结果却不是这次完成写进去的那个"
        );
    }

    drop(pool);
    drop_schema(&url, &schema).await;
}
