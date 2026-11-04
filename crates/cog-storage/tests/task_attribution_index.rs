//! 按 `task_id` 的单点查找要有一条真能用的索引。
//!
//! 额度判定在 LLM 代理路径上每发任务调用都要问一次「这件活是谁交的、跑在哪个
//! 租户」。任务图的主键是 `(workspace_id, task_id)`，只带 `task_id` 的查询用不上
//! 它；没有一条以 `task_id` 打头的索引，这次读就是**每发请求**扫一遍整张任务图，
//! 而这张表只会随任务数增长。
//!
//! 判据不是「索引名在不在」——名字对了、列不对的索引一样服务不了这条读，而按
//! 名字判的用例在把列改成 `workspace_id` 之后照样绿。这里判两件事：
//!
//! - 那条索引的第一列是 `task_id`；
//! - 把顺序扫描关掉之后，同一条读的计划里没有 `Seq Scan`。
//!
//! 需要活库，默认忽略：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test task_attribution_index -- --ignored
//! ```

use sqlx::{PgPool, Row};

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool_with};

/// 与额度判定读的是同一条查询：最近更新的那一行给出的提交人与工作区。
const ATTRIBUTION_LOOKUP: &str = "SELECT NULLIF(task->>'submitted_by', '') AS person,
                                         NULLIF(workspace_id, '')        AS workspace_id
                                    FROM cog_dag_tasks
                                   WHERE task_id = $1
                                   ORDER BY updated_at DESC
                                   LIMIT 1";

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_lookup_by_task_id_can_use_an_index_instead_of_scanning_the_graph() {
    let url = database_url();
    let schema = own_schema(&url, "probe_task_attribution_index").await;
    let pool: PgPool = schema_pool_with(&url, &schema, 1).await;

    sqlx::raw_sql(cog_storage::postgres::state_backend::DDL)
        .execute(&pool)
        .await
        .expect("apply the startup schema");
    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
         VALUES ('ws-1', 't-1', '{\"submitted_by\": \"u-1\"}'::jsonb, 'pending')",
    )
    .execute(&pool)
    .await
    .expect("write a task");

    // 一条连接上把「关掉顺序扫描」与 EXPLAIN 放在一起：`SET` 只作用于本会话，
    // 从池里另取一条连接会让这条读回到默认设置下，计划就说明不了任何事。
    let mut conn = pool.acquire().await.expect("take the pool's connection");

    let leading: Option<String> = sqlx::query(
        "SELECT a.attname
           FROM pg_index i
           JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = 1
          WHERE n.nspname = $1 AND c.relname = 'idx_cog_dag_tasks_task_updated'",
    )
    .bind(&schema)
    .fetch_optional(&mut *conn)
    .await
    .expect("read the index definition")
    .map(|row| row.get::<String, _>("attname"));
    assert_eq!(
        leading.as_deref(),
        Some("task_id"),
        "按任务 id 找一行，索引的第一列必须是 task_id；以 workspace_id 打头的索引\
         对这条读一点用都没有，而「有个索引叫这个名字」判不出这一点"
    );

    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *conn)
        .await
        .expect("turn sequential scans off for this session");
    let plan: String = sqlx::query(&format!("EXPLAIN {ATTRIBUTION_LOOKUP}"))
        .bind("t-1")
        .fetch_all(&mut *conn)
        .await
        .expect("explain the attribution lookup")
        .iter()
        .map(|row| row.get::<String, _>("QUERY PLAN"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan.to_lowercase().contains("seq scan"),
        "额度判定每发任务调用都走这条读；计划里出现顺序扫描，就是每发请求都在扫\
         整张任务图\n{plan}"
    );

    drop(conn);
    drop_schema(&url, &schema).await;
}
