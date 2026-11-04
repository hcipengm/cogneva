//! 账本按人、按任务的两个当日汇总，against a live PostgreSQL.
//!
//! 这两个读各自喂一格里的一份投影：按人的那个覆盖 `quota:user:used:<id>`，判定
//! 「这个人今天还能不能花」；按任务的那个覆盖 `quota:task:used:<id>`，判定「这件
//! 工作还能不能花」。投影的整个前提是「格子里的数与账上逐字相同」，所以这里判的
//! 正是那句前提——判据在数据库里，不在我们这一侧：
//!
//! - 一个人的花销有**两处**证据，行上的属主，或行所指任务的提交人。只认其中一处，
//!   另一路上的花销就不进这个人的格子，于是他在那条路上永远花不完；
//! - 两处都有时属主优先，与覆盖写那一段用的是同一条规则；
//! - 按任务的那份是**当日**的，不是这件工作迄今的总额——框错了窗口，一件上午就
//!   花完预算的工作会在午夜之后仍然被拒；
//! - 没有提交人的任务、没人认领的行，都不该被编进任何一个名字下。
//!
//! A DB-free test would only be restating the queries, so this file is ignored by
//! default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-observability --test person_and_task_spend -- --ignored
//! ```
//!
//! The rows it writes are deleted at the end, and the graph rows go with them.

use chrono::{TimeZone, Utc};
use cog_observability::usage_store::LlmUsageStore;
use sqlx::PgPool;

use cog_test_support::database_url;

/// 一行账：谁、哪件活、多少钱。
async fn ledger_row(
    pool: &PgPool,
    ts: chrono::DateTime<Utc>,
    owner: Option<&str>,
    task: Option<&str>,
    tokens: i64,
) {
    sqlx::query(
        "INSERT INTO gateway_llm_usage
             (id, ts, upstream, api_style, model, result, tokens_input, tokens_output, task_id, owner_id)
         VALUES ($1, $2, 'test-upstream', 'openai', 'test-model', 'ok', $3, 0, $4, $5)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(ts)
    .bind(tokens)
    .bind(task)
    .bind(owner)
    .execute(pool)
    .await
    .expect("write a ledger row");
}

async fn prepare(url: &str) -> (PgPool, LlmUsageStore) {
    let pool = PgPool::connect(url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(url)
        .await
        .expect("connect the store");
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    store
        .init_schema(&schema_runner)
        .await
        .expect("create the ledger schema");
    // 按人那一份要与图连一次，所以图的那张表必须在。只建这里用到的列：它被当成
    // 一张「谁交的活」的名单读，不当成任务读。
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS cog_dag_tasks (
             workspace_id TEXT  NOT NULL,
             task_id      TEXT  NOT NULL,
             task         JSONB NOT NULL DEFAULT '{}'::jsonb,
             status       TEXT,
             updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
             PRIMARY KEY (workspace_id, task_id))",
    )
    .execute(&pool)
    .await
    .expect("create the task graph table");
    (pool, store)
}

/// 按人的那份把两处证据都算进来，属主优先；两处都没有的行不进任何人的格子。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_persons_day_is_what_the_ledger_charges_them_by_either_evidence() {
    let url = database_url();
    let (pool, store) = prepare(&url).await;

    let run = uuid::Uuid::new_v4();
    let day = Utc.with_ymd_and_hms(2002, 1, 1, 0, 0, 0).unwrap();
    let since = day.timestamp();
    let yes = format!("spend-by-person-{run}");
    let either = format!("spend-by-person-either-{run}");
    let task = format!("spend-by-person-task-{run}");
    let nameless = format!("spend-by-person-nameless-{run}");
    let retitled = format!("spend-by-person-retitled-{run}");

    for (task_id, submitted_by) in [
        (&task, Some(yes.as_str())),
        // 一件没有提交人的活：为它花的钱不进任何人的格子。
        (&nameless, None),
        // 提交人与声明属主都不是本人的那一件：属主优先，账只记在属主头上。
        (&retitled, Some("someone-else")),
    ] {
        let task_json = match submitted_by {
            Some(person) => serde_json::json!({ "submitted_by": person }),
            None => serde_json::json!({}),
        };
        sqlx::query(
            "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
             VALUES ($1, $2, $3, 'pending')
             ON CONFLICT (workspace_id, task_id) DO UPDATE SET task = EXCLUDED.task",
        )
        .bind(format!("ws-{run}"))
        .bind(task_id)
        .bind(task_json)
        .execute(&pool)
        .await
        .expect("write a task");
    }

    // 声明了属主的一行：直接算这个人的。
    ledger_row(
        &pool,
        day + chrono::Duration::hours(1),
        Some(&yes),
        None,
        100,
    )
    .await;
    // 没有属主、但所指的活是这个人的：靠图连过来。
    ledger_row(
        &pool,
        day + chrono::Duration::hours(2),
        None,
        Some(&task),
        40,
    )
    .await;
    // 属主与提交人同时存在：属主说了算，这 7 个 token 不算到提交人头上。
    ledger_row(
        &pool,
        day + chrono::Duration::hours(3),
        Some(&either),
        Some(&retitled),
        7,
    )
    .await;
    // 两处都没有：谁的格子都不写。
    ledger_row(&pool, day + chrono::Duration::hours(4), None, None, 11).await;
    // 所指的活没有提交人：同样不进任何人的格子。
    ledger_row(
        &pool,
        day + chrono::Duration::hours(5),
        None,
        Some(&nameless),
        13,
    )
    .await;

    // 同一个 task_id 在第二个工作区里还有一行——任务表的主键是
    // `(workspace_id, task_id)`，这是它允许的形状。普通 join 会把这一行的 token
    // 数两遍，格子里的数凭空变大，人比实际更早被拒。
    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
         VALUES ($1, $2, $3, 'pending')",
    )
    .bind(format!("ws-other-{run}"))
    .bind(&task)
    .bind(serde_json::json!({ "submitted_by": yes }))
    .execute(&pool)
    .await
    .expect("write the twin task");

    let people = store
        .spend_by_person_since(since)
        .await
        .expect("read the person totals");
    let of = |who: &str| {
        people
            .iter()
            .find(|(person, _)| person == who)
            .map(|(_, tokens)| *tokens)
    };

    assert_eq!(
        of(&yes),
        Some(140),
        "属主那一行 + 提交人是他的那件活；同一个 task_id 的两行图记录不许把 40 数成 80"
    );
    assert_eq!(of(&either), Some(7), "两处证据都在时属主优先");
    assert!(
        of("someone-else").is_none(),
        "提交人不是被记的那一个：这一行的属主已经说了算"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE ts >= $1 AND ts < $2")
        .bind(day)
        .bind(day + chrono::Duration::days(1))
        .execute(&pool)
        .await
        .expect("leave the ledger as it was found");
    for workspace in [format!("ws-{run}"), format!("ws-other-{run}")] {
        sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
            .bind(workspace)
            .execute(&pool)
            .await
            .expect("leave the graph as it was found");
    }
}

/// 按任务的那份只数当天的：更早的花销不进格子，没花过的不出现。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_tasks_cell_is_todays_spend_not_the_jobs_lifetime_total() {
    let url = database_url();
    let (pool, store) = prepare(&url).await;

    let run = uuid::Uuid::new_v4();
    let today = Utc.with_ymd_and_hms(2003, 1, 2, 0, 0, 0).unwrap();
    let yesterday = today - chrono::Duration::days(1);
    let spent = format!("spend-by-task-{run}");
    let idle = format!("spend-by-task-idle-{run}");

    ledger_row(
        &pool,
        yesterday + chrono::Duration::hours(1),
        None,
        Some(&spent),
        900,
    )
    .await;
    ledger_row(
        &pool,
        today + chrono::Duration::hours(1),
        None,
        Some(&spent),
        30,
    )
    .await;

    let rows = store
        .spend_by_task_since(&[spent.clone(), idle.clone()], today.timestamp())
        .await
        .expect("read the task totals");

    assert_eq!(
        rows,
        vec![(spent.clone(), 30)],
        "格子里是今天那一笔；昨天的 900 不该进来——那会让一件上午就花完预算的\
         工作在午夜之后继续被拒。没花过的那件不出现，补 0 是调用方的事"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE ts >= $1 AND ts < $2")
        .bind(yesterday)
        .bind(today + chrono::Duration::days(1))
        .execute(&pool)
        .await
        .expect("leave the ledger as it was found");
}

/// 「这件活是谁交的」在任务图上只有一处答案，判定与记账读的是同一个。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_submitter_a_task_names_is_the_person_the_ledger_would_charge() {
    let url = database_url();
    let (pool, store) = prepare(&url).await;

    let run = uuid::Uuid::new_v4();
    let named = format!("submitter-named-{run}");
    let anonymous = format!("submitter-anonymous-{run}");
    let absent = format!("submitter-absent-{run}");
    let person = format!("person-{run}");

    for (task_id, task_json) in [
        (&named, serde_json::json!({ "submitted_by": person })),
        (&anonymous, serde_json::json!({})),
    ] {
        sqlx::query(
            "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
             VALUES ($1, $2, $3, 'pending')",
        )
        .bind(format!("ws-{run}"))
        .bind(task_id)
        .bind(task_json)
        .execute(&pool)
        .await
        .expect("write a task");
    }

    assert_eq!(
        store
            .task_submitter(&named)
            .await
            .expect("read the submitter"),
        Some(person.clone()),
        "判定与账本读的是同一处：任务行上的那一个名字"
    );
    assert_eq!(
        store
            .task_submitter(&anonymous)
            .await
            .expect("read the submitter"),
        None,
        "没有提交人的活不许被编一个名字出来"
    );
    assert_eq!(
        store
            .task_submitter(&absent)
            .await
            .expect("read the submitter"),
        None,
        "图里没有这件活：答不上来，而不是答错一个人"
    );

    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
        .bind(format!("ws-{run}"))
        .execute(&pool)
        .await
        .expect("leave the graph as it was found");
}
