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

/// 按工作区的那份：为一件活做的调用算在它跑在的那个租户头上，谁的都不算的行
/// 不进任何工作区。
///
/// 这一格是「按租户额度」执法的唯一输入。它错一分，工作区的格子就跟着错一分，
/// 而那个格子同时是管理面的读数与 supervisor 的暂停闸门——读出来的数与拦下来
/// 的动作会是同一句谎话。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_workspaces_day_is_the_calls_made_for_the_tasks_it_runs() {
    let url = database_url();
    let (pool, store) = prepare(&url).await;

    let run = uuid::Uuid::new_v4();
    let day = Utc.with_ymd_and_hms(2004, 3, 4, 0, 0, 0).unwrap();
    let since = day.timestamp();
    let tenant = format!("ws-tenant-{run}");
    let other = format!("ws-other-{run}");
    let twin_home = format!("ws-twin-home-{run}");
    let first = format!("ws-first-{run}");
    let second = format!("ws-second-{run}");
    let elsewhere = format!("ws-elsewhere-{run}");

    for (workspace, task_id) in [(&tenant, &first), (&tenant, &second), (&other, &elsewhere)] {
        sqlx::query(
            "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
             VALUES ($1, $2, '{}'::jsonb, 'pending')",
        )
        .bind(workspace)
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("write a task");
    }
    // 同一个 task_id 在第二个工作区里还有一行，且更新——任务图允许这个形状。
    // 普通 join 会把那一行的 token 数两遍，租户会比实际更早被拦。
    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status, updated_at)
         VALUES ($1, $2, '{}'::jsonb, 'pending', NOW() + interval '1 hour')",
    )
    .bind(&twin_home)
    .bind(&second)
    .execute(&pool)
    .await
    .expect("write the twin task");

    // 两件都在 tenant 里：100 + 50。
    ledger_row(
        &pool,
        day + chrono::Duration::hours(1),
        None,
        Some(&first),
        100,
    )
    .await;
    ledger_row(
        &pool,
        day + chrono::Duration::hours(2),
        None,
        Some(&second),
        50,
    )
    .await;
    // 另一件活跑在另一个租户里。
    ledger_row(
        &pool,
        day + chrono::Duration::hours(3),
        None,
        Some(&elsewhere),
        7,
    )
    .await;
    // 没有任务身份的行：不属于任何租户，不许被编进一个占位工作区。
    ledger_row(&pool, day + chrono::Duration::hours(4), None, None, 11).await;

    let rows = store
        .spend_by_workspace_since(since)
        .await
        .expect("read the workspace totals");
    let of = |workspace: &str| {
        rows.iter()
            .find(|(id, _)| id == workspace)
            .map(|(_, tokens)| *tokens)
    };

    assert_eq!(
        of(&tenant),
        Some(100),
        "twin 那行更新，这一笔按它算（与按人同一条规则）"
    );
    assert_eq!(of(&twin_home), Some(50), "同一个 task_id 的调用只算一次");
    assert_eq!(of(&other), Some(7), "另一件活自己那个租户");
    assert_eq!(
        rows.iter().map(|(_, tokens)| tokens).sum::<u64>(),
        157,
        "没任务身份的那 11 个 token 不进任何工作区：总数必须是 100 + 50 + 7，数成 207 就是同一笔算了两遍"
    );
    assert!(
        rows.iter().all(|(id, _)| !id.is_empty()),
        "空 id 不是工作区，不许成一个格子"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE ts >= $1 AND ts < $2")
        .bind(day)
        .bind(day + chrono::Duration::days(1))
        .execute(&pool)
        .await
        .expect("leave the ledger as it was found");
    for workspace in [&tenant, &other, &twin_home] {
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

/// 「这件活是谁交的、跑在哪个租户」在任务图上只有一处答案，判定与记账读的是同一个。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_attribution_a_task_names_is_what_the_ledger_charges_and_the_gateway_refuses_by() {
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

    let attribution = store
        .task_attribution(&named)
        .await
        .expect("read the attribution")
        .expect("the graph has this task");
    assert_eq!(
        attribution.submitter.as_deref(),
        Some(person.as_str()),
        "判定与账本读的是同一处：任务行上的那一个名字"
    );
    assert_eq!(
        attribution.workspace_id.as_deref(),
        Some(format!("ws-{run}").as_str()),
        "同一行上的租户也要一并答出来：按工作区执法用的是这一格，"
    );
    assert_eq!(
        store
            .task_attribution(&anonymous)
            .await
            .expect("read the attribution"),
        Some(cog_core::TaskAttribution {
            submitter: None,
            workspace_id: Some(format!("ws-{run}")),
        }),
        "没有提交人的活不许被编一个名字出来，可它确实跑在某个租户里——这两件事"
    );
    assert_eq!(
        store
            .task_attribution(&absent)
            .await
            .expect("read the attribution"),
        None,
        "图里没有这件活：答不上来，而不是答错一个人"
    );

    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
        .bind(format!("ws-{run}"))
        .execute(&pool)
        .await
        .expect("leave the graph as it was found");
}
