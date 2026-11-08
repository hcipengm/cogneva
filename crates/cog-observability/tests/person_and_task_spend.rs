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
use sqlx::{Executor, PgPool};

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

/// 一行被拒的账：哪一种拒绝、谁、哪件活。被拒的一发不花钱。
async fn refused_row(
    pool: &PgPool,
    ts: chrono::DateTime<Utc>,
    result: cog_core::UsageResult,
    refused_scope: Option<&str>,
    owner: Option<&str>,
    task: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO gateway_llm_usage
             (id, ts, upstream, api_style, model, result, tokens_input, tokens_output, task_id, owner_id, refused_scope)
         VALUES ($1, $2, 'test-upstream', 'openai', 'test-model', $3, 0, 0, $4, $5, $6)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(ts)
    .bind(result.as_str())
    .bind(task)
    .bind(owner)
    .bind(refused_scope)
    .execute(pool)
    .await
    .expect("write a refusal row");
}

/// 建表要在这个进程里串行。PostgreSQL 的 `CREATE TABLE IF NOT EXISTS` 不是
/// 原子的：两个用例并发走到建表这一步时，一个建成了，另一个拿到
/// `23505 duplicate key ... pg_type_typname_nsp_index`。竞态在夹具里，不在被测
/// 代码里——被测的读只读已经存在的表。这条错误实测让 CI 红过一次
/// （`cog_dag_tasks` 的建表）。
static SCHEMA_DDL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

fn schema_ddl_lock() -> &'static tokio::sync::Mutex<()> {
    SCHEMA_DDL.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn prepare(url: &str) -> (PgPool, LlmUsageStore) {
    let pool = PgPool::connect(url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(url)
        .await
        .expect("connect the store");
    let _ddl = schema_ddl_lock().lock().await;
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    store
        .init_schema(&schema_runner)
        .await
        .expect("create the ledger schema");
    // 这几份读连的是任务账本。建表用 095 迁移自己那份 DDL，不在这里手抄一遍列：
    // 手抄的建表与被测的查询各写一份，抄错一处这里照样绿，而线上那条会红。
    let ledger_ddl = include_str!("../../cog-storage/migrations/postgres/095_task_ledger.up.sql");
    pool.execute(ledger_ddl)
        .await
        .expect("create the task ledger");
    (pool, store)
}

/// 任务账本里的一版定义：谁交的、跑在哪个租户，都在这一行上。
///
/// 定义按版本追加，同一个 id 可以有多版；读的人取 `rev` 最大的那一版，所以
/// 写第二版就是「这件活被改过」。
async fn task_rev(
    pool: &PgPool,
    workspace: &str,
    task_id: &str,
    rev: i64,
    submitted_by: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO task_defined
             (task_id, rev, workspace_id, kind, input, created_at, submitted_by)
         VALUES ($1, $2, $3, 'dag_node', 'null', NOW(), $4)
         ON CONFLICT (task_id, rev) DO UPDATE
            SET workspace_id = EXCLUDED.workspace_id,
                submitted_by = EXCLUDED.submitted_by",
    )
    .bind(task_id)
    .bind(rev)
    .bind(workspace)
    .bind(submitted_by)
    .execute(pool)
    .await
    .expect("write a task definition");
}

/// 删掉这一次运行写下的每一版定义。按 run id 收，不按工作区收：别的用例可能正
/// 读着它自己的工作区。
async fn clear_tasks(pool: &PgPool, run: &str) {
    sqlx::query("DELETE FROM task_defined WHERE task_id LIKE '%' || $1 || '%'")
        .bind(run)
        .execute(pool)
        .await
        .expect("leave the graph as it was found");
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
        task_rev(&pool, &format!("ws-{run}"), task_id, 1, submitted_by).await;
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

    // 同一件活再写一版定义，提交人不变。定义按版本追加，普通 join 会把这一笔
    // token 按版本数重复计，格子里的数凭空变大，人比实际更早被拒。
    task_rev(&pool, &format!("ws-other-{run}"), &task, 2, Some(&yes)).await;

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
        "属主那一行 + 提交人是他的那件活；同一个 task_id 的两版定义不许把 40 数成 80"
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
    clear_tasks(&pool, &run.to_string()).await;
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
        task_rev(&pool, workspace, task_id, 1, None).await;
    }
    // 第二件活后来被搬到了另一个工作区，第 2 版定义记的是新家。以最新一版写下
    // 的为准，且只算一次：普通 join 会按版本把那一笔数两遍，租户会比实际更早
    // 被拦。
    task_rev(&pool, &twin_home, &second, 2, None).await;

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
    clear_tasks(&pool, &run.to_string()).await;
}

/// 被上限拒掉的一发算在被那条上限约束的主体头上，别的拒绝不算给任何人。
///
/// 这一份读喂的是排行里的「今天被拒」一列，也是「这个人今天每一发都被拒、花了零」
/// 在读数面上唯一能被看见的地方：花销读不出来他，只有拒绝数能。所以它只数
/// `refused_by_owner_budget`——那是唯一一种「是这个人的上限拒的」；任务预算拒的
/// 那一种算在任务头上，算到人身上就会把「这件活预算超了」读成「这个人超了」。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_refusal_is_counted_against_the_subject_the_ceiling_that_refused_belongs_to() {
    let url = database_url();
    let (pool, store) = prepare(&url).await;

    let run = uuid::Uuid::new_v4();
    let day = Utc.with_ymd_and_hms(2005, 5, 6, 0, 0, 0).unwrap();
    let since = day.timestamp();
    let tenant_a = format!("refusal-ws-a-{run}");
    let tenant_b = format!("refusal-ws-b-{run}");
    let task_a = format!("refusal-task-a-{run}");
    let task_b = format!("refusal-task-b-{run}");
    let person_a = format!("refusal-person-a-{run}");
    let person_b = format!("refusal-person-b-{run}");
    let person_c = format!("refusal-person-c-{run}");
    let person_d = format!("refusal-person-d-{run}");

    for (workspace, task_id, submitted_by) in [
        (&tenant_a, &task_a, &person_a),
        (&tenant_b, &task_b, &person_b),
    ] {
        task_rev(&pool, workspace, task_id, 1, Some(submitted_by)).await;
    }

    // 这个人自己那一格的上限拒的，且没有任务身份：只算人，不进任何租户。
    refused_row(
        &pool,
        day + chrono::Duration::hours(1),
        cog_core::UsageResult::RefusedByOwnerBudget,
        Some("user"),
        Some(&person_a),
        None,
    )
    .await;
    // 没有属主、靠任务连到提交人，拒的是这个人自己那一格：租户那一列不该跟着
    // 记一笔——这正是不按级筛时会出的错。
    refused_row(
        &pool,
        day + chrono::Duration::hours(2),
        cog_core::UsageResult::RefusedByOwnerBudget,
        Some("user"),
        None,
        Some(&task_a),
    )
    .await;
    // 租户那一格的上限拒的。属主只是这一发的当事人，人那一列不该记他的账；
    // 租户那一列跟着任务走，落到 tenant_b。
    refused_row(
        &pool,
        day + chrono::Duration::hours(3),
        cog_core::UsageResult::RefusedByOwnerBudget,
        Some("ws"),
        Some(&person_c),
        Some(&task_b),
    )
    .await;
    // 租户上限拒的、没有属主：只算它跑在的那个租户。
    refused_row(
        &pool,
        day + chrono::Duration::hours(4),
        cog_core::UsageResult::RefusedByOwnerBudget,
        Some("ws"),
        None,
        Some(&task_a),
    )
    .await;
    // 花掉钱的一发不是拒绝，不许进这一列。
    ledger_row(
        &pool,
        day + chrono::Duration::hours(5),
        Some(&person_a),
        None,
        90,
    )
    .await;
    // 任务预算拒的：那是这件活的预算，不是谁的当日额度，谁也不该被记一笔。
    refused_row(
        &pool,
        day + chrono::Duration::hours(6),
        cog_core::UsageResult::RefusedByBudget,
        None,
        Some(&person_d),
        Some(&task_b),
    )
    .await;
    // 这一列存在之前写下的额度拒绝：没有级别可读。两份数都不收它——发布那一
    // 天宁可少算，也不能把同一笔拒绝同时记进人和租户两份账。
    refused_row(
        &pool,
        day + chrono::Duration::hours(7),
        cog_core::UsageResult::RefusedByOwnerBudget,
        None,
        Some(&person_d),
        None,
    )
    .await;
    // 前一天被拒的一发：这个读的窗口与花销同一个，从当日零点起算。
    refused_row(
        &pool,
        day - chrono::Duration::hours(1),
        cog_core::UsageResult::RefusedByOwnerBudget,
        Some("user"),
        Some(&person_d),
        None,
    )
    .await;

    let people = store
        .refusals_by_person_since(since)
        .await
        .expect("read the refusals by person");
    assert_eq!(
        people,
        vec![(person_a.clone(), 2)],
        "只数人这一级的拒绝：属主那一行 + 靠任务连到提交人的那一行；租户上限拒的\
         不记给人；任务预算拒的不算给谁；没有级别的那一行与昨天的那一行都不在"
    );

    let workspaces = store
        .refusals_by_workspace_since(since)
        .await
        .expect("read the refusals by workspace");
    assert_eq!(
        workspaces,
        vec![(tenant_a.clone(), 1), (tenant_b.clone(), 1)],
        "只数租户这一级的拒绝：人这一级拒掉的两发不进租户，没有任务身份的那一发\
         不进任何租户，缺级别的那一发两份都不进"
    );
    assert!(
        workspaces.iter().all(|(id, _)| !id.is_empty()),
        "空 id 不是工作区，不许成一个格子"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE ts >= $1 AND ts < $2")
        .bind(day - chrono::Duration::days(1))
        .bind(day + chrono::Duration::days(1))
        .execute(&pool)
        .await
        .expect("leave the ledger as it was found");
    clear_tasks(&pool, &run.to_string()).await;
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

    for (task_id, submitted_by) in [(&named, Some(person.as_str())), (&anonymous, None)] {
        task_rev(&pool, &format!("ws-{run}"), task_id, 1, submitted_by).await;
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

    clear_tasks(&pool, &run.to_string()).await;
}
