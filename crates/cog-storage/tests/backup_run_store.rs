//! The backup account's storage, against a live PostgreSQL.
//!
//! What these tests check is what the database decides, and none of it can be
//! checked without a server: that the closed set of outcomes is enforced by the
//! column rather than by the caller being careful, that a run's opening row is
//! written once and never moved, that a closing is refused on a row that already
//! holds a conclusion, that a reading run is refused as a conclusion, that a run
//! cannot conclude itself `abandoned`, that the pass over the overdue rows reads
//! each row's own deadline rather than one bound for all of them, and that
//! the newest package's columns are read as one row's answers -- the retention
//! count and the bound it was run under have to come from the same run, and a
//! query that took them from different rows would compare two runs' numbers
//! while reading as a statement about one.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test backup_run_store -- --ignored
//! ```
//!
//! The throwaway database starts empty, so the migrations are applied first: the
//! table's layout is owned by the migrations, and a column added in one place and
//! not the other fails only at runtime. Every row this file writes carries a
//! per-run marker in its identity and is deleted at the end, so the database is
//! left as it was found.
//!
//! The tests in this file run one at a time, under [`ACCOUNT`]. [`read_account`]
//! is an aggregate over the whole table, and a whole table is a shared object: a
//! count read by one test is moved by a row another test opened, so two tests
//! asking it in parallel would each be asserting on the other's write. The lock
//! is what makes "the newest package is mine" a statement about this test rather
//! than about whichever of them opened a run last.
//!
//! [`read_account`]: cog_core::BackupRunStore::read_account

mod common;

use chrono::{Duration, Utc};
use sqlx::PgPool;

use cog_core::{BackupOutcome, BackupRunClosing, BackupRunOpening, BackupRunStore, SFError};
use cog_storage::PostgresBackupRunStore;

/// Serializes the tests in this file. See the module note.
static ACCOUNT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The deadline most of these openings are given, in seconds. One hour, which is
/// what a real run is opened under; the tests that are about the deadline use
/// their own and say so.
const DEADLINE_SECONDS: i32 = 3600;

use cog_test_support::database_url;

struct Probe {
    store: PostgresBackupRunStore,
    pool: PgPool,
    marker: String,
}

impl Probe {
    /// An identity only this test can produce, and only this test will delete.
    fn run_id(&self, label: &str) -> String {
        format!("{}-{label}", self.marker)
    }

    /// An opening under the deadline these tests otherwise use. Kept as a
    /// separate call from [`Probe::open_with_deadline`] so that the tests about
    /// what a deadline does have to name the number they are testing rather than
    /// inheriting it.
    async fn open(
        &self,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
        keep_target: i32,
    ) -> bool {
        self.open_with_deadline(run_id, started_at, keep_target, DEADLINE_SECONDS)
            .await
    }

    async fn open_with_deadline(
        &self,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
        keep_target: i32,
        run_deadline_seconds: i32,
    ) -> bool {
        self.store
            .open_run(&BackupRunOpening {
                run_id: run_id.to_string(),
                started_at,
                keep_target,
                run_deadline_seconds,
            })
            .await
            .expect("open the run")
    }

    async fn abandon_overdue(&self, now: chrono::DateTime<Utc>) -> u64 {
        self.store
            .abandon_overdue(now)
            .await
            .expect("conclude the overdue runs")
    }

    async fn outcome(&self, run_id: &str) -> String {
        sqlx::query_scalar("SELECT outcome FROM backup_runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(&self.pool)
            .await
            .expect("read the outcome")
    }

    async fn finished_at(&self, run_id: &str) -> Option<chrono::DateTime<Utc>> {
        sqlx::query_scalar("SELECT finished_at FROM backup_runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(&self.pool)
            .await
            .expect("read the finish instant")
    }

    async fn close(&self, run_id: &str, closing: BackupRunClosing) -> bool {
        self.store
            .close_run(run_id, &closing)
            .await
            .expect("close the run")
    }

    async fn started_at(&self, run_id: &str) -> chrono::DateTime<Utc> {
        sqlx::query_scalar("SELECT started_at FROM backup_runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(&self.pool)
            .await
            .expect("read the start instant")
    }
}

/// A closing that concluded `ok`, with everything a successful run leaves.
///
/// The keep bound is not among them: it is what the run was opened under, and a
/// closing that carried one would be a second place the same number is written.
fn ok_closing(
    package: &str,
    bytes: i64,
    kept_after: i32,
    oldest_kept_at: chrono::DateTime<Utc>,
) -> BackupRunClosing {
    BackupRunClosing {
        outcome: BackupOutcome::Ok,
        finished_at: Utc::now(),
        package: Some(package.to_string()),
        bytes: Some(bytes),
        // Derived from what this closing carries so two runs of the test never
        // share one: an assertion on this column that reads the wrong row would
        // otherwise pass.
        package_checksum: Some(format!("sha256:{package}:{bytes}")),
        // Likewise derived from the package name: a reading that took this
        // column from another row would return another name's list.
        package_coverage: Some(format!(r#"{{"package":"{package}"}}"#)),
        kept_after: Some(kept_after),
        oldest_kept_at: Some(oldest_kept_at),
        error: None,
    }
}

fn failed_closing(error: &str) -> BackupRunClosing {
    BackupRunClosing {
        outcome: BackupOutcome::Failed,
        finished_at: Utc::now(),
        package: None,
        bytes: None,
        package_checksum: None,
        package_coverage: None,
        kept_after: None,
        oldest_kept_at: None,
        error: Some(error.to_string()),
    }
}

/// A probe on a throwaway database, with the lock held and this test's rows
/// cleared.
async fn probe() -> (tokio::sync::MutexGuard<'static, ()>, Probe) {
    let guard = ACCOUNT.lock().await;
    let url = database_url();
    common::migrate_once(&url, 97_003).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the database");
    let store = PostgresBackupRunStore::new(pool.clone());
    let probe = Probe {
        store,
        pool,
        marker: format!("probe-backup-runs-{}", uuid::Uuid::new_v4()),
    };
    cleanup(&probe).await;
    (guard, probe)
}

/// Delete every row this test wrote. The table is append-only in production and
/// there is no delete in the contract on purpose; a test that left rows behind
/// would make the next run's whole-account reading wrong.
async fn cleanup(probe: &Probe) {
    sqlx::query("DELETE FROM backup_runs WHERE run_id LIKE $1")
        .bind(format!("{}-%", probe.marker))
        .execute(&probe.pool)
        .await
        .expect("delete the rows this test wrote");
}

/// An open run is visible as open, with the instant it opened at.
///
/// The opening row is the whole reason the table has a `running` state: it is
/// what a run that never comes back leaves behind, and a row that could not be
/// read back as open would make the one witness of a killed run unreadable.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_open_run_is_visible_as_open_with_its_start_instant() {
    let (_guard, probe) = probe().await;
    let baseline = probe.store.read_account().await.expect("read the account");
    let run_id = probe.run_id("open");
    let started_at = Utc::now() - Duration::minutes(5);

    assert!(
        probe.open(&run_id, started_at, 7).await,
        "第一次开这一笔必须是新写的一行"
    );

    let account = probe.store.read_account().await.expect("read the account");
    assert_eq!(
        account.running,
        baseline.running + 1,
        "开到一半的运行数没有多出一条：{account:?}"
    );
    let oldest = account
        .oldest_running_at
        .expect("开着的那一笔必须有开始时刻");
    assert!(
        oldest <= started_at,
        "最老的开工时刻比刚开的那一笔还晚，说明它没读到这一行：{oldest} vs {started_at}"
    );
    assert!(
        (probe.started_at(&run_id).await - started_at)
            .num_seconds()
            .abs()
            <= 1,
        "开始时刻读回来变了"
    );
    assert!(
        account
            .newest_package
            .as_ref()
            .is_none_or(|pkg| pkg.run_id != run_id),
        "还没收尾的一次运行不该是新包"
    );

    cleanup(&probe).await;
}

/// Opening the same identity twice writes one row and leaves the first start
/// instant where it was.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn opening_the_same_run_twice_does_not_move_its_start_instant() {
    let (_guard, probe) = probe().await;
    let run_id = probe.run_id("twice");
    let first = Utc::now() - Duration::hours(1);

    assert!(probe.open(&run_id, first, 7).await, "第一次开是新写一行");
    assert!(
        !probe.open(&run_id, Utc::now(), 7).await,
        "第二次开同一个标识不能算新写"
    );

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM backup_runs WHERE run_id = $1")
        .bind(&run_id)
        .fetch_one(&probe.pool)
        .await
        .expect("count the rows");
    assert_eq!(rows, 1, "同一个标识只能有一行");
    assert!(
        (probe.started_at(&run_id).await - first)
            .num_seconds()
            .abs()
            <= 1,
        "开始时刻被第二次开工挪动了"
    );

    cleanup(&probe).await;
}

/// The newest package's columns come back as one run's answers, and the runs
/// that concluded stop being open.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_newest_package_carries_its_own_retention_pair() {
    let (_guard, probe) = probe().await;
    let baseline = probe.store.read_account().await.expect("read the account");
    let older = probe.run_id("older");
    let newer = probe.run_id("newer");
    for (run_id, minutes_ago) in [(&older, 60i64), (&newer, 1i64)] {
        probe
            .open(run_id, Utc::now() - Duration::minutes(minutes_ago), 9)
            .await;
    }
    // 旧那笔留 4 份、新那笔留 9 份：混着读会得到「9 份、界是 9」以外的组合，
    // 而 4 在任何一行上都不成立。
    assert!(
        probe
            .close(
                &older,
                ok_closing("older.tar.zst", 10, 4, Utc::now() - Duration::days(2))
            )
            .await
    );
    assert!(
        probe
            .close(
                &newer,
                ok_closing("newer.tar.zst", 20, 9, Utc::now() - Duration::days(9))
            )
            .await
    );

    let account = probe.store.read_account().await.expect("read the account");
    assert_eq!(
        account.ok,
        baseline.ok + 2,
        "两笔成功没有都进账：{account:?}"
    );
    assert_eq!(account.running, 0, "两笔都收尾了：{account:?}");
    let pkg = account.newest_package.expect("最新一笔成功了就有包");
    assert_eq!(pkg.run_id, newer, "取到的不是最新那一笔");
    assert_eq!(pkg.package.as_deref(), Some("newer.tar.zst"));
    assert_eq!(pkg.bytes, Some(20));
    // 摘要和上面几列一样是**这一行**的答案：旧那笔的字节数不是 20，读到它就会露。
    assert_eq!(
        pkg.package_checksum.as_deref(),
        Some("sha256:newer.tar.zst:20")
    );
    assert_eq!(
        pkg.package_coverage.as_deref(),
        Some(r#"{"package":"newer.tar.zst"}"#)
    );
    // 这一对必须来自同一行。4 是旧那笔留下的，混着读会让 old 的份数配上 new 的
    // 界，得到一个任何一行上都不成立的组合。
    assert_eq!(pkg.kept_after, Some(9), "留下的份数不是这一笔自己的答案");
    assert_eq!(pkg.keep_target, 9);
    assert!(
        pkg.oldest_kept_at
            .is_some_and(|at| at < Utc::now() - Duration::days(8)),
        "最老的包时刻不是这一笔自己报的：{:?}",
        pkg.oldest_kept_at
    );

    cleanup(&probe).await;
}

/// A recorded digest and coverage read back as they were written, and "never
/// recorded" reads back as absent.
///
/// The second half is the reason this is a test and not an assertion elsewhere:
/// a NULL here means the run that wrote the package recorded no digest and no
/// coverage, which is a fact a reader has to be able to tell from "recorded, and
/// equal". Folding it into an empty string -- the shortcut a NOT NULL column
/// would have forced -- would make every package written before these columns
/// existed look compared, and an empty coverage object would read as a package
/// that carried nothing.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_recorded_digest_reads_back_and_an_unrecorded_one_stays_absent() {
    let (_guard, probe) = probe().await;
    let with_digest = probe.run_id("digest");
    let without = probe.run_id("nodigest");
    probe
        .open(&with_digest, Utc::now() - Duration::minutes(2), 7)
        .await;
    assert!(
        probe
            .close(
                &with_digest,
                ok_closing("digest.tar.zst", 11, 7, Utc::now() - Duration::days(1))
            )
            .await
    );
    let account = probe.store.read_account().await.expect("read the account");
    let pkg = account
        .newest_package
        .expect("the run just closed left a package");
    assert_eq!(pkg.run_id, with_digest);
    assert_eq!(
        pkg.package_checksum.as_deref(),
        Some("sha256:digest.tar.zst:11"),
        "记下的摘要没有原样读回来"
    );
    assert_eq!(
        pkg.package_coverage.as_deref(),
        Some(r#"{"package":"digest.tar.zst"}"#),
        "记下的覆盖清单没有原样读回来"
    );

    // 紧跟着的一跑没记摘要：它是最新的包，而最新那一格必须是「没有」而不是空串。
    probe.open(&without, Utc::now(), 7).await;
    let mut closing = ok_closing("without.tar.zst", 12, 7, Utc::now() - Duration::hours(1));
    closing.package_checksum = None;
    closing.package_coverage = None;
    assert!(probe.close(&without, closing).await);
    let account = probe.store.read_account().await.expect("read the account");
    let pkg = account
        .newest_package
        .expect("the newer run left a package");
    assert_eq!(pkg.run_id, without);
    assert_eq!(
        pkg.package_checksum, None,
        "没记过摘要的一跑读回来必须是缺席，不能是空串或别的替身"
    );
    assert_eq!(
        pkg.package_coverage, None,
        "没记过覆盖清单的一跑读回来必须是缺席，不能是空对象"
    );

    cleanup(&probe).await;
}

/// A terminal row is never rewritten, and a reading run cannot be a conclusion.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_conclusion_stands_and_a_reading_run_is_refused() {
    let (_guard, probe) = probe().await;
    let run_id = probe.run_id("sticky");
    let started = Utc::now() - Duration::days(30);
    probe.open(&run_id, started, 7).await;
    assert!(
        probe
            .close(
                &run_id,
                ok_closing("sticky.tar.zst", 5, 3, started - Duration::days(7))
            )
            .await,
        "第一笔结论该写进去"
    );
    assert!(
        !probe.close(&run_id, failed_closing("第二次报告")).await,
        "已经定论的行不能被第二笔结论改写"
    );
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM backup_runs WHERE run_id = $1")
        .bind(&run_id)
        .fetch_one(&probe.pool)
        .await
        .expect("read the outcome");
    assert_eq!(outcome, "ok", "第一笔结论被后面的报告翻掉了");

    let still_running = probe
        .store
        .close_run(
            &run_id,
            &BackupRunClosing {
                outcome: BackupOutcome::Running,
                finished_at: Utc::now(),
                package: None,
                bytes: None,
                package_checksum: None,
                package_coverage: None,
                kept_after: None,
                oldest_kept_at: None,
                error: None,
            },
        )
        .await;
    assert!(
        matches!(still_running, Err(SFError::Validation(_))),
        "把运行中当结论必须被拒：{still_running:?}"
    );

    // 账上没有这一笔时收尾返回 false 而不是报错——「没这行」和「已经结过」都要
    // 能被调用方看见，而不是让一个对不上的标识把备份判失败。
    assert!(
        !probe
            .close(
                &probe.run_id("never-opened"),
                ok_closing("x.tar.zst", 1, 1, Utc::now())
            )
            .await,
        "没开过的标识收尾不该有东西被改"
    );

    cleanup(&probe).await;
}

/// A failed run closes without a package, and does not become the newest package.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_failed_run_closes_without_a_package() {
    let (_guard, probe) = probe().await;
    let baseline = probe.store.read_account().await.expect("read the account");
    let run_id = probe.run_id("failed");
    probe.open(&run_id, Utc::now(), 7).await;
    assert!(
        probe
            .close(&run_id, failed_closing("qdrant snapshot refused"))
            .await
    );

    let account = probe.store.read_account().await.expect("read the account");
    assert_eq!(account.failed, baseline.failed + 1, "{account:?}");
    assert_eq!(account.running, 0);
    assert!(
        account
            .newest_package
            .as_ref()
            .is_none_or(|pkg| pkg.run_id != run_id),
        "失败的一次不该被读成最新的包：{account:?}"
    );

    cleanup(&probe).await;
}

/// The column refuses an outcome outside the closed set.
///
/// This is the half of the set that does not depend on any caller being careful:
/// a writer that spelled a fourth state would otherwise put a row in the table
/// that no reader can name, and the reading would silently lose it.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_outcome_column_refuses_a_value_outside_the_set() {
    let (_guard, probe) = probe().await;
    let rejected = sqlx::query(
        "INSERT INTO backup_runs (run_id, started_at, outcome, keep_target, run_deadline_seconds) \
         VALUES ($1, now(), 'aborted', 7, 3600)",
    )
    .bind(probe.run_id("bad-outcome"))
    .execute(&probe.pool)
    .await;
    assert!(
        rejected.is_err(),
        "闭集之外的结局必须被列拒掉，而不是写进去让读者点不出名"
    );

    cleanup(&probe).await;
}

/// A run past the deadline it was opened under is concluded by the account, and
/// one still inside it is left alone.
///
/// The second half is the half that matters: this pass writes the one conclusion
/// no run asked for, and a version of it that concluded by a bound it read from
/// somewhere other than the row would close runs that are still working. Both
/// runs here are open at the same instant, so only the row's own deadline can
/// separate them -- which is exactly the property being checked.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_run_past_its_own_deadline_is_concluded_and_one_inside_it_is_not() {
    let (_guard, probe) = probe().await;
    let baseline = probe.store.read_account().await.expect("read the account");
    let now = Utc::now();
    let overdue = probe.run_id("overdue");
    let working = probe.run_id("working");
    // The two openings are minutes apart on purpose: the account names one oldest
    // open run, and two rows sharing a start instant would leave which one is
    // named to the planner -- so a test that read the pair off a tie would be
    // checking the query's tiebreak rather than the deadline.
    probe
        .open_with_deadline(&overdue, now - Duration::hours(2), 7, 3600)
        .await;
    probe
        .open_with_deadline(&working, now - Duration::minutes(30), 7, 86_400)
        .await;

    assert_eq!(
        probe.abandon_overdue(now).await,
        1,
        "两笔里只有过了自己那一份期限的那笔该被结掉"
    );

    assert_eq!(probe.outcome(&overdue).await, "abandoned");
    assert_eq!(
        probe.outcome(&working).await,
        "running",
        "还在自己期限里的那笔被结掉了，这一趟把还在跑的运行判成了死的"
    );
    assert!(
        probe.finished_at(&overdue).await.is_none(),
        "没人看见它结束，就没有结束时刻：给一个时刻等于把「没人看见」写成「这时结束的」"
    );

    let account = probe.store.read_account().await.expect("read the account");
    assert_eq!(
        account.abandoned,
        baseline.abandoned + 1,
        "被账本结掉的那笔没有单列一类：{account:?}"
    );
    assert_eq!(account.running, 1, "还该有一笔开着：{account:?}");
    assert_eq!(
        account.oldest_running_at.is_some(),
        account.oldest_running_deadline_seconds == Some(86_400),
        "剩下的那笔的开时刻与它自己的期限必须成对读出来：{account:?}"
    );
    assert!(
        account.latest_finish_at.is_none_or(|at| at <= now),
        "被结掉的那笔不该给出一个结束时刻：{account:?}"
    );

    // 同一趟跑第二遍什么都不该再结——这个判断不是一次性的，它得能被反复跑。
    assert_eq!(
        probe.abandon_overdue(now).await,
        0,
        "结过的行不能第二次被结"
    );

    cleanup(&probe).await;
}

/// No run closes itself as abandoned.
///
/// The state is the account's conclusion about a run that stopped reporting, and
/// a process holding the pen is by definition one that has not. Letting a run
/// write it would make the two authors of conclusions indistinguishable in the
/// one place a reader selects by state.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_run_cannot_close_itself_as_abandoned() {
    let (_guard, probe) = probe().await;
    let run_id = probe.run_id("self-abandon");
    probe.open(&run_id, Utc::now(), 7).await;

    let refused = probe
        .store
        .close_run(
            &run_id,
            &BackupRunClosing {
                outcome: BackupOutcome::Abandoned,
                finished_at: Utc::now(),
                package: None,
                bytes: None,
                package_checksum: None,
                package_coverage: None,
                kept_after: None,
                oldest_kept_at: None,
                error: Some("自己判自己没在跑".into()),
            },
        )
        .await;
    assert!(
        matches!(refused, Err(SFError::Validation(_))),
        "运行不能自己写 abandoned：{refused:?}"
    );
    assert_eq!(
        probe.outcome(&run_id).await,
        "running",
        "被拒的那一笔不该有任何变化"
    );

    cleanup(&probe).await;
}
