//! The decision counters' storage, against a live PostgreSQL.
//!
//! What these tests check is what the database decides, and the one that
//! matters cannot be checked without a server: two writers counting into the
//! same cell at the same time leave a sum, not one of the two increments. The
//! file this table replaces lost increments exactly there, and a test that only
//! wrote and read back one increment at a time would pass against the file.
//!
//! The table also enforces its own invariant -- successes never exceed attempts
//! -- so a row that breaks it cannot be written by any caller, including one
//! that bypasses the store. That is checked by writing such a row directly.
//!
//! Most of it is ignored by default and needs `COGNEVA_TEST_DATABASE_URL`
//! pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test decision_stats_store -- --ignored
//! ```
//!
//! The throwaway database starts empty, so the migrations are applied first.
//! Every key this file writes carries a per-run marker, and the rows are deleted
//! at the end: [`DecisionStatsStore::load_all`] reads the whole table, so a row
//! left behind by one run would be a row the next run's reading counts.
//!
//! The tests here run one at a time, under [`CELLS`], for the same reason.

use std::sync::Arc;

use sqlx::PgPool;

use cog_core::{DecisionStatCell, DecisionStatsStore};
use cog_storage::PostgresDecisionStatsStore;

/// Serializes the tests that read or write the table. See the module note.
static CELLS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use cog_test_support::database_url;

/// One test's cell factory, so every key it writes is its own.
struct Probe {
    store: PostgresDecisionStatsStore,
    pool: PgPool,
    marker: String,
}

impl Probe {
    fn category(&self) -> String {
        // The column is VARCHAR(32); the marker is a uuid, so this is 9 + 36.
        format!("{}-{}", "probe", &self.marker[self.marker.len() - 22..])
    }

    fn cell(&self, group: &str, decision: &str) -> DecisionStatCell {
        DecisionStatCell {
            category: self.category(),
            group_key: format!("{group}-{}", self.marker),
            decision: decision.to_string(),
            attempts: 1,
            successes: 0,
        }
    }

    fn won(&self, group: &str, decision: &str) -> DecisionStatCell {
        DecisionStatCell {
            successes: 1,
            ..self.cell(group, decision)
        }
    }

    async fn row(&self, group: &str, decision: &str) -> (i64, i64) {
        sqlx::query_as(
            "SELECT attempts, successes FROM decision_stats_groups \
             WHERE category = $1 AND group_key = $2 AND decision = $3",
        )
        .bind(self.category())
        .bind(format!("{group}-{}", self.marker))
        .bind(decision)
        .fetch_one(&self.pool)
        .await
        .expect("read the cell")
    }
}

async fn probe() -> (tokio::sync::MutexGuard<'static, ()>, Probe) {
    let guard = CELLS.lock().await;
    let url = database_url();
    migrate_once(&url).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the database");
    let probe = Probe {
        store: PostgresDecisionStatsStore::new(pool.clone()),
        pool,
        marker: uuid::Uuid::new_v4().to_string(),
    };
    cleanup(&probe).await;
    (guard, probe)
}

/// Apply the migrations under a session advisory lock, the same way the other
/// store tests do: the DDL is mutually exclusive, so a second caller waits for
/// the first and then finds every version already applied.
async fn migrate_once(url: &str) {
    let pool = PgPool::connect(url)
        .await
        .expect("connect to take the lock");
    let mut conn = pool.acquire().await.expect("acquire the schema lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(97_004_i64)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    cog_storage::migrate::Migrator::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .run(url)
        .await
        .expect("apply the migrations to the throwaway database");
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(97_004_i64)
        .execute(&mut *conn)
        .await
        .expect("release the schema lock");
}

async fn cleanup(probe: &Probe) {
    sqlx::query("DELETE FROM decision_stats_groups WHERE category = $1")
        .bind(probe.category())
        .execute(&probe.pool)
        .await
        .expect("delete the rows this test wrote");
}

/// The property the file lost: two writers counting into one cell leave the
/// sum. Forty increments across four tasks, because two is the smallest case
/// and the one most likely to pass by luck -- the increments have to overlap
/// for the read-then-write shape to lose one, and four tasks interleaving is
/// what makes them overlap.
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
#[tokio::test]
async fn concurrent_increments_add_up_instead_of_each_overwriting_the_other() {
    let (_guard, probe) = probe().await;
    let store: Arc<dyn DecisionStatsStore> = Arc::new(probe.store.clone());
    let tasks: Vec<_> = (0..4)
        .map(|task| {
            let store = store.clone();
            let cell = probe.won("same-group", "pipeline");
            // Whether this task reports *every* attempt as a success, not how
            // many of them: the comment below counts two tasks times ten
            // attempts, and counting the wins instead would have two tasks
            // reporting one success each while the assertion expected twenty.
            let wins_every_attempt = task % 2 == 0;
            tokio::spawn(async move {
                for _ in 0..10 {
                    let delta = DecisionStatCell {
                        successes: i64::from(wins_every_attempt),
                        ..cell.clone()
                    };
                    store.accumulate(&delta).await.expect("accumulate");
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("join the writer");
    }

    // Four tasks, ten attempts each; two of the four report every attempt a
    // success. Anything less than the full sum means an increment was lost.
    assert_eq!(
        probe.row("same-group", "pipeline").await,
        (40, 20),
        "every increment has to survive, including the ones that overlapped"
    );
    cleanup(&probe).await;
}

/// The decision is part of the cell's identity: the same group's two decisions
/// are two rows, and one group's counters do not move another's.
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
#[tokio::test]
async fn a_decision_is_part_of_the_cell_not_a_column_of_the_group() {
    let (_guard, probe) = probe().await;
    probe
        .store
        .accumulate(&probe.won("one-group", "pipeline"))
        .await
        .unwrap();
    probe
        .store
        .accumulate(&probe.cell("one-group", "roundtable"))
        .await
        .unwrap();
    probe
        .store
        .accumulate(&probe.cell("one-group", "roundtable"))
        .await
        .unwrap();
    probe
        .store
        .accumulate(&probe.won("other-group", "pipeline"))
        .await
        .unwrap();

    assert_eq!(probe.row("one-group", "pipeline").await, (1, 1));
    assert_eq!(probe.row("one-group", "roundtable").await, (2, 0));
    assert_eq!(probe.row("other-group", "pipeline").await, (1, 1));
    cleanup(&probe).await;
}

/// Reading everything back is what a process does at start, and it has to see
/// exactly the rows that were accumulated.
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
#[tokio::test]
async fn loading_returns_every_accumulated_cell() {
    let (_guard, probe) = probe().await;
    probe
        .store
        .accumulate(&probe.won("g1", "pipeline"))
        .await
        .unwrap();
    probe
        .store
        .accumulate(&probe.cell("g2", "pipeline"))
        .await
        .unwrap();
    probe
        .store
        .accumulate(&probe.cell("g2", "pipeline"))
        .await
        .unwrap();

    let cells = probe.store.load_all().await.expect("load every cell");
    let mine: Vec<_> = cells
        .into_iter()
        .filter(|cell| cell.category == probe.category())
        .collect();
    assert_eq!(mine.len(), 2, "one row per (group, decision): {mine:#?}");
    let g1 = mine
        .iter()
        .find(|c| c.group_key.starts_with("g1-"))
        .expect("g1");
    let g2 = mine
        .iter()
        .find(|c| c.group_key.starts_with("g2-"))
        .expect("g2");
    assert_eq!((g1.attempts, g1.successes), (1, 1));
    assert_eq!((g2.attempts, g2.successes), (2, 0));
    cleanup(&probe).await;
}

/// The invariant is the table's, not the store's: a caller that writes the row
/// itself is refused too. Without this the check would only hold for writers
/// that happen to go through `accumulate`.
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
#[tokio::test]
async fn the_table_refuses_successes_beyond_attempts_from_any_writer() {
    let (_guard, probe) = probe().await;
    let refused = sqlx::query(
        "INSERT INTO decision_stats_groups (category, group_key, decision, attempts, successes) \
         VALUES ($1, $2, 'impossible', 1, 2)",
    )
    .bind(probe.category())
    .bind(format!("bad-{}", probe.marker))
    .execute(&probe.pool)
    .await;
    assert!(
        refused.is_err(),
        "two successes out of one attempt is not an outcome, and the row has to say so"
    );
    cleanup(&probe).await;
}

/// A delta that records no decision, or records more successes than attempts,
/// is refused by the store before the statement runs. These need no server: the
/// pool points at a closed port and is never reached, and the error type says
/// so -- a refusal that came from the server would be `Database`, and folding
/// the two together would let "the database is down" read as "this delta is
/// wrong".
#[tokio::test]
async fn a_delta_that_is_not_an_outcome_never_reaches_the_database() {
    let pool = PgPool::connect_lazy("postgres://127.0.0.1:1/nothing").expect("a lazy pool");
    let store = PostgresDecisionStatsStore::new(pool);
    let good = DecisionStatCell {
        category: "pge_mode".into(),
        group_key: "bugfix".into(),
        decision: "pipeline".into(),
        attempts: 1,
        successes: 1,
    };

    for (label, delta) in [
        (
            "no attempts",
            DecisionStatCell {
                attempts: 0,
                ..good.clone()
            },
        ),
        (
            "more successes than attempts",
            DecisionStatCell {
                successes: 2,
                ..good.clone()
            },
        ),
        (
            "negative successes",
            DecisionStatCell {
                successes: -1,
                ..good.clone()
            },
        ),
        (
            "empty group",
            DecisionStatCell {
                group_key: String::new(),
                ..good.clone()
            },
        ),
        (
            "an over-long decision",
            DecisionStatCell {
                decision: "x".repeat(192),
                ..good.clone()
            },
        ),
    ] {
        let err = store
            .accumulate(&delta)
            .await
            .expect_err(&format!("{label} has to be refused before the write"));
        assert!(
            matches!(err, cog_core::SFError::Validation(_)),
            "{label} was refused at the wrong layer: {err:?}"
        );
    }
}
