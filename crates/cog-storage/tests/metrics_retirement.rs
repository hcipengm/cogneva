//! Releasing a retired metric name from every table that stores it, against a
//! live PostgreSQL.
//!
//! What these tests check is the part the database decides: that a retired name
//! leaves all four stores rather than only the log, that a name not on the list
//! is untouched wherever it lives, that a retired series' newest log row goes
//! too — the floor is gone for the kinds whose value is not read through the log
//! — and that a history larger than one statement is drained completely. The
//! tables are created and dropped by the tests themselves, so the database is
//! left as it was found.
//!
//! None of that can be checked without a server, so the tests are ignored by
//! default and need `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway
//! database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test metrics_retirement -- --ignored
//! ```

use sqlx::PgPool;

use cog_storage::MetricsRetirement;

const RETIRED: &str = "retired_probe_total";
const LIVE: &str = "live_probe_total";

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// The four probe tables, named after the suffix of the store each stands in
/// for, so one test run gets its own set and two tests never share one.
struct Probe {
    samples: String,
    counter_totals: String,
    histogram_buckets: String,
    histogram_sums: String,
}

impl Probe {
    fn new(suffix: &str) -> Self {
        Self {
            samples: format!("retire_probe_samples_{suffix}"),
            counter_totals: format!("retire_probe_counters_{suffix}"),
            histogram_buckets: format!("retire_probe_buckets_{suffix}"),
            histogram_sums: format!("retire_probe_sums_{suffix}"),
        }
    }

    async fn create(&self, pool: &PgPool) {
        self.drop(pool).await;
        // The columns the release reads or writes, and no more. In particular
        // the sample log keeps its serial `id`, because the batched delete is
        // written against it.
        for sql in [
            format!(
                "CREATE TABLE {} (
                     id SERIAL PRIMARY KEY,
                     metric_type TEXT NOT NULL,
                     name TEXT NOT NULL,
                     value DOUBLE PRECISION NOT NULL,
                     labels JSONB NOT NULL DEFAULT '{{}}',
                     timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
                 )",
                self.samples
            ),
            format!(
                "CREATE TABLE {} (
                     name TEXT NOT NULL,
                     labels JSONB NOT NULL DEFAULT '{{}}',
                     value DOUBLE PRECISION NOT NULL,
                     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                     PRIMARY KEY (name, labels)
                 )",
                self.counter_totals
            ),
            format!(
                "CREATE TABLE {} (
                     name TEXT NOT NULL,
                     labels JSONB NOT NULL DEFAULT '{{}}',
                     bucket INTEGER NOT NULL,
                     observations BIGINT NOT NULL,
                     PRIMARY KEY (name, labels, bucket)
                 )",
                self.histogram_buckets
            ),
            format!(
                "CREATE TABLE {} (
                     name TEXT NOT NULL,
                     labels JSONB NOT NULL DEFAULT '{{}}',
                     sum DOUBLE PRECISION NOT NULL,
                     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                     PRIMARY KEY (name, labels)
                 )",
                self.histogram_sums
            ),
        ] {
            sqlx::query(&sql).execute(pool).await.unwrap();
        }
    }

    async fn drop(&self, pool: &PgPool) {
        for table in [
            &self.samples,
            &self.counter_totals,
            &self.histogram_buckets,
            &self.histogram_sums,
        ] {
            sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
                .execute(pool)
                .await
                .unwrap();
        }
    }

    /// One row per store, under `name`, in the shape a rename leaves behind: a
    /// counter's total, a histogram's buckets and sum, and a single old log row.
    async fn insert(&self, pool: &PgPool, name: &str, log_rows: i64) {
        sqlx::query(&format!(
            "INSERT INTO {} (metric_type, name, value, labels, timestamp)
             SELECT 'counter', $1, 1.0, '{{}}', NOW() - INTERVAL '2 days'
             FROM generate_series(1, $2)",
            self.samples
        ))
        .bind(name)
        .bind(log_rows)
        .execute(pool)
        .await
        .unwrap();

        for (table, column, value) in [
            (&self.counter_totals, "value", "42"),
            (&self.histogram_sums, "sum", "0.5"),
        ] {
            sqlx::query(&format!(
                "INSERT INTO {table} (name, labels, {column}) VALUES ($1, '{{}}', {value})"
            ))
            .bind(name)
            .execute(pool)
            .await
            .unwrap();
        }

        sqlx::query(&format!(
            "INSERT INTO {} (name, labels, bucket, observations) VALUES ($1, '{{}}', 0, 3)",
            self.histogram_buckets
        ))
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    }

    fn retirement(&self, pool: PgPool) -> MetricsRetirement {
        MetricsRetirement::new(pool)
            .with_tables(
                self.samples.clone(),
                self.counter_totals.clone(),
                self.histogram_buckets.clone(),
                self.histogram_sums.clone(),
            )
            .with_retired_names(vec![RETIRED])
    }

    async fn count_named(&self, pool: &PgPool, table: &str, name: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE name = $1"))
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
    }
}

/// Every store, not just the log. A counter's and a histogram's value lives in
/// an accumulation table with no rows to rank and no age to reach, so leaving
/// those alone is not holding a series — it is never deleting it, and its frozen
/// total reads downstream as "no traffic" rather than as "this metric is gone".
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_retired_name_leaves_every_table_it_is_stored_in() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let probe = Probe::new("every");
    probe.create(&pool).await;
    probe.insert(&pool, RETIRED, 3).await;
    probe.insert(&pool, LIVE, 3).await;

    let outcome = probe.retirement(pool.clone()).release().await.unwrap();

    // The log gets one row per observation, each accumulation table one row per
    // label set, so the live name's remainder differs by store.
    for (table, live_rows) in [
        (&probe.samples, 3),
        (&probe.counter_totals, 1),
        (&probe.histogram_buckets, 1),
        (&probe.histogram_sums, 1),
    ] {
        assert_eq!(
            probe.count_named(&pool, table, RETIRED).await,
            0,
            "{table} still holds the retired name"
        );
        assert_eq!(
            probe.count_named(&pool, table, LIVE).await,
            live_rows,
            "{table} lost a name the release was not told about"
        );
    }
    assert_eq!(outcome.samples, 3);
    assert_eq!(outcome.counter_totals, 1);
    assert_eq!(outcome.histogram_buckets, 1);
    assert_eq!(outcome.histogram_sums, 1);
    assert_eq!(outcome.total(), 6);
    probe.drop(&pool).await;
}

/// The name list is the whole judgement, and a deployment that has not retired
/// anything has an empty one. Releasing nothing has to be a no-op rather than a
/// delete of everything: the pass has no age or rank criterion to fall back on,
/// so an empty list is the one input that could mean "all of it".
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_empty_retired_set_releases_nothing() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let probe = Probe::new("empty");
    probe.create(&pool).await;
    probe.insert(&pool, RETIRED, 3).await;

    let outcome = probe
        .retirement(pool.clone())
        .with_retired_names(vec![])
        .release()
        .await
        .unwrap();

    assert_eq!(outcome.total(), 0);
    assert_eq!(
        probe.count_named(&pool, &probe.samples, RETIRED).await,
        3,
        "an empty list must not be read as every name"
    );
    probe.drop(&pool).await;
}

/// A retired series' newest log row is the newest it will ever have, and it goes
/// like the rest. Holding it is what turns a rename into a series `/metrics`
/// keeps serving with a value from before the rename.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_retired_series_newest_log_row_is_not_exempt() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let probe = Probe::new("head");
    probe.create(&pool).await;
    probe.insert(&pool, RETIRED, 1).await;

    probe.retirement(pool.clone()).release().await.unwrap();

    assert_eq!(
        probe.count_named(&pool, &probe.samples, RETIRED).await,
        0,
        "the only, newest row of the retired series must be gone"
    );
    probe.drop(&pool).await;
}

/// A retired name's history can be most of the log. Drained in batches, a
/// history larger than one batch still has to end up empty rather than losing
/// exactly the first batch.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_history_larger_than_one_batch_is_drained_completely() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let probe = Probe::new("batched");
    probe.create(&pool).await;
    let rows = 5_001;
    probe.insert(&pool, RETIRED, rows).await;

    let outcome = probe.retirement(pool.clone()).release().await.unwrap();

    assert_eq!(outcome.samples, rows as u64);
    assert_eq!(probe.count_named(&pool, &probe.samples, RETIRED).await, 0);
    probe.drop(&pool).await;
}

/// The pass runs on a cadence, so most of its runs find nothing. A second pass
/// over the same names has to report zero and delete nothing — that is what
/// lets two deployments share the list without either having to know who runs
/// first.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_second_pass_finds_nothing_to_release() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let probe = Probe::new("idempotent");
    probe.create(&pool).await;
    probe.insert(&pool, RETIRED, 3).await;
    probe.insert(&pool, LIVE, 2).await;

    let retirement = probe.retirement(pool.clone());
    let first = retirement.release().await.unwrap();
    let second = retirement.release().await.unwrap();

    assert_eq!(first.total(), 6);
    assert_eq!(second.total(), 0);
    assert_eq!(
        probe.count_named(&pool, &probe.samples, LIVE).await,
        2,
        "the second pass must not reach past the retired names"
    );
    probe.drop(&pool).await;
}
