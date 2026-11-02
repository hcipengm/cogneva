//! PostgreSQL-backed storage for PGE stage products.
//!
//! A stage product is what a model said at one point in one run: the plan, the
//! diff, the verdict. Two properties decide the shape of this table.
//!
//! It is rewritten by the same key more than once. A pipeline that died between
//! the generator and the evaluator re-enters that generation and produces the
//! verdict it never wrote, so the row for that stage is inserted again; the
//! newest bytes are the ones that describe the run that actually continued. The
//! upsert replaces them rather than adding a second row, because two rows for
//! one stage of one generation would leave a reader choosing between them with
//! nothing to choose by.
//!
//! It is read whole per generation. Resuming needs the plan even when the
//! generator never ran, and auditing needs to see that a plan exists without a
//! generation beside it, so the read returns every stage of the generation and
//! the caller decides. The primary key is exactly that read's prefix, so there
//! is no second index.
//!
//! `stage_rank` is stored rather than sorted in the query: the order of the
//! stages belongs to the enum, and a `CASE` over stage names here would be a
//! second definition of it that a new stage would have to be added to twice.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{PgeStage, PgeStageProduct, PgeStageStore, SFError, SFResult};

/// This store's schema, and the one a restore replays.
pub const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS pge_stage_product (
    task_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    repair_iteration INTEGER NOT NULL,
    stage TEXT NOT NULL,
    stage_rank SMALLINT NOT NULL,
    request_sha256 TEXT NOT NULL,
    change_identity JSONB,
    product JSONB NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (task_id, generation, repair_iteration, stage)
)"#;

/// Write one stage down, replacing what that key held.
const RECORD_SQL: &str = "\
INSERT INTO pge_stage_product \
    (task_id, generation, repair_iteration, stage, stage_rank, request_sha256, change_identity, product) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
ON CONFLICT (task_id, generation, repair_iteration, stage) DO UPDATE \
    SET stage_rank = EXCLUDED.stage_rank, \
        request_sha256 = EXCLUDED.request_sha256, \
        change_identity = EXCLUDED.change_identity, \
        product = EXCLUDED.product, \
        produced_at = NOW()";

/// Everything one generation recorded, furthest stage last within each repair.
const GENERATION_SQL: &str = "\
SELECT task_id, generation, repair_iteration, stage, request_sha256, change_identity, product \
FROM pge_stage_product \
WHERE task_id = $1 AND generation = $2 \
ORDER BY repair_iteration, stage_rank";

/// PostgreSQL PGE stage-product store.
#[derive(Clone)]
pub struct PostgresPgeStageStore {
    pool: PgPool,
}

impl PostgresPgeStageStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create the table if it is not there.
    pub async fn init_schema(&self) -> SFResult<()> {
        crate::schema_ddl::apply(&self.pool, DDL).await
    }
}

fn decode(row: &sqlx::postgres::PgRow) -> SFResult<PgeStageProduct> {
    let stage_word: String = row
        .try_get("stage")
        .map_err(|e| SFError::Database(e.to_string()))?;
    let stage = PgeStage::parse(&stage_word).ok_or_else(|| {
        SFError::Database(format!(
            "pge stage product carries the stage `{stage_word}`, which this build cannot place"
        ))
    })?;
    let generation: i32 = row
        .try_get("generation")
        .map_err(|e| SFError::Database(e.to_string()))?;
    let repair_iteration: i32 = row
        .try_get("repair_iteration")
        .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(PgeStageProduct {
        task_id: row
            .try_get("task_id")
            .map_err(|e| SFError::Database(e.to_string()))?,
        generation: u32::try_from(generation).map_err(|_| {
            SFError::Database(format!(
                "pge stage product holds generation {generation}, out of range"
            ))
        })?,
        repair_iteration: u32::try_from(repair_iteration).map_err(|_| {
            SFError::Database(format!(
                "pge stage product holds repair iteration {repair_iteration}, out of range"
            ))
        })?,
        stage,
        request_sha256: row
            .try_get("request_sha256")
            .map_err(|e| SFError::Database(e.to_string()))?,
        change_identity: row
            .try_get("change_identity")
            .map_err(|e| SFError::Database(e.to_string()))?,
        product: row
            .try_get("product")
            .map_err(|e| SFError::Database(e.to_string()))?,
    })
}

#[async_trait]
impl PgeStageStore for PostgresPgeStageStore {
    async fn record(&self, product: &PgeStageProduct) -> SFResult<()> {
        let generation = i32::try_from(product.generation)
            .map_err(|_| SFError::Validation("pge stage: generation out of range".into()))?;
        let repair_iteration = i32::try_from(product.repair_iteration)
            .map_err(|_| SFError::Validation("pge stage: repair iteration out of range".into()))?;
        sqlx::query(RECORD_SQL)
            .bind(&product.task_id)
            .bind(generation)
            .bind(repair_iteration)
            .bind(product.stage.as_str())
            .bind(product.stage.rank())
            .bind(&product.request_sha256)
            .bind(product.change_identity.as_ref())
            .bind(&product.product)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    async fn generation(&self, task_id: &str, generation: u32) -> SFResult<Vec<PgeStageProduct>> {
        let generation = i32::try_from(generation)
            .map_err(|_| SFError::Validation("pge stage: generation out of range".into()))?;
        let rows = sqlx::query(GENERATION_SQL)
            .bind(task_id)
            .bind(generation)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        rows.iter().map(decode).collect()
    }
}
