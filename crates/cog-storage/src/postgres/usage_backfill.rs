//! principle: feedback_a_switch_that_starts_empty_loses_the_past
//!
//! 把 `gateway_llm_usage` 已有的行回填成事件，读切换前必须先做完这一步。
//!
//! 不回填就切读，等于把切换之前的花销从读数里抹掉：行里有整整一个月的事实，流里只有
//! 切换那一刻之后的事件，而「这周花了多少」不区分这两段——它只会给出一个小得多的数。
//!
//! 幂等：事件 id 与幂等键都由**行 id** 派生，重跑写的是同一条。因此回填可以分批、
//! 可以中断、可以从头再来，都不需要记住上次跑到哪儿。
//!
//! 回填是**只读行、只写流**：它不改行、不删行——在两条读数相等的证据出现之前，行仍然
//! 是这一族的正本。

use sqlx::{PgPool, Row};

use cog_core::{check as check_envelope, EnvelopeFields, SFError, SFResult};

/// 一批回填的行数。批越小，单条语句持有的锁越短；回填是一次性作业，批数由行数决定。
const BATCH: i64 = 500;

/// 从行里读一页（按时间正序、按 id 断开），写成事件。返回写了多少条。
///
/// `after` 是本批最后一行的 `(ts, id)`；调用方拿着它翻下一页。第一次传 `None`。
pub async fn backfill_page(
    pool: &PgPool,
    writer: &str,
    after: Option<(chrono::DateTime<chrono::Utc>, uuid::Uuid)>,
    limit: i64,
) -> SFResult<(usize, Option<(chrono::DateTime<chrono::Utc>, uuid::Uuid)>)> {
    let rows = sqlx::query(
        "SELECT id, ts, actor, upstream, api_style, model, result, origin,
                tokens_input, tokens_output, tokens_cached, task_id, owner_id
           FROM gateway_llm_usage
          WHERE ($1::timestamptz IS NULL OR (ts, id) > ($1, $2))
          ORDER BY ts, id
          LIMIT $3",
    )
    .bind(after.map(|(ts, _)| ts))
    .bind(after.map(|(_, id)| id))
    .bind(limit.min(BATCH))
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("backfill read: {e}")))?;

    let mut written = 0usize;
    for row in &rows {
        let id: uuid::Uuid = row
            .try_get("id")
            .map_err(|e| SFError::Database(e.to_string()))?;
        let at: chrono::DateTime<chrono::Utc> = row
            .try_get("ts")
            .map_err(|e| SFError::Database(e.to_string()))?;
        let call_id = format!("row:{id}");
        let attributes = serde_json::json!({
            "upstream": row.try_get::<String, _>("upstream").unwrap_or_default(),
            "api_style": row.try_get::<String, _>("api_style").unwrap_or_default(),
            "model": row.try_get::<String, _>("model").unwrap_or_default(),
            "result": row.try_get::<String, _>("result").unwrap_or_default(),
            "origin": row.try_get::<String, _>("origin").unwrap_or_default(),
            "input_tokens": row.try_get::<i64, _>("tokens_input").unwrap_or(0),
            "output_tokens": row.try_get::<i64, _>("tokens_output").unwrap_or(0),
            "cached_tokens": row.try_get::<i64, _>("tokens_cached").unwrap_or(0),
            "task_id": row.try_get::<Option<String>, _>("task_id").ok().flatten(),
            "owner": row.try_get::<Option<String>, _>("owner_id").ok().flatten(),
            "backfilled": true,
        })
        .to_string();
        let actor: String = row.try_get("actor").unwrap_or_else(|_| "unknown".into());
        check_envelope(&EnvelopeFields {
            event_id: event_id_for(&call_id),
            kind: "llm_call".into(),
            subject_kind: "llm_call".into(),
            subject_id: call_id.clone(),
            unit_of_work: call_id.clone(),
            step: "call".into(),
            outcome: "ok".into(),
            idempotency_key: call_id.clone(),
            actor,
            writer: writer.to_string(),
            scope: String::new(),
            attributes: attributes.clone(),
        })
        .map_err(|defect| {
            SFError::Database(format!("backfill refused row {id}: {}", defect.as_str()))
        })?;

        sqlx::query(
            "INSERT INTO cogneva_event (
                 event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt,
                 idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version,
                 attributes, at)
             VALUES ($1, 'llm_call', 'llm_call', $2, $2, 'call', 'ok', 1, $2, NULL, NULL, $3,
                     $4, '', 1, $5, $6)
             ON CONFLICT (writer, idempotency_key) DO NOTHING",
        )
        .bind(event_id_for(&call_id))
        .bind(&call_id)
        .bind(
            row.try_get::<String, _>("actor")
                .unwrap_or_else(|_| "unknown".into()),
        )
        .bind(writer)
        .bind(&attributes)
        .bind(at)
        .execute(pool)
        .await
        .map_err(|e| SFError::Database(format!("backfill write: {e}")))?;
        written += 1;
    }

    let last = rows.last().map(|row| {
        (
            row.try_get::<chrono::DateTime<chrono::Utc>, _>("ts")
                .unwrap_or_else(|_| chrono::Utc::now()),
            row.try_get::<uuid::Uuid, _>("id")
                .unwrap_or_else(|_| uuid::Uuid::nil()),
        )
    });
    Ok((written, last))
}

fn event_id_for(call_id: &str) -> String {
    let mut out = String::with_capacity(26);
    for ch in call_id.chars().filter(|c| c.is_ascii_alphanumeric()) {
        if out.len() == 26 {
            break;
        }
        out.push(ch.to_ascii_uppercase());
    }
    while out.len() < 26 {
        out.push('0');
    }
    out
}

/// 一直翻到没有新行为止。返回一共写了多少条。
///
/// 这是回填的入口：它自己记住游标（就是上一页的最后一行），调用方不需要知道页的大小
/// 与翻页规则。中断时什么都没有丢——下一次从头跑，写过的行按行 id 幂等跳过。
pub async fn backfill_all(pool: &PgPool, writer: &str) -> SFResult<usize> {
    let mut cursor = None;
    let mut total = 0usize;
    loop {
        let (written, last) = backfill_page(pool, writer, cursor, BATCH).await?;
        total += written;
        match last {
            Some(position) if written > 0 => cursor = Some(position),
            _ => break,
        }
    }
    Ok(total)
}
