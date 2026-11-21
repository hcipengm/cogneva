//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//!
//! 正文按块写进 PG 的大对象，行里只留指针。三条不变式，都是"不攒"的形状：
//!
//! - **开、写、封**各自是一次 SQL：`lo_create` 拿到 oid，`lo_write` 每次只写收到的
//!   那一段，`lo_close` 封口。中间不攒整段字节——攒就等于把"无界"换成了"内存里的界"。
//! - **封口才算完**：哈希与长度在封口时由库里的字节算出（`lo_get` 分块回读），所以
//!   "行里有指针"不等于"正文完整"——行只在封口之后写，指针、哈希、长度三列一起出现。
//! - **崩溃留下已到达的字节**：进程死在中途时，库里是已经写进去的那一段，不是零；
//!   行还没写，于是留成**孤儿大对象**，由「扫 `pg_largeobject_metadata` 找没有行引用的
//!   oid」这条读数认出来。不靠一列「未封口时间戳」：行只在封口后写，那一列对每一行都
//!   非空，一条永远为空的条件等于没有读数。
//!
//! 这里不做任何按大小截断：本模块没有"最多写多少"的常量——上限只能来自"读到多
//! 少写多少"。

use sqlx::{PgPool, Row};

use cog_core::{SFError, SFResult};

/// 一段正文的大对象。`sealed` 为 `None` 表示还没封口（写到一半）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeBody {
    pub oid: u32,
    pub sealed: Option<(String, u64)>,
}

/// 回读校验时一次取多少字节。
///
/// 这是**一次数据库往返的载荷**，不是正文的界：正文写多少由上游说了算，与这个数无关。
/// 取这么大是因为 `lo_get` 一条语句返回一个 `bytea`，一次取回整段会让常驻内存跟着
/// 正文大小走——而「无界」指的是正文无界，不是进程内存无界。
// cap: fixed — 一次 lo_get 往返的载荷（单值 bytea），与正文大小无关
const VERIFY_CHUNK: i64 = 1_048_576;

/// 开一段正文，拿到 oid。
pub async fn open(pool: &PgPool) -> SFResult<u32> {
    // `lo_create` 返回 SQL 类型 `oid`，而 `try_get::<i64>` 读不了它：类型对不上，
    // 每一段正文都会在**第一条语句**上失败。在 SQL 里转一次，读侧就只剩一种整数类型。
    let row = sqlx::query("SELECT lo_create(0)::bigint AS oid")
        .fetch_one(pool)
        .await
        .map_err(|e| SFError::Database(format!("lo_create: {e}")))?;
    row.try_get::<i64, _>("oid")
        .map(|v| v as u32)
        .map_err(|e| SFError::Database(format!("lo_create returned nothing: {e}")))
}

/// 写一块，落在 `offset` 上。块就是这一拍收到的字节，不合并、不切分。
///
/// 偏移由写者自己数，不从库里查：`lo_lseek64` 要的是一个**文件描述符**（`lo_open`
/// 的返回值），而 `lo_put` 要的是 `oid`——把两者接在一起，Postgres 的回答是
/// 「没有这个函数」。原先那一行就是这么写的，于是每一块都写不进去。写者本来就知道
/// 自己写了多少字节，数它比回读整段便宜，也不会在并发下拿到别人的长度。
pub async fn write_block(pool: &PgPool, oid: u32, offset: u64, bytes: &[u8]) -> SFResult<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    sqlx::query("SELECT lo_put($1::oid, $2::bigint, $3::bytea)")
        .bind(oid as i64)
        .bind(offset as i64)
        .bind(bytes)
        .execute(pool)
        .await
        .map_err(|e| SFError::Database(format!("lo_put: {e}")))?;
    Ok(())
}

/// 丢掉一段没写成的正文。
///
/// 只给「写入口自己知道这一份不完整、字节另外还有一份」的那条路用。崩溃不走这里：
/// 进程死掉时没人调它，已到达的字节留在库里，孤儿扫描因此把「崩溃」与「被处理掉的
/// 写失败」分开。
pub async fn unlink(pool: &PgPool, oid: u32) -> SFResult<()> {
    // 只在它**确实还在**时删：`lo_unlink` 对不存在的 oid 不是返回 0，是**报错**
    // （`large object N does not exist`）。擦除要能重放——销毁成功、追加事件失败之后
    // 再跑一次是正常路径——所以这里先把「已经不在」当成成功：那正是要的终态。
    sqlx::query("SELECT lo_unlink($1::oid) FROM pg_largeobject_metadata WHERE oid = $1")
        .bind(oid as i64)
        .execute(pool)
        .await
        .map_err(|e| SFError::Database(format!("lo_unlink: {e}")))?;
    Ok(())
}

/// 封口：把整段读回来算 sha256 与长度，落 `sealed`。
///
/// 分块回读，不一次 `lo_get` 全量：一次取回等于把"无界"搬回了内存。
pub async fn seal(pool: &PgPool, oid: u32) -> SFResult<(String, u64)> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    loop {
        let row =
            sqlx::query("SELECT substring(lo_get($1::oid, $2::bigint, $3::int) from 1) AS chunk")
                .bind(oid as i64)
                .bind(total as i64)
                .bind(VERIFY_CHUNK)
                .fetch_one(pool)
                .await
                .map_err(|e| SFError::Database(format!("lo_get: {e}")))?;
        let chunk: Option<Vec<u8>> = row.try_get("chunk").unwrap_or(None);
        let Some(chunk) = chunk else { break };
        if chunk.is_empty() {
            break;
        }
        total += chunk.len() as u64;
        hasher.update(&chunk);
        if (chunk.len() as i64) < VERIFY_CHUNK {
            break;
        }
    }
    Ok((format!("{:x}", hasher.finalize()), total))
}
