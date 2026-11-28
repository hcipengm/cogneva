//! `cogneva keys list|issue|rotate|revoke`：签名钥匙的三步与一读。
//!
//! 这一层只搬参数：产私钥在 KMS/密钥库（页面或操作者取回公钥与指纹），事件里
//! 只出现公钥与指纹。

use std::sync::Arc;

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "keys")
        .skip(1)
        .collect();
    let action = args.first().ok_or(
        "usage: cogneva keys list|issue <id> <purpose> <pubkey> <fingerprint> <actor>|rotate <id> <prev> <pubkey> <fingerprint> [overlap] <actor>|revoke <id> <reason> <actor>",
    )?;
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let writer =
        std::env::var(cog_core::env::DEPLOYMENT_NAME_ENV).unwrap_or_else(|_| "cogneva".to_string());
    let store: Arc<dyn cog_core::EventStore> = Arc::new(
        cog_storage::postgres::event_log::EventLogStore::new(pool.clone(), writer),
    );
    let keys = cog_storage::KeyEventStore::new(pool, store);
    let now = chrono::Utc::now();
    match action.as_str() {
        "list" => {
            for row in cog_core::SecurityKeys::list(&keys).await? {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}",
                    row.key_id,
                    row.state,
                    row.purpose,
                    row.fingerprint,
                    row.overlap_until.unwrap_or_default(),
                    row.revoked_reason.unwrap_or_default()
                );
            }
        }
        "issue" => {
            let [key_id, purpose, public_key, fingerprint, actor] = args
                .get(1..6)
                .ok_or("usage: cogneva keys issue <id> <purpose> <pubkey> <fingerprint> <actor>")?
            else {
                return Err("keys issue takes five arguments".into());
            };
            cog_core::SecurityKeys::issue(
                &keys,
                key_id,
                purpose,
                public_key,
                fingerprint,
                actor,
                now,
            )
            .await?;
            println!("keys: issued {key_id}");
        }
        "rotate" => {
            let usage = "usage: cogneva keys rotate <id> <previous> <pubkey> <fingerprint> <actor> [overlap-until]";
            let rest = args.get(1..).ok_or(usage)?;
            if rest.len() < 5 {
                return Err(usage.into());
            }
            let (key_id, previous, public_key, fingerprint, actor) =
                (&rest[0], &rest[1], &rest[2], &rest[3], &rest[4]);
            let overlap = rest.get(5).cloned();
            cog_core::SecurityKeys::rotate(
                &keys,
                key_id,
                previous,
                public_key,
                fingerprint,
                overlap,
                actor,
                now,
            )
            .await?;
            println!("keys: rotated {key_id} from {previous}");
        }
        "revoke" => {
            let [key_id, reason, actor] = args
                .get(1..4)
                .ok_or("usage: cogneva keys revoke <id> <reason> <actor> [--force]")?
            else {
                return Err("keys revoke takes three arguments".into());
            };
            // 动手之前先判一次会不会永久失去什么：用**吊销之后**剩下的候选钥重验
            // 原则密封包；再也打不开就点名它并停下，除非显式 --force。
            let rows = cog_core::SecurityKeys::list(&keys).await?;
            let remaining_rows: Vec<cog_core::KeyRow> = rows
                .into_iter()
                .filter(|row| row.key_id != *key_id)
                .collect();
            let remaining = crate::principle_corpus::candidate_keys(&remaining_rows, now);
            if let Some(bundle) =
                crate::principle_corpus::bundle_lost_after_revocation(&remaining).await?
            {
                if !args.iter().any(|arg| arg == "--force") {
                    return Err(format!(
                        "revoking {key_id} would leave the principle bundle {bundle} unverifiable; \
                         nothing has been revoked. Pass --force if that is intended"
                    )
                    .into());
                }
                tracing::warn!(bundle = %bundle, "revocation accepted with --force: the bundle will stop verifying");
            }
            cog_core::SecurityKeys::revoke(&keys, key_id, reason, actor, now).await?;
            println!("keys: revoked {key_id}");
        }
        other => return Err(format!("unknown keys action {other}").into()),
    }
    Ok(())
}
