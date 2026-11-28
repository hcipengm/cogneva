//! 启动时装载原则密封包：验签、核哈希、解包，然后把索引留在进程里。
//!
//! 装载与取材分开：这里只把**索引**（每条原则的 slug、载体哈希、门路径、主题）
//! 读进内存，不花任何模型 token；正文按批取用是生成侧的事。未配置或装载失败时
//! 语料为空——生成侧拿不到未经验签的内容，这是失败关闭，不是静默降级。

use std::sync::{Arc, OnceLock};

use cog_core::PrincipleBundle;

static CORPUS: OnceLock<Arc<PrincipleBundle>> = OnceLock::new();

/// 装好的语料；`None` 是没配置或没装成。
pub fn corpus() -> Option<Arc<PrincipleBundle>> {
    CORPUS.get().cloned()
}

pub use cog_core::{paths_in, select_for_paths};

/// 此刻能验包的钥匙：已签发或已轮换，且没被吊销；轮换带重叠期的，过了重叠期就不再验。
pub fn candidate_keys(
    rows: &[cog_core::KeyRow],
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<String> {
    rows.iter()
        .filter(|row| row.state == "issued" || row.state == "rotated")
        .filter(|row| match row.overlap_until.as_deref() {
            Some(until) => chrono::DateTime::parse_from_rfc3339(until)
                .map(|time| time.with_timezone(&chrono::Utc) > now)
                .unwrap_or(true),
            None => true,
        })
        .map(|row| row.public_key_b64.clone())
        .filter(|key| !key.is_empty())
        .collect()
}

/// 验过这个包的是哪一把钥（按公钥对回 key id）。
///
/// 有了这一格，吊销一把钥之前就能回答「哪些包会因此再也验不过」——今天装载路径
/// 只记「验过了」，没记「是谁验的」，吊销的影响面就只能靠猜。
pub fn verifier_key_id(rows: &[cog_core::KeyRow], public_key_b64: &str) -> Option<String> {
    rows.iter()
        .find(|row| row.public_key_b64 == public_key_b64)
        .map(|row| row.key_id.clone())
}

/// 三个环境变量：包在对象存储里的 key、包原文的 sha256、发布公钥（base64）。
pub const BUNDLE_KEY_ENV: &str = "COGNEVA_PRINCIPLE_BUNDLE_KEY";
pub const BUNDLE_SHA_ENV: &str = "COGNEVA_PRINCIPLE_BUNDLE_SHA256";
pub const BUNDLE_PUBKEY_ENV: &str = "COGNEVA_PRINCIPLE_BUNDLE_PUBKEY";

/// 按环境变量装载一次。返回装进来的原则条数；`None` 是没配置。
pub async fn load_and_install() -> Result<Option<usize>, Box<dyn std::error::Error>> {
    let (Ok(key), Ok(sha), Ok(pubkey)) = (
        std::env::var(BUNDLE_KEY_ENV),
        std::env::var(BUNDLE_SHA_ENV),
        std::env::var(BUNDLE_PUBKEY_ENV),
    ) else {
        return Ok(None);
    };
    // 候选钥匙：流上没被吊销的那几把；环境里点名的那把也加上（首次部署时流上还
    // 没有密钥事实）。验包时逐个试，验过即用。
    let mut candidates: Vec<String> = vec![pubkey.clone()];
    let mut key_rows: Vec<cog_core::KeyRow> = Vec::new();
    if let Ok(database) = std::env::var(cog_core::env::DATABASE_URL) {
        if let Ok(pool) = sqlx::PgPool::connect(&database).await {
            match cog_storage::keys(&pool).await {
                Ok(rows) => {
                    candidates.extend(candidate_keys(&rows, chrono::Utc::now()));
                    key_rows = rows;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "key list unreadable; using the configured key")
                }
            }
        }
    }
    candidates.dedup();

    let config = crate::config_loader::load();
    let backend =
        cog_storage::build_object_backend(&config.providers.storage, &config.app.data_dir, None)?;
    let Some(bytes) = backend.get(&key).await? else {
        return Err(format!("principle bundle {key} is not in the object store").into());
    };
    let mut last_error = String::from("no candidate key opened the bundle");
    let mut bundle = None;
    let mut verified_with: Option<String> = None;
    for key in &candidates {
        match cog_core::load_bundle(&bytes, key, &sha) {
            Ok(parsed) => {
                verified_with = Some(key.clone());
                bundle = Some(parsed);
                break;
            }
            Err(error) => last_error = error,
        }
    }
    if let Some(ref key) = verified_with {
        tracing::info!(
            key_id = %verifier_key_id(&key_rows, key).unwrap_or_else(|| "unlisted".into()),
            "principle bundle verified with this key"
        );
    }
    let bundle = bundle.ok_or_else(|| format!("principle bundle refused: {last_error}"))?;
    let count = bundle.principles.len();
    // 装过就不再换：运行中换包会让同一批里前后两条原则来自两个版本。
    cog_core::ignore_lost!(CORPUS.set(Arc::new(bundle)));
    Ok(Some(count))
}

/// 用这组候选钥重验一次原则密封包：若再也验不过，返回包在对象存储里的名字。
///
/// 吊销前调它：`None` 是「没有配置包」或「剩下的钥仍能打开它」；`Some(key)` 才是
/// 「这一吊销会让这个包再也验不过」——那是动手前必须点名的事。
pub async fn bundle_lost_after_revocation(
    candidates: &[String],
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let (Ok(key), Ok(sha)) = (std::env::var(BUNDLE_KEY_ENV), std::env::var(BUNDLE_SHA_ENV)) else {
        return Ok(None);
    };
    let config = crate::config_loader::load();
    let backend =
        cog_storage::build_object_backend(&config.providers.storage, &config.app.data_dir, None)?;
    let Some(bytes) = backend.get(&key).await? else {
        return Ok(None);
    };
    for public_key in candidates {
        if cog_core::load_bundle(&bytes, public_key, &sha).is_ok() {
            return Ok(None);
        }
    }
    Ok(Some(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::BundledPrinciple;

    fn item(slug: &str, themes: &[&str], gate: Option<&str>) -> BundledPrinciple {
        BundledPrinciple {
            slug: slug.into(),
            source: "list-principles.sh".into(),
            lead: "l".into(),
            carrier_path: "memory/x.md".into(),
            carrier_sha256: "0".repeat(64),
            host_gate: None,
            where_red: gate.map(str::to_string),
            check_kind: None,
            priority: None,
            themes: themes.iter().map(|t| t.to_string()).collect(),
            body: "b".into(),
        }
    }

    #[test]
    fn revoked_and_expired_keys_are_not_candidates() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-25T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let row = |id: &str, state: &str, overlap: Option<&str>| cog_core::KeyRow {
            key_id: id.into(),
            purpose: "signing".into(),
            public_key_b64: format!("key-{id}"),
            fingerprint: "fp".into(),
            state: state.into(),
            overlap_until: overlap.map(str::to_string),
            revoked_reason: None,
        };
        let rows = vec![
            row("a", "issued", None),
            row("b", "rotated", Some("2026-12-01T00:00:00Z")),
            row("c", "rotated", Some("2026-11-01T00:00:00Z")),
            row("d", "revoked", None),
        ];
        let keys = candidate_keys(&rows, now);
        assert_eq!(keys, vec!["key-a".to_string(), "key-b".to_string()]);
    }

    #[test]
    fn selection_matches_by_theme_or_gate_prefix_and_nothing_else() {
        let bundle = PrincipleBundle {
            version: cog_core::BUNDLE_VERSION,
            built_at: "2026-11-25T00:00:00Z".into(),
            principles: vec![
                item("by-theme", &["cog-agent/src/context"], None),
                item("by-gate", &[], Some("crates/cog-agent/tests/x.rs")),
                item("unrelated", &["memory"], Some("crates/cog-eval/tests/y.rs")),
            ],
        };
        let touched = vec!["crates/cog-agent/src/context.rs".to_string()];
        let picked: Vec<&str> = select_for_paths(&bundle, &touched)
            .into_iter()
            .map(|item| item.slug.as_str())
            .collect();
        assert_eq!(picked, vec!["by-theme", "by-gate"]);
        assert!(select_for_paths(&bundle, &[]).is_empty());
    }
    /// 验过包的那把钥要能对回 key id；对不上的明说没有这一行。
    #[test]
    fn the_verifying_key_maps_back_to_its_key_id() {
        let row = cog_core::KeyRow {
            key_id: "k-1".into(),
            purpose: "principle-bundle".into(),
            public_key_b64: "PUB".into(),
            fingerprint: "fp".into(),
            state: "issued".into(),
            overlap_until: None,
            revoked_reason: None,
        };
        assert_eq!(
            verifier_key_id(&[row.clone()], "PUB").as_deref(),
            Some("k-1")
        );
        assert_eq!(verifier_key_id(&[row], "OTHER"), None);
    }
}
