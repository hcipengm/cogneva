//! GitHub App 身份（安全网关侧）。
//!
//! 网关代持 GitHub App 私钥，按需用 RS256 JWT 换取 installation access
//! token，作为代码平台透传（`/github/*`、`/git/github/*`、`/attach`）的出口
//! 凭证。配置了 App 时出口以 App bot 身份发出（评论/评审/交叉验证署名归一到
//! App bot）；未配置则回退静态 OAuth/PAT token——属主主流程不依赖 App，
//! 未配置时行为与原先完全一致（零回归）。
//!
//! 凭证只从环境变量读取（K8s Secret 仅注入安全网关进程），私钥不出网关、
//! 不转发给业务 Pod：
//! - `COGNEVA_GITHUB_APP_ID`：App ID（数字）
//! - `COGNEVA_GITHUB_APP_INSTALLATION_ID`：安装 ID（数字）
//! - `COGNEVA_GITHUB_APP_KEY`：App 私钥 PEM（PKCS#1/PKCS#8，
//!   可含字面 `\n` 或 base64 包裹）。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

/// App JWT 有效期：9 分钟（GitHub 硬上限 10 分钟）。
const JWT_TTL_SECS: i64 = 9 * 60;
/// 签发时间提前 60s，容忍网关与 GitHub 时钟偏差。
const JWT_CLOCK_SKEW_SECS: i64 = 60;
/// installation token 在实际过期前留足刷新余量。
const REFRESH_MARGIN_SECS: u64 = 120;
/// 解析不到过期时间时的保守缓存时长（installation token 寿命约 1 小时）。
const FALLBACK_TTL_SECS: u64 = 50 * 60;

/// GitHub App 凭证（仅驻留网关内存，Debug 不打印私钥）。
#[derive(Clone)]
pub struct GitHubAppCreds {
    pub app_id: u64,
    pub installation_id: u64,
    private_key_pem: String,
}

impl std::fmt::Debug for GitHubAppCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubAppCreds")
            .field("app_id", &self.app_id)
            .field("installation_id", &self.installation_id)
            .field("private_key_pem", &"[redacted]")
            .finish()
    }
}

impl GitHubAppCreds {
    /// 从网关环境变量读取 App 凭证；三个变量任一缺失/非法返回 None。
    pub fn from_env() -> Option<Self> {
        let app_id = std::env::var("COGNEVA_GITHUB_APP_ID").ok()?;
        let installation_id = std::env::var("COGNEVA_GITHUB_APP_INSTALLATION_ID").ok()?;
        let key_raw = std::env::var("COGNEVA_GITHUB_APP_KEY").ok()?;
        Self::parse(&app_id, &installation_id, &key_raw)
    }

    /// 纯解析（测试与非 env 来源复用）：app_id/installation_id 为数字、
    /// 私钥可解析为 PEM 时才返回。
    pub fn parse(app_id: &str, installation_id: &str, private_key_raw: &str) -> Option<Self> {
        let app_id = app_id.trim().parse::<u64>().ok()?;
        let installation_id = installation_id.trim().parse::<u64>().ok()?;
        let private_key_pem = normalize_pem(private_key_raw)?;
        // 构造不出签名 key 的私钥视为未配置（fail-closed，避免运行时才报错）。
        EncodingKey::from_rsa_pem(private_key_pem.as_bytes()).ok()?;
        Some(Self {
            app_id,
            installation_id,
            private_key_pem,
        })
    }

    /// 签发一个 App JWT（iss=app_id，RS256，约 9 分钟有效）。
    pub fn mint_jwt(&self) -> Result<String, String> {
        mint_app_jwt(self.app_id, &self.private_key_pem)
    }
}

/// 签发 GitHub App JWT：`iss`=App ID，`iat` 提前 60s、`exp` 9 分钟后。
/// 纯函数（给定私钥即可），便于单测验证签名与声明。
pub fn mint_app_jwt(app_id: u64, private_key_pem: &str) -> Result<String, String> {
    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "iat": now - JWT_CLOCK_SKEW_SECS,
        "exp": now + JWT_TTL_SECS,
        "iss": app_id.to_string(),
    });
    let key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
        .map_err(|e| format!("GitHub App 私钥 PEM 无效: {e}"))?;
    encode(&Header::new(Algorithm::RS256), &claims, &key)
        .map_err(|e| format!("签发 GitHub App JWT 失败: {e}"))
}

/// 把环境变量里的私钥归一化成可解析 PEM：支持字面 `\n` 转义与
/// base64 包裹两种常见的 Secret 写法；无法辨认则原样返回交给签名处校验。
fn normalize_pem(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let unescaped = s.replace("\\n", "\n");
    if unescaped.contains("BEGIN") && unescaped.contains("PRIVATE KEY") {
        return Some(unescaped);
    }
    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(s) {
        if let Ok(text) = std::str::from_utf8(&decoded) {
            let text = text.trim();
            if text.contains("PRIVATE KEY") {
                return Some(text.replace("\\n", "\n"));
            }
        }
    }
    Some(unescaped)
}

/// 缓存的 installation token + 缓存时长。
struct CachedToken {
    token: String,
    valid_for: Duration,
    stored_at: Instant,
}

/// installation token 缓存：1 小时寿命的 token 不必每次透传都去换。
#[derive(Default)]
pub struct AppTokenCache {
    inner: Mutex<Option<CachedToken>>,
}

impl AppTokenCache {
    fn store(&self, token: String, valid_for: Duration) {
        *self.inner.lock().unwrap() = Some(CachedToken {
            token,
            valid_for,
            stored_at: Instant::now(),
        });
    }

    /// 缓存仍新鲜（未到过期前的刷新余量）则返回，否则 None。
    fn fresh(&self) -> Option<String> {
        let guard = self.inner.lock().unwrap();
        guard
            .as_ref()
            .filter(|c| c.stored_at.elapsed() < c.valid_for)
            .map(|c| c.token.clone())
    }
}

/// 用 App JWT 换取 installation access token（优先返回缓存）。
///
/// `api_base` 默认为 `https://api.github.com`；可注入其他基址（测试/自建
/// 网关场景）。换取失败返回错误，由调用方决定是否回退静态 token。
pub async fn installation_token(
    client: &reqwest::Client,
    creds: &GitHubAppCreds,
    cache: &AppTokenCache,
    api_base: &str,
) -> Result<String, String> {
    if let Some(token) = cache.fresh() {
        return Ok(token);
    }
    let jwt = creds.mint_jwt()?;
    let url = format!(
        "{}/app/installations/{}/access_tokens",
        api_base.trim_end_matches('/'),
        creds.installation_id
    );
    let resp = client
        .post(&url)
        .bearer_auth(jwt)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "cogneva-security-gateway")
        .send()
        .await
        .map_err(|e| format!("请求 GitHub installation token 失败: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let body: String = body.chars().take(300).collect();
        return Err(format!("installation token 接口返回 {status}: {body}"));
    }
    let v: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("解析 installation token 响应失败: {e}"))?;
    let token = v
        .get("token")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "installation token 响应缺少 token 字段".to_string())?
        .to_string();
    let valid_for = parse_token_ttl(v.get("expires_at").and_then(|x| x.as_str()));
    cache.store(token.clone(), valid_for);
    Ok(token)
}

/// 从 GitHub 返回的 `expires_at`（RFC3339）推算缓存时长，扣除刷新余量并
/// 收敛到安全区间；解析失败回退保守时长。
fn parse_token_ttl(expires_at: Option<&str>) -> Duration {
    let fallback = Duration::from_secs(FALLBACK_TTL_SECS);
    let Some(raw) = expires_at else {
        return fallback;
    };
    let Ok(exp) = chrono::DateTime::parse_from_rfc3339(raw) else {
        return fallback;
    };
    let secs = (exp.with_timezone(&chrono::Utc) - chrono::Utc::now()).num_seconds();
    let secs = (secs - REFRESH_MARGIN_SECS as i64).clamp(30, 3300);
    Duration::from_secs(secs.max(30) as u64)
}

/// 解析代码平台透传要用的 GitHub 出口凭证：配置了 App 则用 installation
/// token（以 App bot 身份）；换取失败或未配置时回退静态 token（属主主流程
/// 不依赖 App，OAuth/PAT 通道始终可用）。两者皆无返回 None（调用方按
/// "未配置凭证" 处理）。
pub async fn resolve_github_bearer(
    app: Option<&GitHubAppCreds>,
    cache: &AppTokenCache,
    client: &reqwest::Client,
    api_base: &str,
    static_token: Option<&str>,
) -> Option<String> {
    if let Some(creds) = app {
        match installation_token(client, creds, cache, api_base).await {
            Ok(token) => return Some(token),
            Err(e) => {
                tracing::warn!(error=%e, "GitHub App installation token 获取失败，回退静态 token");
            }
        }
    }
    static_token.map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
    use rsa::pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey};
    use rsa::pkcs8::LineEnding;
    use rsa::RsaPrivateKey;

    fn test_keypair() -> (String, String) {
        let mut rng = rand::thread_rng();
        let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("rsa key");
        let public_key = private_key.to_public_key();
        let private_pem = private_key
            .to_pkcs1_pem(LineEnding::LF)
            .expect("private pem")
            .to_string();
        let public_pem = public_key.to_pkcs1_pem(LineEnding::LF).expect("public pem");
        (private_pem, public_pem)
    }

    #[test]
    fn mint_jwt_is_rs256_with_app_identity_claims() {
        let (private_pem, public_pem) = test_keypair();
        let token = mint_app_jwt(123456, &private_pem).expect("mint");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_aud = false;
        let data = decode::<serde_json::Value>(
            &token,
            &DecodingKey::from_rsa_pem(public_pem.as_bytes()).expect("pub pem"),
            &validation,
        )
        .expect("verify");
        assert_eq!(data.claims["iss"], "123456");
        let iat = data.claims["iat"].as_i64().unwrap();
        let exp = data.claims["exp"].as_i64().unwrap();
        assert_eq!(exp - iat, JWT_TTL_SECS + JWT_CLOCK_SKEW_SECS);
        assert_eq!(data.header.alg, Algorithm::RS256);
    }

    #[test]
    fn mint_jwt_rejects_garbage_pem() {
        assert!(mint_app_jwt(1, "not a pem").is_err());
    }

    #[test]
    fn normalize_pem_handles_escaped_newlines() {
        let (private_pem, _) = test_keypair();
        let escaped = private_pem.replace('\n', "\\n");
        let normalized = normalize_pem(&escaped).expect("normalized");
        assert!(mint_app_jwt(7, &normalized).is_ok());
    }

    #[test]
    fn normalize_pem_handles_base64() {
        let (private_pem, _) = test_keypair();
        use base64::Engine;
        let wrapped = base64::engine::general_purpose::STANDARD.encode(private_pem.as_bytes());
        let normalized = normalize_pem(&wrapped).expect("normalized");
        assert!(mint_app_jwt(7, &normalized).is_ok());
    }

    #[test]
    fn parse_requires_numeric_ids_and_valid_key() {
        let (private_pem, _) = test_keypair();
        assert!(GitHubAppCreds::parse("123", "456", &private_pem).is_some());
        assert!(GitHubAppCreds::parse("abc", "456", &private_pem).is_none());
        assert!(GitHubAppCreds::parse("123", "", &private_pem).is_none());
        assert!(GitHubAppCreds::parse("123", "456", "----- not a key -----").is_none());
    }

    #[test]
    fn cache_returns_fresh_token_only_within_ttl() {
        let cache = AppTokenCache::default();
        assert!(cache.fresh().is_none());
        cache.store("short".into(), Duration::ZERO);
        assert!(cache.fresh().is_none(), "zero ttl is immediately stale");
        cache.store("good".into(), Duration::from_secs(3600));
        assert_eq!(cache.fresh().as_deref(), Some("good"));
    }

    #[test]
    fn parse_token_ttl_falls_back_and_clamps() {
        assert_eq!(
            parse_token_ttl(None),
            Duration::from_secs(FALLBACK_TTL_SECS)
        );
        assert_eq!(
            parse_token_ttl(Some("not-a-date")),
            Duration::from_secs(FALLBACK_TTL_SECS)
        );
        // 远未来：clamp 到 3300 秒上限。
        assert_eq!(
            parse_token_ttl(Some("2999-01-01T00:00:00Z")),
            Duration::from_secs(3300)
        );
    }

    #[test]
    fn resolve_bearer_uses_static_token_when_app_absent() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = reqwest::Client::new();
        let cache = AppTokenCache::default();
        let got = rt.block_on(resolve_github_bearer(
            None,
            &cache,
            &client,
            "https://api.github.com",
            Some("pat-token"),
        ));
        assert_eq!(got.as_deref(), Some("pat-token"));
        let none_got = rt.block_on(resolve_github_bearer(
            None,
            &cache,
            &client,
            "https://api.github.com",
            None,
        ));
        assert!(none_got.is_none());
    }

    #[test]
    fn resolve_bearer_falls_back_when_app_exchange_fails() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let cache = AppTokenCache::default();
        let (private_pem, _) = test_keypair();
        let creds = GitHubAppCreds::parse("123", "456", &private_pem).expect("creds");
        // 指向不可达基址：换 token 必然失败，应回退静态 token。
        let got = rt.block_on(resolve_github_bearer(
            Some(&creds),
            &cache,
            &client,
            "http://127.0.0.1:1",
            Some("pat-fallback"),
        ));
        assert_eq!(got.as_deref(), Some("pat-fallback"));
    }
}
