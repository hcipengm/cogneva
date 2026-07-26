//! Keycloak OIDC integration.
//! Provides an OpenID Connect client for Keycloak SSO:
//! - Authorization URL generation
//! - Token exchange (authorization code → access/id tokens)
//! - Token introspection
//! - UserInfo retrieval
//! - JWT validation via JWKS with TTL caching
//! - Role mapping from Keycloak roles to sf-auth roles

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use cog_core::HttpRequest;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

use crate::error::{AuthError, AuthResult};
use cog_core::Role;

// ---------------------------------------------------------------------------
// JWKS caching
// ---------------------------------------------------------------------------

/// A single JSON Web Key as returned by Keycloak's JWKS endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Jwk {
    pub kty: String,
    pub kid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<String>, // RSA modulus (base64url)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub e: Option<String>, // RSA exponent (base64url)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x5c: Option<Vec<String>>, // X.509 certificate chain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#use: Option<String>,
}

/// In-memory JWKS cache keyed by `kid` with TTL support.
pub struct JwksCache {
    keys: RwLock<HashMap<String, Jwk>>,
    last_fetch: RwLock<Option<Instant>>,
    ttl: Duration,
}

impl JwksCache {
    /// Create a new cache with the given TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            last_fetch: RwLock::new(None),
            ttl,
        }
    }

    /// Returns true if the cache is stale or empty.
    pub fn is_stale(&self) -> bool {
        let last = self.last_fetch.read().ok();
        match last.as_deref() {
            None => true,
            Some(None) => true,
            Some(Some(t)) => t.elapsed() >= self.ttl,
        }
    }

    /// Insert a key into the cache and update the timestamp.
    pub fn insert(&self, jwk: Jwk) {
        if let Ok(mut keys) = self.keys.write() {
            keys.insert(jwk.kid.clone(), jwk);
        }
        if let Ok(mut last) = self.last_fetch.write() {
            *last = Some(Instant::now());
        }
    }

    /// Replace the entire cache contents and reset the timestamp.
    pub fn replace(&self, jwks: Vec<Jwk>) {
        if let Ok(mut keys) = self.keys.write() {
            keys.clear();
            for jwk in jwks {
                keys.insert(jwk.kid.clone(), jwk);
            }
        }
        if let Ok(mut last) = self.last_fetch.write() {
            *last = Some(Instant::now());
        }
    }

    /// Look up a JWK by its `kid`.
    pub fn get(&self, kid: &str) -> Option<Jwk> {
        self.keys.read().ok()?.get(kid).cloned()
    }

    /// Clear the cache.
    pub fn clear(&self) {
        if let Ok(mut keys) = self.keys.write() {
            keys.clear();
        }
        if let Ok(mut last) = self.last_fetch.write() {
            *last = None;
        }
    }
}

impl Default for JwksCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(3600))
    }
}

// ---------------------------------------------------------------------------
// Token claims
// ---------------------------------------------------------------------------

/// Standard OIDC token claims from a Keycloak access token.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TokenClaims {
    pub sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm_access: Option<RealmAccess>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_access: Option<HashMap<String, ClientAccess>>,
    pub exp: i64,
    pub iat: i64,
    pub iss: String,
    pub aud: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<i64>,
}

/// Result of validating a JWT token.
#[derive(Debug, Clone)]
pub struct TokenValidationResult {
    pub valid: bool,
    pub claims: Option<TokenClaims>,
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Role mapping
// ---------------------------------------------------------------------------

/// Result of mapping Keycloak roles to internal structures.
#[derive(Debug, Clone, Default)]
pub struct RoleMappingResult {
    pub realm_roles: Vec<String>,
    pub client_roles: HashMap<String, Vec<String>>,
    pub effective_permissions: Vec<String>,
}

/// Extract realm roles from token claims.
pub fn extract_realm_roles(claims: &TokenClaims) -> Vec<String> {
    claims
        .realm_access
        .as_ref()
        .map(|ra| ra.roles.clone())
        .unwrap_or_default()
}

/// Extract client roles from token claims.
pub fn extract_client_roles(claims: &TokenClaims) -> HashMap<String, Vec<String>> {
    claims
        .resource_access
        .as_ref()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, v.roles))
        .collect()
}

/// Build a [`RoleMappingResult`] from token claims.
pub fn map_role_mapping(claims: &TokenClaims) -> RoleMappingResult {
    let realm_roles = extract_realm_roles(claims);
    let client_roles = extract_client_roles(claims);

    let mut effective_permissions = Vec::new();
    for r in &realm_roles {
        effective_permissions.push(r.clone());
    }
    for roles in client_roles.values() {
        for r in roles {
            if !effective_permissions.contains(r) {
                effective_permissions.push(r.clone());
            }
        }
    }

    RoleMappingResult {
        realm_roles,
        client_roles,
        effective_permissions,
    }
}

/// Keycloak realm configuration.
#[derive(Debug, Clone)]
pub struct KeycloakConfig {
    pub base_url: String,
    pub realm: String,
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

impl KeycloakConfig {
    /// Build the OpenID Connect authorization endpoint URL.
    pub fn authorize_url(&self, state: &str, nonce: Option<&str>) -> String {
        let mut url = format!(
            "{}/realms/{}/protocol/openid-connect/auth?client_id={}&response_type=code&redirect_uri={}&scope=openid%20profile%20email&state={}",
            self.base_url,
            self.realm,
            self.client_id,
            urlencoding::encode(&self.redirect_uri),
            urlencoding::encode(state),
        );
        if let Some(n) = nonce {
            url.push_str(&format!("&nonce={}", urlencoding::encode(n)));
        }
        url
    }

    /// Token endpoint URL.
    pub fn token_url(&self) -> String {
        format!(
            "{}/realms/{}/protocol/openid-connect/token",
            self.base_url, self.realm
        )
    }

    /// UserInfo endpoint URL.
    pub fn userinfo_url(&self) -> String {
        format!(
            "{}/realms/{}/protocol/openid-connect/userinfo",
            self.base_url, self.realm
        )
    }

    /// Token introspection endpoint URL.
    pub fn introspect_url(&self) -> String {
        format!(
            "{}/realms/{}/protocol/openid-connect/token/introspect",
            self.base_url, self.realm
        )
    }

    /// OpenID Connect well-known discovery URL.
    pub fn well_known_url(&self) -> String {
        format!(
            "{}/realms/{}/.well-known/openid-configuration",
            self.base_url, self.realm
        )
    }
}

/// Token exchange response from Keycloak.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KeycloakTokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_expires_in: u64,
}

/// Encode key-value pairs as `application/x-www-form-urlencoded`.
fn form_body(pairs: &[(&str, &str)]) -> Vec<u8> {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&")
        .into_bytes()
}

/// Minimal Keycloak OIDC client.
#[derive(Debug, Clone)]
pub struct KeycloakClient {
    config: KeycloakConfig,
    client: Option<Arc<dyn cog_core::HttpClient>>,
}

impl KeycloakClient {
    pub fn new(config: KeycloakConfig) -> Self {
        Self {
            config,
            client: None,
        }
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Returns the underlying configuration.
    pub fn config(&self) -> &KeycloakConfig {
        &self.config
    }

    fn client(&self) -> crate::error::AuthResult<&Arc<dyn cog_core::HttpClient>> {
        self.client.as_ref().ok_or_else(|| {
            crate::error::AuthError::Internal("KeycloakClient has no HttpClient configured".into())
        })
    }

    /// Exchange an authorization code for tokens.
    /// POSTs to the Keycloak token endpoint with client credentials.
    pub async fn exchange_code(
        &self,
        code: &str,
    ) -> crate::error::AuthResult<KeycloakTokenResponse> {
        let body = form_body(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", &self.config.client_id),
            ("client_secret", &self.config.client_secret),
            ("redirect_uri", &self.config.redirect_uri),
        ]);

        let req = HttpRequest::post(self.config.token_url())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .timeout(30);

        let response = self.client()?.execute(req).await.map_err(|e| {
            crate::error::AuthError::Internal(format!("Keycloak token exchange network error: {e}"))
        })?;

        if !response.is_success() {
            let status = response.status;
            let text = response.text().unwrap_or_default();
            return Err(crate::error::AuthError::Internal(format!(
                "Keycloak token exchange failed: HTTP {status}: {text}"
            )));
        }

        let token_response = response.json::<KeycloakTokenResponse>().map_err(|e| {
            crate::error::AuthError::Internal(format!(
                "Failed to parse Keycloak token response: {e}"
            ))
        })?;

        Ok(token_response)
    }

    /// Validate a token via the introspection endpoint.
    /// POSTs to the Keycloak introspection endpoint with client credentials.
    pub async fn introspect(&self, token: &str) -> crate::error::AuthResult<TokenIntrospection> {
        let body = form_body(&[
            ("token", token),
            ("client_id", &self.config.client_id),
            ("client_secret", &self.config.client_secret),
        ]);

        let req = HttpRequest::post(self.config.introspect_url())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .timeout(30);

        let response = self.client()?.execute(req).await.map_err(|e| {
            crate::error::AuthError::Internal(format!("Keycloak introspection network error: {e}"))
        })?;

        if !response.is_success() {
            let status = response.status;
            let text = response.text().unwrap_or_default();
            return Err(crate::error::AuthError::Internal(format!(
                "Keycloak introspection failed: HTTP {status}: {text}"
            )));
        }

        let introspection = response.json::<TokenIntrospection>().map_err(|e| {
            crate::error::AuthError::Internal(format!(
                "Failed to parse Keycloak introspection response: {e}"
            ))
        })?;

        Ok(introspection)
    }

    /// Fetch user info from the UserInfo endpoint.
    /// GETs the Keycloak userinfo endpoint with a Bearer token.
    pub async fn userinfo(&self, access_token: &str) -> crate::error::AuthResult<KeycloakUserInfo> {
        let req = HttpRequest::get(self.config.userinfo_url())
            .header("Authorization", format!("Bearer {access_token}"))
            .timeout(30);

        let response = self.client()?.execute(req).await.map_err(|e| {
            crate::error::AuthError::Internal(format!("Keycloak userinfo network error: {e}"))
        })?;

        if !response.is_success() {
            let status = response.status;
            let text = response.text().unwrap_or_default();
            return Err(crate::error::AuthError::Internal(format!(
                "Keycloak userinfo failed: HTTP {status}: {text}"
            )));
        }

        let userinfo = response.json::<KeycloakUserInfo>().map_err(|e| {
            crate::error::AuthError::Internal(format!(
                "Failed to parse Keycloak userinfo response: {e}"
            ))
        })?;

        Ok(userinfo)
    }
}

/// Token introspection response.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TokenIntrospection {
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
}

/// Keycloak realm role container.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RealmAccess {
    #[serde(default)]
    pub roles: Vec<String>,
}

/// Keycloak client role container.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ClientAccess {
    #[serde(default)]
    pub roles: Vec<String>,
}

/// Keycloak UserInfo response.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KeycloakUserInfo {
    pub sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_verified: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub given_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm_access: Option<RealmAccess>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_access: Option<HashMap<String, ClientAccess>>,
}

/// JWT claims decoded from a Keycloak access token (internal, minimal).
#[derive(Debug, Clone, Deserialize)]
struct KeycloakTokenClaims {
    pub sub: String,
    #[serde(default)]
    pub realm_access: RealmAccess,
    #[serde(default)]
    pub resource_access: HashMap<String, ClientAccess>,
}

/// Result of authenticating a user via Keycloak.
#[derive(Debug, Clone)]
pub struct KeycloakAuthResult {
    pub user_id: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub roles: Vec<Role>,
}

#[derive(Debug)]
struct CachedJwks {
    jwks: JwkSet,
    fetched_at: Instant,
}

const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);

/// Keycloak OIDC authentication provider.
/// Validates JWT access tokens using cached JWKS, fetches UserInfo,
/// and maps Keycloak roles to sf-auth [`Role`]s.
#[derive(Debug)]
pub struct KeycloakAuthProvider {
    client: KeycloakClient,
    jwks_cache: RwLock<Option<CachedJwks>>,
}

impl KeycloakAuthProvider {
    /// Create a new provider wrapping a [`KeycloakClient`].
    pub fn new(client: KeycloakClient) -> Self {
        Self {
            client,
            jwks_cache: RwLock::new(None),
        }
    }

    /// Returns the underlying [`KeycloakClient`].
    pub fn client(&self) -> &KeycloakClient {
        &self.client
    }

    /// Authenticate a bearer token.
    /// 1. Validates the JWT signature and claims (exp, iss, aud) via JWKS.
    /// 2. Calls the Keycloak UserInfo endpoint.
    /// 3. Maps Keycloak roles to sf-auth [`Role`]s.
    pub async fn authenticate(&self, token: &str) -> AuthResult<KeycloakAuthResult> {
        let claims = self.validate_token(token).await?;
        let userinfo = self.client.userinfo(token).await?;
        let roles = resolve_roles_from_claims(&claims);

        Ok(KeycloakAuthResult {
            user_id: claims.sub,
            email: userinfo.email,
            name: userinfo.name,
            roles,
        })
    }

    /// Validate a JWT access token using cached JWKS and return a detailed result.
    pub fn validate_token_with_result(&self, token: &str) -> TokenValidationResult {
        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => {
                let token = token.to_string();
                let this = self;
                match handle.block_on(async move { this.validate_token(&token).await }) {
                    Ok(claims) => {
                        let claims = TokenClaims {
                            sub: claims.sub,
                            preferred_username: None,
                            email: None,
                            realm_access: Some(claims.realm_access),
                            resource_access: Some(claims.resource_access),
                            exp: 0,
                            iat: 0,
                            iss: String::new(),
                            aud: String::new(),
                            nbf: None,
                        };
                        TokenValidationResult {
                            valid: true,
                            claims: Some(claims),
                            error: None,
                        }
                    }
                    Err(e) => TokenValidationResult {
                        valid: false,
                        claims: None,
                        error: Some(e.to_string()),
                    },
                }
            }
            Err(_) => {
                // No runtime available — perform synchronous validation
                match self.validate_token_sync(token) {
                    Ok(claims) => TokenValidationResult {
                        valid: true,
                        claims: Some(claims),
                        error: None,
                    },
                    Err(e) => TokenValidationResult {
                        valid: false,
                        claims: None,
                        error: Some(e.to_string()),
                    },
                }
            }
        }
    }

    /// Synchronous token validation (used when no Tokio runtime is available).
    fn validate_token_sync(&self, token: &str) -> AuthResult<TokenClaims> {
        let jwks = self.get_jwks_sync()?;

        let header = decode_header(token)
            .map_err(|e| AuthError::InvalidToken(format!("decode header failed: {e}")))?;

        let kid = header
            .kid
            .ok_or_else(|| AuthError::InvalidToken("Token missing 'kid' header".into()))?;

        let jwk = jwks
            .find(&kid)
            .ok_or_else(|| AuthError::InvalidToken(format!("No JWK found for kid: {kid}")))?;

        let decoding_key = DecodingKey::from_jwk(jwk)
            .map_err(|e| AuthError::InvalidToken(format!("invalid JWK: {e}")))?;

        let mut validation = Validation::new(header.alg);
        let expected_issuer = format!(
            "{}/realms/{}",
            self.client.config().base_url,
            self.client.config().realm
        );
        validation.set_issuer(&[&expected_issuer]);
        validation.set_audience(&[&self.client.config().client_id]);

        let token_data = decode::<TokenClaims>(token, &decoding_key, &validation).map_err(|e| {
            match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken(e.to_string()),
            }
        })?;

        Ok(token_data.claims)
    }

    /// Validate a JWT access token using cached JWKS.
    async fn validate_token(&self, token: &str) -> AuthResult<KeycloakTokenClaims> {
        let jwks = self.get_jwks().await?;

        let header = decode_header(token)
            .map_err(|e| AuthError::InvalidToken(format!("decode header failed: {e}")))?;

        let kid = header
            .kid
            .ok_or_else(|| AuthError::InvalidToken("Token missing 'kid' header".into()))?;

        let jwk = jwks
            .find(&kid)
            .ok_or_else(|| AuthError::InvalidToken(format!("No JWK found for kid: {kid}")))?;

        let decoding_key = DecodingKey::from_jwk(jwk)
            .map_err(|e| AuthError::InvalidToken(format!("invalid JWK: {e}")))?;

        let mut validation = Validation::new(header.alg);
        let expected_issuer = format!(
            "{}/realms/{}",
            self.client.config().base_url,
            self.client.config().realm
        );
        validation.set_issuer(&[&expected_issuer]);
        validation.set_audience(&[&self.client.config().client_id]);

        let token_data = decode::<KeycloakTokenClaims>(token, &decoding_key, &validation).map_err(
            |e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken(e.to_string()),
            },
        )?;

        Ok(token_data.claims)
    }

    /// Get role mapping from a token string.
    pub async fn get_role_mapping(&self, token: &str) -> AuthResult<RoleMappingResult> {
        let claims = self.validate_token(token).await?;
        let claims = TokenClaims {
            sub: claims.sub,
            preferred_username: None,
            email: None,
            realm_access: Some(claims.realm_access),
            resource_access: Some(claims.resource_access),
            exp: 0,
            iat: 0,
            iss: String::new(),
            aud: String::new(),
            nbf: None,
        };
        Ok(map_role_mapping(&claims))
    }

    /// Retrieve JWKS synchronously, using the in-memory cache when fresh.
    fn get_jwks_sync(&self) -> AuthResult<JwkSet> {
        {
            let cache = self
                .jwks_cache
                .read()
                .map_err(|_| AuthError::Internal("JWKS cache lock poisoned".into()))?;
            if let Some(ref cached) = *cache {
                if cached.fetched_at.elapsed() < JWKS_CACHE_TTL {
                    return Ok(cached.jwks.clone());
                }
            }
        }
        Err(AuthError::Internal(
            "JWKS cache miss — synchronous fetch not available without async runtime".into(),
        ))
    }

    /// Retrieve JWKS, using the in-memory cache when fresh.
    async fn get_jwks(&self) -> AuthResult<JwkSet> {
        {
            let cache = self
                .jwks_cache
                .read()
                .map_err(|_| AuthError::Internal("JWKS cache lock poisoned".into()))?;
            if let Some(ref cached) = *cache {
                if cached.fetched_at.elapsed() < JWKS_CACHE_TTL {
                    return Ok(cached.jwks.clone());
                }
            }
        }

        let jwks = self.fetch_jwks().await?;

        {
            let mut cache = self
                .jwks_cache
                .write()
                .map_err(|_| AuthError::Internal("JWKS cache lock poisoned".into()))?;
            *cache = Some(CachedJwks {
                jwks: jwks.clone(),
                fetched_at: Instant::now(),
            });
        }

        Ok(jwks)
    }

    /// Fetch JWKS from the Keycloak realm.
    async fn fetch_jwks(&self) -> AuthResult<JwkSet> {
        let well_known_url = self.client.config().well_known_url();

        let req = HttpRequest::get(&well_known_url).timeout(30);
        let response = self.client.client()?.execute(req).await.map_err(|e| {
            AuthError::Internal(format!("Keycloak well-known config network error: {e}"))
        })?;

        if !response.is_success() {
            let status = response.status;
            let text = response.text().unwrap_or_default();
            return Err(AuthError::Internal(format!(
                "Keycloak well-known config failed: HTTP {status}: {text}"
            )));
        }

        let config: serde_json::Value = response.json().map_err(|e| {
            AuthError::Internal(format!("Failed to parse Keycloak well-known config: {e}"))
        })?;

        let jwks_uri = config["jwks_uri"]
            .as_str()
            .ok_or_else(|| AuthError::Internal("Missing jwks_uri in well-known config".into()))?;

        let req = HttpRequest::get(jwks_uri).timeout(30);
        let response = self
            .client
            .client()?
            .execute(req)
            .await
            .map_err(|e| AuthError::Internal(format!("Keycloak JWKS network error: {e}")))?;

        if !response.is_success() {
            let status = response.status;
            let text = response.text().unwrap_or_default();
            return Err(AuthError::Internal(format!(
                "Keycloak JWKS fetch failed: HTTP {status}: {text}"
            )));
        }

        let jwks: JwkSet = response
            .json()
            .map_err(|e| AuthError::Internal(format!("Failed to parse Keycloak JWKS: {e}")))?;

        Ok(jwks)
    }
}

/// Resolve sf-auth [`Role`]s from a [`KeycloakUserInfo`] response.
/// Falls back to [`Role::Visitor`] when no recognized roles are found.
pub fn resolve_roles(userinfo: &KeycloakUserInfo) -> Vec<Role> {
    let mut role_set = HashSet::new();

    if let Some(ref realm_access) = userinfo.realm_access {
        for role in &realm_access.roles {
            map_keycloak_role(role, &mut role_set);
        }
    }

    if let Some(ref resource_access) = userinfo.resource_access {
        for access in resource_access.values() {
            for role in &access.roles {
                map_keycloak_role(role, &mut role_set);
            }
        }
    }

    let mut roles: Vec<Role> = role_set.into_iter().collect();
    if roles.is_empty() {
        roles.push(Role::Visitor);
    }
    roles
}

fn resolve_roles_from_claims(claims: &KeycloakTokenClaims) -> Vec<Role> {
    let mut role_set = HashSet::new();

    for role in &claims.realm_access.roles {
        map_keycloak_role(role, &mut role_set);
    }

    for access in claims.resource_access.values() {
        for role in &access.roles {
            map_keycloak_role(role, &mut role_set);
        }
    }

    let mut roles: Vec<Role> = role_set.into_iter().collect();
    if roles.is_empty() {
        roles.push(Role::Visitor);
    }
    roles
}

fn map_keycloak_role(keycloak_role: &str, roles: &mut HashSet<Role>) {
    match keycloak_role.to_lowercase().as_str() {
        "superadmin" | "super_admin" | "realm-admin" | "admin" => {
            roles.insert(Role::SuperAdmin);
        }
        "orgadmin" | "org_admin" | "organization-admin" | "organization_admin" => {
            roles.insert(Role::OrgAdmin);
        }
        "owner" => {
            roles.insert(Role::Owner);
        }
        "member" | "user" | "operator" => {
            roles.insert(Role::Member);
        }
        "viewer" | "guest" | "read_only" | "read-only" | "readonly" => {
            roles.insert(Role::Visitor);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> KeycloakConfig {
        KeycloakConfig {
            base_url: "https://keycloak.example.com".into(),
            realm: "test".into(),
            client_id: "my-client".into(),
            client_secret: "secret".into(),
            redirect_uri: "https://app.example.com/auth/callback".into(),
        }
    }

    #[test]
    fn authorize_url_contains_required_params() {
        let config = test_config();
        let url = config.authorize_url("csrf-state", Some("nonce-123"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=my-client"));
        assert!(url.contains("state=csrf-state"));
        assert!(url.contains("nonce=nonce-123"));
    }

    #[test]
    fn token_url_format() {
        let config = test_config();
        assert_eq!(
            config.token_url(),
            "https://keycloak.example.com/realms/test/protocol/openid-connect/token"
        );
    }

    #[test]
    fn userinfo_url_format() {
        let config = test_config();
        assert_eq!(
            config.userinfo_url(),
            "https://keycloak.example.com/realms/test/protocol/openid-connect/userinfo"
        );
    }

    #[test]
    fn well_known_url_format() {
        let config = test_config();
        assert_eq!(
            config.well_known_url(),
            "https://keycloak.example.com/realms/test/.well-known/openid-configuration"
        );
    }

    #[tokio::test]
    async fn exchange_code_returns_error_for_unreachable_server() {
        let client = KeycloakClient::new(test_config());
        let result = client.exchange_code("dummy-code").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn introspect_returns_error_for_unreachable_server() {
        let client = KeycloakClient::new(test_config());
        let result = client.introspect("dummy-token").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn userinfo_returns_error_for_unreachable_server() {
        let client = KeycloakClient::new(test_config());
        let result = client.userinfo("dummy-access-token").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn auth_provider_authenticate_fails_for_unreachable_server() {
        let client = KeycloakClient::new(test_config());
        let provider = KeycloakAuthProvider::new(client);
        let result = provider.authenticate("dummy-token").await;
        assert!(result.is_err());
    }

    #[test]
    fn resolve_roles_maps_realm_roles() {
        let userinfo = KeycloakUserInfo {
            sub: "user-1".into(),
            preferred_username: Some("alice".into()),
            email: Some("alice@example.com".into()),
            email_verified: Some(true),
            name: Some("Alice".into()),
            given_name: None,
            family_name: None,
            realm_access: Some(RealmAccess {
                roles: vec!["admin".into(), "user".into()],
            }),
            resource_access: None,
        };

        let roles = resolve_roles(&userinfo);
        assert!(roles.contains(&Role::SuperAdmin));
        assert!(roles.contains(&Role::Member));
    }

    #[test]
    fn resolve_roles_maps_client_roles() {
        let mut resource_access = HashMap::new();
        resource_access.insert(
            "cog-gateway".into(),
            ClientAccess {
                roles: vec!["owner".into(), "viewer".into()],
            },
        );

        let userinfo = KeycloakUserInfo {
            sub: "user-2".into(),
            preferred_username: Some("bob".into()),
            email: Some("bob@example.com".into()),
            email_verified: None,
            name: None,
            given_name: None,
            family_name: None,
            realm_access: None,
            resource_access: Some(resource_access),
        };

        let roles = resolve_roles(&userinfo);
        assert!(roles.contains(&Role::Owner));
        assert!(roles.contains(&Role::Visitor));
    }

    #[test]
    fn resolve_roles_falls_back_to_visitor() {
        let userinfo = KeycloakUserInfo {
            sub: "user-3".into(),
            preferred_username: None,
            email: None,
            email_verified: None,
            name: None,
            given_name: None,
            family_name: None,
            realm_access: None,
            resource_access: None,
        };

        let roles = resolve_roles(&userinfo);
        assert_eq!(roles, vec![Role::Visitor]);
    }

    #[test]
    fn resolve_roles_ignores_unknown_roles() {
        let userinfo = KeycloakUserInfo {
            sub: "user-4".into(),
            preferred_username: None,
            email: None,
            email_verified: None,
            name: None,
            given_name: None,
            family_name: None,
            realm_access: Some(RealmAccess {
                roles: vec!["unknown_role".into(), "foobar".into()],
            }),
            resource_access: None,
        };

        let roles = resolve_roles(&userinfo);
        assert_eq!(roles, vec![Role::Visitor]);
    }

    #[test]
    fn test_role_extraction_from_claims() {
        let claims = TokenClaims {
            sub: "user-1".into(),
            preferred_username: Some("alice".into()),
            email: Some("alice@example.com".into()),
            realm_access: Some(RealmAccess {
                roles: vec!["admin".into(), "user".into()],
            }),
            resource_access: None,
            exp: chrono::Utc::now().timestamp() + 3600,
            iat: chrono::Utc::now().timestamp(),
            iss: "https://keycloak.example.com/realms/test".into(),
            aud: "my-client".into(),
            nbf: None,
        };
        let roles = extract_realm_roles(&claims);
        assert_eq!(roles, vec!["admin", "user"]);
    }

    #[test]
    fn test_extract_client_roles() {
        let mut resource_access = HashMap::new();
        resource_access.insert(
            "my-client".into(),
            ClientAccess {
                roles: vec!["reader".into(), "writer".into()],
            },
        );

        let claims = TokenClaims {
            sub: "user-2".into(),
            preferred_username: Some("bob".into()),
            email: Some("bob@example.com".into()),
            realm_access: None,
            resource_access: Some(resource_access),
            exp: chrono::Utc::now().timestamp() + 3600,
            iat: chrono::Utc::now().timestamp(),
            iss: "https://keycloak.example.com/realms/test".into(),
            aud: "my-client".into(),
            nbf: None,
        };

        let client_roles = extract_client_roles(&claims);
        assert_eq!(
            client_roles.get("my-client").unwrap(),
            &vec!["reader", "writer"]
        );
    }

    #[test]
    fn test_map_role_mapping() {
        let mut resource_access = HashMap::new();
        resource_access.insert(
            "my-client".into(),
            ClientAccess {
                roles: vec!["admin".into()],
            },
        );

        let claims = TokenClaims {
            sub: "user-3".into(),
            preferred_username: Some("charlie".into()),
            email: Some("charlie@example.com".into()),
            realm_access: Some(RealmAccess {
                roles: vec!["user".into()],
            }),
            resource_access: Some(resource_access),
            exp: chrono::Utc::now().timestamp() + 3600,
            iat: chrono::Utc::now().timestamp(),
            iss: "https://keycloak.example.com/realms/test".into(),
            aud: "my-client".into(),
            nbf: None,
        };

        let mapping = map_role_mapping(&claims);
        assert_eq!(mapping.realm_roles, vec!["user"]);
        assert_eq!(
            mapping.client_roles.get("my-client").unwrap(),
            &vec!["admin"]
        );
        assert!(mapping.effective_permissions.contains(&"user".into()));
        assert!(mapping.effective_permissions.contains(&"admin".into()));
    }

    #[test]
    fn test_jwks_cache_basic() {
        let cache = JwksCache::new(Duration::from_secs(3600));
        assert!(cache.is_stale());

        let jwk = Jwk {
            kty: "RSA".into(),
            kid: "key-1".into(),
            n: Some("abc".into()),
            e: Some("AQAB".into()),
            x5c: None,
            alg: Some("RS256".into()),
            r#use: Some("sig".into()),
        };

        cache.insert(jwk.clone());
        assert!(!cache.is_stale());

        let fetched = cache.get("key-1");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().kid, "key-1");

        cache.clear();
        assert!(cache.is_stale());
        assert!(cache.get("key-1").is_none());
    }

    #[test]
    fn test_jwks_cache_replace() {
        let cache = JwksCache::default();
        let jwks = vec![
            Jwk {
                kty: "RSA".into(),
                kid: "k1".into(),
                n: Some("n1".into()),
                e: Some("e1".into()),
                x5c: None,
                alg: None,
                r#use: None,
            },
            Jwk {
                kty: "RSA".into(),
                kid: "k2".into(),
                n: Some("n2".into()),
                e: Some("e2".into()),
                x5c: None,
                alg: None,
                r#use: None,
            },
        ];

        cache.replace(jwks);
        assert!(!cache.is_stale());
        assert!(cache.get("k1").is_some());
        assert!(cache.get("k2").is_some());
        assert!(cache.get("k3").is_none());
    }

    #[test]
    fn test_token_validation_result_success() {
        let result = TokenValidationResult {
            valid: true,
            claims: Some(TokenClaims {
                sub: "u1".into(),
                preferred_username: Some("alice".into()),
                email: None,
                realm_access: None,
                resource_access: None,
                exp: 0,
                iat: 0,
                iss: "iss".into(),
                aud: "aud".into(),
                nbf: None,
            }),
            error: None,
        };
        assert!(result.valid);
        assert!(result.claims.is_some());
        assert!(result.error.is_none());
    }

    #[test]
    fn test_token_validation_result_failure() {
        let result = TokenValidationResult {
            valid: false,
            claims: None,
            error: Some("expired".into()),
        };
        assert!(!result.valid);
        assert!(result.claims.is_none());
        assert_eq!(result.error, Some("expired".into()));
    }
}
