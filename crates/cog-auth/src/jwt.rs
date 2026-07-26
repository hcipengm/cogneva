use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use uuid::Uuid;

use crate::error::{AuthError, AuthResult};
use cog_core::{Claims, Permission, Role, User, UserType};

/// JWT configuration.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    pub secret: String,
    pub issuer: String,
    pub audience: String,
    pub access_token_ttl_minutes: i64,
    pub refresh_token_ttl_days: i64,
    /// RSA private key in PEM format (for RS256 signing).
    pub private_key_pem: Option<String>,
    /// RSA public key in PEM format (for RS256 verification).
    pub public_key_pem: Option<String>,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret: "change-me-in-production".into(),
            issuer: "cogneva".into(),
            audience: "cogneva-api".into(),
            access_token_ttl_minutes: 15,
            refresh_token_ttl_days: 7,
            private_key_pem: None,
            public_key_pem: None,
        }
    }
}

/// Manages JWT creation and verification.
pub struct JwtManager {
    config: JwtConfig,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    algorithm: Algorithm,
}

impl JwtManager {
    pub fn new(config: JwtConfig) -> Self {
        let (encoding_key, decoding_key, algorithm) = if let (Some(ref private), Some(ref public)) =
            (&config.private_key_pem, &config.public_key_pem)
        {
            let enc =
                EncodingKey::from_rsa_pem(private.as_bytes()).expect("invalid RSA private key PEM");
            let dec =
                DecodingKey::from_rsa_pem(public.as_bytes()).expect("invalid RSA public key PEM");
            (enc, dec, Algorithm::RS256)
        } else {
            let enc = EncodingKey::from_secret(config.secret.as_bytes());
            let dec = DecodingKey::from_secret(config.secret.as_bytes());
            (enc, dec, Algorithm::HS256)
        };

        Self {
            config,
            encoding_key,
            decoding_key,
            algorithm,
        }
    }

    /// Returns the algorithm currently in use.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Map a [`UserType`] to a default set of [`Role`]s.
    fn roles_for_user_type(user_type: UserType) -> Vec<Role> {
        match user_type {
            UserType::Admin => vec![Role::SuperAdmin],
            UserType::Standard => vec![Role::Member],
            UserType::Guest => vec![Role::Visitor],
        }
    }

    /// Generate an access token and a refresh token for `user`.
    pub fn generate_token(
        &self,
        user: &User,
        workspace_ids: Vec<String>,
        permissions: Vec<Permission>,
    ) -> AuthResult<(String, String)> {
        let now = Utc::now();
        let access_exp = now + Duration::minutes(self.config.access_token_ttl_minutes);
        let refresh_exp = now + Duration::days(self.config.refresh_token_ttl_days);
        let roles = Self::roles_for_user_type(user.user_type);

        let access_claims = Claims {
            sub: user.id.to_string(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            exp: access_exp.timestamp(),
            iat: now.timestamp(),
            jti: Uuid::new_v4().to_string(),
            preferred_username: user.username.clone(),
            user_type: user.user_type,
            workspace_ids: workspace_ids.clone(),
            permissions: permissions.clone(),
            roles: roles.clone(),
        };

        let refresh_claims = Claims {
            sub: user.id.to_string(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            exp: refresh_exp.timestamp(),
            iat: now.timestamp(),
            jti: Uuid::new_v4().to_string(),
            preferred_username: user.username.clone(),
            user_type: user.user_type,
            workspace_ids,
            permissions,
            roles,
        };

        let header = Header::new(self.algorithm);
        let access_token = encode(&header, &access_claims, &self.encoding_key)
            .map_err(|e| AuthError::TokenGenerationFailed(e.to_string()))?;
        let refresh_token = encode(&header, &refresh_claims, &self.encoding_key)
            .map_err(|e| AuthError::TokenGenerationFailed(e.to_string()))?;

        Ok((access_token, refresh_token))
    }

    /// Verify an access or refresh token and return its claims.
    pub fn verify_token(&self, token: &str) -> AuthResult<Claims> {
        let mut validation = Validation::new(self.algorithm);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);

        let token_data = decode::<Claims>(token, &self.decoding_key, &validation).map_err(|e| {
            match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken(e.to_string()),
            }
        })?;

        Ok(token_data.claims)
    }

    /// Refresh an access token using a valid refresh token.
    /// Returns a new access token string.
    pub fn refresh_access_token(&self, refresh_token: &str) -> AuthResult<String> {
        let claims = self.verify_token(refresh_token)?;

        let now = Utc::now();
        let exp = now + Duration::minutes(self.config.access_token_ttl_minutes);

        let new_claims = Claims {
            sub: claims.sub,
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
            jti: Uuid::new_v4().to_string(),
            preferred_username: claims.preferred_username,
            user_type: claims.user_type,
            workspace_ids: claims.workspace_ids,
            permissions: claims.permissions,
            roles: claims.roles,
        };

        let header = Header::new(self.algorithm);
        encode(&header, &new_claims, &self.encoding_key)
            .map_err(|e| AuthError::TokenGenerationFailed(e.to_string()))
    }
}

#[async_trait::async_trait]
impl cog_core::AuthProvider for JwtManager {
    async fn generate_token(
        &self,
        user: &cog_core::User,
        workspace_ids: Vec<String>,
        permissions: Vec<cog_core::Permission>,
    ) -> cog_core::SFResult<cog_core::TokenPair> {
        let (access, refresh) = self
            .generate_token(user, workspace_ids, permissions)
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))?;
        Ok(cog_core::TokenPair {
            access_token: access,
            refresh_token: refresh,
        })
    }

    async fn verify_token(&self, token: &str) -> cog_core::SFResult<cog_core::Claims> {
        self.verify_token(token)
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }

    async fn refresh_access_token(&self, refresh_token: &str) -> cog_core::SFResult<String> {
        self.refresh_access_token(refresh_token)
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }
}

/// Generate a test RSA key pair (2048-bit) in PEM format.
#[cfg(test)]
pub fn generate_test_rsa_keypair() -> (String, String) {
    use rsa::pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey};
    use rsa::pkcs8::LineEnding;
    use rsa::RsaPrivateKey;

    let mut rng = rand::thread_rng();
    let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("failed to generate RSA key");
    let public_key = private_key.to_public_key();

    let private_pem = private_key
        .to_pkcs1_pem(LineEnding::LF)
        .expect("failed to encode private key")
        .to_string();
    let public_pem = public_key
        .to_pkcs1_pem(LineEnding::LF)
        .expect("failed to encode public key");

    (private_pem, public_pem)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_user() -> User {
        User {
            id: Uuid::new_v4(),
            phone: Some("13800138000".into()),
            email: Some("test@example.com".into()),
            username: "testuser".into(),
            display_name: Some("Test User".into()),
            avatar_url: None,
            status: cog_core::UserStatus::Active,
            user_type: UserType::Standard,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn generate_and_verify_token() {
        let mgr = JwtManager::new(JwtConfig::default());
        let user = test_user();
        let (access, refresh) = mgr
            .generate_token(&user, vec!["ws-001".into()], vec![Permission::AgentRead])
            .unwrap();

        assert!(!access.is_empty());
        assert!(!refresh.is_empty());

        let claims = mgr.verify_token(&access).unwrap();
        assert_eq!(claims.sub, user.id.to_string());
        assert_eq!(claims.preferred_username, "testuser");
        assert_eq!(claims.workspace_ids, vec!["ws-001"]);
    }

    #[test]
    fn refresh_access_token_ok() {
        let mgr = JwtManager::new(JwtConfig::default());
        let user = test_user();
        let (access, refresh) = mgr.generate_token(&user, vec![], vec![]).unwrap();

        let new_access = mgr.refresh_access_token(&refresh).unwrap();
        assert!(!new_access.is_empty());
        assert_ne!(new_access, access);

        let claims = mgr.verify_token(&new_access).unwrap();
        assert_eq!(claims.sub, user.id.to_string());
    }

    #[test]
    fn expired_token_fails() {
        let config = JwtConfig {
            access_token_ttl_minutes: -5,
            ..Default::default()
        };
        let mgr = JwtManager::new(config);
        let user = test_user();
        let (access, _) = mgr.generate_token(&user, vec![], vec![]).unwrap();

        let err = mgr.verify_token(&access).unwrap_err();
        assert!(matches!(err, AuthError::TokenExpired));
    }

    #[test]
    fn rs256_generate_and_verify_token() {
        let (private_pem, public_pem) = generate_test_rsa_keypair();
        let config = JwtConfig {
            private_key_pem: Some(private_pem),
            public_key_pem: Some(public_pem),
            ..Default::default()
        };
        let mgr = JwtManager::new(config);
        assert_eq!(mgr.algorithm(), Algorithm::RS256);

        let user = test_user();
        let (access, refresh) = mgr
            .generate_token(&user, vec!["ws-rs256".into()], vec![Permission::AgentRead])
            .unwrap();

        assert!(!access.is_empty());
        assert!(!refresh.is_empty());

        let claims = mgr.verify_token(&access).unwrap();
        assert_eq!(claims.sub, user.id.to_string());
        assert_eq!(claims.workspace_ids, vec!["ws-rs256"]);
    }

    #[test]
    fn rs256_refresh_access_token_ok() {
        let (private_pem, public_pem) = generate_test_rsa_keypair();
        let config = JwtConfig {
            private_key_pem: Some(private_pem),
            public_key_pem: Some(public_pem),
            ..Default::default()
        };
        let mgr = JwtManager::new(config);
        let user = test_user();
        let (access, refresh) = mgr.generate_token(&user, vec![], vec![]).unwrap();

        let new_access = mgr.refresh_access_token(&refresh).unwrap();
        assert!(!new_access.is_empty());
        assert_ne!(new_access, access);

        let claims = mgr.verify_token(&new_access).unwrap();
        assert_eq!(claims.sub, user.id.to_string());
    }

    #[test]
    fn rs256_token_fails_with_wrong_public_key() {
        let (private_pem, _public_pem) = generate_test_rsa_keypair();
        let (_other_private, other_public) = generate_test_rsa_keypair();

        let config = JwtConfig {
            private_key_pem: Some(private_pem),
            public_key_pem: Some(other_public),
            ..Default::default()
        };
        let mgr = JwtManager::new(config);
        let user = test_user();
        let (access, _) = mgr.generate_token(&user, vec![], vec![]).unwrap();

        let err = mgr.verify_token(&access).unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)));
    }
}
