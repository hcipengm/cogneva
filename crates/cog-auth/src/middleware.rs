use axum::{
    extract::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use tower::{Layer, Service};

use crate::error::{AuthError, AuthResult};
use cog_core::{AuthProvider, Claims, Permission, RoleRequirement, TokenUse};

/// Reject any token that is not an access token.
///
/// Access and refresh tokens are signed the same way and carry the same
/// claims, so a refresh token -- valid for up to a week -- would otherwise be
/// accepted as a bearer credential on every protected route. This is the same
/// fail-closed direction `JwtManager::refresh_access_token` enforces in the
/// other direction: claims decoded from a token issued before `token_use`
/// existed read as [`TokenUse::Access`] and keep working.
fn ensure_access_token(claims: &Claims) -> AuthResult<()> {
    if claims.token_use != TokenUse::Access {
        return Err(AuthError::InvalidToken(
            "expected an access token, got a refresh token".into(),
        ));
    }
    Ok(())
}

/// Extract the Bearer token from the `Authorization` header.
fn extract_bearer_token(headers: &axum::http::HeaderMap) -> AuthResult<&str> {
    let auth = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(AuthError::MissingAuthHeader)?
        .to_str()
        .map_err(|_| AuthError::InvalidAuthFormat)?;

    let parts: Vec<&str> = auth.splitn(2, ' ').collect();
    if parts.len() != 2 || parts[0].to_lowercase() != "bearer" {
        return Err(AuthError::InvalidAuthFormat);
    }
    Ok(parts[1])
}

/// Axum middleware: verify JWT and attach `Claims` to request extensions.
pub async fn auth_middleware(
    jwt: Arc<dyn AuthProvider>,
    mut request: Request,
    next: Next,
) -> Response {
    // Skip auth for health endpoints
    let path = request.uri().path();
    if path.starts_with("/health") {
        return next.run(request).await;
    }

    let token = match extract_bearer_token(request.headers()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };

    let claims = match jwt.verify_token(token).await {
        Ok(c) => c,
        Err(e) => return AuthError::from(e).into_response(),
    };

    if let Err(e) = ensure_access_token(&claims) {
        return e.into_response();
    }

    let user_id = claims.sub.clone();
    request.extensions_mut().insert(claims);
    request.extensions_mut().insert(user_id);
    next.run(request).await
}

// ---------------------------------------------------------------------------
// Permission-based RBAC
// ---------------------------------------------------------------------------

/// Layer that enforces a specific permission.
#[derive(Clone)]
pub struct RequirePermissionLayer {
    permission: Permission,
}

impl RequirePermissionLayer {
    pub fn new(permission: Permission) -> Self {
        Self { permission }
    }
}

impl<S> Layer<S> for RequirePermissionLayer {
    type Service = RequirePermissionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequirePermissionService {
            inner,
            permission: self.permission,
        }
    }
}

/// Tower service that checks permissions.
#[derive(Clone)]
pub struct RequirePermissionService<S> {
    inner: S,
    permission: Permission,
}

impl<S> Service<Request> for RequirePermissionService<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let permission = self.permission;
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let claims = req.extensions().get::<Claims>().cloned();
            match claims {
                Some(c) => {
                    let has = c.permissions.contains(&permission);
                    if !has {
                        let resp = AuthError::PermissionDenied(format!("missing {permission:?}"))
                            .into_response();
                        return Ok(resp);
                    }
                    inner.call(req).await
                }
                None => {
                    let resp = AuthError::MissingAuthHeader.into_response();
                    Ok(resp)
                }
            }
        })
    }
}

/// Convenience constructor for `require_permission` layer.
pub fn require_permission(permission: Permission) -> RequirePermissionLayer {
    RequirePermissionLayer::new(permission)
}

// ---------------------------------------------------------------------------
// Role-based RBAC
// ---------------------------------------------------------------------------

/// Layer that enforces a role requirement.
#[derive(Clone)]
pub struct RequireRoleLayer {
    requirement: RoleRequirement,
}

impl RequireRoleLayer {
    pub fn new(requirement: RoleRequirement) -> Self {
        Self { requirement }
    }
}

impl<S> Layer<S> for RequireRoleLayer {
    type Service = RequireRoleService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequireRoleService {
            inner,
            requirement: self.requirement,
        }
    }
}

/// Tower service that checks roles.
#[derive(Clone)]
pub struct RequireRoleService<S> {
    inner: S,
    requirement: RoleRequirement,
}

impl<S> Service<Request> for RequireRoleService<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let requirement = self.requirement;
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let claims = req.extensions().get::<Claims>().cloned();
            match claims {
                Some(c) => {
                    if !requirement.satisfied_by_any(&c.roles) {
                        let resp = AuthError::PermissionDenied(format!(
                            "role requirement {:?} not met",
                            requirement
                        ))
                        .into_response();
                        return Ok(resp);
                    }
                    inner.call(req).await
                }
                None => {
                    let resp = AuthError::MissingAuthHeader.into_response();
                    Ok(resp)
                }
            }
        })
    }
}

/// Convenience constructor for `require_role` layer.
pub fn require_role(requirement: RoleRequirement) -> RequireRoleLayer {
    RequireRoleLayer::new(requirement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Role, UserType};

    fn claims_with(token_use: TokenUse) -> Claims {
        Claims {
            sub: "0f8fad5b-d9cb-469f-a165-70867728950e".into(),
            iss: "cogneva".into(),
            aud: "cogneva-api".into(),
            exp: 4_102_444_800,
            iat: 1_700_000_000,
            jti: "3f1c2f7e-0b0a-4c1a-9a3e-2b2f0f7c9d11".into(),
            token_use,
            preferred_username: "testuser".into(),
            user_type: UserType::Standard,
            workspace_ids: vec!["ws-001".into()],
            permissions: vec![Permission::AgentRead],
            roles: vec![Role::Member],
        }
    }

    #[test]
    fn refresh_token_is_refused_as_a_bearer_credential() {
        let err = ensure_access_token(&claims_with(TokenUse::Refresh)).unwrap_err();
        assert!(
            matches!(err, AuthError::InvalidToken(_)),
            "a refresh token must not authenticate protected routes, got: {err}"
        );
    }

    #[test]
    fn access_token_is_accepted() {
        ensure_access_token(&claims_with(TokenUse::Access)).unwrap();
    }

    #[test]
    fn legacy_claims_without_token_use_still_authenticate() {
        let mut value = serde_json::to_value(claims_with(TokenUse::Access)).unwrap();
        value.as_object_mut().unwrap().remove("token_use").unwrap();
        let legacy: Claims = serde_json::from_value(value).unwrap();

        assert_eq!(legacy.token_use, TokenUse::Access);
        ensure_access_token(&legacy).unwrap();
    }
}
