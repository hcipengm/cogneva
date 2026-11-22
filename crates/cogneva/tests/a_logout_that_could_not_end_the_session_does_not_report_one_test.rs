//! A logout that could not end the session says so.
//!
//! `POST /api/v1/auth/logout` used to answer `200` with `"status": "logged_out"`
//! whatever the session store did: a failed `destroy`/`destroy_all` was written
//! to the log as a warning and the route answered success anyway. The caller on
//! the other end then holds a token it believes is dead, and the session it
//! names is still live -- the reply is the only thing that changed.
//!
//! The case below makes the store refuse, which is what a database that is down
//! looks like from here, and asks the route what it did. It may not report a
//! logout; which failure code it picks is left free.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::Request;
use tower::ServiceExt;

use cog_core::{SFError, SFResult, SessionInfo, SessionManager};
use cog_gateway::create_router;

use crate::router_state;

/// A store whose every write fails. Reads answer "no such session", so nothing
/// but the destroy path can be what the route's answer hangs on.
struct RefusesToDestroy;

#[async_trait::async_trait]
impl SessionManager for RefusesToDestroy {
    async fn create(&self, _session: SessionInfo) -> SFResult<uuid::Uuid> {
        Err(SFError::Database("connection refused".into()))
    }

    async fn get(
        &self,
        _user_id: uuid::Uuid,
        _session_id: uuid::Uuid,
    ) -> SFResult<Option<SessionInfo>> {
        Ok(None)
    }

    async fn destroy(&self, _user_id: uuid::Uuid, _session_id: uuid::Uuid) -> SFResult<()> {
        Err(SFError::Database("connection refused".into()))
    }

    async fn refresh(&self, _user_id: uuid::Uuid, _session_id: uuid::Uuid) -> SFResult<()> {
        Err(SFError::Database("connection refused".into()))
    }

    async fn destroy_all(&self, _user_id: uuid::Uuid) -> SFResult<()> {
        Err(SFError::Database("connection refused".into()))
    }
}

/// Log out without naming a session, so the route takes the path that ends
/// every session the caller has.
#[tokio::test]
async fn a_logout_that_could_not_end_the_session_does_not_report_one() {
    let state = router_state::with_session_manager(Arc::new(RefusesToDestroy));
    let token = router_state::admin_bearer(&state, "logout-probe").await;

    let response = create_router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/logout")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    let body = String::from_utf8_lossy(&bytes).into_owned();

    assert!(
        !status.is_success(),
        "a logout the store refused may not answer success: {status} {body}"
    );
    assert!(
        !body.contains("logged_out"),
        "the reply may not say the caller was logged out: {body}"
    );
}
