//! An anonymous request gets the same answer whether or not the path it names
//! exists.
//!
//! The gateway's authentication is a blanket layer, not a per-route one: a
//! request without a token is rejected before routing decides anything, so the
//! answer cannot depend on which path was asked for. That is a property of where
//! the layer is attached, and it is invisible from the route table -- moving it
//! onto the individual protected routers leaves every route working, for callers
//! that have a token, while giving anonymous ones a different answer for a path
//! that exists than for one that never will. That difference is a way to read
//! the route table from outside without a credential.
//!
//! So the check asks the router twice, anonymously, with one path that is
//! served and one that is not, and requires the two answers to be equal. It does
//! not name the status: which code the layer answers with is a separate decision,
//! and pinning it here would turn a change of that decision into a failure of
//! this one.
//!
//! The equality alone would be worth nothing if the two paths were
//! indistinguishable for some other reason -- two absent paths answer alike
//! whatever the layer does. So the ability to tell them apart is itself checked,
//! in the same run, with a token: authenticated, the served path must not come
//! back "no such route" and the never-registered one must. Without that half, a
//! router whose routes all vanished would pass this file.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_gateway::{create_router, GatewayState};

use crate::router_state;

/// A served, protected path. Chosen from the general protected routers, which is
/// where a per-route attachment would be added first.
const SERVED: &str = "/api/v1/auth/me";

/// A path this router will never have. It is the pair of `SERVED` in every
/// request below; nothing here claims it should be servable.
const NEVER_REGISTERED: &str = "/api/v1/this-path-was-never-registered";

/// The state every suite about the route table builds: no store behind it,
/// and the quota stand-in permits everything it is asked, so a refusal here
/// would have to be arranged rather than inherited.
fn state() -> Arc<GatewayState> {
    router_state::stateless()
}

/// A token this router's own signer accepts, so a request reaches routing
/// instead of stopping at the layer that answers every anonymous one.
async fn bearer(state: &Arc<GatewayState>) -> String {
    router_state::admin_bearer(state, "anonymous-face-probe").await
}
/// What the router answers one path, with or without a credential.
async fn status_of(app: &axum::Router, path: &str, token: Option<&str>) -> StatusCode {
    let mut request = Request::builder()
        .method("GET")
        .uri(path)
        .header("Content-Type", "application/json");
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let request = request
        .body(Body::from(""))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers a request");
    let status = response.status();
    // Read the body rather than drop the response: a handler that starts
    // streaming and is dropped mid-body is a different failure, and this check
    // is not about bodies.
    let _ = to_bytes(response.into_body(), usize::MAX).await;
    status
}

/// "No such route here", or "that route takes another verb". Both are answers
/// about registration; what the path behind them does is not this file's subject.
fn unserved(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND || status == StatusCode::METHOD_NOT_ALLOWED
}

#[tokio::test]
async fn an_anonymous_request_gets_one_answer_whatever_path_it_names() {
    let state = state();
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    // The control on the probe itself. With a credential the two paths are
    // distinguishable -- one is routed, the other is not -- so an anonymous
    // equality below is the layer's doing and not the paths' likeness.
    let served_with_token = status_of(&app, SERVED, Some(&token)).await;
    let absent_with_token = status_of(&app, NEVER_REGISTERED, Some(&token)).await;
    let served_anonymous = status_of(&app, SERVED, None).await;
    let absent_anonymous = status_of(&app, NEVER_REGISTERED, None).await;
    let all = format!(
        "with a token: {SERVED} -> {served_with_token}, {NEVER_REGISTERED} -> \
         {absent_with_token}; anonymously: {SERVED} -> {served_anonymous}, \
         {NEVER_REGISTERED} -> {absent_anonymous}"
    );

    assert!(
        !unserved(served_with_token),
        "{SERVED} is not routed at all, so this probe cannot tell a path that exists \
         from one that does not, and everything below would pass for that reason \
         ({all})"
    );
    assert!(
        unserved(absent_with_token),
        "{NEVER_REGISTERED} is routed, so it is not the control this needs ({all})"
    );

    assert_eq!(
        served_anonymous, absent_anonymous,
        "an anonymous request is answered differently for a path that exists \
         ({SERVED} -> {served_anonymous}) than for one that never will \
         ({NEVER_REGISTERED} -> {absent_anonymous}). The difference is readable \
         without a credential, and it is the route table it reads"
    );
}
