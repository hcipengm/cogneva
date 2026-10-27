//! principle: feedback_a_key_the_platform_cannot_end_is_refused_where_it_enters
//! principle: feedback_a_behaviour_checked_only_by_a_pure_function_and_a_hand_probe_has_no_gate
//!
//! The inbox route refuses a reply stream it did not open, and it is the route
//! that says so.
//!
//! A caller-named `reply_stream` is a key whose owner the platform does not
//! know: it cannot end it when the question is over, and the caller cannot read
//! it either, because the store is not reachable from outside. Accepting the
//! field answers `202` for a key that can stay in the store for good. The
//! handler therefore rejects it before either delivery path, and names the route
//! that does return an answer -- an agent asking another agent through
//! `send_to_agent` with `expect_reply`, whose stream the platform opens and
//! ends.
//!
//! That refusal was only ever verified by hand against a live deployment, and
//! its reason was checked as a pure function of the message. Neither notices the
//! handler losing the call: the pure function would still classify messages
//! correctly, and a probe that is not in the tree runs nowhere. So this asks the
//! served route. A `prompt` carrying the field must be refused; the same route
//! with the same body minus that field must get past the check to the delivery
//! path, and a command that has no such field at all must get past it too.
//! Without those two controls a handler that refused every request would pass.
//!
//! The state behind the router has no agent pool and no registry, so the
//! delivery path answers "not configured". That is the point: this file judges
//! where the refusal comes from, not what a complete deployment would answer.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_gateway::{create_router, GatewayState};

mod router_state;

fn state() -> Arc<GatewayState> {
    router_state::stateless()
}

/// The route a command is addressed to. Its own name for the agent does not
/// matter: the refusal comes before anything looks the name up.
const ROUTE: &str = "/api/v1/agents/probe-agent/messages";

/// What the served router answers this body on `/api/v1/agents/{id}/messages`.
async fn post(app: &axum::Router, token: &str, body: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method("POST")
        .uri(ROUTE)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body comes back");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test]
async fn a_prompt_naming_its_own_reply_stream_is_refused_by_the_route() {
    let state = state();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "reply-stream-probe").await;

    let refused = r#"{"type":"prompt","goal":{"objective":"probe"},
                      "reply_stream":"orchestrator:reply:caller-named"}"#;
    let (status, body) = post(&app, &token, refused).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a caller-named reply stream is refused before any delivery path; \
         the route answered {status} with {body}"
    );
    assert!(
        body.contains("send_to_agent") && body.contains("expect_reply"),
        "the refusal has to name the path that does return an answer, \
         or the caller is told no and given nowhere to go: {body}"
    );

    // Same route, same body, minus the field the check is about: this must get
    // past the refusal to the delivery path, which answers "not configured" in
    // this fixture. Anything else and the test above would be passing for a
    // handler that refuses everything.
    let accepted_shape = r#"{"type":"prompt","goal":{"objective":"probe"}}"#;
    let (status, body) = post(&app, &token, accepted_shape).await;
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "a prompt that names no reply stream is not this check's subject, so it \
         has to reach the delivery path; it answered {status} with {body}"
    );

    // A command with no such field at all takes the same route past the check.
    let (status, body) = post(&app, &token, r#"{"type":"steering","instruction":"probe"}"#).await;
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "a steering command has no reply stream to name and must pass the check; \
         it answered {status} with {body}"
    );
}
