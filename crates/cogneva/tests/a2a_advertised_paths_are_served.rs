//! Every path the Agent Card advertises answers on the router that is served.
//!
//! The card is what an A2A caller reads before it asks for anything, and each
//! flag on it is a promise about a path. A flag left set after its route was
//! deleted is a card that lies, and nothing that reads the card can show it: the
//! flag is still set, the constant is still there, and the caller that believed
//! them gets a 404. The flag and the route are two statements in two places,
//! which is exactly how this card once advertised streaming while no path in the
//! tree contained `message/stream`.
//!
//! So this asks the router instead of the card. The promises are read from the
//! list the card's own flags are derived from, so a capability added there is
//! checked here without being added anywhere, and each listed path is requested
//! the way a caller would. Both verbs are tried rather than the one the route is
//! registered for: which verb a path answers on is one more hand-held fact, and
//! a probe carrying its own copy of it cannot notice the copy going stale.
//!
//! `404` and `405` are the router's answers for "no such route here" and "that
//! route takes another verb"; anything else means a handler for that path
//! exists. The check is therefore about registration and not about what the
//! handler then does, which is the part other suites cover.
//!
//! Two things this had to grow, both found by asking the router rather than by
//! reasoning about it:
//!
//! - The request is **authenticated**. This router answers `401` to every
//!   request it is given -- including paths it does not have -- before routing
//!   decides anything, so an anonymous probe gets a rejection whatever the path
//!   is and can tell a deleted route from a served one exactly never. A token
//!   from the state's own signer is what gets past that layer.
//! - There is a **control path**, one that is certainly not registered, and it
//!   has to come back `404`. Without it, the probe's question is "did anything
//!   answer", and any future catch-all, fallback or blanket rejection makes that
//!   answer yes for every path in the tree -- a gate that reports success by
//!   having lost the ability to see the difference. With it, the ability to
//!   tell is itself a checked claim, and it is checked in the same run.
//!
//! Its own router rather than the shared end-to-end harness, because what this
//! judges has nothing to do with any store: the harness there builds its state
//! around a quota store and skips the whole file when none is reachable, and a
//! contract about the route table should not go quiet on a machine that has no
//! database on it. The one component this state does need a store for is
//! supplied by a stand-in in `router_state`, and the stand-in is never asked
//! anything: a request that reaches a handler at all is already the answer.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_gateway::{create_router, GatewayState};

use crate::router_state;

/// The state every suite about the route table builds: no store behind it,
/// because the answers these suites ask for are decided before anything is
/// reached. It lives in `router_state` so a new state slot is added once.
fn state() -> Arc<GatewayState> {
    router_state::stateless()
}

/// A token this router's own signer accepts, so a request reaches routing
/// instead of stopping at the layer that answers every anonymous one.
async fn bearer(state: &Arc<GatewayState>) -> String {
    router_state::admin_bearer(state, "advertised-paths-probe").await
}
/// What the router answers one method on one path.
async fn status_of(app: &axum::Router, method: &str, path: &str, token: &str) -> StatusCode {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from("{}"))
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

/// A path is unserved when the router says it has no such route, or has one
/// under a different verb. Both are answers about registration; the path
/// behind them does not matter, and neither does what a handler would say.
fn unserved(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND || status == StatusCode::METHOD_NOT_ALLOWED
}

#[tokio::test]
async fn every_advertised_capability_names_a_path_the_router_serves() {
    let state = state();
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    // The control: a path nobody registered. If this is not answered with "no
    // such route", this probe cannot see the difference it is asking about --
    // and every advertised path below would pass for that reason alone.
    let control = format!("{}-that-is-not-a-route", cog_gateway::a2a::MESSAGE_STREAM);
    let mut control_unserved = false;
    for method in ["POST", "GET"] {
        if unserved(status_of(&app, method, &control, &token).await) {
            control_unserved = true;
        }
    }
    assert!(
        control_unserved,
        "a path that was never registered ({control}) was answered as though it were a \
         route, so this probe cannot tell an advertised path from an absent one"
    );

    let mut probed = 0usize;
    for advertised in cog_gateway::a2a::ADVERTISED {
        probed += 1;
        let mut served = false;
        for method in ["POST", "GET"] {
            if !unserved(status_of(&app, method, advertised.endpoint, &token).await) {
                served = true;
            }
        }
        assert!(
            served,
            "the card advertises `{}`, naming {}, and no verb on the router reaches a \
             handler for it -- the promise has no endpoint behind it",
            advertised.capability, advertised.endpoint
        );
    }
    assert!(
        probed > 0,
        "an empty list of promises is nothing to check: this gate would pass without asking"
    );
}
