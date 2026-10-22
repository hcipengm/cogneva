//! principle: feedback_a_client_and_a_server_tested_apart_can_speak_different_wires
//!
//! The A2A client and the A2A server are two halves of one wire, and this drives
//! the client against the served router.
//!
//! The client used to post to `{url}/tasks/send` and read `{url}/tasks/{id}` --
//! shapes from an early draft that nothing serves today, this platform included.
//! Nothing caught it, because no test ever put the client and the router in the
//! same room: the client was exercised against a scripted peer, and the router
//! was exercised by hand-built HTTP requests. Two halves that each look right on
//! their own sheet of paper can still not fit together.
//!
//! So this test uses the real `A2aClient` with an `HttpClient` that hands the
//! request straight to the real `create_router` stack, path and all. It asks for
//! the two things a peer that does not stream does: send a message, then ask for
//! the task until it has an outcome. The outcome has to come back as an
//! artifact, because the caller lives in another process and cannot read our
//! task table.
//!
//! What this does not prove: that an external peer written to the A2A
//! specification would agree. It proves the two halves here agree with each
//! other, and that each of them answers at the address the agent card names.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::Request;
use serde_json::json;
use tower::ServiceExt;

use cog_core::{A2aTask, A2aTaskStatus, HttpClient, HttpRequest, HttpResponse, SFResult};
use cog_gateway::create_router;
use cog_protocol::A2aClient;

mod router_state;

/// The address the agent card puts in front of a peer.
const ENDPOINT: &str = "http://probe/api/v1/protocol/a2a";

/// An `HttpClient` that delivers to the router in this process instead of over a
/// socket. The wire is the same one -- method, path, headers, body -- only the
/// transport is short-circuited.
#[derive(Debug)]
struct InProcess {
    app: axum::Router,
}

#[async_trait::async_trait]
impl HttpClient for InProcess {
    async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
        let mut builder = Request::builder()
            .method(req.method.as_str())
            .uri(path_of(&req.url));
        for (name, value) in &req.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let request = builder
            .body(Body::from(req.body.unwrap_or_default()))
            .expect("the request is well formed");
        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("the router answers");
        let status = response.status().as_u16();
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body comes back")
            .to_vec();
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

/// The path component of an absolute URL.
fn path_of(url: &str) -> String {
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    match after_scheme.find('/') {
        Some(i) => after_scheme[i..].to_string(),
        None => "/".to_string(),
    }
}

/// Ask for the task until it is there. `message/send` is answered as soon as the
/// row is accepted, and the row reaches the graph from a spawned submit -- a
/// submission that is "queued" and one that is "held" read the same on the
/// caller's side, so the first read may legitimately come back empty.
async fn read_until_present(client: &A2aClient, task_id: &str) -> A2aTask {
    for _ in 0..100 {
        match client.get_task(ENDPOINT, task_id).await {
            Ok(task) => return task,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("the submitted row never became readable through tasks/get");
}

#[tokio::test]
async fn a_peer_that_does_not_stream_can_send_and_then_read_the_outcome() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    // The A2A face sits behind the platform's authentication, and its card says
    // so (`schemes: ["jwt"]`). A client without a token gets 401 on every call,
    // which is indistinguishable from "the peer is not there" -- so the client
    // carries one, exactly as a real peer would have to.
    let token = router_state::admin_bearer(&state, "a2a-rpc-probe").await;
    let client = A2aClient::new(Arc::new(InProcess { app })).with_bearer(token);

    let accepted = client
        .send_message(ENDPOINT, "probe goal")
        .await
        .expect("the endpoint accepts a message");
    assert_eq!(
        accepted.status,
        A2aTaskStatus::Submitted,
        "message/send answers with the task as accepted, not with a finished one"
    );
    assert!(
        accepted.artifacts.is_empty(),
        "nothing has been produced at acceptance time: {:?}",
        accepted.artifacts
    );

    let accepted_row = read_until_present(&client, &accepted.id).await;
    assert_eq!(accepted_row.status, A2aTaskStatus::Submitted);

    // Drive the row to an outcome the way the orchestrator would: schedule,
    // start, then complete with a result. This is the half a real run does on
    // its own.
    state
        .orchestrator
        .schedule_task(&accepted.id)
        .await
        .expect("a pending goal can be scheduled");
    state
        .orchestrator
        .start_task(&accepted.id)
        .await
        .expect("a submitted goal can start");
    state
        .orchestrator
        .complete_task(&accepted.id, json!({"answer": 42}))
        .await
        .expect("a running goal can complete");

    let done = client
        .get_task(ENDPOINT, &accepted.id)
        .await
        .expect("the finished task is readable");
    assert_eq!(done.status, A2aTaskStatus::Completed);
    assert_eq!(
        done.artifacts.len(),
        1,
        "a completed run carries exactly one artifact: {:?}",
        done.artifacts
    );
    assert_eq!(done.artifacts[0].name, "result");
    let text = done.artifacts[0]
        .parts
        .first()
        .and_then(|part| match part {
            cog_core::MessagePart::Text { text } => Some(text.as_str()),
            cog_core::MessagePart::File { .. } => None,
        })
        .expect("the artifact's part is text");
    let carried: serde_json::Value = serde_json::from_str(text).expect("the artifact is JSON");
    assert_eq!(
        carried["answer"], 42,
        "the result crosses the wire intact: {text}"
    );

    // A task the peer does not have is a refusal in the envelope, not an empty
    // result: "no such task" and "task with nothing in it" must not read alike.
    let missing = client
        .get_task(ENDPOINT, "no-such-task")
        .await
        .expect_err("an unknown id is refused");
    assert!(
        missing.to_string().contains("no task 'no-such-task'"),
        "the refusal has to name what it could not find: {missing}"
    );
}

/// Ask for the agent card the way a peer on the other side of an ingress would:
/// with the `Host` it reached us at, and the scheme the proxy terminated.
async fn card_behind(
    app: &axum::Router,
    token: &str,
    host: &str,
    forwarded_proto: Option<&str>,
) -> serde_json::Value {
    let mut builder = Request::builder()
        .method("GET")
        .uri("/api/v1/protocol/a2a/agent-card")
        .header("Host", host)
        .header("Authorization", format!("Bearer {token}"));
    if let Some(scheme) = forwarded_proto {
        builder = builder.header("X-Forwarded-Proto", scheme);
    }
    let response = app
        .clone()
        .oneshot(
            builder
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers");
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body comes back");
    serde_json::from_slice(&bytes).expect("the card is JSON")
}

/// The card's `url` is the address the peer should call back, so it has to be
/// the address the peer just reached us at -- not this process's bind address.
/// `0.0.0.0` means "listen everywhere"; it is not a target anyone can dial.
#[tokio::test]
async fn the_card_names_the_address_the_caller_reached_us_at() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-card-probe").await;

    let behind_tls = card_behind(&app, &token, "cogneva.example", Some("https")).await;
    assert_eq!(
        behind_tls["url"], "https://cogneva.example/api/v1/protocol/a2a",
        "the card has to point back at the name and scheme the caller used: {behind_tls}"
    );

    let with_port = card_behind(&app, &token, "10.0.0.7:8080", None).await;
    assert_eq!(
        with_port["url"], "http://10.0.0.7:8080/api/v1/protocol/a2a",
        "a plain request keeps its host and port: {with_port}"
    );
}

/// A peer that has never talked to us has no credential yet -- and the card is
/// what tells it which credential to get. So the card has to answer without
/// one, while the endpoint it names has to refuse.
///
/// Asking with a bearer proves nothing here: the fixture carries a token the
/// real first-contact caller cannot have, and both sides stay green while the
/// peer is stuck at step one.
#[tokio::test]
async fn the_card_answers_a_peer_that_has_no_credential_yet() {
    let state = router_state::stateless();
    let app = create_router(state);

    let card = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/protocol/a2a/agent-card")
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers");
    assert_eq!(
        card.status().as_u16(),
        200,
        "discovery cannot require the credential it is supposed to announce"
    );

    let rpc = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/protocol/a2a")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"jsonrpc": "2.0", "id": 1, "method": "tasks/get", "params": {"id": "x"}})
                        .to_string(),
                ))
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers");
    assert!(
        matches!(rpc.status().as_u16(), 401 | 403),
        "the business endpoint still refuses a caller with no credential, got {}",
        rpc.status()
    );
}

/// One HTTP round trip to the endpoint, returning the JSON body. Used where the
/// test has to send something the client would refuse to build.
async fn post(app: &axum::Router, token: &str, body: serde_json::Value) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/protocol/a2a")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers");
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body comes back");
    serde_json::from_slice(&bytes).expect("the endpoint answers JSON-RPC")
}

/// The endpoint says which methods it serves instead of 404-ing, and a call
/// missing the one field it needs says so. Both are refusals a peer can act on:
/// "this platform does not do that" and "you left out the id" are different
/// problems with different fixes, and neither should read as a dead address.
#[tokio::test]
async fn the_endpoint_names_what_it_will_not_do() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-refusals").await;

    let unknown = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tasks/resubscribe", "params": {"id": "t-1"}}),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32601);
    let said = unknown["error"]["message"]
        .as_str()
        .expect("the refusal carries a message");
    assert!(
        said.contains("tasks/resubscribe") && said.contains("message/send"),
        "the refusal has to name what was asked and what is served: {said}"
    );

    let no_id = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tasks/get", "params": {}}),
    )
    .await;
    assert_eq!(no_id["error"]["code"], -32602);
    assert!(
        no_id["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("params.id"),
        "a missing field is named: {no_id}"
    );
}

/// The card names one address and the specification dispatches on the method, so
/// `message/stream` has to answer there too -- not only on its own path. A peer
/// written to the spec never learns the other path exists; it reads
/// `streaming: true` and posts the method to the address the card gave it.
#[tokio::test]
async fn the_card_address_serves_the_stream_method_too() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-stream").await;

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/protocol/a2a")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "message/stream",
                "params": {"message": {"role": "user",
                                       "parts": [{"kind": "text", "text": "probe stream"}]}},
            })
            .to_string(),
        ))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers");

    assert_eq!(response.status(), 200);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(
        content_type.starts_with("text/event-stream"),
        "the stream method has to answer with a stream here, not with a refusal: {content_type}"
    );
}

/// A peer can stop a task it started, and gets back the row as it stands after
/// the stop -- not an acknowledgement it would have to re-read the task to
/// interpret.
#[tokio::test]
async fn a_peer_can_stop_a_task_it_started() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-cancel").await;
    let client =
        A2aClient::new(Arc::new(InProcess { app: app.clone() })).with_bearer(token.clone());

    let accepted = client
        .send_message(ENDPOINT, "probe cancel")
        .await
        .expect("the endpoint accepts a message");
    read_until_present(&client, &accepted.id).await;

    let cancelled = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 3, "method": "tasks/cancel",
               "params": {"id": accepted.id}}),
    )
    .await;
    assert_eq!(
        cancelled["result"]["status"], "canceled",
        "the answer is the row after the stop: {cancelled}"
    );

    let afterwards = client
        .get_task(ENDPOINT, &accepted.id)
        .await
        .expect("the cancelled task is still readable");
    assert_eq!(afterwards.status, A2aTaskStatus::Canceled);

    // Cancelling something that is not there is a refusal, not a silent success.
    let missing = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 4, "method": "tasks/cancel",
               "params": {"id": "no-such-task"}}),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32001);
    assert!(missing["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("no task 'no-such-task'"));
}
