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
    assert_eq!(done.artifacts[0].name.as_deref(), Some("result"));
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
    // The card is a read-only discovery document that peers re-fetch; a short
    // TTL keeps repeat discovery from recomputing it, and stays short enough
    // that a changed address or capability shows up within minutes.
    assert_eq!(
        card.headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("public, max-age=300"),
        "the card must be cacheable for a short, declared window"
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

/// Two versions are served, and the card says which one the caller asked for.
///
/// A 1.0 card has no top-level `url`: endpoints live in `supportedInterfaces`,
/// and listing both versions there is how a 1.0 peer learns it can also fall
/// back to 0.3. A version nobody serves is refused by name instead of being
/// rounded down to the default -- a 1.2 client must not read a 0.3 answer as
/// "this peer behaves oddly".
#[tokio::test]
async fn the_card_answers_in_the_version_the_caller_asked_for() {
    let state = router_state::stateless();
    let app = create_router(state);

    let ask = |version: Option<&'static str>| {
        let app = app.clone();
        async move {
            let mut builder = Request::builder()
                .method("GET")
                .uri("/api/v1/protocol/a2a/agent-card")
                .header("Host", "cogneva.example");
            if let Some(version) = version {
                builder = builder.header("A2A-Version", version);
            }
            let response = app
                .oneshot(builder.body(Body::empty()).expect("well formed"))
                .await
                .expect("the router answers");
            let status = response.status().as_u16();
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("the body comes back");
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            )
        }
    };

    let (status, default_card) = ask(None).await;
    assert_eq!(status, 200);
    assert_eq!(
        default_card["protocolVersion"], "0.3",
        "an absent version header is 0.3 by the specification's own default"
    );
    assert_eq!(
        default_card["url"],
        "http://cogneva.example/api/v1/protocol/a2a"
    );

    let (status, v10_card) = ask(Some("1.0")).await;
    assert_eq!(status, 200);
    assert!(
        v10_card.get("url").is_none(),
        "1.0 moved the endpoint into supportedInterfaces: {v10_card}"
    );
    let interfaces = v10_card["supportedInterfaces"]
        .as_array()
        .expect("a 1.0 card lists its interfaces");
    let versions: Vec<&str> = interfaces
        .iter()
        .filter_map(|i| i["protocolVersion"].as_str())
        .collect();
    assert!(
        versions.contains(&"1.0") && versions.contains(&"0.3"),
        "a 1.0 peer has to see that 0.3 is served too: {versions:?}"
    );

    let (status, refusal) = ask(Some("1.2")).await;
    assert_eq!(
        status, 400,
        "an unserved version is refused, not rounded down"
    );
    let said = refusal["message"].as_str().unwrap_or_default();
    assert!(
        said.contains("0.3") && said.contains("1.0"),
        "the refusal names the versions that are served: {said}"
    );
}

/// The discovery entry point the specification names, served without credentials.
///
/// A peer finds an agent by GETting `/.well-known/agent-card.json`; there is no
/// step where it is told our own path first. A card reachable only at a private
/// path is invisible to every client written to the specification and to the
/// official conformance kit -- the one class of defect where nothing on our side
/// is wrong and the peer still cannot connect.
///
/// The default there is 1.0, not 0.3: this URI comes from the 1.0 specification,
/// and a 1.0 client needs `supportedInterfaces` to pick a transport at all. The
/// older version stays one header away.
#[tokio::test]
async fn the_card_is_discoverable_where_the_specification_puts_it() {
    let state = router_state::stateless();
    let app = create_router(state);

    let fetch =
        |version: Option<&'static str>, token: Option<&'static str>, inm: Option<String>| {
            let app = app.clone();
            async move {
                let mut builder = Request::builder()
                    .method("GET")
                    .uri("/.well-known/agent-card.json")
                    .header("Host", "cogneva.example");
                if let Some(version) = version {
                    builder = builder.header("A2A-Version", version);
                }
                if let Some(token) = token {
                    builder = builder.header("Authorization", format!("Bearer {token}"));
                }
                if let Some(etag) = inm {
                    builder = builder.header("If-None-Match", etag);
                }
                let response = app
                    .oneshot(builder.body(Body::empty()).expect("well formed"))
                    .await
                    .expect("the router answers");
                let status = response.status().as_u16();
                let cache = response
                    .headers()
                    .get("cache-control")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let bytes = to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("the body comes back");
                let card = serde_json::from_slice::<serde_json::Value>(&bytes)
                    .unwrap_or(serde_json::Value::Null);
                (status, cache, etag, card)
            }
        };

    // 无版本头、无令牌：这就是「第一次接触的对端」发出的那一个请求。
    let (status, cache, etag, card) = fetch(None, None, None).await;
    assert_eq!(
        status, 200,
        "discovery must not need a credential, or it closes a loop: the credential          is what the card is supposed to tell you how to get"
    );
    assert_eq!(
        cache.as_deref(),
        Some("public, max-age=300"),
        "a public, read-only document should be cacheable, and briefly"
    );
    for field in [
        "name",
        "description",
        "version",
        "capabilities",
        "skills",
        "supportedInterfaces",
        "defaultInputModes",
        "defaultOutputModes",
    ] {
        assert!(
            card.get(field).is_some(),
            "a 1.0 card carries {field}: {card}"
        );
    }
    assert!(
        card.get("url").is_none(),
        "1.0 moved the endpoint into supportedInterfaces: {card}"
    );
    let interfaces = card["supportedInterfaces"]
        .as_array()
        .expect("a discovered card lists its interfaces");
    assert!(
        interfaces.iter().any(|i| i["protocolVersion"] == "1.0"),
        "the discovery card has to name the version it is written in: {interfaces:?}"
    );

    // 显式要 0.3 的对端在同一个路径上拿到 0.3 名片（带 `url`，它就是这么拼地址的）。
    let (status, _, _, v03) = fetch(Some("0.3"), None, None).await;
    assert_eq!(status, 200);
    assert_eq!(v03["protocolVersion"], "0.3");
    assert_eq!(
        v03["url"], "http://cogneva.example/api/v1/protocol/a2a",
        "the 0.3 card is what an older peer builds its call address from"
    );

    // 不服务的版本仍是同一个拒绝，不是 404、也不是悄悄回一份缺省名片。
    let (status, _, _, refusal) = fetch(Some("1.2"), None, None).await;
    assert_eq!(status, 400);
    let said = refusal["message"].as_str().unwrap_or_default();
    assert!(
        said.contains("0.3") && said.contains("1.0"),
        "the refusal names what is served: {said}"
    );

    // 带令牌也是同一份名片：这张卡不按调用方不同而不同。
    let (status, _, _, with_token) = fetch(None, Some("irrelevant-but-well-formed"), None).await;
    assert_eq!(
        status, 200,
        "the discovery route is public; a token neither helps nor hurts"
    );
    assert_eq!(with_token, card, "the card is the same for everyone");

    // 名片带一个由内容导出的 ETag，对端过期后用条件请求取而不是重下。
    let etag = etag.expect("a discovery card carries an ETag");
    assert!(etag.starts_with('"'), "an ETag is a quoted string: {etag}");
    let (again_status, _, again_etag, _) = fetch(None, None, None).await;
    assert_eq!(again_status, 200);
    assert_eq!(
        again_etag.as_deref(),
        Some(etag.as_str()),
        "the same card has to hash to the same ETag, or no peer can cache it"
    );
    let (status, _, echoed, body) = fetch(None, None, Some(etag.clone())).await;
    assert_eq!(
        status, 304,
        "a matching If-None-Match gets the unchanged answer, not the body again"
    );
    assert_eq!(echoed.as_deref(), Some(etag.as_str()));
    assert_eq!(body, serde_json::Value::Null, "304 carries no body");
}

/// 名片还带一个弱校验器 `Last-Modified`，而且它在两次取之间不动。
///
/// 强 `ETag` 管「内容变没变」，`Last-Modified` 管「什么时候可能变过」——老代理与
/// 只认时间的客户端只有后者。它必须是**进程级固定值**：每取一次就换一个时刻的头，
/// 会让所有按时间判断的缓存永远判成「刚变过」，等于没有这个头。
#[tokio::test]
async fn the_card_carries_a_last_modified_that_does_not_move_between_fetches() {
    let state = router_state::stateless();
    let app = create_router(state);

    let fetch = |inm: Option<String>| {
        let app = app.clone();
        async move {
            let mut builder = Request::builder()
                .method("GET")
                .uri("/.well-known/agent-card.json")
                .header("Host", "cogneva.example");
            if let Some(etag) = inm {
                builder = builder.header("If-None-Match", etag);
            }
            let response = app
                .oneshot(builder.body(Body::empty()).expect("well formed"))
                .await
                .expect("the router answers");
            let header = |name: &str| {
                response
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            };
            let status = response.status().as_u16();
            (status, header("etag"), header("last-modified"))
        }
    };

    let (status, etag, first) = fetch(None).await;
    assert_eq!(status, 200);
    let etag = etag.expect("a discovery card carries an ETag");
    let last_modified = first.expect("a discovery card carries a Last-Modified");
    // HTTP 只认这一种日期拼法（`Sun, 06 Nov 1994 08:49:37 GMT`）；长度与结尾一起判，
    // 是因为一个「差不多像」的日期串不会被解析器拒绝，只会被静默忽略。
    assert_eq!(
        last_modified.len(),
        29,
        "an HTTP date is 29 characters: {last_modified}"
    );
    assert!(
        last_modified.ends_with(" GMT"),
        "an HTTP date ends in GMT: {last_modified}"
    );

    let (_, _, again) = fetch(None).await;
    assert_eq!(
        again.as_deref(),
        Some(last_modified.as_str()),
        "the same process serves the same Last-Modified, or no cache can trust it"
    );

    let (status, _, on_304) = fetch(Some(etag)).await;
    assert_eq!(status, 304);
    assert_eq!(
        on_304.as_deref(),
        Some(last_modified.as_str()),
        "a 304 repeats the validators it would have sent with the body"
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
        json!({"jsonrpc": "2.0", "id": 1, "method": "tasks/pushNotificationConfig/set",
               "params": {"id": "t-1", "pushNotificationConfig": {"url": "https://peer.example/hook"}}}),
    )
    .await;
    assert_eq!(
        unknown["error"]["code"], -32004,
        "a method A2A defines but this platform does not serve is UnsupportedOperation, \
         not MethodNotFound -- the peer's spelling is right, the capability is absent"
    );
    let said = unknown["error"]["message"]
        .as_str()
        .expect("the refusal carries a message");
    assert!(
        said.contains("tasks/pushNotificationConfig/set") && said.contains("message/send"),
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

/// A peer that times out and sends the same message again is asking about the
/// same work, not for more of it. The `messageId` is the key that says so, and
/// answering with a second task means the goal is decomposed, run and paid for
/// twice while the peer holds two ids for one answer.
#[tokio::test]
async fn a_retry_carrying_the_same_message_id_answers_the_same_task() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-retry").await;

    let send = |message_id: &str| {
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "message/send",
            "params": {"message": {
                "kind": "message",
                "messageId": message_id,
                "role": "user",
                "parts": [{"kind": "text", "text": "probe goal"}]
            }}
        })
    };

    let first = post(&app, &token, send("retry-probe-1")).await;
    let again = post(&app, &token, send("retry-probe-1")).await;
    let other = post(&app, &token, send("retry-probe-2")).await;

    let first_id = first["result"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("the first send answers a task: {first}"));
    assert_eq!(
        again["result"]["id"], first["result"]["id"],
        "the same messageId is the same task: {again}"
    );
    assert_ne!(
        other["result"]["id"],
        json!(first_id),
        "a different message is a different task: {other}"
    );

    // The retry has to answer the row itself, not just the same id: a peer that
    // retries after the task moved on reads whatever state it is in now.
    let read = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tasks/get", "params": {"id": first_id}}),
    )
    .await;
    assert_eq!(read["result"]["id"], json!(first_id), "{read}");
}

/// The card advertises `stateTransitionHistory`, so a peer that asks for the
/// history has to get back the message that created the task -- and a peer that
/// asks for nothing has to get no `history` key at all. An always-empty array is
/// the one answer that makes the card's claim false while every shape-based
/// check still passes: the key is there, the array is an array, and nothing says
/// it should have been longer.
#[tokio::test]
async fn tasks_get_returns_the_message_that_created_the_task_when_it_is_asked_for() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-history").await;
    let goal = "刷新接口未区分访问令牌";
    let client =
        A2aClient::new(Arc::new(InProcess { app: app.clone() })).with_bearer(token.clone());

    let accepted = client
        .send_message(ENDPOINT, goal)
        .await
        .expect("the endpoint accepts a message");

    let asked = post(
        &app,
        &token,
        json!({
            "jsonrpc": "2.0", "id": 9, "method": "tasks/get",
            "params": {"id": accepted.id, "historyLength": 1}
        }),
    )
    .await;
    assert_eq!(
        asked["result"]["id"],
        json!(accepted.id),
        "the history comes back on the row that was asked for: {asked}"
    );
    let history = asked["result"]["history"]
        .as_array()
        .unwrap_or_else(|| panic!("the history was asked for and is an array: {asked}"));
    assert_eq!(history.len(), 1, "{asked}");
    assert_eq!(history[0]["role"], json!("user"));
    assert_eq!(
        history[0]["parts"][0]["text"],
        json!(goal),
        "the history carries what the peer actually said"
    );

    let unasked = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 10, "method": "tasks/get", "params": {"id": accepted.id}}),
    )
    .await;
    assert!(
        unasked["result"].get("history").is_none(),
        "a caller that asked for no history gets no history key, not an empty one: {unasked}"
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

/// A frame off the SSE body, split into its `event:` name and its `data:` JSON.
///
/// Reads exactly one frame at a time and leaves the rest in `buf`: the point of
/// the test below is that frames arrive as the task moves, so consuming the
/// whole body up front would hide exactly what is being measured.
async fn read_frame(
    body: &mut axum::body::BodyDataStream,
    buf: &mut String,
) -> Option<serde_json::Value> {
    use futures::StreamExt;
    loop {
        if let Some(end) = buf.find("\n\n") {
            let frame: String = buf.drain(..end + 2).collect();
            let mut event = None;
            let mut data = String::new();
            for line in frame.trim_end().lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    event = Some(rest.trim().to_string());
                } else if let Some(rest) = line.strip_prefix("data:") {
                    data.push_str(rest.trim_start());
                }
            }
            assert_eq!(
                event.as_deref(),
                Some("message"),
                "every frame on this stream is a JSON-RPC message: {frame}"
            );
            return Some(
                serde_json::from_str(&data)
                    .unwrap_or_else(|e| panic!("the data line is JSON ({e}): {frame}")),
            );
        }
        let next = tokio::time::timeout(Duration::from_secs(5), body.next())
            .await
            .expect("a frame has to arrive before the timeout");
        let chunk = next?.expect("the body yields bytes");
        buf.push_str(&String::from_utf8_lossy(&chunk));
    }
}

/// The stream says "where it is" and "what it made" in separate frames, and it
/// ends when the row reaches a terminal state.
///
/// The rest of this file proves the *shape* of the stream response -- status
/// 200, `text/event-stream`. That is the answer to "does the endpoint exist".
/// What a streaming peer actually acts on is the frames: the snapshot that
/// hands it the task id, the status updates it follows, the artifact frame that
/// carries the outcome, and a stream that stops instead of hanging until the
/// deadline. None of that is visible from the headers, so it is measured here
/// against the same row the HTTP face reads: the test drives the orchestrator
/// between frames exactly as a worker would.
#[tokio::test]
async fn the_stream_carries_states_and_the_outcome_and_then_ends() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-stream-frames").await;

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/protocol/a2a")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 11,
                "method": "message/stream",
                "params": {"message": {"role": "user",
                                       "parts": [{"kind": "text", "text": "probe frames"}]}},
            })
            .to_string(),
        ))
        .expect("the request is well formed");
    let response = app.oneshot(request).await.expect("the router answers");
    assert_eq!(response.status(), 200);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/event-stream"),
        "the stream method has to answer with a stream here: {content_type}"
    );

    let mut body = response.into_body().into_data_stream();
    let mut buf = String::new();

    // 第一帧是整条任务快照：对端靠它拿到 id，后面每一帧都按这个 id 对上。
    let first = read_frame(&mut body, &mut buf)
        .await
        .expect("an accepted stream speaks before it goes quiet");
    assert_eq!(first["jsonrpc"], "2.0");
    assert_eq!(first["id"], 11, "the JSON-RPC id comes back in every frame");
    assert_eq!(first["result"]["kind"], "task");
    assert_eq!(first["result"]["status"]["state"], "submitted");
    let goal_id = first["result"]["id"]
        .as_str()
        .expect("the snapshot names the task the peer is following")
        .to_string();
    assert!(
        first["result"]["artifacts"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(false),
        "an accepted goal has produced nothing yet; an artifact here would be a lie"
    );

    // 让这一行动起来，与别的写者读的是同一行。
    state
        .orchestrator
        .schedule_task(&goal_id)
        .await
        .expect("an accepted goal can be scheduled");
    state
        .orchestrator
        .start_task(&goal_id)
        .await
        .expect("a scheduled goal can be started");

    let working = read_frame(&mut body, &mut buf)
        .await
        .expect("a state change produces a frame");
    assert_eq!(working["result"]["kind"], "status-update");
    assert_eq!(working["result"]["taskId"], goal_id);
    assert_eq!(working["result"]["status"]["state"], "working");
    assert_eq!(working["result"]["final"], false);

    state
        .orchestrator
        .complete_task(&goal_id, json!({"summary": "frames crossed the wire"}))
        .await
        .expect("a running goal can be completed");

    // 终态先发产物、再发状态：一件是「做成了什么」，一件是「到哪一步了」。
    let artifact = read_frame(&mut body, &mut buf)
        .await
        .expect("the outcome is a frame of its own");
    assert_eq!(artifact["result"]["kind"], "artifact-update");
    assert_eq!(artifact["result"]["taskId"], goal_id);
    assert_eq!(artifact["result"]["artifact"]["artifactId"], "result");
    assert_eq!(artifact["result"]["artifact"]["name"], "result");

    let last = read_frame(&mut body, &mut buf)
        .await
        .expect("the terminal state is reported");
    assert_eq!(last["result"]["kind"], "status-update");
    assert_eq!(last["result"]["status"]["state"], "completed");
    assert_eq!(
        last["result"]["final"], true,
        "a peer reads `final` to stop following; without it the stream has no end on its side"
    );

    // 终态之后流必须收：一直接到 deadline 的流，在对端看来与「还在跑」同形。
    use futures::StreamExt;
    let end = tokio::time::timeout(Duration::from_secs(5), body.next())
        .await
        .expect("the stream closes once the row is terminal");
    assert!(
        end.is_none(),
        "the stream has to end after the terminal frame, not keep polling"
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
        cancelled["result"]["status"]["state"], "canceled",
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

/// A peer whose connection dropped mid-run holds a task id, not the message it
/// sent. `tasks/resubscribe` is how it gets the stream back without sending the
/// work a second time; polling `tasks/get` is not the same thing, because a poll
/// cannot see what happened while it was not asking.
#[tokio::test]
async fn a_peer_that_reconnects_can_resubscribe_to_the_task_it_holds() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-resubscribe").await;
    let client =
        A2aClient::new(Arc::new(InProcess { app: app.clone() })).with_bearer(token.clone());

    let accepted = client
        .send_message(ENDPOINT, "probe resubscribe")
        .await
        .expect("the endpoint accepts a message");
    read_until_present(&client, &accepted.id).await;
    state
        .orchestrator
        .schedule_task(&accepted.id)
        .await
        .expect("an accepted goal can be scheduled");
    state
        .orchestrator
        .start_task(&accepted.id)
        .await
        .expect("a scheduled goal can be started");
    state
        .orchestrator
        .complete_task(&accepted.id, json!({"answer": 7}))
        .await
        .expect("a running goal can be completed");

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/protocol/a2a")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(
            json!({"jsonrpc": "2.0", "id": 3, "method": "tasks/resubscribe",
                   "params": {"id": accepted.id}})
            .to_string(),
        ))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers");
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream"),
        "resubscribing opens a stream, not a single JSON answer"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the stream body comes back and ends");
    let text = String::from_utf8_lossy(&body).into_owned();
    let first = text
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap_or_else(|| panic!("the stream carries a data frame: {text}"));
    let frame: serde_json::Value = serde_json::from_str(first).expect("the frame is JSON");
    assert_eq!(
        frame["result"]["id"],
        json!(accepted.id),
        "the first frame is the task the peer asked to follow: {frame}"
    );
    assert_eq!(
        frame["result"]["status"]["state"], "completed",
        "it is the row as it stands now, not a replay from the start: {frame}"
    );
    assert_eq!(
        frame["result"]["artifacts"][0]["name"], "result",
        "a peer that missed the end still gets what the run produced: {frame}"
    );

    // An id nobody has is refused before a stream is opened: the answer is the
    // same envelope `tasks/get` gives, not an empty stream to interpret.
    let missing = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 4, "method": "tasks/resubscribe",
               "params": {"id": "no-such-task"}}),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32001, "{missing}");
    assert!(missing["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("no task 'no-such-task'"));
}

#[tokio::test]
async fn a_peer_that_lost_its_ids_can_list_the_tasks_it_created() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-rpc-list").await;
    let client =
        A2aClient::new(Arc::new(InProcess { app: app.clone() })).with_bearer(token.clone());

    // Two goals, then the sending side disappears: what is left is the platform
    // and a peer that remembers no ids.
    let mut sent = Vec::new();
    for goal in ["probe list one", "probe list two"] {
        let accepted = client
            .send_message(ENDPOINT, goal)
            .await
            .expect("the endpoint accepts a message");
        read_until_present(&client, &accepted.id).await;
        sent.push(accepted.id);
    }

    let all = post_version(
        &app,
        &token,
        "1.0",
        json!({"jsonrpc": "2.0", "id": 1, "method": "ListTasks",
               "params": {"pageSize": 100}}),
    )
    .await;
    let tasks = all["result"]["tasks"]
        .as_array()
        .unwrap_or_else(|| panic!("ListTasks answers a page: {all}"));
    for id in &sent {
        assert!(
            tasks.iter().any(|task| task["id"] == json!(id)),
            "the page holds a task the peer created but can no longer name: {all}"
        );
    }
    assert_eq!(
        all["result"]["totalSize"],
        json!(tasks.len()),
        "a page big enough to hold everything says the whole size: {all}"
    );
    assert!(
        tasks[0]["artifacts"].as_array().is_none_or(Vec::is_empty),
        "the spec's default is to leave artifacts out: {all}"
    );

    // One result per page, and the cursor walks the same order the first page
    // named: paging is not a re-sort.
    let mut seen: Vec<String> = Vec::new();
    let mut token_from_page = String::new();
    loop {
        let mut params = json!({"pageSize": 1});
        if !token_from_page.is_empty() {
            params["pageToken"] = json!(token_from_page);
        }
        let page = post_version(
            &app,
            &token,
            "1.0",
            json!({"jsonrpc": "2.0", "id": 2, "method": "ListTasks", "params": params}),
        )
        .await;
        let rows = page["result"]["tasks"]
            .as_array()
            .unwrap_or_else(|| panic!("every page is a page: {page}"));
        assert!(rows.len() <= 1, "pageSize is a cap: {page}");
        for row in rows {
            seen.push(row["id"].as_str().expect("a task has an id").to_string());
        }
        token_from_page = page["result"]["nextPageToken"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if seen.len() > 32 {
            panic!("the cursor never reaches the end: {seen:?}");
        }
        if token_from_page.is_empty() {
            break;
        }
    }
    assert_eq!(
        seen,
        tasks
            .iter()
            .map(|task| task["id"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>(),
        "walking the cursor visits the same rows in the same order as one big page"
    );

    // Filters narrow; a filter nobody can satisfy reads as an empty page, not
    // as an error, because an empty answer is a legitimate answer.
    let working = post_version(
        &app,
        &token,
        "1.0",
        json!({"jsonrpc": "2.0", "id": 3, "method": "ListTasks",
               "params": {"status": "TASK_STATE_WORKING"}}),
    )
    .await;
    assert_eq!(working["result"]["totalSize"], json!(0), "{working}");

    let one_context = post_version(
        &app,
        &token,
        "1.0",
        json!({"jsonrpc": "2.0", "id": 4, "method": "ListTasks",
               "params": {"contextId": sent[0]}}),
    )
    .await;
    assert_eq!(
        one_context["result"]["totalSize"],
        json!(1),
        "a context is one task on this platform: {one_context}"
    );
    assert_eq!(one_context["result"]["tasks"][0]["id"], json!(sent[0]));

    // A value the endpoint does not understand is refused by name: silently
    // dropping it would answer "all of them" to a peer that asked for a subset.
    let misspelled = post_version(
        &app,
        &token,
        "1.0",
        json!({"jsonrpc": "2.0", "id": 5, "method": "ListTasks",
               "params": {"status": "busy"}}),
    )
    .await;
    assert_eq!(misspelled["error"]["code"], -32602, "{misspelled}");
    let bad_cursor = post_version(
        &app,
        &token,
        "1.0",
        json!({"jsonrpc": "2.0", "id": 6, "method": "ListTasks",
               "params": {"pageToken": "cursor-token"}}),
    )
    .await;
    assert_eq!(bad_cursor["error"]["code"], -32602, "{bad_cursor}");

    // The same rows are rendered in the version the caller speaks: 0.3 spells
    // states in lowercase, and its peers get that spelling even though only 1.0
    // defines this method.
    let v03 = post(
        &app,
        &token,
        json!({"jsonrpc": "2.0", "id": 7, "method": "ListTasks",
               "params": {"contextId": sent[1]}}),
    )
    .await;
    assert_eq!(
        v03["result"]["tasks"][0]["status"]["state"],
        json!("submitted"),
        "a 0.3 caller reads the 0.3 state spelling: {v03}"
    );
}

/// One HTTP round trip that declares the protocol version with the header the
/// endpoint reads, returning the JSON-RPC body.
async fn post_version(
    app: &axum::Router,
    token: &str,
    version: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/protocol/a2a")
        .header("Content-Type", "application/json")
        .header("A2A-Version", version)
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

/// Watches for the goal row and finishes it after `delay`.
///
/// The delay is the whole trick: with it, "the request waited for the task" and
/// "the request happened to read a row that was already finished" stop looking
/// alike. A non-blocking answer cannot come back completed, because nothing has
/// completed it yet when the answer leaves.
async fn finish_the_goal_after(
    orchestrator: Arc<dyn cog_core::OrchestratorControl>,
    delay: Duration,
    result: serde_json::Value,
) {
    let mut seen: Option<std::time::Instant> = None;
    loop {
        if let Some(task) = orchestrator
            .get_all_tasks()
            .await
            .into_iter()
            .find(|task| task.status == cog_core::TaskStatus::Pending)
        {
            let since = *seen.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= delay {
                let _ = orchestrator.schedule_task(&task.id).await;
                let _ = orchestrator.start_task(&task.id).await;
                let _ = orchestrator.complete_task(&task.id, result.clone()).await;
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 1.0's `SendMessage` waits for the task unless the caller asked otherwise,
/// and what it hands back is the finished row with its outcome.
///
/// This is not a style difference from 0.3: the same request promises different
/// things in the two versions. 0.3's `message/send` answers "accepted"; 1.0's
/// answers "done". A peer written to 1.0 takes the reply as the final answer, so
/// a `submitted` row handed to it is read as success -- worse than an error,
/// because nothing tells it to keep asking.
#[tokio::test]
async fn v10_send_waits_for_the_task_and_returns_its_outcome() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-v10-blocking").await;

    let worker = tokio::spawn(finish_the_goal_after(
        state.orchestrator.clone(),
        Duration::from_millis(300),
        json!({"summary": "finished while the peer waited"}),
    ));

    let answer = post_version(
        &app,
        &token,
        "1.0",
        json!({
            "jsonrpc": "2.0",
            "id": 21,
            "method": "message/send",
            "params": {"message": {"role": "user", "parts": [{"text": "v10 blocking probe"}]}},
        }),
    )
    .await;
    worker.await.expect("the worker finished the goal");

    let task = &answer["result"]["task"];
    assert_eq!(
        task["status"]["state"], "TASK_STATE_COMPLETED",
        "without `returnImmediately` a 1.0 SendMessage has to come back finished: {answer}"
    );
    let artifacts = task["artifacts"]
        .as_array()
        .expect("a finished task carries what it made");
    assert!(
        artifacts
            .iter()
            .any(|artifact| artifact["name"] == "result"),
        "the peer lives in another process and can only act on the artifact: {answer}"
    );
}

/// `configuration.returnImmediately: true` is the caller saying it will follow
/// up with `tasks/get`, so the reply is the accepted row and it comes back at
/// once -- not after the task has run.
#[tokio::test]
async fn v10_send_returns_the_accepted_row_when_the_caller_asks_to_not_wait() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-v10-immediate").await;

    // Nothing finishes this goal for long enough that a waiting implementation
    // could not possibly return the submitted row.
    let worker = tokio::spawn(finish_the_goal_after(
        state.orchestrator.clone(),
        Duration::from_secs(5),
        json!({"summary": "much later"}),
    ));

    let started = std::time::Instant::now();
    let answer = post_version(
        &app,
        &token,
        "1.0",
        json!({
            "jsonrpc": "2.0",
            "id": 22,
            "method": "message/send",
            "params": {
                "message": {"role": "user", "parts": [{"text": "v10 immediate probe"}]},
                "configuration": {"returnImmediately": true},
            },
        }),
    )
    .await;
    let elapsed = started.elapsed();
    worker.abort();

    assert_eq!(
        answer["result"]["task"]["status"]["state"], "TASK_STATE_SUBMITTED",
        "`returnImmediately` means the answer is the accepted row: {answer}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "asking not to wait and then waiting {elapsed:?} is the same as not honouring the flag"
    );
}

/// 0.3 has no `returnImmediately`: its `message/send` has always meant "accepted
/// and readable", and `tasks/get` is how the peer follows up.
#[tokio::test]
async fn v03_send_answers_the_accepted_row_and_does_not_wait() {
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "a2a-v03-no-wait").await;

    let worker = tokio::spawn(finish_the_goal_after(
        state.orchestrator.clone(),
        Duration::from_secs(5),
        json!({"summary": "much later"}),
    ));

    let started = std::time::Instant::now();
    let answer = post_version(
        &app,
        &token,
        "0.3",
        json!({
            "jsonrpc": "2.0",
            "id": 23,
            "method": "message/send",
            "params": {"message": {"role": "user", "kind": "message",
                                   "parts": [{"kind": "text", "text": "v03 probe"}]}},
        }),
    )
    .await;
    let elapsed = started.elapsed();
    worker.abort();

    assert_eq!(answer["result"]["kind"], "task");
    assert_eq!(answer["result"]["status"]["state"], "submitted");
    assert!(
        elapsed < Duration::from_secs(2),
        "0.3 never waits for the run: it waited {elapsed:?}"
    );
}
