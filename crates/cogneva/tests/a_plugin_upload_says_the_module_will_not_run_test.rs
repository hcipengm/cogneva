//! An upload that cannot run says so.
//!
//! This build has no runtime that instantiates a WASM module, so nothing an
//! upload hands over can be loaded. The route used to answer `200` with
//! `"status": "loaded"`, and the unload route `200` with `"unloaded"`: two
//! statements about state this deployment never enters. A caller reading them
//! would conclude a module was installed and then removed, when it was never
//! held.
//!
//! The cases go through `create_router` because that is where the claim lived;
//! a unit test of a handler nothing routes would stay green either way. What the
//! answers must not do is name a load or an unload -- neither a success code nor
//! the word may come back. Which refusal code each route answers with is left
//! free; the refusal is not.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{PluginManifest, PluginRegistry, SFResult};
use cog_gateway::{create_router, GatewayState};

use crate::router_state;

/// A registry that hands back the bytes it was given, and nothing else.
///
/// The upload route's other steps -- parsing the manifest, compiling the module
/// -- are the route's own work; this stand-in exists so the bytes that reach the
/// compiler are the test's, not a file's.
struct OneEntry(Vec<u8>);

#[async_trait::async_trait]
impl PluginRegistry for OneEntry {
    async fn discover(&self, _source: &str) -> SFResult<Vec<PluginManifest>> {
        Ok(Vec::new())
    }

    async fn fetch(&self, _manifest: &PluginManifest) -> SFResult<Vec<u8>> {
        Ok(self.0.clone())
    }

    async fn fetch_by_id(&self, _plugin_id: &str) -> SFResult<Vec<u8>> {
        Ok(self.0.clone())
    }
}

/// A state with the two components the upload route asks for, and a module that
/// compiles: the smallest empty WASM module is a legal one.
fn state() -> Arc<GatewayState> {
    router_state::with_plugin_pipeline(
        Arc::new(OneEntry(b"\0asm\x01\0\0\0".to_vec())),
        Arc::new(cog_extension::WasmRuntime::new()),
    )
}

fn manifest() -> String {
    serde_json::json!({
        "name": "demo",
        "version": "0.1.0",
        "author": "test",
        "capabilities": [],
        "entry": "plugin.wasm",
        "signature": "00:00",
        "sandbox": null,
    })
    .to_string()
}

async fn ask(
    state: &Arc<GatewayState>,
    method: &str,
    path: &str,
    body: String,
) -> (StatusCode, String) {
    let token = router_state::admin_bearer(state, "plugin-route-probe").await;
    let response = create_router(state.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(body))
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// A module that compiles is still not a module that runs. The answer may not
/// take the refusal for a success, and may not use the word the old reply used.
#[tokio::test]
async fn an_upload_says_the_module_will_not_run() {
    let state = state();
    let (status, body) = ask(&state, "POST", "/api/v1/plugins", manifest()).await;

    assert!(
        !status.is_success(),
        "an upload that loads nothing may not answer success: {status} {body}"
    );
    assert!(
        !body.contains("loaded"),
        "the reply may not say a module was loaded: {body}"
    );
    assert!(
        body.contains("不会运行") || body.contains("没有运行"),
        "the refusal has to say the module does not run: {body}"
    );
}

/// Nothing was loaded, so nothing is unloaded. Reporting the opposite is the
/// same lie one route over.
#[tokio::test]
async fn an_unload_does_not_report_an_unload() {
    let state = state();
    let (status, body) = ask(&state, "DELETE", "/api/v1/plugins/demo", String::new()).await;

    assert!(
        !status.is_success(),
        "an unload of nothing may not answer success: {status} {body}"
    );
    assert!(
        !body.contains("unloaded"),
        "the reply may not say a plugin was unloaded: {body}"
    );
}
