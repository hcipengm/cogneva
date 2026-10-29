//! Uploading a file goes through the catalog, and the catalog answers what it
//! was told.
//!
//! Two defects meet here. The upload route had no object store behind it at all
//! -- the state slot was never filled, so the route answered 503 on every
//! deployment -- and the store it would have used keeps bytes nothing can name:
//! after the response, no record said what the bucket held, how large it was,
//! or whether the bytes were still the ones written. A catalog row per upload is
//! what makes the second half answerable, and these cases go through the router
//! because a catalog nothing writes to is a table with a promise in it.
//!
//! The stores are stand-ins: bytes in a map, rows in a vector. The questions
//! here are whether the route reaches them and what a caller sees -- the
//! durability of either store is `cog-storage`'s own suites' business.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{ObjectBackend, SFError, SFResult};
use cog_gateway::{create_router, GatewayState};
use cog_storage::{
    sha256_of, DatasetCatalog, DatasetEntry, DatasetStore, DatasetWrite, NewDataset,
    ObjectDatasetStore,
};

mod router_state;

/// Bytes in a map, keyed exactly as the caller named them.
#[derive(Default)]
struct MapObjects {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl ObjectBackend for MapObjects {
    async fn put(&self, key: &str, data: &[u8]) -> SFResult<String> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(format!("map://{key}"))
    }
    async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    async fn delete(&self, key: &str) -> SFResult<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
    async fn presign_url(&self, key: &str, _expiry_secs: u64) -> SFResult<String> {
        Ok(format!("map://{key}"))
    }
    async fn exists(&self, key: &str) -> SFResult<bool> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }
    async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>> {
        let prefix = prefix.unwrap_or_default();
        Ok(self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

/// Rows in a vector, refusing the one thing a catalog may never allow: two
/// contents under one version.
#[derive(Default)]
struct VecCatalog {
    rows: Mutex<Vec<DatasetEntry>>,
}

#[async_trait]
impl DatasetCatalog for VecCatalog {
    async fn register(&self, entry: NewDataset) -> SFResult<DatasetEntry> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(held) = rows
            .iter()
            .find(|r| r.name == entry.name && r.version == entry.version)
        {
            if held.content_sha256 != entry.content_sha256 {
                return Err(SFError::Conflict(
                    "different bytes under one version".into(),
                ));
            }
            return Ok(held.clone());
        }
        let row = DatasetEntry {
            name: entry.name,
            version: entry.version,
            object_key: entry.object_key,
            content_sha256: entry.content_sha256,
            bytes: entry.bytes,
            row_count: entry.row_count,
            coverage_from: entry.coverage_from,
            coverage_to: entry.coverage_to,
            source: entry.source,
            media_type: entry.media_type,
            created_at: chrono::Utc::now(),
        };
        rows.push(row.clone());
        Ok(row)
    }

    async fn find(&self, name: &str, version: &str) -> SFResult<Option<DatasetEntry>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.name == name && r.version == version)
            .cloned())
    }

    async fn latest(&self, name: &str) -> SFResult<Option<DatasetEntry>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|r| r.name == name)
            .cloned())
    }

    async fn list(&self, name: &str, limit: usize) -> SFResult<Vec<DatasetEntry>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.name == name)
            .rev()
            .take(limit)
            .cloned()
            .collect())
    }

    async fn all(&self, limit: usize) -> SFResult<Vec<DatasetEntry>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect())
    }
}

struct Fixture {
    state: Arc<GatewayState>,
    objects: Arc<MapObjects>,
}

fn fixture(with_catalog: bool) -> Fixture {
    let objects = Arc::new(MapObjects::default());
    let catalog = Arc::new(VecCatalog::default());
    let store: Option<Arc<dyn DatasetStore>> = if with_catalog {
        Some(Arc::new(ObjectDatasetStore::new(
            objects.clone(),
            catalog.clone(),
        )))
    } else {
        None
    };
    Fixture {
        state: router_state::stateless_with_datasets(store),
        objects,
    }
}

async fn bearer(state: &Arc<GatewayState>) -> String {
    router_state::admin_bearer(state, "dataset-route-probe").await
}

/// One multipart upload body, the way a browser sends a single file.
fn multipart(file_name: &str, content_type: &str, bytes: &[u8]) -> (String, Vec<u8>) {
    let boundary = "dataset-route-probe-boundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; \
             filename=\"{file_name}\"\r\nContent-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

async fn ask(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("Content-Type", content_type)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

async fn upload(fixture: &Fixture, name: &str, body: &[u8]) -> (StatusCode, serde_json::Value) {
    let app = create_router(fixture.state.clone());
    let token = bearer(&fixture.state).await;
    let (content_type, payload) = multipart(name, "text/plain", body);
    ask(
        &app,
        "POST",
        "/api/v1/files/upload",
        &token,
        &content_type,
        payload,
    )
    .await
}

#[tokio::test]
async fn an_upload_lands_in_the_catalog_and_answers_with_its_digest() {
    let fixture = fixture(true);
    let bytes: &[u8] = b"the first field note";
    let (status, response) = upload(&fixture, "notes.txt", bytes).await;
    assert_eq!(status, StatusCode::CREATED, "{response}");
    let data = &response["data"];
    assert_eq!(data["file_name"], "notes.txt");
    assert_eq!(data["size"], bytes.len());
    assert_eq!(data["content_sha256"], sha256_of(bytes));
    let version = data["file_id"]
        .as_str()
        .expect("the answer names the id")
        .to_string();
    assert_eq!(data["dataset"]["version"], version);
    assert_eq!(data["dataset"]["name"], "notes.txt");

    // 字节真的在桶里，而且回答里的 uri 就指向它们。
    let key = data["uri"].as_str().expect("the answer names the object");
    assert_eq!(
        fixture
            .objects
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(Vec::as_slice),
        Some(bytes),
        "回答说存了，桶里却没有这串字节"
    );

    // 目录行读得到，且记的是同一串字节的摘要。
    let app = create_router(fixture.state.clone());
    let token = bearer(&fixture.state).await;
    let (status, row) = ask(
        &app,
        "GET",
        &format!("/api/v1/datasets/notes.txt/{version}"),
        &token,
        "application/json",
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{row}");
    assert_eq!(row["data"]["content_sha256"], sha256_of(bytes));
    assert_eq!(row["data"]["bytes"], bytes.len());
    assert_eq!(row["data"]["source"], "upload");
    // 一个非表格文件没有行数，也没有覆盖区间：这两格必须是 null，不是 0。
    assert!(row["data"]["row_count"].is_null(), "{row}");
    assert!(row["data"]["coverage_from"].is_null(), "{row}");
}

#[tokio::test]
async fn two_versions_of_one_name_list_newest_first() {
    let fixture = fixture(true);
    let (first_status, first) = upload(&fixture, "notes.txt", b"first edition").await;
    assert_eq!(first_status, StatusCode::CREATED, "{first}");
    let (second_status, second) = upload(&fixture, "notes.txt", b"second edition").await;
    assert_eq!(second_status, StatusCode::CREATED, "{second}");
    let newest = second["data"]["file_id"].as_str().unwrap().to_string();

    let app = create_router(fixture.state.clone());
    let token = bearer(&fixture.state).await;
    let (status, listing) = ask(
        &app,
        "GET",
        "/api/v1/datasets/notes.txt?limit=10",
        &token,
        "application/json",
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listing}");
    assert_eq!(listing["data"]["count"], 2);
    assert_eq!(
        listing["data"]["versions"][0]["version"], newest,
        "最新的版本不在第一行：{listing}"
    );
    assert_eq!(
        listing["data"]["versions"][0]["content_sha256"],
        sha256_of(b"second edition")
    );
    assert_eq!(
        listing["data"]["versions"][1]["content_sha256"],
        sha256_of(b"first edition")
    );
}

#[tokio::test]
async fn a_deployment_without_a_catalog_refuses_the_upload_rather_than_storing_unnamed_bytes() {
    let fixture = fixture(false);
    let (status, response) = upload(&fixture, "notes.txt", b"bytes").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{response}");
    assert_eq!(response["error"], "dataset_catalog_not_configured");
    assert!(
        fixture.objects.objects.lock().unwrap().is_empty(),
        "没有目录可写的时候还是把字节存进去了：那就是一份谁也点不到名的数据"
    );
}

#[tokio::test]
async fn a_version_the_catalog_does_not_hold_answers_not_found() {
    let fixture = fixture(true);
    let app = create_router(fixture.state.clone());
    let token = bearer(&fixture.state).await;
    let (status, response) = ask(
        &app,
        "GET",
        "/api/v1/datasets/notes.txt/never-uploaded",
        &token,
        "application/json",
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
    assert_eq!(response["error"], "dataset_version_not_found");
}

#[tokio::test]
async fn the_catalog_routes_refuse_when_the_deployment_has_no_catalog() {
    // The plain fixture, not one with an absent store: a deployment that never
    // had a catalog configured is the case being judged.
    let state = router_state::stateless();
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "dataset-route-probe").await;
    for path in [
        "/api/v1/datasets/notes.txt",
        "/api/v1/datasets/notes.txt/v1",
    ] {
        let (status, response) =
            ask(&app, "GET", path, &token, "application/json", Vec::new()).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{path} 在没有目录的部署上给了 {status}: {response}"
        );
        assert_eq!(
            response["error"], "dataset_catalog_not_configured",
            "{path}"
        );
    }
}

/// The store is reachable through the write side too: a producer that is not the
/// upload route gets the same guarantee, so the invariant lives in one place.
#[tokio::test]
async fn a_producer_writing_through_the_store_gets_a_catalogued_version() {
    let objects = Arc::new(MapObjects::default());
    let catalog = Arc::new(VecCatalog::default());
    let store = ObjectDatasetStore::new(objects.clone(), catalog.clone());
    let entry = store
        .put(
            DatasetWrite {
                name: "papers".into(),
                version: "2026-10".into(),
                source: "crawler:arxiv".into(),
                row_count: Some(42),
                coverage: None,
                media_type: Some("application/pdf".into()),
            },
            b"%PDF-1.7",
        )
        .await
        .expect("put");
    assert_eq!(entry.row_count, Some(42));
    assert_eq!(entry.content_sha256, sha256_of(b"%PDF-1.7"));
    assert!(catalog
        .find("papers", "2026-10")
        .await
        .expect("find")
        .is_some());
    assert_eq!(objects.objects.lock().unwrap().len(), 1);
}

/// The migration gate: after a restore, "the process is up" is not the answer,
/// and this route is where the answer comes from.
#[tokio::test]
async fn a_verify_pass_answers_for_every_catalogued_object() {
    let fixture = fixture(true);
    let (first, _) = upload(&fixture, "notes.txt", b"first edition").await;
    assert_eq!(first, StatusCode::CREATED);
    let (second, _) = upload(&fixture, "notes.txt", b"second edition").await;
    assert_eq!(second, StatusCode::CREATED);

    let app = create_router(fixture.state.clone());
    let token = bearer(&fixture.state).await;
    let (status, report) = ask(
        &app,
        "POST",
        "/api/v1/datasets/verify",
        &token,
        "application/json",
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["data"]["checked"], 2);
    assert_eq!(report["data"]["verified"], 2);
    assert_eq!(report["data"]["failed"], 0);
    assert_eq!(report["data"]["all_verified"], true);
}

#[tokio::test]
async fn an_object_that_changed_after_it_was_catalogued_fails_the_pass() {
    let fixture = fixture(true);
    let (status, uploaded) = upload(&fixture, "notes.txt", b"first night").await;
    assert_eq!(status, StatusCode::CREATED, "{uploaded}");
    let key = uploaded["data"]["uri"].as_str().expect("the object key");
    // 同样的长度、不同的字节：字节数那一关过得了，只有摘要抓得住。
    fixture
        .objects
        .objects
        .lock()
        .unwrap()
        .insert(key.to_string(), b"FIRST night".to_vec());

    let app = create_router(fixture.state.clone());
    let token = bearer(&fixture.state).await;
    let (status, report) = ask(
        &app,
        "POST",
        "/api/v1/datasets/verify",
        &token,
        "application/json",
        Vec::new(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "校验没过却回了成功状态码：{report}"
    );
    assert_eq!(report["data"]["failed"], 1);
    assert_eq!(report["data"]["all_verified"], false);
    assert_eq!(report["data"]["checks"][0]["outcome"], "mismatched");
    assert_eq!(
        report["data"]["checks"][0]["recorded_bytes"], report["data"]["checks"][0]["actual_bytes"],
        "这次坏的不是长度"
    );
}

/// A catalog with nothing in it has verified nothing, and that is not a pass:
/// it is the exact shape a migration that carried no data would have.
#[tokio::test]
async fn an_empty_catalog_does_not_verify() {
    let fixture = fixture(true);
    let app = create_router(fixture.state.clone());
    let token = bearer(&fixture.state).await;
    let (status, report) = ask(
        &app,
        "POST",
        "/api/v1/datasets/verify",
        &token,
        "application/json",
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{report}");
    assert_eq!(report["data"]["checked"], 0);
    assert_eq!(report["data"]["all_verified"], false);
}
