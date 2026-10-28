//! 单测共用的样例：真的走一遍 HTTP，读回状态码与正文。
//!
//! 它不是任何一处的题目，是每个「这一行指标能不能读出来」的单测都要先做的事。
//! 三个观测器各自写了一遍同一个 `scrape`——读面的地址、上限、正文怎么解，改到
//! 哪一份、漏了哪一份，读起来全靠记性，而三份都还是绿的。

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use tower::ServiceExt;

/// 请求 `app` 的 `path`，读回 `(状态码, 正文)`。
///
/// 走的是真的路由器而不是直接调处理函数：读的判据是「一个 scrape 能拿到什么」，
/// 路由挂没挂、路径对不对，都只有从这里才看得见。正文默认最多读到 1 MiB——超过这个
/// 量的正文本身就说明这一面在无限长，而不是这一条断言写错了。
pub(crate) async fn scrape(app: Router, path: &str) -> (StatusCode, String) {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}
