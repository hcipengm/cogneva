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

/// 收下 warn 级事件的正文，供断言「这一件事在屏幕上说出来了没有」。
///
/// 用法：`let warns = WarnText::default(); let _guard =
/// tracing::subscriber::set_default(tracing_subscriber::registry().with(warns.clone()));`
/// ——`set_default` 是线程局部的，`#[tokio::test]` 默认的 current-thread runtime
/// 不换线程，所以 guard 活着的整段 `.await` 都在收。
#[derive(Clone, Default)]
pub(crate) struct WarnText(pub(crate) std::sync::Arc<std::sync::Mutex<Vec<String>>>);

struct MessageField(String);

impl tracing::field::Visit for MessageField {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnText {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut field = MessageField(String::new());
        event.record(&mut field);
        self.0.lock().unwrap().push(field.0);
    }
}

impl WarnText {
    pub(crate) fn said(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

/// 一个只答一句话的 HTTP 端点：给定的状态码、正文 `{}`，答完挂断。
///
/// 自报那两条循环真正做的事是「发出去、看回话」，而回话是什么正是要测的那一格。
/// 用一个真的 socket 而不是桩客户端：状态码要经过客户端那一整段才算数。
pub(crate) async fn serve_status(status: u16) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut chunk = [0u8; 512];
                loop {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&chunk[..n]),
                    }
                    if head.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let body = "{}";
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                cog_core::ignore_lost!(socket.write_all(response.as_bytes()).await);
                cog_core::ignore_lost!(socket.shutdown().await);
            });
        }
    });
    (format!("http://{addr}"), handle)
}
