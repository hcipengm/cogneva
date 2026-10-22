//! 通知出口能不能把报文真的送到一个 socket 上。
//!
//! `cog-notification` 的单测把 `HttpClient` 换成替身，替身回答的是"我收到了一个
//! 请求对象"，判的是 dispatcher 里的判断逻辑（什么时候算送达、哪种失败记哪一格）。
//! 从替身到真客户端那一跳——解析地址、建连接、写报文、读回状态码——没有一条测试
//! 走过，而它恰好是"出口没接线"这一族缺陷藏身的地方：逻辑全对、报文从来没出门。
//!
//! 现网也读不到这一跳：四个通知地址都空着，`delivery_total` 的增量因此是 0，而
//! 0 既可能是"没人配地址"（合法状态）也可能是"配了也发不出去"（缺陷）。本文件
//! 把后一半钉死：地址给出来的时候，报文必须出现在 socket 上，且读数记成 `ok`；
//! 地址连不上时，读数记成 `unreachable` 并且错误**返回**给调用方——告警规则正是
//! 按这一格起响的，它记不出来就等于没有告警面。
//!
//! 这里用的是真的 `ReqwestHttpClient`（与 `NetPlugin` 发布给全进程的是同一个类型），
//! 只把接收方换成本机的监听端口：被测的是我们这一侧的发送路径，接收方是不是真的
//! 机器人无关紧要。

use std::time::Duration;

use chrono::Utc;
use cog_core::{Notification, NotificationDispatcher, NotificationType};
use cog_net::ReqwestHttpClient;
use cog_notification::delivery::DeliveryResult;
use cog_notification::plugin::OUTLET_WEBHOOK;
use cog_notification::WebhookDispatcher;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn notification(id: &str) -> Notification {
    Notification {
        id: id.into(),
        user_id: "00000000-0000-4000-8000-0000000000ff".into(),
        ntype: NotificationType::System,
        title: format!("Title {id}"),
        body: format!("Body {id}"),
        is_read: false,
        created_at: Utc::now(),
        read_at: None,
    }
}

fn counter(result: DeliveryResult) -> u64 {
    cog_notification::delivery::registry().count(OUTLET_WEBHOOK, result)
}

fn http_client() -> std::sync::Arc<dyn cog_core::HttpClient> {
    std::sync::Arc::new(ReqwestHttpClient::new(reqwest::Client::new()))
}

/// 一次请求的头部结束位置。
fn head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn content_length(head: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(head).to_ascii_lowercase();
    text.lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse().ok())
}

/// 一个只收一次请求的本机接收方，返回它收到的原始字节。
///
/// 收满「头部 + 声明长度的报文」就答复，不等对端关连接：客户端用 keep-alive，
/// 等 EOF 会把测试挂在这里。
async fn sink() -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local sink");
    let addr = listener.local_addr().expect("sink address");
    let received = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept the delivery");
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = socket.read(&mut chunk).await.expect("read the delivery");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(end) = head_end(&buf) {
                let want = content_length(&buf[..end]).unwrap_or(0);
                if buf.len() >= end + want {
                    break;
                }
            }
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .expect("answer the delivery");
        socket.flush().await.expect("flush the answer");
        String::from_utf8_lossy(&buf).to_string()
    });
    (format!("http://{addr}/hook"), received)
}

#[tokio::test]
async fn a_configured_webhook_address_receives_the_notification_and_reads_as_ok() {
    let (url, received) = sink().await;
    let dispatcher = WebhookDispatcher::new(http_client(), url, OUTLET_WEBHOOK);
    let ok_before = counter(DeliveryResult::Ok);

    let id = "delivery-over-a-socket";
    dispatcher
        .dispatch(&notification(id))
        .await
        .expect("a 2xx sink is a delivered notification");

    let request = tokio::time::timeout(Duration::from_secs(10), received)
        .await
        .expect("the sink saw the request")
        .expect("the sink task finished");
    assert!(
        request.starts_with("POST /hook HTTP/1.1"),
        "the outlet must post to the path it was given, got: {}",
        request.lines().next().unwrap_or_default()
    );
    assert!(
        request.contains(id),
        "the receiver must get the notification itself, not an empty body: {request}"
    );
    assert_eq!(
        counter(DeliveryResult::Ok),
        ok_before + 1,
        "a delivery that reached the receiver has to be counted once as ok -- the \
         reading is what a rule fires on, and a delivery nobody counts is invisible \
         exactly when it starts failing"
    );
}

#[tokio::test]
async fn an_address_nobody_listens_on_returns_an_error_and_reads_as_unreachable() {
    // 取一个此刻确实没人监听的端口：绑上拿到号，立刻放掉。
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve a port");
        listener.local_addr().expect("reserved address")
    };
    let dispatcher =
        WebhookDispatcher::new(http_client(), format!("http://{addr}/hook"), OUTLET_WEBHOOK);
    let unreachable_before = counter(DeliveryResult::Unreachable);

    let outcome = dispatcher.dispatch(&notification("nobody-listens")).await;

    assert!(
        outcome.is_err(),
        "a connection that never opened must not be reported as delivered -- the \
         caller is the only place that can still tell a human"
    );
    assert_eq!(
        counter(DeliveryResult::Unreachable),
        unreachable_before + 1,
        "the failing delivery has to land in the unreachable cell, which is what the \
         notification_delivery_failing rule selects on"
    );
}
