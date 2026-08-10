use cog_core::HttpClient;
use cog_supervisor::{ControlPlaneClient, HttpControlPlaneClient, SupervisorStatus};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn http_client_posts_json_to_mock_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint = format!("http://127.0.0.1:{}/status", port);

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = socket.read(&mut buf).await.unwrap();
        buf.truncate(n);
        let req = String::from_utf8_lossy(&buf);

        assert!(
            req.contains("POST /status"),
            "expected POST /status in: {}",
            req
        );
        assert!(
            req.to_lowercase().contains("content-type:"),
            "expected content-type header in: {}",
            req
        );
        assert!(
            req.contains("\"cycle\":42"),
            "expected cycle:42 in body: {}",
            req
        );

        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        socket.write_all(response).await.unwrap();
    });

    let http: Arc<dyn HttpClient> = Arc::new(cog_net::ReqwestHttpClient::from_config(
        &cog_net::HttpClientConfig::default(),
    ));
    let client = HttpControlPlaneClient::new(endpoint).with_client(http);
    let status = SupervisorStatus {
        cycle: 42,
        healthy_agents: 5,
        dead_agents: 1,
        pending_handoffs: 2,
        last_rebalance: None,
        timestamp: chrono::Utc::now(),
    };

    let result = client.report_status(status).await;
    assert!(result.is_ok(), "expected Ok, got {:?}", result);

    server.await.unwrap();
}

#[tokio::test]
async fn http_client_returns_error_on_non_2xx() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint = format!("http://127.0.0.1:{}/status", port);

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = socket.peek(&mut buf).await.unwrap();
        let response = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    let http: Arc<dyn HttpClient> = Arc::new(cog_net::ReqwestHttpClient::from_config(
        &cog_net::HttpClientConfig::default(),
    ));
    let client = HttpControlPlaneClient::new(endpoint).with_client(http);
    let status = SupervisorStatus {
        cycle: 1,
        healthy_agents: 0,
        dead_agents: 0,
        pending_handoffs: 0,
        last_rebalance: None,
        timestamp: chrono::Utc::now(),
    };

    let result = client.report_status(status).await;
    assert!(result.is_err(), "expected Err on 500 response");

    server.await.unwrap();
}
