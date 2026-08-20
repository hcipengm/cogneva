//! Cogneva binary entry point.
//! Delegates all work to `cogneva::run_app()`.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 依赖树里 ring 与 aws-lc-rs 同时存在，rustls 0.23 无法自动选定
    // CryptoProvider，首次 TLS 调用会 panic——必须在任何 TLS 使用之前安装。
    let _ = rustls::crypto::ring::default_provider().install_default();

    #[cfg(windows)]
    if std::env::args().any(|a| a == "--service") {
        return cogneva::windows_service::run();
    }
    // 独立安全网关模式（deploy/k3s/gateway-deployment.yaml 的启动命令）。
    if std::env::args().nth(1).as_deref() == Some("security-gateway") {
        return cog_gateway::security_gateway::run_from_env().await;
    }
    // 独立沙箱执行器模式（deploy/k3s/sandbox-executor-deployment.yaml 的启动命令）。
    if std::env::args().nth(1).as_deref() == Some("sandbox-executor") {
        return cog_extension::command_server::run_from_env().await;
    }
    // 启动前配置与依赖校验（审计 Phase 2 任务 2.5）。
    if std::env::args().nth(1).as_deref() == Some("validate-config") {
        return cogneva::validate_config::run().await;
    }
    cogneva::run_app().await
}
