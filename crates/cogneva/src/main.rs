//! Cogneva binary entry point.
//! Delegates all work to `cogneva::run_app()`.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(windows)]
    if std::env::args().any(|a| a == "--service") {
        return cogneva::windows_service::run();
    }
    // 独立安全网关模式（deploy/k3s/gateway-deployment.yaml 的启动命令）。
    if std::env::args().nth(1).as_deref() == Some("security-gateway") {
        return cog_gateway::security_gateway::run_from_env().await;
    }
    // 启动前配置与依赖校验（审计 Phase 2 任务 2.5）。
    if std::env::args().nth(1).as_deref() == Some("validate-config") {
        return cogneva::validate_config::run().await;
    }
    cogneva::run_app().await
}
