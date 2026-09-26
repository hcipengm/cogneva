//! Cogneva binary entry point.
//! Parses the command line (`cogneva::cli`) and dispatches to one entry point.

use cogneva::cli::{Command, USAGE};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 依赖树里 ring 与 aws-lc-rs 同时存在，rustls 0.23 无法自动选定
    // CryptoProvider，首次 TLS 调用会 panic——必须在任何 TLS 使用之前安装。
    let _ = rustls::crypto::ring::default_provider().install_default();

    // 容器入口即 PID 1，孤儿进程只会被落到这里；这是唯一能收集它们的进程
    // （git 为本地远端起的 sh -c git-upload-pack 就是这类孤儿）。子命令无关：
    // 三个常驻模式都以 PID 1 身份跑，非 PID 1 时该函数直接返回。
    cog_observability::process_zombies::start_orphan_reaper();

    let command = match Command::parse(std::env::args().skip(1)) {
        Ok(command) => command,
        Err(unknown) => {
            // Anything we do not recognize must stop here. Falling through used to
            // start the whole application, so `--help` answered with a plugin
            // initialization error and `--health-check` with a port conflict.
            eprintln!("cogneva: unrecognized argument: {unknown}");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };

    match command {
        // 完整应用（无参数时的默认行为）。
        Command::Run => cogneva::run_app().await,
        Command::Help => {
            println!("{USAGE}");
            Ok(())
        }
        Command::Version => {
            // The build label, not the declared version: the declared version is
            // shared by every commit since the last release, so printing it here
            // makes two different code states answer to one name.
            println!(
                "cogneva {} (rev {})",
                env!("COGNEVA_VERSION_ID"),
                env!("COGNEVA_GIT_REVISION")
            );
            Ok(())
        }
        // 镜像 HEALTHCHECK 调用的探针：只探本进程的 HTTP 端口，不启动应用。
        Command::HealthCheck => cogneva::health_check::run(),
        // 独立安全网关模式（deploy/k3s/gateway-deployment.yaml 的启动命令）。
        Command::SecurityGateway => {
            cog_gateway::security_gateway::run_from_env(Some(env!("COGNEVA_GIT_REVISION"))).await
        }
        // 独立沙箱执行器模式（deploy/k3s/sandbox-executor-deployment.yaml 的启动命令）。
        Command::SandboxExecutor => cog_extension::command_server::run_from_env().await,
        // 启动前配置与依赖校验（审计 Phase 2 任务 2.5）。
        Command::ValidateConfig => cogneva::validate_config::run().await,
        // 主线跟踪自动部署的滚动端：独立 Job Pod 内执行，四部署门禁滚动，
        // 任一失败反向回滚 prev tag（进程本身跑在新镜像里，顺带 smoke test）。
        Command::MainlineRollout => cog_reflection::run_rollout_cli().await,
        // 数据面备份与恢复：CronJob 每日打包，换机/重装由一次性恢复 Job 消费。
        Command::Backup => cogneva::backup::run_backup_from_env().await,
        Command::Restore => cogneva::backup::run_restore_from_env().await,
        // Torn AOF tail repair: runs in the redis Pod's init container, which is
        // what moves "a host that died uncleanly means truncating the AOF by
        // hand" onto the startup path.
        Command::RepairAof => cogneva::aof_repair::run_from_args(),
        Command::WindowsService => {
            #[cfg(windows)]
            {
                cogneva::windows_service::run()
            }
            #[cfg(not(windows))]
            {
                Err("--service is only supported on Windows".into())
            }
        }
    }
}
