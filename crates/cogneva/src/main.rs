//! Cogneva binary entry point.
//! Parses the command line (`cogneva::cli`) and dispatches to one entry point.

use cogneva::cli::{Command, USAGE};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 依赖树里 ring 与 aws-lc-rs 同时存在，rustls 0.23 无法自动选定
    // CryptoProvider，首次 TLS 调用会 panic——必须在任何 TLS 使用之前安装。
    // install_default 仅在本进程已装过 provider 时返回 Err；那同样是可用的
    // provider，因此忽略结果，不阻断启动。
    let _ = rustls::crypto::ring::default_provider().install_default();

    // 构建标签加 git 修订，唯一标识本进程跑的代码状态：一条只告实例的
    // 告警（如 background_loop_stalled 按 loop 名、按 pod 各自判定）要能
    // 对回确切版本。走 stderr 而非 tracing：各命令路径的 subscriber 尚未
    // 装上，这里打 tracing 多半会被丢，而容器运行时收 stderr；stdout 上
    // 的 --help/--version/探针输出不变。
    eprintln!(
        "cogneva starting: version {} (rev {})",
        env!("COGNEVA_VERSION_ID"),
        env!("COGNEVA_GIT_REVISION")
    );

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
        //
        // 死因在交给 `main` 之前先写进容器终止消息：从这里往上，失败只剩 stdout 可走，
        // 而 stdout 随 Pod 一起消失（回滚时它被删，Loki 也没在收主应用的日志）。终止
        // 消息由 kubelet 收进 Pod 状态，滚动端判死那一刻正在读那一面，于是这一轮为什么
        // 没起来能一路走到滚动记录里。写不进去不影响这次失败本身的交付。
        Command::Run => match cogneva::run_app().await {
            Ok(()) => Ok(()),
            Err(e) => {
                cogneva::startup_guard::record_failure(&*e);
                Err(e)
            }
        },
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
        // 上游池在 GatewayConfig::from_env 内解析：无厂商兼容画像的上游
        // （detect_compat 查无此 host）启动即警告，兜底画像放行 newest-OpenAI
        // 字段会让不认这些字段的端点首字节前 400（ark.cn-beijing.volces.com
        // 实证），而池读数对此按设计不动，启动日志是早于告警的那条线索。
        Command::SecurityGateway => {
            // 出站客户端在这里造：装配层认实现（cog-net），安全网关只认
            // `cog_core::HttpClient` 契约。造在这里而不是网关里，是因为网关
            // 这一层没有理由知道本进程用的是哪个 HTTP 实现。
            let http_client: std::sync::Arc<dyn cog_core::HttpClient> = std::sync::Arc::new(
                cog_net::ReqwestHttpClient::from_config(&cog_net::HttpClientConfig::load()?)?,
            );
            // 跨进程池状态的键值面同理由这里造：独立网关模式没有插件上下文可
            // 消费，而"用哪个实现"是本进程的事实，不该由网关那一层去链一个驱动。
            let gateway_config = cog_gateway::security_gateway::SecurityGatewayConfig::from_env();
            let key_value = cogneva::assembly::infra::key_value_store_from_url(
                gateway_config.redis_url.as_deref(),
            );
            cog_gateway::security_gateway::run(
                gateway_config,
                Some(env!("COGNEVA_GIT_REVISION")),
                http_client,
                key_value,
            )
            .await
        }
        // 独立沙箱执行器模式（deploy/k3s/sandbox-executor-deployment.yaml 的启动命令）。
        Command::SandboxExecutor => cog_extension::command_server::run_from_env().await,
        // 卷占用走查：挂在只由第三方进程写入的卷上的边车，把该卷的盘上字节发布成
        // 卷族序列（deploy/k3s/cluster-registry.yaml 的 volume-walker 容器）。
        Command::VolumeWalker => cogneva::volume_walker::run_from_env().await,
        // 宿主账本的拆解面：每节点一个只读进程，把宿主 cgroup v2 的三个 slice 各按
        // anon / file 拆开（deploy/k3s/host-ledger-daemonset.yaml 的启动命令）。
        Command::HostLedger => cogneva::host_ledger::run_from_env().await,
        // 卷账的全集面：每节点一个进程，走查宿主存储根下的每个 claim 目录，把谁也
        // 没挂上的卷（数据库/缓存/队列/对象存储/监控自身的盘）也纳入账
        // （deploy/k3s/claim-store-daemonset.yaml 的启动命令）。
        Command::ClaimStore => cogneva::claim_store::run_from_env().await,
        // 启动前配置与依赖校验（审计 Phase 2 任务 2.5）。
        Command::ValidateConfig => cogneva::validate_config::run().await,
        // 主线跟踪自动部署的滚动端：独立 Job Pod 内执行，四部署门禁滚动，
        // 任一失败反向回滚 prev tag（进程本身跑在新镜像里，顺带 smoke test）。
        Command::MainlineRollout => cog_reflection::run_rollout_cli().await,
        // 撤回一条已交付的 rev：只写部署器的钉，改集群的仍是部署循环那一条路径。
        Command::MainlineUndeliver => cog_reflection::run_undeliver_cli().await,
        // 一条变更的一次执行：独立 Job Pod 内执行，apply → 验证 → 提交 → release
        // 构建 → 暂存二进制，判定写回共享卷上的结果文件，进程退出即结束。
        Command::ExecuteChange => cog_reflection::change_execution::run_execute_change_cli().await,
        // 数据面备份与恢复：CronJob 每日打包，换机/重装由一次性恢复 Job 消费。
        Command::Backup => cogneva::backup::run_backup_from_env().await,
        Command::Restore => cogneva::backup::run_restore_from_env().await,
        // 恢复演练：把最新一个包装进一个跑完迁移的空库，逐表对数、把账行与源库
        // 逐格比，然后删库。跑在独立的 CronJob 里，所以源库哪怕连不上也不影响
        // 主应用——但它必须连得上才谈得上演练。
        Command::BackupDrill => cogneva::backup::run_drill_from_env().await,
        // Torn AOF tail repair: runs in the redis Pod's init container, which is
        // what moves "a host that died uncleanly means truncating the AOF by
        // hand" onto the startup path.
        Command::RepairAof => cogneva::aof_repair::run_from_args(),
        // 主表对比实验：一条命令跑「数据根 × 基准 × 外壳 × 种子」，输出带 pins 的表。
        // 缺上游或缺平台时它照样出表，但把缺什么写进表里并以非零退出——「这一格是 0 分」
        // 与「这一列没跑成」在表里必须分得开。
        Command::EvalTable => cogneva::eval_table::run_from_args().await,
        Command::EvalCaseSubset => cogneva::eval_subset::run_from_args(),
        // 把本机的工具集按 MCP 协议交给一个外部 MCP 客户端：这条传输由客户端拉起
        // 本进程，标准输入输出就是协议本身，所以这条路上不许有别的输出。
        Command::McpServe => cogneva::mcp_serve::run_from_env().await,
        // 带外写者问一次的入口。退出码**就是**答案（0 拿到 / 1 不该写 / 2 没问成 /
        // 3 仲裁读不到 / 4 池读不到），所以这里显式 exit：走 `Err` 会被本函数末尾的
        // 错误路径统一折成 1，「不该写」与「这条命令失败了」就再也分不开。
        Command::WriteRole => std::process::exit(cogneva::write_role::run().await),
        // 面图的只读面（以及一次性导入）：与 write-role 同样从应用自己的配置读两端，
        // 因为调用方可能是没有平台令牌的宿主进程。
        Command::Governance => cogneva::governance::run_from_args().await,
        // 带外 worker 的任务面：读自己名下的任务、按租约开工、把结果发回同一条
        // results 流，完成与依赖调度仍由编排器那一份实现执行。
        Command::Work => cogneva::work::run_from_args().await,
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
