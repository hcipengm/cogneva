//! `cogneva validate-config` — 启动前校验配置与依赖（审计 Phase 2 任务 2.5）。
//!
//! 加载完整 5 层配置栈后执行结构与依赖检查，按 ✅/⚠️/❌ 输出报告；
//! 存在 ❌ 时进程以非零码退出，可用于 CI / 部署前置检查。

use crate::config_loader;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Level {
    Ok,
    Warn,
    Error,
}

struct Report {
    entries: Vec<(Level, String)>,
}

impl Report {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    fn ok(&mut self, msg: impl Into<String>) {
        self.entries.push((Level::Ok, msg.into()));
    }
    fn warn(&mut self, msg: impl Into<String>) {
        self.entries.push((Level::Warn, msg.into()));
    }
    fn error(&mut self, msg: impl Into<String>) {
        self.entries.push((Level::Error, msg.into()));
    }
    fn has_errors(&self) -> bool {
        self.entries.iter().any(|(l, _)| *l == Level::Error)
    }
    fn print(&self) {
        for (level, msg) in &self.entries {
            let mark = match level {
                Level::Ok => "✅",
                Level::Warn => "⚠️",
                Level::Error => "❌",
            };
            println!("{mark} {msg}");
        }
    }
}

/// Run all validation checks. Returns process exit code (0 = pass, 1 = errors).
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut report = Report::new();

    // 1. 配置文件可发现性
    let config_path = std::env::var("COGNEVA_CONFIG_PATH")
        .unwrap_or_else(|_| crate::config_loader::DEFAULT_CONFIG_PATH.into());
    if std::path::Path::new(&config_path).exists() {
        report.ok(format!("config file found: {config_path}"));
    } else {
        report.warn(format!(
            "config file not found: {config_path} (falling back to defaults + env)"
        ));
    }

    // 2. 加载 5 层配置栈
    let mut config = config_loader::load();
    report.ok("configuration loaded (5-layer stack: defaults → file → env file → env vars)");

    // 2.1 解析 secret:// 引用，确保 api_key 等字段可真实落地
    match config_loader::resolve_secret_refs(&mut config).await {
        Ok(()) => report.ok("secret:// references resolved (env/file/vault)"),
        Err(e) => report.error(format!("secret:// reference resolution failed: {e}")),
    }

    // 3. LLM 依赖：优先校验 llm_routing.backends（多后端故障转移），
    // 无 backends 时回退校验 legacy llm 单后端配置。
    // llm_routing 已下沉 cog-llm，经其自载器读取。
    let llm_routing = match cog_llm::LLMRoutingConfig::load() {
        Ok(r) => r,
        Err(e) => {
            report.error(format!("llm_routing load failed: {e}"));
            cog_llm::LLMRoutingConfig::default()
        }
    };
    if llm_routing.backends.is_empty() {
        if config.llm.provider.trim().is_empty() {
            report.error("llm.provider is empty — no LLM provider configured");
        } else {
            report.ok(format!("llm.provider = {}", config.llm.provider));
        }
        if config.llm.model.trim().is_empty() {
            report.error("llm.model is empty");
        } else {
            report.ok(format!("llm.model = {}", config.llm.model));
        }
        if config.llm.api_key.trim().is_empty()
            && std::env::var("COGNEVA_LLM_API_KEY")
                .unwrap_or_default()
                .is_empty()
        {
            report.error("llm.api_key is empty and COGNEVA_LLM_API_KEY is not set");
        } else {
            report.ok("llm.api_key resolved");
        }
        if config.llm.max_tokens == 0 {
            report.error("llm.max_tokens must be > 0");
        }
        if config.llm.timeout_secs == 0 {
            report.error("llm.timeout_secs must be > 0");
        }
    } else {
        let enabled: Vec<_> = llm_routing.backends.iter().filter(|b| b.enabled).collect();
        if enabled.is_empty() {
            report.error("llm_routing.backends has no enabled backend");
        }
        for backend in &enabled {
            if backend.provider.trim().is_empty() || backend.model.trim().is_empty() {
                report.error(format!(
                    "llm_routing backend has empty provider/model: {:?}",
                    backend
                ));
                continue;
            }
            if backend.api_key.trim().is_empty()
                && std::env::var("COGNEVA_LLM_API_KEY")
                    .unwrap_or_default()
                    .is_empty()
            {
                report.error(format!(
                    "llm_routing backend {} has empty api_key",
                    backend.provider
                ));
            } else {
                report.ok(format!(
                    "llm_routing backend {} ({}) api_key resolved",
                    backend.provider, backend.model
                ));
            }
        }
    }

    // 4. Gateway 参数
    if config.gateway.http_port == 0 {
        report.error("gateway.http_port must be > 0");
    } else {
        report.ok(format!("gateway.http_port = {}", config.gateway.http_port));
    }

    // 5. 自进化流水线
    let se = &config.self_evolution;
    if se.enabled {
        for (name, value) in [
            ("patch_dir", &se.patch_dir),
            ("binary_dir", &se.binary_dir),
            ("backup_dir", &se.backup_dir),
        ] {
            if value.trim().is_empty() {
                report.error(format!("self_evolution.{name} is empty while enabled=true"));
            }
        }
        if se.test_timeout_secs == 0 || se.build_timeout_secs == 0 {
            report.error("self_evolution timeouts must be > 0");
        }
        if se.manual_approve {
            report.ok("self_evolution.manual_approve = true (human-in-the-loop gate active)");
        }

        // 沙盒边界预检：与运行时 enforce_sandbox_boundary 同一逻辑
        let signals = cog_reflection::sandbox::SandboxSignals::from_environment();
        let (_, decision) = cog_reflection::sandbox::enforce_sandbox_boundary(se, &signals);
        match decision {
            cog_reflection::sandbox::BoundaryDecision::Allowed(reason) => {
                report.ok(format!("self_evolution sandbox boundary: {reason}"));
            }
            cog_reflection::sandbox::BoundaryDecision::Downgraded(reason) => {
                report.warn(format!(
                    "self_evolution will downgrade to dry-run at runtime: {reason}"
                ));
            }
        }
    } else {
        report.ok("self_evolution disabled");
    }

    // 6. 数据目录
    if config.app.data_dir.trim().is_empty() {
        report.error("app.data_dir is empty");
    } else {
        report.ok(format!("app.data_dir = {}", config.app.data_dir));
    }

    report.print();

    if report.has_errors() {
        eprintln!("\nvalidate-config: FAILED (errors above must be fixed before startup)");
        std::process::exit(1);
    }
    println!("\nvalidate-config: OK");
    Ok(())
}
