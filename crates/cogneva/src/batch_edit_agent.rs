//! `cogneva batch-edit` 的生成侧：第 5 步那个入口。
//!
//! 整批一次进模型，工具结果不完备之前不许出改动——这两件事都在
//! `cog_orchestrator::batch_edit` 里；这里只负责把「一个真能发请求、真能改树的
//! 员工」造出来，并把它交给那条循环。之前这一半是空的：命令读得懂判词、也组得出
//! 请求，但没有 agent，于是它只能报「生成器要由调用方注入」，批永远推不下去。
//!
//! 三个部件都取自与主应用**同一个构造函数**，不在这里另写一份更短的表：
//! 工具（`register_builtins`，环境类工具落在同一个执行器后端上）、模型（同一个
//! llm 路由与 http 客户端）、身份（同一个角色常量）。少一份就多一处「两个说法」。
//!
//! 走 `Agent` 而不是裸 LLM：预算、重试、调用事件、工具调用事件都在 agent 那一层。
//! 生成要的改动也从**树**里读（`Agent` 读 checkout 的入口），不采信模型自己敲出来的
//! 补丁文本——那份是「关于文件的散文」。

use std::sync::Arc;

use cog_core::RuntimeConfig;
use cog_orchestrator::batch_edit::{run_loop, BatchGenerator};

/// 这个入口以谁的名义跑：声明在环境里，不在代码里编一个不存在的员工。
const AGENT_ID_ENV: &str = "COGNEVA_BATCH_EDITOR";

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?;
    // `--dry-run` 只组请求：与真跑同一份组法，模型那一半不参与。
    if std::env::args().any(|a| a == "--dry-run") {
        return cog_orchestrator::batch_edit::run_from_env().await;
    }
    let agent = build_agent()?;
    let generator = match crate::principle_corpus::corpus() {
        Some(corpus) => BatchGenerator::from_agent(agent).with_corpus(corpus),
        None => BatchGenerator::from_agent(agent),
    };
    let rounds = run_loop(&root, &generator).await?;
    println!("batch-edit: 这一批改完了，{rounds} 轮生成");
    Ok(())
}

/// 造出这一次生成用的员工：同一个执行器后端、同一批内建工具、同一个模型入口。
fn build_agent() -> Result<Arc<dyn cog_core::Agent>, Box<dyn std::error::Error>> {
    let config = crate::config_loader::load();
    let system = &config.system;
    let backend = cog_extension::executor_backend(system.sandbox_executor_url.clone())?;
    let agent_loop = cog_agent::AgentLoopConfig::load()?;
    let registry = cog_agent::ToolRegistry::new()
        .with_sandbox_backend(backend.clone())
        .with_guardrail(cog_guardrail::build_default_guardrail(Arc::new(
            cog_guardrail::InMemoryAuditRecorder::new(),
        )));
    // HTTP 客户端按「尽力而为」建：建不出来只少 `http_request` 一个工具，生成照跑。
    let http_client: Option<Arc<dyn cog_core::HttpClient>> = match cog_net::HttpClientConfig::load()
        .and_then(|config| cog_net::ReqwestHttpClient::from_config(&config))
    {
        Ok(client) => Some(Arc::new(client)),
        Err(e) => {
            tracing::warn!(error = %e, "batch-edit: no HTTP client; http_request is not offered");
            None
        }
    };
    cog_agent::tools::builtins::register_builtins(
        &registry,
        cog_agent::tools::builtins::BuiltinInputs {
            http_client: http_client.clone(),
            metrics: None,
            memory_api_base: agent_loop.memory_api_base.clone(),
            external_archive_threshold_chars: agent_loop.external_archive_threshold_chars,
            message_backend: None,
            agent_registry: None,
            web_search_base: agent_loop.web_search_base.clone(),
            web_search_fallback_base: agent_loop.web_search_fallback_base.clone(),
            web_fetch_base: agent_loop.web_fetch_base.clone(),
            sandbox_backend: Some(backend),
            message_events: None,
            intent_events: None,
        },
    );

    let routing = cog_llm::LLMRoutingConfig::load()?;
    let capacity = cog_llm::TuningConfig::load()
        .map(|t| t.stream_capacity)
        .unwrap_or(Default::default());
    let max_tokens = system.anthropic_default_max_tokens;
    let llm = cog_llm::plugin::build_llm_provider(capacity, max_tokens, &routing, http_client)?;
    // 与主应用同一条观测路径：每一笔调用在把流交回下游之前写一行（任务、主体、模型、
    // 用量）。存储由插件在 init 时挂上；这个进程里没挂时只剩进程内计数——那是这一
    // 模式的事实，不在这里另写一份账。
    let llm: Arc<dyn cog_core::LlmClient> =
        Arc::new(cog_llm::observed::ObservedLlmClient::new(llm));

    // 身份从环境读：没有声明就不编一个——编出来的名字会在账上、日志里一路留下痕迹，
    // 而名册上没有这个人。
    let agent_id = std::env::var(AGENT_ID_ENV).map_err(|_| {
        format!("{AGENT_ID_ENV} 没有声明：这一次生成要以谁的名义跑、花销记在谁头上，得先说清楚")
    })?;
    let agent = cog_agent::Agent::new(
        RuntimeConfig {
            agent_id,
            role: cog_core::roles::GENERATOR.into(),
            ..Default::default()
        },
        llm,
    )
    .with_tools(registry);
    Ok(Arc::new(agent))
}
