//! HLE 那一列的工具面：把平台**已有的**检索、抓取与沙箱执行接成 HLE 要的那一束。
//!
//! 这一层不新造能力。`web_search` / `web_fetch` 就是主应用与进化 worker 用的那两件
//! （`cog_agent::web`），`python` 用的是 `cogneva-sandbox-executor` 那个 Pod——主应用
//! 的 `run_command` 走的也是它。评测台与平台此刻量的是同一个执行面，这一点必须在
//! 接线这一处成立：另写一份「评测专用的搜索」会让表里的分数与平台实际能做的事脱钩，
//! 而两者都叫 `web_search`。
//!
//! 三件缺一不可，缺任何一件这一列就不该以主表行跑：
//! - `web_search` 只给排名与摘要，读不到需要点进去的页面；
//! - `web_fetch` 只给一个地址的正文，找不到地址；
//! - `python` 是算数与数据处理，没有它模型只能心算。
//!
//! 少一件仍然跑得下去、分数只会更低——所以缺席在这里是**构造期错误**，由接线那一步
//! 记成「这一列没接上」，而不是把一束更小的工具交出去让它悄悄量别的东西。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cog_core::{
    HttpClient, ResourceLimits, SFError, SandboxBackend, SandboxPayload, SandboxRequest, Tool,
    ToolImplementation,
};
use cog_eval::{ToolFactory, HLE_TOOL_PYTHON, HLE_TOOL_WEB_FETCH, HLE_TOOL_WEB_SEARCH};

/// 接线一次跑要的四样东西：两件网页工具的端点、沙箱执行面、以及一次 `python` 调用的墙。
///
/// 端点都收 `Option` 是因为配置层就是这么表达的（「这个部署没接」与「配错了」在
/// 配置里同形）；把空值变成一句带名字的错是**这里**的活，不是调用方的。
pub struct HleToolInputs {
    pub http: Arc<dyn HttpClient>,
    pub web_search_base: Option<String>,
    pub web_search_fallback_base: Option<String>,
    pub web_fetch_base: Option<String>,
    pub web_fetch_fallback_base: Option<String>,
    pub sandbox: Arc<dyn SandboxBackend>,
}

/// 造 HLE 的工具面。缺席的端点在这里变成错误，错误里带着**要设的那一项**的名字。
pub fn hle_tool_factory(inputs: HleToolInputs) -> anyhow::Result<Arc<ToolFactory>> {
    let search_base = inputs.web_search_base.ok_or_else(|| {
        anyhow::anyhow!(
            "HLE's `{HLE_TOOL_WEB_SEARCH}` has no backend in this run: set \
             COGNEVA_WEB_SEARCH_BASE (agent_loop.web_search_base) to the deployment's \
             metasearch endpoint"
        )
    })?;
    let fetch_base = inputs.web_fetch_base.ok_or_else(|| {
        anyhow::anyhow!(
            "HLE's `{HLE_TOOL_WEB_FETCH}` has no backend in this run: set \
             COGNEVA_WEB_FETCH_BASE (agent_loop.web_fetch_base) to the deployment's page \
             renderer; without it a search result's page cannot be read at all"
        )
    })?;

    let search = cog_agent::web::web_search_with_fallback(
        Some(inputs.http.clone()),
        Some(search_base),
        inputs.web_search_fallback_base,
    );
    let fetch = cog_agent::web::web_fetch_with_fallback(
        Some(inputs.http),
        Some(fetch_base),
        inputs.web_fetch_fallback_base,
    );
    let python = python_tool(inputs.sandbox);

    // 清单不随题变：HLE 的题面里没有环境，一条题与下一条题在工具面上没有区别。写成
    // 工厂是与另两列同一形状，把「清单可以随题变」留在这层协议里而不是留成注释。
    Ok(Arc::new(move |_, _| {
        Ok(vec![search.clone(), fetch.clone(), python.clone()])
    }))
}

/// 一次 `python` 调用的流水号。
///
/// 执行器把 `task_id` 当作「这次请求属于哪棵任务树」：空串是**不进任何任务树**（它
/// 会记一条 unscoped 读数并警告），非空的、路由不出来的 id 直接 500。评测台调用不
/// 属于任何构建任务，所以这里刻意送空——但同一进程里的两次调用仍要能分开，序号
/// 因此留在 `agent_id` 上。
static PYTHON_CALLS: AtomicU64 = AtomicU64::new(0);

/// `python`：把模型给的代码交给沙箱执行器跑，交回它自己的读数。
///
/// 退出码是**结果**不是错误：代码抛异常正是模型要看见的东西，把它变成工具错误会让
/// 外壳以为工具坏了、然后重试同一段代码。
pub fn python_tool(sandbox: Arc<dyn SandboxBackend>) -> Tool {
    Tool {
        name: HLE_TOOL_PYTHON.into(),
        description: "Run a Python 3 program in this deployment's sandbox and return what it \
                      printed. Use it for arithmetic, data processing, and anything you would \
                      otherwise have to compute in your head; the process has no network and \
                      no state between calls, so send one self-contained program per call."
            .into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "The whole program. Print what you want to see: only \
                                    stdout/stderr come back."
                }
            },
            "required": ["code"]
        }),
        implementation: ToolImplementation::Native(Arc::new(move |arguments| {
            let sandbox = sandbox.clone();
            Box::pin(async move {
                let code = arguments
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        SFError::Validation("python wants a `code` string".to_string())
                    })?;
                let call = PYTHON_CALLS.fetch_add(1, Ordering::Relaxed);
                let request = SandboxRequest {
                    // 空：评测台的调用不属于任何构建任务树。见 `PYTHON_CALLS`。
                    task_id: String::new(),
                    agent_id: format!("eval-hle-{call}"),
                    // `cd /tmp` 不依赖执行器的进程工作目录：不锚定任务树时它是继承来的，
                    // 而这个值随部署变。`-c` 的参数经单引号整体包住，代码里的引号、换行、
                    // `$` 都按字面走。
                    payload: SandboxPayload::Command {
                        command: format!(
                            "cd /tmp && exec python3 -c {}",
                            crate::eval_mcp_in_pod::sh_quote(code)
                        ),
                    },
                    input: serde_json::Value::Null,
                    // 远端执行器不转发 `limits`（它只发 payload/身份），所以这里
                    // 写的上限不会生效；隔离由 Pod 边界给，见执行器的部署清单。
                    limits: ResourceLimits::default(),
                };
                let result = sandbox.execute(&request).await?;
                Ok(serde_json::json!({
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "exit_code": result.exit_code,
                }))
            })
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{SFResult, SandboxResult};
    use std::sync::Mutex;

    /// 一个记下请求、按脚本回话的执行面。评测台这一侧只关心「请求长什么样」与
    /// 「读数怎么映射」，不关心命令在哪儿跑。
    struct RecordingSandbox {
        seen: Mutex<Vec<SandboxRequest>>,
        reply: SandboxResult,
    }

    #[async_trait]
    impl SandboxBackend for RecordingSandbox {
        async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
            self.seen.lock().unwrap().push(req.clone());
            Ok(self.reply.clone())
        }

        async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
            Err(cog_core::SFError::Agent("not used".into()))
        }
    }

    fn sandbox(seen: Arc<RecordingSandbox>) -> Arc<dyn SandboxBackend> {
        seen
    }

    fn recorder() -> Arc<RecordingSandbox> {
        Arc::new(RecordingSandbox {
            seen: Mutex::new(Vec::new()),
            reply: SandboxResult {
                stdout: "4\n".into(),
                stderr: "warn\n".into(),
                exit_code: 0,
                ..Default::default()
            },
        })
    }

    /// 一个不会被问到的传输：这一组测试只关心工具面的形状与 `python` 的落点。
    #[derive(Debug)]
    struct UnusedClient;

    #[async_trait]
    impl HttpClient for UnusedClient {
        async fn execute(&self, _req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            Err(cog_core::SFError::Agent("not used".into()))
        }
    }

    fn inputs(sandbox: Arc<dyn SandboxBackend>) -> HleToolInputs {
        HleToolInputs {
            http: Arc::new(UnusedClient),
            web_search_base: Some("http://searxng.invalid:8080".into()),
            web_search_fallback_base: None,
            web_fetch_base: Some("http://crawl4ai.invalid:11235".into()),
            web_fetch_fallback_base: None,
            sandbox,
        }
    }

    /// 交出去的**就是** HLE 钉死的那一束：检索、抓取、Python。
    ///
    /// 这一条对着 `HleToolkit` 的核验跑，而不是自己再列一遍名字——两处各列一份，
    /// 改了一处另一处不会红。
    #[test]
    fn the_factory_hands_out_exactly_the_hle_face() {
        let factory = hle_tool_factory(inputs(sandbox(recorder()))).unwrap();
        let env: Arc<dyn cog_eval::CaseEnv> = Arc::new(cog_eval::NoEnv);
        let tools = factory(&case(), &env).unwrap();
        let mut names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [HLE_TOOL_PYTHON, HLE_TOOL_WEB_FETCH, HLE_TOOL_WEB_SEARCH]
        );
        // 工具包的核验也过：形状对这一层还不够，协议那一层认不认才是它能不能进表的判据。
        assert!(cog_eval::HleToolkit::with_tools(tools).is_ok());
    }

    /// 缺一件网页后端时**构造**就失败，错误点名要设的那一项。
    ///
    /// 让它过去的话，工具照在、调用照错：模型会把一半的回合花在失败的工具上，分数
    /// 低得像是模型不会查资料，而真相是这个部署没接那一件。
    #[test]
    fn a_missing_web_backend_is_an_error_that_names_the_setting() {
        let mut no_search = inputs(sandbox(recorder()));
        no_search.web_search_base = None;
        let err = hle_tool_factory(no_search)
            .err()
            .expect("no search backend is an unwindable state")
            .to_string();
        assert!(err.contains("COGNEVA_WEB_SEARCH_BASE"), "{err}");

        let mut no_fetch = inputs(sandbox(recorder()));
        no_fetch.web_fetch_base = None;
        let err = hle_tool_factory(no_fetch)
            .err()
            .expect("no fetch backend is an unwindable state")
            .to_string();
        assert!(err.contains("COGNEVA_WEB_FETCH_BASE"), "{err}");
    }

    /// `python` 把代码送给沙箱执行器，并把执行器自己的读数原样交回。
    ///
    /// 三条一起钉住：命令是**这一台执行器**跑的（不是进程内的 eval）、它不冒领任何
    /// 任务树（`task_id` 空，否则执行器会去路由一棵不存在的树并 500）、退出码非零
    /// 也是结果而不是错误。
    #[tokio::test]
    async fn python_runs_in_the_sandbox_and_returns_the_programs_readings() {
        let seen = recorder();
        let tool = python_tool(sandbox(seen.clone()));
        let out = face(tool)
            .call(HLE_TOOL_PYTHON, serde_json::json!({"code": "print(2 + 2)"}))
            .await
            .unwrap();
        assert_eq!(out["stdout"], "4\n");
        assert_eq!(out["stderr"], "warn\n");
        assert_eq!(out["exit_code"], 0);

        // 锁在块里取、块里放：拿到的 `request` 是从 guard 上借的，把放手写成
        // `drop(guard)` 时这个借用仍在作用域里，读代码的人分不清下一次 await 之前
        // 锁到底有没有松开。块结束即松手，不用读者去追一遍借用。
        {
            let requests = seen.seen.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let request = &requests[0];
            match &request.payload {
                SandboxPayload::Command { command } => {
                    assert!(command.contains("python3 -c 'print(2 + 2)'"), "{command}");
                    assert!(command.starts_with("cd /tmp"), "{command}");
                }
                other => panic!("python must go to the executor as a command, got {other:?}"),
            }
            assert_eq!(
                request.task_id, "",
                "a benchmark call belongs to no task tree"
            );
        }

        // 代码里的单引号不会把命令拆开：整段经单引号包住。
        let tool = python_tool(sandbox(seen.clone()));
        face(tool)
            .call(
                HLE_TOOL_PYTHON,
                serde_json::json!({"code": "print(\"it's\")"}),
            )
            .await
            .unwrap();
        let requests = seen.seen.lock().unwrap();
        match &requests[1].payload {
            SandboxPayload::Command { command } => {
                assert!(command.contains(r#"'print("it'\''s")'"#), "{command}");
            }
            other => panic!("python must go to the executor as a command, got {other:?}"),
        }
    }

    /// 没给代码是**这一层**的错，不是执行器的错：送一段空命令下去会得到一句关于
    /// shell 的错，读的人会去查执行器。
    #[tokio::test]
    async fn python_refuses_a_call_without_code() {
        let seen = recorder();
        let tool = python_tool(sandbox(seen.clone()));
        let err = face(tool)
            .call(HLE_TOOL_PYTHON, serde_json::json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("code"), "{err}");
        assert!(seen.seen.lock().unwrap().is_empty());
    }

    /// 一件工具包成外壳真正调用的那一层。工具包只收 native 闭包，所以它同时是
    /// 「`python` 交出去的是闭包而不是给后端的声明」的判据。
    fn face(tool: Tool) -> cog_eval::ToolSet {
        cog_eval::ToolSet::new(vec![tool]).expect("python is a native implementation")
    }

    fn case() -> cog_eval::EvalCase {
        cog_eval::EvalCase {
            id: "hle-x".into(),
            name: "hle-x".into(),
            input: serde_json::json!({"question": "q", "image": null}),
            expected_output: Some(serde_json::Value::String("a".into())),
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: std::collections::HashMap::new(),
        }
    }
}
