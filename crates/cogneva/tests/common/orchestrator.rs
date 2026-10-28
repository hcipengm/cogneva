//! 一份能跑的 orchestrator 控制面，几份 supervisor 契约都要它。
//!
//! 三处各写了一遍同样的三行：`DagExecutor::new("ws")` 再包一层
//! `OrchestratorControlImpl`。这不是「各自造各自的依赖」：三份契约问的是同一个
//! supervisor 对同一个控制面的回答，一边换了 workspace 或换了实现而另一边没换，
//! 三者就不再描述同一个对象了——而三份都还是绿的，因为没人比较它们。

// 每个测试二进制各编一次 `common`，只有拿控制面的那几份用它。
#![allow(dead_code)]

use std::sync::Arc;

use cog_core::OrchestratorControl;

/// 挂在 `workspace` 的空 DAG 执行器后面的控制面。
///
/// workspace 由调用方说：它只是一份标签，但这个标签是调用方各自的事实，
/// 不在这里替它们编一个。
pub fn control(workspace: &str) -> Arc<dyn OrchestratorControl> {
    Arc::new(cog_orchestrator::OrchestratorControlImpl::new(Arc::new(
        cog_orchestrator::DagExecutor::new(workspace.into()),
    )))
}

/// 同上，但执行器把任务事件发到调用方那条总线上。
///
/// 它单独是一个入口而不是 `control` 的一个可选参数：要订阅任务事件的用例关心的是
/// 「事件有没有出来」，不要的用例关心的是「这个标签下有没有控制面」。把两件事合进一个
/// 带 `Option` 的签名，前者就得在一个不关心它的调用点上也写一遍 `None`。
pub fn control_with_events(
    workspace: &str,
    events: tokio::sync::broadcast::Sender<cog_core::TaskEvent>,
) -> Arc<dyn OrchestratorControl> {
    Arc::new(cog_orchestrator::OrchestratorControlImpl::new(Arc::new(
        cog_orchestrator::DagExecutor::new(workspace.into()).with_event_tx(events),
    )))
}
