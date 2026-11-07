//! 池暂停闸的延后解析句柄。
//!
//! `SchedulerGate` 由 supervisor 插件发布，而 collaboration 的 `requires` 里
//! 没有 supervisor——两个没有边的插件落在同一层、并发 init，在 init 里读它读到
//! 的是运气：同层的另一个插件可能还没发布完。这不是本 crate 的推断，是
//! `LateService` 的文档明写的两种形状之一。
//!
//! 所以这里不直接持有闸，而是持有一个「第一次被问到才去取」的句柄。Ralph 循环
//! 只在轮次之间问它，那时所有插件的 init 都已经结束，注册表不再变化。取不到就
//! 一路按「没暂停」走：没有闸的部署与从前一样，不多问。
//!
//! 为什么把它包成 `SchedulerGate` 而不是把 `LateService` 传下去：循环只认这一个
//! 契约，传下一个不同的句柄类型会让每一处签名跟着变，而测试里的假闸也得跟着造
//! 一个插件上下文。包一层之后，只有发布侧知道这个先后问题。
use cog_core::{LateService, SchedulerGate, TaskClass};

/// 延后解析的 [`SchedulerGate`]。
pub struct LateGate(LateService<dyn SchedulerGate>);

impl LateGate {
    /// 取一个还没有解析的句柄。第一次调用任一方法时才去 `ctx` 里找闸。
    pub fn new(ctx: cog_core::PluginContext) -> Self {
        Self(LateService::new(ctx))
    }
}

impl SchedulerGate for LateGate {
    fn is_paused(&self) -> bool {
        self.0.get().map(|g| g.is_paused()).unwrap_or(false)
    }

    fn pause(&self) -> bool {
        self.0.get().map(|g| g.pause()).unwrap_or(false)
    }

    fn resume(&self) -> bool {
        self.0.get().map(|g| g.resume()).unwrap_or(false)
    }

    fn is_paused_kind(&self, class: TaskClass) -> bool {
        self.0
            .get()
            .map(|g| g.is_paused_kind(class))
            .unwrap_or(false)
    }

    fn pause_kind(&self, class: TaskClass) -> bool {
        self.0.get().map(|g| g.pause_kind(class)).unwrap_or(false)
    }

    fn resume_kind(&self, class: TaskClass) -> bool {
        self.0.get().map(|g| g.resume_kind(class)).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个只会被 resolve 一次的假闸：`is_paused_kind` 记下它被问过几次。
    struct CountingGate(std::sync::atomic::AtomicUsize);

    impl SchedulerGate for CountingGate {
        fn is_paused(&self) -> bool {
            false
        }
        fn pause(&self) -> bool {
            false
        }
        fn resume(&self) -> bool {
            false
        }
        fn is_paused_kind(&self, _class: TaskClass) -> bool {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        }
        fn pause_kind(&self, _class: TaskClass) -> bool {
            false
        }
        fn resume_kind(&self, _class: TaskClass) -> bool {
            false
        }
    }

    /// 取不到闸时是「没暂停」，不是「暂停」也不是一个错误——没有 supervisor 的
    /// 部署（进化 worker、沙盒）本来就没有闸。
    #[test]
    fn without_a_published_gate_the_answer_is_not_paused() {
        // 一个没人发布过任何服务的上下文：注册表里没有闸。
        let ctx = cog_core::PluginContext::new(cog_core::Config::default());
        let gate = LateGate::new(ctx);
        assert!(!gate.is_paused_kind(TaskClass::LlmDependent));
        assert!(!gate.is_paused());
    }

    /// 句柄造好之后再发布闸，它**要读得到**。
    ///
    /// 这一格挡的是「在 init 里 consume 一次、把 `Option<Arc<..>>` 存下来」那种
    /// 写法：collaboration 与 supervisor 没有依赖边，同层并发 init，那样存下来的
    /// 多半是 `None`，而 `None` 在这条循环里的意思正好是「没有闸、照常跑」。于是
    /// 修复看起来落地了，实际一次都不会生效——这一格用「先造句柄、后发布」的次序
    /// 把那件事变成可判的。
    #[test]
    fn a_gate_published_after_the_handle_was_built_is_still_seen() {
        let ctx = cog_core::PluginContext::new(cog_core::Config::default());
        let gate = LateGate::new(ctx.clone());
        // 句柄已造好，此刻注册表里还没有闸——但这一句不解析，也不要断言它。
        ctx.publish_service::<dyn SchedulerGate>(std::sync::Arc::new(CountingGate(
            std::sync::atomic::AtomicUsize::new(0),
        )));
        assert!(
            gate.is_paused_kind(TaskClass::LlmDependent),
            "闸是在句柄之后发布的，延后解析必须读得到它"
        );
    }
}
