//! 单测共用的样例：把几行「路径 ＋ 文本」解析成一份声明账。
//!
//! 它不是任何一处的题目，是两个测量模块都要先摆好的东西——`principle_ledger` 与
//! `principle_measure` 各写了一遍同一个 `declarations`，`Declarations::parse` 的入参
//! 形状一变，两份不会同时红。

use crate::principle_measure::Declarations;
use tracing_subscriber::prelude::*;

/// 把 `(路径, 文本)` 的行解析成一份声明账。
pub(crate) fn declarations(lines: &[(&str, &str)]) -> Declarations {
    Declarations::parse(
        &lines
            .iter()
            .map(|(path, text)| (path.to_string(), text.to_string()))
            .collect::<Vec<_>>(),
    )
}

/// 收下 warn 级事件的正文，供断言「这一件事在屏幕上说出来了没有」。
///
/// 这一族里的几处修法不改变返回值：读不到时那一趟仍然什么都不做，与「确实
/// 没有要做的事」给的是同一个返回。屏幕上那几行就是唯一能分开两者的地方，
/// 断言只能落在它上面。
///
/// 订阅者**装一次、装成进程级的，不撤下**。`tracing` 对每条 callsite 算一份
/// 兴趣缓存，算的是「这个进程里此刻注册着哪些订阅者」；`set_default` 是线程
/// 局部的，而且只有一条订阅者注册着时它算的是**命中那条线程**上的默认订阅者。
/// 别的测试线程先命中同一条 warn 时，那一次算出来的是 `NoSubscriber` 的 never
/// ——之后我们这一层就再也收不到（2026-11-23 实测：同一批测试单跑绿、整套并行
/// 跑红，`said()` 空着）。装成全局的，任何线程上的默认订阅者都是它。
///
/// 每条事件按 `ThreadId` 分桶，每个测试只读自己那一桶：`#[tokio::test]` 默认的
/// current-thread runtime 不换线程，它 spawn 出来的任务也在同一条线程上跑。
#[derive(Clone, Default)]
pub(crate) struct WarnSink(std::sync::Arc<std::sync::Mutex<Vec<(std::thread::ThreadId, String)>>>);

pub(crate) struct MessageField(String);

impl tracing::field::Visit for MessageField {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnSink {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut field = MessageField(String::new());
        event.record(&mut field);
        self.0
            .lock()
            .unwrap()
            .push((std::thread::current().id(), field.0));
    }
}

pub(crate) struct WarnText {
    sink: WarnSink,
    thread: std::thread::ThreadId,
}

/// 这一条测试线程上收到的那几行 warn。
pub(crate) fn warn_text() -> WarnText {
    static SINK: std::sync::OnceLock<WarnSink> = std::sync::OnceLock::new();
    let sink = SINK.get_or_init(|| {
        let sink = WarnSink::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(sink.clone()),
        )
        .expect("这个测试二进制里不该有别人先装全局订阅者");
        sink
    });
    WarnText {
        sink: sink.clone(),
        thread: std::thread::current().id(),
    }
}

impl WarnText {
    pub(crate) fn said(&self) -> Vec<String> {
        self.sink
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(thread, _)| *thread == self.thread)
            .map(|(_, line)| line.clone())
            .collect()
    }

    pub(crate) fn any(&self, needle: &str) -> bool {
        self.said().iter().any(|line| line.contains(needle))
    }
}
