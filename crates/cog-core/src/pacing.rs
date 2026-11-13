//! 一个自定节拍的回路该隔多久再跑下一轮，以及一件事的时限该给多长。
//!
//! 这两句话在系统里出现在很多处：轮询、扫描、重试冷却、子进程与滚动的超时。每一处
//! 原来都各自写一个数，改一处不会带动另一处，而它们量的是同一件事——最近一次做这件
//! 事花了多久。把律放在这里，是因为用它的回路分属不同 crate（反射侧的轮询与网关侧
//! 的 git 传输），任何一边单独放的副本都无法与另一边比较，第三份、第四份值不一样
//! 时也没有任何东西看得见。
//!
//! 律本身只有一条：**实测决定目标，配置值是它的种子与地板**。
//!
//! - 间隔：还没有实测时是种子；量到过之后是 `上轮耗时 × factor`，被地板托住。
//!   一次跑得比种子久，间隔跟着变长——那是这一轮真实占用的时间；一次跑得比种子
//!   快，间隔也跟着变短——种子是按当时的负载挑的，而负载会变。不想快于配置的
//!   回路把地板设成种子，这个决定留给每一处调用点。
//! - 时限：`max(seed, floor, 实测尾部 × factor)`。尾部取的是窗口里的分位数，不是
//!   最长的一次：一次离群值不该把所有人的等待拉长，而尾部本来就该覆盖绝大多数轮。
//!
//! 两句话都不含「量不到」的答案：没有样本时调用方拿到 `None`，保留种子并如实把
//! 来源标成 `seed`。一个用猜出来的数冒充实测值的回路，比一个停在上一次实测的回路
//! 更难被看见。
//!
//! 种子与地板分两个参数，是因为它们回答的问题不同：种子是部署声明的节拍，地板是
//! 这个回路在任何情况下都不该跨过的界（协议下限、时钟分辨率）。把两者折成一个数
//! 会让「部署把它调低了」和「协议不允许更低」变成同一件事。

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use crate::SFResult;

/// 当前这一拍的目标间隔或时限（秒）。
pub const TARGET_METRIC: &str = "cogneva_pacing_target_seconds";
/// 这一拍算过几次。只增；与目标值分开——同样的实测得出同样的目标是正确行为，
/// 「这一拍算过」要单独有一个证据。
pub const EVALUATIONS_METRIC: &str = "cogneva_pacing_evaluations_total";
/// 回路名：与登记表里的键同一套词，一个回路一条序列。
pub const PACING_LABEL: &str = "pacing";
/// `seed` 或 `derived`。
pub const SOURCE_LABEL: &str = "source";
pub const SOURCE_SEED: &str = "seed";
pub const SOURCE_DERIVED: &str = "derived";

/// 上轮耗时 `pass` 之后，下一轮最早该在什么时候开始。
///
/// 还没有实测时（`pass` 为零）目标就是部署声明的种子；一旦量到过，周期由实测
/// 决定：`pass × factor`，再被地板托住。种子不再是上限——种子是人按当时的负载
/// 挑的，而负载会变；它只是那之前的值。
///
/// `factor` 是「一轮的工作最多占多少时间」的倒数：20 表示一成的等待里干一份活
/// （工作占 5%）。小于 1 的倍率没有意义——那会让回路跑得比它干完一轮还快，实际
/// 只是把积压藏进调度延迟里。想要「不比配置更快」的回路把地板设成种子，这个
/// 决定留给调用方：走查自己的卷与轮询别人的 API，对「能不能更快」的答案不同。
pub fn paced_interval(pass: Duration, seed: Duration, floor: Duration, factor: f64) -> Duration {
    if pass.is_zero() {
        return seed.max(floor);
    }
    pass.mul_f64(factor.max(1.0)).max(floor)
}

/// 一个度量窗口里最近一段实测值。
///
/// 有界，因为要回答的是「最近这件事有多花时间」，不是「历史上最慢的一次」：无界
/// 的容器会给出一笔一个月前留下的离群值，而一个只会变宽的界与一个常量是同一种
/// 缺陷——它不再跟着正在发生的事走。
#[derive(Debug, Clone)]
pub struct DurationWindow {
    samples: VecDeque<Duration>,
    capacity: usize,
}

impl DurationWindow {
    /// 保留最近 `capacity` 个样本。0 按 1 处理：一个什么都装不下的窗口不是窗口。
    pub fn new(capacity: usize) -> Self {
        Self {
            samples: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// 记一个样本；窗口满时挤掉最早的那个。
    pub fn record(&mut self, sample: Duration) {
        if self.samples.len() == self.capacity {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// 窗口里 `quantile` 分位上的值；空窗口是 `None`。
    ///
    /// 分位数取「至少有这么多比例的样本不超过它」的那个样本（最近秩），不对样本
    /// 做插值：插出来的数不对应任何一次真发生过的事，而它要被当成一个真的要等
    /// 或真的要给出去的时长。
    pub fn quantile(&self, quantile: f64) -> Option<Duration> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted: Vec<Duration> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        let q = quantile.clamp(0.0, 1.0);
        let rank = ((q * sorted.len() as f64).ceil() as usize).max(1);
        sorted.get(rank - 1).copied()
    }
}

/// 一件事的时限：实测尾部的 `factor` 倍，被地板托住。
///
/// `window` 为空时返回 `None`——调用方保留种子并说清来源是种子。有样本之后种子
/// 不再参与：它是还没有实测时的值，不是一个上限或下限；想「永不短于配置」的调用
/// 点把地板设成配置，与 [`paced_interval`] 同一个约定。`factor` 把「几乎每一轮都
/// 不超过」的尾部变成「健康的慢轮也不会撞上」的界。
pub fn tail_budget(
    window: &DurationWindow,
    quantile: f64,
    floor: Duration,
    factor: f64,
) -> Option<Duration> {
    window
        .quantile(quantile)
        .map(|tail| tail.mul_f64(factor.max(1.0)).max(floor))
}

/// 时限读数的尾部取哪一分位：几乎每一轮都不超过它，离群值不把界撑开。
pub const TAIL_QUANTILE: f64 = 0.99;
/// 尾部窗口保留多少个样本。有界是刻意的：界要跟着最近发生的事走。
pub const TAIL_WINDOW_SAMPLES: usize = 64;

/// 连续第 `attempt` 次失败之后，下一次之前该等多久。
///
/// 与两条时间律同族，输入却是**次数**而不是时长：环境连着坏了几次，是这一拍唯一
/// 量得到的东西——"它什么时候会好"没有读数，而"它已经坏了几次"每轮都在发生。
/// 第一拍就是种子；之后每多一次连续失败，等待按 `factor` 拉开。次数清零（一次
/// 成功）就回到种子。不给上限：一个坏了几十拍的环境，隔一个多月再看一眼正是对的，
/// 而那个数由次数自己长出来，不是人钉的。
pub fn backoff(attempt: u64, base: Duration, factor: f64) -> Duration {
    if attempt <= 1 {
        return base;
    }
    let total = base.as_secs_f64() * factor.max(1.0).powf((attempt - 1) as f64);
    Duration::try_from_secs_f64(total).unwrap_or(Duration::MAX)
}

#[derive(Debug, Default)]
struct PaceState {
    target: Duration,
    evaluations: u64,
    derived: bool,
}

/// 所有按实测定节拍的回路共用的一个读数面。
///
/// 一个进程里可能有多条这样的回路（轮询、扫描、冷却），它们在这里各占一格：
/// 节拍不同，问题相同。这也让发布面只有一族序列——每行自己造一个指标名，看板上
/// 就会长出二十条只有一处读得懂的线。
#[derive(Debug, Default)]
pub struct PacingReadings {
    states: Mutex<BTreeMap<String, PaceState>>,
}

impl PacingReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// 给一条回路发一个句柄。
    pub fn pacer(
        self: &Arc<Self>,
        name: impl Into<String>,
        seed: Duration,
        floor: Duration,
        factor: f64,
    ) -> Pacer {
        Pacer {
            readings: Arc::clone(self),
            name: name.into(),
            seed,
            floor,
            factor,
        }
    }

    /// 给一条「连续失败之后等多久」的回路发一个句柄。种子是第一拍的等待，之后
    /// 由连续失败的次数决定。
    pub fn backoff(
        self: &Arc<Self>,
        name: impl Into<String>,
        base: Duration,
        factor: f64,
    ) -> Arc<Backoff> {
        Arc::new(Backoff {
            readings: Arc::clone(self),
            name: name.into(),
            base,
            factor,
        })
    }

    /// 给一条「一件事的时限」回路发一个句柄。`seed` 只在还没有样本时说话；
    /// 有了样本之后由实测尾部决定，地板是这个回路在任何情况下都不该跨过的界。
    pub fn tail(
        self: &Arc<Self>,
        name: impl Into<String>,
        seed: Duration,
        floor: Duration,
        factor: f64,
    ) -> Arc<TailBudget> {
        Arc::new(TailBudget {
            readings: Arc::clone(self),
            name: name.into(),
            seed,
            floor,
            factor,
            window: Mutex::new(DurationWindow::new(TAIL_WINDOW_SAMPLES)),
        })
    }

    /// 最近一拍的目标、求值次数与来源。读数与测试共用同一份状态。
    pub fn target(&self, name: &str) -> Option<(Duration, u64, bool)> {
        self.states
            .lock()
            .unwrap()
            .get(name)
            .map(|state| (state.target, state.evaluations, state.derived))
    }
}

/// 一条回路的节拍句柄。克隆便宜，跟着回路走。
#[derive(Clone)]
pub struct Pacer {
    readings: Arc<PacingReadings>,
    name: String,
    seed: Duration,
    floor: Duration,
    factor: f64,
}

impl Pacer {
    /// 回路名，也是读数上的 `pacing` 标签。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 部署声明的种子。没有实测时，目标就是它（与地板取大）。
    pub fn seed(&self) -> Duration {
        self.seed
    }

    /// 记一拍：`pass` 是刚刚结束的那一轮花的时间，第一轮之前传 `None`。
    ///
    /// 返回这一拍的目标周期。周期从**这一轮的起点**算，不是「跑完再睡多久」：
    /// 调用方用 `target.saturating_sub(pass)` 得到本轮的等待，这样一轮跑得比
    /// 种子久时不会在它之外再叠一个种子。
    pub fn beat(&self, pass: Option<Duration>) -> Duration {
        let target = match pass {
            Some(pass) => paced_interval(pass, self.seed, self.floor, self.factor),
            None => self.seed.max(self.floor),
        };
        let mut states = self.readings.states.lock().unwrap();
        let state = states.entry(self.name.clone()).or_default();
        state.target = target;
        state.evaluations = state.evaluations.saturating_add(1);
        state.derived |= pass.is_some();
        target
    }

    /// 只记一拍，不要目标。给「这一拍没有可量的东西、但回路确实算过」的调用方。
    pub fn touch(&self) -> Duration {
        self.beat(None)
    }
}

/// 一条「时限」回路的句柄：收每一轮实测，给出下一轮该申报多长的界。
///
/// 与 [`Pacer`] 是同一个族的两种读数：节拍问「隔多久再来」，时限问「这一件最多
/// 允许多久」。两者都从最近发生的轮次里量，都把种子只用在还没有实测的那一段。
#[derive(Debug)]
pub struct TailBudget {
    readings: Arc<PacingReadings>,
    name: String,
    seed: Duration,
    floor: Duration,
    factor: f64,
    window: Mutex<DurationWindow>,
}

impl TailBudget {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 记一轮实测，返回这一轮之后申报的界。
    pub fn observe(&self, sample: Duration) -> Duration {
        let target = {
            let mut window = self.window.lock().unwrap();
            window.record(sample);
            tail_budget(&window, TAIL_QUANTILE, self.floor, self.factor)
                .unwrap_or(self.seed.max(self.floor))
        };
        let mut states = self.readings.states.lock().unwrap();
        let state = states.entry(self.name.clone()).or_default();
        state.target = target;
        state.evaluations = state.evaluations.saturating_add(1);
        state.derived = true;
        target
    }

    /// 最近一次算出的界；还没有实测时是种子。
    pub fn target(&self) -> Duration {
        self.readings
            .target(&self.name)
            .map(|(target, _, _)| target)
            .unwrap_or(self.seed.max(self.floor))
    }

    /// 这个回路算过几轮。零是「还没量到过」，不是「量到了零」。
    pub fn evaluations(&self) -> u64 {
        self.readings
            .target(&self.name)
            .map(|(_, evaluations, _)| evaluations)
            .unwrap_or(0)
    }

    /// 窗口里现有多少样本。
    pub fn samples(&self) -> usize {
        self.window.lock().unwrap().len()
    }
}

/// 一条「连续失败之后等多久」回路的句柄。
///
/// 它不持有次数：次数是调用方的状态（要落盘、要跨重启），这里只负责把「第 n 次」
/// 换成一张时长，并把这一拍记到读数面上。于是同一份次数在任何一个进程里算出的值
/// 一样，读数上的求值计数说的是"这个回路又算了一拍"。
#[derive(Debug)]
pub struct Backoff {
    readings: Arc<PacingReadings>,
    name: String,
    base: Duration,
    factor: f64,
}

impl Backoff {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 连续第 `attempt` 次失败（从 1 起）之后的等待。`attempt = 0` 是「刚恢复」，
    /// 回到种子；两种都算这一拍算过。
    pub fn evaluate(&self, attempt: u64) -> Duration {
        let target = backoff(attempt, self.base, self.factor);
        let mut states = self.readings.states.lock().unwrap();
        let state = states.entry(self.name.clone()).or_default();
        state.target = target;
        state.evaluations = state.evaluations.saturating_add(1);
        // 第一拍（attempt=1）就是种子：照实说来源是种子，别让下游把「还没坏第二次」
        // 读成一个量出来的等待。
        state.derived |= attempt > 1;
        target
    }

    /// 最近一拍的等待；还没有算过时是种子。
    pub fn target(&self) -> Duration {
        self.readings
            .target(&self.name)
            .map(|(target, _, _)| target)
            .unwrap_or(self.base)
    }

    /// 这个回路算过几拍。
    pub fn evaluations(&self) -> u64 {
        self.readings
            .target(&self.name)
            .map(|(_, evaluations, _)| evaluations)
            .unwrap_or(0)
    }
}

#[async_trait]
impl Observable for PacingReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let states = self.states.lock().unwrap();
        let mut out = Vec::new();
        for (name, state) in states.iter() {
            out.push(
                RawMetric::new(TARGET_METRIC, state.target.as_secs_f64())
                    .with_label(PACING_LABEL, name.clone())
                    .with_label(
                        SOURCE_LABEL,
                        if state.derived {
                            SOURCE_DERIVED
                        } else {
                            SOURCE_SEED
                        },
                    ),
            );
            out.push(
                RawMetric::new(EVALUATIONS_METRIC, state.evaluations as f64)
                    .with_label(PACING_LABEL, name.clone()),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_measured_pass_replaces_the_seed_and_a_longer_one_stretches_the_interval() {
        // 种子 300 秒：一轮 3 秒的实测按倍率 20 得出 60 秒，种子不再是上限。
        assert_eq!(paced_interval(secs(3), secs(300), secs(30), 20.0), secs(60));
        // 一轮 1000 秒的实测决定下一拍：1000 × 20，远超种子。
        assert_eq!(
            paced_interval(secs(1000), secs(300), secs(30), 20.0),
            secs(20_000)
        );
        // 实测变长，目标跟着变长——这是回路在用同一个种子回答不同的输入。
        assert_eq!(
            paced_interval(secs(40), secs(300), secs(30), 20.0),
            secs(800)
        );
    }

    #[test]
    fn the_seed_stands_until_a_pass_is_measured_and_the_floor_holds_below_it() {
        // 没有一轮时，间隔就是种子；种子低于地板时，地板说话。
        assert_eq!(
            paced_interval(Duration::ZERO, secs(300), secs(30), 1.0),
            secs(300)
        );
        assert_eq!(
            paced_interval(Duration::ZERO, secs(5), secs(30), 1.0),
            secs(30)
        );
        // 量到一轮之后，一个很短的实测被地板托住；种子不再参与。
        assert_eq!(
            paced_interval(Duration::from_millis(500), secs(300), secs(30), 20.0),
            secs(30)
        );
    }

    #[test]
    fn a_factor_below_one_does_not_make_the_loop_faster_than_a_pass() {
        // 倍率 0 会把间隔压到零，让这个函数变成一个忙等循环的许可；小于 1 的
        // 倍率按 1 处理，间隔至少覆盖这一轮本身。
        assert_eq!(
            paced_interval(secs(10), Duration::ZERO, Duration::ZERO, 0.0),
            secs(10)
        );
    }

    #[test]
    fn a_tail_budget_follows_the_measured_tail() {
        let mut window = DurationWindow::new(100);
        for i in 1..=100 {
            window.record(Duration::from_millis(i * 10));
        }
        // 尾部 99 分位是 990ms，倍率 2 得 1980ms——种子不参与有样本之后的答案。
        assert_eq!(
            tail_budget(&window, 0.99, Duration::ZERO, 2.0),
            Some(Duration::from_millis(1980))
        );
        // 实测变长，界跟着尾部走：同一个函数换一份输入就是另一个界。
        let mut slower = DurationWindow::new(100);
        slower.record(secs(10));
        assert_eq!(
            tail_budget(&slower, 0.99, Duration::ZERO, 2.0),
            Some(secs(20))
        );
        // 地板托住一次很短的实测。
        assert_eq!(tail_budget(&window, 0.99, secs(5), 2.0), Some(secs(5)));
    }

    #[test]
    fn a_window_returns_nothing_before_it_has_samples() {
        let window = DurationWindow::new(4);
        assert_eq!(window.quantile(0.99), None);
        assert_eq!(tail_budget(&window, 0.99, secs(1), 2.0), None);
    }

    #[test]
    fn a_tail_budget_speaks_the_seed_until_a_round_is_measured_and_then_follows() {
        let readings = Arc::new(PacingReadings::new());
        let budget = readings.tail("test.timeout", secs(300), Duration::ZERO, 2.0);

        // 一轮都还没量到：申报的是种子，求值次数是零。
        assert_eq!(budget.target(), secs(300));
        assert_eq!(budget.evaluations(), 0);

        // 第一轮 40 秒：界变成 80 秒，种子退出。
        assert_eq!(budget.observe(secs(40)), secs(80));
        assert_eq!(readings.target("test.timeout"), Some((secs(80), 1, true)));

        // 输入变了：同一轮跑到 200 秒，下一轮的界跟着变成 400 秒。
        assert_eq!(budget.observe(secs(200)), secs(400));
        assert_eq!(readings.target("test.timeout").unwrap().1, 2);

        // 输入不变时界不变，但求值次数继续走——回路在跑的证据。
        assert_eq!(budget.observe(secs(200)), secs(400));
        assert_eq!(budget.evaluations(), 3);
        assert_eq!(budget.samples(), 3);
    }

    #[test]
    fn a_window_keeps_only_its_most_recent_samples() {
        let mut window = DurationWindow::new(3);
        for i in 1..=5 {
            window.record(secs(i));
        }
        assert_eq!(window.len(), 3);
        // 最早的两笔已经挤出窗口：尾部只回答最近发生了什么。
        assert_eq!(window.quantile(0.0), Some(secs(3)));
        assert_eq!(window.quantile(1.0), Some(secs(5)));
    }

    #[test]
    fn the_quantile_is_a_sample_that_really_happened() {
        let mut window = DurationWindow::new(10);
        for ms in [10, 20, 30, 40] {
            window.record(Duration::from_millis(ms));
        }
        // 最近秩：前一半只可能是 20ms，后一半只可能是 40ms，不出现 25ms。
        assert_eq!(window.quantile(0.5), Some(Duration::from_millis(20)));
        assert_eq!(window.quantile(0.51), Some(Duration::from_millis(30)));
        assert_eq!(window.quantile(1.0), Some(Duration::from_millis(40)));
    }

    #[test]
    fn the_target_follows_a_changed_pass_on_the_next_beat() {
        let readings = Arc::new(PacingReadings::new());
        let pacer = readings.pacer("test.poll", secs(300), secs(30), 1.0);

        // 第一拍还没有实测：目标是种子，来源是 seed。
        assert_eq!(pacer.beat(None), secs(300));
        assert_eq!(readings.target("test.poll"), Some((secs(300), 1, false)));

        // 第二轮实测 3 秒，被地板托住；这一拍之后来源变成 derived。
        assert_eq!(pacer.beat(Some(secs(3))), secs(30));
        assert_eq!(readings.target("test.poll"), Some((secs(30), 2, true)));

        // 输入变了：同一轮跑 1000 秒，下一拍的目标必须跟上。
        assert_eq!(pacer.beat(Some(secs(1000))), secs(1000));
        let (target, evaluations, derived) = readings.target("test.poll").unwrap();
        assert_eq!(target, secs(1000));
        assert_eq!(evaluations, 3);
        assert!(derived);

        // 输入不变时目标不变，但求值计数继续走：这就是「回路在跑」的证据。
        assert_eq!(pacer.beat(Some(secs(1000))), secs(1000));
        assert_eq!(readings.target("test.poll").unwrap().1, 4);
    }

    /// 连续失败次数是输入：第一次是种子，第二次拉开，恢复后回到种子；每一拍
    /// 都留一个求值计数，次数不变时值不变才是正确行为。
    #[test]
    fn a_backoff_stretches_with_the_count_and_returns_to_its_base() {
        let readings = Arc::new(PacingReadings::new());
        let backoff = readings.backoff("test.cooldown", secs(3600), 2.0);

        assert_eq!(backoff.evaluate(1), secs(3600));
        assert_eq!(backoff.evaluate(2), secs(7200));
        assert_eq!(backoff.evaluate(3), secs(14_400));
        let (target, evaluations, derived) = readings.target("test.cooldown").unwrap();
        assert_eq!(target, secs(14_400));
        assert_eq!(evaluations, 3);
        assert!(derived);

        // 一次成功把次数清零：回到种子，但这一拍照样算过。
        assert_eq!(backoff.evaluate(0), secs(3600));
        assert_eq!(readings.target("test.cooldown").unwrap().1, 4);
    }

    #[test]
    fn a_floor_holds_a_seed_that_was_set_below_it() {
        let readings = Arc::new(PacingReadings::new());
        let pacer = readings.pacer("test.floor", secs(5), secs(30), 1.0);
        assert_eq!(pacer.touch(), secs(30));
    }

    #[tokio::test]
    async fn the_published_series_carry_the_target_and_the_evaluation_count() {
        let readings = Arc::new(PacingReadings::new());
        let pacer = readings.pacer("test.published", secs(60), Duration::ZERO, 2.0);
        pacer.beat(Some(secs(10)));
        pacer.beat(Some(secs(40)));

        let metrics = readings.collect_metrics("").await.unwrap();
        let target = metrics
            .iter()
            .find(|m| m.name == TARGET_METRIC)
            .expect("target series");
        // 40 秒的实测 × 2 = 80 秒，越过 60 秒的种子。
        assert_eq!(target.value, 80.0);
        assert!(target
            .labels
            .iter()
            .any(|(k, v)| *k == SOURCE_LABEL && v == SOURCE_DERIVED));

        let evaluations = metrics
            .iter()
            .find(|m| m.name == EVALUATIONS_METRIC)
            .expect("evaluation series");
        assert_eq!(evaluations.value, 2.0);
        assert!(evaluations
            .labels
            .iter()
            .any(|(k, v)| *k == PACING_LABEL && v == "test.published"));
    }
}
