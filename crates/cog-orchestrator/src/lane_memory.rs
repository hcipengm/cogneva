//! 车道许可的内存导出：界从读数来，不从写下它的那天来。
//!
//! 同时能跑几条变更，取决于两个都在动的事实：容器现在能拿多少内存（部署那一刻
//! 的 `memory.max`），和一条任务实际吃多少（本进程实测的足迹）。把界写死在配置里，
//! 记下的是「写它那天的容器」，之后任何一边变了都不会有人知道界已经不对。
//!
//! 所以这里的产物不是个数，是一条控制律：每窗重读 `memory.max` 与 `memory.current`，
//! 由两个读数算出这一窗的上界，再从在用的界一步走去那儿。下降立即（安全方向），
//! 上升每窗一步（一条被一次性抬到顶的界，第二次读数错了也收不回来）。
//!
//! 控制律的形状与 [`cog_reflection::build_gate`] 的 `next_bound` 同一条：那里是宿主级
//! 构建槽，这里是进程内车道许可，两者都是「一台机器能同时承受几件事」，所以不该
//! 有两套不同的算术。跨 crate 复用得先把那条控制律搬进契约层，这一笔没做；先让两边
//! 逐字同形，搬的时候是替换而不是重写。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

/// 容器自己在 cgroup 里的根。cgroup v2 挂在根上；v1 的路径由 [`memory_files`] 兜底。
pub const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// 容器内存上限。内容是字节数，或者字面量 `max`（不设上限）。
pub const MEMORY_MAX_FILE: &str = "memory.max";

/// 容器当前占用的内存，字节数。比 `memory.usage_in_bytes` 少一层缓存噪声，
/// v2 里它就是这一族的常用读数。
pub const MEMORY_CURRENT_FILE: &str = "memory.current";

/// 采样周期。
///
/// 一条变更任务的寿命是十分钟量级（实测 2026-11-07 变更验证的服务时长均值
/// 12.8 分钟），半分钟一采，一条任务的生命里落二十来个样本，够看见它的峰值；
/// 再密就是在读一个半小时都不变的数。
// cap: bound — 半分钟一采，一条任务里二十来个样本；更密只是重复读同一个数
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

/// 一次读数的位置：先按 v2 的布局找，找不到再按 v1 的目录名找。
///
/// 返回的顺序就是尝试的顺序。空列表＝这个进程不在任何有上限的 cgroup 里，
/// 调用方据此**不动**界（不是把界抬到无限），因为「量不到」与「没有上限」
/// 在这里是两件事：前者该保持原样，后者才该由别的维度决定。
pub fn memory_files(root: &Path) -> Vec<(PathBuf, PathBuf)> {
    vec![
        (root.join(MEMORY_MAX_FILE), root.join(MEMORY_CURRENT_FILE)),
        (
            root.join("memory").join("memory.limit_in_bytes"),
            root.join("memory").join("memory.usage_in_bytes"),
        ),
    ]
}

/// 读容器内存上限（字节）。`max` 或不认识的内容都读成 `None`。
pub fn read_memory_max(root: &Path) -> Option<u64> {
    for (max, _) in memory_files(root) {
        if let Ok(raw) = std::fs::read_to_string(&max) {
            return raw.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0);
        }
    }
    None
}

/// 读容器当前占用（字节）。
pub fn read_memory_current(root: &Path) -> Option<u64> {
    for (_, current) in memory_files(root) {
        if let Ok(raw) = std::fs::read_to_string(&current) {
            return raw.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0);
        }
    }
    None
}

/// 本进程的足迹：空闲时多低，干活时多高。
///
/// 两个数分开记，是因为决定「还能再塞几条」的是差值。只看峰值会把进程自己的
/// 常驻开销当成一条任务的价格，界就永远是一；只看均值会把峰值那一窗抹平，
/// 而撑爆容器的正是峰值那一窗。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Footprint {
    /// 没有在途任务时见到的最小值。第一次见到之前是 0（未知）。
    pub idle: u64,
    /// 不论有没有在途任务，见到的最小值。
    ///
    /// 它是空载基线的兜底：这条车道被实测连续六小时满载 1/1，那种窗口里一个空闲
    /// 样本都采不到，只有空闲基线会让界永远停在 1——而那时它停住的理由不是
    /// 「容器装不下第二条」，是「没量到」。最小值是保守的那一侧（把常驻开销估低，
    /// 一条任务的价格就估高，界只会偏小）。
    pub floor: u64,
    /// 有在途任务时见过的最大值，进程生命周期内不衰减。
    ///
    /// 不按窗归零，是这条控制律的安全方向：价格按「见过的最贵一条」算，只会估高
    /// 不会估低，于是界只会偏小。按窗归零的版本在没构建的那一窗算出「价格未知」，
    /// 界掉回 1、下一窗又爬回来，而爬回来的那一刻正是第二条任务被放进去、随后被
    /// 一次重构建顶爆容器的时刻。重启会清掉这个最大值，那也是一次重新测量。
    pub peak: u64,
}

impl Footprint {
    /// 收一个样本。`in_flight` 是这个样本落下时有几条任务在跑。
    ///
    /// 空闲基线只由空闲样本决定，否则第一个任务的峰值会被记成基线，
    /// 之后再高的峰值都算不出价格来。
    pub fn observe(&mut self, current: u64, in_flight: usize) {
        self.floor = if self.floor == 0 {
            current
        } else {
            self.floor.min(current)
        };
        if in_flight == 0 {
            self.idle = if self.idle == 0 {
                current
            } else {
                self.idle.min(current)
            };
        } else {
            self.peak = self.peak.max(current);
        }
    }

    /// 算价格用的空载基线：采到过空闲样本就用它，没有就用见过的全局最小值。
    pub fn baseline(&self) -> u64 {
        if self.idle > 0 {
            self.idle
        } else {
            self.floor
        }
    }

    /// 一条任务的价格。没量到过「有任务时比空载高多少」时返回 `None`——
    /// 那不是零，是还没量。
    pub fn per_task_bytes(&self) -> Option<u64> {
        let baseline = self.baseline();
        if baseline == 0 || self.peak == 0 || self.peak <= baseline {
            return None;
        }
        Some(self.peak - baseline)
    }
}

/// 这一拍的界：拿掉常驻开销之后，容器还装得下几条任务的价格。
///
/// 还没量到价格时返回 1：不是保守取整，是因为「不知道一条任务多贵」时
/// 唯一能确定的数就是「至少一条能跑」——这条车道不能因为一次读失败就停摆。
pub fn derive_ceiling(limit: u64, footprint: Footprint) -> usize {
    let Some(per_task) = footprint.per_task_bytes() else {
        return 1;
    };
    let headroom = limit.saturating_sub(footprint.baseline());
    ((headroom / per_task) as usize).max(1)
}

/// 下一拍用哪个界。
///
/// 下降立即，上升每窗一步：这一窗的算术说还有位置时爬一格，说没有就原地，
/// 说比现在少就当场降到它。与 `build_gate` 的 `next_bound` 同形（那里
/// `calm` 是「宿主安静」，这里是「这一窗的余量装得下下一条」）。
pub fn next_bound(bound: usize, ceiling: usize, room_to_grow: bool) -> usize {
    if room_to_grow {
        bound.saturating_add(1).min(ceiling)
    } else {
        bound.min(ceiling)
    }
    .max(1)
}

/// 把在用的许可调到位。
///
/// 涨用 `add_permits`，缩用 `forget_permits`：后者只能收回**当前空闲**的那几条，
/// 全在跑的窗口收回 0 条——那时返回的差额留给下一窗，而不是假装界已经降了
/// （读数面报的是真正在用的界）。
pub fn apply_bound(slots: &tokio::sync::Semaphore, current: usize, want: usize) -> usize {
    if want > current {
        slots.add_permits(want - current);
        want
    } else if want < current {
        let dropped = slots.forget_permits(current - want);
        current - dropped
    } else {
        current
    }
}

/// 车道许可的内存导出循环。
///
/// `stream` 是读数面上这条车道的那一行：导出的界必须写回它，否则「配了几条」
/// 与「真正许可几条」又分成了两个数，而这正是写死那个数时没人看得见它的原因。
pub fn spawn_lane_memory_bound(
    stream: String,
    slots: Arc<tokio::sync::Semaphore>,
    bound: usize,
    shutdown: cog_core::ShutdownSignal,
    cgroup_root: PathBuf,
) {
    if read_memory_max(&cgroup_root).is_none() {
        warn!(
            stream = %stream,
            root = %cgroup_root.display(),
            "lane bound: the container's memory limit cannot be read, so the bound stays where it \
             started -- a bound of one because nothing was measured is not the same fact as a \
             bound of one because one task fills the container"
        );
    }
    drop(cog_core::loop_health::spawn(
        LANE_MEMORY_LOOP,
        cog_core::loop_health::Cadence::Periodic(SAMPLE_INTERVAL),
        shutdown.clone(),
        move |beat| {
            let shutdown = shutdown.clone();
            let slots = slots.clone();
            let stream = stream.clone();
            let root = cgroup_root.clone();
            // `capacity` 是真正发给信号量的许可条数；`bound` 是这一窗算出来的目标。
            // 两者在缩界时可以不相等：在跑的许可要等它们自己释放才收得回来。
            let mut capacity = bound;
            let mut footprint = Footprint::default();
            async move {
                let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // 第一格立即到点：先收一个空闲样本，再谈能装几条。
                ticker.tick().await;
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    let (Some(limit), Some(current)) =
                        (read_memory_max(&root), read_memory_current(&root))
                    else {
                        // 读不到就不动界：把「量不到」当成「没有上限」会把界
                        // 一次抬到别的维度上去，那不是这次改动想干的事。
                        continue;
                    };
                    let in_flight = capacity
                        .saturating_sub(slots.available_permits())
                        .min(capacity);
                    footprint.observe(current, in_flight);
                    let ceiling = derive_ceiling(limit, footprint);
                    let room_to_grow = ceiling > capacity;
                    let want = next_bound(capacity, ceiling, room_to_grow);
                    let applied = apply_bound(&slots, capacity, want);
                    if applied != capacity {
                        debug!(
                            stream = %stream,
                            from = capacity,
                            to = applied,
                            ceiling,
                            limit,
                            current,
                            baseline = footprint.baseline(),
                            peak = footprint.peak,
                            "lane bound moved with the container's memory"
                        );
                        crate::observable::claim_slots_observable().set_slots(&stream, applied);
                        capacity = applied;
                    }
                }
            }
        },
    ));
}

/// 这条循环在存活登记里的名字。
pub const LANE_MEMORY_LOOP: &str = "lane_memory_bound";

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for (name, body) in files {
            std::fs::write(dir.path().join(name), body).expect("write fixture");
        }
        dir
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    /// 界必须跟着容器上限走：同一个足迹下把 `memory.max` 抬上去，导出值要跟着抬。
    ///
    /// 这就是那条原则的判据——改动前界是配置里的一个数，`memory.max` 怎么改它都不动。
    /// 数字来源：实测 2026-11-07 进化 Pod 空闲基线 3.02 GiB、构建期峰值 15.6 GiB，
    /// 容器上限 16 GiB（同一组实测）。
    #[test]
    fn the_bound_follows_the_container_limit() {
        let dir = fixture(&[(MEMORY_MAX_FILE, "17179869184\n")]);
        assert_eq!(read_memory_max(dir.path()), Some(16 * GIB));

        let footprint = Footprint {
            idle: 3 * GIB,
            floor: 3 * GIB,
            peak: 15 * GIB,
        };
        assert_eq!(derive_ceiling(16 * GIB, footprint), 1);
        assert_eq!(derive_ceiling(32 * GIB, footprint), 2);
        assert_eq!(derive_ceiling(48 * GIB, footprint), 3);

        // 同一份夹具，把上限抬上去之后再读一次，导出的界必须跟着抬。
        std::fs::write(dir.path().join(MEMORY_MAX_FILE), "34359738368\n").expect("raise limit");
        let raised = read_memory_max(dir.path()).expect("limit");
        assert!(
            derive_ceiling(raised, footprint) > derive_ceiling(16 * GIB, footprint),
            "抬了容器的 memory.max，导出的界却没跟着抬"
        );
    }

    /// 不设上限（`max`）不等同于「量到了 0 字节上限」。前者没有比例可算，
    /// 后者会让除法把界抬到天花板。
    #[test]
    fn an_unlimited_container_reads_as_no_limit() {
        let dir = fixture(&[(MEMORY_MAX_FILE, "max\n")]);
        assert_eq!(read_memory_max(dir.path()), None);
    }

    /// 还没量到一条任务的价格之前，界是 1，不是「无限」。
    #[test]
    fn an_unmeasured_task_keeps_the_bound_at_one() {
        let footprint = Footprint::default();
        assert_eq!(footprint.per_task_bytes(), None);
        assert_eq!(derive_ceiling(64 * GIB, footprint), 1);
    }

    /// 连续满载、采不到空闲样本时，用见过的全局最小值兜底：
    /// 否则这条车道会永远停在 1，而停住的理由不是容器装不下两条。
    #[test]
    fn a_lane_that_never_sits_idle_still_measures_its_floor() {
        let mut footprint = Footprint::default();
        footprint.observe(15 * GIB, 1);
        footprint.observe(5 * GIB, 1);
        footprint.observe(15 * GIB, 1);
        assert_eq!(footprint.idle, 0, "一个空闲样本都没有");
        assert_eq!(footprint.baseline(), 5 * GIB);
        assert_eq!(footprint.per_task_bytes(), Some(10 * GIB));
        assert_eq!(derive_ceiling(32 * GIB, footprint), 2);
    }

    /// 空闲基线只由空闲样本决定：第一个任务的峰值不能被记成基线。
    #[test]
    fn the_idle_baseline_is_learned_only_while_idle() {
        let mut footprint = Footprint::default();
        footprint.observe(12 * GIB, 1);
        assert_eq!(footprint.idle, 0);
        footprint.observe(3 * GIB, 0);
        footprint.observe(2 * GIB, 0);
        assert_eq!(footprint.idle, 2 * GIB);
        assert_eq!(footprint.per_task_bytes(), Some(10 * GIB));
    }

    /// 下降立即，上升每窗一格。
    #[test]
    fn the_bound_falls_at_once_and_climbs_one_step() {
        assert_eq!(next_bound(4, 2, false), 2);
        assert_eq!(next_bound(2, 6, true), 3);
        assert_eq!(next_bound(1, 1, true), 1);
        assert_eq!(next_bound(1, 0, false), 1);
    }

    /// 缩界只能收回空闲的那部分，剩下的欠账由下一窗接着收。
    ///
    /// 这条是缩界的全部难点：`forget_permits` 只收得回当前空闲的许可，而在跑的
    /// 那几条释放时会把许可还回池子。所以缩界不是一次动作，是一个收敛过程——
    /// 每窗按「现在的容量 − 目标」重算一次，在跑的活干完一窗就少一格。
    #[tokio::test]
    async fn shrinking_a_busy_lane_is_paid_back_over_the_next_windows() {
        let slots = tokio::sync::Semaphore::new(3);
        let first = slots.acquire().await.expect("permit");
        let second = slots.acquire().await.expect("permit");
        // 两条在跑，只剩一条空闲：这一窗只收得回 1 条，容量 3 → 2。
        let capacity = apply_bound(&slots, 3, 1);
        assert_eq!(capacity, 2, "只有空闲的那一条能当场收回");
        // 在跑的还没结束，这一窗一条都收不回来，界停在 2 而不是假装已是 1。
        assert_eq!(apply_bound(&slots, capacity, 1), 2);
        drop(first);
        drop(second);
        // 它们释放之后，下一窗把最后一条欠账收完。
        assert_eq!(apply_bound(&slots, 2, 1), 1);
        assert_eq!(slots.available_permits(), 1);
    }

    /// v1 的布局同样读得到。
    #[test]
    fn the_v1_layout_is_read_when_v2_is_absent() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("memory")).expect("v1 dir");
        std::fs::write(
            dir.path().join("memory/memory.limit_in_bytes"),
            "8589934592\n",
        )
        .expect("limit");
        std::fs::write(
            dir.path().join("memory/memory.usage_in_bytes"),
            "1073741824\n",
        )
        .expect("usage");
        assert_eq!(read_memory_max(dir.path()), Some(8 * GIB));
        assert_eq!(read_memory_current(dir.path()), Some(GIB));
    }
}
