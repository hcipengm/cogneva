//! 车道许可的内存导出：界从读数来，不从写下它的那天来。
//!
//! 同时能跑几条变更，取决于两个都在动的事实：容器现在能拿多少内存（部署那一刻
//! 的 `memory.max`），和一条任务实际吃多少（本进程实测的足迹）。把界写死在配置里，
//! 记下的是「写它那天的容器」，之后任何一边变了都不会有人知道界已经不对。
//!
//! 所以这里的产物不是个数，是一条控制律：每窗重读 `memory.max` 与 `memory.current`，
//! 由两个读数算出这一窗的上界，再从在用的界一步走去那儿。下降立即（安全方向），
//! 上升要两条凭据：算术说还有余量，且**当前这个界被真的跑满过一拍**——半分钟一拍，
//! 只按算术爬，从 1 到 6 只要两分半，而一次构建的峰值可以在这两分半里任何一刻到来。
//!
//! 价格本身也有保鲜期：只用最近一小时的样本算。价格不衰减的版本在第一次大构建之后
//! 就跟写死一个数没有区别，只是那个数要等到进程重启才会被重新量一次。
//!
//! 读数从哪里来这件事本身就是这条律的一半：Pod 里 `/sys/fs/cgroup` 是宿主的整个
//! 层级，容器的上限写在 `/proc/self/cgroup` 指的那一格下面（实测 2026-11-07）。
//! 只读挂载点的版本读到的永远是「没有上限」，律一格也不会动。
//!
//! 控制律的形状与 [`cog_reflection::build_gate`] 的 `next_bound` 同一条：那里是宿主级
//! 构建槽，这里是进程内车道许可，两者都是「一台机器能同时承受几件事」，所以不该
//! 有两套不同的算术。跨 crate 复用得先把那条控制律搬进契约层，这一笔没做；先让两边
//! 逐字同形，搬的时候是替换而不是重写。

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// 内核告诉进程「你在哪一格」的那份文件。
pub const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

/// 一次读数的位置：先按 v2 的布局找，找不到再按 v1 的目录名找。
///
/// 参数是**一格 cgroup 的目录**，不是挂载点本身：v2 的一格里，`memory.max` 与
/// `memory.current` 就在目录下；v1 的那格里，它们在 `memory/` 子目录里。
///
/// 返回的顺序就是尝试的顺序。空列表＝这个进程不在任何有上限的 cgroup 里，
/// 调用方据此**不动**界（不是把界抬到无限），因为「量不到」与「没有上限」
/// 在这里是两件事：前者该保持原样，后者才该由别的维度决定。
pub fn memory_files(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    vec![
        (dir.join(MEMORY_MAX_FILE), dir.join(MEMORY_CURRENT_FILE)),
        (
            dir.join("memory").join("memory.limit_in_bytes"),
            dir.join("memory").join("memory.usage_in_bytes"),
        ),
    ]
}

/// `/proc/self/cgroup` 正文里 v2 统一层那一行的路径（如 `/kubepods.slice/...`）。
///
/// 只认 `0::` 这一行：v2 把全部控制器放在同一层，行首的 `0` 后面没有控制器名单。
/// v1 的每一行都带一个控制器名，取哪一行都只是那一类控制器的那一格——内存那一格
/// 由 [`own_cgroup_dir`] 的 v1 兜底去读，不在这里猜。
pub fn unified_cgroup_path(contents: &str) -> Option<String> {
    contents
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(|path| path.trim().to_string())
        .filter(|path| path.starts_with('/'))
}

/// 本进程所在的那一格 cgroup 的目录（绝对路径）。
///
/// 为什么必须解析这一格：`/sys/fs/cgroup` 在 Pod 里通常**不是**容器的根，而是宿主
/// 的整个层级。2026-11-07 实测进化 Pod 的容器里 `/sys/fs/cgroup/memory.max` 根本
/// 不存在（宿主根没有上限），而 `/proc/self/cgroup` 指向的那一格写着实测 16 GiB。
/// 把挂载点当成容器根去读，读到的永远是「没有上限」，控制律于是一格也没动过——
/// 这正是「配了导出、量到的却是量不到」的那种坏法。
pub fn own_cgroup_dir(root: &Path, proc_file: &Path) -> Option<PathBuf> {
    let raw = std::fs::read_to_string(proc_file).ok()?;
    let rel = unified_cgroup_path(&raw)?;
    Some(root.join(rel.trim_start_matches('/')))
}

/// 读这一族读数时可以依次尝试的目录。
///
/// 挂载点本身排第一：容器自己的 cgroup 被名字空间化时（`/sys/fs/cgroup` 就是
/// 容器根），那才是对的一格，而它会先被找到。挂载点读不到，再退到本进程那一格。
/// 两处都没有就是真的量不到。
pub fn memory_dirs(root: &Path, proc_file: &Path) -> Vec<PathBuf> {
    let mut dirs = vec![root.to_path_buf()];
    if let Some(own) = own_cgroup_dir(root, proc_file) {
        if own != root {
            dirs.push(own);
        }
    }
    dirs
}

/// 这一格有没有内存读数可读：v2 的上限文件或 v1 的上限文件，任意一个有就算。
fn has_memory_files(dir: &Path) -> bool {
    memory_files(dir)
        .iter()
        .any(|(max, _)| std::fs::metadata(max).is_ok())
}

/// 这一次读数该在哪一格：第一个带内存文件的候选目录。
///
/// 上限与占用必须从**同一格**读：两个读者各自找第一个读得到的文件，会在候选之间
/// 拼出一个上限来自宿主根、占用来自容器格的读数——两个不相干的数算出来的比值，
/// 比量不到更坏，因为它看起来像一个数。
pub fn memory_dir_for(root: &Path, proc_file: &Path) -> Option<PathBuf> {
    memory_dirs(root, proc_file)
        .into_iter()
        .find(|dir| has_memory_files(dir))
}

/// 读容器内存上限（字节）。`max` 或不认识的内容都读成 `None`。
pub fn read_memory_max(root: &Path) -> Option<u64> {
    read_memory_max_from(root, Path::new(PROC_SELF_CGROUP))
}

/// 同 [`read_memory_max`]，但由调用方指定 `/proc/self/cgroup` 的位置。
pub fn read_memory_max_from(root: &Path, proc_file: &Path) -> Option<u64> {
    let dir = memory_dir_for(root, proc_file)?;
    for (max, _) in memory_files(&dir) {
        if let Ok(raw) = std::fs::read_to_string(&max) {
            return raw.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0);
        }
    }
    None
}

/// 读容器当前占用（字节）。
pub fn read_memory_current(root: &Path) -> Option<u64> {
    read_memory_current_from(root, Path::new(PROC_SELF_CGROUP))
}

/// 同 [`read_memory_current`]，但由调用方指定 `/proc/self/cgroup` 的位置。
pub fn read_memory_current_from(root: &Path, proc_file: &Path) -> Option<u64> {
    let dir = memory_dir_for(root, proc_file)?;
    for (_, current) in memory_files(&dir) {
        if let Ok(raw) = std::fs::read_to_string(&current) {
            return raw.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0);
        }
    }
    None
}

/// 价格的保鲜窗：一条任务的价格只在这么新的样本上算。
///
/// 一条变更任务的寿命是十分钟量级（实测 2026-11-07 变更验证的服务时长均值
/// 12.8 分钟），一小时里约五条：窗比这更短，经常一条忙碌样本都没有，价格退回
/// 「没量到」、界掉回 1 又爬回来，来回抖；比这更长，窗里压着一条早已过去的大
/// 构建，界被一个当下不会再发生的事件钉死，直到进程重启才松——那与写死一个数
/// 只差一个重启周期。
// cap: bound — 实测一条变更任务 12.8 分钟，一小时约五条
pub const PRICE_WINDOW: Duration = Duration::from_secs(3600);

/// 一个样本：那一刻容器占了多少，同时有几条任务在跑。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sample {
    at: Instant,
    current: u64,
    in_flight: usize,
}

/// 本进程最近这一窗的足迹。
///
/// 存样本而不是存几个累计量，是因为价格要按「那一刻几条在跑」摊：两条并发时
/// 多出来的那一份，是一条任务的证据，不是一条两倍大的任务的证据。按后者算，
/// 界在 N 条时把 N 条的总加量当成一条的价格，`ceiling ≈ 容器能装几条 / N`，
/// 抬到平方根就停住（能装 6 条却稳在 2），而且停得看起来像内存真的不够。
#[derive(Clone, Debug, Default)]
pub struct Footprint {
    window: VecDeque<Sample>,
}

impl Footprint {
    /// 收一个样本，并把比 [`PRICE_WINDOW`](PRICE_WINDOW) 更老的样本丢掉。
    pub fn observe(&mut self, current: u64, in_flight: usize, at: Instant) {
        self.window.push_back(Sample {
            at,
            current,
            in_flight,
        });
        self.forget_before(at);
    }

    /// 丢掉 `at - PRICE_WINDOW` 之前的样本。时间倒着走（同一拍的样本时间相同）
    /// 时什么都不丢：这一窗的读数比丢样本更值钱。
    pub fn forget_before(&mut self, at: Instant) {
        while let Some(front) = self.window.front() {
            match at.checked_duration_since(front.at) {
                Some(age) if age > PRICE_WINDOW => {
                    self.window.pop_front();
                }
                _ => break,
            }
        }
    }

    /// 窗里有几个样本。
    pub fn samples(&self) -> usize {
        self.window.len()
    }

    /// 空载基线：窗里见过的最低占用。
    ///
    /// 取全窗最小值而不是只取空闲样本，是因为这条车道被实测连续六小时跑满 1/1，
    /// 那种窗口里一个空闲样本都采不到，只有空闲基线会让界永远停在 1——而停住的
    /// 理由不是「容器装不下第二条」，是「没量到」。最小值是保守的那一侧：基线
    /// 估低，价格就估高，界只会偏小。
    pub fn baseline(&self) -> u64 {
        self.window
            .iter()
            .map(|sample| sample.current)
            .min()
            .unwrap_or(0)
    }

    /// 一条任务的价格：窗里每条忙碌样本的增量除以那一刻在跑几条，取最大。
    ///
    /// 没有忙碌样本时返回 `None`——那不是零，是这一窗没量到。
    pub fn per_task_bytes(&self) -> Option<u64> {
        let baseline = self.baseline();
        if baseline == 0 {
            return None;
        }
        self.window
            .iter()
            .filter(|sample| sample.in_flight > 0)
            .map(|sample| {
                let extra = sample.current.saturating_sub(baseline);
                let lanes = sample.in_flight as u64;
                // 向上取整：少算一条任务的价钱就是多放进一条任务。
                extra.div_ceil(lanes)
            })
            .filter(|per_task| *per_task > 0)
            .max()
    }
}

/// 这一拍的界：拿掉常驻开销之后，容器还装得下几条任务的价格。
///
/// 还没量到价格时返回 1：不是保守取整，是因为「不知道一条任务多贵」时
/// 唯一能确定的数就是「至少一条能跑」——这条车道不能因为一次读失败就停摆。
pub fn derive_ceiling(limit: u64, footprint: &Footprint) -> usize {
    let Some(per_task) = footprint.per_task_bytes() else {
        return 1;
    };
    let headroom = limit.saturating_sub(footprint.baseline());
    ((headroom / per_task) as usize).max(1)
}

/// 这一拍该不该往上爬一格。
///
/// 只有一条证据算数：**这个界被真的用满过，而且用满的那一拍仍在容器上限之下**。
/// 算术上还有位置不等于试过还有位置——`memory.current` 在界为 1 时只看得到一条
/// 任务的价格，那时候算出来的「还装得下五条」是把没发生过的并发外推出来的数。
///
/// 用排除法的代价量过：本循环半分钟一拍，按「有空位就爬」写法从 1 爬到 6 只需
/// 两分半，而一次构建的峰值（实测 15.6 GiB / 16 GiB）完全可以在那两分半里的任何
/// 一刻到来。按「用满过才爬」写法，从 1 到 2 要等一条任务真的在跑完一整拍，
/// 从 2 到 3 要等两条同时跑满一整拍——每一步都有一拍变满的代价垫着，
/// 而收缩仍然是立即的。
pub fn should_grow(bound: usize, ceiling: usize, ran_full: bool) -> bool {
    ceiling > bound && ran_full
}

/// 下一拍用哪个界。
///
/// 下降立即，上升要有凭据：这一窗的算术说还有位置、且这个界被用满过时爬一格，
/// 否则原地；算术说比现在少就当场降到它。与 `build_gate` 的 `next_bound` 同形
/// （那里 `calm` 是「宿主安静」，这里是「用满的那一拍仍然装得下」）。
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
            // 这个界有没有被真的用满过。爬一格的前提（见 [`should_grow`]），
            // 每爬一格清一次：新界要重新挣一次「用满还在上限之下」。
            let mut ran_full = false;
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
                    footprint.observe(current, in_flight, std::time::Instant::now());
                    if in_flight >= capacity {
                        ran_full = true;
                    }
                    let ceiling = derive_ceiling(limit, &footprint);
                    let want =
                        next_bound(capacity, ceiling, should_grow(capacity, ceiling, ran_full));
                    let applied = apply_bound(&slots, capacity, want);
                    if applied > capacity {
                        ran_full = false;
                    }
                    if applied != capacity {
                        debug!(
                            stream = %stream,
                            from = capacity,
                            to = applied,
                            ceiling,
                            limit,
                            current,
                            baseline = footprint.baseline(),
                            per_task = footprint.per_task_bytes(),
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

    /// 例如「空载 3 GiB、一条任务 12 GiB」的足迹，采样时刻都落在同一个窗口里。
    fn footprint(idle: u64, busy: u64, in_flight: usize) -> Footprint {
        let at = Instant::now();
        let mut footprint = Footprint::default();
        footprint.observe(idle, 0, at);
        footprint.observe(busy, in_flight, at + Duration::from_secs(30));
        footprint
    }

    /// 界必须跟着容器上限走：同一个足迹下把 `memory.max` 抬上去，导出值要跟着抬。
    ///
    /// 这就是那条原则的判据——改动前界是配置里的一个数，`memory.max` 怎么改它都不动。
    /// 数字来源：实测 2026-11-07 进化 Pod 空闲基线 3.02 GiB、构建期峰值 15.6 GiB，
    /// 容器上限 16 GiB（同一组实测）。
    #[test]
    fn the_bound_follows_the_container_limit() {
        let dir = fixture(&[(MEMORY_MAX_FILE, "17179869184\n")]);
        assert_eq!(read_memory_max(dir.path()), Some(16 * GIB));

        let footprint = footprint(3 * GIB, 15 * GIB, 1);
        assert_eq!(derive_ceiling(16 * GIB, &footprint), 1);
        assert_eq!(derive_ceiling(32 * GIB, &footprint), 2);
        assert_eq!(derive_ceiling(48 * GIB, &footprint), 3);

        // 同一份夹具，把上限抬上去之后再读一次，导出的界必须跟着抬。
        std::fs::write(dir.path().join(MEMORY_MAX_FILE), "34359738368\n").expect("raise limit");
        let raised = read_memory_max(dir.path()).expect("limit");
        assert!(
            derive_ceiling(raised, &footprint) > derive_ceiling(16 * GIB, &footprint),
            "抬了容器的 memory.max，导出的界却没跟着抬"
        );
    }

    /// 两条并发时多出来的量是两条任务的价格，不是一条的。
    ///
    /// 不摊的版本把「两条一起多出来的那一份」记成每条任务的价格，`ceiling` 于是被
    /// 自己的界除了一次：容器装得下三条时算出装得下 1.5 条，界稳在 1 而不是 3。
    /// 这一条红的形状是「lane 明显还有余量，界却一直不涨」。
    #[test]
    fn a_price_measured_at_two_lanes_is_the_increment_of_one() {
        let two_at_once = footprint(2 * GIB, 14 * GIB, 2);
        assert_eq!(two_at_once.per_task_bytes(), Some(6 * GIB));
        // 例如上限 20 GiB、基线 2 GiB ⇒ 余量 18 GiB 装得下三条 6 GiB。
        assert_eq!(derive_ceiling(20 * GIB, &two_at_once), 3);
    }

    /// 价格只用窗里的样本算：一条大构建过去一小时后，不再压着界。
    ///
    /// 峰值不衰减的版本会让第一次大构建把这个进程终生钉在 1，直到重启才重测——
    /// 那与写死一个数只差一个重启周期。
    #[test]
    fn a_burst_that_ages_out_stops_pricing_the_lane() {
        let start = Instant::now();
        let mut footprint = Footprint::default();
        footprint.observe(3 * GIB, 0, start);
        footprint.observe(15 * GIB, 1, start + Duration::from_secs(30));
        assert_eq!(footprint.per_task_bytes(), Some(12 * GIB));
        assert_eq!(derive_ceiling(16 * GIB, &footprint), 1);

        // 一小时后：大构建的样本出了窗，窗里只剩便宜的样本。
        let later = start + PRICE_WINDOW + Duration::from_secs(60);
        footprint.observe(3 * GIB, 0, later);
        footprint.observe(6 * GIB, 1, later + Duration::from_secs(30));
        assert_eq!(footprint.samples(), 2, "旧样本必须真的被丢掉");
        assert_eq!(footprint.per_task_bytes(), Some(3 * GIB));
        assert!(derive_ceiling(16 * GIB, &footprint) > 1);
    }

    /// 整窗一条忙碌样本都没有＝这一窗没量到价格，而不是「价格为零」。
    #[test]
    fn a_window_with_no_busy_sample_carries_no_price() {
        let at = Instant::now();
        let mut footprint = Footprint::default();
        footprint.observe(3 * GIB, 0, at);
        footprint.observe(4 * GIB, 0, at + Duration::from_secs(30));
        assert_eq!(footprint.per_task_bytes(), None);
        assert_eq!(derive_ceiling(64 * GIB, &footprint), 1);
    }

    /// 不设上限（`max`）不等同于「量到了 0 字节上限」。前者没有比例可算，
    /// 后者会让除法把界抬到天花板。
    #[test]
    fn an_unlimited_container_reads_as_no_limit() {
        let dir = fixture(&[(MEMORY_MAX_FILE, "max\n")]);
        assert_eq!(read_memory_max(dir.path()), None);
    }

    /// 算术上有空位而没跑满过，不爬。
    ///
    /// 这就是「半分钟一拍从 1 爬到 6 只要两分半」那道口子的判据：界为 1 且只有一条
    /// 任务的价格时，`derive_ceiling` 外推出来的「还装得下几条」是没发生过的并发；
    /// 在它被真的跑满一拍之前，那个数只能当上限，不能当理由。
    #[test]
    fn a_bound_that_never_ran_full_does_not_climb_on_arithmetic_alone() {
        assert!(!should_grow(1, 6, false));
        assert_eq!(next_bound(1, 6, should_grow(1, 6, false)), 1);
        // 用满过一拍，才爬一格；而且只爬一格，不直接跳到 6。
        assert!(should_grow(1, 6, true));
        assert_eq!(next_bound(1, 6, should_grow(1, 6, true)), 2);
        // 没有空位时，跑满过也不爬。
        assert_eq!(next_bound(3, 3, should_grow(3, 3, true)), 3);
        // 收缩不看凭据：算术说装不下就当场降。
        assert_eq!(next_bound(6, 2, should_grow(6, 2, false)), 2);
    }

    /// 还没量到一条任务的价格之前，界是 1，不是「无限」。
    #[test]
    fn an_unmeasured_task_keeps_the_bound_at_one() {
        let footprint = Footprint::default();
        assert_eq!(footprint.per_task_bytes(), None);
        assert_eq!(derive_ceiling(64 * GIB, &footprint), 1);
    }

    /// 连续满载、采不到空闲样本时，用见过的全局最小值兜底：
    /// 否则这条车道会永远停在 1，而停住的理由不是容器装不下两条。
    #[test]
    fn a_lane_that_never_sits_idle_still_measures_its_floor() {
        let at = Instant::now();
        let mut footprint = Footprint::default();
        footprint.observe(15 * GIB, 1, at);
        footprint.observe(5 * GIB, 1, at + Duration::from_secs(30));
        footprint.observe(15 * GIB, 1, at + Duration::from_secs(60));
        assert_eq!(footprint.baseline(), 5 * GIB);
        assert_eq!(footprint.per_task_bytes(), Some(10 * GIB));
        assert_eq!(derive_ceiling(32 * GIB, &footprint), 2);
    }

    /// 基线是全窗最小值：第一个任务的峰值不能被记成基线，空闲样本一落下就接过来。
    #[test]
    fn the_baseline_is_the_lowest_reading_in_the_window() {
        let at = Instant::now();
        let mut footprint = Footprint::default();
        footprint.observe(12 * GIB, 1, at);
        assert_eq!(footprint.baseline(), 12 * GIB, "还没有更低的读数");
        footprint.observe(3 * GIB, 0, at + Duration::from_secs(30));
        footprint.observe(2 * GIB, 0, at + Duration::from_secs(60));
        assert_eq!(footprint.baseline(), 2 * GIB);
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

    const OWN: &str =
        "/kubepods.slice/kubepods-burstable.slice/pod-1.slice/cri-containerd-abc.scope";

    /// Pod 里 `/sys/fs/cgroup` 是宿主的整个层级，本进程那一格在它下面；上限写在那里。
    ///
    /// 这就是 2026-11-07 在进化 Pod 里实测到的形状：挂载点下没有 `memory.max`，
    /// 而 `/proc/self/cgroup` 指的那一格写着实测 16 GiB。只读挂载点的版本读到的
    /// 永远是「没有上限」，控制律于是从来没动过界——导出在跑，读数却没量到。
    #[test]
    fn the_processes_own_cgroup_is_read_when_the_mount_point_carries_no_limit() {
        let root = tempfile::tempdir().expect("root");
        let own = root.path().join(OWN.trim_start_matches('/'));
        std::fs::create_dir_all(&own).expect("own dir");
        std::fs::write(own.join(MEMORY_MAX_FILE), "17179869184\n").expect("max");
        std::fs::write(own.join(MEMORY_CURRENT_FILE), "1309966336\n").expect("current");
        let proc_file = root.path().join("proc-self-cgroup");
        std::fs::write(&proc_file, format!("0::{OWN}\n")).expect("proc");

        assert_eq!(
            read_memory_max_from(root.path(), &proc_file),
            Some(16 * GIB)
        );
        assert_eq!(
            read_memory_current_from(root.path(), &proc_file),
            Some(1309966336)
        );
    }

    /// 挂载点本身就被名字空间化成容器根时，直接读它——不必去解析 `/proc`。
    #[test]
    fn the_mount_point_wins_when_it_is_already_the_container_root() {
        let root = tempfile::tempdir().expect("root");
        std::fs::write(root.path().join(MEMORY_MAX_FILE), "8589934592\n").expect("max");
        let own = root.path().join(OWN.trim_start_matches('/'));
        std::fs::create_dir_all(&own).expect("own dir");
        std::fs::write(own.join(MEMORY_MAX_FILE), "17179869184\n").expect("own max");
        let proc_file = root.path().join("proc-self-cgroup");
        std::fs::write(&proc_file, format!("0::{OWN}\n")).expect("proc");

        assert_eq!(read_memory_max_from(root.path(), &proc_file), Some(8 * GIB));
    }

    /// `/proc/self/cgroup` 里没有 `0::` 那一行＝这不是 v2 的统一层。
    ///
    /// 读成「本进程那一格未知」而不是「路径是空串」：空串拼出的目录就是挂载点
    /// 本身，那会把「解析失败」变成一次对宿主根的重读，而宿主根的上限与容器的
    /// 上限不是同一件事。
    #[test]
    fn a_proc_file_with_no_unified_line_names_no_directory() {
        assert_eq!(unified_cgroup_path("/\n"), None);
        assert_eq!(
            unified_cgroup_path("12:memory:/docker/abc\n11:cpu:/docker/abc\n"),
            None
        );
        assert_eq!(
            unified_cgroup_path("0::/kubepods.slice/pod-1\n"),
            Some("/kubepods.slice/pod-1".to_string())
        );

        let root = tempfile::tempdir().expect("root");
        let proc_file = root.path().join("proc-self-cgroup");
        std::fs::write(&proc_file, "12:memory:/docker/abc\n").expect("proc");
        assert_eq!(read_memory_max_from(root.path(), &proc_file), None);
    }
}
