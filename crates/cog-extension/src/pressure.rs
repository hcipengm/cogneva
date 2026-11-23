//! How much room a replica still has, what one piece of work is measured to
//! cost, and who gets to start -- in one place.
//!
//! The same question was answered separately by the build lane (a lane count),
//! by the command path (nothing at all), by the capacity signal and by the warm
//! pool, each in its own vocabulary. A reading taken for one of them could not
//! answer another, and a number that says "one" because the container is full
//! could not be told from one that says "one" because nobody measured anything.
//!
//! The shapes here are shared, not wrappers: [`Pressure`] is the room left on
//! this replica, [`Demand`] is the measured cost of one piece of work, [`admits`]
//! is the single verdict on whether something may start, and [`Reservation`] is
//! the room one running piece of work holds until it ends.
//!
//! Nothing here stops a running command. Pressure produces waiting or capacity;
//! the only thing that ends a command is the measured silence window, and the
//! kernel stays the last line behind all of this.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use prometheus::{Encoder, GaugeVec, IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder};

use crate::runtime::cgroup::CgroupFacts;

/// How many finished commands the price of one command is read from.
///
/// In commands rather than in seconds: a command is the event being priced, and
/// commands arrive with the work. A window rather than a running maximum, so a
/// single heavy command stops pricing the container once it ages out; a maximum
/// kept since the process started is a constant with a measurement's reputation.
///
/// The width itself is the build lane's: one definition of "the recent few",
/// read over two streams. See [`crate::lane_sizing`].
pub(crate) use crate::lane_sizing::WINDOW as DEMAND_WINDOW;

/// How much room one replica still has.
///
/// Three numbers, and the two that were not read stay `None`: an unreadable
/// limit and a limit that is really "no limit" both leave `headroom` without an
/// answer, which is not the same as zero room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pressure {
    limit_bytes: Option<u64>,
    working_set_bytes: Option<u64>,
    reserved_bytes: u64,
}

impl Pressure {
    pub fn new(
        limit_bytes: Option<u64>,
        working_set_bytes: Option<u64>,
        reserved_bytes: u64,
    ) -> Self {
        Self {
            limit_bytes,
            working_set_bytes,
            reserved_bytes,
        }
    }

    /// The container as it stands right now, less what running commands have
    /// already claimed. This is the reading a command about to start sees: the
    /// working set it is measured is the live one, because whatever else is
    /// running is already in it.
    pub fn from_facts(facts: &CgroupFacts, reserved_bytes: u64) -> Self {
        Self::new(facts.limit_bytes, facts.working_set_bytes(), reserved_bytes)
    }

    pub fn limit_bytes(&self) -> Option<u64> {
        self.limit_bytes
    }

    pub fn working_set_bytes(&self) -> Option<u64> {
        self.working_set_bytes
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// The room left, or `None` when either figure it is made of was not read.
    /// Saturating: a working set above the limit reads as no room rather than
    /// as a negative one.
    pub fn headroom(&self) -> Option<u64> {
        let limit = self.limit_bytes?;
        let working = self.working_set_bytes?;
        Some(
            limit
                .saturating_sub(working)
                .saturating_sub(self.reserved_bytes),
        )
    }

    /// How many pieces of work at `demand` still fit. `None` when the room is
    /// unknown or the price is: a count invented from an unread figure is the
    /// one thing this module exists to keep out of the answer.
    pub fn fits(&self, demand: &Demand) -> Option<usize> {
        let room = self.headroom()?;
        let cost = demand.bytes().filter(|cost| *cost > 0)?;
        Some((room / cost) as usize)
    }
}

/// Where the price of one piece of work came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemandSource {
    /// The window's own reading, and how many samples it was read from.
    Measured { samples: usize },
    /// Nothing was priced yet; the name is the reading that is missing.
    Unmeasured(&'static str),
}

/// What one piece of work is measured to cost.
///
/// The window's largest sample, because an under-estimate of the price is an
/// over-estimate of how many fit: planning up to the limit means the step that
/// reaches the plan is the step the kernel kills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Demand {
    bytes: Option<u64>,
    source: DemandSource,
}

impl Demand {
    pub fn unmeasured(reading: &'static str) -> Self {
        Self {
            bytes: None,
            source: DemandSource::Unmeasured(reading),
        }
    }

    /// The price read off a window of finished commands. An empty window, or
    /// one whose samples were all zero, names what is missing rather than
    /// pricing the work at nothing.
    pub fn from_samples(samples: &[u64]) -> Self {
        match samples.iter().copied().max().filter(|peak| *peak > 0) {
            Some(peak) => Self {
                bytes: Some(peak),
                source: DemandSource::Measured {
                    samples: samples.len(),
                },
            },
            None => Self::unmeasured("no finished command has been priced"),
        }
    }

    /// The price of one build, as the build lane already measures it.
    pub fn from_peak(peak: Option<u64>) -> Self {
        match peak.filter(|peak| *peak > 0) {
            Some(peak) => Self {
                bytes: Some(peak),
                source: DemandSource::Measured { samples: 1 },
            },
            None => Self::unmeasured("cost of one build"),
        }
    }

    pub fn bytes(&self) -> Option<u64> {
        self.bytes
    }

    pub fn source(&self) -> DemandSource {
        self.source
    }
}

/// The single verdict on whether something may start now.
///
/// Three states, because they are fixed by three different things: `Admitted`
/// means the room was measured and covers the price, `Queued` means it was
/// measured and does not, and `Unmeasured` means nothing was priced or nothing
/// could be read -- the work is let through and counted, never quietly treated
/// as free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Admitted { reserved_bytes: u64 },
    Queued,
    Unmeasured(&'static str),
}

/// Whether work priced at `demand` may start against `pressure`.
///
/// Every caller that answers "can this start" asks this function: the build
/// lane, the ordinary command path, and whatever grows a queue next. The place
/// in line and the time waited belong to the queue that asks, not here.
pub fn admits(pressure: &Pressure, demand: &Demand) -> Admission {
    if let DemandSource::Unmeasured(reading) = demand.source {
        return Admission::Unmeasured(reading);
    }
    match pressure.fits(demand) {
        Some(fit) if fit >= 1 => Admission::Admitted {
            reserved_bytes: demand.bytes().unwrap_or(0),
        },
        Some(_) => Admission::Queued,
        None => Admission::Unmeasured("the container did not say how much room it has"),
    }
}

/// Where a lane landed, and why that replica.
///
/// Each reason is a number the client counted itself. `Nailed` is a pin this
/// caller already holds: the tree is on that replica's volume and moving would
/// trade it for nothing. `Headroom` is the replica carrying the fewest of this
/// caller's lanes -- fewest, not fewest per the replica's own load, which this
/// client cannot read; writing a preference for an unread figure here would be
/// dressing an assumption up as a measurement. `AddedReplica` is a replica that
/// was not in the fleet at the previous placement: the capacity was bought, not
/// found. `OnlyOne` is no choice at all, and is kept apart from the others
/// because a placement with one candidate says nothing about balance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    Nailed { replica: String },
    Headroom { replica: String },
    AddedReplica { replica: String },
    OnlyOne { replica: String },
}

impl Placement {
    pub fn replica(&self) -> &str {
        match self {
            Placement::Nailed { replica }
            | Placement::Headroom { replica }
            | Placement::AddedReplica { replica }
            | Placement::OnlyOne { replica } => replica,
        }
    }

    /// The reason as it appears on the scrape surface.
    pub fn reason(&self) -> &'static str {
        match self {
            Placement::Nailed { .. } => "nailed",
            Placement::Headroom { .. } => "headroom",
            Placement::AddedReplica { .. } => "added_replica",
            Placement::OnlyOne { .. } => "only_one",
        }
    }
}

/// Publish where a lane was placed and why. The replica itself is in the log
/// line for the lane; the counter answers "is the fleet being steered, pinned,
/// or outgrown", which is the question a reader of the scrape has.
pub fn record_placement(placement: &Placement) {
    metrics()
        .placement
        .with_label_values(&[placement.reason()])
        .inc();
}

/// 沙箱的两种形态：一份沙箱只跑一个任务，还是一副副本上跑多个任务。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxShape {
    /// 一份沙箱只跑一个任务：起停与整份卷都记在这一条任务上，没有下次可搬。
    OneTaskPerSandbox,
    /// 一副副本上跑多个任务：起停摊开、卷共用一个底，换副本搬树是它自己的代价。
    ManyTasksPerReplica,
}

impl SandboxShape {
    /// 抓取面上的名字。
    pub fn label(&self) -> &'static str {
        match self {
            SandboxShape::OneTaskPerSandbox => "one_task",
            SandboxShape::ManyTasksPerReplica => "many_tasks",
        }
    }
}

/// 两种形态计价要用的读数，全部来自这台进程量得到的东西。
///
/// 每一格量不到就是 `None`，不拍一个「大概」：缺的那条读数会由 [`sandbox_shape_cost`]
/// 点名，判决停在「没答案」，而不是拿一个偏好的默认值顶上。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ShapeReadings {
    /// 起一份沙箱的墙钟：进程起到能接命令。
    pub startup: Option<f64>,
    /// 切一棵任务树的墙钟（实测中位）。
    pub checkout: Option<f64>,
    /// 一条命令在门里等位子的墙钟（实测分位）。
    pub queue_wait: Option<f64>,
    /// 一副副本空载背着的字节。
    pub idle_bytes: Option<u64>,
    /// 一棵任务树占的字节。
    pub tree_bytes: Option<u64>,
    /// 一副副本同时能跑几份：由压力除以实测单价得出，不是配置。
    pub tasks_per_replica: Option<usize>,
    /// 换副本搬一棵树的墙钟。
    pub carry: Option<f64>,
}

/// 一种形态一条任务的价格。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShapePrice {
    pub shape: SandboxShape,
    /// 一条任务摊到的墙钟：起停、切树、等位子。
    pub seconds_a_task: Option<f64>,
    /// 量不到秒数时缺的那条读数。
    pub seconds_missing: &'static str,
    /// 一条任务摊到的卷。
    pub bytes_a_task: Option<u64>,
    /// 量不到字节数时缺的那条读数。
    pub bytes_missing: &'static str,
    /// 搬一棵树的墙钟。只有共享副本的形态有这一项：一任务一沙箱随任务结束，
    /// 没有下一次可以搬。它是常态之外的一次性代价，不参与上下两条轴的比较，
    /// 但必须出现在同一张表上，否则「搬得多贵」就成了判决之外的秘密。
    pub carry: Option<f64>,
}

/// 两种形态的价格与判决。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShapeCost {
    pub one_task: ShapePrice,
    pub many_tasks: ShapePrice,
    /// 两种形态在墙钟与卷两条轴上都比得出谁便宜时才有值；各有胜负、价格相同、
    /// 或缺一条读数时是 `None`，理由写在 `why`。
    pub cheaper: Option<SandboxShape>,
    pub why: &'static str,
}

const MEASURED: &str = "measured";
const STARTUP_READING: &str = "how long a sandbox takes to start";
const CHECKOUT_READING: &str = "how long a task's tree takes to cut";
const WAIT_READING: &str = "how long a command waits for a slot";
const IDLE_READING: &str = "the bytes a replica carries idle";
const TREE_READING: &str = "the bytes one task's tree takes";
const TASKS_READING: &str = "how many tasks one replica holds";

/// 两种沙箱形态各自一条任务的价格。
///
/// 规则只有两条：一任务一沙箱每份都要起一遍、切一棵树、背一整份底；一副多任务
/// 把起停和底摊在能同时跑的任务上，切树仍按任务算，等位子是它的额外代价。摊开
/// 的依据是这台副本实测同时能跑几份，不是配置里的数；一个还没量到价格的进程说
/// 不出这个数，判决就停在「没答案」。
pub fn sandbox_shape_cost(readings: &ShapeReadings) -> ShapeCost {
    let one_task = price_one_task(readings);
    let many_tasks = price_many_tasks(readings);
    let (cheaper, why) = match (
        one_task.seconds_a_task,
        one_task.bytes_a_task,
        many_tasks.seconds_a_task,
        many_tasks.bytes_a_task,
    ) {
        (Some(one_s), Some(one_b), Some(many_s), Some(many_b)) => {
            if one_s < many_s && one_b < many_b {
                (
                    Some(SandboxShape::OneTaskPerSandbox),
                    "cheaper on both the wall and the volume",
                )
            } else if many_s < one_s && many_b < one_b {
                (
                    Some(SandboxShape::ManyTasksPerReplica),
                    "cheaper on both the wall and the volume",
                )
            } else if one_s == many_s && one_b == many_b {
                (None, "the two shapes price the same")
            } else {
                (None, "each shape is cheaper on one axis")
            }
        }
        // Name the reading the comparison is waiting for: which one is missing is
        // the only actionable thing about an undecided verdict.
        _ => {
            let missing = if one_task.seconds_a_task.is_none() {
                one_task.seconds_missing
            } else if one_task.bytes_a_task.is_none() {
                one_task.bytes_missing
            } else if many_tasks.seconds_a_task.is_none() {
                many_tasks.seconds_missing
            } else {
                many_tasks.bytes_missing
            };
            (None, missing)
        }
    };
    ShapeCost {
        one_task,
        many_tasks,
        cheaper,
        why,
    }
}

fn price_one_task(readings: &ShapeReadings) -> ShapePrice {
    ShapePrice {
        shape: SandboxShape::OneTaskPerSandbox,
        seconds_a_task: readings.startup.zip(readings.checkout).map(|(s, c)| s + c),
        seconds_missing: missing_of(&[
            (readings.startup.is_none(), STARTUP_READING),
            (readings.checkout.is_none(), CHECKOUT_READING),
        ]),
        bytes_a_task: readings
            .idle_bytes
            .zip(readings.tree_bytes)
            .map(|(i, t)| i + t),
        bytes_missing: missing_of(&[
            (readings.idle_bytes.is_none(), IDLE_READING),
            (readings.tree_bytes.is_none(), TREE_READING),
        ]),
        // The sandbox ends with the task: there is no next run to carry a tree to.
        carry: None,
    }
}

fn price_many_tasks(readings: &ShapeReadings) -> ShapePrice {
    let tasks = readings.tasks_per_replica.filter(|n| *n > 0);
    let seconds = match (
        readings.startup,
        readings.checkout,
        readings.queue_wait,
        tasks,
    ) {
        (Some(startup), Some(checkout), Some(wait), Some(tasks)) => {
            Some(startup / tasks as f64 + checkout + wait)
        }
        _ => None,
    };
    let bytes = match (readings.idle_bytes, readings.tree_bytes, tasks) {
        (Some(idle), Some(tree), Some(tasks)) => Some(idle / tasks as u64 + tree),
        _ => None,
    };
    ShapePrice {
        shape: SandboxShape::ManyTasksPerReplica,
        seconds_a_task: seconds,
        seconds_missing: missing_of(&[
            (readings.startup.is_none(), STARTUP_READING),
            (readings.checkout.is_none(), CHECKOUT_READING),
            (readings.queue_wait.is_none(), WAIT_READING),
            (tasks.is_none(), TASKS_READING),
        ]),
        bytes_a_task: bytes,
        bytes_missing: missing_of(&[
            (readings.idle_bytes.is_none(), IDLE_READING),
            (readings.tree_bytes.is_none(), TREE_READING),
            (tasks.is_none(), TASKS_READING),
        ]),
        carry: readings.carry,
    }
}

fn missing_of(inputs: &[(bool, &'static str)]) -> &'static str {
    inputs
        .iter()
        .find(|(absent, _)| *absent)
        .map(|(_, name)| *name)
        .unwrap_or(MEASURED)
}

/// 把价格表发布到抓取面。
///
/// `source` 一栏要么是 `measured`，要么是缺的那条读数的名字：一个「0」带着
/// 读数名字，读者看得出它不是量到的零，而是这一格还没有数。
pub fn publish_shape_cost(cost: &ShapeCost) {
    let metrics = metrics();
    for price in [&cost.one_task, &cost.many_tasks] {
        let (seconds, seconds_source) = match price.seconds_a_task {
            Some(seconds) => (seconds, MEASURED),
            None => (0.0, price.seconds_missing),
        };
        publish_shape_series(
            &metrics.shape_seconds,
            &SHAPE_SECONDS_SERIES,
            price.shape.label(),
            seconds,
            seconds_source,
        );
        let (bytes, bytes_source) = match price.bytes_a_task {
            Some(bytes) => (bytes, MEASURED),
            None => (0, price.bytes_missing),
        };
        publish_shape_series(
            &metrics.shape_bytes,
            &SHAPE_BYTES_SERIES,
            price.shape.label(),
            bytes as f64,
            bytes_source,
        );
    }
    let (carry, carry_source) = match cost.many_tasks.carry {
        Some(seconds) => (seconds, MEASURED),
        None => (0.0, "no move of a lane has been measured"),
    };
    publish_carry_series(&metrics.shape_carry, carry, carry_source);
    for shape in ["one_task", "many_tasks", "undecided"] {
        let hit = match cost.cheaper {
            Some(SandboxShape::OneTaskPerSandbox) => shape == "one_task",
            Some(SandboxShape::ManyTasksPerReplica) => shape == "many_tasks",
            None => shape == "undecided",
        };
        metrics
            .shape_cheaper
            .with_label_values(&[shape])
            .set(i64::from(hit));
    }
}

static SHAPE_SECONDS_SERIES: std::sync::Mutex<Vec<(String, &'static str)>> =
    std::sync::Mutex::new(Vec::new());
static SHAPE_BYTES_SERIES: std::sync::Mutex<Vec<(String, &'static str)>> =
    std::sync::Mutex::new(Vec::new());
/// 搬树只有一个标签，单独记它上一次的来源。
static SHAPE_CARRY_SOURCE: std::sync::Mutex<Option<&'static str>> = std::sync::Mutex::new(None);

fn publish_carry_series(gauge: &GaugeVec, value: f64, source: &'static str) {
    let mut last = SHAPE_CARRY_SOURCE.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(old) = *last {
        if old != source {
            let _ = gauge.remove_label_values(&[old]);
        }
    }
    *last = Some(source);
    gauge.with_label_values(&[source]).set(value);
}

/// 一条序列换来源时，把旧来源那条从抓取面摘掉。
///
/// 一条曾经量到、后来量不到的读数如果留在面上，读者会把旧的 `measured` 值
/// 当成现在的数——正是「量不到 / 量到就是这个值」要分开的那两种情形。
fn publish_shape_series(
    gauge: &GaugeVec,
    published: &std::sync::Mutex<Vec<(String, &'static str)>>,
    shape: &str,
    value: f64,
    source: &'static str,
) {
    let mut seen = published.lock().unwrap_or_else(|p| p.into_inner());
    seen.retain(|(seen_shape, seen_source)| {
        let stale = seen_shape == shape && *seen_source != source;
        if stale {
            let _ = gauge.remove_label_values(&[shape, seen_source]);
        }
        !stale
    });
    if !seen
        .iter()
        .any(|(seen_shape, seen_source)| seen_shape == shape && *seen_source == source)
    {
        seen.push((shape.to_string(), source));
    }
    gauge.with_label_values(&[shape, source]).set(value);
}

/// 这份进程从执行器入口起到能接命令花了多久，毫秒。零是「没量过」。
static STARTUP_MILLIS: AtomicU64 = AtomicU64::new(0);

/// 记下这一次启动的墙钟，写在开始监听之前。它不是一份沙箱 Pod 的全部起停代价
/// （调度与拉镜像在这一层看不到），而是这条进程自己付掉的那一段；读数面按
/// 「进程起到能接命令」标注它，不冒充整个 Pod 的起停。
pub fn record_startup(millis: u64) {
    STARTUP_MILLIS.store(millis, Ordering::Relaxed);
}

/// 这份进程的启动墙钟；没记过（嵌入式用法、测试）时是 `None`。
pub fn startup_seconds() -> Option<f64> {
    match STARTUP_MILLIS.load(Ordering::Relaxed) {
        0 => None,
        millis => Some(millis as f64 / 1000.0),
    }
}

/// The room running work has claimed on this replica.
///
/// One ledger per executor, shared by every path that admits work, so a command
/// admitted here is visible to the build lane planning there. A reservation is
/// released by dropping it, which happens when the child has exited -- the same
/// moment the memory is actually free -- and never by a caller remembering to
/// say so.
pub struct Ledger {
    open_bytes: AtomicU64,
    open_count: AtomicU64,
    /// Work parked until room frees, right now. It is part of the queue this
    /// replica reports outward: a replica whose commands are all waiting for
    /// room is not idle, and a scaler that could not see them would read a
    /// full replica as an empty one.
    waiting: AtomicU64,
    notify: tokio::sync::Notify,
}

impl Ledger {
    pub fn new() -> Arc<Self> {
        let ledger = Arc::new(Self {
            open_bytes: AtomicU64::new(0),
            open_count: AtomicU64::new(0),
            waiting: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
        });
        metrics().reservations_open.set(0);
        metrics().waiting.set(0);
        ledger
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.open_bytes.load(Ordering::Relaxed)
    }

    pub fn open_count(&self) -> u64 {
        self.open_count.load(Ordering::Relaxed)
    }

    /// How many callers are parked waiting for room on this replica.
    pub fn waiting_count(&self) -> usize {
        self.waiting.load(Ordering::Relaxed) as usize
    }

    fn begin_wait(self: &Arc<Self>) -> Waiting {
        let count = self.waiting.fetch_add(1, Ordering::Relaxed) + 1;
        metrics().waiting.set(count as i64);
        Waiting {
            ledger: Arc::clone(self),
        }
    }

    fn end_wait(&self) {
        let left = self.waiting.fetch_sub(1, Ordering::Relaxed) - 1;
        metrics().waiting.set(left as i64);
    }

    /// Claim `bytes` of room until the returned reservation is dropped.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Reservation {
        self.open_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.open_count.fetch_add(1, Ordering::Relaxed);
        metrics().reservations_open.set(self.open_count() as i64);
        Reservation {
            ledger: Arc::clone(self),
            bytes,
        }
    }

    fn release(&self, bytes: u64) {
        self.open_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.open_count.fetch_sub(1, Ordering::Relaxed);
        metrics().reservations_open.set(self.open_count() as i64);
        // Room just became free: whoever is queued for it should look again,
        // and whoever is about to plan should plan against the smaller figure.
        self.notify.notify_waiters();
    }
}

/// One caller parked until room frees. Dropped on every way out of the wait --
/// admitted, refused, or answered unmeasured -- so the count can only move
/// while a caller is actually parked.
struct Waiting {
    ledger: Arc<Ledger>,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.ledger.end_wait();
    }
}

/// Room one running piece of work holds until it ends.
pub struct Reservation {
    ledger: Arc<Ledger>,
    bytes: u64,
}

impl Reservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.ledger.release(self.bytes);
    }
}

/// What a caller got when it asked whether it could start.
pub struct Admitted {
    state: Admission,
    reservation: Option<Reservation>,
}

impl Admitted {
    /// The verdict the last look produced.
    pub fn state(&self) -> Admission {
        self.state
    }

    pub fn reservation(&self) -> Option<&Reservation> {
        self.reservation.as_ref()
    }

    pub fn into_reservation(self) -> Option<Reservation> {
        self.reservation
    }
}

/// A wait that ended because it had already been as long as this executor's
/// queue normally is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refusal {
    pub waited: Duration,
    pub window: Duration,
}

/// Wait until `pressure` covers `demand`, take the reservation, and hand it
/// back -- or refuse once the wait has outlived the measured queue window.
///
/// `may_free` answers "is anything running that could give the room back". When
/// it says no, waiting would be a wait for a change that cannot come, so the
/// work is admitted unmeasured instead: a container whose room is taken by
/// something nobody can release has told us nothing about this command, and
/// refusing it would turn "full" into "no work at all". `window` is the
/// deployment's measured queue wait; `None` means nothing has been measured yet
/// and a waiter waits.
pub async fn wait_for_admission<P, F>(
    ledger: &Arc<Ledger>,
    pressure: P,
    demand: &Demand,
    may_free: F,
    window: Option<Duration>,
) -> Result<Admitted, Refusal>
where
    P: Fn(u64) -> Pressure,
    F: Fn() -> bool,
{
    let started = std::time::Instant::now();
    // Registered on the first verdict that finds no room, and dropped on the
    // way out whichever way that is: this replica's outward queue has to count
    // work parked here, or a scaler reads "all of it waiting" as "nothing to do".
    let mut waiting: Option<Waiting> = None;
    loop {
        let here = pressure(ledger.reserved_bytes());
        publish_pressure(&here, demand);
        match admits(&here, demand) {
            Admission::Admitted { reserved_bytes } => {
                let reservation = ledger.reserve(reserved_bytes);
                publish_admission(Admission::Admitted { reserved_bytes }, demand);
                return Ok(Admitted {
                    state: Admission::Admitted { reserved_bytes },
                    reservation: Some(reservation),
                });
            }
            Admission::Unmeasured(reading) => {
                publish_admission(Admission::Unmeasured(reading), demand);
                return Ok(Admitted {
                    state: Admission::Unmeasured(reading),
                    reservation: None,
                });
            }
            Admission::Queued => {
                waiting.get_or_insert_with(|| ledger.begin_wait());
                let until_the_window =
                    window.map(|window| window.saturating_sub(started.elapsed()));
                if matches!(until_the_window, Some(left) if left.is_zero()) {
                    publish_admission(Admission::Queued, demand);
                    return Err(Refusal {
                        waited: started.elapsed(),
                        window: window.unwrap_or_default(),
                    });
                }
                if !may_free() {
                    let reading = "nothing in flight could give the room back";
                    publish_admission(Admission::Unmeasured(reading), demand);
                    return Ok(Admitted {
                        state: Admission::Unmeasured(reading),
                        reservation: None,
                    });
                }
                // Wait for a reservation to be given back, or until the
                // measured window runs out -- whichever comes first. There is
                // no separate sampling cadence: only two things can change the
                // answer, and both already have a wake-up. Room comes back by
                // a reservation being dropped, which notifies; the deadline is
                // the window the caller measured. A number for "how often to
                // look" would be a third wake-up source with nothing behind it.
                match until_the_window {
                    Some(left) => {
                        tokio::select! {
                            _ = ledger.notify.notified() => {}
                            _ = tokio::time::sleep(left) => {}
                        }
                    }
                    None => ledger.notify.notified().await,
                }
            }
        }
    }
}

/// What this module has measured and decided, on the scrape surface.
///
/// A reader has to be able to tell "admitted because it fit" from "admitted
/// because nothing was priced" from "queued because there was no room", and the
/// open reservations are what say whether the first one is still being counted.
struct Metrics {
    registry: Registry,
    admission: IntCounterVec,
    placement: IntCounterVec,
    reservations_open: IntGauge,
    waiting: IntGauge,
    headroom: IntGauge,
    demand: IntGauge,
    shape_seconds: GaugeVec,
    shape_bytes: GaugeVec,
    shape_carry: GaugeVec,
    shape_cheaper: IntGaugeVec,
}

static METRICS: std::sync::OnceLock<Metrics> = std::sync::OnceLock::new();

fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| {
        let registry = Registry::new();
        let admission = IntCounterVec::new(
            prometheus::Opts::new(
                "sandbox_command_admission_total",
                "Times work asked whether it could start, by the verdict and where the price came from",
            ),
            &["state", "source"],
        )
        .expect("static metric name and labels");
        let placement = IntCounterVec::new(
            prometheus::Opts::new(
                "sandbox_lane_placement_total",
                "Times a lane was placed on a replica, by why that replica",
            ),
            &["reason"],
        )
        .expect("static metric name and labels");
        let reservations_open = IntGauge::new(
            "sandbox_command_reservation_open",
            "Pieces of work holding room they claimed when they were admitted",
        )
        .expect("static metric name");
        let waiting = IntGauge::new(
            "sandbox_command_waiting",
            "Commands parked on this replica because the room they are priced at is not free",
        )
        .expect("static metric name");
        let headroom = IntGauge::new(
            "sandbox_pressure_headroom_bytes",
            "Room left on this replica after its working set and the reservations held against it",
        )
        .expect("static metric name");
        let demand = IntGauge::new(
            "sandbox_command_demand_bytes",
            "What one command is priced at, read from the window of finished commands",
        )
        .expect("static metric name");
        let shape_seconds = GaugeVec::new(
            prometheus::Opts::new(
                "sandbox_shape_seconds_a_task",
                "Wall clock one task pays in a sandbox shape, and whether it was measured",
            ),
            &["shape", "source"],
        )
        .expect("static metric name and labels");
        let shape_bytes = GaugeVec::new(
            prometheus::Opts::new(
                "sandbox_shape_bytes_a_task",
                "Volume one task occupies in a sandbox shape, and whether it was measured",
            ),
            &["shape", "source"],
        )
        .expect("static metric name and labels");
        let shape_carry = GaugeVec::new(
            prometheus::Opts::new(
                "sandbox_shape_carry_seconds",
                "Wall clock a lane's tree took to move between replicas, as the shared shape pays it",
            ),
            &["source"],
        )
        .expect("static metric name and labels");
        let shape_cheaper = IntGaugeVec::new(
            prometheus::Opts::new(
                "sandbox_shape_cheaper",
                "Which sandbox shape the measured prices favour; undecided when they do not say",
            ),
            &["shape"],
        )
        .expect("static metric name and labels");
        // Every verdict is on the surface from the first scrape: a zero that
        // was never incremented says "this has not happened here", while a
        // missing series cannot be told apart from a build without the reader.
        for state in ["admitted", "queued", "unmeasured"] {
            for source in ["measured", "unmeasured"] {
                let _ = admission.with_label_values(&[state, source]);
            }
        }
        for reason in ["nailed", "headroom", "added_replica", "only_one"] {
            let _ = placement.with_label_values(&[reason]);
        }
        for shape in ["one_task", "many_tasks", "undecided"] {
            let _ = shape_cheaper.with_label_values(&[shape]);
        }
        for collector in [
            Box::new(admission.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(placement.clone()),
            Box::new(reservations_open.clone()),
            Box::new(waiting.clone()),
            Box::new(headroom.clone()),
            Box::new(demand.clone()),
            Box::new(shape_seconds.clone()),
            Box::new(shape_bytes.clone()),
            Box::new(shape_carry.clone()),
            Box::new(shape_cheaper.clone()),
        ] {
            registry
                .register(collector)
                .expect("static collector registration");
        }
        Metrics {
            registry,
            admission,
            placement,
            reservations_open,
            waiting,
            headroom,
            demand,
            shape_seconds,
            shape_bytes,
            shape_carry,
            shape_cheaper,
        }
    })
}

fn source_label(demand: &Demand) -> &'static str {
    match demand.source {
        DemandSource::Measured { .. } => "measured",
        DemandSource::Unmeasured(_) => "unmeasured",
    }
}

fn state_label(admission: Admission) -> &'static str {
    match admission {
        Admission::Admitted { .. } => "admitted",
        Admission::Queued => "queued",
        Admission::Unmeasured(_) => "unmeasured",
    }
}

fn publish_admission(admission: Admission, demand: &Demand) {
    metrics()
        .admission
        .with_label_values(&[state_label(admission), source_label(demand)])
        .inc();
}

/// The two figures a reader needs beside a verdict: how much room is left, and
/// what the work in front of the door is priced at. A figure that was not read
/// is left off the surface rather than published as zero, which would read as a
/// real answer.
fn publish_pressure(pressure: &Pressure, demand: &Demand) {
    if let Some(room) = pressure.headroom() {
        metrics().headroom.set(room.min(i64::MAX as u64) as i64);
    }
    if let Some(bytes) = demand.bytes() {
        metrics().demand.set(bytes.min(i64::MAX as u64) as i64);
    }
}

/// The admission surface as Prometheus text.
pub fn render() -> String {
    let mut buf = Vec::new();
    if TextEncoder::new()
        .encode(&metrics().registry.gather(), &mut buf)
        .is_ok()
    {
        String::from_utf8_lossy(&buf).into_owned()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    /// The three figures as a reader would hold them: a limit, what is already
    /// resident, and what running work has claimed.
    fn room(limit: u64, working: u64, reserved: u64) -> Pressure {
        Pressure::new(Some(limit), Some(working), reserved)
    }

    /// A count of one that nobody measured and one that was measured have to
    /// read differently. Here the first half: a container that cannot say how
    /// much room it has lets work through unmeasured rather than as full.
    #[test]
    fn a_container_that_cannot_say_its_room_is_not_a_full_one() {
        let unread = Pressure::new(None, Some(GIB), 0);
        assert_eq!(unread.headroom(), None);
        let demand = Demand::from_samples(&[GIB]);
        assert_eq!(
            admits(&unread, &demand),
            Admission::Unmeasured("the container did not say how much room it has")
        );

        // Reading the working set but not the limit is the same answer, and a
        // working set above the limit is no room rather than a negative one.
        assert_eq!(Pressure::new(Some(GIB), None, 0).headroom(), None);
        assert_eq!(room(GIB, 2 * GIB, 0).headroom(), Some(0));
        assert_eq!(room(4 * GIB, GIB, GIB).headroom(), Some(2 * GIB));
    }

    /// Nothing priced is not something free: it is admitted, and counted as an
    /// admission nobody could price.
    #[test]
    fn an_unpriced_piece_of_work_is_admitted_and_says_why() {
        let demand = Demand::unmeasured("no finished command has been priced");
        assert_eq!(
            admits(&room(8 * GIB, GIB, 0), &demand),
            Admission::Unmeasured("no finished command has been priced")
        );
        assert_eq!(Demand::from_samples(&[]).bytes(), None);
        assert_eq!(Demand::from_samples(&[0, 0]).bytes(), None);
    }

    /// The price is the worst sample in the window, so an under-estimate of the
    /// price cannot become an over-estimate of how many fit.
    #[test]
    fn the_price_is_the_worst_of_the_window_it_is_given() {
        assert_eq!(
            Demand::from_samples(&[GIB, 3 * GIB, 2 * GIB]).bytes(),
            Some(3 * GIB)
        );
        assert_eq!(
            Demand::from_samples(&[GIB, 2 * GIB]).source(),
            DemandSource::Measured { samples: 2 }
        );
        assert!(matches!(
            Demand::from_samples(&[]).source(),
            DemandSource::Unmeasured(_)
        ));
    }

    /// The proof the price is derived rather than written down: change the
    /// samples, and who is admitted changes with them. A figure that stood
    /// still through this would be a constant wearing a measurement's clothes.
    #[test]
    fn changing_the_demand_changes_who_is_admitted() {
        // 例如：8 GiB 限额、1 GiB 常驻——余地 7 GiB。
        let here = room(8 * GIB, GIB, 0);
        assert_eq!(here.fits(&Demand::from_samples(&[7 * GIB])), Some(1));
        assert_eq!(here.fits(&Demand::from_samples(&[4 * GIB])), Some(1));
        assert_eq!(here.fits(&Demand::from_samples(&[2 * GIB])), Some(3));
        assert_eq!(here.fits(&Demand::from_samples(&[GIB])), Some(7));
        assert_eq!(
            admits(&here, &Demand::from_samples(&[7 * GIB + 1])),
            Admission::Queued
        );

        // And the other input: the same price against room that running work
        // has already claimed is no longer admitted.
        assert_eq!(
            admits(
                &room(8 * GIB, GIB, 6 * GIB),
                &Demand::from_samples(&[2 * GIB])
            ),
            Admission::Queued
        );
    }

    /// 读数面那几格共用的一把锁。
    ///
    /// 每个测试各写一个 `static SERIAL` 时锁的是**自己**：三个测试互相看不见对方的
    /// 预留，而它们读的 `render()` 是同一份进程级读数——一个测试正持着 2 GiB，另一个
    /// 刚把自己的那一格放下，读到的却不是 0（2026-11-23 实测：偶发红在
    /// `sandbox_command_reservation_open 0`）。所以锁只有一把，凡是预留或读那一面的
    /// 测试都拿它。
    static PRESSURE_SURFACE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A reservation is room taken until the work that took it ends; dropping
    /// it is what gives the room back, so a caller cannot forget to.
    #[test]
    fn a_reservation_is_held_until_it_is_dropped() {
        let _serial = PRESSURE_SURFACE.lock().unwrap_or_else(|p| p.into_inner());
        let ledger = Ledger::new();
        assert_eq!(ledger.reserved_bytes(), 0);
        let held = ledger.reserve(2 * GIB);
        assert_eq!(ledger.reserved_bytes(), 2 * GIB);
        assert_eq!(ledger.open_count(), 1);
        assert_eq!(held.bytes(), 2 * GIB);
        drop(held);
        assert_eq!(ledger.reserved_bytes(), 0);
        assert_eq!(ledger.open_count(), 0);
    }

    /// A queued command is woken by the room coming back, not by luck: the
    /// reservation it takes is visible to whatever plans next.
    #[tokio::test]
    async fn waiting_ends_when_the_room_is_given_back() {
        let _serial = PRESSURE_SURFACE.lock().unwrap_or_else(|p| p.into_inner());
        let ledger = Ledger::new();
        let held = ledger.reserve(6 * GIB);
        let demand = Demand::from_samples(&[2 * GIB]);
        let waiting = {
            let ledger = Arc::clone(&ledger);
            tokio::spawn(async move {
                wait_for_admission(
                    &ledger,
                    |reserved| room(8 * GIB, GIB, reserved),
                    &demand,
                    || true,
                    None,
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "started while the room was taken");
        assert_eq!(
            ledger.waiting_count(),
            1,
            "一条被余量挡住的命令必须出现在这副副本对外的队里，否则扩缩读不懂「在看它等」"
        );

        drop(held);
        let admitted = waiting.await.unwrap().expect("the room came back");
        assert!(matches!(admitted.state(), Admission::Admitted { .. }));
        assert_eq!(ledger.waiting_count(), 0, "放行之后不再是等待者");
        assert_eq!(
            ledger.reserved_bytes(),
            2 * GIB,
            "the reservation it took has to be visible to the next planner"
        );
    }

    /// Waiting for room that nothing can give back is not a wait: the work goes
    /// through unmeasured instead of being refused for the platform's own
    /// footprint.
    #[tokio::test]
    async fn a_wait_nothing_could_end_is_not_a_wait() {
        let ledger = Ledger::new();
        let demand = Demand::from_samples(&[8 * GIB]);
        let admitted =
            wait_for_admission(&ledger, |_| room(8 * GIB, GIB, 0), &demand, || false, None)
                .await
                .expect("nothing in flight is not a refusal");
        assert!(matches!(admitted.state(), Admission::Unmeasured(_)));
        assert!(admitted.reservation().is_none());
    }

    /// Past the measured queue window a waiter is refused, with both numbers so
    /// the reader can recompute the judgement.
    #[tokio::test]
    async fn a_wait_past_the_measured_window_is_refused() {
        let ledger = Ledger::new();
        let demand = Demand::from_samples(&[8 * GIB]);
        let refused = wait_for_admission(
            &ledger,
            |_| room(8 * GIB, GIB, 0),
            &demand,
            || true,
            Some(Duration::from_millis(30)),
        )
        .await;
        match refused {
            Err(refusal) => assert_eq!(refusal.window, Duration::from_millis(30)),
            Ok(_) => panic!("a wait that outlived the window was admitted"),
        }
    }

    /// The three verdicts and the open reservations have to reach the scrape
    /// surface, and the gauge has to come back down -- otherwise "still holding
    /// room" and "holding nothing" read the same.
    #[test]
    fn the_verdicts_and_the_open_reservations_are_on_the_scrape_surface() {
        let _serial = PRESSURE_SURFACE.lock().unwrap_or_else(|p| p.into_inner());
        let ledger = Ledger::new();
        let held = ledger.reserve(1024);
        let text = render();
        assert!(text.contains("sandbox_command_admission_total"), "{text}");
        assert!(
            text.contains("sandbox_command_reservation_open 1"),
            "{text}"
        );
        assert!(text.contains("sandbox_command_waiting 0"), "{text}");
        drop(held);
        assert!(render().contains("sandbox_command_reservation_open 0"));
    }

    /// 四个落点原因从第一次抓取起就是四条各自为 0 的序列，记一次只推它自己
    /// 那一条：读者能把「用钉子留住的」「被导向的」「靠加副本接住的」分开。
    #[test]
    fn each_placement_reason_is_its_own_series() {
        let _serial = PRESSURE_SURFACE.lock().unwrap_or_else(|p| p.into_inner());
        let before = render();
        for reason in ["nailed", "headroom", "added_replica", "only_one"] {
            assert!(
                before.contains(&format!(
                    "sandbox_lane_placement_total{{reason=\"{reason}\"}} 0"
                )),
                "{before}"
            );
        }
        record_placement(&Placement::Headroom {
            replica: "http://executor-1:9090".into(),
        });
        let text = render();
        assert!(
            text.contains("sandbox_lane_placement_total{reason=\"headroom\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("sandbox_lane_placement_total{reason=\"nailed\"} 0"),
            "{text}"
        );
    }

    /// 这份价格的好消息全在一个任务不用自己起一遍沙箱：起停被能同时跑的任务摊开。
    fn shared_readings(wait: f64) -> ShapeReadings {
        ShapeReadings {
            startup: Some(2.0),
            checkout: Some(1.0),
            queue_wait: Some(wait),
            idle_bytes: Some(4 * GIB),
            tree_bytes: Some(GIB),
            tasks_per_replica: Some(4),
            carry: None,
        }
    }

    #[test]
    fn a_start_shared_by_four_tasks_is_cheaper_than_one_start_a_task() {
        let cost = sandbox_shape_cost(&shared_readings(0.1));
        assert_eq!(cost.cheaper, Some(SandboxShape::ManyTasksPerReplica));
        // 2 秒起停摊到 4 份，再加 1 秒切树和 0.1 秒等位子；一任务一沙箱是 3 秒。
        assert_eq!(cost.many_tasks.seconds_a_task, Some(1.6));
        assert_eq!(cost.one_task.seconds_a_task, Some(3.0));
        // 例如：4 GiB 的常驻底摊到 4 份再加 1 GiB 的树，对 5 GiB 一整份。
        assert_eq!(cost.many_tasks.bytes_a_task, Some(2 * GIB));
        assert_eq!(cost.one_task.bytes_a_task, Some(5 * GIB));
    }

    /// 等位子的时间是从这次起停里省下来的时间里扣的：等得比起停分摊还久，
    /// 共享就不便宜了。判决不挑一个偏好顶上，而是把这两条轴摆开说各赢一边。
    #[test]
    fn a_wait_longer_than_the_start_it_saves_leaves_the_choice_open() {
        let cost = sandbox_shape_cost(&shared_readings(5.0));
        assert_eq!(cost.many_tasks.seconds_a_task, Some(6.5));
        assert_eq!(cost.cheaper, None);
        assert_eq!(cost.why, "each shape is cheaper on one axis");

        // 同一份输入只把等位子换成短的，判决就翻过来：这是「换输入 ⇒ 值跟着变」。
        assert_eq!(
            sandbox_shape_cost(&shared_readings(0.1)).cheaper,
            Some(SandboxShape::ManyTasksPerReplica)
        );
    }

    /// 搬树的代价是共享形态每搬一次付的额外账，不是两种形态的稳态价格之比：
    /// 把它算进每任务价格，就会让判决跟着「多久搬一次」变，而稳态比较不该变。
    #[test]
    fn the_cost_of_moving_a_lane_is_published_but_not_in_the_comparison() {
        let readings = ShapeReadings {
            carry: Some(600.0),
            ..shared_readings(0.1)
        };
        let cost = sandbox_shape_cost(&readings);
        assert_eq!(cost.many_tasks.carry, Some(600.0));
        assert_eq!(cost.one_task.carry, None);
        assert_eq!(cost.cheaper, Some(SandboxShape::ManyTasksPerReplica));
    }

    /// 缺读数时判决是「没答案」，而且点名缺哪条——一个偏好或者一个 0 都会
    /// 把「还没量」读成「量到了就是这个值」。
    #[test]
    fn a_missing_reading_leaves_the_choice_undecided_and_names_it() {
        let nothing = sandbox_shape_cost(&ShapeReadings::default());
        assert_eq!(nothing.cheaper, None);
        assert_eq!(nothing.why, "how long a sandbox takes to start");

        // 一副副本能跑几份是唯一挡住共享价格的那条时，点名的是它。
        let cost = sandbox_shape_cost(&ShapeReadings {
            tasks_per_replica: None,
            ..shared_readings(0.1)
        });
        assert_eq!(cost.one_task.seconds_a_task, Some(3.0));
        assert_eq!(cost.many_tasks.seconds_a_task, None);
        assert_eq!(cost.why, "how many tasks one replica holds");

        // 量到「一份也跑不下」与量不到同形：不能除以 0，也不能当作「无限共享」。
        let zero = sandbox_shape_cost(&ShapeReadings {
            tasks_per_replica: Some(0),
            ..shared_readings(0.1)
        });
        assert_eq!(zero.many_tasks.seconds_a_task, None);
        assert_eq!(zero.why, "how many tasks one replica holds");
    }

    /// 价格与判决都要上抓取面，包括「没答案」那一格：`source` 栏带着缺的读数名，
    /// 读者分得清 0 是量到的零还是这一格没有数。
    #[test]
    fn the_price_of_both_shapes_reaches_the_scrape_surface() {
        let _serial = PRESSURE_SURFACE.lock().unwrap_or_else(|p| p.into_inner());

        publish_shape_cost(&sandbox_shape_cost(&shared_readings(0.1)));
        let text = render();
        assert!(
            text.contains(
                "sandbox_shape_seconds_a_task{shape=\"many_tasks\",source=\"measured\"} 1.6"
            ),
            "{text}"
        );
        assert!(
            text.contains("sandbox_shape_cheaper{shape=\"many_tasks\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("sandbox_shape_cheaper{shape=\"one_task\"} 0"),
            "{text}"
        );

        publish_shape_cost(&sandbox_shape_cost(&ShapeReadings::default()));
        let text = render();
        assert!(
            text.contains("sandbox_shape_cheaper{shape=\"undecided\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("source=\"the bytes a replica carries idle\""),
            "{text}"
        );
    }
}
