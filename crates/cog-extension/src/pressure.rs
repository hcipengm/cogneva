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

use prometheus::{Encoder, IntCounterVec, IntGauge, Registry, TextEncoder};

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
        for collector in [
            Box::new(admission.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(placement.clone()),
            Box::new(reservations_open.clone()),
            Box::new(waiting.clone()),
            Box::new(headroom.clone()),
            Box::new(demand.clone()),
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

    /// A reservation is room taken until the work that took it ends; dropping
    /// it is what gives the room back, so a caller cannot forget to.
    #[test]
    fn a_reservation_is_held_until_it_is_dropped() {
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
        static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
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
        static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
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
}
