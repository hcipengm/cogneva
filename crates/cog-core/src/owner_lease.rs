//! Which process is allowed to act as a single-writer role.
//!
//! Several background loops here do work that has to happen once rather than
//! once per replica: following up on discovered issues, demoting traces between
//! tiers, holding the metrics sample log under a capacity. What makes them
//! single-writer is never the loop — it is what the loop touches. A record of
//! what has already been answered, private to the process that wrote it. An
//! index whose key every replica would spell the same way, so a second writer
//! overwrites the first one's row and the file it pointed at is no longer
//! findable. A row count two processes would each delete towards, leaving
//! "how big is this" and "who made it this big" as two different answers.
//!
//! None of that state can be handed to a second process, so the deployment used
//! to pin the owner with a static flag: exactly one workload ran each loop, and
//! no workload could ever be scaled past one replica. This module is the other
//! way of deciding the same thing — a **lease**, held by one process for a
//! bounded term, pushed out every cycle by whoever holds it and taken over by
//! another process once a term has passed without being renewed. A loop whose
//! work can outlast its own cycle holds its term with [`RoleHold`], which asks
//! on the lease's cadence rather than on the loop's.
//!
//! The deployment flag stays, and keeps its meaning: it says whether a workload
//! takes part in this role at all, which is a statement about what that workload
//! is for. The lease decides which of the participating replicas is the one
//! acting. A flag set to "no" and a lease held by somebody else look the same
//! from the outside, which is why the reading has to separate them: a loop that
//! was never given a role publishes nothing about ownership, while one that
//! contended and lost publishes that it is not the holder.
//!
//! Three properties are load-bearing, and each is a decision about which way an
//! uncertain state should fall:
//!
//! - **The arbiter is the store the work is about.** A role is contended over
//!   the shared state that made it single-writer in the first place, so a
//!   deployment that cannot reach that store has nothing to protect: the loops
//!   that need a lease are the loops that already require the store. A lease
//!   kept anywhere else — a cluster API object, a coordination service — would
//!   add a second dependency for a question whose answer only matters next to
//!   the data, and would go on being honoured in a deployment where the data is
//!   unreachable and nothing can be done anyway.
//! - **A process that cannot reach the arbiter does not act.** Not knowing
//!   whether someone else is the holder is not evidence that nobody is. Both
//!   mistakes cost something, and they do not cost the same: acting wrongly
//!   writes the duplicate that cannot be taken back — the follow-up sent twice,
//!   the index row overwritten, the archived file nobody can find — while
//!   holding back costs a delay, and the next cycle repairs it. Every unclear
//!   answer therefore falls on the side that keeps the work undone, and the
//!   cycle says so in its own reading.
//! - **With no arbiter at all, every process acts — as it did before.** A
//!   deployment with no shared store has no roles to arbitrate and no replicas
//!   contending: the state the loops touch is on one process's own volume. That
//!   is the pre-lease behaviour, and it must stay reachable, because a loop that
//!   silently stops when its arbiter is missing is a worse failure than the
//!   duplicate it was guarding against. What is *not* covered is a deployment
//!   with two replicas and no arbiter; that state is not arbitrated, and the
//!   only thing that can say so is the absence of the readings below.
//!
//! The holder names the process, not the instance. An instance identity has to
//! survive a restart, and this is the one identity whose whole point is to stop
//! existing when the process does: a lease inherited by a process restarting
//! under the same name would be a term nobody is filling, and the role would
//! sit unheld behind a row that looks renewed. So the holder is a pod name, or
//! the pid where there is no pod — volatile by construction, which is what the
//! takeover needs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::loop_health::Beat;
use crate::SFResult;

/// The longest a loop may take between two asks of the arbiter, and the term it
/// holds a role for once one is answered.
///
/// One pair of numbers for every role, rather than a term each caller picks for
/// itself, because the alerting on them is written once: a rule that says a role
/// is unowned has to know how long a handover may legitimately take, and a
/// window cannot be derived per loop from the series the loops publish. A loop
/// whose own work cadence is slower than the ask period asks on a timer of its
/// own instead of stretching these — the term bounds how long the role stays
/// unheld after its holder dies, which is a property of the deployment rather
/// than of how often the work happens to be due.
///
/// The term covers several asks, so a renewal lost to a slow query or a restart
/// does not cost the holder its role, and the wait after a holder dies is
/// bounded by the term plus one ask period.
pub const ASK_PERIOD: Duration = Duration::from_secs(120);
pub const TERM: Duration = Duration::from_secs(360);

/// A term shorter than a few ask periods would expire between two cycles of a
/// holder that is asking exactly as often as it is allowed to, which is the
/// duplicate side effect a lease exists to prevent rather than a handover.
const _: () = assert!(
    TERM.as_secs() >= 3 * ASK_PERIOD.as_secs(),
    "the lease term has to outlast several ask periods"
);

/// What one ask of the arbiter came back as.
///
/// Three states rather than a bool, because two of them are the same answer to
/// "may I act" and different answers to "why not": a process told that another
/// holds the role has learned something about the deployment, and one that
/// could not ask at all has learned nothing. Collapsing them would let an
/// unreachable arbiter read as a healthy deployment with a single owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ownership {
    /// The role is this process's for the current term.
    Held,
    /// A live holder exists and it is another process. This one must not act.
    Elsewhere,
    /// The arbiter could not be consulted. This says nothing about who holds
    /// the role, and this process must not act either.
    Unprovable,
}

impl Ownership {
    /// Whether a process that read this may do the role's work.
    pub fn may_act(self) -> bool {
        matches!(self, Ownership::Held)
    }
}

/// One role, from one process's side.
#[async_trait]
pub trait OwnerLease: Send + Sync {
    /// The role, as the reading labels it. Bounded by the loops that take a
    /// lease, never by traffic.
    fn role(&self) -> &str;

    /// Ask for the role, renewing the term if it is already this process's.
    ///
    /// `Ok(true)` means this process holds it now — either a term it already
    /// held was pushed out, or no term was live and this ask took it.
    /// `Ok(false)` means a live term belongs to someone else. An error means
    /// the arbiter could not be consulted, which is neither of those and must
    /// never be read as one: see [`Ownership`].
    async fn try_hold(&self) -> SFResult<bool>;
}

/// The leases one deployment can arbitrate.
///
/// Published by whoever owns the shared store, and published as `None` where
/// there is none — a process that does not find this service runs its loops
/// the way it did before leases existed.
pub trait OwnerLeaseBroker: Send + Sync {
    /// The lease over `role`, with a term of `ttl`.
    ///
    /// The term is a parameter because a probe of the arbitration needs one
    /// short enough to wait out; a deployment passes [`TERM`], and it is that
    /// pair of constants — not the argument — that the alerting on this is
    /// sized against. A term shorter than the period its holder asks on is a
    /// role that expires between two cycles and gets taken over by a second
    /// process, which is a duplicate side effect rather than a handover.
    fn lease(&self, role: &str, ttl: Duration) -> Arc<dyn OwnerLease>;
}

/// A loop's claim on one role.
///
/// Built once, asked once per cycle, and the beat it was built with carries the
/// reading: the same handle the loop already stamps is what publishes whether
/// this process holds the role, how often it took it, how often it lost it, and
/// how often it could not find out.
pub struct RoleClaim {
    /// `None` when the deployment published no arbiter — see the module note on
    /// what that means.
    lease: Option<Arc<dyn OwnerLease>>,
    beat: Beat,
    /// Whether the current run of unprovable answers has already been logged.
    ///
    /// A loop whose arbiter is down asks every cycle, and the log should carry
    /// one line per outage rather than one per attempt: the count is in the
    /// reading, and the line is for the cause the reading cannot carry. Reset by
    /// any answer that is not an error, so the next outage is logged too.
    reported_unprovable: AtomicBool,
}

impl RoleClaim {
    /// This process's claim on `role`, or a claim that always holds when the
    /// deployment published no broker.
    pub fn new(
        broker: Option<Arc<dyn OwnerLeaseBroker>>,
        role: &str,
        ttl: Duration,
        beat: Beat,
    ) -> Self {
        Self {
            lease: broker.map(|b| b.lease(role, ttl)),
            beat,
            reported_unprovable: AtomicBool::new(false),
        }
    }

    /// Whether this cycle may do the role's work.
    ///
    /// Every answer is recorded on the beat before it is acted on, including
    /// the ones that stop the work: a loop that stops acting because another
    /// process holds the role, and one that never started, are the same silence
    /// without it.
    pub async fn may_act(&self) -> bool {
        let Some(lease) = self.lease.as_ref() else {
            return true;
        };

        let ownership = match lease.try_hold().await {
            Ok(true) => {
                self.reported_unprovable.store(false, Ordering::Relaxed);
                Ownership::Held
            }
            Ok(false) => {
                self.reported_unprovable.store(false, Ordering::Relaxed);
                Ownership::Elsewhere
            }
            Err(e) => {
                if !self.reported_unprovable.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        role = lease.role(),
                        loop_name = self.beat.name(),
                        error = %e,
                        "could not ask who holds this role; holding the work back until an \
                         answer comes, because not knowing that nobody holds it is not the \
                         same as knowing it"
                    );
                }
                Ownership::Unprovable
            }
        };

        self.beat.note_ownership(ownership);
        ownership.may_act()
    }
}

/// A claim that keeps asking while the loop works, instead of once per cycle.
///
/// A claim asked once per cycle is enough for a loop whose whole cycle fits
/// inside the ask period. A loop whose cycle does not — an hourly scan, a sweep
/// over a large table, a migration pass — would let its term run out while it
/// was still working, and a term that expires mid-pass hands the role to a
/// second process that starts the same work: the duplicate the lease exists to
/// prevent, arrived at by the lease itself.
///
/// So the asking is moved off the loop's cadence and onto the contract's. A
/// task renews the term every [`ASK_PERIOD`] for as long as this is alive, and
/// the loop still asks where it acts, because a process that has lost the role
/// has to stop acting in the cycle it lost it — not one ask period later.
pub struct RoleHold {
    claim: Arc<RoleClaim>,
    renewal: tokio::task::JoinHandle<()>,
}

impl RoleHold {
    /// Start contending for `role`, renewing the term on the lease contract's
    /// cadence until this is dropped or `shutdown` fires.
    pub fn start(
        broker: Option<Arc<dyn OwnerLeaseBroker>>,
        role: &str,
        beat: Beat,
        shutdown: &crate::ShutdownSignal,
    ) -> Self {
        Self::asking_every(broker, role, beat, shutdown, ASK_PERIOD)
    }

    /// The same, asking on a period given here rather than the contract's.
    ///
    /// Reachable from this crate's tests, which cannot wait out a period of
    /// minutes to see the renewal task do anything. A deployment takes
    /// [`RoleHold::start`], and its period is the number the alerting on a role
    /// is sized against.
    pub(crate) fn asking_every(
        broker: Option<Arc<dyn OwnerLeaseBroker>>,
        role: &str,
        beat: Beat,
        shutdown: &crate::ShutdownSignal,
        period: Duration,
    ) -> Self {
        let claim = Arc::new(RoleClaim::new(broker, role, TERM, beat));
        let asking = Arc::clone(&claim);
        let shutdown = shutdown.clone();
        let renewal = tokio::spawn(async move {
            let mut ask = tokio::time::interval(period);
            // A slow ask must not queue up renewals that then land back to back:
            // what the term needs is one ask per period, and a burst of them
            // says nothing an operator can use.
            ask.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The first tick is immediate, so the takeover after a holder dies
            // is bounded by one ask period of the living process rather than by
            // when its own cycle happens to come round.
            loop {
                tokio::select! {
                    _ = ask.tick() => {
                        asking.may_act().await;
                    }
                    _ = shutdown.wait() => break,
                }
            }
        });
        Self { claim, renewal }
    }

    /// Whether this process may do the role's work, asked freshly.
    pub async fn may_act(&self) -> bool {
        self.claim.may_act().await
    }
}

impl Drop for RoleHold {
    fn drop(&mut self) {
        // The renewal task outlives the loop body it was started in otherwise,
        // and goes on writing terms for a process that has stopped acting.
        self.renewal.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::loop_health::{Cadence, LoopHealth};

    /// A lease whose answers are written by the test.
    struct Scripted {
        role: String,
        answers: Mutex<Vec<SFResult<bool>>>,
        asked: Mutex<usize>,
    }

    impl Scripted {
        fn new(role: &str, answers: Vec<SFResult<bool>>) -> Arc<Self> {
            Arc::new(Self {
                role: role.to_string(),
                answers: Mutex::new(answers),
                asked: Mutex::new(0),
            })
        }
    }

    #[async_trait]
    impl OwnerLease for Scripted {
        fn role(&self) -> &str {
            &self.role
        }

        async fn try_hold(&self) -> SFResult<bool> {
            let mut asked = self.asked.lock().unwrap();
            *asked += 1;
            let mut answers = self.answers.lock().unwrap();
            if answers.is_empty() {
                panic!("the lease was asked more times than the test scripted");
            }
            answers.remove(0)
        }
    }

    struct ScriptedBroker(Arc<Scripted>);

    impl OwnerLeaseBroker for ScriptedBroker {
        fn lease(&self, _role: &str, _ttl: Duration) -> Arc<dyn OwnerLease> {
            self.0.clone()
        }
    }

    fn beat_for(name: &str) -> Beat {
        LoopHealth::new().register(name, Cadence::Periodic(Duration::from_secs(60)))
    }

    fn claim(lease: Arc<Scripted>, beat: &Beat) -> RoleClaim {
        let broker: Arc<dyn OwnerLeaseBroker> = Arc::new(ScriptedBroker(lease));
        RoleClaim::new(
            Some(broker),
            "probe_role",
            Duration::from_secs(180),
            beat.clone(),
        )
    }

    /// Without a broker the deployment is the one that had no arbiter before
    /// this module existed, and its loops keep running.
    #[tokio::test]
    async fn no_broker_means_the_loop_acts() {
        let beat = beat_for("unarbitrated");
        let claim = RoleClaim::new(None, "probe_role", Duration::from_secs(180), beat);
        assert!(claim.may_act().await);
        assert!(claim.may_act().await);
    }

    #[tokio::test]
    async fn a_held_role_acts_and_another_holder_does_not() {
        let beat = beat_for("contended");
        let lease = Scripted::new("probe_role", vec![Ok(false), Ok(true)]);
        let claim = claim(lease.clone(), &beat);

        assert!(
            !claim.may_act().await,
            "another holder's term is not this one's"
        );
        assert!(claim.may_act().await, "the next ask took the expired term");
        assert_eq!(*lease.asked.lock().unwrap(), 2);
    }

    /// The ask that could not be answered keeps the work undone. It is the
    /// direction this whole module exists to get right: acting on an unknown
    /// answer writes the duplicate that cannot be taken back.
    #[tokio::test]
    async fn an_unanswerable_ask_keeps_the_work_undone() {
        let beat = beat_for("unreachable");
        let lease = Scripted::new(
            "probe_role",
            vec![Err(crate::SFError::Database("probe".into())), Ok(true)],
        );
        let claim = claim(lease, &beat);

        assert!(!claim.may_act().await);
        assert!(claim.may_act().await);
    }

    /// A lease that answers the same thing every time, and counts the asks.
    ///
    /// A sequence cannot be scripted for a holder that asks on a timer of its
    /// own: which ask of the sequence is the loop's and which is the renewer's
    /// is not something a test can order.
    struct Answering {
        held: AtomicBool,
        asked: Mutex<usize>,
    }

    #[async_trait]
    impl OwnerLease for Answering {
        fn role(&self) -> &str {
            "probe_role"
        }

        async fn try_hold(&self) -> SFResult<bool> {
            *self.asked.lock().unwrap() += 1;
            Ok(self.held.load(Ordering::Relaxed))
        }
    }

    struct AnsweringBroker(Arc<Answering>);

    impl OwnerLeaseBroker for AnsweringBroker {
        fn lease(&self, _role: &str, _ttl: Duration) -> Arc<dyn OwnerLease> {
            self.0.clone()
        }
    }

    fn answering(held: bool) -> Arc<Answering> {
        Arc::new(Answering {
            held: AtomicBool::new(held),
            asked: Mutex::new(0),
        })
    }

    fn holding(lease: Arc<Answering>, beat: &Beat, period: Duration) -> RoleHold {
        let broker: Arc<dyn OwnerLeaseBroker> = Arc::new(AnsweringBroker(lease));
        RoleHold::asking_every(
            Some(broker),
            "probe_role",
            beat.clone(),
            &crate::ShutdownSignal::default(),
            period,
        )
    }

    /// Without a broker there is no arbiter to ask, and the loop runs as it did
    /// before leases existed.
    #[tokio::test]
    async fn a_hold_without_a_broker_acts() {
        let beat = beat_for("hold_unarbitrated");
        let hold = RoleHold::start(None, "probe_role", beat, &crate::ShutdownSignal::default());
        assert!(hold.may_act().await);
        assert!(hold.may_act().await);
    }

    /// What the loop reads is a fresh ask, not the renewer's last answer: a
    /// process that has lost the role has to stop working in the cycle it lost
    /// it, and the renewal task's answer may be a whole period old.
    #[tokio::test]
    async fn a_hold_answers_the_loop_from_the_lease_not_from_the_renewer() {
        let beat = beat_for("hold_answers");
        let lease = answering(true);
        let hold = holding(Arc::clone(&lease), &beat, Duration::from_secs(3600));

        assert!(hold.may_act().await);
        lease.held.store(false, Ordering::Relaxed);
        assert!(
            !hold.may_act().await,
            "the loop's ask is what decides, and the role is no longer held"
        );
        lease.held.store(true, Ordering::Relaxed);
        assert!(hold.may_act().await);
    }

    /// The renewal has to happen without the loop asking again, which is what
    /// keeps a term alive across work longer than the loop's own cycle.
    #[tokio::test]
    async fn a_hold_keeps_asking_while_it_is_alive() {
        let beat = beat_for("hold_renews");
        let lease = answering(true);
        let hold = holding(Arc::clone(&lease), &beat, Duration::from_millis(5));

        let mut asks = 0;
        for _ in 0..400 {
            asks = *lease.asked.lock().unwrap();
            if asks >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            asks >= 3,
            "the renewal task asked {asks} time(s); a term of hours cannot be held by an ask \
             per cycle when the cycle is a pass over the log"
        );

        // And it stops when the hold does, rather than renewing a term for a
        // loop that has ended. The first wait is for the ask that was already
        // in flight when the hold was dropped; the second is the window an
        // abandoned task would have asked in several times over.
        drop(hold);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let settled = *lease.asked.lock().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *lease.asked.lock().unwrap(),
            settled,
            "the renewal task must stop when the hold is dropped"
        );
    }
}
