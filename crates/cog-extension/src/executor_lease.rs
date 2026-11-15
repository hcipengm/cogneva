//! Which executor replica drives a lane, and when that right lapses.
//!
//! More than one replica behind the Service means two commands from the same
//! agent can land on two pods whose checkouts are two different trees. The tree
//! is the agent's work, not a cache: a read answered by the other pod answers
//! about a tree that never had this agent's edits, and nothing in the answer
//! says so. So a lane stays on one replica until its pin lapses.
//!
//! The pin lapses on a clock, not on a goodbye. A replica that dies never says
//! it is done, and a pin that waited for a goodbye would hold its lanes for as
//! long as the client lived. On the other side a live caller does say goodbye
//! to a replica it cannot reach: a refused connection releases the lane at
//! once, so the clock below is only the backstop for a caller that vanished
//! without saying anything.
//!
//! What a moved pin costs is said out loud: [`Grant::Moved`] carries where the
//! lane was and how long its pin had been lapsed, because a lane that changes
//! replica changes tree, and the caller is the only layer that can decide
//! whether that is acceptable for the command it is about to run.

use std::collections::HashMap;

/// How long a pin holds without being used again.
///
/// A pin costs nothing to keep and a lot to lose: the next command on a moved
/// lane sees a tree without the previous commands' edits. So this is long, and
/// the cheap releases (the holder completing, a refused connection) carry the
/// precision that would otherwise have to come from a short clock here.
// cap: bound — 后台兜底：一个消失了、没说再见的调用方最多把一条 lane 占到这么久
pub const LANE_LEASE_SECS: u64 = 600;

/// What happened when a lane was pinned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grant {
    /// The lane was not pinned to any replica; this one took it.
    Fresh,
    /// This replica already held the lane and the hold was extended.
    Extended,
    /// The pin had lapsed before this request: the lane was still pointed at
    /// the named replica, and any work that replica did after that moment is
    /// not necessarily in the tree this request will run against.
    Moved { from: usize, lapsed_secs: u64 },
}

/// One lane's pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Hold {
    replica: usize,
    until: u64,
}

/// The lanes this caller is driving and the replica each one sits on.
#[derive(Debug, Default)]
pub struct LeaseBook {
    live: HashMap<String, Hold>,
    next: usize,
}

/// Where a lane went and how it got there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pinned {
    pub replica: usize,
    pub grant: Grant,
}

impl LeaseBook {
    /// Point `lane` at a replica and extend its hold to `now + ttl`.
    ///
    /// A lane nobody holds goes to the next replica in turn, so a fresh set of
    /// lanes spreads over the replicas instead of stacking on the first one.
    /// A lane that is held stays where it is; a lane whose hold has lapsed is
    /// reported as [`Grant::Moved`] and lands wherever the rotation is now.
    pub fn pin(&mut self, lane: &str, replicas: usize, now: u64, ttl: u64) -> Pinned {
        let until = now.saturating_add(ttl);
        if replicas == 0 {
            // No replica to name. The caller has nothing to do with this
            // answer, but a panic would take a working process down over a
            // configuration mistake that is already visible at the config.
            return Pinned {
                replica: 0,
                grant: Grant::Fresh,
            };
        }
        let existing = self.live.get(lane).copied();
        let (replica, grant) = match existing {
            Some(hold) if hold.until > now && hold.replica < replicas => {
                (hold.replica, Grant::Extended)
            }
            Some(hold) if hold.until > now => {
                // The replica this lane was on has gone away (a scale-down or
                // a rollout), so the hold has to move even though its clock has
                // not run out.
                (
                    self.take_turn(replicas),
                    Grant::Moved {
                        from: hold.replica,
                        lapsed_secs: 0,
                    },
                )
            }
            Some(hold) => (
                self.take_turn(replicas),
                Grant::Moved {
                    from: hold.replica,
                    lapsed_secs: now.saturating_sub(hold.until),
                },
            ),
            None => (self.take_turn(replicas), Grant::Fresh),
        };
        self.live.insert(lane.to_string(), Hold { replica, until });
        Pinned { replica, grant }
    }

    /// The replica a lane sits on, or `None` when nothing (live) holds it.
    pub fn holder(&self, lane: &str, now: u64) -> Option<usize> {
        self.live
            .get(lane)
            .filter(|hold| hold.until > now)
            .map(|hold| hold.replica)
    }

    /// Give a lane up. Only the replica holding it can; a wrong replica's
    /// release would otherwise free a lane it never drove, and the next command
    /// on that lane would land on a tree without this caller's edits.
    pub fn release(&mut self, lane: &str, replica: usize) -> bool {
        match self.live.get(lane) {
            Some(hold) if hold.replica == replica => {
                self.live.remove(lane);
                true
            }
            _ => false,
        }
    }

    /// How many lanes are pinned right now (live or lapsed-but-unused).
    pub fn pinned_lanes(&self) -> usize {
        self.live.len()
    }

    fn take_turn(&mut self, replicas: usize) -> usize {
        let replica = self.next % replicas;
        self.next = self.next.wrapping_add(1);
        replica
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every pin in this module is written against a clock the test owns: the
    /// rules are about when a hold lapses, and a rule proven by sleeping
    /// proves the sleep.
    const TTL: u64 = LANE_LEASE_SECS;

    #[test]
    fn the_same_lane_keeps_landing_on_the_same_replica() {
        let mut book = LeaseBook::default();
        let first = book.pin("agent-a", 2, 1_000, TTL);
        let second = book.pin("agent-a", 2, 1_030, TTL);
        assert_eq!(first.grant, Grant::Fresh);
        assert_eq!(second.grant, Grant::Extended);
        assert_eq!(first.replica, second.replica);
    }

    #[test]
    fn a_fresh_set_of_lanes_spreads_over_the_replicas() {
        let mut book = LeaseBook::default();
        let mut seen = [0usize; 2];
        for lane in ["a", "b", "c", "d"] {
            let pinned = book.pin(lane, 2, 1_000, TTL);
            assert_eq!(pinned.grant, Grant::Fresh);
            seen[pinned.replica] += 1;
        }
        assert_eq!(seen, [2, 2], "四条新 lane 不该全堆在一副副本上");
    }

    /// A pin that lapses while nobody asks says so when it is taken again —
    /// the caller has to know the tree may have moved under it.
    #[test]
    fn a_lane_whose_pin_lapsed_is_reported_as_having_moved() {
        let mut book = LeaseBook::default();
        let first = book.pin("agent-a", 2, 1_000, TTL);
        assert_eq!(
            book.holder("agent-a", 1_000 + TTL),
            None,
            "刚好到期时已不再是持有人"
        );
        let again = book.pin("agent-a", 2, 1_000 + TTL + 45, TTL);
        assert_eq!(
            again.grant,
            Grant::Moved {
                from: first.replica,
                lapsed_secs: 45
            }
        );
    }

    /// A replica that is no longer there cannot keep its lanes: the hold moves
    /// even though its clock has not run out.
    #[test]
    fn a_lane_pinned_to_a_vanished_replica_moves_before_its_clock_runs_out() {
        let mut book = LeaseBook::default();
        // Turn order decides which replica each lane sat on: `agent-b` takes
        // the first turn, so `agent-a` is the one that sat on replica 1 and is
        // the one the scale-down has to move.
        book.pin("agent-b", 2, 1_000, TTL);
        assert_eq!(book.pin("agent-a", 2, 1_000, TTL).replica, 1);
        let survivor = book.pin("agent-a", 1, 1_010, TTL);
        assert_eq!(survivor.replica, 0);
        assert!(matches!(survivor.grant, Grant::Moved { .. }));
    }

    #[test]
    fn only_the_replica_that_holds_a_lane_can_release_it() {
        let mut book = LeaseBook::default();
        let pinned = book.pin("agent-a", 2, 1_000, TTL);
        let other = (pinned.replica + 1) % 2;
        assert!(!book.release("agent-a", other));
        assert_eq!(book.holder("agent-a", 1_000), Some(pinned.replica));
        assert!(book.release("agent-a", pinned.replica));
        assert_eq!(book.holder("agent-a", 1_000), None);
        assert!(
            !book.release("agent-a", pinned.replica),
            "已经放掉的不能再放一次"
        );
        assert_eq!(book.pin("agent-a", 2, 1_001, TTL).grant, Grant::Fresh);
    }

    #[test]
    fn the_book_says_how_many_lanes_it_is_holding() {
        let mut book = LeaseBook::default();
        assert_eq!(book.pinned_lanes(), 0);
        book.pin("a", 2, 1_000, TTL);
        book.pin("b", 2, 1_000, TTL);
        assert_eq!(book.pinned_lanes(), 2);
        let held = book.holder("a", 1_000).expect("刚钉上的 lane 有持有人");
        book.release("a", held);
        assert_eq!(book.pinned_lanes(), 1);
    }
}
