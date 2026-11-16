//! Which executor replica drives a lane, and when that right lapses.
//!
//! More than one replica behind the Service means two commands from the same
//! agent can land on two pods whose checkouts are two different trees. The tree
//! is the agent's work, not a cache: a read answered by the other pod answers
//! about a tree that never had this agent's edits, and nothing in the answer
//! says so. So a lane stays on one replica until its pin lapses.
//!
//! A pin names the replica's **address**, not its position in the list. The
//! list is not a deployment fact the client owns any more: it is resolved from
//! the replicas that exist right now, and a replica added for capacity appends
//! or inserts into that list. A pin held by position would silently reassign
//! every lane after the insertion point to a different pod -- the lane would
//! move without the clock ever running out, which is the one move this module
//! exists to make visible.
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grant {
    /// The lane was not pinned to any replica; this one took it.
    Fresh,
    /// This replica already held the lane and the hold was extended.
    Extended,
    /// The pin had lapsed before this request: the lane was still pointed at
    /// the named replica, and any work that replica did after that moment is
    /// not necessarily in the tree this request will run against.
    Moved { from: String, lapsed_secs: u64 },
}

/// One lane's pin.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Hold {
    replica: String,
    until: u64,
}

/// The lanes this caller is driving and the replica each one sits on.
#[derive(Debug, Default)]
pub struct LeaseBook {
    live: HashMap<String, Hold>,
    next: usize,
}

/// Where a lane went and how it got there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pinned {
    pub replica: String,
    pub grant: Grant,
}

impl LeaseBook {
    /// Point `lane` at a replica and extend its hold to `now + ttl`.
    ///
    /// A lane nobody holds goes to the next replica in turn, so a fresh set of
    /// lanes spreads over the replicas instead of stacking on the first one.
    /// A lane that is held stays where it is; a lane whose hold has lapsed is
    /// reported as [`Grant::Moved`] and lands wherever the rotation is now.
    ///
    /// `None` when there is no replica to name. The caller has to turn that
    /// into a failure it can read: a command that silently ran somewhere else
    /// would be running on a tree nobody chose.
    pub fn pin(&mut self, lane: &str, replicas: &[String], now: u64, ttl: u64) -> Option<Pinned> {
        let until = now.saturating_add(ttl);
        let existing = self.live.get(lane).cloned();
        let holds = existing.as_ref().is_some_and(|hold| hold.until > now);
        let (replica, grant) = match existing {
            // Still live and the replica is still there: extend in place.
            Some(hold) if holds && replicas.contains(&hold.replica) => {
                (hold.replica, Grant::Extended)
            }
            // Still live but the replica is gone (a scale-down or a rollback
            // that replaced the pod): the hold has to move even though its
            // clock has not run out.
            Some(hold) if holds => (
                self.take_turn(replicas)?,
                Grant::Moved {
                    from: hold.replica,
                    lapsed_secs: 0,
                },
            ),
            Some(hold) => (
                self.take_turn(replicas)?,
                Grant::Moved {
                    from: hold.replica,
                    lapsed_secs: now.saturating_sub(hold.until),
                },
            ),
            None => (self.take_turn(replicas)?, Grant::Fresh),
        };
        self.live.insert(
            lane.to_string(),
            Hold {
                replica: replica.clone(),
                until,
            },
        );
        Some(Pinned { replica, grant })
    }

    /// The replica a lane sits on, or `None` when nothing (live) holds it.
    pub fn holder(&self, lane: &str, now: u64) -> Option<&str> {
        self.live
            .get(lane)
            .filter(|hold| hold.until > now)
            .map(|hold| hold.replica.as_str())
    }

    /// Give a lane up. Only the replica holding it can; a wrong replica's
    /// release would otherwise free a lane it never drove, and the next command
    /// on that lane would land on a tree without this caller's edits.
    pub fn release(&mut self, lane: &str, replica: &str) -> bool {
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

    fn take_turn(&mut self, replicas: &[String]) -> Option<String> {
        if replicas.is_empty() {
            return None;
        }
        let replica = replicas[self.next % replicas.len()].clone();
        self.next = self.next.wrapping_add(1);
        Some(replica)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every pin in this module is written against a clock the test owns: the
    /// rules are about when a hold lapses, and a rule proven by sleeping
    /// proves the sleep.
    const TTL: u64 = LANE_LEASE_SECS;

    fn two() -> Vec<String> {
        vec![
            "http://executor-0:9090".to_string(),
            "http://executor-1:9090".to_string(),
        ]
    }

    #[test]
    fn the_same_lane_keeps_landing_on_the_same_replica() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        let second = book.pin("agent-a", &replicas, 1_030, TTL).unwrap();
        assert_eq!(first.grant, Grant::Fresh);
        assert_eq!(second.grant, Grant::Extended);
        assert_eq!(first.replica, second.replica);
    }

    #[test]
    fn a_fresh_set_of_lanes_spreads_over_the_replicas() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let mut seen = std::collections::HashMap::new();
        for lane in ["a", "b", "c", "d"] {
            let pinned = book.pin(lane, &replicas, 1_000, TTL).unwrap();
            assert_eq!(pinned.grant, Grant::Fresh);
            *seen.entry(pinned.replica).or_insert(0) += 1;
        }
        assert_eq!(seen.len(), 2, "四条新 lane 不该全堆在一副副本上");
        assert!(seen.values().all(|count| *count == 2));
    }

    /// A pin that lapses while nobody asks says so when it is taken again —
    /// the caller has to know the tree may have moved under it.
    #[test]
    fn a_lane_whose_pin_lapsed_is_reported_as_having_moved() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        assert_eq!(
            book.holder("agent-a", 1_000 + TTL),
            None,
            "刚好到期时已不再是持有人"
        );
        let again = book
            .pin("agent-a", &replicas, 1_000 + TTL + 45, TTL)
            .unwrap();
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
        let replicas = two();
        // Turn order decides which replica each lane sat on: `agent-b` takes
        // the first turn, so `agent-a` is the one that sat on the second and is
        // the one the scale-down has to move.
        book.pin("agent-b", &replicas, 1_000, TTL).unwrap();
        let on_second = book.pin("agent-a", &replicas, 1_000, TTL).unwrap().replica;
        assert_eq!(on_second, replicas[1]);
        let survivor = book.pin("agent-a", &replicas[..1], 1_010, TTL).unwrap();
        assert_eq!(survivor.replica, replicas[0]);
        assert_eq!(
            survivor.grant,
            Grant::Moved {
                from: on_second,
                lapsed_secs: 0
            }
        );
    }

    /// The whole point of naming the address: a replica added in the middle of
    /// the list must not move a lane that is sitting on a replica which is
    /// still there. Positional pins fail exactly here.
    #[test]
    fn adding_a_replica_does_not_move_a_lane_that_is_still_at_its_address() {
        let mut book = LeaseBook::default();
        let before = two();
        let pinned = book.pin("agent-b", &before, 1_000, TTL).unwrap();
        let mut after = vec!["http://executor-new:9090".to_string()];
        after.extend(before.iter().cloned());
        after.sort();
        let again = book.pin("agent-b", &after, 1_005, TTL).unwrap();
        assert_eq!(again.replica, pinned.replica);
        assert_eq!(again.grant, Grant::Extended);
    }

    #[test]
    fn only_the_replica_that_holds_a_lane_can_release_it() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let pinned = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        let other = replicas
            .iter()
            .find(|one| **one != pinned.replica)
            .expect("another replica exists");
        assert!(!book.release("agent-a", other));
        assert_eq!(book.holder("agent-a", 1_000), Some(pinned.replica.as_str()));
        assert!(book.release("agent-a", &pinned.replica));
        assert_eq!(book.holder("agent-a", 1_000), None);
        assert!(
            !book.release("agent-a", &pinned.replica),
            "已经放掉的不能再放一次"
        );
        assert_eq!(
            book.pin("agent-a", &replicas, 1_001, TTL).unwrap().grant,
            Grant::Fresh
        );
    }

    #[test]
    fn the_book_says_how_many_lanes_it_is_holding() {
        let mut book = LeaseBook::default();
        let replicas = two();
        assert_eq!(book.pinned_lanes(), 0);
        book.pin("a", &replicas, 1_000, TTL).unwrap();
        book.pin("b", &replicas, 1_000, TTL).unwrap();
        assert_eq!(book.pinned_lanes(), 2);
        let held = book
            .holder("a", 1_000)
            .expect("刚钉上的 lane 有持有人")
            .to_string();
        book.release("a", &held);
        assert_eq!(book.pinned_lanes(), 1);
    }

    /// No replica to name is "no answer", not "replica zero". A caller that
    /// ignores this would send the command to an address nobody chose.
    #[test]
    fn a_lane_cannot_be_pinned_when_no_replica_is_named() {
        let mut book = LeaseBook::default();
        assert!(book.pin("agent-a", &[], 1_000, TTL).is_none());
        assert_eq!(book.pinned_lanes(), 0);
    }
}
