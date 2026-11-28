//! Which executor replica drives a lane, and when that right lapses.
//!
//! More than one replica behind the Service means two commands from the same
//! agent can land on two pods whose checkouts are two different trees. The tree
//! is the agent's work, not a cache: a read answered by the other pod answers
//! about a tree that never had this agent's edits, and nothing in the answer
//! says so. So a lane stays on one replica for as long as that replica is
//! there.
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
//! long as the client lived. A lapse only refreshes the right to the lane; it
//! does not move the lane, because the tree it is working in is still on that
//! replica's volume. On the other side a live caller does say goodbye
//! to a replica it cannot reach: a refused connection gives the lane up at
//! once, so the clock below is only the backstop for a caller that vanished
//! without saying anything.
//!
//! What a moved pin costs is said out loud: [`Grant::Moved`] carries where the
//! lane was and how long its pin had been lapsed, because a lane that changes
//! replica changes tree, and the caller is the only layer that can decide
//! whether that is acceptable for the command it is about to run. A pin whose
//! clock ran out does not move anything by itself -- [`Grant::Resumed`] says
//! the lane is back on the same replica, and whatever the replica itself says
//! about the tree is the only fact about the work.

use std::collections::{HashMap, HashSet};

use crate::pressure::Placement;

/// How long a pin counts as live without being used again.
///
/// The clock is not what keeps a lane on its replica -- being on that replica
/// is, because the tree is on its volume. What the clock decides is whether
/// the next command can say "this session never stopped" or has to say "nobody
/// asked for this lane for a while": the second one is still sent to the same
/// replica, but the replica may have reclaimed the tree in the meantime, and
/// its own first frame is what settles that.
// cap: fixed — 一条钉子多久算「还活着」：超过它，下一次只能读成「隔了一段时间才回来」
// keep: 删掉后下一次命令分不出「一直没停」与「隔了一段时间才回来」，被回收的树会被读成还在；由「一条钉子多久算还活着」兜住
pub const LANE_LEASE_SECS: u64 = 600;

/// What happened when a lane was pinned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grant {
    /// The lane was not pinned to any replica; this one took it.
    Fresh,
    /// This replica already held the lane and the hold was extended.
    Extended,
    /// The pin had lapsed before this request and the lane's replica is not
    /// one this request could reach: the lane was still pointed at the named
    /// replica, and any work that replica did after that moment is not
    /// necessarily in the tree this request will run against.
    Moved { from: String, lapsed_secs: u64 },
    /// The pin's clock had run out, and the lane is going back to the same
    /// replica. Nothing moved: that replica's volume is where the lane's tree
    /// is, so a lapse is not a reason to leave it. Whether the tree survived
    /// is the replica's own answer, on the first frame of the command.
    Resumed { lapsed_secs: u64 },
}

/// One lane's pin.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Hold {
    replica: String,
    until: u64,
    /// The replica stopped answering and the caller said so. The lane is not
    /// held any more -- the next command must be free to go somewhere that
    /// answers -- but the address is remembered so that command says which
    /// tree it left behind instead of arriving as if this were a new lane.
    abandoned: bool,
}

/// The lanes this caller is driving and the replica each one sits on.
#[derive(Debug, Default)]
pub struct LeaseBook {
    live: HashMap<String, Hold>,
    next: usize,
    /// The replicas this book was offered at its previous placement. A replica
    /// that is not among them has appeared since -- that is capacity the
    /// deployment added, and saying so is different from saying this lane
    /// happened to take an emptier replica.
    fleet: HashSet<String>,
}

/// Where a lane went and how it got there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pinned {
    pub replica: String,
    pub grant: Grant,
    /// Why this replica, in the four shapes the scrape surface can tell apart.
    pub placement: Placement,
}

impl LeaseBook {
    /// Point `lane` at a replica and extend its hold to `now + ttl`.
    ///
    /// A lane nobody holds goes to the next replica in turn, so a fresh set of
    /// lanes spreads over the replicas instead of stacking on the first one.
    /// A lane that is held stays where it is, and so does a lane whose clock
    /// ran out while its replica is still there: the tree lives on that
    /// replica's volume, and leaving it would throw away work for nothing but
    /// the amount of time nobody asked. Only a lane whose replica is gone is
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
            // The replica stopped answering: the lane moves somewhere else, and
            // the move is what the command has to hear about even though no
            // clock ran out. The rotation must skip the replica that stopped
            // answering -- landing back on it fails the command again, and the
            // move reads the same at both ends, so it also loses the line that
            // says what was left behind. With nowhere else to go (one replica
            // known), the lane stays where it was: there is no move to report,
            // and the command's failure says the replica is unreachable.
            Some(hold) if hold.abandoned => {
                let elsewhere: Vec<String> = replicas
                    .iter()
                    .filter(|replica| **replica != hold.replica)
                    .cloned()
                    .collect();
                let destination = if elsewhere.is_empty() {
                    replicas
                } else {
                    &elsewhere
                };
                (
                    self.take_turn(lane, destination)?,
                    Grant::Moved {
                        from: hold.replica,
                        lapsed_secs: 0,
                    },
                )
            }
            // The replica is still there: stay, whether the clock ran out or
            // not. Its volume is where the lane's tree is; rotating a lapsed
            // lane to another replica would trade the tree for nothing. A
            // lapsed hold is reported as its own kind of grant so that a
            // reader can tell "kept" from "kept after nobody asked for a
            // while" -- the replica's first frame says whether the tree under
            // it survived, and there is nothing else to say here.
            Some(hold) if replicas.contains(&hold.replica) => (
                hold.replica,
                if holds {
                    Grant::Extended
                } else {
                    Grant::Resumed {
                        lapsed_secs: now.saturating_sub(hold.until),
                    }
                },
            ),
            // The replica is gone (a scale-down or a rollback that replaced
            // the pod): the hold has to move even though its clock has not
            // run out.
            Some(hold) if holds => (
                self.take_turn(lane, replicas)?,
                Grant::Moved {
                    from: hold.replica,
                    lapsed_secs: 0,
                },
            ),
            Some(hold) => (
                self.take_turn(lane, replicas)?,
                Grant::Moved {
                    from: hold.replica,
                    lapsed_secs: now.saturating_sub(hold.until),
                },
            ),
            None => (self.take_turn(lane, replicas)?, Grant::Fresh),
        };
        let placement = match &grant {
            // The tree is here; staying is not a choice between replicas.
            Grant::Extended | Grant::Resumed { .. } => Placement::Nailed {
                replica: replica.clone(),
            },
            _ if replicas.len() == 1 => Placement::OnlyOne {
                replica: replica.clone(),
            },
            // The fleet this book was last offered did not contain this
            // replica: the placement used capacity that was added since, which
            // is a different fact from picking the emptier of two.
            _ if !self.fleet.is_empty() && !self.fleet.contains(&replica) => {
                Placement::AddedReplica {
                    replica: replica.clone(),
                }
            }
            _ => Placement::Headroom {
                replica: replica.clone(),
            },
        };
        self.fleet = replicas.iter().cloned().collect();
        self.live.insert(
            lane.to_string(),
            Hold {
                replica: replica.clone(),
                until,
                abandoned: false,
            },
        );
        Some(Pinned {
            replica,
            grant,
            placement,
        })
    }

    /// The replica a lane sits on, or `None` when nothing (live) holds it.
    pub fn holder(&self, lane: &str, now: u64) -> Option<&str> {
        self.live
            .get(lane)
            .filter(|hold| hold.until > now)
            .map(|hold| hold.replica.as_str())
    }

    /// Whether this book has ever placed the lane, whether or not its clock
    /// has run out.
    ///
    /// A lapsed pin still names the replica whose volume the tree is on, so a
    /// caller that has one must not go looking for the tree elsewhere; only a
    /// book with no entry at all belongs to a driver that has not seen this
    /// lane yet.
    pub fn knows(&self, lane: &str) -> bool {
        self.live.contains_key(lane)
    }

    /// Put a lane on a replica that has just said it holds the lane's tree.
    ///
    /// This is what a driver that has just started does with its empty book:
    /// the tree outlives the process, and the replica that kept it answers for
    /// it. Adopting is not a move -- nothing was left behind and nothing is
    /// announced as missing; the replica's first frame still says whether the
    /// tree it hands over is the existing one.
    ///
    /// A lane this book already placed keeps its pin: the existing answer is
    /// about time and rotation, which this call has no business overriding.
    pub fn adopt(&mut self, lane: &str, replica: &str, now: u64, ttl: u64) {
        if self.live.contains_key(lane) {
            return;
        }
        self.live.insert(
            lane.to_string(),
            Hold {
                replica: replica.to_string(),
                until: now.saturating_add(ttl),
                abandoned: false,
            },
        );
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

    /// Mark a lane's replica as gone. Only the replica holding it can; a wrong
    /// replica's word would move a lane it never drove, and the next command on
    /// that lane would land in a tree without this caller's edits.
    ///
    /// This is not [`Self::release`]: releasing forgets where the lane was, and
    /// the next command then arrives as [`Grant::Fresh`] -- silently, in
    /// whatever tree the rotation picks. A lane whose replica died still had a
    /// tree there, and the command that lands elsewhere must say so.
    pub fn abandon(&mut self, lane: &str, replica: &str) -> bool {
        match self.live.get_mut(lane) {
            Some(hold) if hold.replica == replica => {
                hold.abandoned = true;
                hold.until = 0;
                true
            }
            _ => false,
        }
    }

    /// How many lanes are pinned right now (live or lapsed-but-unused).
    pub fn pinned_lanes(&self) -> usize {
        self.live.values().filter(|hold| !hold.abandoned).count()
    }

    /// The replica with the fewest of this book's lanes on it.
    ///
    /// The count is the caller's own: how many lanes this book holds on each
    /// replica, which is a fact about this client. What a replica knows about
    /// its own load is not readable from here, and inventing it would make
    /// "spread by what I can see" and "spread by what it is doing" read the
    /// same. Ties are broken by the rotation, so a fresh set of lanes still
    /// spreads evenly instead of stacking on the first replica. A lane being
    /// placed is not counted against its old replica: it is leaving.
    fn take_turn(&mut self, lane: &str, replicas: &[String]) -> Option<String> {
        if replicas.is_empty() {
            return None;
        }
        let start = self.next % replicas.len();
        let mut best = start;
        for offset in 1..replicas.len() {
            let at = (start + offset) % replicas.len();
            if self.held_on(&replicas[at], lane) < self.held_on(&replicas[best], lane) {
                best = at;
            }
        }
        self.next = (best + 1) % replicas.len();
        Some(replicas[best].clone())
    }

    fn held_on(&self, replica: &str, except_lane: &str) -> usize {
        self.live
            .iter()
            .filter(|(lane, hold)| {
                hold.replica == replica && !hold.abandoned && lane.as_str() != except_lane
            })
            .count()
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

    /// 客户端重启、这副账是空的：那副答「树在我这」的副本先被记下，之后这条
    /// lane 的每次放置都回到它身上，而不是按轮转另找一副。
    #[test]
    fn an_adopted_lane_stays_on_the_replica_that_answered_for_it() {
        let mut book = LeaseBook::default();
        let replicas = two();
        assert!(!book.knows("agent-a"));
        book.adopt("agent-a", &replicas[1], 1_000, TTL);
        assert!(book.knows("agent-a"));
        let first = book.pin("agent-a", &replicas, 1_010, TTL).unwrap();
        assert_eq!(first.replica, replicas[1]);
        assert_eq!(first.grant, Grant::Extended);
    }

    /// 已经记过一条 lane 的账不会被一次答话改写：那条 lane 的树在哪副，是
    /// 时间与轮转算出来的，不是一次读目录说了算。
    #[test]
    fn adopting_a_lane_this_book_already_placed_changes_nothing() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let placed = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        book.adopt("agent-a", &replicas[0], 1_000, TTL);
        let again = book.pin("agent-a", &replicas, 1_010, TTL).unwrap();
        assert_eq!(again.replica, placed.replica);
    }

    /// 一副副本时说不出「平衡」：`OnlyOne` 与「挑中了最少的那副」是两件事，
    /// 混在一起会让「只有一副可去」看起来像是导向的结果。
    #[test]
    fn a_single_replica_is_not_a_choice_between_replicas() {
        let mut book = LeaseBook::default();
        let replicas = vec!["http://executor-0:9090".to_string()];
        let pinned = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        assert_eq!(
            pinned.placement,
            Placement::OnlyOne {
                replica: replicas[0].clone()
            }
        );
        assert_eq!(pinned.placement.reason(), "only_one");
    }

    /// 第一次放置没有「上一次的副本集合」可比较：读成余量，不读成「副本是
    /// 新加的」——后者说的是部署加过容量，而这里什么也没加。
    #[test]
    fn the_first_placement_of_a_book_is_not_an_added_replica() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        assert_eq!(
            first.placement,
            Placement::Headroom {
                replica: replicas[0].clone()
            }
        );
    }

    /// 留在原地的 lane 说的是「树在这副」，不是「这副最空」：它不参与平衡。
    #[test]
    fn a_lane_kept_on_its_tree_is_nailed_not_balanced() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        book.pin("agent-b", &replicas, 1_000, TTL).unwrap();
        let again = book.pin("agent-a", &replicas, 1_010, TTL).unwrap();
        assert_eq!(again.replica, first.replica);
        assert_eq!(
            again.placement,
            Placement::Nailed {
                replica: first.replica.clone()
            }
        );
    }

    /// 重活导向与 lane 路由是同一个判决：去向是这本簿在各副上持有的 lane 数
    /// 最少的那副。放掉一条之后那一副重新变空，下一条新 lane 就去那里。
    #[test]
    fn a_lane_goes_to_the_replica_this_book_has_fewest_lanes_on() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let a = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        let b = book.pin("agent-b", &replicas, 1_000, TTL).unwrap();
        assert_ne!(a.replica, b.replica);
        assert!(book.release("agent-a", &a.replica));
        let c = book.pin("agent-c", &replicas, 1_010, TTL).unwrap();
        assert_eq!(c.replica, a.replica, "最空的那副没被挑中");
        assert_eq!(
            c.placement,
            Placement::Headroom {
                replica: a.replica.clone()
            }
        );
    }

    /// 上次放置之后才出现的副本：那是部署加进来的容量，不是这本簿挑中的余量。
    #[test]
    fn a_replica_that_appeared_since_the_last_placement_is_named_as_added() {
        let mut book = LeaseBook::default();
        let before = two();
        book.pin("agent-a", &before, 1_000, TTL).unwrap();
        book.pin("agent-b", &before, 1_000, TTL).unwrap();
        let mut after = before.clone();
        after.push("http://executor-2:9090".to_string());
        let placed = book.pin("agent-c", &after, 1_001, TTL).unwrap();
        assert_eq!(placed.replica, after[2]);
        assert_eq!(
            placed.placement,
            Placement::AddedReplica {
                replica: after[2].clone()
            }
        );
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

    /// 时钟走完了不等于要换副本：那棵树立在这副副本的卷上。回到同一副，
    /// 读成「续上」而不是「搬家」，树在不在由这副副本的第一帧回答。
    #[test]
    fn a_lane_whose_clock_ran_out_stays_where_its_tree_lives() {
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
        assert_eq!(again.replica, first.replica, "过期不是离开那棵树的理由");
        assert_eq!(again.grant, Grant::Resumed { lapsed_secs: 45 });
    }

    /// 过期之后别的 lane 把轮转指针推走也没关系：这条 lane 不去轮转。
    #[test]
    fn a_lapsed_lane_is_not_carried_away_by_other_lanes_rotating() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        assert_eq!(first.replica, replicas[0]);
        // 两条新 lane 的钉把下一次轮转推到第二副。
        book.pin("agent-b", &replicas, 1_001, TTL).unwrap();
        book.pin("agent-c", &replicas, 1_002, TTL).unwrap();
        let again = book
            .pin("agent-a", &replicas, 1_000 + TTL + 5, TTL)
            .unwrap();
        assert_eq!(again.replica, first.replica);
        assert_eq!(again.grant, Grant::Resumed { lapsed_secs: 5 });
    }

    /// 过期且那副副本已经不在：没有树可回，读成搬家，并说出过了多久。
    #[test]
    fn a_lane_that_lapsed_and_lost_its_replica_still_moves() {
        let mut book = LeaseBook::default();
        let replicas = two();
        book.pin("agent-b", &replicas, 1_000, TTL).unwrap();
        let on_second = book.pin("agent-a", &replicas, 1_000, TTL).unwrap().replica;
        assert_eq!(on_second, replicas[1]);
        let moved = book
            .pin("agent-a", &replicas[..1], 1_000 + TTL + 60, TTL)
            .unwrap();
        assert_eq!(moved.replica, replicas[0]);
        assert_eq!(
            moved.grant,
            Grant::Moved {
                from: on_second,
                lapsed_secs: 60
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

    /// 一副副本不再应答时，那条 lane 不是「从没钉过」：它在那副卷上有一棵树。
    /// 下一条命令要去一副会应答的副本，并且必须说出它离开了哪一副——这正是
    /// 放掉钉子做不到的事（放掉之后下一条读成 Fresh，落进一棵没人说过的树）。
    #[test]
    fn a_lane_whose_replica_stopped_answering_moves_and_says_where_it_left() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        assert!(book.abandon("agent-a", &first.replica));
        assert_eq!(
            book.holder("agent-a", 1_000),
            None,
            "不再应答的副本不是持有人"
        );
        assert_eq!(book.pinned_lanes(), 0, "被弃的钉子不算还钉着的 lane");
        let next = book.pin("agent-a", &replicas, 1_001, TTL).unwrap();
        assert_eq!(
            next.grant,
            Grant::Moved {
                from: first.replica.clone(),
                lapsed_secs: 0
            },
            "弃掉的钉子要读成搬家，不是一条新 lane"
        );
        assert_ne!(next.replica, first.replica, "不能回到不再应答的那副");
    }

    /// 排除必须是真的排除，不是「这一圈刚好转不到它」：别的 lane 的钉会把
    /// 轮转的指针推走，回到那副不应答的副本上时，命令再白失败一次，而且
    /// 搬家那句因为两端相同被压掉——既没跑成也没说清。
    #[test]
    fn an_abandoned_lane_does_not_go_back_to_the_replica_that_stopped_answering() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        assert_eq!(first.replica, replicas[0]);
        assert!(book.abandon("agent-a", &first.replica));

        // 另一条 lane 的钉把轮转指针推到「下一次正好回到 replicas[0]」。
        let other = book.pin("agent-b", &replicas, 1_001, TTL).unwrap();
        assert_eq!(other.replica, replicas[1]);

        let next = book.pin("agent-a", &replicas, 1_002, TTL).unwrap();
        assert_ne!(next.replica, first.replica, "又落回不应答的那副副本");
        assert_eq!(
            next.grant,
            Grant::Moved {
                from: first.replica,
                lapsed_secs: 0
            }
        );
    }

    /// 只有一副副本时没有别处可去：留在原地，不报「搬家」，那副副本自己的
    /// 不可达错误就是这条命令的答案。
    #[test]
    fn an_abandoned_lane_stays_when_there_is_nowhere_else_to_go() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let first = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        book.abandon("agent-a", &first.replica);
        let same = book
            .pin("agent-a", &replicas[..1], 1_001, TTL)
            .expect("还有一副副本可去，即使它刚失联");
        assert_eq!(same.replica, first.replica);
    }

    #[test]
    fn only_the_holding_replica_can_abandon_a_lane() {
        let mut book = LeaseBook::default();
        let replicas = two();
        let pinned = book.pin("agent-a", &replicas, 1_000, TTL).unwrap();
        let other = replicas
            .iter()
            .find(|one| **one != pinned.replica)
            .expect("another replica exists");
        assert!(!book.abandon("agent-a", other));
        assert_eq!(
            book.holder("agent-a", 1_000),
            Some(pinned.replica.as_str()),
            "别人说一句「它不应答了」不该把这条 lane 挪走"
        );
        assert_eq!(
            book.pin("agent-a", &replicas, 1_001, TTL).unwrap().grant,
            Grant::Extended
        );
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
