//! Removes the inbox streams of agents that no longer exist.
//!
//! Every agent gets a stream of its own — `orchestrator:agent:{id}:inbox` — and
//! a consumer group whose name carries the agent id. Nothing deleted either one.
//! The population therefore grew with the number of agent instances ever
//! started rather than with the number running: a live deployment held 12,571
//! such keys, of which 12,568 were empty streams left by agents that had
//! retired (the squad loop alone creates three per round, and every rollout
//! renames the instances). Empty or not, each key is a stream with its own
//! metadata and a group table in the server's memory, and Redis is configured
//! `noeviction` — the memory they hold is memory the working set cannot have.
//!
//! The liveness anchor is the same one the WAL retirement uses: an agent that
//! still exists has a registration, renewed by its heartbeat, so a missing
//! registry record means no consumer will ever read this stream again. Two
//! things guard the deletion beyond that:
//!
//! - **The stream must owe nothing to a reader.** An entry no consumer has taken
//!   is a command somebody was told had been delivered; removing it would destroy
//!   work that was accepted, which is not this pass's to decide. A stream whose
//!   group has read and acknowledged everything owes nothing even though `XLEN`
//!   still counts the history — length cannot tell the two apart, only the group
//!   state can.
//! - **Two passes must agree**, one full period apart, exactly as
//!   [`crate::wal::confirmed_for_retirement`] requires for logs. A stream is
//!   created when its agent starts and the registration lands a moment later;
//!   for that instant it has no owner, and deleting on one look would destroy
//!   the inbox of an agent that is starting up.
//!
//! What the pass never does is judge a *group* dead: groups live inside the
//! stream and go with it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::wal::RetireOutcome;

/// The loop's name in the process's loop readings.
///
/// A loop of its own rather than a step inside another sweep: only this one
/// deletes streams, so a reader asking "has retirement run" must be able to see
/// it run.
pub const INBOX_RETIRE_SWEEP_LOOP: &str = "storage_inbox_retire_sweep";

/// How often the pass looks. Matched to the WAL sweep: both delete, both need
/// the same second look, and there is nothing time-critical about either.
pub const INBOX_RETIRE_SWEEP_PERIOD: Duration = Duration::from_secs(600);

/// The prefix and suffix of an agent's inbox stream.
///
/// Written out rather than derived from [`cog_core::agent_inbox_stream`]
/// because this side has to *parse* what that function *builds*; the unit test
/// at the bottom pins the two spellings together, which is the same guard the
/// builder carries on its own side.
const INBOX_PREFIX: &str = "orchestrator:agent:";
const INBOX_SUFFIX: &str = ":inbox";

/// The agent id inside an inbox stream key, or `None` for a key that only looks
/// like one.
fn inbox_agent_id(key: &str) -> Option<&str> {
    key.strip_prefix(INBOX_PREFIX)?.strip_suffix(INBOX_SUFFIX)
}

/// What one walk of the inbox population found.
///
/// Three lists, because an unowned inbox sits in one of three states and they
/// answer different questions. `empty` and `consumed` are this pass's work: both
/// owe nothing to a reader. `holding` is the part it must never touch -- an inbox
/// whose agent is gone but whose entries no consumer has taken. Nothing used to
/// count that part at all, and nothing used to separate it from the history a
/// group had already read, so a stream that had done its job looked exactly like
/// stranded work until its entries were counted against the group state.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OrphanScan {
    /// Unowned inboxes with no entries: candidates for removal.
    pub empty: Vec<(String, i64)>,
    /// Unowned inboxes whose entries have all been taken and acknowledged, with
    /// their lengths: candidates for removal, and their entries are history
    /// rather than work.
    pub consumed: Vec<(String, i64)>,
    /// Unowned inboxes that still owe a reader something, with the number of
    /// entries still owed rather than the stream length.
    pub holding: Vec<(String, i64)>,
}

impl OrphanScan {
    /// Entries no reader will ever take, across the unowned inboxes.
    pub fn held_entries(&self) -> u64 {
        self.holding
            .iter()
            .map(|(_, unread)| (*unread).max(0) as u64)
            .sum()
    }
}

/// What one walk's script returns, in the order its reply packs it: empty keys,
/// holding keys, the entries each of them owes, consumed keys, and the length
/// each consumed stream carries.
///
/// Named rather than written out at the call site because the five-part tuple is
/// the wire shape of one Lua reply and nothing else; a reader comparing it to the
/// script's `return` line should only have to look in one place.
type WalkReply = (Vec<String>, Vec<String>, Vec<i64>, Vec<String>, Vec<i64>);

/// Redis connection used by the pass.
pub struct InboxRetirement {
    connection: ConnectionManager,
    readings: Arc<crate::inbox_orphan_readings::InboxOrphanReadings>,
}

impl InboxRetirement {
    /// Build the pass over an existing connection.
    ///
    /// Takes a connection rather than a URL: the process keeps one shared Redis
    /// manager, and a second one would be a second pool of sockets for the same
    /// server.
    pub fn new(connection: ConnectionManager) -> Self {
        Self {
            connection,
            readings: Arc::new(crate::inbox_orphan_readings::InboxOrphanReadings::new()),
        }
    }

    /// The reading this pass keeps for readers outside the process.
    pub fn readings(&self) -> Arc<crate::inbox_orphan_readings::InboxOrphanReadings> {
        Arc::clone(&self.readings)
    }

    /// Inbox streams with no live agent, sorted by what they still owe.
    ///
    /// The length comes back with every key so the confirmation step is the same
    /// "same key, same length, twice" rule the log retirement uses rather than a
    /// second mechanism that would need its own argument.
    ///
    /// The same walk also returns the unowned inboxes that still owe a reader
    /// something. They are not candidates and never will be; collecting them here
    /// costs one more `XINFO` in a script that already has the key in hand, and a
    /// second walk would be a second answer to "what is in the store" that can
    /// disagree with this one.
    pub async fn scan(&self) -> Result<OrphanScan, redis::RedisError> {
        let mut conn = self.connection.clone();
        let keys: WalkReply = redis::cmd("EVAL")
            .arg(
                // One walk, inside the server: thousands of keys, and asking for
                // them one round trip at a time costs more than the walk costs
                // in blocking. The ownership check comes first so only the keys
                // nobody owns pay for the group lookup.
                //
                // The prefix and suffix come in as arguments rather than being
                // spelled again here: a second spelling of the key shape is the
                // one thing that would make this pass find nothing and look
                // idle instead of broken.
                r#"
                local prefix = ARGV[1]
                local suffix = ARGV[2]
                local pattern = prefix .. "*" .. suffix
                local from = string.len(prefix) + 1
                local to = -string.len(suffix) - 1

                -- What a stream still owes a reader: entries its groups have
                -- not taken, and entries taken but never acknowledged. A group
                -- that read and acked everything leaves `lag` and `pending` at
                -- zero while XLEN still counts the history, so the length alone
                -- cannot answer this. Older servers and reset groups may not
                -- report `lag`; fall back to the read position rather than
                -- reading "no answer" as "nothing owed".
                local function owed(key, len)
                  local groups = redis.call("XINFO", "GROUPS", key)
                  if #groups == 0 then
                    return len, 0
                  end
                  local unread = 0
                  local pending = 0
                  for i = 1, #groups do
                    local lag = nil
                    local read = nil
                    for j = 1, #groups[i], 2 do
                      local field = groups[i][j]
                      local value = groups[i][j + 1]
                      if field == "lag" then
                        lag = value
                      elseif field == "entries-read" then
                        read = value
                      elseif field == "pending" then
                        pending = pending + value
                      end
                    end
                    if lag then
                      unread = unread + lag
                    elseif read then
                      unread = unread + math.max(0, len - read)
                    else
                      unread = unread + len
                    end
                  end
                  return unread, pending
                end

                local cur = "0"
                local empty = {}
                local holding = {}
                local owed_entries = {}
                local consumed = {}
                local consumed_lengths = {}
                repeat
                  local r = redis.call("SCAN", cur, "MATCH", pattern, "COUNT", 1000)
                  cur = r[1]
                  for i = 1, #r[2] do
                    local key = r[2][i]
                    local id = string.sub(key, from, to)
                    if string.len(id) > 0
                       and redis.call("EXISTS", ARGV[3] .. id) == 0
                       and redis.call("TYPE", key)["ok"] == "stream" then
                      local len = redis.call("XLEN", key)
                      if len == 0 then
                        empty[#empty + 1] = key
                      else
                        local unread, pending = owed(key, len)
                        if unread == 0 and pending == 0 then
                          consumed[#consumed + 1] = key
                          consumed_lengths[#consumed_lengths + 1] = len
                        else
                          holding[#holding + 1] = key
                          owed_entries[#owed_entries + 1] = unread
                        end
                      end
                    end
                  end
                until cur == "0"
                return {empty, holding, owed_entries, consumed, consumed_lengths}
                "#,
            )
            .arg(0)
            .arg(INBOX_PREFIX)
            .arg(INBOX_SUFFIX)
            .arg(crate::redis::registry::AGENT_KEY_PREFIX)
            .query_async(&mut conn)
            .await?;
        // Every list comes back with the length the confirmation rule compares
        // against; producing it here keeps the rule identical to the log
        // retirement's rather than a second one.
        Ok(OrphanScan {
            empty: keys.0.into_iter().map(|key| (key, 0)).collect(),
            consumed: keys.3.into_iter().zip(keys.4).collect(),
            holding: keys.1.into_iter().zip(keys.2).collect(),
        })
    }

    /// Inbox streams with no live agent and nothing in them: the pass's work.
    ///
    /// Kept as its own name because that is what the confirmation step takes —
    /// the callers that only retire should not have to know about the half that
    /// is reported rather than removed.
    pub async fn orphaned_streams(&self) -> Result<Vec<(String, i64)>, redis::RedisError> {
        Ok(self.scan().await?.empty)
    }

    /// Remove the given streams and report the entries that went with them.
    ///
    /// The entry conditions are asked again inside the script. The scan and the
    /// delete are separate round trips, and an inbox differs from a log in what
    /// a stale answer costs: a `PUBLISH` to an agent that is not registered yet
    /// is how a command waits for an agent that is starting up, so an entry can
    /// arrive at an unowned stream between the two. That entry is accepted work
    /// somebody was told had been queued, and deleting it would be this pass
    /// deciding to drop it — which it never does.
    ///
    /// A stream that still carries entries is removed only when its groups have
    /// taken and acknowledged all of them. Its history leaves with the key, and
    /// `records` counts it: the number is history, not work, which is why the
    /// outcome reports it rather than leaving the reader to assume zero.
    pub async fn retire_streams(
        &self,
        keys: &[String],
    ) -> Result<RetireOutcome, redis::RedisError> {
        if keys.is_empty() {
            return Ok(RetireOutcome::default());
        }
        // Only a key this pass can read as an inbox is handed to the script.
        // The scan only produces those, so anything else is a caller that built
        // its own list, and the one thing worth knowing about the delete is that
        // it can only ever remove what an agent's inbox is spelled like.
        let keys: Vec<&String> = keys
            .iter()
            .filter(|k| {
                let inbox = inbox_agent_id(k).is_some();
                if !inbox {
                    tracing::warn!(key = k.as_str(), "not an inbox key; left alone");
                }
                inbox
            })
            .collect();
        if keys.is_empty() {
            return Ok(RetireOutcome::default());
        }
        let mut conn = self.connection.clone();
        let (retired, records): (u64, u64) = redis::cmd("EVAL")
            .arg(
                // The same "what does this stream still owe" test the scan
                // uses, repeated rather than trusted: between the scan and this
                // call an entry can arrive (accepted work, must survive) or the
                // last pending entry can be acknowledged (history, may go).
                r#"
                local records = 0
                local retired = 0
                local from = string.len(ARGV[1]) + 1
                local to = -string.len(ARGV[2]) - 1

                local function owed(key, len)
                  local groups = redis.call("XINFO", "GROUPS", key)
                  if #groups == 0 then
                    return len, 0
                  end
                  local unread = 0
                  local pending = 0
                  for i = 1, #groups do
                    local lag = nil
                    local read = nil
                    for j = 1, #groups[i], 2 do
                      local field = groups[i][j]
                      local value = groups[i][j + 1]
                      if field == "lag" then
                        lag = value
                      elseif field == "entries-read" then
                        read = value
                      elseif field == "pending" then
                        pending = pending + value
                      end
                    end
                    if lag then
                      unread = unread + lag
                    elseif read then
                      unread = unread + math.max(0, len - read)
                    else
                      unread = unread + len
                    end
                  end
                  return unread, pending
                end

                for i = 1, #KEYS do
                  local key = KEYS[i]
                  local id = string.sub(key, from, to)
                  if string.len(id) > 0
                     and redis.call("EXISTS", ARGV[3] .. id) == 0
                     and redis.call("TYPE", key)["ok"] == "stream" then
                    local len = redis.call("XLEN", key)
                    local unread, pending = 0, 0
                    if len > 0 then
                      unread, pending = owed(key, len)
                    end
                    if unread == 0 and pending == 0 then
                      redis.call("DEL", key)
                      retired = retired + 1
                      records = records + len
                    end
                  end
                end
                return {retired, records}
                "#,
            )
            .arg(keys.len())
            .arg(keys)
            .arg(INBOX_PREFIX)
            .arg(INBOX_SUFFIX)
            .arg(crate::redis::registry::AGENT_KEY_PREFIX)
            .query_async(&mut conn)
            .await?;
        Ok(RetireOutcome { retired, records })
    }

    /// Run the pass on its own period until shutdown.
    pub fn spawn_retirement(
        self: Arc<Self>,
        shutdown: cog_core::ShutdownSignal,
    ) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            INBOX_RETIRE_SWEEP_LOOP,
            cog_core::loop_health::Cadence::Periodic(INBOX_RETIRE_SWEEP_PERIOD),
            shutdown.clone(),
            move |beat| {
                let backend = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move {
                    // The confirmation belongs to this loop's own history, not to
                    // the store: a restarted process has not looked twice at
                    // anything and must earn the second look again. The two
                    // candidate lists are confirmed apart because a stream moves
                    // between them as its group acknowledges entries, and a walk
                    // that saw it empty is not the same witness as one that saw
                    // its history fully read.
                    let mut last_seen: HashMap<String, i64> = HashMap::new();
                    let mut last_seen_consumed: HashMap<String, i64> = HashMap::new();
                    loop {
                        beat.beat();
                        match backend.scan().await {
                            Ok(scan) => {
                                // The reading is recorded before anything is
                                // removed: a walk that found the store dirty and
                                // then failed to clean it must not read as clean.
                                backend.readings().record(&scan);
                                let (mut confirmed, current) =
                                    crate::wal::confirmed_for_retirement(scan.empty, &last_seen);
                                let (consumed, current_consumed) =
                                    crate::wal::confirmed_for_retirement(
                                        scan.consumed,
                                        &last_seen_consumed,
                                    );
                                confirmed.extend(consumed);
                                if !confirmed.is_empty() {
                                    match backend.retire_streams(&confirmed).await {
                                        Ok(outcome) => tracing::info!(
                                            streams = outcome.retired,
                                            entries = outcome.records,
                                            "Removed agent inbox streams whose agent is no longer registered"
                                        ),
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            "Agent inbox stream retirement failed"
                                        ),
                                    }
                                }
                                last_seen = current;
                                last_seen_consumed = current_consumed;
                            }
                            Err(e) => tracing::warn!(
                                error = %e,
                                "Agent inbox stream orphan scan failed"
                            ),
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(INBOX_RETIRE_SWEEP_PERIOD) => {}
                            _ = shutdown.wait() => break,
                        }
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The parser and the builder are the two halves of one wire fact. If either
    /// side changes its spelling alone, this fails here rather than in the
    /// cluster, where the pass would quietly stop finding anything to retire.
    #[test]
    fn the_parser_reads_what_the_builder_writes() {
        for id in ["cogneva-worker-0", "agent:with:colons", "x"] {
            let key = cog_core::agent_inbox_stream(id);
            assert_eq!(inbox_agent_id(&key), Some(id));
        }
    }

    #[test]
    fn a_key_of_another_shape_is_not_an_inbox() {
        assert_eq!(inbox_agent_id("orchestrator:agent:x"), None);
        assert_eq!(inbox_agent_id("orchestrator:agent:x:outbox"), None);
        assert_eq!(inbox_agent_id("wal:x"), None);
    }
}
