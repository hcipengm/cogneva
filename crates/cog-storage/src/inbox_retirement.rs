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
//! - **The stream must be empty.** A stream with entries is a queue of commands
//!   somebody was told had been delivered; removing it would destroy work that
//!   was accepted, which is not this pass's to decide.
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
/// Two lists rather than one, because the two halves answer different questions
/// and only one of them is this pass's work. `empty` is what retirement removes;
/// `holding` is the part it must never touch -- an inbox whose agent is gone but
/// which still carries commands somebody was told had been delivered. Nothing
/// used to count the second half at all, so it grew in silence: the retirement
/// loop reports what it removed and its counters cannot show what it left.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OrphanScan {
    /// Unowned inboxes with no entries: candidates for removal.
    pub empty: Vec<(String, i64)>,
    /// Unowned inboxes that still hold entries, with their lengths.
    pub holding: Vec<(String, i64)>,
}

impl OrphanScan {
    /// Entries still sitting in inboxes no live agent will ever read.
    pub fn held_entries(&self) -> u64 {
        self.holding
            .iter()
            .map(|(_, len)| (*len).max(0) as u64)
            .sum()
    }
}

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

    /// Inbox streams with no live agent, and their length.
    ///
    /// The length is zero for every candidate — that is the entry condition —
    /// and comes back with the key so the confirmation step is the same
    /// "same key, same length, twice" rule the log retirement uses rather than a
    /// second mechanism that would need its own argument.
    ///
    /// The same walk also returns the unowned inboxes that are *not* empty.
    /// They are not candidates and never will be; collecting them here costs one
    /// more `XLEN` in a script that already calls it, and a second walk would be
    /// a second answer to "what is in the store" that can disagree with this one.
    pub async fn scan(&self) -> Result<OrphanScan, redis::RedisError> {
        let mut conn = self.connection.clone();
        let keys: (Vec<String>, Vec<String>, Vec<i64>) = redis::cmd("EVAL")
            .arg(
                // One walk, inside the server: thousands of keys, and asking for
                // them one round trip at a time costs more than the walk costs
                // in blocking. `XLEN` is checked after existence so a stream on a
                // session that still holds an unread command is never a
                // candidate.
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
                local cur = "0"
                local empty = {}
                local holding = {}
                local lengths = {}
                repeat
                  local r = redis.call("SCAN", cur, "MATCH", pattern, "COUNT", 1000)
                  cur = r[1]
                  for i = 1, #r[2] do
                    local key = r[2][i]
                    local id = string.sub(key, from, to)
                    if string.len(id) > 0
                       and redis.call("EXISTS", "orchestrator:agents:" .. id) == 0
                       and redis.call("TYPE", key)["ok"] == "stream" then
                      -- One length read decides both halves: zero is the entry
                      -- condition for retirement, and anything else is work no
                      -- reader will ever take.
                      local len = redis.call("XLEN", key)
                      if len == 0 then
                        empty[#empty + 1] = key
                      else
                        holding[#holding + 1] = key
                        lengths[#lengths + 1] = len
                      end
                    end
                  end
                until cur == "0"
                return {empty, holding, lengths}
                "#,
            )
            .arg(0)
            .arg(INBOX_PREFIX)
            .arg(INBOX_SUFFIX)
            .query_async(&mut conn)
            .await?;
        // Zero is the entry condition, so every candidate comes back with the
        // length the confirmation rule compares against; producing it here keeps
        // the rule identical to the log retirement's rather than a second one.
        Ok(OrphanScan {
            empty: keys.0.into_iter().map(|key| (key, 0)).collect(),
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

    /// Remove the given streams and report what they held.
    ///
    /// Both entry conditions are asked again inside the script. The scan and the
    /// delete are separate round trips, and an inbox differs from a log in what
    /// a stale answer costs: a `PUBLISH` to an agent that is not registered yet
    /// is how a command waits for an agent that is starting up, so an entry can
    /// arrive at an unowned stream between the two. That entry is accepted work
    /// somebody was told had been queued, and deleting it would be this pass
    /// deciding to drop it — which it never does.
    ///
    /// `retired` counts streams and `records` the entries they carried, which is
    /// zero by construction; it is reported anyway so a caller reading the
    /// outcome cannot mistake a stream that was not empty for one that was.
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
                r#"
                local records = 0
                local retired = 0
                local from = string.len(ARGV[1]) + 1
                local to = -string.len(ARGV[2]) - 1
                for i = 1, #KEYS do
                  local key = KEYS[i]
                  local id = string.sub(key, from, to)
                  if string.len(id) > 0
                     and redis.call("EXISTS", "orchestrator:agents:" .. id) == 0
                     and redis.call("TYPE", key)["ok"] == "stream"
                     and redis.call("XLEN", key) == 0 then
                    -- Empty by the test above, so nothing is added to `records`:
                    -- the field stays in the reply so a caller reading the
                    -- outcome sees zero rather than having to know it.
                    redis.call("DEL", key)
                    retired = retired + 1
                  end
                end
                return {retired, records}
                "#,
            )
            .arg(keys.len())
            .arg(keys)
            .arg(INBOX_PREFIX)
            .arg(INBOX_SUFFIX)
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
                    // anything and must earn the second look again.
                    let mut last_seen: HashMap<String, i64> = HashMap::new();
                    loop {
                        beat.beat();
                        match backend.scan().await {
                            Ok(scan) => {
                                // The reading is recorded before anything is
                                // removed: a walk that found the store dirty and
                                // then failed to clean it must not read as clean.
                                backend.readings().record(&scan);
                                let (confirmed, current) =
                                    crate::wal::confirmed_for_retirement(scan.empty, &last_seen);
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
