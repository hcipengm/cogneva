//! A declared ceiling on how many tokens a day may be spent, enforced before
//! the call leaves the gateway.
//!
//! Three readings already exist around spend and none of them is a ceiling:
//! the ledger says what was spent, the rollup says it per hour, and window
//! pacing says how much of an upstream's own quota is left. All three answer
//! "what did the world allow". A budget answers the other question — "what did
//! we allow ourselves" — and it is the only bound that stays true when the
//! upstream is generous, when the window is long, or when the platform is
//! having a good day and could spend a month's money in an afternoon.
//!
//! The number comes from configuration and the spend comes from the ledger, so
//! the two are never the same kind of fact: the ceiling is declared, the spend
//! is measured. Nothing here estimates what a request *will* cost beyond the
//! input side it can see, which is why a call may cross the line by the size of
//! its own output — the next call is the first one refused.
//!
//! Default off, and off means off: with no cap declared the module answers
//! `None` for every call and the channel behaves exactly as it did before. A
//! ceiling that switches itself on would refuse calls in a deployment whose
//! operator never named a number.
//!
//! Blind is not zero. A ledger that cannot be read leaves [`Reading::snapshot`]
//! at `None`, which allows the call — refusing on a database hiccup turns the
//! meter into an outage, and the gateway's job is to proxy. The reading surfaces
//! that state as its own series so a blind meter is visible rather than read as
//! "nothing spent".

use std::collections::BTreeMap;

/// The pool-wide cap, in tokens per UTC day. Absent or empty declares none.
pub const POOL_BUDGET_ENV: &str = "COGNEVA_LLM_DAILY_TOKEN_BUDGET";

/// Per-actor caps, `actor=cap` separated by commas. Absent or empty declares
/// none.
pub const ACTOR_BUDGET_ENV: &str = "COGNEVA_LLM_DAILY_TOKEN_BUDGET_ACTORS";

const SECONDS_PER_DAY: i64 = 86_400;

/// Start of the UTC day containing `now_unix`.
///
/// The day is fixed to UTC rather than to a local zone because the number is
/// compared against a ledger whose timestamps are UTC, and a boundary derived
/// from the host's zone would move the reset hour when the pod moved.
pub fn day_start_unix(now_unix: i64) -> i64 {
    now_unix.div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY
}

/// Seconds from `now_unix` until the next UTC midnight, at least one.
///
/// At least one because a `retry-after` of zero reads as "retry now", and the
/// call that reads it would be the one refused again.
pub fn seconds_to_next_day(now_unix: i64) -> u64 {
    (day_start_unix(now_unix) + SECONDS_PER_DAY - now_unix).max(1) as u64
}

/// The caps a deployment declared. Both empty means no ceiling at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpendBudget {
    pub pool_tokens: Option<u64>,
    pub actor_tokens: BTreeMap<String, u64>,
}

impl SpendBudget {
    /// Whether anything at all is declared, i.e. whether a read of the ledger is
    /// worth taking.
    pub fn is_declared(&self) -> bool {
        self.pool_tokens.is_some() || !self.actor_tokens.is_empty()
    }

    /// The configured caps, parsed from the two environment values.
    ///
    /// Every refusal here is a configuration error that would otherwise be
    /// silent in the direction that matters: a malformed cap that is dropped
    /// removes a ceiling while the deployment still claims to have one, and a
    /// cap on an actor name no call can carry never matches anything. So the
    /// parse names each bad entry rather than returning an empty budget, and
    /// the caller decides whether to drop the entry or refuse to run.
    pub fn parse(pool: Option<&str>, actors: Option<&str>) -> Result<Self, String> {
        let mut budget = Self::default();
        if let Some(raw) = pool.map(str::trim).filter(|s| !s.is_empty()) {
            budget.pool_tokens = Some(parse_cap(POOL_BUDGET_ENV, raw)?);
        }
        for entry in actors.unwrap_or_default().split(',').map(str::trim) {
            if entry.is_empty() {
                continue;
            }
            let (actor, raw_cap) = entry
                .split_once('=')
                .ok_or_else(|| format!("{ACTOR_BUDGET_ENV}: `{entry}` 不是 actor=cap 的形式"))?;
            let actor = actor.trim();
            let raw_cap = raw_cap.trim();
            if !crate::actor_labels::declared(actor) {
                return Err(format!(
                    "{ACTOR_BUDGET_ENV}: `{actor}` 不是一个调用方可以声明的身份——网关会把它改写成 \
                     unknown，这条上限永远不会匹配任何一次调用"
                ));
            }
            let cap = parse_cap(ACTOR_BUDGET_ENV, raw_cap)?;
            if budget.actor_tokens.insert(actor.to_string(), cap).is_some() {
                return Err(format!(
                    "{ACTOR_BUDGET_ENV}: `{actor}` 声明了两次——两条之中哪一条生效，读者从配置里看不出来"
                ));
            }
        }
        Ok(budget)
    }

    /// The configured caps as the deployment states them, dropping entries that
    /// do not parse and naming each one.
    ///
    /// Dropping is the lesser evil at startup: the gateway is the only path to
    /// an upstream, so refusing to start over a typo in an optional ceiling
    /// takes the platform down to protect a number nobody had before. The
    /// warning is what keeps the drop from being silent.
    pub fn from_env() -> Self {
        let pool = std::env::var(POOL_BUDGET_ENV).ok();
        let actors = std::env::var(ACTOR_BUDGET_ENV).ok();
        match Self::parse(pool.as_deref(), actors.as_deref()) {
            Ok(budget) => budget,
            Err(e) => {
                // One bad entry must not take the others with it, so the entries
                // are retried one at a time and only the offender is dropped.
                tracing::warn!(error = %e, "LLM 日预算：配置解析失败，逐条重试");
                let mut kept = Self::default();
                if let Some(raw) = pool.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                    match Self::parse(Some(raw), None) {
                        Ok(budget) => kept.pool_tokens = budget.pool_tokens,
                        Err(e) => tracing::warn!(error = %e, "LLM 日预算：池级上限被丢弃"),
                    }
                }
                for entry in actors.unwrap_or_default().split(',').map(str::trim) {
                    if entry.is_empty() {
                        continue;
                    }
                    match Self::parse(None, Some(entry)) {
                        Ok(budget) => kept.actor_tokens.extend(budget.actor_tokens),
                        Err(e) => {
                            tracing::warn!(error = %e, entry, "LLM 日预算：这一条上限被丢弃")
                        }
                    }
                }
                kept
            }
        }
    }

    /// Who to refuse, with the numbers the decision was made from. `None` means
    /// let the call through.
    ///
    /// `snapshot` is `None` when today's spend could not be read, and then the
    /// call is allowed: a ceiling needs a reading, and a missing reading is not
    /// a reading of zero. A snapshot taken on an earlier day says nothing about
    /// today — the spend it counts has been reset — so it is not used either.
    ///
    /// The actor cap is judged before the pool cap because it names the smaller
    /// knob: a caller that hit its own ceiling is fixed by raising that one, and
    /// reporting the pool figure would send the reader to the wrong line of
    /// configuration.
    pub fn judge(
        &self,
        actor: &str,
        estimated_input: u64,
        snapshot: Option<&SpendSnapshot>,
        now_unix: i64,
    ) -> Option<BudgetRefusal> {
        if !self.is_declared() {
            return None;
        }
        let snapshot = snapshot.filter(|s| s.day_start_unix == day_start_unix(now_unix))?;
        let retry_after_secs = seconds_to_next_day(now_unix);
        if let Some(cap) = self.actor_tokens.get(actor).copied() {
            let spent = snapshot.actor_tokens.get(actor).copied().unwrap_or(0);
            if spent.saturating_add(estimated_input) > cap {
                return Some(BudgetRefusal {
                    scope: BudgetScope::Actor,
                    cap_tokens: cap,
                    spent_tokens: spent,
                    estimated_input,
                    retry_after_secs,
                });
            }
        }
        if let Some(cap) = self.pool_tokens {
            if snapshot.total_tokens.saturating_add(estimated_input) > cap {
                return Some(BudgetRefusal {
                    scope: BudgetScope::Pool,
                    cap_tokens: cap,
                    spent_tokens: snapshot.total_tokens,
                    estimated_input,
                    retry_after_secs,
                });
            }
        }
        None
    }
}

fn parse_cap(key: &str, raw: &str) -> Result<u64, String> {
    let value: u64 = raw
        .parse()
        .map_err(|_| format!("{key}: `{raw}` 不是一个以 token 计的正整数"))?;
    if value == 0 {
        // Accepting zero would make the ceiling a kill switch spelled like a
        // number, and a template that rendered an empty value as 0 would take
        // every LLM call down while looking configured.
        return Err(format!(
            "{key}: 0 不是一个上限——它会让每一次调用都被拒；要设一个能用的下限请写 1"
        ));
    }
    Ok(value)
}

/// Today's spend as the ledger last reported it.
///
/// `day_start_unix` travels with the figures because they are only true of the
/// day they were summed over: a snapshot taken before midnight and read after it
/// would otherwise be compared against a fresh day's ceiling while still
/// counting the previous day's tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendSnapshot {
    pub day_start_unix: i64,
    pub total_tokens: u64,
    pub actor_tokens: BTreeMap<String, u64>,
}

/// What the last tick could say about today's spend.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reading {
    /// The last successful sum. `None` until a read succeeds on the current day;
    /// a successful sum from an earlier day is dropped rather than carried,
    /// because carrying it would refuse calls against a ceiling that has already
    /// reset.
    pub snapshot: Option<SpendSnapshot>,
    /// When the last successful sum was taken, unix seconds. `None` until one
    /// is. A boolean "measured" would say the same thing on a healthy process
    /// and would stop saying it once the gateway died, which is the state a
    /// reader most needs to see.
    pub measured_at_unix: Option<i64>,
}

impl Reading {
    /// Drop a snapshot that belongs to a day other than `now_unix`'s.
    ///
    /// Called by the refresh on both arms, so that a failed read at 00:00:30
    /// cannot leave yesterday's spend standing as today's.
    pub fn carry_forward(self, now_unix: i64) -> Self {
        let today = day_start_unix(now_unix);
        match self.snapshot.as_ref() {
            Some(snapshot) if snapshot.day_start_unix == today => self,
            _ => Self::default(),
        }
    }

    /// The snapshot a decision may use: today's, or none at all.
    pub fn usable(&self, now_unix: i64) -> Option<&SpendSnapshot> {
        self.snapshot
            .as_ref()
            .filter(|s| s.day_start_unix == day_start_unix(now_unix))
    }
}

/// Which cap refused the call. Two values, not one, because the repair differs:
/// the pool figure is raised by whoever owns the deployment's spend, an actor
/// figure by whoever owns that component's allowance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetScope {
    Pool,
    Actor,
}

impl BudgetScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pool => "pool",
            Self::Actor => "actor",
        }
    }
}

/// A refusal, with the numbers the decision was made from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetRefusal {
    pub scope: BudgetScope,
    pub cap_tokens: u64,
    pub spent_tokens: u64,
    /// The part of the request the gateway could see before sending it.
    pub estimated_input: u64,
    pub retry_after_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOON: i64 = 1_600_000_000 - (1_600_000_000 % SECONDS_PER_DAY) + 12 * 3600;

    fn snapshot(total: u64, actors: &[(&str, u64)]) -> SpendSnapshot {
        SpendSnapshot {
            day_start_unix: day_start_unix(NOON),
            total_tokens: total,
            actor_tokens: actors.iter().map(|(a, t)| (a.to_string(), *t)).collect(),
        }
    }

    #[test]
    fn the_day_boundary_is_utc_midnight() {
        assert_eq!(day_start_unix(0), 0);
        assert_eq!(day_start_unix(SECONDS_PER_DAY - 1), 0);
        assert_eq!(day_start_unix(SECONDS_PER_DAY), SECONDS_PER_DAY);
        // Before the epoch the floor has to go down, not toward zero.
        assert_eq!(day_start_unix(-1), -SECONDS_PER_DAY);
    }

    #[test]
    fn the_wait_until_reset_is_the_rest_of_the_day() {
        assert_eq!(
            seconds_to_next_day(day_start_unix(NOON)),
            SECONDS_PER_DAY as u64
        );
        assert_eq!(
            seconds_to_next_day(day_start_unix(NOON) + SECONDS_PER_DAY - 1),
            1
        );
    }

    #[test]
    fn no_declared_cap_allows_everything() {
        let budget = SpendBudget::default();
        assert!(!budget.is_declared());
        assert_eq!(
            budget.judge("ralph", u64::MAX, Some(&snapshot(1, &[])), NOON),
            None
        );
    }

    #[test]
    fn an_empty_pool_value_declares_nothing() {
        let budget = SpendBudget::parse(Some("  "), Some("")).unwrap();
        assert!(!budget.is_declared());
    }

    #[test]
    fn a_pool_cap_refuses_the_call_that_would_cross_it() {
        let budget = SpendBudget::parse(Some("1000"), None).unwrap();
        let today = snapshot(900, &[("ralph", 900)]);
        assert_eq!(budget.judge("ralph", 100, Some(&today), NOON), None);
        let refusal = budget.judge("ralph", 101, Some(&today), NOON).unwrap();
        assert_eq!(refusal.scope, BudgetScope::Pool);
        assert_eq!(refusal.spent_tokens, 900);
        assert_eq!(refusal.estimated_input, 101);
        assert_eq!(refusal.cap_tokens, 1000);
        assert_eq!(refusal.retry_after_secs, seconds_to_next_day(NOON));
    }

    #[test]
    fn an_actor_cap_holds_that_actor_and_names_itself() {
        let budget = SpendBudget::parse(None, Some("ralph=100,evolution=100000")).unwrap();
        let today = snapshot(150, &[("ralph", 150), ("evolution", 0)]);
        let refusal = budget.judge("ralph", 1, Some(&today), NOON).unwrap();
        assert_eq!(refusal.scope, BudgetScope::Actor);
        assert_eq!(refusal.cap_tokens, 100);
        assert_eq!(refusal.spent_tokens, 150);
        // The same ledger lets the other actor through: the caps are separate.
        assert_eq!(budget.judge("evolution", 1, Some(&today), NOON), None);
    }

    #[test]
    fn an_actor_with_no_row_has_spent_nothing() {
        let budget = SpendBudget::parse(None, Some("ralph=100")).unwrap();
        let today = snapshot(500, &[("evolution", 500)]);
        assert_eq!(budget.judge("ralph", 100, Some(&today), NOON), None);
        assert!(budget.judge("ralph", 101, Some(&today), NOON).is_some());
    }

    #[test]
    fn a_missing_reading_is_not_a_reading_of_zero() {
        let budget = SpendBudget::parse(Some("1"), None).unwrap();
        // Blind: the ceiling cannot be judged, so the call goes through.
        assert_eq!(budget.judge("ralph", u64::MAX, None, NOON), None);
    }

    #[test]
    fn yesterdays_spend_does_not_count_against_todays_ceiling() {
        let budget = SpendBudget::parse(Some("1000"), None).unwrap();
        let yesterday = SpendSnapshot {
            day_start_unix: day_start_unix(NOON) - SECONDS_PER_DAY,
            total_tokens: 10_000,
            actor_tokens: BTreeMap::new(),
        };
        assert_eq!(budget.judge("ralph", 500, Some(&yesterday), NOON), None);
        // The reading itself refuses to hand out a stale snapshot either, so a
        // caller that skips the judge's own filter still cannot use it.
        let reading = Reading {
            snapshot: Some(yesterday),
            measured_at_unix: Some(NOON),
        };
        assert!(reading.usable(NOON).is_none());
    }

    #[test]
    fn a_failed_read_at_midnight_drops_yesterdays_snapshot() {
        let yesterday = Reading {
            snapshot: Some(snapshot(10_000, &[])),
            measured_at_unix: Some(NOON),
        };
        let just_after_midnight = day_start_unix(NOON) + SECONDS_PER_DAY + 30;
        let carried = yesterday.carry_forward(just_after_midnight);
        assert!(carried.snapshot.is_none());
        assert!(carried.measured_at_unix.is_none());
    }

    #[test]
    fn a_failed_read_inside_the_day_keeps_the_last_good_sum() {
        let earlier = Reading {
            snapshot: Some(snapshot(10_000, &[])),
            measured_at_unix: Some(NOON),
        };
        let carried = earlier.clone().carry_forward(NOON + 60);
        assert_eq!(carried, earlier);
    }

    #[test]
    fn a_cap_of_zero_is_refused_rather_than_read_as_a_kill_switch() {
        assert!(SpendBudget::parse(Some("0"), None).is_err());
        assert!(SpendBudget::parse(None, Some("ralph=0")).is_err());
    }

    #[test]
    fn an_unparseable_cap_names_the_variable() {
        let e = SpendBudget::parse(Some("lots"), None).unwrap_err();
        assert!(e.contains(POOL_BUDGET_ENV), "{e}");
        let e = SpendBudget::parse(None, Some("ralph=lots")).unwrap_err();
        assert!(e.contains(ACTOR_BUDGET_ENV), "{e}");
        let e = SpendBudget::parse(None, Some("ralph")).unwrap_err();
        assert!(e.contains("ralph"), "{e}");
    }

    #[test]
    fn a_cap_on_an_actor_no_call_can_carry_is_refused() {
        // `normalize_actor` rewrites anything undeclared to `unknown`, so this
        // row would never match a call while reading as protection.
        let e = SpendBudget::parse(None, Some("Ralph=100")).unwrap_err();
        assert!(e.contains("Ralph"), "{e}");
        assert!(SpendBudget::parse(
            None,
            Some(&format!("{} = 5", crate::actor_labels::UNKNOWN_ACTOR))
        )
        .is_ok());
    }

    #[test]
    fn a_duplicated_actor_is_refused() {
        let e = SpendBudget::parse(None, Some("ralph=10,ralph=20")).unwrap_err();
        assert!(e.contains("ralph"), "{e}");
    }
}
