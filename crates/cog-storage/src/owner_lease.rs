//! The shared-database lease behind [`cog_core::OwnerLease`].
//!
//! A row per role, holding who has it and when their term ends. Taking a role
//! is one statement: insert the row, or update it if the same holder is asking
//! again or the term in it has run out. Everything the arbitration needs is in
//! that statement, and that is the point — a read followed by a write would
//! leave a window between them in which two processes each see a free role and
//! each take it, and the window is wide enough to be hit by exactly the event
//! this is here to survive, two replicas starting at once.
//!
//! **The database's clock is the only clock.** Both the term's end and the
//! comparison against it are `now()` inside the server, so two pods whose own
//! clocks disagree — which is what pod clocks are, on a node that has been up
//! long enough — still agree on whose term has expired. A client-side `now()`
//! would make the term's length a property of the asked-for value rather than
//! of the answer.
//!
//! **A row, not an advisory lock.** An advisory lock is held by the connection
//! that took it, so a pooled connection going back to the pool would carry the
//! lock with it, and the role would belong to whichever task next borrowed that
//! connection. It is also invisible without a query the arbiters themselves do
//! not run, and it has no term: a process that stops without its connection
//! closing would hold the role until the connection is reaped, with nothing
//! saying when. The row has a term written on its face, so both the takeover
//! and an operator asking "who has this" read the same two columns.
//!
//! **The holder is the process.** See [`lease_holder`] for why that is the pod
//! name rather than the instance identity everything else here is named by.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{OwnerLease, OwnerLeaseBroker, SFError, SFResult};

/// The table the roles are held in. One row per role, so its size is bounded by
/// the loops that take a lease rather than by anything they do.
pub const LEASE_TABLE: &str = "cogneva_owner_leases";

/// The identity a lease is held under: the process, not the instance.
///
/// Every other name in this system that has to survive a restart is derived
/// from the instance fingerprint, and this one must deliberately not be: a
/// lease is a claim on a term, and a term that survived the process would be a
/// role held by nobody — the row would look renewed after a restart while
/// nothing had renewed it. The pod name, or the pid where there is no pod, dies
/// with the process that wrote it, which is exactly what makes the takeover
/// work. An empty `HOSTNAME` counts as absent: a deployment that sets the
/// variable to nothing would otherwise hand every replica the same identity and
/// make each of them renew the other's term.
pub fn lease_holder() -> String {
    match std::env::var("HOSTNAME") {
        Ok(name) if !name.trim().is_empty() => name.trim().to_string(),
        _ => format!("pid-{}", std::process::id()),
    }
}

/// The statement that takes or renews a role.
///
/// The `WHERE` on the conflict path is the arbitration: an update happens only
/// if the row already names this holder, or if its term has run out. Without the
/// second half every ask would take the role and the lease would be a formality;
/// without the first, a holder asking again would have to win against itself.
const TAKE_ROLE_SQL: &str = "\
INSERT INTO cogneva_owner_leases (role, holder, acquired_at, expires_at)
VALUES ($1, $2, now(), now() + ($3::bigint * interval '1 second'))
ON CONFLICT (role) DO UPDATE SET
    holder = EXCLUDED.holder,
    acquired_at = CASE
        WHEN cogneva_owner_leases.holder = EXCLUDED.holder
        THEN cogneva_owner_leases.acquired_at
        ELSE now()
    END,
    expires_at = EXCLUDED.expires_at
WHERE cogneva_owner_leases.holder = EXCLUDED.holder
   OR cogneva_owner_leases.expires_at <= now()
RETURNING holder";

/// A role's lease, as one process's side of it.
pub struct PgOwnerLease {
    pool: PgPool,
    role: String,
    holder: String,
    ttl: Duration,
}

#[async_trait]
impl OwnerLease for PgOwnerLease {
    fn role(&self) -> &str {
        &self.role
    }

    async fn try_hold(&self) -> SFResult<bool> {
        let seconds = self.ttl.as_secs().max(1) as i64;
        let row = sqlx::query(TAKE_ROLE_SQL)
            .bind(&self.role)
            .bind(&self.holder)
            .bind(seconds)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("lease on role {}: {e}", self.role)))?;
        // No row back means the conflict path's `WHERE` refused: a live term
        // belongs to another holder. A row back means this process holds it,
        // whether the insert created it or the update took it over.
        Ok(row.is_some_and(|r| r.get::<String, _>("holder") == self.holder))
    }
}

/// The leases this deployment can arbitrate, backed by the shared database.
pub struct PgOwnerLeaseBroker {
    pool: PgPool,
    holder: String,
}

impl PgOwnerLeaseBroker {
    /// Open the broker, creating the table if it is not there.
    ///
    /// Failing here is the caller's to weigh: without the broker the loops run
    /// unarbitrated, which is the right answer for a deployment that has no
    /// shared database and the wrong one for a deployment whose database is
    /// merely unreachable this second.
    pub async fn new(pool: PgPool) -> SFResult<Self> {
        Self::with_holder(pool, lease_holder()).await
    }

    /// Open the broker under an identity given here rather than read from the
    /// environment.
    ///
    /// Exists so two holders can contend inside one process. A test that had to
    /// be two processes to prove the arbitration would be a test the gates do
    /// not run, and the arbitration is precisely the thing that must not rest on
    /// a reading taken by hand.
    pub async fn with_holder(pool: PgPool, holder: impl Into<String>) -> SFResult<Self> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {LEASE_TABLE} (
                 role TEXT PRIMARY KEY,
                 holder TEXT NOT NULL,
                 acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 expires_at TIMESTAMPTZ NOT NULL
             )"
        ))
        .execute(&pool)
        .await
        .map_err(|e| SFError::Database(format!("create {LEASE_TABLE}: {e}")))?;
        Ok(Self {
            pool,
            holder: holder.into(),
        })
    }

    /// The identity this broker's leases are held under, for a log line that
    /// has to say which pod a takeover displaced.
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// The row as an operator would read it, for the log line that reports what
    /// is actually held right now rather than what this process believes.
    pub async fn holders(&self) -> SFResult<Vec<(String, String)>> {
        let rows = sqlx::query(&format!(
            "SELECT role, holder FROM {LEASE_TABLE} ORDER BY role"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("read {LEASE_TABLE}: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("role"), r.get::<String, _>("holder")))
            .collect())
    }
}

impl OwnerLeaseBroker for PgOwnerLeaseBroker {
    fn lease(&self, role: &str, ttl: Duration) -> Arc<dyn OwnerLease> {
        Arc::new(PgOwnerLease {
            pool: self.pool.clone(),
            role: role.to_string(),
            holder: self.holder.clone(),
            ttl,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The holder falls back to the pid when there is no pod name, and never to
    /// an empty string: an identity every replica shares is one they would all
    /// renew, which is a lease that holds nothing.
    #[test]
    fn the_holder_is_never_empty() {
        let holder = lease_holder();
        assert!(!holder.trim().is_empty());
    }

    /// The statement takes the role only from a free term or from itself.
    ///
    /// A text-level reading of the one edit that leaves the statement valid and
    /// the arbitration gone: dropping the `WHERE` turns every ask into a
    /// takeover, and the compiler, the types and the shape of the query all
    /// stay exactly as they were. What holds the behaviour itself is the live
    /// two-holder test in `tests/owner_lease.rs`; this is the cheap half that
    /// runs everywhere.
    #[test]
    fn the_take_statement_yields_to_a_live_holder() {
        assert!(
            TAKE_ROLE_SQL.contains("ON CONFLICT (role) DO UPDATE"),
            "the take must be one statement, not a read and a write"
        );
        assert!(
            TAKE_ROLE_SQL.contains("cogneva_owner_leases.expires_at <= now()"),
            "a live term must be able to refuse the take"
        );
        assert!(
            TAKE_ROLE_SQL.contains("cogneva_owner_leases.holder = EXCLUDED.holder"),
            "the holder must be able to renew its own term"
        );
        assert!(
            TAKE_ROLE_SQL.contains("RETURNING holder"),
            "the answer must be the row's, not the caller's belief"
        );
    }
}
