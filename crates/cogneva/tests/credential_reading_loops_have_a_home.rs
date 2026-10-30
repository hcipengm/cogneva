//! Gate: a loop that needs a Secret is started by the process that may read it.
//!
//! Moving the credential readers into the security gateway is only half a move.
//! The loops that *use* those readers — the Gitee OAuth refresher and the git
//! identity bootstrap — have to travel with them, because both read a key out of
//! the Secret and write the rotated result back. Deleting them from the plugin
//! and not starting them on the gateway side leaves no trace at all: the build
//! is clean, every deployment rolls, and the only symptom is that the token
//! stops being renewed — which reads exactly like "there was nothing to renew"
//! until the egress channel starts answering 401 a day later.
//!
//! The reverse mistake is just as quiet and worse: leaving a copy in the plugin
//! makes the main application spawn a task whose every attempt is a 403, and
//! both copies race over a single-use refresh token, so the one that renews may
//! be the one that loses.
//!
//! Neither is catchable by a behaviour test here — starting the gateway binds
//! five ports and needs a cluster — so the claim is asserted over the sources:
//! each of these loops has a call site in the process that holds the Secret, and
//! none in the process that does not. A test that only says "the function
//! exists" would have passed throughout the window in which it was never called.

use crate::common;

use common::routes::repo_root;

use common::rust_production::production_text;

/// The process that holds `cogneva-secrets` / `cogneva-secrets-oob`.
const HOLDER: &str = "crates/cog-gateway/src/security_gateway.rs";
/// The process that holds no credential and must not start these loops.
const PLUGIN: &str = "crates/cog-gateway/src/plugin.rs";

/// `(spawner, why it needs the Secret)`, one entry per loop that reads or
/// rewrites a key. Adding a loop of this kind means adding a line here; the
/// entry is checked in both directions, so a stale one fails rather than
/// quietly widening what this gate covers.
const LOOPS: &[(&str, &str)] = &[
    (
        "spawn_gitee_token_refresher",
        "reads gitee-refresh-token and writes the rotated pair back; the token is single-use, so two copies invalidate each other",
    ),
    (
        "spawn_git_identity_bootstrap",
        "reads the mounted deploy key and writes the newly registered public key into the out-of-band slice",
    ),
];

fn production(relative: &str) -> String {
    let path = repo_root().join(relative);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    production_text(&text)
}

/// A call, not a mention: the name followed by an opening parenthesis. The
/// definition has the same shape, so the hub file is excluded by the caller
/// rather than by a cleverer pattern — `spawn_gitee_token_refresher` is
/// *defined* in `contribution_admin.rs`, and that is where a reader should look
/// for what it does.
fn calls(text: &str, spawner: &str) -> bool {
    text.contains(&format!("{spawner}("))
}

#[test]
fn the_gateway_starts_the_loops_that_need_the_secret() {
    let holder = production(HOLDER);
    for (spawner, why) in LOOPS {
        assert!(
            calls(&holder, spawner),
            "{spawner} must be started from {HOLDER}: it {why}, and that process is the only one allowed to read the Secret"
        );
    }
}

#[test]
fn the_plugin_does_not_start_a_loop_it_cannot_feed() {
    let plugin = production(PLUGIN);
    for (spawner, why) in LOOPS {
        assert!(
            !calls(&plugin, spawner),
            "{PLUGIN} must not start {spawner}: it {why}, and this process has no get on the Secret — every attempt 403s, and it races the holder's copy over the same single-use token"
        );
    }
}

/// The spawners are only meaningful while the module that owns the Secret
/// access is the one the gateway calls into. A rename that moves them into a
/// crate the gateway cannot reach would make both assertions above pass on a
/// stale name, so the definitions are pinned too.
#[test]
fn both_loops_are_still_defined_where_the_gateway_reaches_them() {
    let hub = production("crates/cog-gateway/src/contribution_admin.rs");
    let git = production("crates/cog-gateway/src/git_identity.rs");
    for (spawner, definitions) in [
        ("spawn_gitee_token_refresher", hub.as_str()),
        ("spawn_git_identity_bootstrap", git.as_str()),
    ] {
        assert!(
            definitions.contains(&format!("pub fn {spawner}(")),
            "the holder starts {spawner} by name, so it must stay a public function here"
        );
    }
}
