//! The listening port shared by the single-purpose pods that serve readings.
//!
//! The claim-store, host-ledger and volume-walker modes are three programs with
//! one shape: one pod per node, no data plane, `/metrics` on a port the
//! deployment may override. They read the same default and parse the override
//! the same way, so both live here once — three copies of "9100 unless the
//! environment says otherwise" is three places for a deployment to be described
//! differently from what the pods actually do.

/// Port served when the deployment names none.
///
/// One number for all three modes: each runs in its own pod, so nothing
/// collides, and a deployment that moves the port moves it in one place.
// cap: bound — 三种模式各在自己的 Pod 里，不会撞；部署要挪端口就一处挪
pub const DEFAULT_PORT: u16 = 9100;

/// Port from `env`, falling back to [`DEFAULT_PORT`]. A value that is present
/// but blank counts as unset: a blank entry is a deployment that meant nothing,
/// not a request to bind port zero.
pub fn port_from_env(env: &str) -> u16 {
    parse_port(std::env::var(env).ok().as_deref()).unwrap_or(DEFAULT_PORT)
}

/// Pure half of [`port_from_env`], so the default and the parse are testable
/// without touching a process-wide variable other tests share.
fn parse_port(raw: Option<&str>) -> Option<u16> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blank_or_unparseable_override_leaves_the_default_in_force() {
        assert_eq!(parse_port(Some("9104")), Some(9104));
        assert_eq!(parse_port(Some(" 9104 ")), Some(9104));
        assert_eq!(parse_port(None), None);
        assert_eq!(parse_port(Some("")), None);
        assert_eq!(parse_port(Some("nine thousand")), None);
    }
}
