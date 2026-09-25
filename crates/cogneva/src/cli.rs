//! Command-line dispatch for the `cogneva` binary.
//!
//! Parsing is a pure function so that the rule below is testable: **an argument
//! we do not recognize stops the process with a usage message.** An earlier
//! version dispatched only known subcommands and let everything else fall
//! through to a full application boot, so `--help` answered with a plugin
//! initialization error and `--health-check` — which the image runs inside a
//! container already serving on the target port — answered with `bind failed`.
//! Both were the same hole: a missing branch was indistinguishable from
//! "no arguments". Handling them one argument at a time would have left the
//! next unknown argument on the same path.

/// What the binary was asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// No arguments: run the application.
    Run,
    Help,
    Version,
    HealthCheck,
    SecurityGateway,
    SandboxExecutor,
    ValidateConfig,
    MainlineRollout,
    Backup,
    Restore,
    RepairAof,
    WindowsService,
}

/// Subcommands, in the order they appear in [`USAGE`]. A command added here but
/// not documented fails the usage test.
const SUBCOMMANDS: [(&str, Command); 7] = [
    ("security-gateway", Command::SecurityGateway),
    ("sandbox-executor", Command::SandboxExecutor),
    ("validate-config", Command::ValidateConfig),
    ("mainline-rollout", Command::MainlineRollout),
    ("backup", Command::Backup),
    ("restore", Command::Restore),
    ("repair-aof", Command::RepairAof),
];

/// Flags that take effect wherever they appear in the argument list.
const FLAGS: [(&str, Command); 6] = [
    ("--version", Command::Version),
    ("-V", Command::Version),
    ("--help", Command::Help),
    ("-h", Command::Help),
    ("--health-check", Command::HealthCheck),
    ("--service", Command::WindowsService),
];

pub const USAGE: &str = "\
Cogneva — AI multi-agent collaboration platform

USAGE:
    cogneva [COMMAND] [OPTIONS]

COMMANDS:
    security-gateway       Run the security gateway only
    sandbox-executor       Run the sandbox executor only
    validate-config        Validate configuration and dependencies, then exit non-zero on errors
    mainline-rollout       Roll the mainline revision across deployments (in-cluster Job)
    backup                 Package the data plane into a backup file
    restore <package>      Restore the data plane from a backup file
    repair-aof <aof-dir>   Drop a torn tail from Redis' AOF files, then exit

    With no COMMAND the full application starts.

OPTIONS:
    -h, --help             Print this help and exit
    -V, --version          Print version and revision, then exit
        --health-check     Probe the local HTTP health endpoint, then exit
        --service          Run as a Windows service (Windows only)";

impl Command {
    /// Parse `argv` without the program name.
    ///
    /// `Err` carries the first argument that names no command and no flag; the
    /// caller prints the usage and exits non-zero. Arguments *after* a known
    /// subcommand are left alone — `restore <package>` and `mainline-rollout
    /// --tag <rev>` both read their own operands from the process environment.
    pub fn parse<I, S>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args: Vec<String> = args.into_iter().map(|a| a.as_ref().to_string()).collect();

        // Flags win over a positional argument, and keep the historical order:
        // `--version` used to be checked before every other branch.
        for (flag, command) in FLAGS {
            if args.iter().any(|a| a == flag) {
                return Ok(command);
            }
        }

        match args.first().map(String::as_str) {
            None => Ok(Self::Run),
            Some(first) => SUBCOMMANDS
                .iter()
                .find(|(name, _)| *name == first)
                .map(|(_, command)| *command)
                .ok_or_else(|| first.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, String> {
        Command::parse(args.iter().copied())
    }

    #[test]
    fn no_arguments_runs_the_application() {
        assert_eq!(parse(&[]), Ok(Command::Run));
    }

    #[test]
    fn every_subcommand_resolves() {
        for (name, command) in SUBCOMMANDS {
            assert_eq!(parse(&[name]), Ok(command), "subcommand {name}");
        }
    }

    #[test]
    fn operands_after_a_subcommand_are_left_to_it() {
        // These forms all exist in the deployment: `restore <package>` in the
        // restore Job, `mainline-rollout --tag <rev> ...` in the rollout Job, and
        // `repair-aof <dir>` in the redis pod's init container.
        assert_eq!(
            parse(&["restore", "/backups/x.tar.zst"]),
            Ok(Command::Restore)
        );
        assert_eq!(
            parse(&["repair-aof", "/data/appendonlydir"]),
            Ok(Command::RepairAof)
        );
        assert_eq!(
            parse(&[
                "mainline-rollout",
                "--tag",
                "main-abc",
                "--namespace",
                "cogneva"
            ]),
            Ok(Command::MainlineRollout)
        );
    }

    #[test]
    fn flags_resolve_wherever_they_appear() {
        assert_eq!(parse(&["--version"]), Ok(Command::Version));
        assert_eq!(parse(&["-V"]), Ok(Command::Version));
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(parse(&["-h"]), Ok(Command::Help));
        assert_eq!(parse(&["--health-check"]), Ok(Command::HealthCheck));
        assert_eq!(parse(&["--service"]), Ok(Command::WindowsService));
        assert_eq!(parse(&["backup", "--version"]), Ok(Command::Version));
    }

    #[test]
    fn an_unknown_argument_is_an_error_not_an_application_boot() {
        assert_eq!(parse(&["--help-me"]), Err("--help-me".into()));
        assert_eq!(parse(&["--healthcheck"]), Err("--healthcheck".into()));
        assert_eq!(parse(&["--tag", "main-abc"]), Err("--tag".into()));
        assert_eq!(parse(&["validat-config"]), Err("validat-config".into()));
    }

    #[test]
    fn every_command_and_flag_is_documented_in_the_usage_text() {
        for (name, _) in SUBCOMMANDS {
            assert!(USAGE.contains(name), "usage text omits subcommand {name}");
        }
        for (flag, _) in FLAGS {
            assert!(USAGE.contains(flag), "usage text omits flag {flag}");
        }
    }

    #[test]
    fn flags_start_with_a_dash_so_they_cannot_shadow_a_subcommand() {
        // A flag without the leading dash would swallow the positional slot and
        // make `cogneva <name>` resolve to it instead of the subcommand.
        for (flag, _) in FLAGS {
            assert!(flag.starts_with('-'), "{flag} is not flag-shaped");
        }
    }
}
