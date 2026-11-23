//! principle: feedback_a_read_only_subcommand_is_not_the_one_that_writes_the_artifact
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
    /// 2.1–2.4 的入口：把 2.5 的假清单变成一次生成请求。
    BatchEdit,
    BatchPlan,
    /// 五格台账的读法与批次规则：`cogneva ledger status`。
    Ledger,
    VolumeWalker,
    HostLedger,
    ProcessCensus,
    ClaimStore,
    ValidateConfig,
    MainlineRollout,
    MainlineUndeliver,
    ExecuteChange,
    Backup,
    Restore,
    BackupDrill,
    RebuildMetricProjections,
    ImportChangeRecords,
    ExportChangeDiff,
    RepairAof,
    EvalTable,
    EvalCaseSubset,
    McpServe,
    WriteRole,
    Governance,
    Work,
    Principles,
    WindowsService,
}

/// Subcommands, in the order they appear in [`USAGE`]. A command added here but
/// not documented fails the usage test.
const SUBCOMMANDS: [(&str, Command); 27] = [
    ("security-gateway", Command::SecurityGateway),
    ("sandbox-executor", Command::SandboxExecutor),
    ("volume-walker", Command::VolumeWalker),
    ("host-ledger", Command::HostLedger),
    ("process-census", Command::ProcessCensus),
    ("claim-store", Command::ClaimStore),
    ("validate-config", Command::ValidateConfig),
    ("mainline-rollout", Command::MainlineRollout),
    ("mainline-undeliver", Command::MainlineUndeliver),
    ("execute-change", Command::ExecuteChange),
    ("backup", Command::Backup),
    ("restore", Command::Restore),
    ("backup-drill", Command::BackupDrill),
    (
        "rebuild-metric-projections",
        Command::RebuildMetricProjections,
    ),
    ("import-change-records", Command::ImportChangeRecords),
    ("export-change-diff", Command::ExportChangeDiff),
    ("repair-aof", Command::RepairAof),
    ("eval-table", Command::EvalTable),
    ("eval-case-subset", Command::EvalCaseSubset),
    ("mcp-serve", Command::McpServe),
    ("write-role", Command::WriteRole),
    ("governance", Command::Governance),
    ("work", Command::Work),
    ("principles", Command::Principles),
    // `batch-edit` 与 `batch-plan` 一直不在表里：枚举里有、分发里有，而 `parse` 查的是
    // 这张表——于是「这条命令不存在」，而代码看起来写完了（2026-11-23 实测：`BatchEdit`
    // 的 match 臂从加进来那天起没有一条命令能走到）。
    ("batch-edit", Command::BatchEdit),
    ("batch-plan", Command::BatchPlan),
    ("ledger", Command::Ledger),
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

/// Subcommands that only ever read: they print or serve a projection out of
/// state they hold no lease on, then exit. A subcommand whose name reads like
/// "generate a projection" — a lister that prints its kind-to-path table and
/// exits 0 — belongs here, and the principle
/// `feedback_a_read_only_subcommand_is_not_the_one_that_writes_the_artifact`
/// is the rule that it must never grow the write it prints. The set is a list
/// rather than a name pattern because the pattern is what the test scans the
/// names with: a lister whose name says what it is fails that test until it is
/// classified here, which is what decides the question at parse time, before
/// the handler runs.
///
/// Writers are everything not in this list, so the read-only set and the
/// writer set are disjoint by construction rather than by two tables kept in
/// step by hand.
const READ_ONLY_SUBCOMMANDS: &[Command] = &[
    Command::ValidateConfig,
    Command::HostLedger,
    Command::ClaimStore,
    Command::BatchEdit,
    Command::BatchPlan,
    Command::Ledger,
    Command::VolumeWalker,
    Command::McpServe,
];

pub const USAGE: &str = "\
Cogneva — AI multi-agent collaboration platform

USAGE:
    cogneva [COMMAND] [OPTIONS]

COMMANDS:
    security-gateway       Run the security gateway only
    sandbox-executor       Run the sandbox executor only
    volume-walker          Walk a mounted volume and serve its footprint on /metrics
    host-ledger            Read the host's cgroup slices and serve their memory on /metrics
    process-census         Report every container's process table on this node (hostPID, one
                           DaemonSet pod per node; reads the read-only host cgroup mount)
    claim-store            Walk every claim directory on the node's storage root and serve
                           their footprints on /metrics
    validate-config        Validate configuration and dependencies, then exit non-zero on errors
    mainline-rollout       Roll the mainline revision across deployments (in-cluster Job)
    mainline-undeliver <rev> --to <rev> --actor <name> --reason <text>
                           Withdraw a revision: hold the mainline at --to and keep the withdrawn
                           one (and anything containing it) from rolling again; --clear removes
                           the hold. Writes the deployer's pin file and exits; the loop applies
                           it on its next round
    execute-change         Apply, verify, commit and build one evolution change, then exit
    backup                 Package the data plane into a backup file
    restore <package>      Restore the data plane from a backup file
    backup-drill [--package <name> | --also-unproven]
                           Restore a package into a scratch database, record what it held,
                           then drop it; --package names one in the retention window instead
                           of the newest, --also-unproven adds the package the window has
                           gone longest without proving
    rebuild-metric-projections
                           Rewrite the three metric accumulation tables from Prometheus,
                           which is their source of truth; sets each row's `as_of` to the
                           moment of the query. Needs COGNEVA_DATABASE_URL and
                           COGNEVA_PROMETHEUS_URL
    export-change-diff <id>
                           Write one change's diff out of the queue, headed by the
                           `event_id` it was exported from; needs COGNEVA_DATABASE_URL
    import-change-records  Import the legacy <id>.json change records into the queue (the
                           event stream) and delete the files; needs COGNEVA_DATABASE_URL
                           and reads COGNEVA_EVOLUTION_CHANGE_DIR (default
                           ./evolution-changes). Unreadable records are left in place and
                           named, and the exit code is non-zero
    repair-aof <aof-dir>   Drop a torn tail from Redis' AOF files, then exit
    eval-table [OPTIONS]   Run the main comparison table (benchmarks x scaffolds x seeds)
                           against a pinned data root, then exit; --pins names the pins file
    eval-case-subset       Draw a fixed-seed subset of cases and archive it; runs read that
                           archive through `eval-table --subset` instead of redrawing
    mcp-serve              Serve this machine's tool set to an MCP client on stdin/stdout
    write-role [OPTIONS]   Ask the shared write-role lease from outside the process that
                           usually holds it; --holder names this writer, --role picks the
                           role. Exit code is the answer: 0 holds, 1 must not write,
                           2 the ask could not be made, 3 no arbiter reading, 4 no pool
                           reading
    governance <SUB>       Read the face graph, print its verdict, write its projection,
                           or import a roster; <SUB> is one of faces, drift,
                           project, import-faces, readings, drivers, fronts,
                           progress and their import-* forms
    work <SUB> --unit <u>  Act on the tasks the graph assigns to one unit; <SUB> is one
                           of queue, claim, start, heartbeat, complete, fail, release,
                           inbox
    principles <SUB>       Read the rules the platform's employees work by, and decide
                           them; <SUB> is one of list, propose, approve, reject, retire
    batch-edit             Turn the current edit verdict into one generation request
    batch-plan             Write the plan for a batch of ledger rows read from stdin
    ledger <SUB>           Read the five-cell ledger, say what the next step is, or
                           open a round on it; <SUB> is one of status, next, open

    With no COMMAND the full application starts.

OPTIONS:
    -h, --help             Print this help and exit
    -V, --version          Print version and revision, then exit
        --health-check     Probe the local HTTP health endpoint, then exit
        --service          Run as a Windows service (Windows only)";

impl Command {
    /// Whether this command only ever reads.
    ///
    /// Decided from the one classification rather than in the handler, so the
    /// answer is known at parse time — before the act — and no handler can
    /// disagree with it. A read-only command prints or serves a projection and
    /// exits; it is never the branch that writes the artifact it prints.
    pub fn reads_only(&self) -> bool {
        READ_ONLY_SUBCOMMANDS.contains(self)
    }

    /// Parse `argv` without the program name.
    ///
    /// `Err` carries the first argument that names no command and no flag; the
    /// caller prints the usage and exits non-zero. Arguments *after* a known
    /// subcommand are left alone — `restore <package>` and `mainline-rollout
    /// --tag <rev>` both read their own operands, and `execute-change
    /// --request <file>` reads the change it must run out of that file.
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
        assert_eq!(
            parse(&[
                "execute-change",
                "--request",
                "/work/request.json",
                "--outcome",
                "/work/outcome.json"
            ]),
            Ok(Command::ExecuteChange)
        );
        // `eval-table` reads its own flags: none of them is a flag this parser knows,
        // so a run's whole command line has to land on the subcommand untouched.
        assert_eq!(
            parse(&[
                "eval-table",
                "--pins",
                "/srv/pins.tsv",
                "--tools",
                "none",
                "--backbone",
                "m@1"
            ]),
            Ok(Command::EvalTable)
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

    /// principle: feedback_a_read_only_subcommand_is_not_the_one_that_writes_the_artifact
    ///
    /// A subcommand whose name reads like "generate a projection" — a lister
    /// that prints its kind-to-path table and exits 0 — may only ever print
    /// it. The gate answers from the shape of the act it rules out, and it
    /// answers before the act: the classification is read off the parse table,
    /// so a projection-shaped name nobody classified fails here, before any
    /// handler of it could run.
    #[test]
    fn a_projection_shaped_subcommand_is_classified_read_only() {
        // The prefixes a projection printer's name is spelled with. A name
        // carrying one claims to print a view of state rather than to change
        // it, so the classification has to agree.
        const PROJECTION_PREFIXES: [&str; 6] = [
            "list-",
            "print-",
            "render-",
            "show-",
            "describe-",
            "project-",
        ];
        for (name, command) in SUBCOMMANDS {
            if PROJECTION_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
            {
                assert!(
                    command.reads_only(),
                    "{name} names a projection printer but is not classified read-only"
                );
            }
        }
    }

    /// The other half of the rule: the read-only set and the writer set are
    /// one classification read two ways, so they are disjoint by construction
    /// — a command is a writer exactly when it is not listed. What can still
    /// drift is the name, so a name that promises to change state must never
    /// be classified read-only: that is the writer hiding behind a lister's
    /// answer.
    #[test]
    fn a_writer_shaped_subcommand_is_never_classified_read_only() {
        const WRITER_PREFIXES: [&str; 6] = [
            "write-",
            "execute-",
            "repair-",
            "restore",
            "backup",
            "mainline-",
        ];
        for (name, command) in SUBCOMMANDS {
            if WRITER_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
            {
                assert!(
                    !command.reads_only(),
                    "{name} names a writer but is classified read-only"
                );
            }
        }
    }

    /// One classification with one answer per command: a command listed twice
    /// would be two answers to the same question, and the writer set —
    /// everything not listed — stays disjoint from this one only while this
    /// one carries no duplicates.
    #[test]
    fn the_read_only_classification_has_one_answer_per_command() {
        for (index, command) in READ_ONLY_SUBCOMMANDS.iter().enumerate() {
            assert!(
                !READ_ONLY_SUBCOMMANDS[..index].contains(command),
                "{command:?} is classified read-only twice"
            );
        }
    }
}
