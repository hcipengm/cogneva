//! Command-line parsing for the `cogneva-bootstrap` launcher.
//!
//! The rule this module exists to enforce: **an argument the launcher does not
//! understand stops it with a usage message.** Everything the installer can be
//! told lives in the environment (see [`USAGE`]), so no argument has a meaning
//! that could justify "ignore it and install anyway". Before this module every
//! argument was dropped on the floor and the install ran with defaults, which
//! is the worst possible answer: a caller who wrote `--cn-mirror` or `--home
//! /opt/x` got a *successful* install built on settings that were never read,
//! and nothing in the output said so. The launcher mutates the host — it
//! installs a cluster, writes `/var/lib/cogneva-data` and installs host tools —
//! so "it started and finished" is not evidence that it did what was asked.

/// What the launcher was asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// No arguments: run the install.
    Run,
    /// Rewrite the host's writeback watermarks from the reading in the
    /// `COGNEVA_NODE_IO_*` variables, installing nothing.
    ///
    /// The install path writes the same drop-in, but only from inside the
    /// K3s-install branch, which returns early once a cluster is up. On a host
    /// that already has one, that condition is false forever: the derivation is
    /// in the tree and the file is never on the disk, and both look like "done".
    /// This subcommand is the same derivation and the same write, reachable
    /// without the install.
    NodeIo,
    /// Apply the rendered ResourceQuota/LimitRange to the cluster, installing
    /// nothing else.
    ///
    /// The rollout loop skips governance kinds on purpose (replaying a committed
    /// default every revision would overwrite a ceiling someone tuned), and the
    /// install face applies them only while installing. A ceiling raised in the
    /// tree therefore has no carrier between installs: the cluster keeps
    /// admitting against the old number. This command is that carrier, and it
    /// takes the number from the rendered artifacts rather than from any
    /// environment variable, so what lands is the tree's own copy.
    Governance,
    Help,
    Version,
}

/// Flags that take effect wherever they appear in the argument list.
const FLAGS: [(&str, Command); 4] = [
    ("--help", Command::Help),
    ("-h", Command::Help),
    ("--version", Command::Version),
    ("-V", Command::Version),
];

pub const USAGE: &str = "\
Cogneva 元启动引导器

用法：
    cogneva-bootstrap [选项]
    cogneva-bootstrap node-io     只用环境变量重写宿主脏页水位，不安装任何东西
    cogneva-bootstrap governance  把渲染产物里的 ResourceQuota/LimitRange apply 到集群

不带选项即开始安装。安装参数一律经环境变量传入，不认参数即拒绝执行：

`node-io` 做的是安装路径里同一件事：按设备稳态写重算 `vm.dirty_bytes` /
`vm.dirty_background_bytes` 并写 `/etc/sysctl.d/99-cogneva-node-io.conf`。安装路径里那次
写入只在「集群还没起」时执行，所以宿主一旦有集群，那份文件就再也不会由安装产生——代码在
树里、文件不在盘上，两边看起来都像已经做了。这个子命令走同一个推导、同一个写入口，不需要
装任何东西；`COGNEVA_NODE_IO_*` 五个变量在这里全部必需。

`governance` 只做一件事：从 `deploy/rendered/<profile>/` 里挑出 `ResourceQuota` 与
`LimitRange` 两份多文档清单，原样 `kubectl apply`。部署循环有意不投射这两类对象（每
rev 重放会把有人调过的天花板覆盖回去），安装面又只在装的时候 apply 一次，所以树里
抬高的额度在两次安装之间没有任何载体；这条子命令就是那个载体。profile 默认按当前
集群探测，可用 `COGNEVA_GOVERNANCE_PROFILE` 指定；治理面关掉的 profile 没有可 apply
的对象，这时它以非零退出，而不是报一个「空的成功」。

    COGNEVA_CN_MIRROR=1              强制国内镜像路径（0 强制海外），缺省自动探测
    COGNEVA_BOOTSTRAP_NONINTERACTIVE=1   全程不提问（无人值守）
    COGNEVA_CLUSTER_DISTRO           集群供给方式：k3s（默认）| kubespray
    COGNEVA_CLUSTER_NODES            多节点供给的目标节点数
    COGNEVA_NODES                    节点清单（kubespray 通道）
    COGNEVA_IMAGE_REGISTRY           运行时镜像来源 registry 覆盖
    COGNEVA_IMAGE_URL                运行时镜像包下载地址覆盖
    COGNEVA_REPO_ROOT                部署资产所在目录（缺省用二进制内嵌资产解包）
    COGNEVA_INTENT_CONFIG            intent_config.yaml 输出路径
    COGNEVA_MANAGEMENT_PLAN          management_plan.yaml 输出路径
    COGNEVA_NODE_IO_WRITE_BYTES_PER_SEC  实测的设备稳态写（字节/秒）；设了才写宿主脏页水位
    COGNEVA_NODE_IO_DEVICE           该读数读自哪张整盘（如 nvme0n1）
    COGNEVA_NODE_IO_WINDOW           读数取的窗口（如 5m）
    COGNEVA_NODE_IO_BUSY_PERCENT     同窗设备忙率（整数百分比）
    COGNEVA_NODE_IO_MEASURED_AT      测量时刻（UTC）

选项：
    -h, --help       打印本帮助并退出
    -V, --version    打印版本并退出";

impl Command {
    /// Parse `argv` without the program name.
    ///
    /// `Err` carries the first argument that is neither a flag nor nothing at
    /// all; the caller prints the usage and exits non-zero.
    pub fn parse<I, S>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args: Vec<String> = args.into_iter().map(|a| a.as_ref().to_string()).collect();

        for (flag, command) in FLAGS {
            if args.iter().any(|a| a == flag) {
                return Ok(command);
            }
        }

        match args.first().map(String::as_str) {
            None => Ok(Self::Run),
            // The subcommand takes no operands: everything it reads is in the
            // environment, the same as the install. A second argument is
            // therefore rejected by name rather than dropped, for the same
            // reason the install rejects arguments it does not understand.
            Some("node-io") if args.len() == 1 => Ok(Self::NodeIo),
            Some("node-io") => Err(args[1].clone()),
            Some("governance") if args.len() == 1 => Ok(Self::Governance),
            Some("governance") => Err(args[1].clone()),
            Some(first) => Err(first.to_string()),
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
    fn no_arguments_runs_the_install() {
        assert_eq!(parse(&[]), Ok(Command::Run));
    }

    #[test]
    fn flags_resolve_wherever_they_appear() {
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(parse(&["-h"]), Ok(Command::Help));
        assert_eq!(parse(&["--version"]), Ok(Command::Version));
        assert_eq!(parse(&["-V"]), Ok(Command::Version));
    }

    #[test]
    fn node_io_is_a_subcommand_with_no_operands() {
        // The one argument that is not a flag. It exists because the same write
        // sits behind the K3s installer, which returns early once a cluster is
        // up -- so on a live host the file could never be produced at all.
        assert_eq!(parse(&["node-io"]), Ok(Command::NodeIo));
        // Its operands are the environment variables, not arguments: a second
        // word is refused by name instead of being quietly ignored.
        assert_eq!(parse(&["node-io", "/etc/x"]), Err("/etc/x".into()));
        // Near miss, same rule as the flags below.
        assert_eq!(parse(&["node-ios"]), Err("node-ios".into()));
    }

    #[test]
    fn governance_is_a_subcommand_with_no_operands() {
        // Same shape as node-io: the operand it needs (which profile) is an
        // environment variable, so a second word is refused by name rather
        // than dropped -- a caller who wrote `governance k3s-multi` must not
        // get a successful apply of the detected profile instead.
        assert_eq!(parse(&["governance"]), Ok(Command::Governance));
        assert_eq!(parse(&["governance", "k3s-multi"]), Err("k3s-multi".into()));
        assert_eq!(parse(&["governance2"]), Err("governance2".into()));
    }

    #[test]
    fn an_unknown_argument_is_an_error_not_an_install() {
        // The shape `bootstrap.sh` hands over sits in this list: it used to
        // forward its own operands verbatim, so anything a user typed reached
        // the launcher and was discarded there.
        assert_eq!(parse(&["--cn-mirror=1"]), Err("--cn-mirror=1".into()));
        assert_eq!(parse(&["--home", "/opt/x"]), Err("--home".into()));
        assert_eq!(parse(&["install"]), Err("install".into()));
        assert_eq!(parse(&["-x"]), Err("-x".into()));
        // A near miss must not be accepted either: silently treating it as a
        // no-op flag would reintroduce the same hole one character over.
        assert_eq!(parse(&["--help-me"]), Err("--help-me".into()));
        assert_eq!(parse(&["--vers"]), Err("--vers".into()));
    }

    #[test]
    fn the_environment_surface_is_documented_in_the_usage_text() {
        // The usage text is the only place the supported settings are named;
        // a setting that disappears from it becomes undiscoverable, and the
        // caller's next guess is an argument — which this module rejects.
        for name in [
            "COGNEVA_CN_MIRROR",
            "COGNEVA_BOOTSTRAP_NONINTERACTIVE",
            "COGNEVA_CLUSTER_DISTRO",
            "COGNEVA_CLUSTER_NODES",
            "COGNEVA_NODES",
            "COGNEVA_IMAGE_REGISTRY",
            "COGNEVA_IMAGE_URL",
            "COGNEVA_REPO_ROOT",
            "COGNEVA_GOVERNANCE_PROFILE",
            "COGNEVA_NODE_IO_WRITE_BYTES_PER_SEC",
            "COGNEVA_NODE_IO_DEVICE",
            "COGNEVA_NODE_IO_WINDOW",
            "COGNEVA_NODE_IO_BUSY_PERCENT",
            "COGNEVA_NODE_IO_MEASURED_AT",
        ] {
            assert!(USAGE.contains(name), "usage text omits {name}");
        }
    }

    #[test]
    fn every_flag_is_documented_and_dash_shaped() {
        for (flag, _) in FLAGS {
            assert!(USAGE.contains(flag), "usage text omits flag {flag}");
            assert!(flag.starts_with('-'), "{flag} is not flag-shaped");
        }
    }
}
