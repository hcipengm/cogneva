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

不带选项即开始安装。安装参数一律经环境变量传入，不认参数即拒绝执行：

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

        match args.first() {
            None => Ok(Self::Run),
            Some(first) => Err(first.clone()),
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
