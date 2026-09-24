//! 权限结算。
//!
//! 引导器会把宿主机改成另一个状态：装 K3s、写 `/etc/rancher`、建
//! `/var/lib/cogneva-data`、apt 装包。以非 root 跑会在**深处**的第一个写操作上
//! EACCES，而那个位置的上层报错通常指向不相干的一步（实测：报的是「K3s 安装
//! 失败」，真正失败的是它前面写 registries.yaml）。所以身份要在入口就结算清楚，
//! 报错也要直说需要什么权限、怎么拿到。
//!
//! 提权只在一个地方做：shell 层的 `run_launcher` 把整个引导器升到 root，这里的
//! `require_root` 只是守门。刻意**不**提供"逐条命令加 sudo"的执行器——那条路上
//! 只要漏掉一个动作，失败就会落到它下游不相干的位置，正是本模块要消除的症状。

use anyhow::{bail, Result};
use std::process::Command;

/// 进程的实际 uid。`id` 取不到时返回 None —— 判据只在"确定非 root"时才拦人，
/// 缺个 coreutil 不该把安装挡下来。
pub fn effective_uid() -> Option<u32> {
    let out = Command::new("id").arg("-u").output().ok()?;
    if !out.status.success() {
        return None;
    }
    parse_uid(&String::from_utf8_lossy(&out.stdout))
}

/// `id -u` 的读数解析。取不到数字就是 None：把解析失败当成 0（root）会让守卫
/// 静默放行，当成非 0 会在 `id` 缺失时误拦——两者都不是"确定"。
fn parse_uid(text: &str) -> Option<u32> {
    text.trim().parse().ok()
}

pub fn is_root() -> bool {
    effective_uid() == Some(0)
}

/// 特权阶段的入口守卫：非 root 时立刻失败，并给出可直接照抄的两条命令。
pub fn require_root(step: &str) -> Result<()> {
    if is_root() {
        return Ok(());
    }
    match effective_uid() {
        Some(uid) => bail!(
            "{step}需要 root 权限，当前以非 root 运行（uid {uid}）。\n  \
             管道方式: curl -fsSL <入口地址>/bootstrap.sh | sudo sh\n  \
             脚本方式: sudo -E ./bootstrap.sh"
        ),
        None => bail!(
            "{step}需要 root 权限，当前不是 root（取 uid 失败）。\n  \
             请以 root 运行，或用 `| sudo sh` 走管道方式"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uid_parses_or_is_absent_but_never_lies_about_root() {
        // 判据的自一致性：is_root 只可能与 effective_uid 的读数一致
        assert_eq!(is_root(), effective_uid() == Some(0));
        // 读数的解析：`id -u` 的换行要被吃掉；取不到数字必须是 None（既不能当 0
        // 静默放行，也不能当非 0 误拦）
        assert_eq!(parse_uid("0\n"), Some(0));
        assert_eq!(parse_uid("1000\n"), Some(1000));
        assert_eq!(parse_uid(""), None);
        assert_eq!(parse_uid("uid=1000"), None);
    }
}
