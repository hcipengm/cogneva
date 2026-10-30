//! 这台机器自己的内存总数，从 `/proc/meminfo` 读。
//!
//! 同一个数有三个消费者，而它们要的是同一件事：**这台节点有多少内存**。安装链
//! 按它算节点的 QoS 预留与驱逐水位（kubespray 清单里的 SSH 探针、K3s agent 首次
//! 启动前的同一个探针），评测闸门按它折算内存地板与绿灯线，构建闸门按它把上限拟合
//! 到这台机器上。三处各写一遍解析，拼法一改就换成一个**另一个数**：把 `MemTotal`
//! 换成 `MemAvailable`、把 `/1024` 换成 `/1024/1024`、或让探针多印一行，另一条路径
//! 就按另一个容量去算。它不会失败、不会少一个读数——只是那些份额不是按这台节点算的。
//!
//! 所以字段名与它的解析都在这里，只此一处；安装器那个 SSH 探针串也由
//! [`MEMINFO_TOTAL_FIELD`] 拼出来，跨 SSH 边界的那份「另一个程序」读的因此也
//! 是同一个字段名。
//!
//! `gate`: `crates/cogneva/tests/a_node_capacity_probe_is_written_once_test.rs` 普查
//! 生产行里出现 `MemTotal:` 的文件，必须恰好是本文件。

/// `/proc/meminfo` 里那一行的字段名，带冒号。
///
/// 带冒号是因为 `/proc/meminfo` 打印的就是 `MemTotal:`，而不带冒号的前缀
/// （`MemTotal`）还会匹配到 `MemTotalFoo` 这类本机并不存在、但别的内核版本可能
/// 多印出来的行。
pub const MEMINFO_TOTAL_FIELD: &str = "MemTotal:";

/// Total memory in KiB, read out of the text of `/proc/meminfo`.
///
/// The one place this host's own memory total is parsed from the kernel.
///
/// `None` when the line is missing or its number does not parse. A caller that
/// cannot read the total has to say so: zero is a number the arithmetic above
/// accepts, and it would mean "this machine has no memory" rather than "the
/// reading did not happen".
pub fn mem_total_kb(meminfo: &str) -> Option<u64> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix(MEMINFO_TOTAL_FIELD))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_total_is_read_out_of_its_own_line() {
        let meminfo = "MemTotal:       65496336 kB\nMemFree:         1234567 kB\n";
        assert_eq!(mem_total_kb(meminfo), Some(65_496_336));
    }

    #[test]
    fn a_missing_or_unreadable_total_is_no_reading_not_zero() {
        assert_eq!(mem_total_kb("MemTotal: 1048576\n"), Some(1_048_576));
        assert_eq!(mem_total_kb("MemTotal:\n"), None);
        assert_eq!(mem_total_kb("MemTotal: lots kB\n"), None);
        assert_eq!(mem_total_kb("MemTotalFoo: 999 kB\n"), None);
    }
}
