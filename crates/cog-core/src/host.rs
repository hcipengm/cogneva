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

/// The host's busy share over the span between two reads of the kernel's
/// aggregate cpu accounting, as a fraction of the cores it has.
///
/// The one place this host's own cpu time is parsed, the way
/// [`mem_total_kb`] is for its memory, and for the same reason: what "busy"
/// counts is a definition, not an arithmetic detail, so a second caller cannot
/// spell it differently. `busy` is the total minus `idle` and minus IO wait, on
/// purpose: a host blocked on its disk reads as busy under load average, and
/// that wait is already the io dimension's to report, so charging it here would
/// report one stall twice.
///
/// It is a delta between two cumulative reads rather than a division of one
/// snapshot, so the span is the caller's to state and never a property of when
/// this function was called. A share taken over a sub-second window is the
/// sampler's cadence, not the machine's capacity: on a quiet host it spreads
/// over a factor of four, and on a busy one it reads a full core whenever the
/// window happened to catch a build's fan-out. The alert this share feeds is
/// written against a one-minute mean, so the caller passes a minute's worth of
/// counters.
///
/// `None` when the `cpu` line is missing or unparseable, or when the counters
/// did not advance between the two reads. Zero is a number the bound above
/// accepts, and it would mean "no core is busy" rather than "the reading did
/// not happen"; a divide by zero would be worse than either.
pub fn cpu_busy_share(before: &str, after: &str) -> Option<f64> {
    let (busy_before, total_before) = cpu_seconds(before)?;
    let (busy_after, total_after) = cpu_seconds(after)?;
    let busy = busy_after.checked_sub(busy_before)?;
    let total = total_after.checked_sub(total_before)?;
    if total == 0 {
        return None;
    }
    Some(busy as f64 / total as f64)
}

/// `(busy, total)` cpu seconds out of the text of the kernel's `cpu` line.
///
/// The first eight fields of the `cpu` line only. The line continues with
/// `guest` and `guest_nice`, which are time already counted inside `user` and
/// `nice`: adding them again would inflate the denominator and read a busy host
/// as a quieter one, on exactly the hosts where the number matters most.
fn cpu_seconds(stat: &str) -> Option<(u64, u64)> {
    let mut fields = stat
        .lines()
        .find(|line| line.split_whitespace().next() == Some("cpu"))?
        .split_whitespace();
    fields.next()?;
    let times: Vec<u64> = fields
        .map(|field| field.parse().ok())
        .collect::<Option<Vec<u64>>>()?;
    if times.len() < 4 {
        return None;
    }
    let idle = times[3];
    let iowait = times.get(4).copied().unwrap_or(0);
    let total: u64 = times.iter().take(8).sum();
    Some((total.saturating_sub(idle).saturating_sub(iowait), total))
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

    #[test]
    fn the_share_is_a_delta_of_two_reads_minus_idle_and_io_wait() {
        let before = "cpu  100 0 100 800 0 0 0 0\n";
        let after = "cpu  200 0 150 900 50 0 0 0\n";
        // 300 seconds passed, 150 of them busy: the 50 in io wait are the
        // io dimension's to report and are not charged here.
        assert_eq!(cpu_busy_share(before, after), Some(0.5));
    }

    #[test]
    fn a_missing_line_or_still_counters_is_no_reading_not_zero() {
        let before = "cpu  100 0 100 800 0 0 0 0\n";
        assert_eq!(cpu_busy_share(before, before), None);
        assert_eq!(cpu_busy_share(before, "cpu 100 0 100 800\n"), None);
        assert_eq!(cpu_busy_share(before, "intr 12345 0 0 0 0\n"), None);
        assert_eq!(cpu_busy_share(before, "cpu  lots 0 100 800\n"), None);
    }
}
