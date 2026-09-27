//! 判定容器**自己这一次跑**用掉了多少资源。
//!
//! 滚动判定跑在一个短命的 Job Pod 里：它出生、跑几分钟、消失。cadvisor 与
//! kube-state-metrics 都是按"对象还存在"采样再聚合的，这类容器的读数落在两条通用资源
//! 判据的时间窗之外——Pod 被 TTL 收走时，最忙的那几分钟的样本也一起没了。所以"声明了
//! 多少"与"这次用掉多少"只能在**同一个进程里**对照：Job 清单里的额度是部署器写进去的，
//! 用掉的量只有这个进程自己能读，而它还活着的时候正好读得到。
//!
//! 读的是容器自己的 cgroup v2 文件（容器里 `/sys/fs/cgroup` 就是自己的那一层）。全部
//! 读不到时返回 `None`：那说明这个节点上没有一个能自读的 cgroup，是"没得读"，不是"读到了
//! 零"——两者的区别正是本模块存在的理由。
//!
//! 读数写在容器的终止消息里（见 `mainline_deployer::report_termination_message`）：这个
//! Job 上没有任何回调面可用——没有 DB、没有 metrics 后端，RBAC 也只给了 pods 的
//! get/list/watch，没有 exec/log 通道。终止消息是 k8s 给"这个容器为什么结束"留的窄通道，
//! 部署器本来就按结构化字段读它，第二行正好装得下。

use std::fmt;
use std::path::Path;

/// 行首标记。读数与失败落点共用一条终止消息，读回时按这个标记认自己的那一行，不猜。
pub const TAG: &str = "rollout-resources";

/// 线上格式的版本。形状变了就换一个值：读回端认不出的版本宁可当"没有读数"，也不按旧
/// 字段位置硬解——解错的值比缺的值更坏，它看起来像个读数。
const FORMAT_VERSION: &str = "v1";

/// 判定容器自己的 cgroup 根。
pub const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// 判定进程跑完时，它自己看到的资源量。
///
/// 每个字段都是 `Option`：读不到与读到零是两件事，前者不进编码、也不参与比值。整条读数
/// 里没有任何"缺省 0"——`0` 只可能是真的量到了 0。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RolloutResourceReading {
    /// 读数取走的时刻（秒，判定进程自己的钟），用来把读数钉在**哪一次跑**上。
    pub at_unix: Option<u64>,
    /// cgroup 声明的 CPU 配额与周期，单位微秒。`cpu.max` 写 `max` 时配额缺席。
    pub cpu_quota_us: Option<u64>,
    pub cpu_period_us: Option<u64>,
    /// 这次跑经历的 CFS 周期数与其中被限流的周期数。
    pub cpu_periods: Option<u64>,
    pub cpu_throttled_periods: Option<u64>,
    /// 内存上限与整轮运行的峰值占用，单位字节。上限为 `max` 时缺席。
    pub memory_max_bytes: Option<u64>,
    pub memory_peak_bytes: Option<u64>,
}

/// 字段名与它们在结构里的位置，**编码与解码同读这一张表**：两边各拼一遍名字，改一处就
/// 会静默地少读一个字段。顺序即线上顺序，只影响可读性，解析按 `key=value` 走。
const FIELDS: [&str; 7] = [
    "at",
    "cpu_quota_us",
    "cpu_period_us",
    "cpu_periods",
    "cpu_throttled_periods",
    "memory_max",
    "memory_peak",
];

impl RolloutResourceReading {
    /// 读一次这个进程自己的 cgroup。`at_unix` 由调用方给，读文件这件事不碰时钟。
    ///
    /// 每一个文件独立读：读不到就不填它那几个字段，不影响读得到的部分。一个字段都填不上
    /// 时返回 `None`——那是"这个环境没得读"。
    pub fn read_from(cgroup_root: &Path, at_unix: u64) -> Option<Self> {
        let mut reading = RolloutResourceReading {
            at_unix: Some(at_unix),
            ..Default::default()
        };
        let mut measured = false;

        // `cpu.max`：`<配额> <周期>`，配额为 `max` 时不给数（不是 0——0 是一个被限死的量，
        // 与"不设限"处置相反）。
        if let Ok(text) = std::fs::read_to_string(cgroup_root.join("cpu.max")) {
            let mut it = text.split_whitespace();
            reading.cpu_quota_us = it.next().and_then(|q| q.parse::<u64>().ok());
            reading.cpu_period_us = it.next().and_then(|p| p.parse::<u64>().ok());
            measured = true;
        }

        if let Ok(text) = std::fs::read_to_string(cgroup_root.join("cpu.stat")) {
            for line in text.lines() {
                let mut parts = line.split_whitespace();
                let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
                    continue;
                };
                let Ok(value) = value.parse::<u64>() else {
                    continue;
                };
                match key {
                    "nr_periods" => reading.cpu_periods = Some(value),
                    "nr_throttled" => reading.cpu_throttled_periods = Some(value),
                    _ => {}
                }
            }
            measured = true;
        }

        if let Some(bytes) = read_cgroup_bytes(&cgroup_root.join("memory.max")) {
            reading.memory_max_bytes = bytes;
            measured = true;
        }
        if let Some(bytes) = read_cgroup_bytes(&cgroup_root.join("memory.peak")) {
            reading.memory_peak_bytes = bytes;
            measured = true;
        }

        measured.then_some(reading)
    }

    /// 被限流的周期占比。这是"上限咬着这次跑"的**频度**读数。
    ///
    /// 周期数为 0 时返回 `None`：那说明这段 cgroup 还没过完一个周期，比值的分母不存在，
    /// 而不是比值为 0。
    pub fn cpu_throttled_period_ratio(&self) -> Option<f64> {
        let periods = self.cpu_periods?;
        let throttled = self.cpu_throttled_periods?;
        (periods > 0).then(|| throttled as f64 / periods as f64)
    }

    /// 峰值内存占上限的比例，即"离顶还有多远"。
    ///
    /// 上限缺席（`memory.max` = `max`）或为 0 时返回 `None`：没有上限就没有比例，这与
    /// "占用为 0"是两回事。
    pub fn memory_peak_ratio(&self) -> Option<f64> {
        let max = self.memory_max_bytes?;
        let peak = self.memory_peak_bytes?;
        (max > 0).then(|| peak as f64 / max as f64)
    }

    /// 一个字段的取值，按 [`FIELDS`] 里的名字取。
    fn get(&self, key: &str) -> Option<u64> {
        match key {
            "at" => self.at_unix,
            "cpu_quota_us" => self.cpu_quota_us,
            "cpu_period_us" => self.cpu_period_us,
            "cpu_periods" => self.cpu_periods,
            "cpu_throttled_periods" => self.cpu_throttled_periods,
            "memory_max" => self.memory_max_bytes,
            "memory_peak" => self.memory_peak_bytes,
            _ => None,
        }
    }

    /// 把一个字段落进结构。名字不认识就返回 `false`——解码端据此忽略陌生字段，而不是把
    /// 整条读数丢掉。
    fn set(&mut self, key: &str, value: u64) -> bool {
        match key {
            "at" => self.at_unix = Some(value),
            "cpu_quota_us" => self.cpu_quota_us = Some(value),
            "cpu_period_us" => self.cpu_period_us = Some(value),
            "cpu_periods" => self.cpu_periods = Some(value),
            "cpu_throttled_periods" => self.cpu_throttled_periods = Some(value),
            "memory_max" => self.memory_max_bytes = Some(value),
            "memory_peak" => self.memory_peak_bytes = Some(value),
            _ => return false,
        }
        true
    }

    /// 装进终止消息那一行：`rollout-resources:v1:at=…:cpu_periods=…`。
    ///
    /// 只写量到的字段：缺的键不在行里，读回端于是也读成缺。
    pub fn to_line(&self) -> String {
        let mut parts = vec![TAG.to_string(), FORMAT_VERSION.to_string()];
        for key in FIELDS {
            if let Some(value) = self.get(key) {
                parts.push(format!("{key}={value}"));
            }
        }
        parts.join(":")
    }

    /// 从一行里读回读数。不是自己写的形状（标记不对、版本不认）一律 `None`。
    ///
    /// 单个字段解不出来只丢那个字段：一行里有个数被写坏了，不代表同一行的其他读数是假的。
    pub fn from_line(line: &str) -> Option<Self> {
        let mut parts = line.trim().split(':');
        if parts.next()? != TAG || parts.next()? != FORMAT_VERSION {
            return None;
        }
        let mut reading = RolloutResourceReading::default();
        for part in parts {
            let Some((key, value)) = part.split_once('=') else {
                continue;
            };
            let Ok(value) = value.parse::<u64>() else {
                continue;
            };
            reading.set(key.trim(), value);
        }
        Some(reading)
    }
}

impl fmt::Display for RolloutResourceReading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_line())
    }
}

/// 读一个 cgroup 的字节数文件。`max`（不设限）读成 `None` 但算"读到了"——它与"文件不在" 不是
/// 一回事，前者说明这个字段**没有上限**，后者说明这个字段**没有读数**。
fn read_cgroup_bytes(path: &Path) -> Option<Option<u64>> {
    let text = std::fs::read_to_string(path).ok()?;
    Some(text.trim().parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_with(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, body) in files {
            std::fs::write(dir.path().join(name), body).unwrap();
        }
        dir
    }

    /// 一份真实内核写出来的样子（取自部署机上的判定容器）。
    fn live_shape() -> tempfile::TempDir {
        dir_with(&[
            ("cpu.max", "200000 100000\n"),
            (
                "cpu.stat",
                "usage_usec 2041858\nuser_usec 1500000\nsystem_usec 541858\nnr_periods 263\nnr_throttled 7\nthrottled_usec 90685\n",
            ),
            ("memory.max", "2147483648\n"),
            ("memory.peak", "141529088\n"),
        ])
    }

    #[test]
    fn a_reading_reads_every_field_the_kernel_offers() {
        let dir = live_shape();
        let reading = RolloutResourceReading::read_from(dir.path(), 1_700_000_000).unwrap();
        assert_eq!(reading.cpu_quota_us, Some(200_000));
        assert_eq!(reading.cpu_period_us, Some(100_000));
        assert_eq!(reading.cpu_periods, Some(263));
        assert_eq!(reading.cpu_throttled_periods, Some(7));
        assert_eq!(reading.memory_max_bytes, Some(2_147_483_648));
        assert_eq!(reading.memory_peak_bytes, Some(141_529_088));
        assert_eq!(reading.at_unix, Some(1_700_000_000));
        assert_eq!(reading.cpu_throttled_period_ratio(), Some(7.0 / 263.0));
        assert_eq!(
            reading.memory_peak_ratio(),
            Some(141_529_088.0 / 2_147_483_648.0)
        );
    }

    #[test]
    fn a_line_survives_the_round_trip() {
        let dir = live_shape();
        let reading = RolloutResourceReading::read_from(dir.path(), 1_700_000_000).unwrap();
        let line = reading.to_line();
        assert!(line.starts_with("rollout-resources:v1:at=1700000000"));
        assert_eq!(RolloutResourceReading::from_line(&line), Some(reading));
    }

    /// 缺的字段不在行里，读回来还是缺。这条守住的是"没读到"不会在往返里变成 0。
    #[test]
    fn a_field_nobody_measured_leaves_no_key_and_comes_back_absent() {
        let reading = RolloutResourceReading {
            at_unix: Some(7),
            cpu_periods: Some(10),
            cpu_throttled_periods: Some(3),
            ..Default::default()
        };
        let line = reading.to_line();
        assert_eq!(
            line,
            "rollout-resources:v1:at=7:cpu_periods=10:cpu_throttled_periods=3"
        );
        let back = RolloutResourceReading::from_line(&line).unwrap();
        assert_eq!(back, reading);
        assert_eq!(back.cpu_quota_us, None);
        assert_eq!(back.memory_peak_ratio(), None);
    }

    /// 一个字段写坏了只丢那个字段：同一行的其他读数是独立量出来的。
    #[test]
    fn one_broken_field_does_not_take_the_rest_of_the_line_with_it() {
        let back = RolloutResourceReading::from_line(
            "rollout-resources:v1:at=7:cpu_periods=abc:cpu_throttled_periods=3",
        )
        .unwrap();
        assert_eq!(back.at_unix, Some(7));
        assert_eq!(back.cpu_periods, None);
        assert_eq!(back.cpu_throttled_periods, Some(3));
        assert_eq!(back.cpu_throttled_period_ratio(), None);
    }

    /// 陌生的一行不是读数：标记不对、版本不认、或者干脆是别的东西（失败落点就走在同一条
    /// 通道里）。认错了会让部署器把一段别处的文本当成测量值上报。
    #[test]
    fn a_foreign_line_is_not_a_reading() {
        for line in [
            "version:job:rollout:build",
            "rollout-resources:v2:at=7",
            "at=7:cpu_periods=10",
            "",
        ] {
            assert_eq!(RolloutResourceReading::from_line(line), None, "{line}");
        }
    }

    /// 一个 cgroup 文件都没有时不是"读到了零"，是没得读（cgroup v1 的节点、或者干脆不在
    /// 容器里）。这一档必须与"读到 0 个周期"分开：后者说明这段 cgroup 一个周期都没过完。
    #[test]
    fn an_environment_with_nothing_to_read_is_not_a_reading_of_zero() {
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(RolloutResourceReading::read_from(empty.path(), 7), None);

        let v1 = dir_with(&[("cpu.limit", "max"), ("memory.limit_in_bytes", "0")]);
        assert_eq!(RolloutResourceReading::read_from(v1.path(), 7), None);
    }

    /// 不设限的字段缺席，而不是记 0。`max` 的 CPU 配额与 0 的配额处置相反：后者是被限死。
    #[test]
    fn an_unlimited_field_is_absent_not_zero() {
        let dir = dir_with(&[
            ("cpu.max", "max 100000\n"),
            ("cpu.stat", "nr_periods 5\nnr_throttled 0\n"),
            ("memory.max", "max\n"),
            ("memory.peak", "1024\n"),
        ]);
        let reading = RolloutResourceReading::read_from(dir.path(), 7).unwrap();
        assert_eq!(reading.cpu_quota_us, None);
        assert_eq!(reading.cpu_period_us, Some(100_000));
        assert_eq!(reading.memory_max_bytes, None);
        assert_eq!(reading.memory_peak_bytes, Some(1024));
        // 周期数有了，占比照样算得出；内存没上限，比例就是没有。
        assert_eq!(reading.cpu_throttled_period_ratio(), Some(0.0));
        assert_eq!(reading.memory_peak_ratio(), None);
    }

    /// 分母不存在时比值是"没有"，不是 0：一个还没过完周期的 cgroup 与一个完全没被限流的
    /// cgroup，读数上必须分得开。
    #[test]
    fn a_ratio_without_its_denominator_is_absent() {
        let no_periods = RolloutResourceReading {
            cpu_periods: Some(0),
            cpu_throttled_periods: Some(0),
            memory_max_bytes: Some(0),
            memory_peak_bytes: Some(0),
            ..Default::default()
        };
        assert_eq!(no_periods.cpu_throttled_period_ratio(), None);
        assert_eq!(no_periods.memory_peak_ratio(), None);

        let half = RolloutResourceReading {
            cpu_periods: Some(0),
            ..Default::default()
        };
        assert_eq!(half.cpu_throttled_period_ratio(), None);
    }
}
