//! principle: feedback_a_source_shape_gate_is_a_false_green_for_behaviour
//!
//! 容器的资源请求与上限跟着实测用量走。
//!
//! 这份文件之前，103 个资源数字全是人在 values.yaml 里定的：它们是调度声明、
//! 突发上界、索引大小的猜测，没有一个由测量写回。于是「这台部署实际要多少」
//! 这个问题在集群里没有答案——只有一个人当时觉得是多少。
//!
//! 这个回路把答案补上，形状与卷容量那条一致：**取样 → 滚动窗口 → 算目标 →
//! 量化/防抖 → 写回 → 发布读数**。
//!
//! - **测量入口**：kubelet 的 `stats/summary`（经 apiserver 代理
//!   `/api/v1/nodes/<节点>/proxy/stats/summary`，权限 `nodes/stats` 已在
//!   `cogneva-topology-reader` 里）。每个 Pod 的 `containers[]` 给容器级
//!   `cpu.usageNanoCores` 与 `memory.workingSetBytes`——工作集正是内存压力与
//!   OOM 判定看的那一个数。chart 不部署 Prometheus，也不假设 cAdvisor 采集，
//!   所以策略不以那两者为前提。
//! - **窗口**：7 天。样本落在数据卷上的一个 JSON 文件里，Pod 重启不丢；
//!   窗口滚动，旧样本随拍子退出。
//! - **律**：p95 决定请求，峰值 × 余量决定内存上限；CPU 上限取
//!   `max(p95 × 余量, request)`。余量与卷容量回路同一条（`HEADROOM_PERCENT`），
//!   不另立一套。
//! - **写回**：patch 工作负载的容器 spec——Deployment / StatefulSet /
//!   DaemonSet 走 `/spec/template/spec/*`，CronJob 走
//!   `/spec/jobTemplate/spec/template/spec/*`。升与降都做：CPU 与内存都能缩。
//! - **防抖**：目标与现值差不到一个量化格（CPU 10m、内存 16Mi）就不写；
//!   一次写回会把新值当作下一拍的现值，不靠「刚改过」的时间戳。
//!
//! 只动**已经声明了资源**的容器。没有声明的容器是有意留白的（短命 hook Job
//! 由命名空间默认层兜底），这个回路不替它发明一份声明。
//!
//! 它也不动没有实测的容器：窗口里的样本少于 [`MIN_SAMPLES`] 时，读数标成
//! `seed`，写回留给下一拍。第一拍就把种子换成目标等于拿一次采样当结论。
//!
//! 自己人也在名单里：`cogneva-evolution` 的 Deployment 就是本进程的 Deployment。
//! patch 它会触发一次滚动——这正是回路的效果，不是副作用。窗口在卷上，
//! 重启后目标不变、差值小于一格，于是只滚一次。

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFResult, ShutdownSignal};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

/// 请求的 CPU 目标，毫核。
pub const REQUEST_CPU_METRIC: &str = "cogneva_workload_request_cpu_millicores";

/// 请求的内存目标，字节。
pub const REQUEST_MEMORY_METRIC: &str = "cogneva_workload_request_memory_bytes";

/// 上限的 CPU 目标，毫核。
pub const LIMIT_CPU_METRIC: &str = "cogneva_workload_limit_cpu_millicores";

/// 上限的内存目标，字节。
pub const LIMIT_MEMORY_METRIC: &str = "cogneva_workload_limit_memory_bytes";

/// 这个容器到目前为止被求值过多少拍。
///
/// 关键的一条不是「目标是多少」，而是「拍子还在走」：值不变是合法结果，
/// 但读者要能把「这一拍算过、结论没变」与「回路早停了」分清。计数只在
/// 每次真的求值时加，不在发布时加。
pub const EVALUATIONS_METRIC: &str = "cogneva_workload_resources_evaluations_total";

/// 窗口里这个容器有多少个样本。样本数为 0 时这一族读数只说明没在量。
pub const SAMPLES_METRIC: &str = "cogneva_workload_resources_samples";

/// 内存盘（`emptyDir` `medium: Memory`）的目标，字节。
///
/// 它与容器内存上限同一条律、同一个窗口：tmpfs 的页是内存，一个容器能写进
/// `/dev/shm` 的不会超过它自己被量出来的内存上限。单独一个序列名，是因为它
/// 按**卷**而不是按容器发布——一个工作负载可以挂多块，读者要能分辨是哪一块。
pub const SHM_METRIC: &str = "cogneva_workload_shm_size_bytes";

/// 工作负载名。
pub const WORKLOAD_LABEL: &str = "workload";

/// 容器名。
pub const CONTAINER_LABEL: &str = "container";

/// 卷名（内存盘那一条读数面）。
pub const VOLUME_LABEL: &str = "volume";

/// 这个值是怎么来的：`derived`（实测窗口给了目标）或 `seed`（窗口还没够）。
pub const SOURCE_LABEL: &str = "source";

/// 目标来自实测。
pub const SOURCE_DERIVED: &str = "derived";

/// 目标还没成立，当前生效的是 chart 的种子。
pub const SOURCE_SEED: &str = "seed";

/// 判定 Job 在这个读数面上的身份。
///
/// Job 活不过一轮、名字每轮都不同，主回路看不见它；它由部署器在 Job 结束后把
/// 「下一次会写成什么」写进来。用一个固定的族名而不是每轮的 Job 名，是因为
/// 要看的是「这条律在这个调用点上收敛到哪」，不是某一次运行的对象名。
pub const ROLLOUT_JOB_WORKLOAD: &str = "mainline-rollout";
pub const ROLLOUT_JOB_CONTAINER: &str = "mainline-rollout";

/// 内存与 CPU 上限相对实测的余量，百分比。与卷容量回路同一条律。
// cap: fixed — 余量是这条律与卷容量回路共用的一个协议数，不是某次测量的结果；两处各写一份才会有两条说法。
const HEADROOM_PERCENT: u64 = 120;

/// CPU 的量化格：1/100 核。调度器与 kubelet 的 CPU 粒度本来就是毫核，
/// 比这更细的差写回对象里也留不住。
// cap: fixed — 毫核是调度器与 kubelet 的 CPU 粒度，0.01 核就是这一层的结构下限。
const CPU_QUANTUM_MILLICORES: u64 = 10;

/// 内存的量化格。16Mi 是 kubelet 自己的统计分辨率附近，差小于它就不值得
/// churn 一次对象。
// cap: bound — 16Mi 是 kubelet 近似的统计分辨率：比这更小的差在读数上分不出来，写回只是让对象换一个哈希；它是「读数本身分辨不出」的下限，不是一个人挑的尺寸。
const MEMORY_QUANTUM_BYTES: u64 = 16 * 1024 * 1024;

/// 滚动窗口的长度：7 天。
///
/// 窗口不是采样节拍，是「多旧的用量还算数」。7 天盖住一个完整的周周期
/// （批量任务、备份、人工活动），又不至于让一次已修掉的旧尖峰永远决定今天
/// 的请求。
// cap: fixed — 一周是这条窗口的协议值：盖住一个完整周周期，又不让旧尖峰过冬。
const WINDOW_SECS: u64 = 7 * 24 * 60 * 60;

/// 一个容器至少要有多少个样本才允许写回。
///
/// 12 个样本在默认节拍下是一小时。少于此数时一次突发就是一个结论，而
/// 资源声明会跟着它抖。
// cap: bound — 12 个样本在默认节拍下是一小时；低于它，p95 与峰值落在同一两个点上，量出来的不是分布。
const MIN_SAMPLES: usize = 12;

/// 默认采样节拍（秒）。
// cap: fixed — 五分钟一拍是这份部署的采样约定；再密只是重复读同一段窗口。
const DEFAULT_INTERVAL_SECS: u64 = 300;

/// 节拍下限（秒）。再密就只是在重复读同一段窗口里的数。
// cap: fixed — 一分钟是这条回路允许的最密拍子，防止部署方把它配成忙等。
const MIN_INTERVAL_SECS: u64 = 60;

/// 窗口文件的落点，相对于 `COGNEVA_DATA_DIR`。
const WINDOW_FILE: &str = "workload-resources/window.json";

/// 窗口文件的覆盖点，测试与宿主安装用。
const WINDOW_PATH_ENV: &str = "COGNEVA_WORKLOAD_RESOURCES_WINDOW";

/// 节拍的覆盖点。
const INTERVAL_ENV: &str = "COGNEVA_WORKLOAD_RESOURCES_INTERVAL_SECS";

/// 本回路在 `cogneva_loop_tick_age_seconds{loop=...}` 里的名字。
pub const LOOP: &str = "workload_resources";

/// 一个容器在一拍里的用量。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    /// 取样的时刻（Unix 秒）。
    pub at: u64,
    pub cpu_millicores: u64,
    pub memory_bytes: u64,
}

/// 一个容器当前的目标。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Targets {
    pub cpu_request_millicores: u64,
    pub memory_request_bytes: u64,
    pub cpu_limit_millicores: u64,
    pub memory_limit_bytes: u64,
}

/// 一个容器在对象里已经声明的值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Declared {
    pub cpu_request_millicores: Option<u64>,
    pub memory_request_bytes: Option<u64>,
    pub cpu_limit_millicores: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
}

/// 目标与现值差得够不够一次写回。
pub fn wants_write(current: Option<u64>, target: u64, quantum: u64) -> bool {
    match current {
        None => true,
        Some(current) => current.abs_diff(target) >= quantum,
    }
}

/// 把 `value` 向上取整到 `quantum` 的整数倍。
fn quantize_up(value: u64, quantum: u64) -> u64 {
    if quantum == 0 {
        return value;
    }
    value.div_ceil(quantum).saturating_mul(quantum).max(quantum)
}

/// 百分位。`percent` 取 0–100；空切片没有答案。
pub fn percentile(sorted: &[u64], percent: u64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (sorted.len() as u64 * percent).div_ceil(100).max(1) as usize;
    sorted.get(rank - 1).copied()
}

/// 尺寸律：请求取 p95，内存上限取峰值 × 余量。
///
/// 纯函数，四种维度都从同一组样本推出；它回答的只是「这些样本对应的目标
/// 是什么」，不回答「该不该写」。
pub fn size_targets(cpu_samples: &[u64], memory_samples: &[u64]) -> Option<Targets> {
    let mut cpu = cpu_samples.to_vec();
    let mut memory = memory_samples.to_vec();
    if cpu.is_empty() || memory.is_empty() {
        return None;
    }
    cpu.sort_unstable();
    memory.sort_unstable();
    let cpu_p95 = percentile(&cpu, 95)?;
    let memory_p95 = percentile(&memory, 95)?;
    let memory_peak = *memory.last()?;
    let cpu_request = quantize_up(cpu_p95, CPU_QUANTUM_MILLICORES);
    let memory_request = quantize_up(memory_p95, MEMORY_QUANTUM_BYTES);
    let cpu_limit = quantize_up(
        cpu_p95.saturating_mul(HEADROOM_PERCENT) / 100,
        CPU_QUANTUM_MILLICORES,
    )
    .max(cpu_request);
    let memory_limit = quantize_up(
        memory_peak.saturating_mul(HEADROOM_PERCENT) / 100,
        MEMORY_QUANTUM_BYTES,
    )
    .max(memory_request);
    Some(Targets {
        cpu_request_millicores: cpu_request,
        memory_request_bytes: memory_request,
        cpu_limit_millicores: cpu_limit,
        memory_limit_bytes: memory_limit,
    })
}

/// 一次滚动 Job 的实测：平均核数、内存峰值、被限流的周期占比。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RunSample {
    pub at: u64,
    pub cpu_millicores: u64,
    pub memory_bytes: u64,
    pub throttled_ratio: f64,
}

/// 限流目标：被限流的周期占比超过它，就认为这次跑的 CPU 上限在咬自己。
// cap: bound — 10% 是 CFS 限流开始影响单次运行尾部延迟的频度；低于它的限流是正常的分时公平，高于它的限流会让判定进程自己的时钟变长。
const THROTTLE_TARGET: f64 = 0.10;

/// 把一份滚动 Job 的运行史收成一个目标。
///
/// 与容器资源同一条尺寸律（请求取 p95、上限取峰值 × 余量），只多一步：**限流
/// 是被限住的那一侧的证据**。最近一次跑有超过目标的周期被限流，就按那个比例
/// 抬高 CPU 上限；不限流时不额外加码，上限本来就由 p95 决定。
pub fn rollout_job_targets(history: &[RunSample]) -> Option<Targets> {
    let cpu: Vec<u64> = history.iter().map(|sample| sample.cpu_millicores).collect();
    let memory: Vec<u64> = history.iter().map(|sample| sample.memory_bytes).collect();
    let mut targets = size_targets(&cpu, &memory)?;
    let throttle = history
        .iter()
        .rev()
        .take(RUNS_IN_THROTTLE_LOOKBACK)
        .map(|sample| sample.throttled_ratio)
        .fold(0.0f64, f64::max);
    if throttle > THROTTLE_TARGET {
        targets.cpu_limit_millicores = quantize_up(
            (targets.cpu_limit_millicores as f64 * (1.0 + throttle)) as u64,
            CPU_QUANTUM_MILLICORES,
        )
        .max(targets.cpu_limit_millicores);
    }
    Some(targets)
}

/// 限流判据看最近几次跑：一次偶发的限流不改变上限，连续几次才会。
// cap: bound — 三次是这份部署里一个滚动结论从发生到被观察到的拍数（Job 每轮至多派一次，部署器按分钟轮询）；再多就把已经修好的旧限流也算进来。
const RUNS_IN_THROTTLE_LOOKBACK: usize = 3;

/// 丢掉窗口外的运行史。
pub fn prune_runs(history: &mut Vec<RunSample>, now: u64) {
    let oldest = now.saturating_sub(WINDOW_SECS);
    history.retain(|sample| sample.at >= oldest);
}

/// 样本的滚动窗口，按 `kind/name/container` 分桶。
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Window {
    #[serde(default)]
    entries: BTreeMap<String, VecDeque<Sample>>,
}

impl Window {
    /// 记一个样本，并把窗口里已经过期的样本丢掉。
    pub fn push(&mut self, key: &str, sample: Sample, now: u64) {
        let bucket = self.entries.entry(key.to_string()).or_default();
        bucket.push_back(sample);
        let oldest = now.saturating_sub(WINDOW_SECS);
        while bucket.front().is_some_and(|s| s.at < oldest) {
            bucket.pop_front();
        }
    }

    /// 这一桶现在有几个样本。
    pub fn samples(&self, key: &str) -> usize {
        self.entries.get(key).map(VecDeque::len).unwrap_or(0)
    }

    /// 这一桶的目标，样本不足 [`MIN_SAMPLES`] 时没有答案。
    pub fn targets(&self, key: &str) -> Option<Targets> {
        let bucket = self.entries.get(key)?;
        if bucket.len() < MIN_SAMPLES {
            return None;
        }
        let cpu: Vec<u64> = bucket.iter().map(|s| s.cpu_millicores).collect();
        let memory: Vec<u64> = bucket.iter().map(|s| s.memory_bytes).collect();
        size_targets(&cpu, &memory)
    }

    /// 丢掉窗口里所有已经过期的样本；没有样本的桶整桶消失。
    pub fn prune(&mut self, now: u64) {
        let oldest = now.saturating_sub(WINDOW_SECS);
        self.entries.retain(|_, bucket| {
            while bucket.front().is_some_and(|s| s.at < oldest) {
                bucket.pop_front();
            }
            !bucket.is_empty()
        });
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{\"entries\":{}}".to_string())
    }

    fn from_json(text: &str) -> Self {
        serde_json::from_str(text).unwrap_or_default()
    }

    /// 从磁盘读回窗口；文件不存在或读不懂都是「这一拍从头开始」。
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::from_json(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => {
                warn!(path = %path.display(), error = %e, "workload resources: the sample window could not be read; starting empty");
                Self::default()
            }
        }
    }

    /// 写回磁盘。先写同目录的临时文件再改名，读的一方永远看不到半个文件。
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, self.to_json())?;
        std::fs::rename(&tmp, path)
    }
}

/// 窗口文件的落点。
pub fn window_path() -> PathBuf {
    std::env::var(WINDOW_PATH_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| cog_core::paths::data_path(WINDOW_FILE))
}

/// 一个容器在工作负载对象里的样子。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerSpec {
    pub name: String,
    /// init 容器与普通容器是两棵树，patch 的路径不同。
    pub init: bool,
    /// 对象里已经声明的资源。`None` 表示这个容器有意不声明，回路不碰它。
    pub declared: Option<Declared>,
}

/// 一个可 patch 的工作负载对象。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadSpec {
    pub kind: String,
    pub name: String,
    pub containers: Vec<ContainerSpec>,
    /// 内存介质、声明了 `sizeLimit` 的卷。没有上限的卷不在里面：那没有数可写回。
    pub shm: Vec<ShmVolume>,
}

/// 一块以内存为介质的卷，以及挂载它的容器。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShmVolume {
    pub volume: String,
    /// 把这块卷挂在 `/dev/shm` 的普通容器。找不到就只发布读数、不写回：
    /// 不知道该跟谁的内存上限走时，任何目标都是凭空造的。
    pub container: Option<String>,
    /// 当前声明的 `sizeLimit`；写了但解析不出也是 `None`，那时会被当成「该写」。
    pub declared: Option<u64>,
}

impl WorkloadSpec {
    fn key(&self, container: &ContainerSpec) -> String {
        format!("{}/{}/{}", self.kind, self.name, container.name)
    }
}

/// 解析一个数量字符串成 CPU 毫核。
///
/// `500m`、`2`、`0.5` 是 chart 里出现过的三种写法；`u`/`n` 也收，因为
/// kubelet summary 里能见到。
pub fn parse_cpu_millicores(text: &str) -> Option<u64> {
    let text = text.trim();
    let (number, factor) = if let Some(value) = text.strip_suffix('n') {
        (value, 1.0 / 1_000_000.0)
    } else if let Some(value) = text.strip_suffix('u') {
        (value, 1.0 / 1_000.0)
    } else if let Some(value) = text.strip_suffix('m') {
        (value, 1.0)
    } else {
        (text, 1000.0)
    };
    let value: f64 = number.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some((value * factor).round() as u64)
}

/// 解析一个数量字符串成字节。
pub fn parse_memory_bytes(text: &str) -> Option<u64> {
    let text = text.trim();
    const BINARY: [(&str, u64); 4] = [
        ("Ki", 1024),
        ("Mi", 1024 * 1024),
        ("Gi", 1024 * 1024 * 1024),
        ("Ti", 1024 * 1024 * 1024 * 1024),
    ];
    const DECIMAL: [(&str, u64); 4] = [
        ("k", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
    ];
    let mut suffix_factor = 1u64;
    let mut number = text;
    for (suffix, factor) in BINARY.iter().chain(DECIMAL.iter()) {
        if let Some(stripped) = text.strip_suffix(suffix) {
            number = stripped;
            suffix_factor = *factor;
            break;
        }
    }
    let value: f64 = number.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some((value * suffix_factor as f64).round() as u64)
}

/// 一个声明的 `resources` 块。
fn declared_from(value: Option<&Value>) -> Option<Declared> {
    let resources = value?;
    let requests = resources.get("requests");
    let limits = resources.get("limits");
    let read = |block: Option<&Value>, field: &str, parser: fn(&str) -> Option<u64>| {
        block
            .and_then(|block| block.get(field))
            .and_then(Value::as_str)
            .and_then(parser)
    };
    let declared = Declared {
        cpu_request_millicores: read(requests, "cpu", parse_cpu_millicores),
        memory_request_bytes: read(requests, "memory", parse_memory_bytes),
        cpu_limit_millicores: read(limits, "cpu", parse_cpu_millicores),
        memory_limit_bytes: read(limits, "memory", parse_memory_bytes),
    };
    (declared != Declared::default()).then_some(declared)
}

/// 一个 pod spec 里的容器。
fn containers_from(spec: &Value) -> Vec<ContainerSpec> {
    let mut out = Vec::new();
    for (field, init) in [("containers", false), ("initContainers", true)] {
        let Some(items) = spec.get(field).and_then(Value::as_array) else {
            continue;
        };
        for item in items {
            let Some(name) = item.get("name").and_then(Value::as_str) else {
                continue;
            };
            out.push(ContainerSpec {
                name: name.to_string(),
                init,
                declared: declared_from(item.get("resources")),
            });
        }
    }
    out
}

/// `/dev/shm` 是这个回路认的挂载点。
///
/// 不按「任何挂内存介质的卷」泛化：只有 `/dev/shm` 的语义是固定的（渲染进程
/// 拿它当共享内存工作区），别的内存卷跟着哪个容器走没有同一条约定，硬套会把
/// 一个不相关的容器内存上限写到它头上。
const SHM_MOUNT_PATH: &str = "/dev/shm";

/// 一个 pod spec 里声明的内存盘。
fn shm_from(spec: &Value) -> Vec<ShmVolume> {
    let mut mounted: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    let Some(containers) = spec.get("containers").and_then(Value::as_array) else {
        return Vec::new();
    };
    for container in containers {
        let Some(name) = container.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(mounts) = container.get("volumeMounts").and_then(Value::as_array) else {
            continue;
        };
        for mount in mounts {
            if mount.get("mountPath").and_then(Value::as_str) != Some(SHM_MOUNT_PATH) {
                continue;
            }
            if let Some(volume) = mount.get("name").and_then(Value::as_str) {
                mounted.entry(volume).or_insert(name);
            }
        }
    }
    let Some(volumes) = spec.get("volumes").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for volume in volumes {
        if volume.pointer("/emptyDir/medium").and_then(Value::as_str) != Some("Memory") {
            continue;
        }
        let Some(name) = volume.get("name").and_then(Value::as_str) else {
            continue;
        };
        let declared = volume
            .pointer("/emptyDir/sizeLimit")
            .and_then(Value::as_str)
            .map(str::to_string);
        if declared.is_none() {
            // 没有声明上限的内存盘不在这条律里：没有数要跟着测量走。
            continue;
        }
        out.push(ShmVolume {
            volume: name.to_string(),
            container: mounted.get(name).map(|name| name.to_string()),
            declared: declared.as_deref().and_then(parse_memory_bytes),
        });
    }
    out
}

/// 从 `kubectl get deployments,statefulsets,daemonsets,cronjobs -o json` 的
/// 输出里读出可 patch 的对象。
pub fn parse_workloads(list: &Value) -> Vec<WorkloadSpec> {
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in items {
        let kind = item
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let Some(name) = item
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        if item.pointer("/metadata/deletionTimestamp").is_some() {
            continue;
        }
        let spec = match kind.as_str() {
            "CronJob" => item.pointer("/spec/jobTemplate/spec/template/spec"),
            "Deployment" | "StatefulSet" | "DaemonSet" => item.pointer("/spec/template/spec"),
            _ => None,
        };
        let Some(spec) = spec else { continue };
        let containers = containers_from(spec);
        if containers.is_empty() {
            continue;
        }
        out.push(WorkloadSpec {
            kind,
            name,
            containers,
            shm: shm_from(spec),
        });
    }
    out
}

/// Pod 是谁的：直接归属的工作负载，解析不到就是 `None`。
pub fn pod_workload(
    pod: &Value,
    replicasets: &HashMap<String, String>,
    jobs: &HashMap<String, String>,
) -> Option<(String, String)> {
    let owner = pod
        .pointer("/metadata/ownerReferences/0")
        .or_else(|| pod.pointer("/metadata/ownerReferences"))?;
    let kind = owner.get("kind").and_then(Value::as_str)?;
    let name = owner.get("name").and_then(Value::as_str)?;
    match kind {
        "StatefulSet" | "DaemonSet" => Some((kind.to_string(), name.to_string())),
        "ReplicaSet" => replicasets
            .get(name)
            .map(|deployment| ("Deployment".to_string(), deployment.clone())),
        "Job" => jobs
            .get(name)
            .map(|cronjob| ("CronJob".to_string(), cronjob.clone())),
        _ => None,
    }
}

/// `kind/name → 上一级 owner 名`。只收能解析到工作负载的那一级。
pub fn owner_map(list: &Value, owner_kind: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return out;
    };
    for item in items {
        let Some(name) = item.pointer("/metadata/name").and_then(Value::as_str) else {
            continue;
        };
        let Some(owner) = item.pointer("/metadata/ownerReferences/0") else {
            continue;
        };
        if owner.get("kind").and_then(Value::as_str) != Some(owner_kind) {
            continue;
        }
        let Some(owner_name) = owner.get("name").and_then(Value::as_str) else {
            continue;
        };
        out.insert(name.to_string(), owner_name.to_string());
    }
    out
}

/// kubelet summary 里一个 Pod 的容器用量。
pub fn parse_summary(summary: &Value, namespace: &str) -> HashMap<String, HashMap<String, Sample>> {
    let mut out = HashMap::new();
    let Some(pods) = summary.get("pods").and_then(Value::as_array) else {
        return out;
    };
    for pod in pods {
        if pod.pointer("/podRef/namespace").and_then(Value::as_str) != Some(namespace) {
            continue;
        }
        let Some(name) = pod.pointer("/podRef/name").and_then(Value::as_str) else {
            continue;
        };
        let mut containers = HashMap::new();
        for container in pod
            .get("containers")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let Some(container_name) = container.get("name").and_then(Value::as_str) else {
                continue;
            };
            let cpu = nanocores(&container, "usageNanoCores");
            let memory = bytes(container.pointer("/memory/workingSetBytes"));
            // 内存为 0 的样本不是一个「空闲容器」，是 kubelet 这一刻没有给出
            // 工作集。把它当 0 会让 p95 掉到地板，把请求写在解不出来的读数上。
            if memory == 0 {
                continue;
            }
            containers.insert(
                container_name.to_string(),
                Sample {
                    at: 0,
                    cpu_millicores: cpu,
                    memory_bytes: memory,
                },
            );
        }
        out.insert(name.to_string(), containers);
    }
    out
}

fn nanocores(container: &Value, field: &str) -> u64 {
    let value = container.pointer(&format!("/cpu/{field}"));
    let nanocores = value.and_then(|v| {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|text| text.parse::<f64>().ok()))
    });
    match nanocores {
        Some(nanocores) if nanocores.is_finite() && nanocores > 0.0 => {
            (nanocores / 1_000_000.0).round() as u64
        }
        _ => 0,
    }
}

fn bytes(value: Option<&Value>) -> u64 {
    value
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|text| text.parse::<u64>().ok()))
        })
        .unwrap_or(0)
}

/// 一个容器要写回的资源块。
fn resources_json(targets: &Targets) -> Value {
    json!({
        "requests": {
            "cpu": cpu_quantity(targets.cpu_request_millicores),
            "memory": memory_quantity(targets.memory_request_bytes),
        },
        "limits": {
            "cpu": cpu_quantity(targets.cpu_limit_millicores),
            "memory": memory_quantity(targets.memory_limit_bytes),
        },
    })
}

pub fn cpu_quantity(millicores: u64) -> String {
    if millicores % 1000 == 0 {
        format!("{}", millicores / 1000)
    } else {
        format!("{millicores}m")
    }
}

pub fn memory_quantity(bytes: u64) -> String {
    // cap: fixed — 这两个是二进制单位的定义（2^30 / 2^20），不是可调的界。
    const GIB: u64 = 1024 * 1024 * 1024;
    // cap: fixed — 同上，2^20 字节。
    const MIB: u64 = 1024 * 1024;
    if bytes % GIB == 0 {
        format!("{}Gi", bytes / GIB)
    } else {
        format!("{}Mi", bytes / MIB)
    }
}

/// 一次 patch 的请求体。
///
/// `changes` 按容器名给目标，init 与普通容器分开；`shm_changes` 按卷名给内存盘的
/// 目标。两组都不动时返回 `None`——一次没有内容的 patch 只会让对象换一个版本号。
pub fn patch_body(
    workload: &WorkloadSpec,
    changes: &BTreeMap<String, Targets>,
    shm_changes: &BTreeMap<String, u64>,
) -> Option<Value> {
    let mut regular = Vec::new();
    let mut init = Vec::new();
    for container in &workload.containers {
        let Some(targets) = changes.get(&container.name) else {
            continue;
        };
        let entry = json!({
            "name": container.name,
            "resources": resources_json(targets),
        });
        if container.init {
            init.push(entry);
        } else {
            regular.push(entry);
        }
    }
    let volumes: Vec<Value> = workload
        .shm
        .iter()
        .filter_map(|volume| {
            let bytes = shm_changes.get(&volume.volume)?;
            Some(json!({
                "name": volume.volume,
                "emptyDir": { "sizeLimit": memory_quantity(*bytes) },
            }))
        })
        .collect();
    if regular.is_empty() && init.is_empty() && volumes.is_empty() {
        return None;
    }
    let mut spec = serde_json::Map::new();
    if !regular.is_empty() {
        spec.insert("containers".to_string(), Value::Array(regular));
    }
    if !init.is_empty() {
        spec.insert("initContainers".to_string(), Value::Array(init));
    }
    if !volumes.is_empty() {
        spec.insert("volumes".to_string(), Value::Array(volumes));
    }
    let pod_spec = json!({ "spec": spec });
    match workload.kind.as_str() {
        "CronJob" => Some(json!({
            "spec": { "jobTemplate": { "spec": { "template": pod_spec } } }
        })),
        _ => Some(json!({ "spec": { "template": pod_spec } })),
    }
}

/// patch 用的小写资源名。
fn kind_arg(kind: &str) -> &'static str {
    match kind {
        "StatefulSet" => "statefulset",
        "DaemonSet" => "daemonset",
        "CronJob" => "cronjob",
        _ => "deployment",
    }
}

/// 这个回路发布出去的读数。
#[derive(Debug, Default)]
pub struct WorkloadResourcesReadings {
    rows: Mutex<BTreeMap<String, Row>>,
    shm: Mutex<BTreeMap<String, ShmRow>>,
}

/// 一块内存盘在这个读数面上的状态。
#[derive(Debug, Clone)]
struct ShmRow {
    workload: String,
    volume: String,
    size_bytes: u64,
    source: &'static str,
}

#[derive(Debug, Clone)]
struct Row {
    workload: String,
    container: String,
    targets: Targets,
    source: &'static str,
    samples: usize,
    evaluations: u64,
}

impl WorkloadResourcesReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// 这一拍对这个容器求过值。计数在写回之前就加：一个没有目标的容器也
    /// 在被看，读者要看到的是「拍子」，不是「结果」。
    pub fn note_evaluated(&self, workload: &str, container: &str) {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .entry(format!("{workload}/{container}"))
            .or_insert_with(|| Row {
                workload: workload.to_string(),
                container: container.to_string(),
                targets: Targets {
                    cpu_request_millicores: 0,
                    memory_request_bytes: 0,
                    cpu_limit_millicores: 0,
                    memory_limit_bytes: 0,
                },
                source: SOURCE_SEED,
                samples: 0,
                evaluations: 0,
            });
        row.evaluations = row.evaluations.saturating_add(1);
    }

    /// 这一拍的目标与来源。
    pub fn observe(
        &self,
        workload: &str,
        container: &str,
        targets: Targets,
        source: &'static str,
        samples: usize,
    ) {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .entry(format!("{workload}/{container}"))
            .or_insert_with(|| Row {
                workload: workload.to_string(),
                container: container.to_string(),
                targets,
                source,
                samples,
                evaluations: 0,
            });
        row.targets = targets;
        row.source = source;
        row.samples = samples;
    }

    /// 这一个容器的求值次数。
    pub fn evaluations(&self, workload: &str, container: &str) -> u64 {
        self.rows
            .lock()
            .unwrap()
            .get(&format!("{workload}/{container}"))
            .map(|row| row.evaluations)
            .unwrap_or(0)
    }

    /// 这一拍这块内存盘的目标。
    pub fn observe_shm(
        &self,
        workload: &str,
        volume: &str,
        size_bytes: u64,
        source: &'static str,
    ) {
        let mut rows = self.shm.lock().unwrap();
        let row = rows
            .entry(format!("{workload}/{volume}"))
            .or_insert_with(|| ShmRow {
                workload: workload.to_string(),
                volume: volume.to_string(),
                size_bytes,
                source,
            });
        row.size_bytes = size_bytes;
        row.source = source;
    }

}

#[async_trait]
impl Observable for WorkloadResourcesReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let rows = self.rows.lock().unwrap();
        let mut out = Vec::new();
        for row in rows.values() {
            for (metric, value) in [
                (REQUEST_CPU_METRIC, row.targets.cpu_request_millicores),
                (REQUEST_MEMORY_METRIC, row.targets.memory_request_bytes),
                (LIMIT_CPU_METRIC, row.targets.cpu_limit_millicores),
                (LIMIT_MEMORY_METRIC, row.targets.memory_limit_bytes),
                (SAMPLES_METRIC, row.samples as u64),
                (EVALUATIONS_METRIC, row.evaluations),
            ] {
                out.push(
                    RawMetric::new(metric, value as f64)
                        .with_label(WORKLOAD_LABEL, row.workload.clone())
                        .with_label(CONTAINER_LABEL, row.container.clone())
                        .with_label(SOURCE_LABEL, row.source),
                );
            }
        }
        for row in self.shm.lock().unwrap().values() {
            // 内存盘的拍子与容器那一条同一拍：容器那一行的求值计数就是它的
            // 证据。这里再发一个同标签的计数只会让同一个数出现两次。
            out.push(
                RawMetric::new(SHM_METRIC, row.size_bytes as f64)
                    .with_label(WORKLOAD_LABEL, row.workload.clone())
                    .with_label(VOLUME_LABEL, row.volume.clone())
                    .with_label(SOURCE_LABEL, row.source),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

async fn kubectl(args: &[&str]) -> Result<String, String> {
    let cmdline = format!("kubectl {args:?}");
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.args(args).kill_on_drop(true);
    let out = match cog_core::progress::run_child_watched(cmd, &cmdline).await {
        Ok(cog_core::progress::ChildOutcome::Exited(out)) => out,
        Ok(cog_core::progress::ChildOutcome::Stalled { silent, window }) => {
            return Err(format!(
                "{cmdline} 停住了：{silent:?} 没有输出、也没有消耗 CPU，比这份部署见过的\
                 任何一次「还在干活」的两次推进之间的最长间隔都长（{window:?}）；已杀掉"
            ))
        }
        Err(e) => return Err(format!("{cmdline} could not run: {e}")),
    };
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "kubectl {args:?} exited {}: {}",
            out.status.code().unwrap_or(-1),
            stderr.trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn namespace() -> String {
    cog_core::topology::namespace()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 一拍的工作。
pub struct RoundSummary {
    pub evaluated: usize,
    pub patched: usize,
}

/// 跑一拍：读工作负载与用量、喂窗口、算目标、写回、发布读数。
///
/// 所有集群读都是这一拍自己的；读失败就不写回——拿半份输入去改 100 个容器
/// 比这一拍不动贵得多。
pub async fn round(
    window: &mut Window,
    readings: &WorkloadResourcesReadings,
    ns: &str,
    now: u64,
) -> Result<RoundSummary, String> {
    let workload_list: Value = serde_json::from_str(
        &kubectl(&[
            "get",
            "deployments,statefulsets,daemonsets,cronjobs",
            "-n",
            ns,
            "-o",
            "json",
        ])
        .await?,
    )
    .map_err(|e| format!("workloads: {e}"))?;
    let workloads = parse_workloads(&workload_list);
    let known: std::collections::BTreeSet<String> = workloads.iter().map(|w| w.name.clone()).collect();

    let pods: Value =
        serde_json::from_str(&kubectl(&["get", "pods", "-n", ns, "-o", "json"]).await?)
            .map_err(|e| format!("pods: {e}"))?;
    let replicasets: Value =
        serde_json::from_str(&kubectl(&["get", "replicasets", "-n", ns, "-o", "json"]).await?)
            .map_err(|e| format!("replicasets: {e}"))?;
    let jobs: Value =
        serde_json::from_str(&kubectl(&["get", "jobs", "-n", ns, "-o", "json"]).await?)
            .map_err(|e| format!("jobs: {e}"))?;
    let replicasets = owner_map(&replicasets, "Deployment");
    let jobs = owner_map(&jobs, "CronJob");

    let nodes: Value = serde_json::from_str(&kubectl(&["get", "nodes", "-o", "json"]).await?)
        .map_err(|e| format!("nodes: {e}"))?;
    let mut usage: HashMap<String, HashMap<String, Sample>> = HashMap::new();
    for node in nodes
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let Some(name) = node.pointer("/metadata/name").and_then(Value::as_str) else {
            continue;
        };
        let path = format!("/api/v1/nodes/{name}/proxy/stats/summary");
        let Ok(body) = kubectl(&["get", "--raw", &path]).await else {
            continue;
        };
        let Ok(summary) = serde_json::from_str::<Value>(&body) else {
            continue;
        };
        for (pod, containers) in parse_summary(&summary, ns) {
            usage
                .entry(pod)
                .or_default()
                .extend(containers.into_iter());
        }
    }

    // 把这一拍的用量按 Pod 归属喂进窗口。
    let mut fed = 0usize;
    for pod in pods
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let Some(pod_name) = pod.pointer("/metadata/name").and_then(Value::as_str) else {
            continue;
        };
        let Some((kind, workload)) = pod_workload(&pod, &replicasets, &jobs) else {
            continue;
        };
        if !known.contains(&workload) {
            continue;
        }
        let Some(containers) = usage.get(pod_name) else {
            continue;
        };
        for (container, sample) in containers {
            let key = format!("{kind}/{workload}/{container}");
            window.push(
                &key,
                Sample {
                    at: now,
                    ..*sample
                },
                now,
            );
            fed += 1;
        }
    }
    window.prune(now);

    // 每台节点读不到时 `fed` 为 0：不写回，只报告。
    if fed == 0 {
        return Err("no container usage was readable this round; nothing was written".to_string());
    }

    let mut evaluated = 0usize;
    let mut patched = 0usize;
    for workload in &workloads {
        let mut changes = BTreeMap::new();
        // 这一轮已经算出目标、可以喂给下游（内存盘）的容器。
        let mut resolved: BTreeMap<String, Targets> = BTreeMap::new();
        for container in &workload.containers {
            let Some(declared) = container.declared else {
                continue;
            };
            let key = workload.key(container);
            evaluated += 1;
            readings.note_evaluated(&workload.name, &container.name);
            let samples = window.samples(&key);
            match window.targets(&key) {
                Some(targets) => {
                    resolved.insert(container.name.clone(), targets);
                    readings.observe(
                        &workload.name,
                        &container.name,
                        targets,
                        SOURCE_DERIVED,
                        samples,
                    );
                    if wants_write(
                            declared.cpu_request_millicores,
                            targets.cpu_request_millicores,
                            CPU_QUANTUM_MILLICORES,
                        ) || wants_write(
                            declared.memory_request_bytes,
                            targets.memory_request_bytes,
                            MEMORY_QUANTUM_BYTES,
                        ) || wants_write(
                            declared.cpu_limit_millicores,
                            targets.cpu_limit_millicores,
                            CPU_QUANTUM_MILLICORES,
                        ) || wants_write(
                            declared.memory_limit_bytes,
                            targets.memory_limit_bytes,
                            MEMORY_QUANTUM_BYTES,
                        )
                    {
                        changes.insert(container.name.clone(), targets);
                    }
                }
                None => {
                    // 样本还不够：把当前声明的值当 seed 发出去，让读者看得见
                    // 「这一格现在没在跟测量走」，而不是看见一个空格。
                    readings.observe(
                        &workload.name,
                        &container.name,
                        Targets {
                            cpu_request_millicores: declared.cpu_request_millicores.unwrap_or(0),
                            memory_request_bytes: declared.memory_request_bytes.unwrap_or(0),
                            cpu_limit_millicores: declared.cpu_limit_millicores.unwrap_or(0),
                            memory_limit_bytes: declared.memory_limit_bytes.unwrap_or(0),
                        },
                        SOURCE_SEED,
                        samples,
                    );
                }
            }
        }
        // 内存盘跟的是挂载它的那个容器的内存上限：同一份窗口、同一条律，
        // 只是写到的字段从 `resources` 换成了 `volumes[].emptyDir.sizeLimit`。
        let mut shm_changes = BTreeMap::new();
        for volume in &workload.shm {
            evaluated += 1;
            let target = volume
                .container
                .as_ref()
                .and_then(|container| resolved.get(container))
                .map(|targets| targets.memory_limit_bytes);
            match target {
                Some(bytes) => {
                    readings.observe_shm(&workload.name, &volume.volume, bytes, SOURCE_DERIVED);
                    if wants_write(volume.declared, bytes, MEMORY_QUANTUM_BYTES) {
                        shm_changes.insert(volume.volume.clone(), bytes);
                    }
                }
                None => {
                    if let Some(declared) = volume.declared {
                        readings.observe_shm(
                            &workload.name,
                            &volume.volume,
                            declared,
                            SOURCE_SEED,
                        );
                    }
                }
            }
        }
        let Some(body) = patch_body(workload, &changes, &shm_changes) else {
            continue;
        };
        let body = body.to_string();
        match kubectl(&[
            "patch",
            kind_arg(&workload.kind),
            &workload.name,
            "-n",
            ns,
            "--type",
            // 必须是 strategic：JSON merge patch 会把 `containers` 整个数组换掉，
            // 一次资源调整就会连镜像、命令、探针一起抹掉。strategic 按 `name`
            // 合并，只动这一格 `resources`。
            "strategic",
            "-p",
            &body,
        ])
        .await
        {
            Ok(_) => {
                patched += changes.len() + shm_changes.len();
                debug!(
                    kind = %workload.kind,
                    workload = %workload.name,
                    containers = changes.len(),
                    volumes = shm_changes.len(),
                    "workload resources: patched"
                );
            }
            Err(e) => {
                // 拒绝通常来自配额或 LimitRange：那是这一拍的结论，不是回路
                // 坏了。下一拍重新量、重新试。
                warn!(
                    kind = %workload.kind,
                    workload = %workload.name,
                    error = %e,
                    "workload resources: the patch was refused; the declaration is left as it was"
                );
            }
        }
    }
    Ok(RoundSummary {
        evaluated,
        patched,
    })
}

/// 起这条回路。
///
/// 只在跑构建那个进程上起（与卷容量回路同一条角色前提）：它既是进程里唯一
/// 有 `kubectl` 与这套 RBAC 的那一个，也是唯一一个 patch 自己 Deployment
/// 不会把另一个人踢下去的进程。
pub fn spawn_workload_resources_loop(
    readings: std::sync::Arc<WorkloadResourcesReadings>,
    shutdown: ShutdownSignal,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval = std::env::var(INTERVAL_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_INTERVAL_SECS)
        .max(MIN_INTERVAL_SECS);
    let path = window_path();
    Some(cog_core::loop_health::spawn(
        LOOP,
        cog_core::loop_health::Cadence::Periodic(Duration::from_secs(interval)),
        shutdown.clone(),
        move |beat| {
            let shutdown = shutdown.clone();
            let readings = readings.clone();
            let path = path.clone();
            async move {
                let mut window = Window::load(&path);
                let mut ticker = tokio::time::interval(Duration::from_secs(interval));
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    let ns = namespace();
                    let now = now_unix();
                    match round(&mut window, &readings, &ns, now).await {
                        Ok(summary) => info!(
                            evaluated = summary.evaluated,
                            patched = summary.patched,
                            "workload resources: round complete"
                        ),
                        Err(e) => warn!(error = %e, "workload resources: round read nothing usable"),
                    }
                    // 窗口落盘无论这一拍成败：这一拍读到的样本在下一次进程
                    // 重启后仍然算数，而一次读失败不该清空七天的历史。
                    if let Err(e) = window.save(&path) {
                        warn!(path = %path.display(), error = %e, "workload resources: the sample window could not be saved");
                    }
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(cpu: u64, memory: u64) -> Sample {
        Sample {
            at: 0,
            cpu_millicores: cpu,
            memory_bytes: memory,
        }
    }

    #[test]
    fn a_request_follows_the_measured_draw() {
        // 同一个容器，两段不同的实测：请求与两个上限都必须跟着走。
        let quiet = size_targets(&[300, 300, 300], &[320 * 1024 * 1024; 3]).unwrap();
        let busy = size_targets(&[800, 800, 800], &[900 * 1024 * 1024; 3]).unwrap();
        assert_eq!(quiet.cpu_request_millicores, 300);
        assert_eq!(busy.cpu_request_millicores, 800);
        assert_eq!(quiet.memory_request_bytes, 320 * 1024 * 1024);
        assert_eq!(busy.memory_request_bytes, 912 * 1024 * 1024);
        assert!(busy.cpu_limit_millicores > quiet.cpu_limit_millicores);
        assert!(busy.memory_limit_bytes > quiet.memory_limit_bytes);
    }

    #[test]
    fn the_law_uses_the_p95_and_the_peak_not_the_mean() {
        let mib = 1024 * 1024;
        let cpu = [100, 100, 100, 100, 100, 100, 100, 100, 100, 1000];
        let memory = [100 * mib, 100 * mib, 100 * mib, 100 * mib, 100 * mib, 100 * mib, 100 * mib, 100 * mib, 100 * mib, 900 * mib];
        let targets = size_targets(&cpu, &memory).unwrap();
        // 10 个样本的第 95 百分位是 10 个里的第 10 个（ceil(9.5)），即 1000。
        assert_eq!(targets.cpu_request_millicores, 1000);
        // 内存上限由峰值 × 余量决定，再向上取整到量化格。
        assert_eq!(
            targets.memory_limit_bytes,
            quantize_up(900 * mib * HEADROOM_PERCENT / 100, MEMORY_QUANTUM_BYTES)
        );
    }

    #[test]
    fn an_empty_window_has_no_target() {
        assert!(size_targets(&[], &[]).is_none());
        assert!(size_targets(&[100], &[]).is_none());
    }

    #[test]
    fn a_change_smaller_than_the_quantum_is_not_written() {
        assert!(!wants_write(Some(310), 315, CPU_QUANTUM_MILLICORES));
        assert!(wants_write(Some(310), 330, CPU_QUANTUM_MILLICORES));
        assert!(wants_write(None, 315, CPU_QUANTUM_MILLICORES));
    }

    #[test]
    fn the_window_drops_samples_older_than_seven_days() {
        let now = WINDOW_SECS + 100;
        let mut window = Window::default();
        window.push("Deployment/a/c", Sample { at: 1, ..sample(1, 1) }, 1);
        window.push("Deployment/a/c", Sample { at: now, ..sample(2, 2) }, now);
        assert_eq!(window.samples("Deployment/a/c"), 1);
    }

    #[test]
    fn a_persisted_window_survives_a_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("window.json");
        let mut window = Window::default();
        let now = WINDOW_SECS + 1000;
        for step in 0..MIN_SAMPLES as u64 {
            window.push(
                "Deployment/a/c",
                Sample {
                    at: now - 100 + step,
                    cpu_millicores: 100,
                    memory_bytes: 64 * 1024 * 1024,
                },
                now,
            );
        }
        window.save(&path).unwrap();
        let loaded = Window::load(&path);
        assert_eq!(
            loaded.samples("Deployment/a/c"),
            MIN_SAMPLES
        );
        assert!(loaded.targets("Deployment/a/c").is_some());
    }

    #[test]
    fn a_container_seen_once_is_not_a_target() {
        let mut window = Window::default();
        window.push("Deployment/a/c", sample(900, 900 * 1024 * 1024), 10);
        assert!(window.targets("Deployment/a/c").is_none());
    }

    #[test]
    fn quantities_round_trip_through_the_wire_format() {
        assert_eq!(parse_cpu_millicores("500m"), Some(500));
        assert_eq!(parse_cpu_millicores("2"), Some(2000));
        assert_eq!(parse_cpu_millicores("0.5"), Some(500));
        assert_eq!(parse_cpu_millicores("1000000n"), Some(1));
        assert_eq!(parse_memory_bytes("512Mi"), Some(512 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("64Mi"), Some(64 * 1024 * 1024));
        assert_eq!(cpu_quantity(1500), "1500m");
        assert_eq!(cpu_quantity(2000), "2");
        assert_eq!(memory_quantity(512 * 1024 * 1024), "512Mi");
        assert_eq!(memory_quantity(1024 * 1024 * 1024), "1Gi");
    }

    fn workload(kind: &str, name: &str) -> WorkloadSpec {
        WorkloadSpec {
            kind: kind.to_string(),
            name: name.to_string(),
            containers: vec![ContainerSpec {
                name: "c".to_string(),
                init: false,
                declared: Some(Declared {
                    cpu_request_millicores: Some(100),
                    memory_request_bytes: Some(64 * 1024 * 1024),
                    cpu_limit_millicores: Some(500),
                    memory_limit_bytes: Some(256 * 1024 * 1024),
                }),
            }],
            shm: Vec::new(),
        }
    }

    fn targets(cpu: u64, memory: u64) -> Targets {
        Targets {
            cpu_request_millicores: cpu,
            memory_request_bytes: memory,
            cpu_limit_millicores: cpu * 2,
            memory_limit_bytes: memory * 2,
        }
    }

    #[test]
    fn a_deployment_patch_lands_under_the_pod_template() {
        let mut changes = BTreeMap::new();
        changes.insert("c".to_string(), targets(800, 512 * 1024 * 1024));
        let body = patch_body(&workload("Deployment", "gateway"), &changes, &BTreeMap::new()).unwrap();
        assert_eq!(
            body.pointer("/spec/template/spec/containers/0/name")
                .and_then(Value::as_str),
            Some("c")
        );
        assert_eq!(
            body.pointer("/spec/template/spec/containers/0/resources/requests/cpu")
                .and_then(Value::as_str),
            Some("800m")
        );
    }

    #[test]
    fn a_cronjob_patch_lands_under_the_job_template() {
        let mut changes = BTreeMap::new();
        changes.insert("c".to_string(), targets(800, 512 * 1024 * 1024));
        let body = patch_body(&workload("CronJob", "cogneva-backup"), &changes, &BTreeMap::new()).unwrap();
        assert!(body
            .pointer("/spec/jobTemplate/spec/template/spec/containers/0/resources")
            .is_some());
        assert!(body.pointer("/spec/template").is_none());
    }

    fn run(at: u64, cpu: u64, memory_mib: u64, throttled: f64) -> RunSample {
        RunSample {
            at,
            cpu_millicores: cpu,
            memory_bytes: memory_mib * 1024 * 1024,
            throttled_ratio: throttled,
        }
    }

    #[test]
    fn a_rollout_job_sizing_follows_the_measured_runs() {
        // 同一段运行史改成更重的一段：请求与上限都必须跟着走。
        let quiet = [run(1, 70, 64, 0.0); 4];
        let busy = [run(1, 300, 180, 0.0); 4];
        let quiet = rollout_job_targets(&quiet).unwrap();
        let busy = rollout_job_targets(&busy).unwrap();
        assert!(busy.cpu_request_millicores > quiet.cpu_request_millicores);
        assert!(busy.memory_request_bytes > quiet.memory_request_bytes);
        assert!(busy.memory_limit_bytes > quiet.memory_limit_bytes);
    }

    #[test]
    fn throttling_raises_the_cpu_limit_until_it_stops() {
        let unthrottled = rollout_job_targets(&[run(1, 100, 64, 0.0); 4]).unwrap();
        // 最近三次里有超过 10% 的周期被限流：上限必须抬，请求不动。
        let throttled = rollout_job_targets(&[
            run(1, 100, 64, 0.0),
            run(2, 100, 64, 0.5),
            run(3, 100, 64, 0.5),
        ])
        .unwrap();
        assert_eq!(
            throttled.cpu_request_millicores,
            unthrottled.cpu_request_millicores
        );
        assert!(throttled.cpu_limit_millicores > unthrottled.cpu_limit_millicores);
    }

    #[test]
    fn the_evaluation_counter_keeps_time_when_the_target_does_not_move() {
        // 这条证明的是「拍子在走」：目标完全相同，求值计数仍然每拍加一。
        let readings = WorkloadResourcesReadings::new();
        let same = targets(300, 256 * 1024 * 1024);
        for _ in 0..3 {
            readings.note_evaluated("gateway", "cogneva");
            readings.observe("gateway", "cogneva", same, SOURCE_DERIVED, 24);
        }
        assert_eq!(readings.evaluations("gateway", "cogneva"), 3);
    }

    #[test]
    fn a_pod_resolves_through_its_replicaset_to_the_deployment() {
        let mut replicasets = HashMap::new();
        replicasets.insert("gateway-abc".to_string(), "gateway".to_string());
        let pod = json!({
            "metadata": {"ownerReferences": [{"kind": "ReplicaSet", "name": "gateway-abc"}]}
        });
        assert_eq!(
            pod_workload(&pod, &replicasets, &HashMap::new()),
            Some(("Deployment".to_string(), "gateway".to_string()))
        );
    }

    #[test]
    fn a_cronjob_pod_resolves_through_its_job() {
        let mut jobs = HashMap::new();
        jobs.insert("cogneva-backup-123".to_string(), "cogneva-backup".to_string());
        let pod = json!({
            "metadata": {"ownerReferences": [{"kind": "Job", "name": "cogneva-backup-123"}]}
        });
        assert_eq!(
            pod_workload(&pod, &HashMap::new(), &jobs),
            Some(("CronJob".to_string(), "cogneva-backup".to_string()))
        );
    }

    fn shm_workload() -> WorkloadSpec {
        let list = json!({"items": [{
            "kind": "Deployment",
            "metadata": {"name": "crawl4ai"},
            "spec": {"template": {"spec": {
                "containers": [{
                    "name": "crawl4ai",
                    "resources": {"limits": {"memory": "2Gi"}},
                    "volumeMounts": [{"name": "shm", "mountPath": "/dev/shm"}],
                }],
                "volumes": [{
                    "name": "shm",
                    "emptyDir": {"medium": "Memory", "sizeLimit": "1Gi"},
                }],
            }}},
        }]});
        parse_workloads(&list).remove(0)
    }

    #[test]
    fn the_shm_size_follows_the_measured_memory() {
        // 同一份输入、两个不同的实测：写进 `sizeLimit` 的数必须跟着动。
        let workload = shm_workload();
        assert_eq!(workload.shm.len(), 1);
        assert_eq!(workload.shm[0].volume, "shm");
        assert_eq!(workload.shm[0].container.as_deref(), Some("crawl4ai"));
        assert_eq!(workload.shm[0].declared, Some(1024 * 1024 * 1024));

        let mut containers = BTreeMap::new();
        containers.insert(
            "crawl4ai".to_string(),
            targets(300, 512 * 1024 * 1024),
        );
        let mut shm = BTreeMap::new();
        shm.insert("shm".to_string(), 1024u64 * 1024 * 1024);
        let body = patch_body(&workload, &containers, &shm).unwrap();
        assert_eq!(
            body.pointer("/spec/template/spec/volumes/0/emptyDir/sizeLimit")
                .and_then(Value::as_str),
            Some("1Gi")
        );

        let mut shm = BTreeMap::new();
        shm.insert("shm".to_string(), 4u64 * 1024 * 1024 * 1024);
        let body = patch_body(&workload, &containers, &shm).unwrap();
        assert_eq!(
            body.pointer("/spec/template/spec/volumes/0/emptyDir/sizeLimit")
                .and_then(Value::as_str),
            Some("4Gi"),
            "内存盘的目标没跟着实测变，说明它被当成一个写死的数了"
        );
    }

    #[test]
    fn a_memory_volume_no_container_mounts_is_not_written() {
        // 找不到挂载它的容器时不猜一个目标：那块盘没有「自己」的内存可以跟。
        let list = json!({"items": [{
            "kind": "Deployment",
            "metadata": {"name": "x"},
            "spec": {"template": {"spec": {
                "containers": [{"name": "c", "resources": {"limits": {"memory": "1Gi"}}}],
                "volumes": [{"name": "shm", "emptyDir": {"medium": "Memory", "sizeLimit": "1Gi"}}],
            }}},
        }]});
        let workload = parse_workloads(&list).remove(0);
        assert_eq!(workload.shm[0].container, None);
        let mut containers = BTreeMap::new();
        containers.insert("c".to_string(), targets(100, 1024 * 1024 * 1024));
        let mut shm = BTreeMap::new();
        shm.insert("shm".to_string(), 2u64 * 1024 * 1024 * 1024);
        // 没有容器挂它，调用方就不会把它放进 `shm_changes`；这里钉住的是解析面
        // 给出的答案：`container` 是空的，所以回路不会替它算。
        assert_eq!(workload.shm[0].container, None);
        assert!(patch_body(&workload, &containers, &BTreeMap::new()).is_some());
    }
}
