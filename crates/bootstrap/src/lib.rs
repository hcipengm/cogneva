//! k8m 多集群管理计划（审计 2.5.2）+ backend 自动部署规划（审计 2.5.3）。
//!
//! `ManagementPlan` 是 K3s 与 K8s 共用的声明式部署抽象：AI 只生成/调用
//! 统一计划，框架按环境标签把同一计划渲染为 Helm values（profile）并同步
//! 到目标集群。backend 选择规则内聚于此，无需业务代码改动。

pub mod cli;

use serde::{Deserialize, Serialize};

/// 集群供给发行版：元启动新建集群时装什么。`K3s` = 装 K3s（单节点或多节点）；
/// `Kubespray` = 用 kubespray 官方镜像新建标准 Kubernetes（即 K8s）。标准 K8s
/// 也可能由用户自行搭好、元启动只复用——那种情况不经过这里的供给选择。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Distro {
    /// K3s（零决策、单二进制，默认）。
    #[default]
    K3s,
    /// kubespray 新建标准 Kubernetes。
    Kubespray,
}

/// 三种部署 profile 的名字，各写一次。
///
/// 同一个名字同时是 Helm 的 `deploy/helm/cogneva/profiles/<name>.yaml`、CI 预渲染
/// 的 `deploy/rendered/<name>/`、以及计划里部署分支的序列化值。这几处只要有一处
/// 拼错，元启动就会按一个渲染器没产出过的名字去找目录：读出来是空的，而「目录不
/// 存在」和「这个 profile 没有渲染产物」在调用点长得一样，于是它安静地退回默认
/// 分支，部署到的是另一种形态的集群。
pub const PROFILE_K3S_SINGLE: &str = "k3s-single";
/// 见 [`PROFILE_K3S_SINGLE`]。
pub const PROFILE_K3S_MULTI: &str = "k3s-multi";
/// 见 [`PROFILE_K3S_SINGLE`]。
pub const PROFILE_K8S_STANDARD: &str = "k8s-standard";

/// 全部 profile 名，顺序即渲染器产出顺序。
pub const PROFILE_NAMES: [&str; 3] = [PROFILE_K3S_SINGLE, PROFILE_K3S_MULTI, PROFILE_K8S_STANDARD];

/// 部署分支（与 main.rs 的部署 profile 同名对齐）。它描述"往什么形态的集群上
/// 部署应用"，序列化名与 Helm profile / 渲染目录一一对应：`k3s-single` /
/// `k3s-multi` / `k8s-standard`。前两者由 K3s 供给产出，`K8sStandard` 由
/// kubespray 新建的标准 K8s 或用户既有标准集群产出。
///
/// 序列化走 [`PlanBranch::name`] 而不是 `#[serde(rename = …)]`：那个属性只能写字
/// 面量，会和 [`PROFILE_NAMES`] 各自演进。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanBranch {
    /// K3s 单节点。
    K3sSingle,
    /// K3s 多节点（server + agents）。
    K3sMulti,
    /// 标准 Kubernetes（kubeadm / kubespray / EKS 等）。
    K8sStandard,
}

impl PlanBranch {
    /// 这条分支对应的 profile 名，也就是它在计划里序列化出来的字面量。
    pub fn name(self) -> &'static str {
        match self {
            PlanBranch::K3sSingle => PROFILE_K3S_SINGLE,
            PlanBranch::K3sMulti => PROFILE_K3S_MULTI,
            PlanBranch::K8sStandard => PROFILE_K8S_STANDARD,
        }
    }

    /// [`PlanBranch::name`] 的反向：认不出的名字返回 `None`，由调用点决定是拒绝
    /// 还是回退。
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            PROFILE_K3S_SINGLE => Some(PlanBranch::K3sSingle),
            PROFILE_K3S_MULTI => Some(PlanBranch::K3sMulti),
            PROFILE_K8S_STANDARD => Some(PlanBranch::K8sStandard),
            _ => None,
        }
    }
}

impl Serialize for PlanBranch {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for PlanBranch {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        PlanBranch::from_name(&raw)
            .ok_or_else(|| serde::de::Error::unknown_variant(&raw, &PROFILE_NAMES))
    }
}

/// 计划元信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanMeta {
    pub name: String,
    /// 环境标签（如 "prod" / "edge" / "dev"），同步目标按标签匹配。
    pub environment: String,
}

/// 单个 backend 的声明。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSpec {
    /// postgres / redis / qdrant / nats / mysql / meilisearch
    pub kind: String,
    pub enabled: bool,
    /// 选择理由（审计与可解释性）。
    pub reason: String,
}

/// 同步目标集群。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncTarget {
    pub cluster: String,
    /// kubeconfig 路径；空 = 当前 context。
    #[serde(default)]
    pub kubeconfig: String,
}

/// k8m 多集群管理计划。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementPlan {
    pub api_version: String,
    pub kind: String,
    pub metadata: PlanMeta,
    pub spec: PlanSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanSpec {
    /// The namespace the deployment declares. The plan is what the sync path
    /// hands to helm, so it carries the name the chart declares instead of
    /// writing one of its own.
    pub namespace: String,
    pub branch: PlanBranch,
    pub image_tag: String,
    pub gateway_replicas: u32,
    pub backends: Vec<BackendSpec>,
    #[serde(default)]
    pub sync_targets: Vec<SyncTarget>,
}

/// 硬件画像（与 main.rs 探测结构对齐的精简版）。
#[derive(Debug, Clone, Copy)]
pub struct HardwareProfile {
    pub memory_gb: u64,
    pub cpu_cores: u32,
    pub nodes: u32,
}

/// 分支规则：kubespray 供给的是标准 Kubernetes → `K8sStandard`；K3s 供给按
/// 规模——内存 < 2GB 或单节点 → K3s 单节点，多节点高配 → K3s 多节点
/// （server + agents）。用户既有标准集群同样落 `K8sStandard`（部署探测阶段
/// 判定，不经此函数）。
pub fn decide_branch(hw: &HardwareProfile, distro: Distro) -> PlanBranch {
    match distro {
        Distro::Kubespray => PlanBranch::K8sStandard,
        Distro::K3s => {
            if hw.memory_gb < 2 || hw.nodes <= 1 {
                PlanBranch::K3sSingle
            } else {
                PlanBranch::K3sMulti
            }
        }
    }
}

/// 是否多节点形态：K3s 多节点与标准 Kubernetes 都按多节点/生产形态配事件总线。
fn is_multi_node(branch: PlanBranch) -> bool {
    matches!(branch, PlanBranch::K3sMulti | PlanBranch::K8sStandard)
}

/// backend 自动选择规则（审计 2.5.3）：
/// - postgres：持久层必选；
/// - redis：单节点/低内存消息与配额缓存；
/// - nats：多节点形态启用 JetStream 事件总线（K3s 多节点、标准 K8s），单节点由 Redis Streams 覆盖；
/// - qdrant：内存 ≥ 4GB 才本地部署，否则禁用（走外部向量库）；
/// - mysql/meilisearch：默认禁用，显式需要时由 AI 在计划里开启。
pub fn decide_backends(hw: &HardwareProfile, branch: PlanBranch) -> Vec<BackendSpec> {
    let multi = is_multi_node(branch);
    vec![
        BackendSpec {
            kind: "postgres".into(),
            enabled: true,
            reason: "持久层必选：审计链/配额/会话状态".into(),
        },
        BackendSpec {
            kind: "redis".into(),
            enabled: true,
            reason: "消息队列与配额缓存的轻量默认".into(),
        },
        BackendSpec {
            kind: "nats".into(),
            enabled: multi,
            reason: if multi {
                "多节点形态：JetStream 事件总线".into()
            } else {
                "单节点形态：Redis Streams 已覆盖".into()
            },
        },
        BackendSpec {
            kind: "qdrant".into(),
            enabled: hw.memory_gb >= 4,
            reason: if hw.memory_gb >= 4 {
                "内存充足：本地向量库".into()
            } else {
                "内存 < 4GB：改用外部向量服务".into()
            },
        },
        BackendSpec {
            kind: "mysql".into(),
            enabled: false,
            reason: "默认禁用；需要 MySQL 协议时显式开启".into(),
        },
        BackendSpec {
            kind: "meilisearch".into(),
            enabled: false,
            reason: "默认禁用；需要全文检索时显式开启".into(),
        },
    ]
}

impl ManagementPlan {
    /// 依据硬件画像、环境标签与供给发行版生成声明式计划。
    pub fn for_environment(
        environment: impl Into<String>,
        hw: &HardwareProfile,
        distro: Distro,
        namespace: impl Into<String>,
    ) -> Self {
        let environment = environment.into();
        let branch = decide_branch(hw, distro);
        Self {
            api_version: "k8m.cogneva/v1alpha1".into(),
            kind: "ManagementPlan".into(),
            metadata: PlanMeta {
                name: format!("cogneva-{}", environment),
                environment,
            },
            spec: PlanSpec {
                namespace: namespace.into(),
                branch,
                image_tag: env!("CARGO_PKG_VERSION").into(),
                gateway_replicas: match branch {
                    PlanBranch::K3sSingle => 1,
                    PlanBranch::K3sMulti | PlanBranch::K8sStandard => 3,
                },
                backends: decide_backends(hw, branch),
                sync_targets: Vec::new(),
            },
        }
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    pub fn from_yaml(s: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(s)
    }

    /// 渲染为 Helm values（deploy/helm/cogneva）：统一计划的执行视图。
    pub fn to_helm_values(&self) -> serde_json::Value {
        let backend = |kind: &str| {
            self.spec
                .backends
                .iter()
                .find(|b| b.kind == kind)
                .map(|b| b.enabled)
                .unwrap_or(false)
        };
        serde_json::json!({
            "image": { "tag": self.spec.image_tag },
            "gateway": { "replicas": self.spec.gateway_replicas },
            "backends": {
                "postgres": { "enabled": backend("postgres") },
                "redis": { "enabled": backend("redis") },
                "qdrant": { "enabled": backend("qdrant") },
                "nats": { "enabled": backend("nats") },
            },
        })
    }

    /// 将计划同步到所有目标集群：每目标生成 values 文件并执行
    /// `helm upgrade --install`。返回每个目标的结果。
    pub async fn sync(&self) -> Vec<(String, anyhow::Result<()>)> {
        let mut results = Vec::new();
        let values =
            serde_yaml::to_string(&self.to_helm_values()).unwrap_or_else(|_| "{}".to_string());
        for target in &self.spec.sync_targets {
            let r = self.sync_target(target, &values).await;
            results.push((target.cluster.clone(), r));
        }
        results
    }

    async fn sync_target(&self, target: &SyncTarget, values: &str) -> anyhow::Result<()> {
        let tmp = std::env::temp_dir().join(format!("cogneva-plan-values-{}.yaml", target.cluster));
        tokio::fs::write(&tmp, values).await?;

        let mut args = vec![
            "upgrade".to_string(),
            "--install".to_string(),
            self.metadata.name.clone(),
            "deploy/helm/cogneva".to_string(),
            "-n".to_string(),
            self.spec.namespace.clone(),
            "--create-namespace".to_string(),
            "-f".to_string(),
            tmp.to_string_lossy().to_string(),
        ];
        if !target.kubeconfig.is_empty() {
            args.push("--kubeconfig".to_string());
            args.push(target.kubeconfig.clone());
        }
        let output = tokio::process::Command::new("helm")
            .args(&args)
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "helm sync to {} failed: {}",
                target.cluster,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }
}

/// What a node keeps for everything that is not a pod, and when it starts
/// taking memory back from pods.
///
/// K3s ships `--eviction-hard=imagefs.available<5%,nodefs.available<5%`: it
/// replaces the whole flag, so the upstream memory threshold never reaches the
/// kubelet and a node with a full page cache and no free memory evicts nothing
/// -- the kernel reclaims first and pages until it thrashes. The node also
/// starts with `allocatable == capacity` (no reservation at all), so the
/// scheduler is free to fill every byte the host's own processes need. Both
/// facts are the same defect: nothing on the node distinguishes the memory a
/// pod may take from the memory the machine needs to keep running.
///
/// The numbers are derived from the node's own readings rather than chosen:
/// the memory reservation is the documented Kubernetes table for system
/// daemons applied to this node's total, the CPU reservation is that table's
/// per-core rule, and the eviction threshold is 5% of the node -- the same
/// share K3s itself uses for the file-system signals -- with the upstream
/// 100Mi floor so a small node is not left with a threshold that means nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeQos {
    /// Reserved for the non-pod processes, in millicores.
    pub reserved_cpu_milli: u64,
    /// Reserved for the non-pod processes, in MiB.
    pub reserved_memory_mb: u64,
    /// `memory.available` at or below which the kubelet evicts, in MiB.
    pub eviction_memory_mb: u64,
}

/// Memory a node keeps for non-pod processes, from the documented table:
/// 255Mi below 1GiB, then 25% / 20% / 10% / 6% / 2% of the total as the node
/// grows. Rounded up to 100MiB: the reading behind it is a `/proc/meminfo`
/// total, and rounding up never reserves less than the table asks for.
fn reserved_memory_mb(mem_total_mb: u64) -> u64 {
    match mem_total_mb {
        0..=1023 => 255,
        _ => {
            let percent = match mem_total_mb {
                1024..=4095 => 25,
                4096..=16383 => 20,
                16384..=65535 => 10,
                65536..=262143 => 6,
                _ => 2,
            };
            // percent/100 of the total, rounded up to the next 100MiB.
            (mem_total_mb * percent).div_ceil(10_000) * 100
        }
    }
}

/// CPU a node keeps for non-pod processes: 6% of the first core, 1% of the
/// second, 0.5% of the next two, 0.25% of the rest. Millicores are integers,
/// so 0.25% is rounded up to 3m per core and the total to the next 10m.
fn reserved_cpu_milli(cpu_cores: u32) -> u64 {
    let cores = cpu_cores as u64;
    let milli = 60 * cores.min(1)
        + 10 * cores.min(2).saturating_sub(1)
        + 5 * (cores.min(4).saturating_sub(2))
        + 3 * cores.saturating_sub(4);
    milli.div_ceil(10) * 10
}

/// The reservations and eviction threshold this node should run with.
pub fn node_qos(mem_total_mb: u64, cpu_cores: u32) -> NodeQos {
    let share = mem_total_mb.div_ceil(20);
    NodeQos {
        reserved_cpu_milli: reserved_cpu_milli(cpu_cores),
        reserved_memory_mb: reserved_memory_mb(mem_total_mb),
        // Never below the upstream 100Mi default, rounded up like the
        // reservation so both numbers read as deliberate.
        eviction_memory_mb: share.max(100).div_ceil(100) * 100,
    }
}

/// Render the K3s config file that carries the node's QoS settings.
///
/// Every eviction signal is written out together, including the two K3s
/// already sets and the two inode signals: changing the flag replaces the
/// default set rather than extending it, and a signal left out is silently
/// zero -- an `imagefs` with no threshold is a disk that fills up without the
/// kubelet noticing.
pub fn k3s_qos_config_yaml(qos: &NodeQos) -> String {
    format!(
        "kubelet-arg:\n  - \"system-reserved=cpu={}m,memory={}Mi\"\n  - \"eviction-hard=memory.available<{}Mi,nodefs.available<5%,imagefs.available<5%,nodefs.inodesFree<5%,imagefs.inodesFree<5%\"\n",
        qos.reserved_cpu_milli, qos.reserved_memory_mb, qos.eviction_memory_mb
    )
}

/// Where the drop-in that carries this node's disk writeback watermarks lands.
///
/// `/etc/sysctl.d` rather than a line in `/etc/sysctl.conf`, because the number
/// is a reading of the device and changes when the device does: a file this
/// system owns can be rewritten by whoever takes the reading, and the operator's
/// own `/etc/sysctl.conf` -- applied after this directory by `sysctl --system`,
/// so a value put there still overrules this one -- is left untouched.
pub const NODE_IO_SYSCTL_FILE: &str = "/etc/sysctl.d/99-cogneva-node-io.conf";

/// Seconds of the device's own sustained write the node lets pile up before the
/// kernel starts writing back in the background.
// cap: bound
const DIRTY_BACKGROUND_SECONDS: u64 = 2;

/// The same, for the watermark at which a writer is made to write back before it
/// can dirty more.
// cap: bound
const DIRTY_FORCE_SECONDS: u64 = 5;

/// Floor for the background watermark. Below the speed the flusher threads can
/// drain, the kernel starts writing back while the writers are still faster than
/// it, so the queue this bound exists to shape never forms and the two points
/// collapse into a permanent trickle. These floors are the smallest pair that
/// still lets a burst finish.
// cap: bound
const DIRTY_BACKGROUND_FLOOR_BYTES: u64 = 32 * 1024 * 1024;

/// Floor for the force watermark, above the background floor by construction.
// cap: bound
const DIRTY_FLOOR_BYTES: u64 = 64 * 1024 * 1024;

/// The unflushed data never grows past this share of RAM: one twentieth, a
/// twentieth of the ceiling below.
///
/// A cap and not the derivation -- the derivation is the device's throughput --
/// but the cap is memory's only honest part here. RAM does not decide how much
/// unflushed data is affordable; it decides only that a device so fast the
/// throughput-derived number would be enormous must not be allowed to hold the
/// whole node's memory dirty, which would turn a disk bound into an OOM.
// cap: bound
const DIRTY_MAX_RAM_DIVISOR: u64 = 20;

/// Hard ceiling on either watermark, for a node whose RAM would put the share
/// above it.
// cap: bound
const DIRTY_CEILING_BYTES: u64 = 1024 * 1024 * 1024;

/// The dirty-page watermarks this node should run with, in bytes.
///
/// The two numbers are read from the device's own sustained write throughput,
/// not from a share of RAM. `vm.dirty_ratio` is a percentage of memory, and the
/// unflushed data a node can afford has nothing to do with how much memory it
/// has: what it costs is how long the device takes to write that data back,
/// which is throughput times seconds. Read against this deployment, the default
/// 20% of 64 GiB is 12.5 GiB, which at the 33-52 MB/s this node's device writes
/// while it is working is four to seven minutes of unflushed data -- and every
/// second a reader waits behind that flush is the stall the ratio produces. The
/// bytes form says the same thing in the unit that is actually bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeDiskIo {
    /// The device's sustained (post-cache) write throughput, bytes per second.
    pub sustained_write_bytes_per_sec: u64,
    /// `vm.dirty_background_bytes`: where the kernel begins writing back in the
    /// background, without blocking a writer.
    pub dirty_background_bytes: u64,
    /// `vm.dirty_bytes`: where a writer is made to write back before it can
    /// dirty more, which is what a build pays as a stall.
    pub dirty_bytes: u64,
}

/// The watermarks for a node whose disk writes back at
/// `sustained_write_bytes_per_sec`.
///
/// `mem_total_mb` is read only for the cap: the amount consumed here is the
/// device's throughput, and RAM's part is to keep the pile from growing into a
/// large share of it.
pub fn node_disk_io(sustained_write_bytes_per_sec: u64, mem_total_mb: u64) -> NodeDiskIo {
    let ram_bytes = mem_total_mb.saturating_mul(1024 * 1024);
    // Floored as well as capped, so the pair below it still has the floors to
    // fall back to on a node whose RAM is smaller than its own floors.
    let ceiling = DIRTY_CEILING_BYTES
        .min(ram_bytes / DIRTY_MAX_RAM_DIVISOR)
        .max(DIRTY_FLOOR_BYTES);
    let force = (sustained_write_bytes_per_sec * DIRTY_FORCE_SECONDS)
        .max(DIRTY_FLOOR_BYTES)
        .min(ceiling);
    // Capped at half the force point as well as the ceiling, so the two stay
    // distinct at every input: a background point at or above the force point is
    // a writeback that only starts once it is already too late, which is the
    // same as having no background point at all.
    let background = (sustained_write_bytes_per_sec * DIRTY_BACKGROUND_SECONDS)
        .max(DIRTY_BACKGROUND_FLOOR_BYTES)
        .min(force / 2);
    NodeDiskIo {
        sustained_write_bytes_per_sec,
        dirty_background_bytes: background,
        dirty_bytes: force,
    }
}

/// Where the throughput a node's watermarks were read from came from.
///
/// Carried alongside the pair, because the pair is a function of the window it
/// was read over and of how much of that window the device spent working. A
/// rate averaged over a window the device spent mostly idle measures the idle
/// time rather than the device, and the two readings yield watermarks that
/// differ by as much as the device was underworked. The install-time reading is
/// the worst case of this: it is taken while the page cache absorbs the writes,
/// so it sees the device's burst and not its sustained rate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThroughputReading {
    /// The whole disk the rate was read on. The watermark is global to the node,
    /// so this names whichever device the reading was taken on rather than the
    /// one the filesystem is mounted from.
    pub device: String,
    /// The window the rate was averaged over.
    pub window: String,
    /// How much of that window the device was busy, in whole percent.
    pub busy_percent: u8,
    /// When the reading was taken, UTC.
    pub measured_at: String,
}

/// The sysctl drop-in that carries the watermarks, and the reading they came
/// from.
///
/// The reading goes in as a comment block above the values, because a file that
/// carries only the numbers cannot be checked against a later reading: two files
/// that disagree are then indistinguishable from two files read over different
/// windows, which is the routine case for a number that moves with the device.
/// A reader comparing a delivered file against a freshly derived one therefore
/// parses the values and leaves the comment block out of the comparison; a
/// comparison of whole files reports a difference every time the device is
/// re-measured, including when nothing about the watermarks changed.
///
/// The two ratio keys are written as zero beside the byte keys rather than left
/// out. The kernel keeps one of each pair and reads the other back as zero once
/// either is written, so `vm.dirty_bytes` alone already clears `vm.dirty_ratio`
/// -- but the file is also what a person reads to see the node's writeback
/// policy, and half a pair that looks unset reads as if the ratios were still in
/// play. Both halves stated is what makes the answer whole.
///
/// The ratios come **first**, and that order is load-bearing: writing either
/// ratio zeroes the byte key of its pair. Stated bytes-first, `sysctl --system`
/// exits 0 and prints both byte values while it applies them, and the kernel
/// then lands with `vm.dirty_background_bytes = 0` and `vm.dirty_background_ratio
/// = 0` -- a background threshold of zero bytes, so the flusher starts the
/// moment anything is dirty. The file would state 78 MB and the node would run
/// the opposite policy, and both the file's own text and `vm.dirty_ratio`
/// (which is zero either way) read as if it had worked.
pub fn sysctl_node_io_conf(disk: &NodeDiskIo, reading: &ThroughputReading) -> String {
    format!(
        "# Writeback watermarks read from the device's sustained write throughput,\n\
         # not from a share of RAM. Where that reading came from:\n\
         # device {device}\n\
         # window {window}\n\
         # busy {busy}% of that window\n\
         # measured at {at}\n\
         # sustained write {throughput} bytes/s, read as the rate of\n\
         # node_disk_written_bytes_total over a window whose occupancy is the rate of\n\
         # node_disk_io_time_seconds_total.\n\
         # The ratios are stated before the bytes: writing a ratio zeroes the byte\n\
         # key of its pair, so the reverse order leaves both bytes keys unset.\n\
         vm.dirty_background_ratio = 0\n\
         vm.dirty_ratio = 0\n\
         vm.dirty_background_bytes = {background}\n\
         vm.dirty_bytes = {force}\n",
        device = reading.device,
        window = reading.window,
        busy = reading.busy_percent,
        at = reading.measured_at,
        throughput = disk.sustained_write_bytes_per_sec,
        background = disk.dirty_background_bytes,
        force = disk.dirty_bytes,
    )
}

/// The `key = value` settings a sysctl drop-in states, comments and blank lines
/// dropped.
///
/// The drop-in this system writes carries a comment block naming the reading its
/// numbers came from, so its text moves whenever the device is re-measured even
/// when the watermarks do not. Two files are therefore compared by the settings
/// they state rather than by their bytes: a re-measurement that changed nothing
/// reads as no change, and it is the file that disagrees that reads as one.
pub fn sysctl_settings(conf: &str) -> Vec<(String, String)> {
    conf.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

/// How the settings already on the node disagree with the ones just derived,
/// one sentence per key, empty when they agree.
///
/// This is the read-back face of a write that never overwrites: once the file
/// exists this path stops writing, so nothing else can say whether the file on
/// the node still holds what the derivation would put there -- or whether it was
/// written by hand, to a value this code would not produce.
pub fn sysctl_conf_disagreements(on_node: &str, derived: &str) -> Vec<String> {
    let have: std::collections::BTreeMap<String, String> =
        sysctl_settings(on_node).into_iter().collect();
    let want: std::collections::BTreeMap<String, String> =
        sysctl_settings(derived).into_iter().collect();
    let mut out = Vec::new();
    for (key, value) in &want {
        match have.get(key) {
            Some(held) if held == value => {}
            Some(held) => out.push(format!("{key}: on the node {held}, derived {value}")),
            None => out.push(format!("{key}: missing on the node, derived {value}")),
        }
    }
    for key in have.keys() {
        if !want.contains_key(key) {
            out.push(format!("{key}: on the node but not in the derivation"));
        }
    }
    out
}

/// How long an image may sit unused before the kubelet may reclaim it.
///
/// A week, for three reasons that agree: it is longer than any rollback this
/// system performs (the deployer keeps the previous release minutes old), it is
/// the interval the promotion report runs on, so nothing a report could still be
/// about is reclaimed while that report is being written, and it bounds the
/// store at about a week of releases instead of at whatever the disk allows.
pub const IMAGE_GC_MAX_AGE: &str = "168h";

/// The K3s drop-in that bounds the image store by age.
///
/// Kubelet's only bounded-resource trigger for images is a percentage of its
/// image filesystem: it collects when that filesystem passes
/// `imageGCHighThresholdPercent`, 85% by default. On a single-disk node the
/// image store shares the root filesystem, so that percentage is a share of a
/// filesystem that mostly is not images -- 85% of it is far more free space
/// than the whole store occupies, which means nothing is ever reclaimed and the
/// store grows until someone deletes it by hand.
///
/// The age bound is the trigger that does not depend on the watermark: an image
/// unused for longer than this is reclaimed on every collection pass, whatever
/// the disk says. Two properties of it are worth knowing before setting a value.
/// Kubelet does not collect by age until it has itself been running longer than
/// the age, so a node reclaims nothing for the first stretch after it starts --
/// the same stretch in which it has nothing old to reclaim. And the age is
/// measured from when the image was last used by a container, so an image that
/// was pulled and never run ages from the moment kubelet first saw it.
///
/// This is a file rather than a `kubelet-arg` because `imageMaximumGCAge` has no
/// kubelet flag. Every other image GC setting does, which is what makes the flag
/// the expected home; a `kubelet-arg` naming a flag that does not exist stops
/// the kubelet, and a kubelet that will not start is a node that never comes up.
/// The drop-in directory is the one surface that carries the fields the flags
/// cannot reach: K3s hands it to the kubelet as its config directory, and K3s
/// writes its own defaults there as `00-k3s-defaults.conf`, which states
/// `imageMaximumGCAge: 0s`. Files are merged in lexical order, later files
/// winning, so this one has to sort after that one and before the file K3s
/// copies an operator's `--config` into -- it does both.
pub fn k3s_kubelet_image_gc_dropin_yaml() -> String {
    format!(
        "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\nimageMaximumGCAge: {IMAGE_GC_MAX_AGE}\n"
    )
}

/// The name the drop-in has to carry, next to the content for the same reason
/// the content is here: both ends of the ordering are part of the artifact.
///
/// It sorts after `00-k3s-defaults.conf`, which is where K3s states the
/// disabled value, and before `10-cli-config.conf`, which is where K3s copies a
/// configuration file an operator pointed `--config` at. Landing between the
/// two keeps both of their decisions intact: the default cannot undo this
/// bound, and an operator's own value still can.
pub const K3S_IMAGE_GC_DROPIN_FILE: &str = "05-cogneva-image-gc.conf";

/// The same age bound stated as kubespray host variables.
///
/// A fragment rather than a document: it is appended to the node's other
/// kubelet settings, and a second YAML document in one host_vars file is not a
/// thing Ansible promises to read. Kubespray renders a real
/// `KubeletConfiguration` per node, so this needs no drop-in -- the pinned tag's
/// kubelet-config template merges `kubelet_config_extra_args` into it, which is
/// how a field with no flag is expressed there.
pub fn kubespray_image_gc_vars() -> String {
    format!("kubelet_config_extra_args:\n  imageMaximumGCAge: \"{IMAGE_GC_MAX_AGE}\"\n")
}

/// The same QoS settings stated as kubespray host variables.
///
/// A K3s node reads one file before its first start; a standard Kubernetes
/// node is built by kubespray, which renders the kubelet configuration per
/// node from inventory variables. So the same account is written in the other
/// provisioner's language, and the names are the ones the pinned kubespray tag
/// reads: its `roles/kubernetes/node/defaults/main.yml` declares
/// `kube_*_reserved` / `system_*_reserved` and `eviction_hard`, and its
/// kubelet-config template writes `kubeReserved` and `systemReserved` whether
/// or not the cgroup toggles are on.
///
/// Leaving those variables unset does not leave the node empty: kubespray's
/// fixed 100m/256Mi and 500m/512Mi would apply to every node whatever its
/// size, which is the account this exists to replace. So both scopes are
/// stated in full -- the share for work that is not a pod in the system scope,
/// the same sentence the K3s path states with `system-reserved`, and zeros in
/// the kube scope rather than an arbitrary constant. `system_reserved` itself
/// stays false: the share comes off allocatable without a dedicated cgroup
/// being enforced, exactly as on a K3s node.
pub fn kubespray_qos_host_vars(qos: &NodeQos) -> String {
    format!(
        "---\n\
         # Derived from this node's own /proc/meminfo and CPU count.\n\
         kube_cpu_reserved: \"0m\"\n\
         kube_memory_reserved: \"0Mi\"\n\
         kube_ephemeral_storage_reserved: \"0Mi\"\n\
         kube_pid_reserved: \"0\"\n\
         system_cpu_reserved: \"{}m\"\n\
         system_memory_reserved: \"{}Mi\"\n\
         system_ephemeral_storage_reserved: \"0Mi\"\n\
         system_pid_reserved: \"0\"\n\
         # Every eviction signal is written together: kubelet replaces its own\n\
         # default set with the one here, and a signal left out is silently\n\
         # zero -- an imagefs with no threshold is a disk that fills up\n\
         # without the kubelet noticing.\n\
         eviction_hard:\n\
         \x20 memory.available: \"{}Mi\"\n\
         \x20 nodefs.available: \"5%\"\n\
         \x20 imagefs.available: \"5%\"\n\
         \x20 nodefs.inodesFree: \"5%\"\n\
         \x20 imagefs.inodesFree: \"5%\"\n",
        qos.reserved_cpu_milli, qos.reserved_memory_mb, qos.eviction_memory_mb
    )
}

/// A node's memory and CPU count, as read from the node itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeReadings {
    /// Total memory, in MiB.
    pub mem_total_mb: u64,
    pub cpu_cores: u32,
}

/// Read the two numbers the QoS account is derived from out of one command's
/// output: total memory in MiB, then the CPU count. Anything else -- a
/// truncated read, a node that answered with a shell error -- is not a
/// reading, and a node whose readings are missing must not be given the share
/// of another node.
pub fn parse_node_readings(out: &str) -> Option<NodeReadings> {
    let mut lines = out.lines().map(str::trim).filter(|l| !l.is_empty());
    let mem_total_mb: u64 = lines.next()?.parse().ok()?;
    let cpu_cores: u32 = lines.next()?.parse().ok()?;
    if mem_total_mb == 0 || cpu_cores == 0 {
        return None;
    }
    Some(NodeReadings {
        mem_total_mb,
        cpu_cores,
    })
}

/// The command that produces the two readings above on a node reached over SSH.
///
/// One constant because two install shapes ask the same node the same question
/// -- the kubespray inventory walk and the K3s agent's first start -- and each
/// answer becomes a QoS account on that node. Spelt twice, a change to one copy
/// (a different `/proc` field, a different divisor, one extra line before the
/// numbers) leaves the other shape reading nodes differently, and nothing in
/// the tree compares the two: the install simply reserves a different share,
/// with no reading that says so.
pub const NODE_CAPACITY_PROBE: &str = "awk '/^MemTotal:/{print int($2/1024)}' /proc/meminfo; nproc";

/// Total memory in KiB, read out of the text of `/proc/meminfo`.
///
/// The one place this host's own memory total is parsed from the kernel. Every
/// caller derives from it -- the dirty-page watermarks, the cargo job count --
/// so the field read and its unit are decided once.
///
/// `None` when the line is missing or its number does not parse. A caller that
/// cannot read the total has to say so: zero is a number the arithmetic below
/// accepts, and it would mean "this machine has no memory" rather than "the
/// reading did not happen".
pub fn mem_total_kb(meminfo: &str) -> Option<u64> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// A node's memory account as the cluster reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMemoryAccount {
    pub node: String,
    /// `status.capacity.memory`, in KiB.
    pub capacity_kib: u64,
    /// `status.allocatable.memory`, in KiB.
    pub allocatable_kib: u64,
}

impl NodeMemoryAccount {
    /// Kubelet derives allocatable as capacity minus kubeReserved,
    /// systemReserved and the hard eviction threshold, so an equality here
    /// says all three are unset: the node holds nothing back for the work
    /// outside the cluster, and with no memory threshold it never reports
    /// MemoryPressure either.
    pub fn holds_back_nothing(&self) -> bool {
        self.allocatable_kib == self.capacity_kib
    }
}

/// Read `kubectl get nodes -o custom-columns=NAME,CAPACITY,ALLOCATABLE` back
/// into accounts. Unreadable lines are skipped rather than kept as zeros: a
/// node nobody could read is not a node holding nothing back.
pub fn parse_node_memory_accounts(out: &str) -> Vec<NodeMemoryAccount> {
    out.lines()
        .filter_map(|line| {
            let mut cols = line.split_whitespace();
            let node = cols.next()?.to_string();
            let capacity_kib = parse_kib(cols.next()?)?;
            let allocatable_kib = parse_kib(cols.next()?)?;
            Some(NodeMemoryAccount {
                node,
                capacity_kib,
                allocatable_kib,
            })
        })
        .collect()
}

/// Kubelet reports memory as `65741456Ki`; any other unit is not this reading.
fn parse_kib(value: &str) -> Option<u64> {
    value.strip_suffix("Ki")?.parse().ok()
}

/// Select the governance objects out of the rendered manifests and return them
/// as one multi-document YAML ready for `kubectl apply -f -`.
///
/// Resource ceilings are the operator's: the rollout loop skips these kinds on
/// purpose, because replaying a committed default every revision would
/// overwrite a number someone tuned. The install face applies the whole
/// directory once, so between two installs a ceiling raised in the tree has no
/// carrier at all — the cluster keeps admitting against the old number, and a
/// reading that asks "does it fit" answers yes on either copy.
///
/// Input order is preserved (rendered files sort by name, so the same tree
/// produces the same sequence), and an empty result is a value the caller must
/// refuse rather than an apply of nothing: a profile with governance disabled
/// renders no such object, and "there was nothing to apply" must not print the
/// same success as "it was applied".
pub fn governance_documents(files: &[(String, String)]) -> Result<String, String> {
    let mut out = String::new();
    for (name, text) in files {
        for chunk in split_yaml_documents(text) {
            if chunk.trim().is_empty() {
                continue;
            }
            let value: serde_yaml::Value =
                serde_yaml::from_str(chunk).map_err(|e| format!("{name}: 解析 YAML 失败：{e}"))?;
            match value.get("kind").and_then(|k| k.as_str()) {
                Some("ResourceQuota") | Some("LimitRange") => {
                    let doc = serde_yaml::to_string(&value)
                        .map_err(|e| format!("{name}: 重新序列化失败：{e}"))?;
                    out.push_str(&doc);
                    if !doc.ends_with('\n') {
                        out.push('\n');
                    }
                    out.push_str("---\n");
                }
                _ => {}
            }
        }
    }
    Ok(out)
}

/// Split a manifest file on `---` separator lines. A separator carries nothing
/// of its own, so an empty leading chunk (files that start with `---`) is
/// dropped by the caller for the same reason an empty trailing one is.
fn split_yaml_documents(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        if line.trim_end() == "---" {
            let chunk = &text[start..offset];
            if !chunk.trim().is_empty() {
                out.push(chunk);
            }
            start = offset + line.len();
        }
        offset += line.len();
    }
    let tail = &text[start..];
    if !tail.trim().is_empty() {
        out.push(tail);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hw(memory_gb: u64, nodes: u32) -> HardwareProfile {
        HardwareProfile {
            memory_gb,
            cpu_cores: 4,
            nodes,
        }
    }

    #[test]
    fn branch_rules_match_bootstrap() {
        assert_eq!(decide_branch(&hw(1, 1), Distro::K3s), PlanBranch::K3sSingle);
        assert_eq!(
            decide_branch(&hw(16, 1), Distro::K3s),
            PlanBranch::K3sSingle
        );
        assert_eq!(decide_branch(&hw(16, 3), Distro::K3s), PlanBranch::K3sMulti);
        // kubespray 供给的一律是标准 K8s 形态，与规模无关。
        assert_eq!(
            decide_branch(&hw(16, 1), Distro::Kubespray),
            PlanBranch::K8sStandard
        );
        assert_eq!(
            decide_branch(&hw(16, 3), Distro::Kubespray),
            PlanBranch::K8sStandard
        );
    }

    #[test]
    fn backend_rules_follow_environment() {
        let light = decide_backends(&hw(1, 1), PlanBranch::K3sSingle);
        let nats = light.iter().find(|b| b.kind == "nats").unwrap();
        assert!(!nats.enabled);
        let qdrant = light.iter().find(|b| b.kind == "qdrant").unwrap();
        assert!(!qdrant.enabled);

        let prod = decide_backends(&hw(16, 3), PlanBranch::K3sMulti);
        assert!(prod.iter().find(|b| b.kind == "nats").unwrap().enabled);
        assert!(prod.iter().find(|b| b.kind == "qdrant").unwrap().enabled);
        assert!(!prod.iter().find(|b| b.kind == "mysql").unwrap().enabled);

        // 标准 K8s 形态同样按多节点启用 nats。
        let stdk8s = decide_backends(&hw(16, 3), PlanBranch::K8sStandard);
        assert!(stdk8s.iter().find(|b| b.kind == "nats").unwrap().enabled);
    }

    #[test]
    fn plan_yaml_roundtrip() {
        let plan = ManagementPlan::for_environment("prod", &hw(16, 3), Distro::K3s, "cogneva");
        let yaml = plan.to_yaml().unwrap();
        assert!(yaml.contains("k8m.cogneva/v1alpha1"));
        assert!(yaml.contains("ManagementPlan"));
        let parsed = ManagementPlan::from_yaml(&yaml).unwrap();
        assert_eq!(parsed.spec.branch, PlanBranch::K3sMulti);
        assert_eq!(parsed.spec.gateway_replicas, 3);
    }

    #[test]
    fn kubespray_plan_is_k8s_standard() {
        let plan =
            ManagementPlan::for_environment("prod", &hw(16, 3), Distro::Kubespray, "cogneva");
        assert_eq!(plan.spec.branch, PlanBranch::K8sStandard);
        assert_eq!(plan.spec.gateway_replicas, 3);
        let yaml = plan.to_yaml().unwrap();
        assert!(yaml.contains("k8s-standard"));
        assert!(
            plan.spec
                .backends
                .iter()
                .find(|b| b.kind == "nats")
                .unwrap()
                .enabled
        );
    }

    #[test]
    fn branch_serialization_uses_profile_names() {
        let single = ManagementPlan::for_environment("edge", &hw(1, 1), Distro::K3s, "cogneva");
        let multi = ManagementPlan::for_environment("prod", &hw(16, 3), Distro::K3s, "cogneva");
        assert!(single.to_yaml().unwrap().contains("k3s-single"));
        assert!(multi.to_yaml().unwrap().contains("k3s-multi"));
        // 标准 K8s 形态序列化为 k8s-standard，与 Helm profile 同名。
        let stdk8s =
            ManagementPlan::for_environment("prod", &hw(16, 3), Distro::Kubespray, "cogneva");
        assert!(stdk8s.to_yaml().unwrap().contains("k8s-standard"));
    }

    #[test]
    fn the_qos_reservation_follows_the_documented_capacity_table() {
        // 64199MiB / 24 cores: 10% of the total (the 16-64GiB bucket), the
        // per-core CPU rule, and a fifth of the node as the eviction floor.
        let qos = node_qos(64199, 24);
        assert_eq!(qos.reserved_memory_mb, 6500);
        assert_eq!(qos.reserved_cpu_milli, 140);
        assert_eq!(qos.eviction_memory_mb, 3300);
    }

    #[test]
    fn a_small_node_reserves_a_share_it_can_afford() {
        // 512MiB: the table's fixed floor, not a percentage of nearly nothing.
        let small = node_qos(512, 1);
        assert_eq!(small.reserved_memory_mb, 255);
        assert_eq!(small.reserved_cpu_milli, 60);
        // The eviction threshold keeps the upstream 100Mi default instead of
        // shrinking to 5% of half a gigabyte.
        assert_eq!(small.eviction_memory_mb, 100);
        // 4GiB takes the 20% bucket; 8 cores take the flat rate past the fourth.
        let medium = node_qos(4096, 8);
        assert_eq!(medium.reserved_memory_mb, 900);
        assert_eq!(medium.reserved_cpu_milli, 100);
    }

    #[test]
    fn every_eviction_signal_is_written_out() {
        let yaml = k3s_qos_config_yaml(&node_qos(64199, 24));
        for signal in [
            "memory.available<3300Mi",
            "nodefs.available<5%",
            "imagefs.available<5%",
            "nodefs.inodesFree<5%",
            "imagefs.inodesFree<5%",
        ] {
            assert!(yaml.contains(signal), "missing {signal} in {yaml}");
        }
        assert!(yaml.contains("system-reserved=cpu=140m,memory=6500Mi"));
        // The file is a kubelet-arg list and nothing else: K3s reads it before
        // the first start, so a stray key here is a node that never comes up.
        assert_eq!(yaml.lines().filter(|l| !l.starts_with("  - ")).count(), 1);
        assert!(yaml.starts_with("kubelet-arg:\n"));
    }

    #[test]
    fn the_watermarks_are_read_from_the_devices_own_throughput() {
        // A device writing 39.1 MB/s sustained on a 64199 MiB node: two seconds
        // of the device starts the background writeback, five seconds is where a
        // writer is made to wait.
        let disk = node_disk_io(39_100_000, 64199);
        assert_eq!(disk.dirty_background_bytes, 78_200_000);
        assert_eq!(disk.dirty_bytes, 195_500_000);
        // The RAM share is nowhere near binding here, which is the point: the
        // device, not the memory, sets both numbers.
        assert!(disk.dirty_bytes < 1024 * 1024 * 1024);
    }

    #[test]
    fn a_slow_device_keeps_the_floors_and_a_fast_one_keeps_the_cap() {
        // A device so slow the throughput product falls under the floors: the
        // floors hold, because below them the flusher never catches up.
        let slow = node_disk_io(1_000_000, 8192);
        assert_eq!(slow.dirty_background_bytes, 32 * 1024 * 1024);
        assert_eq!(slow.dirty_bytes, 64 * 1024 * 1024);
        // A device fast enough to ask for more than a small node's RAM share in
        // five seconds is capped at that share instead.
        let fast = node_disk_io(200_000_000, 8192);
        let share = 8192 * 1024 * 1024 / DIRTY_MAX_RAM_DIVISOR;
        assert_eq!(fast.dirty_bytes, share);
        assert!(
            fast.dirty_bytes < 200_000_000 * DIRTY_FORCE_SECONDS,
            "the cap, not the device, set this one"
        );
    }

    #[test]
    fn the_force_watermark_is_always_above_the_background_one() {
        for throughput in [0u64, 1, 1_000_000, 39_100_000, 200_000_000, 5_000_000_000] {
            for ram_mb in [512u64, 4096, 8192, 64199, 1_048_576] {
                let disk = node_disk_io(throughput, ram_mb);
                assert!(
                    disk.dirty_bytes > disk.dirty_background_bytes,
                    "throughput {throughput}, ram {ram_mb}: force {} is not above background {}",
                    disk.dirty_bytes,
                    disk.dirty_background_bytes
                );
                assert!(
                    disk.dirty_background_bytes <= disk.dirty_bytes / 2,
                    "throughput {throughput}, ram {ram_mb}: the background point is not \
                     meaningfully below the force point"
                );
            }
        }
    }

    #[test]
    fn the_dropin_states_both_halves_of_each_pair() {
        let conf = sysctl_node_io_conf(&node_disk_io(39_100_000, 64199), &a_reading());
        assert!(
            conf.contains("vm.dirty_background_bytes = 78200000\n"),
            "{conf}"
        );
        assert!(conf.contains("vm.dirty_bytes = 195500000\n"), "{conf}");
        // The ratios are stated zero rather than left out, so the file reads as
        // the whole policy and not half of one.
        assert!(conf.contains("vm.dirty_background_ratio = 0\n"), "{conf}");
        assert!(conf.contains("vm.dirty_ratio = 0\n"), "{conf}");
        // ... and they are stated first. Writing a ratio zeroes the byte key of
        // its pair, so the other order applies cleanly, prints both byte values
        // as it goes, and leaves `vm.dirty_background_bytes` at 0 -- a threshold
        // of zero bytes, the opposite of what the file says.
        for (ratio, bytes) in [
            ("vm.dirty_background_ratio = 0", "vm.dirty_background_bytes"),
            ("vm.dirty_ratio = 0", "vm.dirty_bytes"),
        ] {
            let ratio_at = conf.find(ratio).expect("ratio line");
            let bytes_at = conf.find(bytes).expect("bytes line");
            assert!(
                ratio_at < bytes_at,
                "{ratio} must be stated before {bytes}, or the kernel zeroes the bytes"
            );
        }
    }

    /// A device reading, in the shape the measuring side hands one over: the
    /// busiest window of a real device rather than the install-time burst.
    fn a_reading() -> ThroughputReading {
        ThroughputReading {
            device: "nvme0n1".to_string(),
            window: "5m".to_string(),
            busy_percent: 86,
            measured_at: "2026-10-13T00:47:00Z".to_string(),
        }
    }

    #[test]
    fn the_dropin_says_which_window_its_number_came_from() {
        let conf = sysctl_node_io_conf(&node_disk_io(52_000_000, 64199), &a_reading());
        for fact in [
            "# device nvme0n1\n",
            "# window 5m\n",
            "# busy 86% of that window\n",
            "# measured at 2026-10-13T00:47:00Z\n",
            "# sustained write 52000000 bytes/s",
        ] {
            assert!(conf.contains(fact), "{fact:?} missing from:\n{conf}");
        }
        // Everything before the first setting is a comment, and the settings are
        // exactly the four keys -- which is what lets a reader compare two files
        // by what they set and not by what they say about where it came from.
        let settings: Vec<&str> = conf.lines().filter(|line| !line.starts_with('#')).collect();
        assert_eq!(
            settings,
            vec![
                "vm.dirty_background_ratio = 0",
                "vm.dirty_ratio = 0",
                "vm.dirty_background_bytes = 104000000",
                "vm.dirty_bytes = 260000000",
            ],
            "{conf}"
        );
        assert!(
            conf.starts_with("# Writeback watermarks"),
            "the reading goes above the values:\n{conf}"
        );
    }

    #[test]
    fn a_remeasurement_that_moves_no_value_changes_no_setting() {
        // The same device read twice: the header differs, the policy does not.
        // A comparison that reads whole files calls this a drift and asks an
        // operator to reconcile a node whose watermarks never moved.
        let disk = node_disk_io(52_000_000, 64199);
        let first = sysctl_node_io_conf(&disk, &a_reading());
        let second = sysctl_node_io_conf(
            &disk,
            &ThroughputReading {
                busy_percent: 82,
                measured_at: "2026-10-13T06:00:00Z".to_string(),
                ..a_reading()
            },
        );
        assert_ne!(first, second, "the reading is what moved");
        assert_eq!(sysctl_settings(&first), sysctl_settings(&second));
        assert!(
            sysctl_conf_disagreements(&first, &second).is_empty(),
            "a re-measurement that moved no watermark is not a disagreement"
        );
    }

    #[test]
    fn the_setting_lines_read_back_as_keys_and_values() {
        let parsed = sysctl_settings(
            "# a comment\n\
             vm.dirty_bytes = 195500000\n\
             \n\
             vm.dirty_ratio=0\n",
        );
        assert_eq!(
            parsed,
            vec![
                ("vm.dirty_bytes".to_string(), "195500000".to_string()),
                ("vm.dirty_ratio".to_string(), "0".to_string()),
            ]
        );
    }

    #[test]
    fn a_node_whose_file_disagrees_is_named_key_by_key() {
        let disk = node_disk_io(39_100_000, 64199);
        let derived = sysctl_node_io_conf(&disk, &a_reading());
        // A file that matches the derivation except on the force watermark and
        // the ratio -- what an operator's hand-written file would differ by.
        let on_node = format!(
            "vm.dirty_background_bytes = {}\n\
             vm.dirty_bytes = 1\n\
             vm.dirty_background_ratio = 0\n\
             vm.dirty_ratio = 20\n",
            disk.dirty_background_bytes
        );
        let diffs = sysctl_conf_disagreements(&on_node, &derived);
        assert!(
            diffs.iter().any(|d| d
                == &format!(
                    "vm.dirty_bytes: on the node 1, derived {}",
                    disk.dirty_bytes
                )),
            "a watermark that disagrees must be named with both numbers: {diffs:?}"
        );
        assert!(
            diffs
                .iter()
                .any(|d| d == "vm.dirty_ratio: on the node 20, derived 0"),
            "the ratio the derivation would clear must be named: {diffs:?}"
        );
        assert_eq!(
            diffs.len(),
            2,
            "only the keys that disagree are reported, not the whole file: {diffs:?}"
        );
    }

    #[test]
    fn a_file_the_derivation_would_write_reports_no_disagreement() {
        let conf = sysctl_node_io_conf(&node_disk_io(52_000_000, 64199), &a_reading());
        assert!(sysctl_conf_disagreements(&conf, &conf).is_empty());
    }

    #[test]
    fn the_kubespray_host_vars_state_the_same_account_as_the_k3s_flag() {
        let yaml = kubespray_qos_host_vars(&node_qos(64199, 24));
        assert!(yaml.contains("system_cpu_reserved: \"140m\""));
        assert!(yaml.contains("system_memory_reserved: \"6500Mi\""));
        // The kube scope is stated at zero rather than left out: an unset
        // kubespray variable is not an empty reservation, it is its own fixed
        // 100m/256Mi for every node.
        assert!(yaml.contains("kube_cpu_reserved: \"0m\""));
        assert!(yaml.contains("kube_memory_reserved: \"0Mi\""));
        // Same five signals as the K3s path, for the same reason.
        for signal in [
            "memory.available: \"3300Mi\"",
            "nodefs.available: \"5%\"",
            "imagefs.available: \"5%\"",
            "nodefs.inodesFree: \"5%\"",
            "imagefs.inodesFree: \"5%\"",
        ] {
            assert!(yaml.contains(signal), "missing {signal} in {yaml}");
        }
        assert!(yaml.starts_with("---\n"));
    }

    /// The age bound has no kubelet flag, so it cannot ride in the config file's
    /// `kubelet-arg` list: that list is a flag passthrough, and naming a flag
    /// that does not exist stops the kubelet, which is a node that never comes
    /// up. It goes in a drop-in, which has to be a KubeletConfiguration and has
    /// to carry the age as a duration string.
    #[test]
    fn the_image_gc_bound_is_a_dropin_and_not_a_kubelet_arg() {
        let dropin = k3s_kubelet_image_gc_dropin_yaml();
        // Parsed rather than grepped: the kubelet's config decoder rejects
        // unknown fields, so a stray key here is a kubelet that will not start.
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(&dropin).expect("drop-in 必须是合法 YAML");
        assert_eq!(
            parsed.get("kind").and_then(|v| v.as_str()),
            Some("KubeletConfiguration")
        );
        assert_eq!(
            parsed.get("apiVersion").and_then(|v| v.as_str()),
            Some("kubelet.config.k8s.io/v1beta1")
        );
        assert_eq!(
            parsed.get("imageMaximumGCAge").and_then(|v| v.as_str()),
            Some(IMAGE_GC_MAX_AGE)
        );
        assert_eq!(
            parsed.as_mapping().map(|m| m.len()),
            Some(3),
            "只有这三个键：KubeletConfiguration 的字段名拼错或多余都会让 kubelet 起不来"
        );
        assert!(
            !k3s_qos_config_yaml(&node_qos(64199, 24)).contains("gc-age"),
            "the age must not be written as a kubelet-arg: there is no flag for it"
        );
    }

    /// The drop-in directory merges in lexical order, later files winning. K3s
    /// writes `imageMaximumGCAge: 0s` into `00-k3s-defaults.conf` and copies a
    /// file an operator pointed `--config` at into `10-cli-config.conf`, so this
    /// name has to land between the two: after the first or the disabled value
    /// wins, before the second or the bound overrules an operator's own value.
    #[test]
    fn the_image_gc_dropin_sorts_between_the_k3s_defaults_and_a_cli_config() {
        // The name K3s gives its own generated file, and the one it copies an
        // operator's `--config` to; both read off the K3s source.
        const K3S_DEFAULTS_DROPIN_FILE: &str = "00-k3s-defaults.conf";
        const K3S_CLI_CONFIG_DROPIN_FILE: &str = "10-cli-config.conf";
        assert!(
            K3S_IMAGE_GC_DROPIN_FILE > K3S_DEFAULTS_DROPIN_FILE,
            "lexical order: a name that sorts first is overridden by the disabled default"
        );
        assert!(
            K3S_IMAGE_GC_DROPIN_FILE < K3S_CLI_CONFIG_DROPIN_FILE,
            "an operator's own configuration file has to be able to overrule this bound"
        );
        assert!(
            K3S_IMAGE_GC_DROPIN_FILE.ends_with(".conf"),
            "kubelet 只读 .conf"
        );
    }

    #[test]
    fn the_kubespray_side_states_the_same_age_bound() {
        let vars = kubespray_image_gc_vars();
        // A fragment, not a document: it is appended under the node's other
        // kubelet settings, and one host_vars file has to stay one document --
        // Ansible reads the first document of a vars file.
        let combined = format!("{}{}", kubespray_qos_host_vars(&node_qos(64199, 24)), vars);
        assert_eq!(
            serde_yaml::Deserializer::from_str(&combined).count(),
            1,
            "host_vars 必须是单个文档，否则追加的那一截不会被读到"
        );
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(&combined).expect("host_vars 必须是合法 YAML");
        assert!(
            parsed.get("eviction_hard").is_some(),
            "追加不能把原有的键挤出去"
        );
        assert_eq!(
            parsed
                .get("kubelet_config_extra_args")
                .and_then(|v| v.get("imageMaximumGCAge"))
                .and_then(|v| v.as_str()),
            Some(IMAGE_GC_MAX_AGE)
        );
    }

    #[test]
    fn node_readings_need_both_numbers() {
        assert_eq!(
            parse_node_readings("64199\n24\n"),
            Some(NodeReadings {
                mem_total_mb: 64199,
                cpu_cores: 24
            })
        );
        // A node that answered with nothing, or with a shell error, is not a
        // node to derive a share for: the caller warns instead of guessing.
        assert_eq!(parse_node_readings(""), None);
        assert_eq!(parse_node_readings("64199\n"), None);
        assert_eq!(parse_node_readings("sh: awk: not found\n24\n"), None);
        assert_eq!(parse_node_readings("0\n0\n"), None);
    }

    /// `/proc/meminfo` carries the total in kB with a unit suffix; the caller
    /// wants KiB, and every caller divides from there. So the suffix is the
    /// parser's job, not each caller's: `"65496336 kB".parse::<u64>()` fails,
    /// and a caller that strips the wrong field reads the unit as the number.
    #[test]
    fn the_memory_total_is_the_first_number_on_the_memtotal_line() {
        let meminfo = "MemTotal:       65496336 kB\nMemFree:         1234567 kB\n";
        assert_eq!(mem_total_kb(meminfo), Some(65_496_336));

        // The field the kernel puts after the number is a unit, not a reading,
        // and a line with nothing after the colon is not a reading either.
        assert_eq!(mem_total_kb("MemTotal: 1048576\n"), Some(1_048_576));
        assert_eq!(mem_total_kb("MemTotal:\n"), None);

        // Missing, or a value that does not parse, is "did not read" -- not
        // zero, which the watermarks would accept as a real memory total.
        assert_eq!(mem_total_kb("MemFree: 1234567 kB\n"), None);
        assert_eq!(mem_total_kb("MemTotal: lots kB\n"), None);
        assert_eq!(mem_total_kb(""), None);

        // A same-prefix neighbour is not the total.
        assert_eq!(mem_total_kb("MemTotalFoo: 999 kB\n"), None);
    }

    #[test]
    fn the_memory_account_is_read_back_from_the_api_units() {
        let out = "cogneva   65741456Ki   65741456Ki\nworker-0   32870428Ki   31000000Ki\n";
        let accounts = parse_node_memory_accounts(out);
        assert_eq!(accounts.len(), 2);
        assert!(accounts[0].holds_back_nothing());
        assert!(!accounts[1].holds_back_nothing());
        assert_eq!(accounts[1].allocatable_kib, 31000000);
        // A line in another unit is not the reading this parses; dropping it
        // must not leave a zero-valued node that reads as "holds back nothing".
        assert!(parse_node_memory_accounts("cogneva   65741456Mi   65741456Mi\n").is_empty());
    }

    /// 渲染产物里只有治理对象该进这份 apply 输入：Secret 混入是红线，工作负载、
    /// ConfigMap 都不属于这张面。顺序按输入给（调用方先按文件名排序），所以同一
    /// 棵树每次产出同一份字节，重复 apply 不产生无谓的 last-applied 变化。
    #[test]
    fn governance_documents_take_only_the_operator_owned_kinds() {
        let files = vec![
            (
                "01-resourcequota.yaml".to_string(),
                "# generated\napiVersion: v1\nkind: ResourceQuota\nmetadata:\n  name: cogneva-quota\n  annotations:\n    cogneva.io/runtime-pods-limits-cpu: \"3\"\nspec:\n  hard:\n    limits.memory: 52Gi\n".to_string(),
            ),
            (
                "02-limitrange.yaml".to_string(),
                "---\napiVersion: v1\nkind: LimitRange\nmetadata:\n  name: cogneva-limits\nspec:\n  limits:\n    - type: Container\n      default:\n        memory: 512Mi\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: not-governance\n".to_string(),
            ),
            (
                "41-deployment.yaml".to_string(),
                "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: cogneva\n".to_string(),
            ),
        ];
        let out = governance_documents(&files).unwrap();
        let kinds: Vec<String> = out
            .split("\n---")
            .filter_map(|d| {
                let v: serde_yaml::Value = serde_yaml::from_str(d).ok()?;
                v.get("kind").and_then(|k| k.as_str()).map(str::to_string)
            })
            .collect();
        assert_eq!(kinds, vec!["ResourceQuota", "LimitRange"]);
        // 引号里的 "3" 过了序列化仍是字符串，不会变成 3：注解的值是字符串面。
        assert!(out.contains("runtime-pods-limits-cpu"));
        assert!(!out.contains("not-governance"));
        assert!(!out.contains("Deployment"));
    }

    /// 没有治理对象时返回空串，不返回一份看似成功的空 apply——调用方靠这个值
    /// 报错（profile 关掉了 governance 与「apply 过」不是同一件事）。
    #[test]
    fn governance_documents_are_empty_when_the_profile_declares_none() {
        let files = vec![(
            "10-configmap.yaml".to_string(),
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n".to_string(),
        )];
        assert_eq!(governance_documents(&files).unwrap(), "");
    }

    #[test]
    fn helm_values_reflect_enabled_backends() {
        let plan = ManagementPlan::for_environment("edge", &hw(1, 1), Distro::K3s, "cogneva");
        let values = plan.to_helm_values();
        assert_eq!(values["gateway"]["replicas"], serde_json::json!(1));
        assert_eq!(
            values["backends"]["nats"]["enabled"],
            serde_json::json!(false)
        );
        assert_eq!(
            values["backends"]["postgres"]["enabled"],
            serde_json::json!(true)
        );
    }
}
