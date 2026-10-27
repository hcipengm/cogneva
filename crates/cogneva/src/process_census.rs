//! 每节点一个的进程采集器：宿主上**每一个**容器实例里跑着什么。
//!
//! 为什么不是让每个容器自报（那条路已经在跑，见 `process_report`）：只有跑本平台镜像
//! 的容器才认得上报端点，postgres、nats、registry、评测环境这些第三方镜像一个都不报，
//! 而屏上那一格读起来像「里面没有进程」。进程属于容器，这件事对谁家的镜像都成立。
//!
//! 也不伸手进容器里跑 `ps`：那要一项对业务容器执行命令的权限，每看一次多敲一次，
//! 而这一面是轮询的。
//!
//! 走的是宿主自己的账，不依赖容器里的任何东西：
//!
//! 1. 带 `hostPID` 起，于是 `/proc` 是宿主的那一份，`/proc/<pid>/comm` 读得到任何进程；
//! 2. 宿主的 cgroup 根只读挂进来，按 `cri-containerd-<id>.scope` 找叶子——一个叶子就是
//!    一个容器实例，目录名里那串 id 与 Pod 状态里的 `containerID` 是同一个；
//! 3. 叶子的 `cgroup.procs` 给出这个容器的全部 PID，逐个读 `/proc/<pid>/{stat,comm,statm}`。
//!
//! 这条路径上唯一的权限是「看得到宿主进程」。它读不到别人的内存、改不了别人的状态，
//! 也不需要特权；`cgroup.procs` 与 `/proc/<pid>/comm` 都是只读文件。
//!
//! 不读 `/proc/<pid>/cgroup` 反查容器：非特权 Pod 的 cgroup 命名空间是私有的，那条
//! 路径渲染成相对自己 cgroup 的 `/../../..`，认不出是谁（实测过）。从宿主 cgroup 树
//! 往下数叶子不需要这一步。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;

use crate::process_report::{read_process, ReportedProcess, MAX_PROCESSES};

/// 这条循环在 `loop_health` 里的名字。
pub const LOOP_NAME: &str = "process-census";

/// 宿主 cgroup v2 根在这个 Pod 里挂在哪。
pub const ROOT_ENV: &str = "COGNEVA_PROCESS_CENSUS_ROOT";
pub const DEFAULT_ROOT: &str = "/hostcgroup";
/// 报给谁。与容器自报同一个端点：这是同一条通道、同一份账。
pub const URL_ENV: &str = "COGNEVA_PROCESS_CENSUS_URL";
pub const DEFAULT_URL: &str = "http://cogneva:8085/api/v1/cluster/process-report";
/// 自己跑在哪台节点上。空着照样报，但屏上就没有「这台节点有人在采」的活体。
///
/// 名字声明在 cog-core：容器自报与宿主侧读数读的是同一个变量，两处各写一份时
/// 改一处不报错，另一处照旧报一个空节点——而空节点读起来像「这条读数不来自
/// 任何地方」，不像「变量改名了」。
pub const NODE_ENV: &str = cog_core::env::NODE_NAME;

/// 一拍多久。与容器自报同一个节奏：两边报的是同一份读数，节奏不该有两套。
pub const INTERVAL: Duration = Duration::from_secs(60);

/// cgroup 树往下最多走几层。宿主布局是 `/kubepods.slice/<qos>.slice/<pod>.slice/<scope>`，
/// 四层；留一倍余量，再多就不是我们认识的布局，而不是「树很深」。
const MAX_DEPTH: usize = 8;

/// systemd 驱动给容器起的 scope 名。cgroupfs 驱动下叶子目录名就是 id 本身，
/// 所以前缀可有可无，判据落在「末段是不是十六进制 id」上。
const SCOPE_PREFIX: &str = "cri-containerd-";
const SCOPE_SUFFIX: &str = ".scope";

#[derive(Debug, Serialize)]
struct Envelope<'a> {
    node: &'a str,
    reports: Vec<ContainerReport>,
}

#[derive(Debug, Serialize)]
struct ContainerReport {
    container_id: String,
    processes: Vec<ReportedProcess>,
}

/// 一个 cgroup 目录名是不是某个容器实例的叶子；是就给出那串 id。
///
/// 判据是「去掉 scope 前后缀之后是十六进制且够长」：Pod 层的目录名里有 `pod` 前缀与
/// 下划线（`kubepods-burstable-pod<uid>.slice`），systemd 自己的 slice 也不是纯十六进制，
/// 两者都不会被认成容器 id。
pub fn container_id_of_dir(name: &str) -> Option<String> {
    let without_prefix = name.strip_prefix(SCOPE_PREFIX).unwrap_or(name);
    let id = without_prefix
        .strip_suffix(SCOPE_SUFFIX)
        .unwrap_or(without_prefix);
    (id.len() >= 16 && id.chars().all(|c| c.is_ascii_hexdigit())).then(|| id.to_string())
}

/// 走一遍宿主 cgroup 树，把每个容器实例的进程表读出来。
///
/// 读不到（目录不在、权限不对、这一拍正好没有容器）就是空表——不是错误：采集器在
/// 一个空的节点上也该报「我在，什么都没有」。
pub fn scan(root: &Path) -> BTreeMap<String, Vec<ReportedProcess>> {
    let mut out = BTreeMap::new();
    walk(root, 0, &mut out);
    out
}

fn walk(dir: &Path, depth: usize, out: &mut BTreeMap<String, Vec<ReportedProcess>>) {
    if depth > MAX_DEPTH {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    // 排序只为让读数稳定：两台节点上的同一次扫描该按同一个顺序读出来。
    let mut children: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    children.sort();
    for child in children {
        let Some(name) = child.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        match container_id_of_dir(name) {
            Some(id) => {
                let processes = cgroup_processes(&child);
                if !processes.is_empty() {
                    out.insert(id, processes);
                }
            }
            None => walk(&child, depth + 1, out),
        }
    }
}

/// 一个容器 cgroup 里的进程。`cgroup.procs` 给出进程 id（不是线程 id），逐个读 `/proc`。
fn cgroup_processes(dir: &Path) -> Vec<ReportedProcess> {
    let Ok(text) = std::fs::read_to_string(dir.join("cgroup.procs")) else {
        return Vec::new();
    };
    let mut pids: Vec<u32> = text
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect();
    pids.sort_unstable();
    pids.truncate(MAX_PROCESSES);
    pids.into_iter().filter_map(read_process).collect()
}

/// 宿主 cgroup 根挂进来了没有。空表不能同时表示「挂错」与「没有容器」。
pub fn root_is_usable(root: &Path) -> Result<(), String> {
    if root.is_dir() {
        return Ok(());
    }
    Err(format!(
        "`{ROOT_ENV}` 指向的 {root:?} 不是一个目录：宿主的 cgroup 根没挂进这个 Pod，\
         这一路读到的永远是空表，而空表在屏上读作「这台节点上没有容器」"
    ))
}

/// 采集器自己的入口。
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(
        std::env::var(ROOT_ENV)
            .ok()
            .filter(|root| !root.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_ROOT.to_string()),
    );
    // 挂错了路径不进入循环：那之后每一拍读到的都是空表，而「空表」在这一面读作
    // 「这台节点上没有容器」——一个永不报错的静默空账。退出让 Pod 反复重启，
    // DaemonSet 上看得见。
    root_is_usable(&root)?;
    // 走 `loop_health::spawn_unstoppable` 而不是裸循环：这一面在外面真正的活体是
    // 网关那一格的 `process_census_at`（它停了就停在这一拍上），而重启要有人做——
    // 采集器自己 panic 之后没人拉它起来的话，那格读数会冻在上一拍，和「这台节点上
    // 没有容器」同形。
    cog_core::loop_health::spawn_unstoppable(
        LOOP_NAME,
        cog_core::loop_health::Cadence::Periodic(INTERVAL),
        move |beat| {
            let root = root.clone();
            async move {
                let Ok(census) = Census::from_env(root) else {
                    return;
                };
                census.run(beat).await;
            }
        },
    );
    // 子命令进程的寿命就是这个循环的寿命：`run_from_env` 返回，进程退出，采集器
    // 消失，而 DaemonSet 会把「没在采」显示成一次正常的 Completed。
    std::future::pending::<()>().await;
    Ok(())
}

/// 一次采集需要的东西：读哪棵树、报给谁、以什么身份。
struct Census {
    root: PathBuf,
    node: String,
    url: String,
    client: reqwest::Client,
}

impl Census {
    fn from_env(root: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let node = std::env::var(NODE_ENV).unwrap_or_default();
        let url = std::env::var(URL_ENV)
            .ok()
            .filter(|url| !url.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_URL.to_string());
        // 客户端走工厂：超时与连接池只有一处定义。五秒封顶同容器自报那条——这一拍报
        // 不上去不该拖住下一拍；不走出站代理，因为这条流量去的是本命名空间的主应用
        // Service。
        let config = cog_net::HttpClientConfig {
            timeout_secs: 5,
            proxy_url: None,
            ..Default::default()
        };
        let client = cog_net::build_client(&config)?;
        Ok(Self {
            root,
            node,
            url,
            client,
        })
    }

    async fn run(self, beat: cog_core::loop_health::Beat) {
        let mut ticker = tokio::time::interval(INTERVAL);
        loop {
            ticker.tick().await;
            // 每一拍盖章：报不上去也是这一拍发生了，断章要说的是「采集器停了」。
            beat.beat();
            self.report_once().await;
        }
    }

    async fn report_once(&self) {
        let reports: Vec<ContainerReport> = scan(&self.root)
            .into_iter()
            .map(|(container_id, processes)| ContainerReport {
                container_id,
                processes,
            })
            .collect();
        // 一个容器都没有也照发：这一拍同时是「这台节点我在」的活体，而「采集器在跑、
        // 这台节点上此刻没有容器」与「这台节点上没有采集器」在屏上必须分得开。
        let body = Envelope {
            node: self.node.trim(),
            reports,
        };
        // 报不上去不是这个进程的事：下一拍再说。
        let _ = self.client.post(&self.url).json(&body).send().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const B: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    #[test]
    fn a_scope_directory_names_a_container_instance() {
        assert_eq!(
            container_id_of_dir(&format!("cri-containerd-{A}.scope")).as_deref(),
            Some(A)
        );
        // cgroupfs 驱动：叶子目录名就是 id。
        assert_eq!(container_id_of_dir(A).as_deref(), Some(A));
    }

    #[test]
    fn the_pod_layer_is_not_a_container() {
        // 这一条没有就会红：把 Pod 层当成容器，屏上就多出一格谁也对不上的读数。
        assert_eq!(
            container_id_of_dir("kubepods-burstable-pod1288f852_f85f_4fce_a027_9fffbdcb0f26.slice"),
            None
        );
        assert_eq!(container_id_of_dir("kubepods.slice"), None);
        assert_eq!(container_id_of_dir("system.slice"), None);
    }

    fn tree(root: &Path) {
        let qos = root.join("kubepods.slice/kubepods-burstable.slice");
        let pod = qos.join("kubepods-burstable-podaaaa.slice");
        std::fs::create_dir_all(pod.join(format!("cri-containerd-{A}.scope"))).unwrap();
        std::fs::create_dir_all(pod.join(format!("cri-containerd-{B}.scope"))).unwrap();
        // 一个空叶子：容器刚起、或者已经退出。它不该出现在读数里。
        std::fs::create_dir_all(qos.join(format!("cri-containerd-{B}.scope"))).unwrap();
        std::fs::write(
            pod.join(format!("cri-containerd-{A}.scope/cgroup.procs")),
            format!("{}\n", std::process::id()),
        )
        .unwrap();
        std::fs::write(
            pod.join(format!("cri-containerd-{B}.scope/cgroup.procs")),
            "",
        )
        .unwrap();
    }

    #[test]
    fn a_container_is_read_from_the_pids_in_its_own_cgroup() {
        // 没有这次改动就会红：采集器根本不读宿主 cgroup 树，这一格拿不到任何进程。
        let root = std::env::temp_dir().join(format!("cog-census-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        tree(&root);
        let scanned = scan(&root);
        let mine = scanned.get(A).expect("这个 cgroup 里有一个真实进程");
        assert_eq!(mine[0].pid, std::process::id());
        assert!(
            !scanned.contains_key(B),
            "空的 cgroup 不是一个容器，报上去读作「里面没有进程」"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_missing_tree_stops_the_collector_instead_of_reporting_empty() {
        // 没有这次改动就会红：挂错路径时每一拍照样报「这一拍什么都没有」，与「这台
        // 节点上没有容器」同形——一个永不报错的静默空账。
        let missing = std::env::temp_dir().join("cog-census-does-not-exist");
        assert!(root_is_usable(&missing).is_err());
        let root = std::env::temp_dir().join(format!("cog-census-ok-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        assert!(root_is_usable(&root).is_ok());
        let _ = std::fs::remove_dir_all(&root);
    }
}
