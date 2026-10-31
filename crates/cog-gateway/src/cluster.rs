//! Cluster component health — reports the fixed deployments that make up a
//! Cogneva install so the topology view can render real nodes instead of
//! placeholders.
//!
//! The main app is the process serving this request, so it is always "up".
//! The rest are read from the Kubernetes API; when the service account lacks
//! permission (or we are not in a cluster) they degrade to "unknown".
//!
//! The list is the platform's own workloads, not everything running in the
//! namespace. Data services and third-party pods are on the topology view,
//! which reads the namespace directly and cannot fall behind a list; what this
//! list answers is "is every deployment this product ships ready", and a
//! workload missing from it reads exactly like one that is healthy — the strip
//! says nothing about a deployment it was never asked about.

use axum::Json;
use cog_core::claim_footprint::quantity_bytes;
use serde::Serialize;

use crate::llm_admin::KubeClient;

#[derive(Debug, Serialize)]
pub struct ComponentStatus {
    /// Stable identifier consumed by the WebUI: main / security-gateway / evolution.
    pub id: String,
    /// up / down / unknown.
    pub status: String,
    pub ready_replicas: Option<u32>,
    pub desired_replicas: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ClusterComponents {
    pub components: Vec<ComponentStatus>,
}

async fn deployment_status(client: &KubeClient, id: &str, deployment: &str) -> ComponentStatus {
    let path = format!(
        "/apis/apps/v1/namespaces/{}/deployments/{}",
        client.namespace(),
        deployment
    );
    match client.get_json(&path).await {
        Ok(dep) => {
            let desired = dep
                .pointer("/spec/replicas")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32)
                .unwrap_or(1);
            let ready = dep
                .pointer("/status/readyReplicas")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32)
                .unwrap_or(0);
            ComponentStatus {
                id: id.to_string(),
                status: if ready > 0 { "up" } else { "down" }.to_string(),
                ready_replicas: Some(ready),
                desired_replicas: Some(desired),
            }
        }
        Err(_) => ComponentStatus {
            id: id.to_string(),
            status: "unknown".to_string(),
            ready_replicas: None,
            desired_replicas: None,
        },
    }
}

/// 这一面要读 Deployment 的组件：面板上的 id，以及读哪个 Deployment。
///
/// id 不是另起一套名字，就是工作负载上的组件标签值：同一台 Gateway 在面板上叫
/// 一个名字、平台选中它时叫另一个名字，读的人就要自己在脑子里对齐两张表，而对
/// 不齐时两边都不报错——一边少一行、一边选了空集。
///
/// 读不到集群的分支走同一张表：两个分支各写一份，加一个组件时只会改到其中一份，
/// 而少的那一行读出来正是「这台没部署」，不是「没读到集群」。
const READ_COMPONENTS: &[(&str, &str)] = &[
    (
        cog_core::topology::COMPONENT_SECURITY_GATEWAY,
        cog_core::topology::SECURITY_GATEWAY,
    ),
    (
        cog_core::topology::COMPONENT_SANDBOX_EXECUTOR,
        cog_core::topology::SANDBOX_EXECUTOR,
    ),
    (
        cog_core::topology::COMPONENT_EVOLUTION,
        cog_core::topology::EVOLUTION,
    ),
    // 镜像仓也在这张表里：它一停，任何一次滚动都拉不到镜像，而「工作负载」这一条
    // 读作全绿——少一行与「这台起够了」在屏上同形。
    (
        cog_core::topology::COMPONENT_REGISTRY,
        cog_core::topology::REGISTRY,
    ),
];

pub async fn cluster_components_handler() -> Json<ClusterComponents> {
    let mut components = vec![ComponentStatus {
        id: "main".to_string(),
        status: "up".to_string(),
        ready_replicas: None,
        desired_replicas: None,
    }];

    match KubeClient::in_cluster() {
        Ok(client) => {
            for (id, deployment) in READ_COMPONENTS {
                components.push(deployment_status(&client, id, deployment).await);
            }
        }
        Err(_) => {
            for (id, _) in READ_COMPONENTS {
                components.push(ComponentStatus {
                    id: id.to_string(),
                    status: "unknown".to_string(),
                    ready_replicas: None,
                    desired_replicas: None,
                });
            }
        }
    }

    Json(ClusterComponents { components })
}

// ───────────────────────── 拓扑：集群 → 节点 → Pod → 容器 ─────────────────────────
//
// 这一面要回答的是「东西跑在哪台机器上、那台机器还剩多少」。三条数据来源都是
// apiserver 自己：`/version`、`/api/v1/nodes`、命名空间下的 pods。不经过 Prometheus
// 之类的中间层——一张拓扑图上的每一个数都要能顺着指回产生它的那个对象。
//
// 「多集群」在这棵树里目前只有一个成员：本进程所在的集群。返回结构仍然是一个
// 数组：这一层将来会有第二个元素，而前端不必在那一天重写；现在就画两个空壳集群
// 才是撒谎。

use chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct Topology {
    pub clusters: Vec<ClusterView>,
    pub collected_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ClusterView {
    /// 稳定标识，前端用它做选中态。这是接口给这个连接的编号（`in-cluster`），
    /// 不是集群的名字——Kubernetes 的 API 里没有「集群叫什么」这一项。
    pub id: String,
    /// 这一面读的是**这一个命名空间**里的 Pod：节点是整台机器的，Pod 只取平台自己的
    /// 那一个。界面要把这句说出来，否则「Pod 37」会被读成「集群里一共 37 个 Pod」，
    /// 而 `kubectl get pods -A` 会给出另一个数。
    pub namespace: String,
    /// 这次读取有没有真的读到集群。`false` 时 `error` 说明为什么。
    pub reachable: bool,
    pub error: Option<String>,
    pub version: Option<String>,
    /// 实际用量这一次是从哪儿读到的：`metrics-server`、`kubelet-summary`，
    /// 或者 `None`（两个来源都读不到，界面要说明白「只有申报量」，不能画一条零线）。
    pub usage_source: Option<String>,
    pub nodes: Vec<NodeView>,
    /// Pod 自己说它没有被调度到任何节点上（`spec.nodeName` 空）。这是调度器的事实。
    pub unscheduled: Vec<PodView>,
    /// Pod 说自己落在某台节点上，但这次**没有读到那台节点**——节点列表读失败（权限、
    /// apiserver 出错），或者节点刚被删。它们不是「没有被调度」，把它们画进
    /// `unscheduled` 就是一句假话：节点读失败时整屏的 Pod 都会变成「未调度」。
    pub unplaced: Vec<PodView>,
}

#[derive(Debug, Serialize)]
pub struct NodeView {
    pub name: String,
    pub ready: bool,
    pub roles: Vec<String>,
    pub cpu_allocatable_millicores: Option<u64>,
    pub memory_allocatable_bytes: Option<u64>,
    pub pods_allocatable: Option<u64>,
    pub cpu_requested_millicores: u64,
    pub memory_requested_bytes: u64,
    /// 节点上所有 Pod 的实际用量之和（来自 metrics-server）。
    pub cpu_used_millicores: Option<u64>,
    pub memory_used_bytes: Option<u64>,
    /// Ready / MemoryPressure / DiskPressure / PIDPressure。压力条件为真时是坏事，
    /// 与 Pod 的条件语义相反，界面按名字分开解读。
    pub conditions: Vec<ConditionView>,
    /// 这台节点上的进程采集器最后一次报到的时刻。
    ///
    /// `None` 不是「这台节点没有进程」，而是**这台节点上没有活着的采集器**：它上面
    /// 每一个容器的进程表都没有人能填。与「采集器在跑、这一格读不到」是两个现场，
    /// 修法相反，所以这里必须分得开。
    pub process_census_at: Option<chrono::DateTime<Utc>>,
    /// 这台节点上采集器看到几个容器的进程表。`None` ＝ 这台节点上没有活着的采集器
    /// （没人能报，不是零个）。
    pub census_containers: Option<usize>,
    /// 上面这些容器里，有**几个是这一面读不到的**：采集器读的是整台宿主的 cgroup
    /// 树，而这一面画的是有 Pod 的容器。对不上的那些（构建容器、直接起在宿主上的
    /// 进程）有进程读数，却没有一格能显示它们。
    ///
    /// 把差读出来，是为了让「全量」这句话可以被检验：这一格不为 0 时，屏上的容器
    /// 就不是这台机器上的全部容器，而这句话只能由这个数说出来。
    pub unclaimed_containers: Option<usize>,
    /// 上面那个数点名的那些容器，一个一条。与它同时有值、同时为 `None`。
    ///
    /// 计数答「漏了几个」，这一列答「漏的是谁」——没有名字的话，读者看到「3 个没有
    /// 认领」只能猜，而宿主上最吃 CPU 的恰恰是这几个。进程名是这一面唯一读得到的
    /// 身份：cgroup 只给容器 id，Kubernetes 的状态里又没有这些容器。
    pub unclaimed: Vec<crate::process_reports::UnclaimedContainer>,
    pub pods: Vec<PodView>,
}

#[derive(Debug, Serialize)]
pub struct PodView {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    pub ready: bool,
    pub restarts: u32,
    pub node: Option<String>,
    pub cpu_requested_millicores: u64,
    pub memory_requested_bytes: u64,
    pub cpu_used_millicores: Option<u64>,
    pub memory_used_bytes: Option<u64>,
    /// 这个 Pod 里跑着几个进程，来自 kubelet 的 summary（`process_stats.process_count`）。
    /// 只有数，没有名字：Kubernetes 的读接口不报进程名单，要看名字只能 exec 进容器
    /// 跑 `ps`，那是对运行中的业务容器执行命令，不给读面这个权限。metrics-server
    /// 那条腿不报这个数，所以 `None` 说的是「这个来源量不到」，不是「没有进程」。
    pub processes: Option<u64>,
    /// 这个 Pod 所在节点上的进程采集器最后一次报到的时刻。
    ///
    /// 从节点那一层抄到每一个 Pod 上，是因为读它的是「这一格为什么没有进程表」这句话，
    /// 而那句话只在容器的展开面板里说。`None` ＝ 这台节点上没有活着的采集器。
    pub process_census_at: Option<chrono::DateTime<Utc>>,
    pub containers: Vec<ContainerView>,
    pub conditions: Vec<ConditionView>,
    pub volumes: Vec<VolumeView>,
    /// 最近几条事件，新的在前。空表示没有——不是「没读」，没读会在集群那一层报错。
    pub events: Vec<EventView>,
}

#[derive(Debug, Serialize)]
pub struct ContainerView {
    pub name: String,
    pub image: String,
    pub ready: bool,
    pub restarts: u32,
    /// running / waiting:<原因> / terminated:<原因> / unknown
    pub state: String,
    pub ports: Vec<PortView>,
    /// 申报量（决定排不排得进来）与上限（决定会不会被 OOM 杀掉）分开给。
    pub cpu_request_millicores: Option<u64>,
    pub memory_request_bytes: Option<u64>,
    pub cpu_limit_millicores: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    /// 上一次是怎么结束的。`restarts` 只说重启了几次，不说为什么——
    /// 退出码和原因才是那句话。
    pub last_termination: Option<TerminationView>,
    pub mounts: Vec<MountView>,
    /// liveness / readiness / startup，谁配了谁在这里。
    pub probes: Vec<String>,
    /// 实际用量。没有 metrics-server 时是 `None`——与「用了 0」不是一回事。
    pub cpu_used_millicores: Option<u64>,
    pub memory_used_bytes: Option<u64>,
    /// 容器自己报上来的进程表。只有跑我们镜像的容器会报；`None` 是「这个容器
    /// 不报」或「这一拍还没到」，不是「里面没有进程」。进程属于容器而不属于
    /// Pod：同一个 Pod 的两个容器各有各的 PID 命名空间。
    pub processes: Option<Vec<crate::process_reports::ReportedProcess>>,
}

#[derive(Debug, Serialize)]
pub struct PortView {
    pub name: Option<String>,
    pub container_port: u16,
    pub protocol: String,
}

#[derive(Debug, Serialize)]
pub struct TerminationView {
    /// Completed / Error / OOMKilled / ContainerCannotRun / Evicted …
    pub reason: String,
    pub exit_code: i64,
    pub message: Option<String>,
    pub finished_at: Option<String>,
}

/// Pod 的各个条件（Scheduled / Initialized / Ready / ContainersReady …）。
/// `phase` 只给一个总数，条件说清卡在哪一步：卡在 Initialized 是卷挂不上，
/// 卡在 Ready 是进程起来了但探针不过。
#[derive(Debug, Clone, Serialize)]
pub struct ConditionView {
    pub kind: String,
    pub ok: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MountView {
    pub name: String,
    pub mount_path: String,
    pub read_only: bool,
}

/// 一个卷从哪儿来。只读到「哪一类」为止：再往下是 PVC → PV → 存储类 → 节点上的
/// 实际目录，那条链要另外几个对象的读权限，而排障的第一步只要知道是「配置卷」
/// 还是「持久卷」——前者改了要重启，后者改了不生效。
#[derive(Debug, Serialize)]
pub struct VolumeView {
    pub name: String,
    pub kind: String,
    /// 持久卷才有：这条卷声明绑到了哪一块盘、多大、什么存储类。
    pub claim: Option<ClaimView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClaimView {
    pub name: String,
    /// Pending / Bound / Lost
    pub phase: String,
    pub storage_class: Option<String>,
    pub capacity_bytes: Option<u64>,
    pub access_modes: Vec<String>,
    pub volume_name: Option<String>,
    /// 盘上实际占了多少，来自平台自己的走查端；`None` 是这个卷还没被量到，
    /// 不是「空的」——声明多大在上一个字段里，两者本来就不是一个数。
    pub measured_bytes: Option<u64>,
}

/// 这个对象最近发生过什么。事件是 Kubernetes 里唯一会说出「为什么」的地方：
/// 状态只告诉你它在等，事件告诉你它在等什么。
#[derive(Debug, Clone, Serialize)]
pub struct EventView {
    /// Normal / Warning
    pub kind: String,
    pub reason: String,
    pub message: String,
    pub count: u32,
    pub last_seen: Option<String>,
}

pub async fn cluster_topology_handler() -> Json<Topology> {
    let collected_at = Utc::now();
    let Ok(client) = KubeClient::in_cluster() else {
        // 不在集群里跑（本机开发、或者这个进程没有 ServiceAccount）。空数组比一个
        // 画出来的假集群好：读者立刻知道这一面没有数据源，而不是以为集群空了。
        return Json(Topology {
            clusters: Vec::new(),
            collected_at,
        });
    };

    let mut view = ClusterView {
        id: "in-cluster".to_string(),
        namespace: client.namespace().to_string(),
        reachable: true,
        error: None,
        version: None,
        usage_source: None,
        nodes: Vec::new(),
        unscheduled: Vec::new(),
        unplaced: Vec::new(),
    };

    match client.get_json("/version").await {
        Ok(version) => {
            view.version = version
                .get("gitVersion")
                .and_then(Value::as_str)
                .map(str::to_string)
        }
        Err(e) => view.error = Some(format!("读不到集群版本：{e}")),
    }

    match client.get_json("/api/v1/nodes").await {
        Ok(nodes) => {
            view.nodes = nodes
                .get("items")
                .and_then(Value::as_array)
                .map(|items| items.iter().map(node_view).collect())
                .unwrap_or_default();
            if view.nodes.is_empty() {
                view.error = Some("apiserver 回了空节点列表".to_string());
            }
        }
        Err(e) => {
            view.reachable = false;
            view.error = Some(format!(
                "读不到节点：{e}。这一面需要 ServiceAccount 对 nodes 有 list 权限"
            ));
        }
    }

    // 实际用量的来源按「谁在集群里就有」排，不把任何一个当必需品：
    // 1. metrics-server —— 装了就有，但它是一个可选组件；
    // 2. kubelet 自己的 summary 接口，经 apiserver 代理读 —— 每台机器的 kubelet
    //    都在跑，不需要任何额外组件，只要 SA 对 `nodes/stats` 有 get；
    // 3. 两个都没有：只显示申报量，并在界面上说明白，不画一条零线。
    // 读不到不是错误：这一面照样能画申报量，把它当成读失败会让一个没装 metrics-server
    // 的集群整天亮着一张红条。
    // 用量与 Pod 都按整个集群读，与节点那一层同范围：采集器量的是整台宿主上的
    // 每一个容器，这一面若只画自己命名空间里的，差出来的那些容器就只有进程读数、
    // 没有格子（2026-10-27 实测 71 个容器里 22 个在本命名空间）。
    let (usage, usage_source) = match client.get_json("/apis/metrics.k8s.io/v1beta1/pods").await {
        Ok(list) => (usage_by_pod(&list), Some(USAGE_FROM_METRICS.to_string())),
        Err(_) => match usage_from_kubelets(&client, &view.nodes).await {
            Some(usage) => (usage, Some(USAGE_FROM_KUBELET.to_string())),
            None => (std::collections::HashMap::new(), None),
        },
    };
    view.usage_source = usage_source;

    // 卷声明读一次，给持久卷补上「绑到哪块盘、多大」。读不到就只是缺这一层信息。
    let claims = match client.get_json(pvc_list_path()).await {
        Ok(list) => claims_by_name(&list),
        Err(_) => std::collections::HashMap::new(),
    };

    // 事件读一次、按对象名分给各个 Pod：一个 Pod 一次请求在几十个 Pod 的命名空间里
    // 会把一面拓扑变成几十次 apiserver 往返，而这一面是轮询的。
    let events = match client.get_json(event_list_path()).await {
        Ok(list) => events_by_object(&list),
        Err(_) => std::collections::HashMap::new(),
    };

    let pods_path = "/api/v1/pods".to_string();
    match client.get_json(&pods_path).await {
        Ok(pods) => {
            // 卷的实际占用来自平台自己的走查端：每台节点上都有一个 claim-store，
            // 它量的是节点磁盘上每个卷的真实字节数。这条不经过 metrics-server，
            // 也不经过 kubelet——它读的是这个平台自己记的账。
            let measured = measured_claim_bytes(client.http(), pods.get("items"), &claims).await;
            let sources = Sources {
                events,
                usage,
                claims,
                measured,
            };
            let parsed: Vec<PodView> = pods
                .get("items")
                .and_then(Value::as_array)
                .map(|items| items.iter().map(|pod| pod_view(pod, &sources)).collect())
                .unwrap_or_default();
            place_pods(&mut view, parsed);
            // 采集器读的是整台宿主上的每一个容器，这一面画的是每个有 Pod 的容器。
            // 两者差出来的那些（宿主上的构建容器、不经 k8s 起的进程）有进程读数却没有
            // 格子。差读在 Pod 读成功之后——读不到 Pod 时不知道谁被认领了，那时的差
            // 不是差，是不知道。
            let claims = claims_of_pods(pods.get("items"));
            for node in &mut view.nodes {
                if let Some(gap) = crate::process_reports::census_gap(&node.name, &claims) {
                    node.census_containers = Some(gap.seen);
                    // 计数从这一列里数出来，不另算一遍：两处各算一次，差一格的时候
                    // 屏上会出现「3 个没有认领」而下面只列着 2 条的场面，而读者没有
                    // 办法知道该信哪一个。
                    node.unclaimed_containers = Some(gap.unclaimed.len());
                    node.unclaimed = gap.unclaimed;
                }
            }
        }
        Err(e) => {
            view.error = Some(match view.error {
                Some(previous) => format!("{previous}；读不到 Pod：{e}"),
                None => format!("读不到 Pod：{e}"),
            });
        }
    }

    if view.usage_source.is_some() {
        for node in &mut view.nodes {
            let used: Vec<(u64, u64)> = node
                .pods
                .iter()
                .filter_map(|pod| pod.cpu_used_millicores.zip(pod.memory_used_bytes))
                .collect();
            if !used.is_empty() {
                node.cpu_used_millicores = Some(used.iter().map(|(cpu, _)| cpu).sum());
                node.memory_used_bytes = Some(used.iter().map(|(_, memory)| memory).sum());
            }
        }
    }

    Json(Topology {
        clusters: vec![view],
        collected_at,
    })
}

pub const USAGE_FROM_METRICS: &str = "metrics-server";
pub const USAGE_FROM_KUBELET: &str = "kubelet-summary";

/// 用量来源的第二条腿：每台节点的 kubelet 都在服务 summary 接口，经 apiserver 代理
/// 就能读，不需要集群里装任何额外组件。
///
/// 这条腿要的是 `nodes/stats` 的读权限，不是 `nodes/proxy`：apiserver 按路径把代理
/// 过来的请求映射到子资源，`/stats/*` 落到 `nodes/stats`。两者差着一整个 kubelet
/// API——`nodes/proxy` 还能 exec 进任何 Pod，那是给这一面读用量用不着的权力。
///
/// 一台读不到不影响其它台：只要有一台回了，这一面就有用量；全读不到才退回到「只
/// 有申报量」。
async fn usage_from_kubelets(
    client: &KubeClient,
    nodes: &[NodeView],
) -> Option<std::collections::HashMap<String, PodUsage>> {
    let mut merged = std::collections::HashMap::new();
    let mut read_any = false;
    for node in nodes {
        let path = format!("/api/v1/nodes/{}/proxy/stats/summary", node.name);
        let Ok(summary) = client.get_json(&path).await else {
            continue;
        };
        read_any = true;
        for (name, usage) in usage_from_summary(&summary, client.namespace()) {
            merged.insert(name, usage);
        }
    }
    read_any.then_some(merged)
}

/// kubelet summary 的形状与 metrics-server 不同，语义也不同：这里只收本命名空间的
/// Pod（summary 是整台机器的，别的命名空间的 Pod 不该出现在这一面），并且用
/// `workingSetBytes` 而不是 `rssBytes`——工作集正是内存压力与 OOM 判定看的那一个数。
fn usage_from_summary(summary: &Value, namespace: &str) -> Vec<(String, PodUsage)> {
    let mut out = Vec::new();
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
        let mut usage = PodUsage {
            cpu: nanocores_millicores(pod.pointer("/cpu/usageNanoCores")),
            memory: bytes_at(pod.pointer("/memory/workingSetBytes")),
            // 进程数只在 Pod 这一级有：summary 不给每个容器分进程，给了也没有名字
            // 可拼成一个说得通的行。读不到就是 `None`，不写 0——0 是一句「没有进程」。
            processes: pod
                .pointer("/process_stats/process_count")
                .and_then(Value::as_u64),
            containers: std::collections::HashMap::new(),
        };
        for container in pod
            .get("containers")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let Some(container_name) = container.get("name").and_then(Value::as_str) else {
                continue;
            };
            usage.containers.insert(
                container_name.to_string(),
                (
                    nanocores_millicores(container.pointer("/cpu/usageNanoCores")),
                    bytes_at(container.pointer("/memory/workingSetBytes")),
                ),
            );
        }
        out.push((name.to_string(), usage));
    }
    out
}

/// summary 的数分两种写法：nanoCores 是十进制小数串或整数，字节量在 summary 里
/// 是十进制数字串。读不出来按 0 算——这里已经在「读到了」这条腿里，缺一个容器的
/// 数不等于整个 Pod 没有用量；缺的那一个会在容器那一层显示为 0，而 Pod 的总数
/// 仍由加总得到。没有来源的情况（两个接口都读不到）在调用点分成 `None` 处理。
fn nanocores_millicores(value: Option<&Value>) -> u64 {
    let Some(value) = value else { return 0 };
    let nanocores = value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()));
    match nanocores {
        Some(nanocores) if nanocores.is_finite() && nanocores > 0.0 => {
            (nanocores / 1_000_000.0).round() as u64
        }
        _ => 0,
    }
}

fn bytes_at(value: Option<&Value>) -> u64 {
    let Some(value) = value else { return 0 };
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
        .unwrap_or(0)
}

/// metrics-server 回的每个 Pod：各容器用量，加总成 Pod 的用量。
///
/// 只收容器粒度再自己加，是因为 Pod 那一级只有总数时，读者点进某个容器会看到
/// 一个和 Pod 对不上的数——两个数并排出现在同一屏，就会有一个被当成错的。
#[derive(Debug, Default)]
struct PodUsage {
    cpu: u64,
    memory: u64,
    /// kubelet summary 独有：这个 Pod 的进程数。metrics-server 不报，`None` 即读不到。
    processes: Option<u64>,
    containers: std::collections::HashMap<String, (u64, u64)>,
}

/// 读数表按「命名空间/名字」索引。
///
/// Pod、卷声明、事件的对象名都只在各自的命名空间里唯一：这一面读整个集群之后，
/// 两个命名空间各有一个 `redis-0` 是常态（平台自己就有一份，别的命名空间也可能有）。
/// 按裸名字索引会让其中一个认领另一个的用量、事件、卷字节数——错得没有声音，
/// 因为两行都填满了，只是填的是别人的数。
fn namespaced_key(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

fn usage_by_pod(list: &Value) -> std::collections::HashMap<String, PodUsage> {
    let mut usage = std::collections::HashMap::new();
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return usage;
    };
    for item in items {
        let Some(name) = item.pointer("/metadata/name").and_then(Value::as_str) else {
            continue;
        };
        let namespace = item
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .unwrap_or("");
        let mut pod = PodUsage::default();
        for container in item
            .get("containers")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let Some(container_name) = container.get("name").and_then(Value::as_str) else {
                continue;
            };
            let cpu = container
                .pointer("/usage/cpu")
                .and_then(Value::as_str)
                .and_then(millicores)
                .unwrap_or(0);
            let memory = container
                .pointer("/usage/memory")
                .and_then(Value::as_str)
                .and_then(quantity_bytes)
                .unwrap_or(0.0) as u64;
            pod.cpu += cpu;
            pod.memory += memory;
            pod.containers
                .insert(container_name.to_string(), (cpu, memory));
        }
        usage.insert(namespaced_key(namespace, name), pod);
    }
    usage
}

/// 卷声明与事件读的范围：整个集群，与 Pod 那一读同一个范围。
///
/// Pod 是按整个集群读的（采集器量的是整台宿主上的每一个容器，这一面只画自己命名
/// 空间里的，差出来的那些就只有进程读数、没有格子）。这两张表若还按命名空间读，
/// 别的命名空间里的 Pod 会带着空的卷声明与空事件出现在屏上——那不是它没有，是这一
/// 面没去找，而「没有」与「没读」在格子上同形。三张表都用 `namespace/name` 作键，
/// 扩大范围不会让不同命名空间的同名对象串在一起。
fn pvc_list_path() -> &'static str {
    "/api/v1/persistentvolumeclaims"
}

fn event_list_path() -> &'static str {
    "/api/v1/events"
}

/// 一条集群读在指标上留下的两格。**closed set，两格都在启动时写 0。**
pub const READ_OK: &str = "ok";
pub const READ_FAILED: &str = "failed";

/// 探针循环的名字：停在 `cogneva_loop_tick_age_seconds` 上，停滞规则按它自己
/// 声明的周期判。
pub const READ_PROBE_LOOP: &str = "cluster_read_probe";

/// 探针的周期。
///
/// 一分钟：比面板的 10 秒轮询慢得多，因为探针要答的是「这条读还通不通」，
/// 不是「此刻集群里有什么」。授权被收窄这种事以小时计，一分钟足够在有人第一次
/// 打开页面之前就把它变成一条告警；再快只是在 apiserver 上多花请求，而这一面
/// 每次只读一行（`limit=1`）。
// cap: bound — 只读探针的间隔，每次只读一行（limit=1），界的是面板读取对 apiserver 的压力
pub const READ_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// 探针要读的每一条集合：(资源名, 路径)。
///
/// 路径从**面板自己用的那几个构造函数**来，而不是在这里另抄一份：两份路径表迟早
/// 有一份先改，而那时探针会替一条已经不存在的读报健康，或者反过来把一条好读报红。
/// 资源名是 apiserver 的名字（`deployments`、`persistentvolumeclaims`），因为告警
/// 要拿它去点 RBAC 里缺的那条规则。
///
/// 不探的两条读，各有各的理由：`/version` 任何已认证身份都读得到，它失败说明的
/// 是 apiserver 不在，而那时节点那一读已经红了；`metrics.k8s.io/pods` 与
/// `nodes/<n>/proxy/stats/summary` 是**可选**用量来源，没装 metrics-server 的集群
/// 上它们本来就该 403，把可选的缺席报成故障就是一张永远亮着的红条。
pub fn read_targets(namespace: &str) -> Vec<(&'static str, String)> {
    let mut targets = vec![
        ("nodes", "/api/v1/nodes".to_string()),
        ("pods", "/api/v1/pods".to_string()),
        ("persistentvolumeclaims", pvc_list_path().to_string()),
        ("events", event_list_path().to_string()),
        (
            "resourcequotas",
            format!("/api/v1/namespaces/{namespace}/resourcequotas"),
        ),
        (
            "limitranges",
            format!("/api/v1/namespaces/{namespace}/limitranges"),
        ),
    ];
    for kind in WorkloadKind::ALL {
        let resource = kind.resource();
        // 裸 Pod 那条按命名空间读，与上面集群范围那一读是**同一条授权**
        // （`pods` 的 list 不分范围），探两遍只会让同一个故障在两格里各报一次。
        if targets.iter().any(|(name, _)| *name == resource) {
            continue;
        }
        targets.push((resource, kind.list_path(namespace)));
    }
    targets
}

/// 谁在探：这个进程跑成的那个工作负载的名字。
///
/// 这条读数由两个 Deployment 发布（网关与进化），它们是同一个二进制、同一套路由，
/// 但是**两个 ServiceAccount**——授权面上一有问题，两边可以一边通一边不通。只按
/// `resource` 读，响的那一条既说不出是哪个进程，也会把两边的失败并成一行：2026-10-28
/// 实测，进化进程的四条集群读 403 时网关那一侧同样有计数，两边的失败挤在同一个
/// dedup key 下，读者拿到的是「nodes 读不通」，得自己去翻抓取面才知道说的是谁。
///
/// 取 `COGNEVA_DEPLOYMENT_NAME`（与任务事件、用量保留那边同一个拼写），不取抓取配置
/// 的 `job`：那个名字是 Prometheus 起的，改一次 scrape config 就把一条正在响的告警
/// 拆成两条，滚动更新时又不会变。没设这个变量（本机跑、没经过清单）时不写这个标签，
/// 而不是写一个空串——空串会被读成一个部署名。
fn deployment_label() -> Option<String> {
    std::env::var(cog_core::env::DEPLOYMENT_NAME_ENV)
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

/// 一格读数的标签。`deployment` 缺席时**不写这个键**，不写空串：空串会被读成
/// 一个叫「」的部署名，而那正好是「读的人以为有人报过」的形状。
fn read_labels(
    resource: &str,
    outcome: &str,
    deployment: Option<&str>,
) -> std::collections::HashMap<String, String> {
    let mut labels = std::collections::HashMap::new();
    labels.insert(crate::LABEL_OUTCOME.to_string(), outcome.to_string());
    labels.insert(crate::LABEL_RESOURCE.to_string(), resource.to_string());
    if let Some(deployment) = deployment {
        labels.insert(crate::LABEL_DEPLOYMENT.to_string(), deployment.to_string());
    }
    labels
}

/// 把一条读的结局写进指标。没有指标后端时什么都不写：这一面本身不依赖它。
async fn publish_read(
    metrics: &Option<std::sync::Arc<dyn cog_core::MetricsBackend>>,
    resource: &str,
    outcome: &str,
    deployment: Option<&str>,
    value: f64,
) {
    let Some(backend) = metrics.as_ref() else {
        return;
    };
    let labels = read_labels(resource, outcome, deployment);
    let _ = backend
        .record_counter(cog_core::metric_names::CLUSTER_READ_TOTAL, value, labels)
        .await;
}

#[cfg(test)]
mod read_label_tests {
    use super::*;

    /// 两个进程的失败不能被并成一格。这条读数由网关与进化两个 Deployment 发布，
    /// 它们是同一个二进制、两个 ServiceAccount：授权坏在绑定时只会坏一边，而
    /// 少了 `deployment` 这个标签，响的那条既说不出是谁，抓取面上两边也合成一行。
    #[test]
    fn a_read_is_attributed_to_the_deployment_that_ran_it() {
        let gateway = read_labels("nodes", READ_FAILED, Some("cogneva"));
        let evolution = read_labels("nodes", READ_FAILED, Some("cogneva-evolution"));
        assert_eq!(
            gateway.get(crate::LABEL_DEPLOYMENT).map(String::as_str),
            Some("cogneva")
        );
        assert_eq!(
            evolution.get(crate::LABEL_DEPLOYMENT).map(String::as_str),
            Some("cogneva-evolution")
        );
        assert_ne!(
            gateway, evolution,
            "两个进程的同一格读数带着同一组标签：它们在抓取面上就是同一行"
        );
    }

    /// 不知道自己是哪个部署时，`deployment` 这个键缺席，而不是留一个空串——
    /// 空串是一个值，读者会以为有一个叫空名字的部署在报数。
    #[test]
    fn an_unnamed_process_writes_no_deployment_label_at_all() {
        let labels = read_labels("nodes", READ_OK, None);
        assert!(
            !labels.contains_key(crate::LABEL_DEPLOYMENT),
            "没设部署名时写了一个空串：那会被读成一个部署"
        );
    }
}

/// 启动时把每一格写成 0。
///
/// 不写这一步，一个从没探过的网关与一条根本不发布这条读数的网关在抓取面上同形：
/// 两格都缺席。写了 0 之后，缺席只意味着「这个进程不发布这条读数」，0 意味着探针
/// 跑过、每一条读都答了——那是两种不同的状态，修法也不同。
pub async fn publish_vocabulary(metrics: &Option<std::sync::Arc<dyn cog_core::MetricsBackend>>) {
    let namespace = KubeClient::in_cluster()
        .map(|client| client.namespace().to_string())
        .unwrap_or_else(|_| "cogneva".to_string());
    let deployment = deployment_label();
    for (resource, _) in read_targets(&namespace) {
        for outcome in [READ_OK, READ_FAILED] {
            publish_read(metrics, resource, outcome, deployment.as_deref(), 0.0).await;
        }
    }
}

/// 探一遍这一面要读的每一条集合，把结局写进 `cogneva_cluster_read_total`。
pub(crate) async fn probe_reads(
    client: &KubeClient,
    metrics: &Option<std::sync::Arc<dyn cog_core::MetricsBackend>>,
) {
    let deployment = deployment_label();
    for (resource, path) in read_targets(client.namespace()) {
        // `limit=1`：探的是这条读通不通，不是它返回什么。一次读回整个集群的 Pod
        // 只为判权限，是把每分钟一次的探针变成了真正的负载。
        let outcome = match client.get_json(&format!("{path}?limit=1")).await {
            Ok(_) => READ_OK,
            Err(error) => {
                // 读数只说「这条读没通」，说不到「为什么」：403 是授权被收窄、401 是
                // 令牌没到、超时是 apiserver 忙。告警文案让读的人去看 Pod 日志里的状态行，
                // 所以这里必须真的写一行——否则那句话点的是一个空的日志。
                tracing::warn!(
                    resource,
                    %error,
                    "cluster read failed; the status line from the apiserver is in this message"
                );
                READ_FAILED
            }
        };
        publish_read(metrics, resource, outcome, deployment.as_deref(), 1.0).await;
    }
}

/// 起探针循环。
///
/// 为什么要循环、而不是只在请求路径上计数：这一面的读**只在有人打开页面时发生**
/// （`/api/v1/cluster/topology` 是按请求算的），所以请求路径上的计数器只在一个浏览器
/// 标签开着的时候才动——那是「有人正在看」，不是监控。2026-10-27 的现场是：一条
/// ClusterRole 少授了两条集合读，拓扑面在屏上整块空白了几个小时，没有任何读数说出
/// 这件事，因为那段时间没人在看。
///
/// 不在集群里（本机开发、没有 ServiceAccount）时启动即返回：探不到不是错，缺的是
/// 数据源，而那条事实由「序列缺席」表达。
pub fn spawn_read_probe(
    metrics: Option<std::sync::Arc<dyn cog_core::MetricsBackend>>,
    shutdown: cog_core::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let stop = shutdown.clone();
    cog_core::loop_health::spawn(
        READ_PROBE_LOOP,
        cog_core::loop_health::Cadence::Periodic(READ_PROBE_INTERVAL),
        shutdown,
        move |beat| {
            let metrics = metrics.clone();
            let stop = stop.clone();
            async move {
                let Ok(client) = KubeClient::in_cluster() else {
                    return;
                };
                let mut interval = tokio::time::interval(READ_PROBE_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = stop.wait() => return,
                        _ = interval.tick() => {}
                    }
                    probe_reads(&client, &metrics).await;
                }
            }
        },
    )
}

fn claims_by_name(list: &Value) -> std::collections::HashMap<String, ClaimView> {
    let mut claims = std::collections::HashMap::new();
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return claims;
    };
    for item in items {
        let Some(name) = item.pointer("/metadata/name").and_then(Value::as_str) else {
            continue;
        };
        let namespace = item
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .unwrap_or("");
        claims.insert(
            namespaced_key(namespace, name),
            ClaimView {
                name: name.to_string(),
                phase: item
                    .pointer("/status/phase")
                    .and_then(Value::as_str)
                    .unwrap_or("Unknown")
                    .to_string(),
                storage_class: item
                    .pointer("/spec/storageClassName")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                capacity_bytes: item
                    .pointer("/status/capacity/storage")
                    .and_then(Value::as_str)
                    .and_then(quantity_bytes)
                    .map(|bytes| bytes as u64),
                access_modes: item
                    .pointer("/status/accessModes")
                    .and_then(Value::as_array)
                    .map(|modes| {
                        modes
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
                volume_name: item
                    .pointer("/spec/volumeName")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                measured_bytes: None,
            },
        );
    }
    claims
}

/// 组装一个 Pod 读数要用到的四份外部读数。分成一个结构体是因为它们各自的缺失
/// 含义不同，但拼装方式一样：缺哪一份，就少那一层，别的层照旧。
#[derive(Debug, Default)]
struct Sources {
    events: std::collections::HashMap<String, Vec<EventView>>,
    usage: std::collections::HashMap<String, PodUsage>,
    claims: std::collections::HashMap<String, ClaimView>,
    /// 卷的实际占用，按声明名索引，来自平台自己的走查端。
    measured: std::collections::HashMap<String, u64>,
}

/// 每个节点上都有一个 claim-store，它走查节点存储根，把每个卷的真实字节数发布在
/// `/metrics` 上。这一面直接读那些端点：声明写了多大、盘上真的占了多少，两个数
/// 并排才是「资源账本」。
///
/// 走 Pod IP 而不是 Service 名：那个 Service 是 headless 的，一条 DNS 查询会得到
/// 每台节点一个地址，随便取一个就只看到一台机器的卷。走查端只认自己的节点，所以
/// 这里把每个端点的页都读一遍再合并——同一个声明出现在两台机器上是不可能的
/// （local-path 的卷只在一块盘上），后读到的不会盖掉先读到的另一个值。
///
/// 走查端轮询单次的上限：2s。
///
/// 这一面是轮询的，一个不响的走查端不能把整次读取拖住；共用客户端的默认期限
/// 是给 apiserver 往返的，套在这里会把一次前端刷新拖到那个数。
// cap: bound — 走查里轮询一次的上限；apiserver 往返本来就快，套大了会把一次前端刷新拖到那个数
const WALKTHROUGH_POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// 每个节点上都有一个 claim-store，它走查节点存储根，把每个卷的真实字节数发布在
/// `/metrics` 上。这一面直接读那些端点：声明写了多大、盘上真的占了多少，两个数
/// 并排才是「资源账本」。
///
/// 走 Pod IP 而不是 Service 名：那个 Service 是 headless 的，一条 DNS 查询会得到
/// 每台节点一个地址，随便取一个就只看到一台机器的卷。走查端只认自己的节点，所以
/// 这里把每个端点的页都读一遍再合并——同一个声明出现在两台机器上是不可能的
/// （local-path 的卷只在一块盘上），后读到的不会盖掉先读到的另一个值。
///
/// 读不到就是读不到：端点没起来、端口换了、页面里没有这个声明，都只让这一格是
/// `None`，不拿声明量去填——那是另一个数。
async fn measured_claim_bytes(
    http: &reqwest::Client,
    pods: Option<&Value>,
    claims: &std::collections::HashMap<String, ClaimView>,
) -> std::collections::HashMap<String, u64> {
    let mut measured = std::collections::HashMap::new();
    if claims.is_empty() {
        return measured;
    }
    let Some(items) = pods.and_then(Value::as_array) else {
        return measured;
    };
    for pod in items {
        let is_scanner = pod
            .pointer("/metadata/labels/app.kubernetes.io~1component")
            .and_then(Value::as_str)
            == Some("claim-store");
        if !is_scanner {
            continue;
        }
        let Some(ip) = pod.pointer("/status/podIP").and_then(Value::as_str) else {
            continue;
        };
        let Some(port) = pod
            .pointer("/spec/containers/0/ports")
            .and_then(Value::as_array)
            .and_then(|ports| {
                ports
                    .iter()
                    .find(|port| port.get("name").and_then(Value::as_str) == Some("http"))
                    .and_then(|port| port.get("containerPort"))
                    .and_then(Value::as_u64)
            })
        else {
            continue;
        };
        let url = format!("http://{ip}:{port}/metrics");
        // 期限压得很短、且挂在请求上：这一面是轮询的，一个不响的走查端不能把
        // 整次读取拖住；而共用客户端的默认期限是给 apiserver 往返的，套在这里
        // 会把一次前端刷新拖到那个数。
        let Ok(response) = http
            .get(&url)
            .timeout(WALKTHROUGH_POLL_TIMEOUT)
            .send()
            .await
        else {
            continue;
        };
        let Ok(text) = response.text().await else {
            continue;
        };
        for (key, claim) in claims {
            // 暴露面上印的是卷声明自己的名字，不带命名空间；表里的键带命名空间。
            // 拿键去匹配就一条都对不上，屏上每一块盘都是「没量到」。
            if let Some(bytes) =
                cog_core::claim_footprint::used_bytes_from_exposition(&text, &claim.name)
            {
                measured.insert(key.clone(), bytes);
            }
        }
    }
    measured
}

/// 把 Pod 挂到它所在的节点上，挂不上的按**为什么挂不上**分两栏。
///
/// 单独成一个函数是因为这条映射是这一面唯一会错的地方，而且错法不止一种：
/// - Pod 自己没有节点（`spec.nodeName` 空）：调度器的事实，进 `unscheduled`；
/// - Pod 写着在某台节点上，但这台节点不在这次读到的节点列表里：节点读失败或者
///   节点刚被删，进 `unplaced`。
///
/// 两种合成一栏会让节点读失败时的整屏 Pod 都变成「没有被调度」——那不是少一点
/// 信息，那是一句假话，而它恰好出现在最需要看清集群的时候。
fn place_pods(cluster: &mut ClusterView, pods: Vec<PodView>) {
    for pod in pods {
        let Some(node_name) = pod.node.clone() else {
            cluster.unscheduled.push(pod);
            continue;
        };
        let placed = cluster
            .nodes
            .iter_mut()
            .find(|candidate| candidate.name == node_name);
        match placed {
            Some(node) => {
                node.cpu_requested_millicores += pod.cpu_requested_millicores;
                node.memory_requested_bytes += pod.memory_requested_bytes;
                node.pods.push(pod);
            }
            None => cluster.unplaced.push(pod),
        }
    }
    for node in &mut cluster.nodes {
        node.pods
            .sort_by(|a, b| a.name.cmp(&b.name).then(a.namespace.cmp(&b.namespace)));
    }
    cluster.unscheduled.sort_by(|a, b| a.name.cmp(&b.name));
    cluster.unplaced.sort_by(|a, b| a.name.cmp(&b.name));
}

fn node_view(node: &Value) -> NodeView {
    let name = node
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or("<没有名字>")
        .to_string();

    let labels = node.pointer("/metadata/labels").and_then(Value::as_object);
    let mut roles: Vec<String> = labels
        .map(|labels| {
            labels
                .keys()
                .filter_map(|key| key.strip_prefix("node-role.kubernetes.io/"))
                .filter(|role| !role.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if roles.is_empty() {
        roles.push("worker".to_string());
    }
    roles.sort();

    let ready = node
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .map(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(Value::as_str) == Some("Ready")
                    && condition.get("status").and_then(Value::as_str) == Some("True")
            })
        })
        .unwrap_or(false);

    // allocatable 才是能排的量；capacity 是机器的物理量。调度看前者，所以这里读
    // 前者，读不到才退回 capacity——两者混着用会让「还剩多少」差出一整块系统预留。
    let allocatable = node
        .pointer("/status/allocatable")
        .or_else(|| node.pointer("/status/capacity"));

    let process_census_at = crate::process_reports::census_at(&name);
    NodeView {
        name,
        ready,
        process_census_at,
        roles,
        conditions: conditions_of(node),
        cpu_allocatable_millicores: allocatable
            .and_then(|value| value.get("cpu"))
            .and_then(Value::as_str)
            .and_then(millicores),
        memory_allocatable_bytes: allocatable
            .and_then(|value| value.get("memory"))
            .and_then(Value::as_str)
            .and_then(quantity_bytes)
            .map(|bytes| bytes as u64),
        pods_allocatable: allocatable
            .and_then(|value| value.get("pods"))
            .and_then(Value::as_str)
            .and_then(|text| text.parse::<u64>().ok()),
        cpu_requested_millicores: 0,
        memory_requested_bytes: 0,
        cpu_used_millicores: None,
        memory_used_bytes: None,
        census_containers: None,
        unclaimed_containers: None,
        unclaimed: Vec::new(),
        pods: Vec::new(),
    }
}

/// 这一面读到的 Pod 认领了哪些容器，以及它们自己是谁。
///
/// 采集器按容器 id 入账（cgroup 路径的末段就是它），Pod 状态里也带着同一个 id，
/// 两边不用交换名字就能对上。`initContainerStatuses` 也算：init 容器在跑的时候，
/// 宿主上同样有一个叶子，不认领它就会被算成「这一面读不到的容器」。
///
/// Pod 的 uid 单独收一遍，因为沙箱容器（pause）不在任何 `containerStatuses` 里，
/// 而它在宿主上有自己的一份进程表。没有这一组，每一个 Pod 都会在缺口里留下一条
/// 「没人认领的容器」——那不是宿主上的野进程，是这一面自己没认出来。
fn claims_of_pods(pods: Option<&Value>) -> crate::process_reports::Claims {
    let mut claims = crate::process_reports::Claims::default();
    let Some(items) = pods.and_then(Value::as_array) else {
        return claims;
    };
    for pod in items {
        if let Some(uid) = pod.pointer("/metadata/uid").and_then(Value::as_str) {
            if !uid.is_empty() {
                claims.pods.insert(uid.to_string());
            }
        }
        for field in ["/status/containerStatuses", "/status/initContainerStatuses"] {
            let Some(statuses) = pod.pointer(field).and_then(Value::as_array) else {
                continue;
            };
            for status in statuses {
                let Some(id) = status.get("containerID").and_then(Value::as_str) else {
                    continue;
                };
                let id = crate::process_reports::container_id_of(id);
                if !id.is_empty() {
                    claims.containers.insert(id.to_string());
                }
            }
        }
    }
    claims
}

fn pod_view(pod: &Value, sources: &Sources) -> PodView {
    let usage = &sources.usage;
    let events = &sources.events;
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let name = pod
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or("<没有名字>")
        .to_string();
    let phase = pod
        .pointer("/status/phase")
        .and_then(Value::as_str)
        .unwrap_or("Unknown")
        .to_string();
    let node = pod
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let (cpu, memory) = pod_requests(pod);
    let statuses = pod
        .pointer("/status/containerStatuses")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let specs = pod
        .pointer("/spec/containers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let pod_usage = usage.get(&namespaced_key(&namespace, &name));
    let container_usage = pod_usage
        .map(|pod| pod.containers.clone())
        .unwrap_or_default();

    let containers: Vec<ContainerView> = specs
        .iter()
        .map(|spec| {
            let container_name = spec.get("name").and_then(Value::as_str).unwrap_or("");
            let status = statuses
                .iter()
                .find(|status| status.get("name").and_then(Value::as_str) == Some(container_name));
            ContainerView {
                name: container_name.to_string(),
                image: spec
                    .get("image")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                ready: status
                    .and_then(|status| status.get("ready"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                restarts: status
                    .and_then(|status| status.get("restartCount"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as u32,
                state: container_state(status),
                ports: container_ports(spec),
                cpu_request_millicores: spec
                    .pointer("/resources/requests/cpu")
                    .and_then(Value::as_str)
                    .and_then(millicores),
                memory_request_bytes: spec
                    .pointer("/resources/requests/memory")
                    .and_then(Value::as_str)
                    .and_then(quantity_bytes)
                    .map(|bytes| bytes as u64),
                cpu_limit_millicores: spec
                    .pointer("/resources/limits/cpu")
                    .and_then(Value::as_str)
                    .and_then(millicores),
                memory_limit_bytes: spec
                    .pointer("/resources/limits/memory")
                    .and_then(Value::as_str)
                    .and_then(quantity_bytes)
                    .map(|bytes| bytes as u64),
                last_termination: status
                    .and_then(|status| status.pointer("/lastState/terminated"))
                    .map(termination_view),
                mounts: container_mounts(spec),
                probes: ["livenessProbe", "readinessProbe", "startupProbe"]
                    .into_iter()
                    .filter(|probe| spec.get(*probe).is_some())
                    .map(|probe| probe.trim_end_matches("Probe").to_string())
                    .collect(),
                cpu_used_millicores: container_usage.get(container_name).map(|(cpu, _)| *cpu),
                memory_used_bytes: container_usage
                    .get(container_name)
                    .map(|(_, memory)| *memory),
                processes: status
                    .and_then(|status| status.get("containerID"))
                    .and_then(Value::as_str)
                    .map(crate::process_reports::container_id_of)
                    .filter(|id| !id.is_empty())
                    .and_then(crate::process_reports::latest)
                    .map(|report| report.processes),
            }
        })
        .collect();

    let ready = !containers.is_empty() && containers.iter().all(|container| container.ready);
    let restarts = containers
        .iter()
        .map(|container| container.restarts)
        .max()
        .unwrap_or(0);

    let volumes = pod_volumes(pod, sources, &namespace);
    let pod_events = events
        .get(&namespaced_key(&namespace, &name))
        .cloned()
        .unwrap_or_default();
    let conditions = conditions_of(pod);
    let process_census_at = node.as_deref().and_then(crate::process_reports::census_at);

    PodView {
        namespace,
        name,
        phase,
        ready,
        restarts,
        node,
        cpu_requested_millicores: cpu,
        memory_requested_bytes: memory,
        containers,
        conditions,
        cpu_used_millicores: pod_usage.map(|pod| pod.cpu),
        memory_used_bytes: pod_usage.map(|pod| pod.memory),
        processes: pod_usage.and_then(|pod| pod.processes),
        process_census_at,
        volumes,
        events: pod_events,
    }
}

fn termination_view(value: &Value) -> TerminationView {
    TerminationView {
        // 没有 reason 的终止（正常退出、被删）不该被写成空字符串：退出码 0 就是
        // 「自己跑完了」，写 `Completed` 是 k8s 对这个情况的叫法。
        reason: value
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                if value.get("exitCode").and_then(Value::as_i64) == Some(0) {
                    "Completed".to_string()
                } else {
                    "Error".to_string()
                }
            }),
        exit_code: value.get("exitCode").and_then(Value::as_i64).unwrap_or(-1),
        message: value
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string),
        finished_at: value
            .get("finishedAt")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

/// 读 `status.conditions`（节点与 Pod 是同一个形状）。`ok` 只看 `status == "True"`：
/// Kubernetes 用的是字符串三元组，缺字段按「没成立」处理——把未知读成成立，会让一个
/// 卡住的 Pod 显示成正常。
fn conditions_of(object: &Value) -> Vec<ConditionView> {
    object
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .map(|conditions| {
            conditions
                .iter()
                .filter_map(|condition| {
                    Some(ConditionView {
                        kind: condition.get("type").and_then(Value::as_str)?.to_string(),
                        ok: condition.get("status").and_then(Value::as_str) == Some("True"),
                        reason: condition
                            .get("reason")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        message: condition
                            .get("message")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn container_ports(spec: &Value) -> Vec<PortView> {
    spec.get("ports")
        .and_then(Value::as_array)
        .map(|ports| {
            ports
                .iter()
                .filter_map(|port| {
                    Some(PortView {
                        name: port.get("name").and_then(Value::as_str).map(str::to_string),
                        container_port: port.get("containerPort").and_then(Value::as_u64)? as u16,
                        protocol: port
                            .get("protocol")
                            .and_then(Value::as_str)
                            .unwrap_or("TCP")
                            .to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn container_mounts(spec: &Value) -> Vec<MountView> {
    spec.get("volumeMounts")
        .and_then(Value::as_array)
        .map(|mounts| {
            mounts
                .iter()
                .filter_map(|mount| {
                    Some(MountView {
                        name: mount.get("name").and_then(Value::as_str)?.to_string(),
                        mount_path: mount
                            .get("mountPath")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        read_only: mount
                            .get("readOnly")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// 卷的类型就是除 `name` 之外那个键。读不出来（投影卷、CSI 内联卷等）时写 `other`，
/// 不猜一个名字——猜出来的类型会让读者去改错的东西。
fn pod_volumes(pod: &Value, sources: &Sources, namespace: &str) -> Vec<VolumeView> {
    pod.pointer("/spec/volumes")
        .and_then(Value::as_array)
        .map(|volumes| {
            volumes
                .iter()
                .filter_map(|volume| {
                    let object = volume.as_object()?;
                    let name = object.get("name").and_then(Value::as_str)?.to_string();
                    let kind = object
                        .keys()
                        .find(|key| key.as_str() != "name")
                        .cloned()
                        .unwrap_or_else(|| "other".to_string());
                    let claim = object
                        .get("persistentVolumeClaim")
                        .and_then(|claim| claim.get("claimName"))
                        .and_then(Value::as_str)
                        .and_then(|claim_name| {
                            sources.claims.get(&namespaced_key(namespace, claim_name))
                        })
                        .map(|claim| ClaimView {
                            name: claim.name.clone(),
                            phase: claim.phase.clone(),
                            storage_class: claim.storage_class.clone(),
                            capacity_bytes: claim.capacity_bytes,
                            access_modes: claim.access_modes.clone(),
                            volume_name: claim.volume_name.clone(),
                            measured_bytes: sources
                                .measured
                                .get(&namespaced_key(namespace, &claim.name))
                                .copied(),
                        });
                    Some(VolumeView { name, kind, claim })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// 按「对象名 → 事件」分组，每个对象只留最近五条。
///
/// 事件带 `involvedObject.kind`，这里只收 Pod：同名的 Service 与 Pod 会撞在一起，
/// 把它们混进一份列表，读者会拿 Service 的告警去改 Pod。
fn events_by_object(list: &Value) -> std::collections::HashMap<String, Vec<EventView>> {
    let mut grouped: std::collections::HashMap<String, Vec<EventView>> =
        std::collections::HashMap::new();
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return grouped;
    };
    for item in items {
        if item.pointer("/involvedObject/kind").and_then(Value::as_str) != Some("Pod") {
            continue;
        }
        let Some(name) = item.pointer("/involvedObject/name").and_then(Value::as_str) else {
            continue;
        };
        let namespace = item
            .pointer("/involvedObject/namespace")
            .and_then(Value::as_str)
            .or_else(|| item.pointer("/metadata/namespace").and_then(Value::as_str))
            .unwrap_or("");
        grouped
            .entry(namespaced_key(namespace, name))
            .or_default()
            .push(EventView {
                kind: item
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("Normal")
                    .to_string(),
                reason: item
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                message: item
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                count: item.get("count").and_then(Value::as_u64).unwrap_or(1) as u32,
                last_seen: item
                    .get("lastTimestamp")
                    .or_else(|| item.get("eventTime"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
    }
    for events in grouped.values_mut() {
        // `lastTimestamp` 是 Kubernetes 自己写的时间；缺失时保持读进来的顺序，
        // 不拿本机时间补一个——补出来的时间会让「最近发生」排错。
        events.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
        events.truncate(5);
    }
    grouped
}

fn container_state(status: Option<&Value>) -> String {
    let Some(status) = status else {
        return "unknown".to_string();
    };
    let Some(state) = status.get("state") else {
        return "unknown".to_string();
    };
    for (key, label) in [
        ("running", "running"),
        ("terminated", "terminated"),
        ("waiting", "waiting"),
    ] {
        if let Some(value) = state.get(key) {
            let reason = value.get("reason").and_then(Value::as_str);
            return match (key, reason) {
                ("running", _) => label.to_string(),
                (_, Some(reason)) => format!("{label}:{reason}"),
                (_, None) => label.to_string(),
            };
        }
    }
    "unknown".to_string()
}

/// 一个 Pod 向调度器申报的量。
///
/// 普通容器相加；init 容器取最大值（它们依次跑，不叠加）；两者取大者。这正是
/// kube-scheduler 的算法——少算一个 init 容器，就会出现「拓扑上还剩很多、新 Pod
/// 却排不进去」这种谁都解释不了的画面。
fn pod_requests(pod: &Value) -> (u64, u64) {
    let sum = |path: &str, pick: fn(&str) -> Option<u64>| -> (u64, u64) {
        let Some(containers) = pod.pointer(path).and_then(Value::as_array) else {
            return (0, 0);
        };
        let mut cpu = 0;
        let mut memory = 0;
        for container in containers {
            let requests = container.pointer("/resources/requests");
            cpu += requests
                .and_then(|value| value.get("cpu"))
                .and_then(Value::as_str)
                .and_then(pick)
                .unwrap_or(0);
            memory += requests
                .and_then(|value| value.get("memory"))
                .and_then(Value::as_str)
                .and_then(quantity_bytes)
                .unwrap_or(0.0) as u64;
        }
        (cpu, memory)
    };

    let (regular_cpu, regular_memory) = sum("/spec/containers", millicores);

    let init = pod
        .pointer("/spec/initContainers")
        .and_then(Value::as_array)
        .map(|containers| {
            containers.iter().fold((0, 0), |acc, container| {
                let requests = container.pointer("/resources/requests");
                let cpu = requests
                    .and_then(|value| value.get("cpu"))
                    .and_then(Value::as_str)
                    .and_then(millicores)
                    .unwrap_or(0);
                let memory = requests
                    .and_then(|value| value.get("memory"))
                    .and_then(Value::as_str)
                    .and_then(quantity_bytes)
                    .unwrap_or(0.0) as u64;
                (acc.0.max(cpu), acc.1.max(memory))
            })
        })
        .unwrap_or((0, 0));

    (regular_cpu.max(init.0), regular_memory.max(init.1))
}

fn millicores(text: &str) -> Option<u64> {
    quantity_bytes(text).map(|value| (value * 1000.0).round() as u64)
}

// ─────────────────── 资源头寸：配额、默认限额、部署版本 ───────────────────
//
// 拓扑回答「东西跑在哪、那台机器还剩多少」，这一节回答另一半：「这个命名空间
// 还剩多少配额、新 Pod 排不排得进来、每个工作负载现在跑的是第几版」。
//
// 配额与限额是准入前的账，部署的 revision 是变更落到了第几版。两者都读 apiserver，
// 不经过任何缓存：一张配额卡上的数如果来自别处，它就和「申请被拒」不是同一个数了。

use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct QuotaEntry {
    /// 资源名原样来自 apiserver：`requests.cpu`、`limits.memory`、`pods`……
    pub resource: String,
    /// hard 与 used 保留 apiserver 的写法（milli 与二进制后缀都原样）。换算是读者的事，
    /// 这一层只多做一件事：给出两者相除的百分比，供界面画进度条。
    pub hard: String,
    pub used: String,
    /// 两边都认得出同一维度才有值。认不出就不填——两个不同维度的数硬除出来的
    /// 百分比不是读数，是发明。
    pub percent: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuotaView {
    pub name: String,
    pub entries: Vec<QuotaEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LimitEntry {
    /// `Container` 或 `Pod`。
    pub kind: String,
    pub resource: String,
    pub max: Option<String>,
    pub min: Option<String>,
    /// 没写 requests 时准入补上的默认值。
    pub default: Option<String>,
    pub default_request: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LimitView {
    pub name: String,
    pub entries: Vec<LimitEntry>,
}

#[derive(Debug, Serialize)]
pub struct ResourceReport {
    pub namespace: String,
    /// 读不到时写明原因。空列表本身不说明「配额是零」。
    pub error: Option<String>,
    pub quotas: Vec<QuotaView>,
    pub limits: Vec<LimitView>,
    pub collected_at: DateTime<Utc>,
}

/// 命名空间里的一个工作负载，不分工种。
///
/// 这一面原先只读 Deployment，理由是版本号与滚动进度只有它有。但「这一版滚到第几步」
/// 只是这一面要答的一半问题，另一半是「这个命名空间里到底跑着什么」——只列
/// Deployment 会让 postgres（StatefulSet）、三个常驻采集器（DaemonSet）、每次主线与
/// 演练各一个 Job 从页面上消失，而页面看起来是完整的。
///
/// 副本那四格按工种各自的读法填：Deployment / StatefulSet 读 status 的四个计数，
/// DaemonSet 读 desiredNumberScheduled / numberReady 那组，Job 的「到位」是
/// succeeded，CronJob 没有副本概念、只有在跑的 Job 数。**某个工种没有的读法一律给
/// None，不给 0**：0 是一个读数，None 是「这个工种没有这一格」，混成一个数读者就
/// 会拿「0 个新版本」去回答「没滚上去」。
#[derive(Debug, Clone, Serialize)]
pub struct WorkloadView {
    /// 工种：Deployment / StatefulSet / DaemonSet / CronJob / Job / Pod。
    pub kind: String,
    pub name: String,
    /// 裸 Pod 的相位（`Pending` / `Running` / `Succeeded` / `Failed`），别的工种为 `None`。
    ///
    /// 没有这一格，一个已经跑完的裸 Pod（评测剩下的、探针付完账留下的）在屏上就是
    /// `0 / 1`——和「有副本起不来」同一个形状，读作坏了，而它其实只是结束了。
    pub phase: Option<String>,
    pub ready: Option<u32>,
    pub desired: Option<u32>,
    pub updated: Option<u32>,
    pub available: Option<u32>,
    /// `deployment.kubernetes.io/revision`：这一版是第几次改动滚动上去的。只有
    /// Deployment 用这个数字；StatefulSet 的历史是 ControllerRevision 对象，不是
    /// 一个可以印在行上的序号。
    pub revision: Option<String>,
    pub images: Vec<String>,
    /// 含 `Progressing` / `Available` 与滚动卡住时的 `ProgressDeadlineExceeded`。
    pub conditions: Vec<ConditionView>,
}

#[derive(Debug, Serialize)]
pub struct WorkloadsReport {
    pub namespace: String,
    pub error: Option<String>,
    pub workloads: Vec<WorkloadView>,
    pub collected_at: DateTime<Utc>,
}

/// 把 apiserver 的数量写法换算成同一维度的数，只为算「用了百分之几」。
///
/// 二进制后缀、十进制后缀与 milli 三种都要认；认不出就返回 None。同一维度的两个数
/// （核数与毫核、GiB 与 MiB）才有比值，跨维度的两个数之间没有比值可言。
fn quantity_units(text: &str) -> Option<f64> {
    const BINARY: [(&str, f64); 5] = [
        ("Ki", 1024.0),
        ("Mi", 1024.0 * 1024.0),
        ("Gi", 1024.0 * 1024.0 * 1024.0),
        ("Ti", 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("Pi", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ];
    const DECIMAL: [(&str, f64); 4] = [("k", 1e3), ("M", 1e6), ("G", 1e9), ("T", 1e12)];

    let text = text.trim();
    for (suffix, scale) in BINARY {
        if let Some(number) = text.strip_suffix(suffix) {
            return number.trim().parse::<f64>().ok().map(|value| value * scale);
        }
    }
    for (suffix, scale) in DECIMAL {
        if let Some(number) = text.strip_suffix(suffix) {
            return number.trim().parse::<f64>().ok().map(|value| value * scale);
        }
    }
    if let Some(number) = text.strip_suffix('m') {
        return number
            .trim()
            .parse::<f64>()
            .ok()
            .map(|value| value / 1000.0);
    }
    text.parse::<f64>().ok()
}

fn quota_percent(hard: &str, used: &str) -> Option<f64> {
    let hard = quantity_units(hard)?;
    if hard <= 0.0 {
        return None;
    }
    let used = quantity_units(used)?;
    // 一位小数：界面上画进度条与写百分比，再多一位也没有人用得到。
    Some(((used / hard) * 1000.0).round() / 10.0)
}

fn quotas_from(list: &Value) -> Vec<QuotaView> {
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .map(|item| {
            let hard = item.pointer("/status/hard").and_then(Value::as_object);
            let used = item.pointer("/status/used").and_then(Value::as_object);
            // 取两边的并集：一个刚建出来的配额对象可能只在 hard 里出现，
            // 只按 used 列会把「已经限住但还没人用」的资源整条漏掉。
            let mut resources: BTreeMap<&str, ()> = BTreeMap::new();
            for map in [hard, used].into_iter().flatten() {
                for resource in map.keys() {
                    resources.insert(resource.as_str(), ());
                }
            }
            let entries = resources
                .keys()
                .map(|resource| {
                    let hard_text = hard
                        .and_then(|map| map.get(*resource))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let used_text = used
                        .and_then(|map| map.get(*resource))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let percent = if hard_text.is_empty() || used_text.is_empty() {
                        None
                    } else {
                        quota_percent(&hard_text, &used_text)
                    };
                    QuotaEntry {
                        resource: (*resource).to_string(),
                        hard: hard_text,
                        used: used_text,
                        percent,
                    }
                })
                .collect();
            QuotaView {
                name: item
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                entries,
            }
        })
        .collect()
}

fn limits_from(list: &Value) -> Vec<LimitView> {
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .map(|item| {
            // 一个 LimitRange 的一个条目同时带 max / min / default / defaultRequest
            // 四张表。按 (kind, resource) 并成一行：读的人问的是「内存这一档的上下限
            // 与默认值是多少」，不是「spec.limits 里有几条」。
            let mut merged: BTreeMap<(String, String), LimitEntry> = BTreeMap::new();
            for spec in item
                .pointer("/spec/limits")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let kind = spec
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                for field in ["max", "min", "default", "defaultRequest"] {
                    let Some(map) = spec.get(field).and_then(Value::as_object) else {
                        continue;
                    };
                    for (resource, value) in map {
                        let entry = merged
                            .entry((kind.clone(), resource.clone()))
                            .or_insert_with(|| LimitEntry {
                                kind: kind.clone(),
                                resource: resource.clone(),
                                max: None,
                                min: None,
                                default: None,
                                default_request: None,
                            });
                        let slot = match field {
                            "max" => &mut entry.max,
                            "min" => &mut entry.min,
                            "default" => &mut entry.default,
                            _ => &mut entry.default_request,
                        };
                        *slot = value.as_str().map(str::to_string);
                    }
                }
            }
            LimitView {
                name: item
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                entries: merged.into_values().collect(),
            }
        })
        .collect()
}

/// 这一面认的六种工作负载，顺序就是它们在页面上的顺序。
///
/// 常驻的排在前面、一次性的排在后面：Job 一天能攒几十个（每次主线部署、每次备份演练
/// 各一个），排在后面才不会把「谁在跑第几版」淹掉。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    DaemonSet,
    CronJob,
    Job,
    /// 没有任何控制器拥有的 Pod：评测环境（toolathlon 的一道题就是一台 Pod）、
    /// 一次性的探针、宿主脚本直接 apply 的那类。
    ///
    /// 它算工作负载，理由与另外五种一样：它是一条**在跑的东西**，有镜像、有副本数
    /// （1），有它自己的就绪状态；而它不出现在这五张表里，就一条都不出现在屏上——
    /// 评测环境跑起来的时候，工作负载那一栏看不出这件事在发生。
    Pod,
}

impl WorkloadKind {
    pub const ALL: [WorkloadKind; 6] = [
        WorkloadKind::Deployment,
        WorkloadKind::StatefulSet,
        WorkloadKind::DaemonSet,
        WorkloadKind::CronJob,
        WorkloadKind::Job,
        WorkloadKind::Pod,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            WorkloadKind::Deployment => "Deployment",
            WorkloadKind::StatefulSet => "StatefulSet",
            WorkloadKind::DaemonSet => "DaemonSet",
            WorkloadKind::CronJob => "CronJob",
            WorkloadKind::Job => "Job",
            WorkloadKind::Pod => "Pod",
        }
    }

    /// apiserver 上的资源名，与 [`WorkloadKind::as_str`] 的界面名不是一回事：
    /// 界面名给人看（`StatefulSet`），资源名给 RBAC 对（`statefulsets`）。
    ///
    /// 裸 Pod 落 `pods`，与集群范围那一读同一个名字——两边读的是同一种资源，
    /// 一次 403 会同时打在两条路径上，分成两格只会让同一个故障报两遍。
    pub fn resource(self) -> &'static str {
        match self {
            WorkloadKind::Deployment => "deployments",
            WorkloadKind::StatefulSet => "statefulsets",
            WorkloadKind::DaemonSet => "daemonsets",
            WorkloadKind::CronJob => "cronjobs",
            WorkloadKind::Job => "jobs",
            WorkloadKind::Pod => "pods",
        }
    }

    /// apiserver 上取这一工种列表的路径。
    ///
    /// Deployment / StatefulSet / DaemonSet 在 apps/v1，CronJob / Job 在 batch/v1：
    /// 分组不是可以统一拼出来的，写成分支。
    fn list_path(self, namespace: &str) -> String {
        match self {
            WorkloadKind::Deployment => {
                format!("/apis/apps/v1/namespaces/{namespace}/deployments")
            }
            WorkloadKind::StatefulSet => {
                format!("/apis/apps/v1/namespaces/{namespace}/statefulsets")
            }
            WorkloadKind::DaemonSet => format!("/apis/apps/v1/namespaces/{namespace}/daemonsets"),
            WorkloadKind::CronJob => format!("/apis/batch/v1/namespaces/{namespace}/cronjobs"),
            WorkloadKind::Job => format!("/apis/batch/v1/namespaces/{namespace}/jobs"),
            // 核心组没有 `/apis/<group>` 前缀，路径只有 `/api/v1`。
            WorkloadKind::Pod => format!("/api/v1/namespaces/{namespace}/pods"),
        }
    }

    /// 容器声明在对象里的位置。CronJob 的模板套在 jobTemplate 里，比别的工种深一层。
    fn containers(self) -> &'static str {
        match self {
            WorkloadKind::CronJob => "/spec/jobTemplate/spec/template/spec/containers",
            WorkloadKind::Pod => "/spec/containers",
            _ => "/spec/template/spec/containers",
        }
    }
}

fn workloads_from(kind: WorkloadKind, list: &Value) -> Vec<WorkloadView> {
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| workload_from(kind, item))
        .collect()
}

fn workload_from(kind: WorkloadKind, item: &Value) -> Option<WorkloadView> {
    // 只有「没有控制器」的 Pod 是这一栏里的工作负载：有主的那些已经由它的 Job /
    // ReplicaSet / StatefulSet 数过一遍，再列一次就是同一件事的两行。
    if kind == WorkloadKind::Pod
        && item
            .pointer("/metadata/ownerReferences")
            .and_then(Value::as_array)
            .map(|owners| !owners.is_empty())
            .unwrap_or(false)
    {
        return None;
    }
    let name = item.pointer("/metadata/name").and_then(Value::as_str)?;
    let count = |pointer: &str| {
        item.pointer(pointer)
            .and_then(Value::as_u64)
            .map(|value| value as u32)
    };
    let (ready, desired, updated, available) = match kind {
        // spec.replicas 缺省是 1，不是 0：没写这个字段的 Deployment 与 StatefulSet
        // 也是要一个副本的。写成 0 会把它们读成「缩到零」。
        WorkloadKind::Deployment | WorkloadKind::StatefulSet => (
            count("/status/readyReplicas"),
            count("/spec/replicas").or(Some(1)),
            count("/status/updatedReplicas"),
            count("/status/availableReplicas"),
        ),
        // DaemonSet 的「要几个」由节点数决定，写在 status 里而不在 spec 里。
        WorkloadKind::DaemonSet => (
            count("/status/numberReady"),
            count("/status/desiredNumberScheduled"),
            count("/status/updatedNumberScheduled"),
            count("/status/numberAvailable"),
        ),
        // Job 的「到位」是成功跑完的次数；它没有滚动，也没有「可用副本」。
        WorkloadKind::Job => (
            count("/status/succeeded"),
            count("/spec/completions").or(Some(1)),
            None,
            None,
        ),
        // 一台裸 Pod 的「副本」是它自己：就绪看容器，要几个看声明了几个容器。
        WorkloadKind::Pod => (
            item.pointer("/status/containerStatuses")
                .and_then(Value::as_array)
                .map(|statuses| {
                    statuses
                        .iter()
                        .filter(|status| status.get("ready").and_then(Value::as_bool) == Some(true))
                        .count() as u32
                }),
            item.pointer("/spec/containers")
                .and_then(Value::as_array)
                .map(|containers| containers.len() as u32),
            None,
            None,
        ),
        // CronJob 没有副本：能读的只有此刻有几个 Job 在跑，以及下一次排在什么时候
        // （后者在 spec.schedule 上，不在这一行）。
        WorkloadKind::CronJob => (
            item.pointer("/status/active")
                .and_then(Value::as_array)
                .map(|active| active.len() as u32),
            None,
            None,
            None,
        ),
    };

    Some(WorkloadView {
        kind: kind.as_str().to_string(),
        name: name.to_string(),
        phase: match kind {
            WorkloadKind::Pod => item
                .pointer("/status/phase")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        },
        ready,
        desired,
        updated,
        available,
        revision: match kind {
            WorkloadKind::Deployment => item
                .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        },
        images: item
            .pointer(kind.containers())
            .and_then(Value::as_array)
            .map(|containers| {
                containers
                    .iter()
                    .filter_map(|container| container.get("image").and_then(Value::as_str))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        conditions: conditions_of(item),
    })
}

pub async fn cluster_quota_handler() -> Json<ResourceReport> {
    let collected_at = Utc::now();
    let Ok(client) = KubeClient::in_cluster() else {
        return Json(ResourceReport {
            namespace: String::new(),
            error: Some("不在集群里跑，读不到配额".to_string()),
            quotas: Vec::new(),
            limits: Vec::new(),
            collected_at,
        });
    };

    let namespace = client.namespace().to_string();
    let mut error = None;

    let quotas = match client
        .get_json(&format!("/api/v1/namespaces/{namespace}/resourcequotas"))
        .await
    {
        Ok(list) => quotas_from(&list),
        Err(e) => {
            error = Some(format!("读不到 ResourceQuota：{e}"));
            Vec::new()
        }
    };

    let limits = match client
        .get_json(&format!("/api/v1/namespaces/{namespace}/limitranges"))
        .await
    {
        Ok(list) => limits_from(&list),
        Err(e) => {
            error = Some(match error {
                Some(previous) => format!("{previous}；读不到 LimitRange：{e}"),
                None => format!("读不到 LimitRange：{e}"),
            });
            Vec::new()
        }
    };

    Json(ResourceReport {
        namespace,
        error,
        quotas,
        limits,
        collected_at,
    })
}

pub async fn cluster_deployments_handler() -> Json<WorkloadsReport> {
    let collected_at = Utc::now();
    let Ok(client) = KubeClient::in_cluster() else {
        return Json(WorkloadsReport {
            namespace: String::new(),
            error: Some("不在集群里跑，读不到工作负载".to_string()),
            workloads: Vec::new(),
            collected_at,
        });
    };

    let namespace = client.namespace().to_string();
    let paths: Vec<String> = WorkloadKind::ALL
        .iter()
        .map(|kind| kind.list_path(&namespace))
        .collect();
    // 六个工种互不依赖，一起问。先后问的话一次页面刷新要串六次 apiserver，
    // 其中 Job 那一列还是全命名空间最大的一列。
    let (deployment, statefulset, daemonset, cronjob, job, pod) = tokio::join!(
        client.get_json(&paths[0]),
        client.get_json(&paths[1]),
        client.get_json(&paths[2]),
        client.get_json(&paths[3]),
        client.get_json(&paths[4]),
        client.get_json(&paths[5]),
    );

    let mut workloads = Vec::new();
    let mut errors = Vec::new();
    let results = [
        (WorkloadKind::Deployment, deployment),
        (WorkloadKind::StatefulSet, statefulset),
        (WorkloadKind::DaemonSet, daemonset),
        (WorkloadKind::CronJob, cronjob),
        (WorkloadKind::Job, job),
        (WorkloadKind::Pod, pod),
    ];
    for (kind, result) in results {
        match result {
            Ok(list) => workloads.extend(workloads_from(kind, &list)),
            // 一种读不到不该把另外几种也吞掉：报出是哪种，其余照列，
            // 让人分得清「集群里没有 StatefulSet」和「StatefulSet 没读到」。
            Err(e) => errors.push(format!("读不到 {}：{e}", kind.as_str())),
        }
    }

    Json(WorkloadsReport {
        namespace,
        error: if errors.is_empty() {
            None
        } else {
            Some(errors.join("；"))
        },
        workloads,
        collected_at,
    })
}

#[cfg(test)]
mod topology_tests {
    use super::*;
    use serde_json::json;

    fn node(name: &str, cpu: &str, memory: &str) -> serde_json::Value {
        json!({
            "metadata": {
                "name": name,
                "labels": {
                    "node-role.kubernetes.io/control-plane": "",
                    "node-role.kubernetes.io/master": "",
                    "kubernetes.io/hostname": name
                }
            },
            "status": {
                "conditions": [{ "type": "Ready", "status": "True" }],
                "allocatable": { "cpu": cpu, "memory": memory, "pods": "110" }
            }
        })
    }

    fn pod(name: &str, on_node: Option<&str>, cpu: &str, memory: &str) -> serde_json::Value {
        json!({
            "metadata": { "name": name, "namespace": "cogneva" },
            "spec": {
                "nodeName": on_node,
                "containers": [{
                    "name": "app",
                    "image": "ghcr.io/example/app:1",
                    "resources": { "requests": { "cpu": cpu, "memory": memory } }
                }]
            },
            "status": {
                "phase": if on_node.is_some() { "Running" } else { "Pending" },
                "containerStatuses": [{
                    "name": "app",
                    "ready": true,
                    "restartCount": 2,
                    "state": { "running": {} }
                }]
            }
        })
    }

    fn cluster_with(nodes: Vec<serde_json::Value>) -> ClusterView {
        ClusterView {
            id: "in-cluster".to_string(),
            namespace: "cogneva".to_string(),
            reachable: true,
            error: None,
            version: None,
            usage_source: None,
            nodes: nodes.iter().map(node_view).collect(),
            unscheduled: Vec::new(),
            unplaced: Vec::new(),
        }
    }

    #[test]
    fn a_container_gets_the_process_table_its_own_instance_reported() {
        // 进程属于容器：同一个 Pod 的两个容器各有各的 PID 命名空间。按 Pod 名归档
        // 会把两份表混成一份，按容器名归档则对不上——报上来的那一方只读得到自己
        // 的 cgroup（容器 id），读不到自己叫什么名字。这里两头都对上才算数。
        let pod = json!({
            "metadata": { "name": "stack", "namespace": "cogneva" },
            "spec": {
                "nodeName": "cogneva-01",
                "containers": [
                    { "name": "app", "image": "localhost:30500/cogneva:x" },
                    { "name": "sidecar", "image": "localhost:30500/cogneva:x" }
                ]
            },
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {
                        "name": "app",
                        "ready": true,
                        "restartCount": 0,
                        "containerID": "containerd://aaaa",
                        "state": { "running": {} }
                    },
                    {
                        "name": "sidecar",
                        "ready": true,
                        "restartCount": 0,
                        "containerID": "containerd://bbbb",
                        "state": { "running": {} }
                    }
                ]
            }
        });
        let report = |command: &str| crate::process_reports::Report {
            node: "cogneva-node".to_string(),
            pod_uid: None,
            reported_at: Utc::now(),
            processes: vec![crate::process_reports::ReportedProcess {
                pid: 1,
                parent_pid: 0,
                command: command.to_string(),
                rss_bytes: 1024,
            }],
        };
        crate::process_reports::record("aaaa", report("cogneva-app"));
        crate::process_reports::record("bbbb", report("cogneva-sidecar"));

        let view = pod_view(&pod, &Sources::default());
        let app = view
            .containers
            .iter()
            .find(|container| container.name == "app")
            .expect("app");
        let sidecar = view
            .containers
            .iter()
            .find(|container| container.name == "sidecar")
            .expect("sidecar");
        assert_eq!(
            app.processes.as_ref().map(|p| p[0].command.as_str()),
            Some("cogneva-app"),
            "app 容器拿到的是它自己那一份"
        );
        assert_eq!(
            sidecar.processes.as_ref().map(|p| p[0].command.as_str()),
            Some("cogneva-sidecar"),
            "边车拿到的是它自己那一份，不是同 Pod 另一个容器的"
        );
    }

    #[test]
    fn a_container_that_never_reported_has_no_process_table() {
        // 第三方镜像不会报。这一格必须是「没有自报」，不是空数组——空数组在屏上
        // 会读成「里面没有进程」。
        crate::process_reports::record(
            "cccc",
            crate::process_reports::Report {
                pod_uid: None,
                node: "cogneva-node".to_string(),
                reported_at: Utc::now(),
                processes: vec![crate::process_reports::ReportedProcess {
                    pid: 1,
                    parent_pid: 0,
                    command: "x".to_string(),
                    rss_bytes: 0,
                }],
            },
        );
        let pod = json!({
            "metadata": { "name": "quiet", "namespace": "cogneva" },
            "spec": {
                "nodeName": "cogneva-01",
                "containers": [{ "name": "db", "image": "postgres:16" }]
            },
            "status": {
                "phase": "Running",
                "containerStatuses": [{
                    "name": "db",
                    "ready": true,
                    "restartCount": 0,
                    // 状态里还没拿到 containerID：这一拍没有可对的实例。
                    "state": { "running": {} }
                }]
            }
        });
        let view = pod_view(&pod, &Sources::default());
        assert!(
            view.containers[0].processes.is_none(),
            "没有自报就不是一张空表"
        );
    }

    #[test]
    fn the_face_claims_the_containers_it_draws() {
        // 采集器按容器 id 入账，这一面靠 Pod 状态里的同一个 id 认领。认领不到的那些
        // 会被算成「这一面读不到的容器」，所以 init 容器与运行时的 URL 前缀都必须
        // 折对——认错一个，屏上就多算一个看不见的容器。
        let pods = json!({
            "items": [
                {
                    "metadata": {
                        "name": "api",
                        "namespace": "cogneva",
                        "uid": "1288f852-f85f-4fce-a027-9fffbdcb0f26"
                    },
                    "status": {
                        "containerStatuses": [
                            { "name": "app", "containerID": "containerd://aaaa" },
                            { "name": "waiting", "state": { "waiting": {} } }
                        ],
                        "initContainerStatuses": [
                            { "name": "init", "containerID": "containerd://cccc" }
                        ]
                    }
                },
                {
                    "metadata": { "name": "quiet", "namespace": "cogneva" },
                    "status": { "containerStatuses": [] }
                }
            ]
        });
        let claims = claims_of_pods(pods.get("items"));
        assert!(claims.containers.contains("aaaa"), "运行中的容器要认领");
        assert!(
            claims.containers.contains("cccc"),
            "init 容器同样占着宿主上的一个叶子"
        );
        assert_eq!(
            claims.containers.len(),
            2,
            "没有 containerID 的状态不该凭空认领"
        );
        assert!(
            claims.pods.contains("1288f852-f85f-4fce-a027-9fffbdcb0f26"),
            "Pod 的 uid 也要认：沙箱容器不在 containerStatuses 里，它在宿主上有进程表"
        );
        let empty = claims_of_pods(None);
        assert!(
            empty.containers.is_empty() && empty.pods.is_empty(),
            "读不到 Pod 列表时谁都没认领——那一格要报「不知道」，不是「全都没认领」"
        );
    }

    #[test]
    fn a_node_reads_its_allocatable_and_its_roles() {
        let view = node_view(&node("cogneva-01", "24000m", "62Gi"));
        assert!(view.ready, "Ready=True 的节点必须读成就绪");
        assert_eq!(view.roles, vec!["control-plane", "master"]);
        assert_eq!(view.cpu_allocatable_millicores, Some(24000));
        // 夹具用 Gi 后缀：按 1024³ 换算。把它读成 10⁹ 会让「还剩多少」差出几个 GiB。
        assert_eq!(view.memory_allocatable_bytes, Some(62 * 1024 * 1024 * 1024));
        assert_eq!(view.pods_allocatable, Some(110));
    }

    #[test]
    fn pods_land_on_their_node_and_the_unscheduled_ones_stay_visible() {
        let mut cluster = cluster_with(vec![
            node("cogneva-01", "24000m", "62Gi"),
            node("cogneva-02", "8000m", "16Gi"),
        ]);
        place_pods(
            &mut cluster,
            vec![
                pod_view(
                    &pod("api-1", Some("cogneva-01"), "500m", "1Gi"),
                    &Sources::default(),
                ),
                pod_view(
                    &pod("worker-1", Some("cogneva-01"), "1500m", "2Gi"),
                    &Sources::default(),
                ),
                pod_view(
                    &pod("cache-1", Some("cogneva-02"), "250m", "512Mi"),
                    &Sources::default(),
                ),
                pod_view(&pod("waiting-1", None, "100m", "64Mi"), &Sources::default()),
                // 节点刚被删：Pod 上还写着那个名字，但它已经不在任何节点下。
                pod_view(
                    &pod("orphan-1", Some("cogneva-gone"), "100m", "64Mi"),
                    &Sources::default(),
                ),
            ],
        );

        assert_eq!(cluster.nodes[0].pods.len(), 2);
        assert_eq!(cluster.nodes[0].cpu_requested_millicores, 2000);
        assert_eq!(
            cluster.nodes[0].memory_requested_bytes,
            3 * 1024 * 1024 * 1024
        );
        assert_eq!(cluster.nodes[1].cpu_requested_millicores, 250);
        // 没被调度的 Pod 与「写着在某台节点上、但那台节点这次没读到」的 Pod 都在，
        // 但不能混成一栏：前者是调度器的事实，后者是这次读取的缺口。
        let unscheduled: Vec<&str> = cluster
            .unscheduled
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(unscheduled, vec!["waiting-1"]);
        let unplaced: Vec<&str> = cluster.unplaced.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(unplaced, vec!["orphan-1"]);
    }

    /// 节点列表读失败（权限不够、apiserver 报错）时，节点是空的，但 Pod 全都写着
    /// 自己在 `cogneva` 上。这时候把它们叫「没有被调度」是整屏的假话——而这一屏
    /// 恰恰是集群出问题时最该看的那一屏。
    #[test]
    fn a_pod_on_a_node_that_was_not_read_is_not_called_unscheduled() {
        let mut cluster = cluster_with(Vec::new());
        place_pods(
            &mut cluster,
            vec![
                pod_view(
                    &pod("api-1", Some("cogneva"), "500m", "1Gi"),
                    &Sources::default(),
                ),
                pod_view(
                    &pod("api-2", Some("cogneva"), "500m", "1Gi"),
                    &Sources::default(),
                ),
            ],
        );

        assert!(
            cluster.unscheduled.is_empty(),
            "这两个 Pod 都写着在 cogneva 节点上，不该被叫作未调度"
        );
        assert_eq!(
            cluster.unplaced.len(),
            2,
            "读不到节点时 Pod 要落到「节点没读到」这一栏"
        );
    }

    #[test]
    fn requests_are_the_larger_of_the_containers_sum_and_the_largest_init() {
        let pod = json!({
            "metadata": { "name": "migrate", "namespace": "cogneva" },
            "spec": {
                "initContainers": [
                    { "name": "wait-db", "resources": { "requests": { "cpu": "2", "memory": "4Gi" } } },
                    { "name": "small", "resources": { "requests": { "cpu": "100m", "memory": "64Mi" } } }
                ],
                "containers": [{
                    "name": "app",
                    "resources": { "requests": { "cpu": "500m", "memory": "512Mi" } }
                }]
            },
            "status": { "phase": "Running", "containerStatuses": [] }
        });
        let (cpu, memory) = pod_requests(&pod);
        assert_eq!(cpu, 2000, "init 容器取最大值，且与普通容器取大者");
        assert_eq!(memory, 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn a_whole_core_is_a_thousand_millicores() {
        assert_eq!(millicores("2"), Some(2000));
        assert_eq!(millicores("500m"), Some(500));
        assert_eq!(millicores("0"), Some(0));
        assert_eq!(millicores("nonsense"), None, "读不出来的量不是零");
    }

    #[test]
    fn a_waiting_container_says_why() {
        let status = json!({ "state": { "waiting": { "reason": "CrashLoopBackOff" } } });
        assert_eq!(container_state(Some(&status)), "waiting:CrashLoopBackOff");
        let running = json!({ "state": { "running": {} } });
        assert_eq!(container_state(Some(&running)), "running");
        assert_eq!(container_state(None), "unknown");
    }

    #[test]
    fn a_container_carries_its_ports_limits_mounts_and_probes() {
        let pod = json!({
            "metadata": { "name": "api-1", "namespace": "cogneva" },
            "spec": {
                "volumes": [
                    { "name": "cfg", "configMap": { "name": "cogneva-json" } },
                    { "name": "data", "persistentVolumeClaim": { "claimName": "app-data" } }
                ],
                "containers": [{
                    "name": "api",
                    "image": "ghcr.io/example/api:1",
                    "ports": [{ "name": "http", "containerPort": 8080, "protocol": "TCP" }],
                    "resources": {
                        "requests": { "cpu": "500m", "memory": "1Gi" },
                        "limits": { "cpu": "2", "memory": "2Gi" }
                    },
                    "volumeMounts": [
                        { "name": "cfg", "mountPath": "/etc/cogneva", "readOnly": true }
                    ],
                    "readinessProbe": { "httpGet": { "path": "/healthz", "port": 8080 } }
                }]
            },
            "status": { "phase": "Running", "containerStatuses": [] }
        });
        let view = pod_view(&pod, &Sources::default());
        let container = &view.containers[0];
        assert_eq!(container.ports[0].container_port, 8080);
        assert_eq!(container.cpu_limit_millicores, Some(2000));
        assert_eq!(container.memory_limit_bytes, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(container.mounts[0].mount_path, "/etc/cogneva");
        assert!(container.mounts[0].read_only);
        assert_eq!(container.probes, vec!["readiness"]);
        let kinds: Vec<&str> = view.volumes.iter().map(|v| v.kind.as_str()).collect();
        assert_eq!(kinds, vec!["configMap", "persistentVolumeClaim"]);
    }

    #[test]
    fn events_are_grouped_by_pod_and_only_the_newest_five_survive() {
        let mut items = Vec::new();
        for minute in 1..=7 {
            items.push(json!({
                "involvedObject": { "kind": "Pod", "name": "api-1", "namespace": "cogneva" },
                "type": "Warning",
                "reason": "Unhealthy",
                "message": "readiness probe failed",
                "count": minute,
                "lastTimestamp": format!("2026-10-25T13:0{minute}:00Z")
            }));
        }
        items.push(json!({
            "involvedObject": { "kind": "Service", "name": "api-1", "namespace": "cogneva" },
            "type": "Normal",
            "reason": "EnsuredLoadBalancer",
            "message": "not a pod event"
        }));

        let grouped = events_by_object(&json!({ "items": items }));
        let events = &grouped["cogneva/api-1"];
        assert_eq!(events.len(), 5, "每个对象最多留五条");
        assert_eq!(
            events[0].last_seen.as_deref(),
            Some("2026-10-25T13:07:00Z"),
            "新的在前"
        );
        assert!(
            events
                .iter()
                .all(|event| event.reason != "EnsuredLoadBalancer"),
            "同名的 Service 事件不许混进 Pod 的列表"
        );
    }

    #[test]
    fn usage_comes_from_metrics_and_a_missing_source_is_not_zero() {
        let pod = json!({
            "metadata": { "name": "api-1", "namespace": "cogneva" },
            "spec": { "nodeName": "cogneva-01", "containers": [
                { "name": "api", "resources": { "requests": { "cpu": "500m", "memory": "1Gi" } } }
            ]},
            "status": { "phase": "Running", "containerStatuses": [] }
        });

        let without = pod_view(&pod, &Sources::default());
        assert_eq!(
            without.cpu_used_millicores, None,
            "没有数据源时是「不知道」，不是 0"
        );

        let metrics = json!({ "items": [{
            "metadata": { "name": "api-1", "namespace": "cogneva" },
            "containers": [{ "name": "api", "usage": { "cpu": "120m", "memory": "300Mi" } }]
        }]});
        let usage = usage_by_pod(&metrics);
        let with = pod_view(
            &pod,
            &Sources {
                usage,
                ..Sources::default()
            },
        );
        assert_eq!(with.cpu_used_millicores, Some(120));
        assert_eq!(with.memory_used_bytes, Some(300 * 1024 * 1024));
        assert_eq!(
            with.containers[0].cpu_used_millicores,
            Some(120),
            "容器的数要与 Pod 对得上"
        );
    }

    #[test]
    fn a_restart_says_how_the_last_attempt_ended_not_just_that_it_restarted() {
        let pod = json!({
            "metadata": { "name": "api-1", "namespace": "cogneva" },
            "spec": { "containers": [{ "name": "api" }] },
            "status": {
                "phase": "Running",
                "containerStatuses": [{
                    "name": "api",
                    "ready": true,
                    "restartCount": 4,
                    "state": { "running": {} },
                    "lastState": { "terminated": {
                        "reason": "OOMKilled",
                        "exitCode": 137,
                        "message": "container was killed for exceeding its limit",
                        "finishedAt": "2026-10-25T13:07:00Z"
                    }}
                }]
            }
        });
        let view = pod_view(&pod, &Sources::default());
        let termination = view.containers[0]
            .last_termination
            .as_ref()
            .expect("重启过就要带着上一次的结局");
        assert_eq!(termination.reason, "OOMKilled");
        assert_eq!(termination.exit_code, 137);
        assert_eq!(
            termination.finished_at.as_deref(),
            Some("2026-10-25T13:07:00Z")
        );

        // 没重启过就没有「上一次」，不能补一条空白的。
        let fresh = json!({
            "metadata": { "name": "api-1" },
            "spec": { "containers": [{ "name": "api" }] },
            "status": { "containerStatuses": [{ "name": "api", "state": { "running": {} } }] }
        });
        assert!(pod_view(&fresh, &Sources::default()).containers[0]
            .last_termination
            .is_none());
    }

    #[test]
    fn a_termination_without_a_reason_is_read_from_its_exit_code() {
        let clean = termination_view(&json!({ "exitCode": 0 }));
        assert_eq!(clean.reason, "Completed", "退出码 0 是跑完了，不是错误");
        let failed = termination_view(&json!({ "exitCode": 1 }));
        assert_eq!(failed.reason, "Error");
        assert_eq!(failed.exit_code, 1);
    }

    #[test]
    fn container_requests_and_limits_are_read_side_by_side() {
        let pod = json!({
            "metadata": { "name": "api-1", "namespace": "cogneva" },
            "spec": { "containers": [{
                "name": "api",
                "resources": {
                    "requests": { "cpu": "500m", "memory": "1Gi" },
                    "limits": { "cpu": "2", "memory": "2Gi" }
                }
            }]},
            "status": { "phase": "Running", "containerStatuses": [] }
        });
        let container = &pod_view(&pod, &Sources::default()).containers[0];
        assert_eq!(container.cpu_request_millicores, Some(500));
        assert_eq!(container.memory_request_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(container.cpu_limit_millicores, Some(2000));
        assert_eq!(container.memory_limit_bytes, Some(2 * 1024 * 1024 * 1024));
    }

    #[test]
    fn pod_conditions_say_which_step_a_stuck_pod_is_sitting_on() {
        let pod = json!({
            "metadata": { "name": "api-1" },
            "spec": { "containers": [{ "name": "api" }] },
            "status": {
                "phase": "Pending",
                "conditions": [
                    { "type": "PodScheduled", "status": "True" },
                    { "type": "Initialized", "status": "False",
                      "reason": "ContainersNotInitialized",
                      "message": "containers with incomplete status: [wait-db]" },
                    { "type": "Ready", "status": "False", "reason": "ContainersNotReady" }
                ],
                "containerStatuses": []
            }
        });
        let conditions = pod_view(&pod, &Sources::default()).conditions;
        let names: Vec<&str> = conditions.iter().map(|c| c.kind.as_str()).collect();
        assert_eq!(names, vec!["PodScheduled", "Initialized", "Ready"]);
        assert!(conditions[0].ok);
        assert!(!conditions[1].ok);
        assert_eq!(
            conditions[1].reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        assert!(conditions[1]
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("wait-db"));
    }

    #[test]
    fn a_condition_with_no_status_is_not_counted_as_met() {
        let pod = json!({
            "metadata": { "name": "api-1" },
            "spec": { "containers": [{ "name": "api" }] },
            "status": { "phase": "Unknown", "conditions": [{ "type": "Ready" }] }
        });
        let conditions = pod_view(&pod, &Sources::default()).conditions;
        assert!(
            !conditions[0].ok,
            "缺 status 字段按没成立算：读成成立会把卡住的 Pod 画成正常"
        );
    }

    #[test]
    fn the_kubelet_summary_is_a_usage_source_of_its_own_without_metrics_server() {
        // 这是真实的 `stats/summary` 形状（kubelet 自己服务，装在每台机器上）。
        let summary = json!({
            "node": { "nodeName": "cogneva-01" },
            "pods": [
                {
                    "podRef": { "name": "api-1", "namespace": "cogneva" },
                    "process_stats": { "process_count": 3u64 },
                    "cpu": { "usageNanoCores": 250_000_000u64 },
                    "memory": { "workingSetBytes": 734_003_200u64, "rssBytes": 700_000_000u64 },
                    "containers": [
                        { "name": "api", "cpu": { "usageNanoCores": 200_000_000u64 },
                          "memory": { "workingSetBytes": 600_000_000u64 } },
                        { "name": "sidecar", "cpu": { "usageNanoCores": 50_000_000u64 },
                          "memory": { "workingSetBytes": 134_003_200u64 } }
                    ]
                },
                {
                    "podRef": { "name": "postgres-0", "namespace": "other" },
                    "cpu": { "usageNanoCores": 900_000_000u64 },
                    "memory": { "workingSetBytes": 2_000_000_000u64 },
                    "containers": []
                }
            ]
        });

        let usage = usage_from_summary(&summary, "cogneva");
        assert_eq!(usage.len(), 1, "别的命名空间的 Pod 不属于这一面");
        let (name, pod) = &usage[0];
        assert_eq!(name, "api-1");
        assert_eq!(pod.cpu, 250, "nanoCores 换算成毫核");
        assert_eq!(pod.memory, 734_003_200, "取工作集，不取 RSS");
        assert_eq!(pod.containers["api"], (200, 600_000_000));
        assert_eq!(pod.containers["sidecar"], (50, 134_003_200));
        assert_eq!(
            pod.processes,
            Some(3),
            "进程数在 Pod 这一级，summary 报多少就是多少"
        );
    }

    #[test]
    fn a_summary_that_names_no_namespace_matches_nothing_rather_than_everything() {
        // 缺 namespace 的条目如果被当成「匹配」，整台机器的 Pod 都会记到这一面。
        let summary =
            json!({ "pods": [{ "podRef": { "name": "api-1" }, "cpu": {}, "memory": {} }] });
        assert!(usage_from_summary(&summary, "cogneva").is_empty());
    }

    #[test]
    fn nanocores_and_byte_fields_survive_both_number_and_string_forms() {
        // 1.5 毫核按最近整数进：用量是读数，不必保留小数位。
        assert_eq!(nanocores_millicores(Some(&json!(1_500_000u64))), 2);
        assert_eq!(nanocores_millicores(Some(&json!("2000000"))), 2);
        assert_eq!(nanocores_millicores(Some(&json!(0))), 0);
        assert_eq!(nanocores_millicores(None), 0, "缺字段是 0，不是猜测");
        assert_eq!(bytes_at(Some(&json!(4096))), 4096);
        assert_eq!(bytes_at(Some(&json!("8192"))), 8192);
        assert_eq!(bytes_at(None), 0);
    }

    #[test]
    fn node_pressure_conditions_are_read_the_same_way_as_pod_conditions() {
        let node = json!({
            "metadata": { "name": "cogneva-01" },
            "status": {
                "conditions": [
                    { "type": "Ready", "status": "True" },
                    { "type": "MemoryPressure", "status": "True",
                      "reason": "KubeletHasInsufficientMemory", "message": "kubelet has insufficient memory" },
                    { "type": "DiskPressure", "status": "False" }
                ],
                "allocatable": { "cpu": "24000m", "memory": "62Gi", "pods": "110" }
            }
        });
        let view = node_view(&node);
        let kinds: Vec<(&str, bool)> = view
            .conditions
            .iter()
            .map(|condition| (condition.kind.as_str(), condition.ok))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("Ready", true),
                ("MemoryPressure", true),
                ("DiskPressure", false)
            ],
            "压力条件为真就是坏消息，界面按名字解读"
        );
        assert_eq!(
            view.conditions[1].reason.as_deref(),
            Some("KubeletHasInsufficientMemory")
        );
    }

    #[test]
    fn a_claim_carries_the_bytes_the_platform_measured_beside_the_bytes_it_declared() {
        let pod = json!({
            "metadata": { "name": "api-1", "namespace": "cogneva" },
            "spec": {
                "volumes": [{ "name": "data", "persistentVolumeClaim": { "claimName": "app-data" } }],
                "containers": [{ "name": "api" }]
            },
            "status": { "phase": "Running", "containerStatuses": [] }
        });
        let claims = claims_by_name(&json!({ "items": [{
            "metadata": { "name": "app-data", "namespace": "cogneva" },
            "spec": { "storageClassName": "local-path" },
            "status": { "phase": "Bound", "capacity": { "storage": "10Gi" }, "accessModes": ["ReadWriteOnce"] }
        }]}));
        let mut measured = std::collections::HashMap::new();
        measured.insert("cogneva/app-data".to_string(), 3_221_225_472u64);
        let sources = Sources {
            claims,
            measured,
            ..Sources::default()
        };
        let claim = pod_view(&pod, &sources).volumes[0]
            .claim
            .clone()
            .expect("持久卷要带上声明");
        assert_eq!(claim.capacity_bytes, Some(10 * 1024 * 1024 * 1024));
        assert_eq!(
            claim.measured_bytes,
            Some(3_221_225_472),
            "盘上真的占了多少"
        );

        // 走查端没量到这一个：留空，不拿声明量去填——两个数不是一回事。
        let unmeasured = pod_view(
            &pod,
            &Sources {
                claims: claims_by_name(&json!({ "items": [{
                    "metadata": { "name": "app-data", "namespace": "cogneva" },
                    "status": { "phase": "Pending" }
                }]})),
                ..Sources::default()
            },
        );
        assert_eq!(
            unmeasured.volumes[0].claim.as_ref().unwrap().measured_bytes,
            None
        );
    }

    /// 这一面读的是整个集群的 Pod，于是别处也有 `redis-0` 是常态。
    ///
    /// 用量、事件、卷三张表原先按裸名字索引：两个命名空间里同名的对象会认领
    /// 对方那一行，而屏上两行都填满了——错得没有声音。键改成「命名空间/名字」，
    /// 这条测试就是那个不改就会红的地方。
    #[test]
    fn a_pod_in_another_namespace_does_not_inherit_its_namesakes_readings() {
        let pod = |namespace: &str| {
            json!({
                "metadata": { "name": "redis-0", "namespace": namespace },
                "spec": { "containers": [{ "name": "redis" }] },
                "status": { "phase": "Running", "containerStatuses": [] }
            })
        };
        let usage = usage_by_pod(&json!({ "items": [{
            "metadata": { "name": "redis-0", "namespace": "cogneva" },
            "containers": [{ "name": "redis", "usage": { "cpu": "12m", "memory": "64Mi" } }]
        }]}));
        let events = events_by_object(&json!({ "items": [{
            "type": "Warning",
            "reason": "BackOff",
            "message": "restarting",
            "involvedObject": { "kind": "Pod", "name": "redis-0", "namespace": "cogneva" }
        }]}));
        let sources = Sources {
            usage,
            events,
            ..Sources::default()
        };

        let mine = pod_view(&pod("cogneva"), &sources);
        assert_eq!(mine.cpu_used_millicores, Some(12));
        assert_eq!(mine.events.len(), 1);

        let theirs = pod_view(&pod("another-team"), &sources);
        assert_eq!(
            theirs.cpu_used_millicores, None,
            "别的命名空间里的同名 Pod 没有用量读数，不是拿到了这一个的"
        );
        assert!(
            theirs.events.is_empty(),
            "别处的同 Pod 不该继承这个 Pod 的 BackOff"
        );
    }

    #[tokio::test]
    async fn a_scanner_with_no_address_or_no_http_port_is_skipped_not_requested() {
        // 夹具里给不出地址与端口：这条路径必须不发请求就返回空，否则一次轮询会
        // 卡在一个连不上的地址上。测试能跑完本身就是这条断言。
        let mut claims = std::collections::HashMap::new();
        claims.insert(
            "app-data".to_string(),
            ClaimView {
                name: "app-data".to_string(),
                phase: "Bound".to_string(),
                storage_class: None,
                capacity_bytes: None,
                access_modes: Vec::new(),
                volume_name: None,
                measured_bytes: None,
            },
        );
        let no_ip = json!([{
            "metadata": { "name": "cogneva-claim-store-x", "labels": { "app.kubernetes.io/component": "claim-store" } },
            "spec": { "containers": [{ "ports": [{ "name": "http", "containerPort": 9100 }] }] },
            "status": {}
        }]);
        // 一个不会发出请求的客户端就够：这几条路径在发请求之前就返回。
        let http = reqwest::Client::new();
        assert!(measured_claim_bytes(&http, Some(&no_ip), &claims)
            .await
            .is_empty());

        let no_port = json!([{
            "metadata": { "name": "cogneva-claim-store-x", "labels": { "app.kubernetes.io/component": "claim-store" } },
            "spec": { "containers": [{ "ports": [{ "name": "metrics-admin", "containerPort": 9100 }] }] },
            "status": { "podIP": "127.0.0.1" }
        }]);
        assert!(measured_claim_bytes(&http, Some(&no_port), &claims)
            .await
            .is_empty());

        // 没有声明要量，就不必去敲任何端点。
        assert!(
            measured_claim_bytes(&http, Some(&no_port), &std::collections::HashMap::new())
                .await
                .is_empty()
        );
    }

    #[test]
    fn a_persistent_volume_names_the_claim_it_binds_to() {
        let pod = json!({
            "metadata": { "name": "api-1", "namespace": "cogneva" },
            "spec": {
                "volumes": [{ "name": "data", "persistentVolumeClaim": { "claimName": "app-data" } }],
                "containers": [{ "name": "api" }]
            },
            "status": { "phase": "Running", "containerStatuses": [] }
        });
        let claims = claims_by_name(&json!({ "items": [{
            "metadata": { "name": "app-data", "namespace": "cogneva" },
            "spec": { "storageClassName": "local-path", "volumeName": "pvc-1234" },
            "status": {
                "phase": "Bound",
                "capacity": { "storage": "10Gi" },
                "accessModes": ["ReadWriteOnce"]
            }
        }]}));

        let view = pod_view(
            &pod,
            &Sources {
                claims,
                ..Sources::default()
            },
        );
        let claim = view.volumes[0].claim.as_ref().expect("持久卷要带上声明");
        assert_eq!(claim.phase, "Bound");
        assert_eq!(claim.storage_class.as_deref(), Some("local-path"));
        assert_eq!(claim.capacity_bytes, Some(10 * 1024 * 1024 * 1024));
        assert_eq!(claim.access_modes, vec!["ReadWriteOnce"]);
    }

    /// 没有这一条就会红：把卷声明或事件的读法改回按命名空间读，编译、其余测试、
    /// 页面都照过——只有别的命名空间里的 Pod 会带着空的卷声明与空事件出现，而那与
    /// 「它本来就没有」在格子上是同一副样子。
    #[test]
    fn the_claim_and_event_lists_cover_the_same_scope_as_the_pods() {
        assert_eq!(
            pvc_list_path(),
            "/api/v1/persistentvolumeclaims",
            "卷声明要按整个集群读：Pod 已经是整个集群的"
        );
        assert_eq!(
            event_list_path(),
            "/api/v1/events",
            "事件要按整个集群读：Pod 已经是整个集群的"
        );
        assert!(
            !pvc_list_path().contains("/namespaces/")
                && !event_list_path().contains("/namespaces/"),
            "路径里带命名空间就是把范围收窄回一处，别的命名空间的 Pod 会缺这两格"
        );
    }

    /// 两个命名空间里同名的卷声明不能串：键是 `namespace/name`，不是名字。
    #[test]
    fn two_namespaces_can_hold_the_same_claim_name() {
        let claims = claims_by_name(&json!({ "items": [
            {
                "metadata": { "name": "data", "namespace": "cogneva" },
                "status": { "phase": "Bound", "capacity": { "storage": "10Gi" } }
            },
            {
                "metadata": { "name": "data", "namespace": "monitoring" },
                "status": { "phase": "Pending", "capacity": { "storage": "1Gi" } }
            }
        ]}));
        let pod = |namespace: &str| {
            json!({
                "metadata": { "name": "probe", "namespace": namespace },
                "spec": {
                    "volumes": [{ "name": "data", "persistentVolumeClaim": { "claimName": "data" } }],
                    "containers": [{ "name": "app" }]
                },
                "status": { "phase": "Running", "containerStatuses": [] }
            })
        };
        let sources = Sources {
            claims,
            ..Sources::default()
        };
        let here = pod_view(&pod("cogneva"), &sources);
        let there = pod_view(&pod("monitoring"), &sources);
        assert_eq!(
            here.volumes[0].claim.as_ref().map(|c| c.phase.as_str()),
            Some("Bound")
        );
        assert_eq!(
            there.volumes[0].claim.as_ref().map(|c| c.phase.as_str()),
            Some("Pending"),
            "另一个命名空间里同名的卷声明是它自己的那份，不是本命名空间那条"
        );
    }

    #[test]
    fn a_quota_percentage_needs_two_readable_numbers() {
        assert_eq!(quantity_units("500m"), Some(0.5));
        assert_eq!(quantity_units("4Gi"), Some(4.0 * 1024.0 * 1024.0 * 1024.0));
        assert_eq!(quota_percent("2", "500m"), Some(25.0));
        assert_eq!(quota_percent("4Gi", "1Gi"), Some(25.0));
        // 读不出数就不给百分比：宁可缺一格，也不发明一个比值。
        assert_eq!(quota_percent("4Gi", "lots"), None);
        assert_eq!(quota_percent("0", "0"), None);
    }

    #[test]
    fn quota_entries_keep_resources_that_only_have_a_hard_limit() {
        let list = json!({"items": [{
            "metadata": {"name": "compute"},
            "status": {
                "hard": {"requests.cpu": "4", "requests.memory": "8Gi", "pods": "20"},
                "used": {"requests.cpu": "500m", "requests.memory": "2Gi"}
            }
        }]});

        let quotas = quotas_from(&list);
        assert_eq!(quotas.len(), 1);
        let entries = &quotas[0].entries;
        assert_eq!(entries.len(), 3, "只出现在 hard 里的 pods 不能被漏掉");

        let pods = entries.iter().find(|e| e.resource == "pods").expect("pods");
        assert_eq!(pods.hard, "20");
        assert_eq!(pods.used, "");
        assert_eq!(pods.percent, None, "还没人用不是一个百分比");

        let memory = entries
            .iter()
            .find(|e| e.resource == "requests.memory")
            .expect("memory");
        assert_eq!(memory.percent, Some(25.0));
    }

    #[test]
    fn a_limit_range_row_joins_its_four_tables() {
        let list = json!({"items": [{
            "metadata": {"name": "defaults"},
            "spec": {"limits": [{
                "type": "Container",
                "max": {"memory": "2Gi", "cpu": "1"},
                "default": {"memory": "512Mi"},
                "defaultRequest": {"memory": "256Mi"}
            }]}
        }]});

        let limits = limits_from(&list);
        let entries = &limits[0].entries;
        assert_eq!(entries.len(), 2, "按 (kind, resource) 并成行");

        let memory = entries
            .iter()
            .find(|e| e.resource == "memory")
            .expect("memory");
        assert_eq!(memory.kind, "Container");
        assert_eq!(memory.max.as_deref(), Some("2Gi"));
        assert_eq!(memory.min, None);
        assert_eq!(memory.default.as_deref(), Some("512Mi"));
        assert_eq!(memory.default_request.as_deref(), Some("256Mi"));

        let cpu = entries.iter().find(|e| e.resource == "cpu").expect("cpu");
        assert_eq!(cpu.default, None);
    }

    #[test]
    fn a_deployment_carries_its_revision_images_and_rollout_verdict() {
        let list = json!({"items": [{
            "metadata": {
                "name": "cogneva",
                "annotations": {"deployment.kubernetes.io/revision": "7"}
            },
            "spec": {"replicas": 2, "template": {"spec": {"containers": [
                {"name": "app", "image": "cogneva:abc"},
                {"name": "sidecar", "image": "proxy:def"}
            ]}}},
            "status": {
                "readyReplicas": 1,
                "updatedReplicas": 2,
                "availableReplicas": 1,
                "conditions": [{
                    "type": "Progressing",
                    "status": "False",
                    "reason": "ProgressDeadlineExceeded"
                }]
            }
        }]});

        let deployments = workloads_from(WorkloadKind::Deployment, &list);
        let deployment = &deployments[0];
        assert_eq!(deployment.kind, "Deployment");
        assert_eq!(deployment.revision.as_deref(), Some("7"));
        assert_eq!(deployment.desired, Some(2));
        assert_eq!(deployment.ready, Some(1));
        assert_eq!(deployment.available, Some(1));
        assert_eq!(deployment.images, vec!["cogneva:abc", "proxy:def"]);
        assert!(
            !deployment.conditions[0].ok,
            "Progressing=False 是滚动卡住，不是进行中"
        );
    }

    #[test]
    fn a_deployment_without_annotations_still_says_how_many_it_wants() {
        let list = json!({"items": [{"metadata": {"name": "bare"}, "spec": {}}]});

        let deployments = workloads_from(WorkloadKind::Deployment, &list);
        assert_eq!(deployments[0].desired, Some(1), "缺省是 1，不是 0");
        assert_eq!(deployments[0].revision, None);
        assert!(deployments[0].images.is_empty());
    }

    /// 这一面的清单只读 Deployment 时，页面上就只剩 Deployment：postgres / redis
    /// 是 StatefulSet，claim-store / host-ledger / buildah 是 DaemonSet，备份是
    /// CronJob，主线部署是 Job——它们在集群里跑着而页面说「没有」，是最坏的一种
    /// 读法，因为空白读起来像「本来就没有」。
    #[test]
    fn the_other_four_kinds_are_counted_by_their_own_status_fields() {
        let statefulset = json!({"items": [{
            "metadata": {"name": "postgres"},
            "spec": {"replicas": 1, "template": {"spec": {"containers": [
                {"name": "postgres", "image": "postgres:16"}
            ]}}},
            "status": {"readyReplicas": 1, "updatedReplicas": 1}
        }]});
        let statefulset = &workloads_from(WorkloadKind::StatefulSet, &statefulset)[0];
        assert_eq!(statefulset.kind, "StatefulSet");
        assert_eq!(statefulset.desired, Some(1));
        assert_eq!(statefulset.ready, Some(1));
        assert_eq!(
            statefulset.revision, None,
            "revision 注解是 Deployment 的滚动版本，别的工种没有"
        );
        assert_eq!(statefulset.images, vec!["postgres:16"]);

        let daemonset = json!({"items": [{
            "metadata": {"name": "claim-store"},
            "spec": {"template": {"spec": {"containers": [{"name": "app", "image": "cog:1"}]}}},
            "status": {
                "desiredNumberScheduled": 1,
                "numberReady": 1,
                "updatedNumberScheduled": 1,
                "numberAvailable": 1
            }
        }]});
        let daemonset = &workloads_from(WorkloadKind::DaemonSet, &daemonset)[0];
        assert_eq!(daemonset.desired, Some(1), "「要几个」由节点数决定");
        assert_eq!(daemonset.ready, Some(1));
        assert_eq!(daemonset.available, Some(1));

        let job = json!({"items": [{
            "metadata": {"name": "cogneva-mainline-4bce6962bc33"},
            "spec": {"completions": 1, "template": {"spec": {"containers": [
                {"name": "mainline", "image": "cogneva:main-1"}
            ]}}},
            "status": {"succeeded": 1}
        }]});
        let job = &workloads_from(WorkloadKind::Job, &job)[0];
        assert_eq!(job.kind, "Job");
        assert_eq!(job.ready, Some(1), "Job 的到位是「跑完了」");
        assert_eq!(job.desired, Some(1));
        assert_eq!(
            (job.updated, job.available),
            (None, None),
            "Job 不滚动，这两个数对它没有意义；给 0 会被读成「零个就绪」"
        );
        assert_eq!(job.images, vec!["cogneva:main-1"]);

        // CronJob 的容器声明套在 jobTemplate 里，比别的工种深一层；读错位置就
        // 只能读到空镜像列表，而那读起来像「这个 CronJob 没有容器」。
        let cronjob = json!({"items": [{
            "metadata": {"name": "cogneva-backup"},
            "spec": {"schedule": "0 3 * * *", "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "backup", "image": "cogneva:main-1"}]
            }}}}},
            "status": {"active": [{"name": "cogneva-backup-29000000"}]}
        }]});
        let cronjob = &workloads_from(WorkloadKind::CronJob, &cronjob)[0];
        assert_eq!(cronjob.kind, "CronJob");
        assert_eq!(cronjob.ready, Some(1), "此刻有一个 Job 在跑");
        assert_eq!(
            (cronjob.desired, cronjob.updated, cronjob.available),
            (None, None, None)
        );
        assert_eq!(cronjob.images, vec!["cogneva:main-1"]);
    }

    /// 每个工种问的是自己那一组接口，路径不是拼出来的。
    #[test]
    fn every_kind_lists_its_own_resource() {
        let paths: Vec<String> = WorkloadKind::ALL
            .iter()
            .map(|kind| kind.list_path("cogneva"))
            .collect();
        assert_eq!(
            paths,
            vec![
                "/apis/apps/v1/namespaces/cogneva/deployments",
                "/apis/apps/v1/namespaces/cogneva/statefulsets",
                "/apis/apps/v1/namespaces/cogneva/daemonsets",
                "/apis/batch/v1/namespaces/cogneva/cronjobs",
                "/apis/batch/v1/namespaces/cogneva/jobs",
                "/api/v1/namespaces/cogneva/pods",
            ]
        );
    }

    #[test]
    fn only_a_pod_without_a_controller_is_a_workload_of_its_own() {
        // 没有这次改动就会红：评测环境（toolathlon 一道题一台 Pod）与宿主脚本 apply
        // 的探针既不是 Deployment 也不是 Job，工作负载那一栏里一条都不出现——跑起来
        // 的时候屏上看不出这件事在发生。
        let list = json!({"items": [
            {
                "metadata": {"name": "eval-toolathlon-task-1"},
                "spec": {"containers": [{"name": "app", "image": "eval-pack:1"}]},
                "status": {"containerStatuses": [{"name": "app", "ready": true}]}
            },
            {
                "metadata": {
                    "name": "cogneva-mainline-4bce6962bc33-abc",
                    "ownerReferences": [{"kind": "Job", "name": "cogneva-mainline-4bce6962bc33"}]
                },
                "spec": {"containers": [{"name": "mainline", "image": "cogneva:1"}]},
                "status": {"containerStatuses": [{"name": "mainline", "ready": true}]}
            },
            // 跑完的裸 Pod：就绪数是 0，光看 `0 / 1` 分不清「结束了」与「起不来」。
            {
                "metadata": {"name": "searxprobe2"},
                "spec": {"containers": [{"name": "probe", "image": "probe:1"}]},
                "status": {"phase": "Succeeded", "containerStatuses": [{"name": "probe", "ready": false}]}
            }
        ]});
        let pods = workloads_from(WorkloadKind::Pod, &list);
        assert_eq!(
            pods.len(),
            2,
            "有主的 Pod 由它的控制器数过一遍，不再列第二遍"
        );
        let live = pods
            .iter()
            .find(|pod| pod.name == "eval-toolathlon-task-1")
            .expect("评测的裸 Pod 要在这一栏里");
        assert_eq!(live.kind, "Pod");
        assert_eq!(live.ready, Some(1));
        assert_eq!(live.desired, Some(1), "它就是自己那一份副本");
        assert_eq!(live.images, vec!["eval-pack:1"]);

        // 结束的裸 Pod 与起不来的裸 Pod 在 `0 / 1` 上是同一行；只有相位能把它们分开。
        let finished = pods
            .iter()
            .find(|pod| pod.name == "searxprobe2")
            .expect("跑完的裸 Pod 也要看得见");
        assert_eq!(finished.ready, Some(0));
        assert_eq!(
            finished.phase.as_deref(),
            Some("Succeeded"),
            "没有相位，`0 / 1` 读作「有一个起不来」，而它只是结束了"
        );
    }
}
