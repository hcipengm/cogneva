//! Cluster component health — reports the three fixed deployments that make
//! up a Cogneva install (main app, security gateway, evolution sandbox) so
//! the topology view can render real nodes instead of placeholders.
//!
//! The main app is the process serving this request, so it is always "up".
//! The other two are read from the Kubernetes API; when the service account
//! lacks permission (or we are not in a cluster) they degrade to "unknown".

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

pub async fn cluster_components_handler() -> Json<ClusterComponents> {
    let mut components = vec![ComponentStatus {
        id: "main".to_string(),
        status: "up".to_string(),
        ready_replicas: None,
        desired_replicas: None,
    }];

    if let Ok(client) = KubeClient::in_cluster() {
        components
            .push(deployment_status(&client, "security-gateway", "cogneva-security-gateway").await);
        components.push(deployment_status(&client, "evolution", "cogneva-evolution").await);
    } else {
        for id in ["security-gateway", "evolution"] {
            components.push(ComponentStatus {
                id: id.to_string(),
                status: "unknown".to_string(),
                ready_replicas: None,
                desired_replicas: None,
            });
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
    let (usage, usage_source) = match client
        .get_json(&format!(
            "/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods",
            client.namespace()
        ))
        .await
    {
        Ok(list) => (usage_by_pod(&list), Some(USAGE_FROM_METRICS.to_string())),
        Err(_) => match usage_from_kubelets(&client, &view.nodes).await {
            Some(usage) => (usage, Some(USAGE_FROM_KUBELET.to_string())),
            None => (std::collections::HashMap::new(), None),
        },
    };
    view.usage_source = usage_source;

    // 卷声明读一次，给持久卷补上「绑到哪块盘、多大」。读不到就只是缺这一层信息。
    let claims = match client
        .get_json(&format!(
            "/api/v1/namespaces/{}/persistentvolumeclaims",
            client.namespace()
        ))
        .await
    {
        Ok(list) => claims_by_name(&list),
        Err(_) => std::collections::HashMap::new(),
    };

    // 事件读一次、按对象名分给各个 Pod：一个 Pod 一次请求在几十个 Pod 的命名空间里
    // 会把一面拓扑变成几十次 apiserver 往返，而这一面是轮询的。
    let events = match client
        .get_json(&format!("/api/v1/namespaces/{}/events", client.namespace()))
        .await
    {
        Ok(list) => events_by_object(&list),
        Err(_) => std::collections::HashMap::new(),
    };

    let pods_path = format!("/api/v1/namespaces/{}/pods", client.namespace());
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

fn usage_by_pod(list: &Value) -> std::collections::HashMap<String, PodUsage> {
    let mut usage = std::collections::HashMap::new();
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return usage;
    };
    for item in items {
        let Some(name) = item.pointer("/metadata/name").and_then(Value::as_str) else {
            continue;
        };
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
        usage.insert(name.to_string(), pod);
    }
    usage
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
        claims.insert(
            name.to_string(),
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
        for claim in claims.keys() {
            if let Some(bytes) = cog_core::claim_footprint::used_bytes_from_exposition(&text, claim)
            {
                measured.insert(claim.clone(), bytes);
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

    NodeView {
        name,
        ready,
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
        pods: Vec::new(),
    }
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

    let pod_usage = usage.get(&name);
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
            }
        })
        .collect();

    let ready = !containers.is_empty() && containers.iter().all(|container| container.ready);
    let restarts = containers
        .iter()
        .map(|container| container.restarts)
        .max()
        .unwrap_or(0);

    let volumes = pod_volumes(pod, sources);
    let pod_events = events.get(&name).cloned().unwrap_or_default();
    let conditions = conditions_of(pod);

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
fn pod_volumes(pod: &Value, sources: &Sources) -> Vec<VolumeView> {
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
                        .and_then(|claim_name| sources.claims.get(claim_name))
                        .map(|claim| ClaimView {
                            name: claim.name.clone(),
                            phase: claim.phase.clone(),
                            storage_class: claim.storage_class.clone(),
                            capacity_bytes: claim.capacity_bytes,
                            access_modes: claim.access_modes.clone(),
                            volume_name: claim.volume_name.clone(),
                            measured_bytes: sources.measured.get(&claim.name).copied(),
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
        grouped
            .entry(name.to_string())
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

#[derive(Debug, Clone, Serialize)]
pub struct DeploymentView {
    pub name: String,
    pub ready_replicas: u32,
    pub desired_replicas: u32,
    pub updated_replicas: u32,
    pub available_replicas: u32,
    /// `deployment.kubernetes.io/revision`：这一版是第几次改动滚动上去的。
    pub revision: Option<String>,
    pub images: Vec<String>,
    /// 含 `Progressing` / `Available` 与滚动卡住时的 `ProgressDeadlineExceeded`。
    pub conditions: Vec<ConditionView>,
}

#[derive(Debug, Serialize)]
pub struct DeploymentsReport {
    pub namespace: String,
    pub error: Option<String>,
    pub deployments: Vec<DeploymentView>,
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

fn deployments_from(list: &Value) -> Vec<DeploymentView> {
    let Some(items) = list.get("items").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .map(|item| {
            let count =
                |pointer: &str| item.pointer(pointer).and_then(Value::as_u64).unwrap_or(0) as u32;
            DeploymentView {
                name: item
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                ready_replicas: count("/status/readyReplicas"),
                // spec.replicas 缺省是 1，不是 0：没写这个字段的 Deployment
                // 也是要一个副本的。
                desired_replicas: item
                    .pointer("/spec/replicas")
                    .and_then(Value::as_u64)
                    .unwrap_or(1) as u32,
                updated_replicas: count("/status/updatedReplicas"),
                available_replicas: count("/status/availableReplicas"),
                revision: item
                    .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                images: item
                    .pointer("/spec/template/spec/containers")
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
            }
        })
        .collect()
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

pub async fn cluster_deployments_handler() -> Json<DeploymentsReport> {
    let collected_at = Utc::now();
    let Ok(client) = KubeClient::in_cluster() else {
        return Json(DeploymentsReport {
            namespace: String::new(),
            error: Some("不在集群里跑，读不到部署".to_string()),
            deployments: Vec::new(),
            collected_at,
        });
    };

    let namespace = client.namespace().to_string();
    let (deployments, error) = match client
        .get_json(&format!("/apis/apps/v1/namespaces/{namespace}/deployments"))
        .await
    {
        Ok(list) => (deployments_from(&list), None),
        Err(e) => (Vec::new(), Some(format!("读不到 Deployment：{e}"))),
    };

    Json(DeploymentsReport {
        namespace,
        error,
        deployments,
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
                "involvedObject": { "kind": "Pod", "name": "api-1" },
                "type": "Warning",
                "reason": "Unhealthy",
                "message": "readiness probe failed",
                "count": minute,
                "lastTimestamp": format!("2026-10-25T13:0{minute}:00Z")
            }));
        }
        items.push(json!({
            "involvedObject": { "kind": "Service", "name": "api-1" },
            "type": "Normal",
            "reason": "EnsuredLoadBalancer",
            "message": "not a pod event"
        }));

        let grouped = events_by_object(&json!({ "items": items }));
        let events = &grouped["api-1"];
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
            "metadata": { "name": "api-1" },
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
            "metadata": { "name": "app-data" },
            "spec": { "storageClassName": "local-path" },
            "status": { "phase": "Bound", "capacity": { "storage": "10Gi" }, "accessModes": ["ReadWriteOnce"] }
        }]}));
        let mut measured = std::collections::HashMap::new();
        measured.insert("app-data".to_string(), 3_221_225_472u64);
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
                    "metadata": { "name": "app-data" },
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
            "metadata": { "name": "app-data" },
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

        let deployments = deployments_from(&list);
        let deployment = &deployments[0];
        assert_eq!(deployment.revision.as_deref(), Some("7"));
        assert_eq!(deployment.desired_replicas, 2);
        assert_eq!(deployment.ready_replicas, 1);
        assert_eq!(deployment.available_replicas, 1);
        assert_eq!(deployment.images, vec!["cogneva:abc", "proxy:def"]);
        assert!(
            !deployment.conditions[0].ok,
            "Progressing=False 是滚动卡住，不是进行中"
        );
    }

    #[test]
    fn a_deployment_without_annotations_still_says_how_many_it_wants() {
        let list = json!({"items": [{"metadata": {"name": "bare"}, "spec": {}}]});

        let deployments = deployments_from(&list);
        assert_eq!(deployments[0].desired_replicas, 1, "缺省是 1，不是 0");
        assert_eq!(deployments[0].revision, None);
        assert!(deployments[0].images.is_empty());
    }
}
