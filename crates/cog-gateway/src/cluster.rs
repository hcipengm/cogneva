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
    /// 稳定标识，前端用它做选中态。
    pub id: String,
    pub name: String,
    /// 这次读取有没有真的读到集群。`false` 时 `error` 说明为什么。
    pub reachable: bool,
    pub error: Option<String>,
    pub version: Option<String>,
    /// 这个集群有没有 metrics-server。`false` 时所有用量字段都是 `None`，
    /// 界面要说明白「只看到申报量」，而不是画一条零的线。
    pub metrics_available: bool,
    pub nodes: Vec<NodeView>,
    /// 没有落到任何节点上的 Pod（待调度、或节点被删）。它们不出现在任何节点下，
    /// 但还是集群里的事实，不能因为画不出来就消失。
    pub unscheduled: Vec<PodView>,
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
#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
pub struct ClaimView {
    pub name: String,
    /// Pending / Bound / Lost
    pub phase: String,
    pub storage_class: Option<String>,
    pub capacity_bytes: Option<u64>,
    pub access_modes: Vec<String>,
    pub volume_name: Option<String>,
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
        name: client.namespace().to_string(),
        reachable: true,
        error: None,
        version: None,
        metrics_available: false,
        nodes: Vec::new(),
        unscheduled: Vec::new(),
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

    // 用量来自 metrics-server，它可能压根没装。读不到不是错误：这一面照样能画申报量，
    // 只是要在界面上说清楚「这里只有申报量」。把它当成读失败，会让一个没装组件的
    // 集群整天亮着一张红条。
    let (usage, metrics_available) = match client
        .get_json(&format!(
            "/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods",
            client.namespace()
        ))
        .await
    {
        Ok(list) => (usage_by_pod(&list), true),
        Err(_) => (std::collections::HashMap::new(), false),
    };
    view.metrics_available = metrics_available;

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
            let parsed: Vec<PodView> = pods
                .get("items")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .map(|pod| pod_view(pod, &events, &usage, &claims))
                        .collect()
                })
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

    if metrics_available {
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

/// metrics-server 回的每个 Pod：各容器用量，加总成 Pod 的用量。
///
/// 只收容器粒度再自己加，是因为 Pod 那一级只有总数时，读者点进某个容器会看到
/// 一个和 Pod 对不上的数——两个数并排出现在同一屏，就会有一个被当成错的。
#[derive(Debug, Default)]
struct PodUsage {
    cpu: u64,
    memory: u64,
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
            },
        );
    }
    claims
}

/// 把 Pod 挂到它所在的节点上，剩下的进 `unscheduled`。
///
/// 单独成一个函数是因为这条映射是这一面唯一会错的地方：节点名对不上（节点刚被
/// 删、Pod 刚从别处搬来）、Pod 还没被调度、命名空间里读到的 Pod 落在不归这一面
/// 管的节点上——三种都要落到看得见的位置，而不是静默消失。
fn place_pods(cluster: &mut ClusterView, pods: Vec<PodView>) {
    for pod in pods {
        let placed = pod.node.as_ref().and_then(|node| {
            cluster
                .nodes
                .iter_mut()
                .find(|candidate| &candidate.name == node)
        });
        match placed {
            Some(node) => {
                node.cpu_requested_millicores += pod.cpu_requested_millicores;
                node.memory_requested_bytes += pod.memory_requested_bytes;
                node.pods.push(pod);
            }
            None => cluster.unscheduled.push(pod),
        }
    }
    for node in &mut cluster.nodes {
        node.pods
            .sort_by(|a, b| a.name.cmp(&b.name).then(a.namespace.cmp(&b.namespace)));
    }
    cluster.unscheduled.sort_by(|a, b| a.name.cmp(&b.name));
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

fn pod_view(
    pod: &Value,
    events: &std::collections::HashMap<String, Vec<EventView>>,
    usage: &std::collections::HashMap<String, PodUsage>,
    claims: &std::collections::HashMap<String, ClaimView>,
) -> PodView {
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

    let volumes = pod_volumes(pod, claims);
    let pod_events = events.get(&name).cloned().unwrap_or_default();
    let conditions = pod_conditions(pod);

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

/// 读 `status.conditions`。`ok` 只看 `status == "True"`：Kubernetes 用的是字符串
/// 三元组，缺字段按「没成立」处理——把未知读成成立，会让一个卡住的 Pod 显示成正常。
fn pod_conditions(pod: &Value) -> Vec<ConditionView> {
    pod.pointer("/status/conditions")
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
fn pod_volumes(
    pod: &Value,
    claims: &std::collections::HashMap<String, ClaimView>,
) -> Vec<VolumeView> {
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
                        .and_then(|claim_name| claims.get(claim_name))
                        .map(|claim| ClaimView {
                            name: claim.name.clone(),
                            phase: claim.phase.clone(),
                            storage_class: claim.storage_class.clone(),
                            capacity_bytes: claim.capacity_bytes,
                            access_modes: claim.access_modes.clone(),
                            volume_name: claim.volume_name.clone(),
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

    fn none() -> std::collections::HashMap<String, Vec<EventView>> {
        std::collections::HashMap::new()
    }

    fn no_usage() -> std::collections::HashMap<String, PodUsage> {
        std::collections::HashMap::new()
    }

    fn no_claims() -> std::collections::HashMap<String, ClaimView> {
        std::collections::HashMap::new()
    }

    fn cluster_with(nodes: Vec<serde_json::Value>) -> ClusterView {
        ClusterView {
            id: "in-cluster".to_string(),
            name: "cogneva".to_string(),
            reachable: true,
            error: None,
            version: None,
            metrics_available: false,
            nodes: nodes.iter().map(node_view).collect(),
            unscheduled: Vec::new(),
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
                    &none(),
                    &no_usage(),
                    &no_claims(),
                ),
                pod_view(
                    &pod("worker-1", Some("cogneva-01"), "1500m", "2Gi"),
                    &none(),
                    &no_usage(),
                    &no_claims(),
                ),
                pod_view(
                    &pod("cache-1", Some("cogneva-02"), "250m", "512Mi"),
                    &none(),
                    &no_usage(),
                    &no_claims(),
                ),
                pod_view(
                    &pod("waiting-1", None, "100m", "64Mi"),
                    &none(),
                    &no_usage(),
                    &no_claims(),
                ),
                // 节点刚被删：Pod 上还写着那个名字，但它已经不在任何节点下。
                pod_view(
                    &pod("orphan-1", Some("cogneva-gone"), "100m", "64Mi"),
                    &none(),
                    &no_usage(),
                    &no_claims(),
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
        // 名字对不上节点的 Pod 与没被调度的 Pod 都是集群里的事实，不能消失。
        assert_eq!(cluster.unscheduled.len(), 2);
        let names: Vec<&str> = cluster
            .unscheduled
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(names, vec!["orphan-1", "waiting-1"]);
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
        let view = pod_view(&pod, &none(), &no_usage(), &no_claims());
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

        let without = pod_view(&pod, &none(), &no_usage(), &no_claims());
        assert_eq!(
            without.cpu_used_millicores, None,
            "没有数据源时是「不知道」，不是 0"
        );

        let metrics = json!({ "items": [{
            "metadata": { "name": "api-1" },
            "containers": [{ "name": "api", "usage": { "cpu": "120m", "memory": "300Mi" } }]
        }]});
        let usage = usage_by_pod(&metrics);
        let with = pod_view(&pod, &none(), &usage, &no_claims());
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
        let view = pod_view(&pod, &none(), &no_usage(), &no_claims());
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
        assert!(
            pod_view(&fresh, &none(), &no_usage(), &no_claims()).containers[0]
                .last_termination
                .is_none()
        );
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
        let container = &pod_view(&pod, &none(), &no_usage(), &no_claims()).containers[0];
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
        let conditions = pod_view(&pod, &none(), &no_usage(), &no_claims()).conditions;
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
        let conditions = pod_view(&pod, &none(), &no_usage(), &no_claims()).conditions;
        assert!(
            !conditions[0].ok,
            "缺 status 字段按没成立算：读成成立会把卡住的 Pod 画成正常"
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

        let view = pod_view(&pod, &none(), &no_usage(), &claims);
        let claim = view.volumes[0].claim.as_ref().expect("持久卷要带上声明");
        assert_eq!(claim.phase, "Bound");
        assert_eq!(claim.storage_class.as_deref(), Some("local-path"));
        assert_eq!(claim.capacity_bytes, Some(10 * 1024 * 1024 * 1024));
        assert_eq!(claim.access_modes, vec!["ReadWriteOnce"]);
    }
}
