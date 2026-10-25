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
    pub containers: Vec<ContainerView>,
}

#[derive(Debug, Serialize)]
pub struct ContainerView {
    pub name: String,
    pub image: String,
    pub ready: bool,
    pub restarts: u32,
    /// running / waiting:<原因> / terminated:<原因> / unknown
    pub state: String,
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

    let pods_path = format!("/api/v1/namespaces/{}/pods", client.namespace());
    match client.get_json(&pods_path).await {
        Ok(pods) => {
            let parsed: Vec<PodView> = pods
                .get("items")
                .and_then(Value::as_array)
                .map(|items| items.iter().map(pod_view).collect())
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

    Json(Topology {
        clusters: vec![view],
        collected_at,
    })
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
        pods: Vec::new(),
    }
}

fn pod_view(pod: &Value) -> PodView {
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
            }
        })
        .collect();

    let ready = !containers.is_empty() && containers.iter().all(|container| container.ready);
    let restarts = containers
        .iter()
        .map(|container| container.restarts)
        .max()
        .unwrap_or(0);

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
    }
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

    fn cluster_with(nodes: Vec<serde_json::Value>) -> ClusterView {
        ClusterView {
            id: "in-cluster".to_string(),
            name: "cogneva".to_string(),
            reachable: true,
            error: None,
            version: None,
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
        // 62Gi 是 62 × 1024³。把它读成 62×10⁹ 会让「还剩多少」差出 3 GiB。
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
                pod_view(&pod("api-1", Some("cogneva-01"), "500m", "1Gi")),
                pod_view(&pod("worker-1", Some("cogneva-01"), "1500m", "2Gi")),
                pod_view(&pod("cache-1", Some("cogneva-02"), "250m", "512Mi")),
                pod_view(&pod("waiting-1", None, "100m", "64Mi")),
                // 节点刚被删：Pod 上还写着那个名字，但它已经不在任何节点下。
                pod_view(&pod("orphan-1", Some("cogneva-gone"), "100m", "64Mi")),
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
}
