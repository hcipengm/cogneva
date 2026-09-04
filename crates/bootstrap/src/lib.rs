//! k8m 多集群管理计划（审计 2.5.2）+ backend 自动部署规划（审计 2.5.3）。
//!
//! `ManagementPlan` 是 K3s 与 K8s 共用的声明式部署抽象：AI 只生成/调用
//! 统一计划，框架按环境标签把同一计划渲染为 Helm values（profile）并同步
//! 到目标集群。backend 选择规则内聚于此，无需业务代码改动。

use serde::{Deserialize, Serialize};

/// 部署分支（与 main.rs 的探测规则一致）。分支名是历史命名：元启动新建集群时
/// 两个分支装的都是 K3s——`K3s` = K3s 单节点，`K8s` = K3s 多节点（server +
/// agents）。标准 Kubernetes（kubeadm / EKS 等）元启动不新建、只复用用户已
/// 搭好的集群；原因不是技术不可行，而是成本收益不对等（见 main.rs 的
/// `ensure_multi_node_cluster`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlanBranch {
    K3s,
    K8s,
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

/// 分支规则：内存 < 2GB 或单节点 → K3s 单节点；多节点高配 → K3s 多节点
/// （server + agents）。两个分支新建的都是 K3s；标准 Kubernetes 不在新建
/// 范围内，只复用用户既有集群（见 main.rs `ensure_multi_node_cluster`）。
pub fn decide_branch(hw: &HardwareProfile) -> PlanBranch {
    if hw.memory_gb < 2 || hw.nodes <= 1 {
        PlanBranch::K3s
    } else {
        PlanBranch::K8s
    }
}

/// backend 自动选择规则（审计 2.5.3）：
/// - postgres：持久层必选；
/// - redis：单节点/低内存消息与配额缓存；
/// - nats：仅多节点分支（K3s 多节点用 JetStream 事件总线，单节点由 Redis Streams 覆盖）；
/// - qdrant：内存 ≥ 4GB 才本地部署，否则禁用（走外部向量库）；
/// - mysql/meilisearch：默认禁用，显式需要时由 AI 在计划里开启。
pub fn decide_backends(hw: &HardwareProfile, branch: PlanBranch) -> Vec<BackendSpec> {
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
            enabled: branch == PlanBranch::K8s,
            reason: if branch == PlanBranch::K8s {
                "多节点分支：JetStream 事件总线".into()
            } else {
                "单节点分支：Redis Streams 已覆盖".into()
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
    /// 依据硬件画像与环境标签生成声明式计划。
    pub fn for_environment(environment: impl Into<String>, hw: &HardwareProfile) -> Self {
        let environment = environment.into();
        let branch = decide_branch(hw);
        Self {
            api_version: "k8m.cogneva/v1alpha1".into(),
            kind: "ManagementPlan".into(),
            metadata: PlanMeta {
                name: format!("cogneva-{}", environment),
                environment,
            },
            spec: PlanSpec {
                branch,
                image_tag: env!("CARGO_PKG_VERSION").into(),
                gateway_replicas: match branch {
                    PlanBranch::K3s => 1,
                    PlanBranch::K8s => 3,
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
            "cogneva".to_string(),
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
        assert_eq!(decide_branch(&hw(1, 1)), PlanBranch::K3s);
        assert_eq!(decide_branch(&hw(16, 1)), PlanBranch::K3s);
        assert_eq!(decide_branch(&hw(16, 3)), PlanBranch::K8s);
    }

    #[test]
    fn backend_rules_follow_environment() {
        let light = decide_backends(&hw(1, 1), PlanBranch::K3s);
        let nats = light.iter().find(|b| b.kind == "nats").unwrap();
        assert!(!nats.enabled);
        let qdrant = light.iter().find(|b| b.kind == "qdrant").unwrap();
        assert!(!qdrant.enabled);

        let prod = decide_backends(&hw(16, 3), PlanBranch::K8s);
        assert!(prod.iter().find(|b| b.kind == "nats").unwrap().enabled);
        assert!(prod.iter().find(|b| b.kind == "qdrant").unwrap().enabled);
        assert!(!prod.iter().find(|b| b.kind == "mysql").unwrap().enabled);
    }

    #[test]
    fn plan_yaml_roundtrip() {
        let plan = ManagementPlan::for_environment("prod", &hw(16, 3));
        let yaml = plan.to_yaml().unwrap();
        assert!(yaml.contains("k8m.cogneva/v1alpha1"));
        assert!(yaml.contains("ManagementPlan"));
        let parsed = ManagementPlan::from_yaml(&yaml).unwrap();
        assert_eq!(parsed.spec.branch, PlanBranch::K8s);
        assert_eq!(parsed.spec.gateway_replicas, 3);
    }

    #[test]
    fn helm_values_reflect_enabled_backends() {
        let plan = ManagementPlan::for_environment("edge", &hw(1, 1));
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
