//! 系统部署测试 Harness（docs/2026-06-29_16-00 cog-eval 增强方案 §2.2 模块 3）。
//! 负责 D10（部署）/ D13（防御）指标的采集编排：
//! - 端到端部署时间（curl|sh → WebUI ready）、部署步数、一次成功率；
//! - 故障注入 + MTTR 测量（PodCrash / NodeOffline / DiskFull）；
//! - 安全渗透探测（凭证外泄 / 沙盒逃逸 / 网关绕过）；
//! - 大规模并发 Agent 伸缩弹性。
//!   具体环境操作（kubectl / buildah / 渗透脚本）由调用方以 SystemRunner 注入，
//!   本模块只做计时、聚合与判定，保证可单测。

use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// 目标机器规格（1C2G / 4C8G / 16C32G …）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineSpec {
    pub name: String,
    pub cpu_cores: u32,
    pub memory_gb: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultType {
    PodCrash,
    NodeOffline,
    DiskFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityScenario {
    /// 沙盒尝试外泄凭证 → 安全网关应拦截。
    CredentialExfiltration,
    /// 沙盒逃逸尝试。
    SandboxEscape,
    /// 绕过安全网关直连外网 → NetworkPolicy 应拦截。
    GatewayBypass,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeploymentTestConfig {
    pub machine_specs: Vec<MachineSpec>,
    #[serde(default = "default_repetitions")]
    pub repetitions: u32,
    #[serde(default)]
    pub fault_scenarios: Vec<FaultType>,
}

fn default_repetitions() -> u32 {
    3
}

/// 单次部署观测。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployObservation {
    pub duration_ms: u64,
    pub step_count: u32,
    pub success: bool,
    /// 非首次重复部署时填 false（一次成功率统计用）。
    pub first_attempt: bool,
}

/// 故障注入观测：从注入到服务恢复的时间。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultObservation {
    pub fault: FaultType,
    pub mttr_ms: u64,
    pub auto_recovered: bool,
}

/// 安全探测观测：攻击是否被防线拦截。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityObservation {
    pub scenario: SecurityScenario,
    pub intercepted: bool,
    /// 防线名称（security_gateway / network_policy / sandbox …）。
    pub intercepted_by: Option<String>,
}

/// 伸缩观测：N 个 Agent 启动延迟分布。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleObservation {
    pub agent_count: u32,
    pub startup_latencies_ms: Vec<u64>,
    pub cpu_overhead_pct: f64,
    pub memory_overhead_pct: f64,
}

impl ScaleObservation {
    pub fn percentile(&self, pct: f64) -> f64 {
        let mut s = self.startup_latencies_ms.clone();
        if s.is_empty() {
            return 0.0;
        }
        s.sort_unstable();
        let idx = ((s.len() - 1) as f64 * pct).round() as usize;
        s[idx] as f64
    }
}

/// 环境操作抽象：由调用方实现（真实环境 = kubectl/buildah 脚本；测试 = fake）。
#[async_trait::async_trait]
pub trait SystemRunner: Send + Sync {
    async fn deploy(
        &self,
        spec: &MachineSpec,
        first_attempt: bool,
    ) -> Result<DeployObservation, String>;
    async fn inject_fault(&self, fault: FaultType) -> Result<FaultObservation, String>;
    async fn probe_security(
        &self,
        scenario: SecurityScenario,
    ) -> Result<SecurityObservation, String>;
    async fn scale(&self, agent_count: u32) -> Result<ScaleObservation, String>;
}

/// D10 指标汇总（对应 d10_deployment 维度的测量值来源）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct D10Metrics {
    pub deployment_time_ms: f64,
    pub deployment_step_count: f64,
    pub first_time_success_rate: f64,
    pub mttr_pod_crash_ms: Option<f64>,
    pub mttr_node_offline_ms: Option<f64>,
    pub mttr_disk_full_ms: Option<f64>,
    pub scale_elasticity_p50_ms: Option<f64>,
    pub scale_elasticity_p99_ms: Option<f64>,
    pub resource_overhead_cpu_pct: Option<f64>,
    pub resource_overhead_memory_pct: Option<f64>,
}

/// D13 指标汇总。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct D13Metrics {
    /// 凭证外泄拦截率（0-1）。
    pub credential_leak_intercept_rate: f64,
    /// 沙盒逃逸成功率（0-1，越低越好）。
    pub sandbox_escape_success_rate: f64,
    /// 绕过网关攻击被拦截的比例。
    pub gateway_bypass_block_rate: f64,
}

pub struct SystemEvalHarness {
    runner: Arc<dyn SystemRunner>,
    config: DeploymentTestConfig,
}

impl SystemEvalHarness {
    pub fn new(runner: Arc<dyn SystemRunner>, config: DeploymentTestConfig) -> Self {
        Self { runner, config }
    }

    /// 端到端部署时间测量（每个规格跑 repetitions 次，取均值 + 一次成功率）。
    pub async fn measure_deployment_time(&self, spec: &MachineSpec) -> D10Metrics {
        let reps = self.config.repetitions.max(1);
        let mut durations = Vec::new();
        let mut steps = Vec::new();
        let mut first_success = 0usize;
        let mut first_total = 0usize;
        for i in 0..reps {
            let first_attempt = i == 0;
            match self.runner.deploy(spec, first_attempt).await {
                Ok(obs) => {
                    if obs.success {
                        durations.push(obs.duration_ms as f64);
                        steps.push(obs.step_count as f64);
                    }
                    if first_attempt {
                        first_total += 1;
                        first_success += obs.success as usize;
                    }
                }
                Err(e) => {
                    tracing::warn!(spec = %spec.name, error = %e, "部署测量失败");
                    if first_attempt {
                        first_total += 1;
                    }
                }
            }
        }
        let mean = |v: &[f64]| {
            if v.is_empty() {
                0.0
            } else {
                v.iter().sum::<f64>() / v.len() as f64
            }
        };
        D10Metrics {
            deployment_time_ms: mean(&durations),
            deployment_step_count: mean(&steps),
            first_time_success_rate: if first_total == 0 {
                0.0
            } else {
                first_success as f64 / first_total as f64
            },
            ..Default::default()
        }
    }

    /// 故障注入 + MTTR 测量，结果并入 D10 指标。
    pub async fn inject_fault_and_measure_mttr(
        &self,
        fault: FaultType,
    ) -> Result<FaultObservation, String> {
        self.runner.inject_fault(fault).await
    }

    /// 安全渗透探测（单场景）。
    pub async fn security_probe(
        &self,
        scenario: SecurityScenario,
    ) -> Result<SecurityObservation, String> {
        self.runner.probe_security(scenario).await
    }

    /// 对全部故障场景跑一轮，聚合进 D10。
    pub async fn measure_fault_tolerance(&self, base: &mut D10Metrics) {
        for fault in self.config.fault_scenarios.clone() {
            match self.runner.inject_fault(fault).await {
                Ok(obs) => {
                    let ms = Some(obs.mttr_ms as f64);
                    match fault {
                        FaultType::PodCrash => base.mttr_pod_crash_ms = ms,
                        FaultType::NodeOffline => base.mttr_node_offline_ms = ms,
                        FaultType::DiskFull => base.mttr_disk_full_ms = ms,
                    }
                }
                Err(e) => tracing::warn!(fault = ?fault, error = %e, "故障注入失败"),
            }
        }
    }

    /// 大规模并发 Agent 伸缩测试，聚合进 D10。
    pub async fn scale_test(&self, agent_count: u32, base: &mut D10Metrics) {
        match self.runner.scale(agent_count).await {
            Ok(obs) => {
                base.scale_elasticity_p50_ms = Some(obs.percentile(0.50));
                base.scale_elasticity_p99_ms = Some(obs.percentile(0.99));
                base.resource_overhead_cpu_pct = Some(obs.cpu_overhead_pct);
                base.resource_overhead_memory_pct = Some(obs.memory_overhead_pct);
            }
            Err(e) => tracing::warn!(agent_count, error = %e, "伸缩测试失败"),
        }
    }

    /// 全场景安全渗透，聚合 D13。
    pub async fn measure_defense(&self, repetitions: u32) -> D13Metrics {
        let mut leak_blocked = 0usize;
        let mut leak_total = 0usize;
        let mut escape_succeeded = 0usize;
        let mut escape_total = 0usize;
        let mut bypass_blocked = 0usize;
        let mut bypass_total = 0usize;
        for _ in 0..repetitions.max(1) {
            for scenario in [
                SecurityScenario::CredentialExfiltration,
                SecurityScenario::SandboxEscape,
                SecurityScenario::GatewayBypass,
            ] {
                match self.runner.probe_security(scenario).await {
                    Ok(obs) => match scenario {
                        SecurityScenario::CredentialExfiltration => {
                            leak_total += 1;
                            leak_blocked += obs.intercepted as usize;
                        }
                        SecurityScenario::SandboxEscape => {
                            escape_total += 1;
                            escape_succeeded += (!obs.intercepted) as usize;
                        }
                        SecurityScenario::GatewayBypass => {
                            bypass_total += 1;
                            bypass_blocked += obs.intercepted as usize;
                        }
                    },
                    Err(e) => tracing::warn!(scenario = ?scenario, error = %e, "安全探测失败"),
                }
            }
        }
        let rate = |a: usize, b: usize| if b == 0 { 0.0 } else { a as f64 / b as f64 };
        D13Metrics {
            credential_leak_intercept_rate: rate(leak_blocked, leak_total),
            sandbox_escape_success_rate: rate(escape_succeeded, escape_total),
            gateway_bypass_block_rate: rate(bypass_blocked, bypass_total),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeRunner {
        deploy_ms: u64,
        mttr_ms: u64,
        intercepted: bool,
    }

    #[async_trait::async_trait]
    impl SystemRunner for FakeRunner {
        async fn deploy(
            &self,
            _spec: &MachineSpec,
            first_attempt: bool,
        ) -> Result<DeployObservation, String> {
            Ok(DeployObservation {
                duration_ms: self.deploy_ms,
                step_count: 5,
                success: true,
                first_attempt,
            })
        }
        async fn inject_fault(&self, fault: FaultType) -> Result<FaultObservation, String> {
            Ok(FaultObservation {
                fault,
                mttr_ms: self.mttr_ms,
                auto_recovered: true,
            })
        }
        async fn probe_security(
            &self,
            scenario: SecurityScenario,
        ) -> Result<SecurityObservation, String> {
            Ok(SecurityObservation {
                scenario,
                intercepted: self.intercepted,
                intercepted_by: Some("security_gateway".into()),
            })
        }
        async fn scale(&self, agent_count: u32) -> Result<ScaleObservation, String> {
            Ok(ScaleObservation {
                agent_count,
                startup_latencies_ms: (1..=agent_count as u64).map(|i| i * 10).collect(),
                cpu_overhead_pct: 5.0,
                memory_overhead_pct: 8.0,
            })
        }
    }

    fn spec() -> MachineSpec {
        MachineSpec {
            name: "4C8G".into(),
            cpu_cores: 4,
            memory_gb: 8,
        }
    }

    #[tokio::test]
    async fn deployment_time_aggregated() {
        let h = SystemEvalHarness::new(
            Arc::new(FakeRunner {
                deploy_ms: 120_000,
                mttr_ms: 30_000,
                intercepted: true,
            }),
            DeploymentTestConfig {
                machine_specs: vec![spec()],
                repetitions: 3,
                fault_scenarios: vec![FaultType::PodCrash],
            },
        );
        let mut d10 = h.measure_deployment_time(&spec()).await;
        assert!((d10.deployment_time_ms - 120_000.0).abs() < 1e-9);
        assert!((d10.first_time_success_rate - 1.0).abs() < 1e-9);
        h.measure_fault_tolerance(&mut d10).await;
        assert_eq!(d10.mttr_pod_crash_ms, Some(30_000.0));
        h.scale_test(150, &mut d10).await;
        assert!(d10.scale_elasticity_p50_ms.unwrap() > 0.0);
        assert_eq!(d10.scale_elasticity_p99_ms, Some(1490.0));
    }

    #[tokio::test]
    async fn defense_rates() {
        let h = SystemEvalHarness::new(
            Arc::new(FakeRunner {
                deploy_ms: 0,
                mttr_ms: 0,
                intercepted: true,
            }),
            DeploymentTestConfig::default(),
        );
        let d13 = h.measure_defense(2).await;
        assert!((d13.credential_leak_intercept_rate - 1.0).abs() < 1e-9);
        assert!((d13.sandbox_escape_success_rate - 0.0).abs() < 1e-9);
        assert!((d13.gateway_bypass_block_rate - 1.0).abs() < 1e-9);
    }
}
