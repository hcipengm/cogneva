//! Sandbox boundary detection for the self-evolution auto-deploy pipeline.
//!
//! `auto_apply` / `auto_deploy` default to `true` (real autonomy), but
//! applying and deploying LLM-generated changes must only happen inside an
//! isolated environment (Kubernetes Pod, container, or an explicitly
//! declared sandbox). When no isolation is detected and the operator has not
//! set `force_autonomous`, the pipeline is downgraded to dry-run:
//! changes are still applied and tested, but the working tree is rolled
//! back and no binary switch happens.

use cog_core::SelfEvolutionConfig;

/// The kind of isolated environment that was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxKind {
    /// Running inside a Kubernetes Pod (service account env or token).
    Kubernetes,
    /// Running inside a generic container (docker/containerd cgroup markers).
    Container,
    /// Working directory lives under the dedicated sandbox root
    /// (`COGNEVA_SANDBOX_DIR`, default `/opt/cogneva/sandbox`).
    DedicatedDir,
    /// Operator declared a sandbox via the `COGNEVA_SANDBOX` env var.
    DeclaredEnv,
}

impl std::fmt::Display for SandboxKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SandboxKind::Kubernetes => write!(f, "kubernetes"),
            SandboxKind::Container => write!(f, "container"),
            SandboxKind::DedicatedDir => write!(f, "dedicated-dir"),
            SandboxKind::DeclaredEnv => write!(f, "declared-env"),
        }
    }
}

/// Default dedicated sandbox root, matching the K3s evolution pod layout
/// (`/opt/cogneva/sandbox/src` is the pod working repo).
pub const DEFAULT_SANDBOX_DIR: &str = "/opt/cogneva/sandbox";

/// Raw environment signals, separated from interpretation so detection
/// stays unit-testable without touching the real host.
#[derive(Debug, Clone, Default)]
pub struct SandboxSignals {
    /// Value of `KUBERNETES_SERVICE_HOST`, if set.
    pub kubernetes_service_host: Option<String>,
    /// Whether `/var/run/secrets/kubernetes.io/serviceaccount` exists.
    pub kubernetes_service_account: bool,
    /// Whether `/.dockerenv` exists.
    pub dockerenv: bool,
    /// Whether `/run/.containerenv` exists.
    pub containerenv: bool,
    /// Contents of `/proc/1/cgroup`, if readable.
    pub cgroup_v1: Option<String>,
    /// Value of `COGNEVA_SANDBOX`, if set.
    pub cogneva_sandbox: Option<String>,
    /// Dedicated sandbox root: `COGNEVA_SANDBOX_DIR` if set, else
    /// [`DEFAULT_SANDBOX_DIR`].
    pub sandbox_dir: Option<std::path::PathBuf>,
    /// The process working directory, tested for containment in
    /// `sandbox_dir`.
    pub current_dir: Option<std::path::PathBuf>,
}

impl SandboxSignals {
    /// Collect signals from the real host environment.
    pub fn from_environment() -> Self {
        Self {
            kubernetes_service_host: std::env::var("KUBERNETES_SERVICE_HOST").ok(),
            kubernetes_service_account: std::path::Path::new(
                "/var/run/secrets/kubernetes.io/serviceaccount",
            )
            .exists(),
            dockerenv: std::path::Path::new("/.dockerenv").exists(),
            containerenv: std::path::Path::new("/run/.containerenv").exists(),
            cgroup_v1: std::fs::read_to_string("/proc/1/cgroup").ok(),
            cogneva_sandbox: std::env::var("COGNEVA_SANDBOX").ok(),
            sandbox_dir: Some(
                std::env::var("COGNEVA_SANDBOX_DIR")
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_SANDBOX_DIR)),
            ),
            current_dir: std::env::current_dir().ok(),
        }
    }
}

/// Interpret the signals: return the detected sandbox kind, if any.
pub fn detect_sandbox(signals: &SandboxSignals) -> Option<SandboxKind> {
    if signals.kubernetes_service_host.is_some() || signals.kubernetes_service_account {
        return Some(SandboxKind::Kubernetes);
    }

    if signals.dockerenv || signals.containerenv {
        return Some(SandboxKind::Container);
    }

    if let Some(ref cgroup) = signals.cgroup_v1 {
        let container_markers = ["docker", "kubepods", "containerd", "podman", "lxc"];
        if container_markers.iter().any(|m| cgroup.contains(m)) {
            return Some(SandboxKind::Container);
        }
    }

    if let Some(ref flag) = signals.cogneva_sandbox {
        let truthy = matches!(flag.as_str(), "1" | "true" | "TRUE" | "yes" | "YES");
        if truthy {
            return Some(SandboxKind::DeclaredEnv);
        }
    }

    if let (Some(sandbox_dir), Some(current_dir)) = (&signals.sandbox_dir, &signals.current_dir) {
        if current_dir.starts_with(sandbox_dir) {
            return Some(SandboxKind::DedicatedDir);
        }
    }

    None
}

/// The result of enforcing the sandbox boundary on a config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryDecision {
    /// Auto apply/deploy may run for real; carries the detected isolation
    /// or the reason it is not required.
    Allowed(String),
    /// No isolation detected: auto_apply/auto_deploy were downgraded to
    /// dry-run. Carries a human-readable explanation.
    Downgraded(String),
}

/// Enforce the sandbox boundary on `config`.
///
/// Returns the (possibly downgraded) effective config plus the decision.
/// Isolation is accepted from any of: a detected sandbox environment,
/// `sandbox_mode = true` (operator declares a dedicated sandbox), or
/// `force_autonomous = true` (explicit bypass).
pub fn enforce_sandbox_boundary(
    config: &SelfEvolutionConfig,
    signals: &SandboxSignals,
) -> (SelfEvolutionConfig, BoundaryDecision) {
    if !config.auto_apply && !config.auto_deploy {
        return (
            config.clone(),
            BoundaryDecision::Allowed(
                "auto_apply and auto_deploy are both disabled; nothing to protect".into(),
            ),
        );
    }

    if let Some(kind) = detect_sandbox(signals) {
        return (
            config.clone(),
            BoundaryDecision::Allowed(format!("isolated environment detected: {}", kind)),
        );
    }

    // Firecracker 编排者（审计 2.5.4）：host 不直接执行 change，而是冷启动
    // 微虚拟机、由 VM 在硬件级隔离内执行进化；host 侧因此无需自身处于
    // 沙盒环境。
    if config.microvm.enabled {
        return (
            config.clone(),
            BoundaryDecision::Allowed(
                "microvm.enabled=true: host orchestrates cold-start firecracker VMs; \
                 evolution executes inside the microVM, not on this host"
                    .into(),
            ),
        );
    }

    if config.sandbox_mode {
        return (
            config.clone(),
            BoundaryDecision::Allowed(
                "sandbox_mode=true declares a dedicated sandbox environment".into(),
            ),
        );
    }

    if config.force_autonomous {
        return (
            config.clone(),
            BoundaryDecision::Allowed(
                "force_autonomous=true: operator explicitly bypassed the sandbox boundary check"
                    .into(),
            ),
        );
    }

    let mut downgraded = config.clone();
    downgraded.auto_apply = false;
    downgraded.auto_deploy = false;
    (
        downgraded,
        BoundaryDecision::Downgraded(
            "auto_apply/auto_deploy requested but no isolated environment detected \
             (no Kubernetes Pod, container, dedicated sandbox directory, or \
             COGNEVA_SANDBOX marker). \
             Downgraded to dry-run: changes are applied and tested but rolled back, \
             and no binary switch will happen. Set self_evolution.sandbox_mode=true \
             when running in a dedicated sandbox, or self_evolution.force_autonomous=true \
             to bypass this check explicitly."
                .into(),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bare_signals() -> SandboxSignals {
        SandboxSignals {
            cgroup_v1: Some("1:name=systemd:/\n2:cpu:/".into()),
            ..Default::default()
        }
    }

    #[test]
    fn detects_kubernetes_via_env() {
        let signals = SandboxSignals {
            kubernetes_service_host: Some("10.96.0.1".into()),
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), Some(SandboxKind::Kubernetes));
    }

    #[test]
    fn detects_kubernetes_via_service_account() {
        let signals = SandboxSignals {
            kubernetes_service_account: true,
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), Some(SandboxKind::Kubernetes));
    }

    #[test]
    fn detects_container_via_dockerenv() {
        let signals = SandboxSignals {
            dockerenv: true,
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), Some(SandboxKind::Container));
    }

    #[test]
    fn detects_container_via_cgroup() {
        let signals = SandboxSignals {
            cgroup_v1: Some("1:name=systemd:/kubepods/burstable/pod123/abc".into()),
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), Some(SandboxKind::Container));
    }

    #[test]
    fn detects_declared_env() {
        let signals = SandboxSignals {
            cogneva_sandbox: Some("true".into()),
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), Some(SandboxKind::DeclaredEnv));
    }

    #[test]
    fn bare_host_detects_nothing() {
        assert_eq!(detect_sandbox(&bare_signals()), None);
    }

    #[test]
    fn falsey_declared_env_detects_nothing() {
        let signals = SandboxSignals {
            cogneva_sandbox: Some("0".into()),
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), None);
    }

    #[test]
    fn detects_dedicated_dir_containment() {
        let signals = SandboxSignals {
            sandbox_dir: Some(std::path::PathBuf::from("/opt/cogneva/sandbox")),
            current_dir: Some(std::path::PathBuf::from("/opt/cogneva/sandbox/src")),
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), Some(SandboxKind::DedicatedDir));
    }

    #[test]
    fn dedicated_dir_exact_root_counts() {
        let signals = SandboxSignals {
            sandbox_dir: Some(std::path::PathBuf::from("/opt/cogneva/sandbox")),
            current_dir: Some(std::path::PathBuf::from("/opt/cogneva/sandbox")),
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), Some(SandboxKind::DedicatedDir));
    }

    #[test]
    fn sibling_dir_does_not_count() {
        // `/opt/cogneva/sandboxx` must not match `/opt/cogneva/sandbox`.
        let signals = SandboxSignals {
            sandbox_dir: Some(std::path::PathBuf::from("/opt/cogneva/sandbox")),
            current_dir: Some(std::path::PathBuf::from("/opt/cogneva/sandboxx")),
            ..Default::default()
        };
        assert_eq!(detect_sandbox(&signals), None);
    }

    #[test]
    fn downgrades_when_no_isolation() {
        let config = SelfEvolutionConfig {
            auto_apply: true,
            auto_deploy: true,
            ..Default::default()
        };
        let (effective, decision) = enforce_sandbox_boundary(&config, &bare_signals());
        assert!(!effective.auto_apply);
        assert!(!effective.auto_deploy);
        assert!(matches!(decision, BoundaryDecision::Downgraded(_)));
    }

    #[test]
    fn allows_when_kubernetes_detected() {
        let config = SelfEvolutionConfig::default();
        let signals = SandboxSignals {
            kubernetes_service_host: Some("10.96.0.1".into()),
            ..Default::default()
        };
        let (effective, decision) = enforce_sandbox_boundary(&config, &signals);
        assert!(effective.auto_apply);
        assert!(effective.auto_deploy);
        assert!(matches!(decision, BoundaryDecision::Allowed(_)));
    }

    #[test]
    fn allows_when_sandbox_mode_declared() {
        let config = SelfEvolutionConfig {
            sandbox_mode: true,
            ..Default::default()
        };
        let (effective, decision) = enforce_sandbox_boundary(&config, &bare_signals());
        assert!(effective.auto_apply);
        assert!(matches!(decision, BoundaryDecision::Allowed(_)));
    }

    #[test]
    fn allows_when_force_autonomous() {
        let config = SelfEvolutionConfig {
            force_autonomous: true,
            ..Default::default()
        };
        let (effective, decision) = enforce_sandbox_boundary(&config, &bare_signals());
        assert!(effective.auto_apply);
        assert!(matches!(decision, BoundaryDecision::Allowed(_)));
    }

    #[test]
    fn allows_when_microvm_orchestrator() {
        let mut config = SelfEvolutionConfig::default();
        config.microvm.enabled = true;
        let (effective, decision) = enforce_sandbox_boundary(&config, &bare_signals());
        assert!(effective.auto_apply);
        assert!(effective.auto_deploy);
        match decision {
            BoundaryDecision::Allowed(reason) => assert!(reason.contains("microvm")),
            other => panic!("expected Allowed, got {other:?}"),
        }
    }

    #[test]
    fn no_downgrade_when_already_manual() {
        let config = SelfEvolutionConfig {
            auto_apply: false,
            auto_deploy: false,
            ..Default::default()
        };
        let (effective, decision) = enforce_sandbox_boundary(&config, &bare_signals());
        assert!(!effective.auto_apply);
        assert!(matches!(decision, BoundaryDecision::Allowed(_)));
    }
}
