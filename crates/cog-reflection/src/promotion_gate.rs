//! 晋级分级策略引擎（docs/2026-08-06_真自治全进化无人值守方案.md）。
//!
//! patch 在沙盒闯过验证关后，按触及的文件决定晋级通道：
//!
//! - 黑名单（依赖清单 / 密钥文件）→ 直接拒收，连沙盒都不让进；
//! - 全部落在 L0 配置路径 → 热更新通道（不碰二进制）；
//! - 触及 L2 核心路径（存储 / 调度 / 安全网关 / 凭证 / 部署）→ 人工审批；
//! - 全部落在 L1 白名单且 diff 不超限 → 自动金丝雀晋级；
//! - 其余模糊地带一律按 L2 转人工（宁严勿宽，fail-closed）。
//!
//! 纯函数无 IO，配置来自 [`crate::PromotionGateConfig`]。

use crate::PromotionGateConfig;

/// 晋级门判定结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateVerdict {
    /// 黑名单命中：依赖清单、密钥材料。拒绝进入沙盒执行管线。
    Reject { reason: String },
    /// L0：仅配置 / prompt 变化，走热更新通道。
    AutoConfig,
    /// L1：白名单低风险代码，走自动金丝雀晋级。
    AutoRollout,
    /// L2：核心路径 / 超大 diff / 模糊地带，机器全绿后转人工审批。
    RequireApproval { reason: String },
}

impl GateVerdict {
    pub fn is_auto(&self) -> bool {
        matches!(self, GateVerdict::AutoConfig | GateVerdict::AutoRollout)
    }
}

/// 统计 unified diff 的有效变更行数（+ / - 行，排除 +++ / --- 文件头）。
pub fn count_diff_lines(content: &str) -> usize {
    content
        .lines()
        .filter(|l| {
            (l.starts_with('+') && !l.starts_with("+++"))
                || (l.starts_with('-') && !l.starts_with("---"))
        })
        .count()
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn has_forbidden_extension(path: &str, forbidden: &[String]) -> bool {
    file_name(path)
        .rsplit_once('.')
        .map(|(_, ext)| forbidden.iter().any(|f| f.eq_ignore_ascii_case(ext)))
        .unwrap_or(false)
}

fn matches_any(path: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|p| path.starts_with(p.as_str()))
}

/// 对 patch 触及的文件清单做晋级分级。
///
/// 判定顺序（命中即返回）：
/// 1. 空清单 → Reject（无法确认影响面，fail-closed）；
/// 2. 任一文件命中黑名单文件名 / 扩展名 → Reject；
/// 3. diff 行数超上限 → RequireApproval；
/// 4. 全部文件落在 L0 配置路径 → AutoConfig（配置路径是核心路径的
///    特例，必须先判；混入任何非配置文件会自然掉到后续判定）；
/// 5. 任一文件命中 L2 核心路径 → RequireApproval；
/// 6. 全部文件落在 L1 白名单 → AutoRollout；
/// 7. 其余 → RequireApproval（模糊从严）。
pub fn classify(files: &[String], diff_lines: usize, policy: &PromotionGateConfig) -> GateVerdict {
    if files.is_empty() {
        return GateVerdict::Reject {
            reason: "patch 未触及任何文件，无法确认影响面".into(),
        };
    }

    for f in files {
        let name = file_name(f);
        if policy.forbidden_names.iter().any(|n| n == name) {
            return GateVerdict::Reject {
                reason: format!("触及受保护文件 {name}（依赖清单/密钥文件禁止自动进化）"),
            };
        }
        if has_forbidden_extension(f, &policy.forbidden_extensions) {
            return GateVerdict::Reject {
                reason: format!("触及受保护扩展名 {f}（密钥/证书材料禁止自动进化）"),
            };
        }
    }

    if diff_lines > policy.max_diff_lines {
        return GateVerdict::RequireApproval {
            reason: format!("diff {diff_lines} 行超过上限 {} 行", policy.max_diff_lines),
        };
    }

    if files
        .iter()
        .all(|f| matches_any(f, &policy.config_prefixes))
    {
        return GateVerdict::AutoConfig;
    }

    if let Some(core) = files.iter().find(|f| matches_any(f, &policy.core_prefixes)) {
        return GateVerdict::RequireApproval {
            reason: format!("触及核心路径 {core}"),
        };
    }

    if files
        .iter()
        .all(|f| matches_any(f, &policy.whitelist_prefixes))
    {
        return GateVerdict::AutoRollout;
    }

    GateVerdict::RequireApproval {
        reason: "触及未列入白名单的路径，模糊地带从严转人工".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> PromotionGateConfig {
        PromotionGateConfig::default()
    }

    fn files(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn empty_file_list_rejected() {
        assert!(matches!(
            classify(&[], 10, &policy()),
            GateVerdict::Reject { .. }
        ));
    }

    #[test]
    fn cargo_toml_rejected() {
        let v = classify(&files(&["crates/cog-agent/Cargo.toml"]), 5, &policy());
        assert!(matches!(v, GateVerdict::Reject { .. }), "{v:?}");
    }

    #[test]
    fn cargo_lock_rejected_even_with_safe_files() {
        let v = classify(&files(&["docs/readme.md", "Cargo.lock"]), 5, &policy());
        assert!(matches!(v, GateVerdict::Reject { .. }), "{v:?}");
    }

    #[test]
    fn secret_files_rejected() {
        for f in [
            ".env",
            ".envrc",
            "certs/tls.pem",
            "keys/id.key",
            "x.crt",
            "y.p12",
        ] {
            let v = classify(&files(&[f]), 5, &policy());
            assert!(matches!(v, GateVerdict::Reject { .. }), "{f}: {v:?}");
        }
    }

    #[test]
    fn prompts_only_is_auto_config() {
        let v = classify(&files(&["prompts/system_prompts.yaml"]), 20, &policy());
        assert_eq!(v, GateVerdict::AutoConfig);
    }

    #[test]
    fn configmap_yaml_is_auto_config() {
        let v = classify(
            &files(&["deploy/k3s/cogneva-json-configmap.yaml"]),
            20,
            &policy(),
        );
        assert_eq!(v, GateVerdict::AutoConfig);
    }

    #[test]
    fn config_plus_prompts_is_auto_config() {
        let v = classify(
            &files(&[
                "prompts/default.yaml",
                "deploy/k3s/cogneva-json-configmap.yaml",
            ]),
            20,
            &policy(),
        );
        assert_eq!(v, GateVerdict::AutoConfig);
    }

    #[test]
    fn whitelist_tool_impl_is_auto_rollout() {
        let v = classify(&files(&["crates/cog-agent/src/tools.rs"]), 100, &policy());
        assert_eq!(v, GateVerdict::AutoRollout);
    }

    #[test]
    fn whitelist_web_and_docs_is_auto_rollout() {
        let v = classify(
            &files(&["web/apps/ui/src/App.tsx", "docs/note.md"]),
            100,
            &policy(),
        );
        assert_eq!(v, GateVerdict::AutoRollout);
    }

    #[test]
    fn core_storage_requires_approval() {
        let v = classify(
            &files(&["crates/cog-storage/src/postgres/state_backend.rs"]),
            10,
            &policy(),
        );
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    #[test]
    fn core_path_wins_over_whitelist() {
        let v = classify(
            &files(&[
                "crates/cog-agent/src/tools.rs",
                "crates/cog-orchestrator/src/dag_executor/mod.rs",
            ]),
            10,
            &policy(),
        );
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    #[test]
    fn gateway_auth_requires_approval() {
        let v = classify(
            &files(&["crates/cog-gateway/src/auth/jwt.rs"]),
            10,
            &policy(),
        );
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    #[test]
    fn deploy_dir_requires_approval_except_configmap() {
        let v = classify(
            &files(&["deploy/k3s/evolution-deployment.yaml"]),
            10,
            &policy(),
        );
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    #[test]
    fn oversized_diff_requires_approval() {
        let v = classify(&files(&["docs/big.md"]), 501, &policy());
        match v {
            GateVerdict::RequireApproval { reason } => assert!(reason.contains("501")),
            other => panic!("expected RequireApproval, got {other:?}"),
        }
    }

    #[test]
    fn unlisted_path_fails_closed() {
        let v = classify(&files(&["crates/cog-memory/src/causal.rs"]), 10, &policy());
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    #[test]
    fn config_path_exceeding_diff_limit_requires_approval() {
        let v = classify(&files(&["prompts/default.yaml"]), 9999, &policy());
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    #[test]
    fn count_diff_lines_ignores_headers() {
        let patch = "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n-old\n+new\n context\n";
        assert_eq!(count_diff_lines(patch), 2);
    }

    #[test]
    fn is_auto_only_for_l0_l1() {
        assert!(GateVerdict::AutoConfig.is_auto());
        assert!(GateVerdict::AutoRollout.is_auto());
        assert!(!GateVerdict::Reject {
            reason: String::new()
        }
        .is_auto());
        assert!(!GateVerdict::RequireApproval {
            reason: String::new()
        }
        .is_auto());
    }
}
