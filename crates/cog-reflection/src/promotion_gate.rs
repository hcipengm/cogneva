//! 晋级分级策略引擎。
//!
//! change 在沙盒闯过验证关后，按触及的文件决定晋级通道：
//!
//! - 黑名单（依赖清单 / 密钥文件）→ 直接拒收，连沙盒都不让进；
//! - 全部落在 L0 配置路径 → 热更新通道（不碰二进制）；
//! - 触及 L2 核心路径（存储 / 调度 / 安全网关 / 凭证）→ 人工审批；
//! - 全部落在 `deploy/**` → 自动晋级（这一族由应用侧的可执行准入条件判，判不过的
//!   变更在应用那一步就被拒收，走不到这里）；
//! - 全部落在 L1 白名单且 diff 不超限 → 自动金丝雀晋级；
//! - 其余路径族 → 拒收，并点名是哪一族没有准入条件。
//!
//! 最后一条原来写的是「模糊地带一律转人工（宁严勿宽）」。`没想到` 不是风险
//! 等级：把没列过的路径判成「等人批」，得到的是一条**永远没有人来批**的待办，
//! 既不会落地也不会消失。所以这一格的结局是拒收——同一条路径族的准入条件由
//! 配置补上之后，它才会走进前两档。
//!
//! 纯函数无 IO，配置来自 [`crate::PromotionGateConfig`]。

#[cfg(test)]
mod feedback_verification_surface {
    use crate::criteria_face::{is_carrier_path, Tier, TierReason, Tiering};

    /// The face a verdict about a change is scored against.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum JudgmentFace {
        /// The checkout the change landed in. Usable only when the change did
        /// not move the criteria it is being judged by.
        ChangedCheckout,
        /// A pristine revision the change cannot have reached.
        PristineCheckout,
    }

    /// Where a tiering's verdict has to come from: a change that touches the
    /// criteria code cannot be judged by the surface it just rewrote.
    fn judgment_face(tiering: &Tiering) -> JudgmentFace {
        if tiering.touches_criteria_code {
            JudgmentFace::PristineCheckout
        } else {
            JudgmentFace::ChangedCheckout
        }
    }

    fn tiering_for(paths: &[&str]) -> Tiering {
        if paths.iter().copied().any(is_carrier_path) {
            Tiering {
                tier: Tier::RealGate,
                reasons: vec![TierReason::CriteriaCarrier],
                touches_criteria_code: true,
            }
        } else {
            Tiering {
                tier: Tier::Tests,
                reasons: vec![TierReason::Code],
                touches_criteria_code: false,
            }
        }
    }

    /// principle: feedback_verification_must_not_run_on_the_surface_under_repair
    ///
    /// 修 A 的验证若跑在 A 自己的判据面上就是自指——既可能被 A 的缺陷误伤，也可能
    /// 为验证给被检面注入病因。因此触及判据代码的变更，裁决只能取自该变更无法抵达
    /// 的独立面；把裁决路回被修改的判据面，必须失败关闭。
    #[test]
    fn a_change_that_touches_the_criteria_face_is_judged_off_that_face() {
        // The classifier file names itself as a carrier, so a change rewriting
        // it is exactly the self-verification case.
        let repaired = tiering_for(&["crates/cog-reflection/src/criteria_face.rs"]);
        assert!(repaired.touches_criteria_code);
        assert_eq!(repaired.tier, Tier::RealGate);
        assert_eq!(judgment_face(&repaired), JudgmentFace::PristineCheckout);
        // Fail closed: scoring the verdict against the repaired surface is
        // rejected rather than routed back at the changed checkout.
        assert_ne!(judgment_face(&repaired), JudgmentFace::ChangedCheckout);

        // The rule is about the criteria face, not every change: ordinary
        // source keeps its verdict in the checkout it changed.
        let ordinary = tiering_for(&["crates/cog-reflection/src/promoter.rs"]);
        assert!(!ordinary.touches_criteria_code);
        assert_eq!(judgment_face(&ordinary), JudgmentFace::ChangedCheckout);
    }
}

use crate::PromotionGateConfig;
use cog_core::PromotionGateKind;

/// 晋级门判定结果。
///
/// 每个变体都带 [`PromotionGateKind`]：分类的依据在判定的那一刻最清楚，等到
/// 台账里只剩一句理由文本就再也取不回来了。理由文本给人看，kind 给聚合用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateVerdict {
    /// 黑名单命中或影响面不可确认。拒绝进入沙盒执行管线。
    Reject {
        kind: PromotionGateKind,
        reason: String,
    },
    /// L0：仅配置 / prompt 变化，走热更新通道。
    AutoConfig,
    /// L1：白名单低风险代码，走自动金丝雀晋级。
    AutoRollout,
    /// L2：核心路径 / 超大 diff / 模糊地带，机器全绿后转人工审批。
    RequireApproval {
        kind: PromotionGateKind,
        reason: String,
    },
}

impl GateVerdict {
    pub fn is_auto(&self) -> bool {
        matches!(self, GateVerdict::AutoConfig | GateVerdict::AutoRollout)
    }

    /// 判定的类型化结果。
    pub fn kind(&self) -> PromotionGateKind {
        match self {
            GateVerdict::Reject { kind, .. } => *kind,
            GateVerdict::AutoConfig => PromotionGateKind::AutoConfig,
            GateVerdict::AutoRollout => PromotionGateKind::AutoRollout,
            GateVerdict::RequireApproval { kind, .. } => *kind,
        }
    }

    /// 判定理由：拒收与转人工带具体原因；自动通道没有可说的理由，返回 None。
    pub fn reason(&self) -> Option<&str> {
        match self {
            GateVerdict::Reject { reason, .. } | GateVerdict::RequireApproval { reason, .. } => {
                Some(reason)
            }
            GateVerdict::AutoConfig | GateVerdict::AutoRollout => None,
        }
    }
}

/// 变更行数的唯一计数器在 cog-core：生成侧（路由分档）、晋级门与落地侧必须
/// 用同一个读数，否则同一份 diff 会对一个读者超限、对另一个读者不超限，而下游
/// 分不出是哪一次读数放它过去的。
pub use cog_core::count_diff_lines;

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn has_forbidden_extension<'a>(path: &str, forbidden: impl Iterator<Item = &'a str>) -> bool {
    file_name(path)
        .rsplit_once('.')
        .map(|(_, ext)| forbidden.into_iter().any(|f| f.eq_ignore_ascii_case(ext)))
        .unwrap_or(false)
}

/// The names a change may not touch, whatever this policy says.
///
/// The floor is the contract's. `PROTECTED_FILE_NAMES` is the list that carries
/// the doc and the other readers -- the evaluator and the apply gate both refuse
/// a change naming one of them -- while a deployment's config is additions to
/// it. Config may therefore widen the floor and may not narrow it: a policy that
/// left a name out used to hand this gate a change the rest of the pipeline
/// refuses, which is the same fact held by two readers of different widths.
fn forbidden_names<'a>(policy: &'a PromotionGateConfig) -> impl Iterator<Item = &'a str> + 'a {
    cog_core::PROTECTED_FILE_NAMES
        .iter()
        .copied()
        .chain(policy.forbidden_names.iter().map(String::as_str))
}

/// The extensions a change may not touch, on the same terms as
/// [`forbidden_names`].
fn forbidden_extensions<'a>(policy: &'a PromotionGateConfig) -> impl Iterator<Item = &'a str> + 'a {
    cog_core::PROTECTED_FILE_EXTENSIONS
        .iter()
        .copied()
        .chain(policy.forbidden_extensions.iter().map(String::as_str))
}

fn matches_any(path: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|p| path.starts_with(p.as_str()))
}

/// `crates/<成员>/src/...`：工作区成员自己的源码。
///
/// 按**分量**判而不是按前缀：`crates/x/src` 与 `crates/x/src2` 是两回事，而
/// 前者是这一族的边界。测试与夹具不在 `src/` 下，也就自然不落进这一族——它们
/// 没有对应的准入条件，走的是拒收那一格。
fn is_member_src(path: &str) -> bool {
    let mut parts = path.split('/');
    parts.next() == Some("crates")
        && parts.next().is_some_and(|m| !m.is_empty())
        && parts.next() == Some("src")
}

/// `deploy/...`：部署拓扑这一族。
///
/// 按**分量**判，与流水线里那条准入条件的族边界逐字相同（`deploy/` 下不允许
/// 空段、`.`、`..`）：分类与准入对「这一笔算不算部署」必须给同一个答案，否则
/// 一笔 `deploy/../x` 会在准入那里被当成部署、在这里被当成别的族。
fn is_deploy_path(path: &str) -> bool {
    let path = path.replace('\\', "/");
    path.strip_prefix("deploy/").is_some_and(|rest| {
        !rest.is_empty()
            && rest
                .split('/')
                .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
    })
}

/// The directory components a change may not reach into, on the same terms as
/// [`forbidden_names`].
///
/// A third axis rather than a third list inside the second: a name is matched
/// against the last component alone, so an evaluation suite — which is a
/// directory — could not have been named there however the policy spelled it.
/// Like the other two, the contract's list is the floor and the policy only
/// adds.
fn forbidden_dir_segments<'a>(
    policy: &'a PromotionGateConfig,
) -> impl Iterator<Item = &'a str> + 'a {
    cog_core::PROTECTED_DIR_SEGMENTS
        .iter()
        .copied()
        .chain(policy.forbidden_dir_segments.iter().map(String::as_str))
}

/// 对 change 触及的文件清单做晋级分级。
///
/// 判定顺序（命中即返回）：
/// 1. 空清单 → Reject（无法确认影响面，fail-closed）；
/// 2. 任一文件命中黑名单文件名 / 扩展名 / 目录分量 → Reject；
/// 3. diff 行数超上限 → RequireApproval；
/// 4. 全部文件落在 L0 配置路径 → AutoConfig（配置路径是核心路径的
///    特例，必须先判；混入任何非配置文件会自然掉到后续判定）；
/// 5. 全部文件是 `deploy/**` → AutoRollout（这一族的准入条件在应用侧执行）；
/// 6. 任一文件命中 L2 核心路径 → RequireApproval（准入条件里缺的那一条写进理由）；
/// 7. 全部文件是成员 crate 的 `src/` → AutoRollout（编译 + 测试已在沙盒跑过）；
/// 8. 全部文件落在 L1 白名单 → AutoRollout；
/// 9. 其余 → Reject（这条路径族没有准入条件）。
pub fn classify(files: &[String], diff_lines: usize, policy: &PromotionGateConfig) -> GateVerdict {
    if files.is_empty() {
        return GateVerdict::Reject {
            kind: PromotionGateKind::RejectUnmeasured,
            reason: "change 未触及任何文件，无法确认影响面".into(),
        };
    }

    for f in files {
        let name = file_name(f);
        if forbidden_names(policy).any(|n| n == name) {
            return GateVerdict::Reject {
                kind: PromotionGateKind::RejectProtected,
                reason: format!("触及受保护文件 {name}（依赖清单/密钥文件禁止自动进化）"),
            };
        }
        if has_forbidden_extension(f, forbidden_extensions(policy)) {
            return GateVerdict::Reject {
                kind: PromotionGateKind::RejectProtected,
                reason: format!("触及受保护扩展名 {f}（密钥/证书材料禁止自动进化）"),
            };
        }
        if let Some(segment) =
            cog_core::path_protected_dir_segment(f, forbidden_dir_segments(policy))
        {
            return GateVerdict::Reject {
                kind: PromotionGateKind::RejectProtected,
                reason: format!(
                    "触及受保护目录 {segment}/（裁判冻结：判据套件禁止自动进化改动，{f}）"
                ),
            };
        }
    }

    if diff_lines > policy.max_diff_lines {
        return GateVerdict::RequireApproval {
            kind: PromotionGateKind::ApprovalDiffOverLimit,
            reason: format!("diff {diff_lines} 行超过上限 {} 行", policy.max_diff_lines),
        };
    }

    if files
        .iter()
        .all(|f| matches_any(f, &policy.config_prefixes))
    {
        return GateVerdict::AutoConfig;
    }

    // 部署拓扑这一族由**可执行条件**判，不由人判：条件在应用那一侧跑（渲染新鲜度、
    // parity、服务端 dry-run、回滚走过一遍），跑不过的变更在那里就被拒收、连这一格
    // 都到不了。所以到了这里还活着的部署变更，是条件全过的那些。
    //
    // 它此前落在下面的核心路径档，于是条件全过的变更仍要挂 24 小时等人，到点被
    // 判失败——那不是更安全，那是把「有条件可落」实现成「有通路但没人走」。条件
    // 已经由程序判完了，这一格再要一个人，等于同一件事判两遍、第二遍永远没人来。
    if files.iter().all(|f| is_deploy_path(f)) {
        return GateVerdict::AutoRollout;
    }

    if let Some(core) = files.iter().find(|f| matches_any(f, &policy.core_prefixes)) {
        // 核心路径的准入条件写在表里，但其中一项今天还不可执行：回滚路径没有
        // 在晋级链上被执行过一次的读数。缺条件的那一条要说出来是**哪一条**，
        // 否则这一格又变成一句「要人看」。审批有时间上限，到点按这句判据决断。
        return GateVerdict::RequireApproval {
            kind: PromotionGateKind::ApprovalCorePath,
            reason: format!(
                "触及核心路径 {core}：准入条件缺可回滚实证（回滚路径还没有在晋级链上被执行过一次）"
            ),
        };
    }

    // 成员 crate 的源码族：准入条件是编译 + 测试，而这两件事在晋级门见到这条
    // 变更之前就已经在沙盒里跑过了（沙盒 apply → test → build → switch 全过才
    // 会有晋级判定）。所以这一族不需要人工，也不需要在这里重跑一遍。
    if files.iter().all(|f| is_member_src(f)) {
        return GateVerdict::AutoRollout;
    }

    if files
        .iter()
        .all(|f| matches_any(f, &policy.whitelist_prefixes))
    {
        return GateVerdict::AutoRollout;
    }

    // 「没想到的路径」不再等于「要人批」。等人批的那条路没有下一个动作：一条
    // 永远没有人来批的待办既不会落地也不会消失，它只会在审批台上把真正的待办
    // 淹掉。这里拒收并点名缺条件的那一族——下一个动作是改配置，不是等人。
    let unclassified = files.first().map(String::as_str).unwrap_or_default();
    GateVerdict::Reject {
        kind: PromotionGateKind::RejectUnclassified,
        reason: format!(
            "触及没有准入条件的路径族 {unclassified}：不是等人批，是这一族还没有可执行条件——\
             把它加进白名单/配置前缀，或给这一族写一条准入条件"
        ),
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

    /// The contract's protected set is a floor the policy can only add to.
    ///
    /// Read with the policy's own lists emptied, because that is the shape that
    /// tells a floor apart from a transcription: the default config used to name
    /// four of the contract's nine, so `Dockerfile`, `Containerfile`,
    /// `docker-compose.yml`, `setup.sh` and `cogneva.json` were promotable here
    /// while the evaluator and the apply gate both refuse them.
    #[test]
    fn the_contract_floor_holds_when_the_policy_names_nothing() {
        let mut policy = PromotionGateConfig::default();
        policy.forbidden_names.clear();
        policy.forbidden_extensions.clear();
        policy.forbidden_dir_segments.clear();

        for name in cog_core::PROTECTED_FILE_NAMES {
            let v = classify(&files(&[name]), 5, &policy);
            assert!(
                matches!(v, GateVerdict::Reject { .. }),
                "受保护文件 {name} 在这个门里没有被拒收: {v:?}"
            );
        }
        for ext in cog_core::PROTECTED_FILE_EXTENSIONS {
            let path = format!("certs/x.{ext}");
            let v = classify(&files(&[&path]), 5, &policy);
            assert!(
                matches!(v, GateVerdict::Reject { .. }),
                "受保护扩展名 .{ext} 在这个门里没有被拒收: {v:?}"
            );
        }
        for segment in cog_core::PROTECTED_DIR_SEGMENTS {
            let path = format!("{segment}/suite.py");
            let v = classify(&files(&[&path]), 5, &policy);
            assert!(
                matches!(v, GateVerdict::Reject { .. }),
                "受保护目录 {segment}/ 在这个门里没有被拒收: {v:?}"
            );
        }
    }

    /// The other direction: a policy may still add a name of its own.
    #[test]
    fn a_policy_can_add_a_name_of_its_own() {
        let mut policy = PromotionGateConfig::default();
        policy.forbidden_names.push("secrets.yaml".into());
        policy.forbidden_extensions.push("p8".into());
        policy.forbidden_dir_segments.push("judge".into());

        let v = classify(&files(&["deploy/secrets.yaml"]), 5, &policy);
        assert!(matches!(v, GateVerdict::Reject { .. }), "{v:?}");
        let v = classify(&files(&["certs/client.p8"]), 5, &policy);
        assert!(matches!(v, GateVerdict::Reject { .. }), "{v:?}");
        let v = classify(&files(&["poche/judge/cases.py"]), 5, &policy);
        assert!(matches!(v, GateVerdict::Reject { .. }), "{v:?}");
    }

    /// 目录轴按**分量**比，不按前缀：项目自己的判据套件与源码同级，不在仓库根。
    #[test]
    fn a_protected_directory_matches_at_any_depth() {
        let policy = PromotionGateConfig::default();

        let v = classify(&files(&["examples/eval/run.py"]), 5, &policy);
        assert!(
            matches!(v, GateVerdict::Reject { .. }),
            "嵌套的 eval/ 没有被拒收: {v:?}"
        );
        // 分量相等是判据，不是子串：`eval_datasets` 与 `evaluation/` 都不该被
        // 这条轴碰到（前者是既有 carrier，后者是另一件事）。
        for path in ["eval_datasets/humaneval.jsonl", "docs/evaluation/notes.md"] {
            let v = classify(&files(&[path]), 5, &policy);
            // 这两条只考目录轴，不考归属：前者落在没有准入条件的路径族，后者
            // 落在白名单。它们共同的反面证据是**没有**被当成受保护目录拒收。
            assert!(
                !matches!(
                    v,
                    GateVerdict::Reject {
                        kind: PromotionGateKind::RejectProtected,
                        ..
                    }
                ),
                "{path} 被目录轴误拒: {v:?}"
            );
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

    /// 部署拓扑这一族不进人工审批：它的条件是在应用那一侧逐条跑的可执行命令，
    /// 跑不过的变更在那里就被拒收。条件全过还挂一个人，就是把「有条件可落」写成
    /// 「有通路但没人走」——挂到期限后按理由判失败，等于程序判过一遍再让人空判一遍。
    #[test]
    fn deploy_change_rolls_on_its_own_admission_conditions() {
        let v = classify(
            &files(&["deploy/k3s/evolution-deployment.yaml"]),
            10,
            &policy(),
        );
        assert_eq!(v, GateVerdict::AutoRollout);
    }

    /// 同时碰部署与别的族的一笔变更不进自动档：没有任何一族的准入条件整笔覆盖它，
    /// 拿部署族的条件替它背书，等于让一个 `crates/cog-core` 的改动搭着部署变更
    /// 的车过去。它仍落在核心路径那一档。
    #[test]
    fn a_mixed_change_that_touches_the_topology_still_waits_for_a_person() {
        let v = classify(
            &files(&["deploy/helm/x.yaml", "prompts/default.yaml"]),
            10,
            &policy(),
        );
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    /// 族边界按**分量**判，与准入那一侧同一个边界：`deploy/../x` 在准入那里不是
    /// 部署路径（那一条要求每段都干净），所以部署族的条件不会为它跑。分级这一侧
    /// 不能自作主张把它算成部署族的自动档——它拿不到那一族的背书，落回核心路径
    /// 那一档，由人看。两边的结论可以不同，但**都不能说它是部署族**。
    #[test]
    fn a_deploy_path_that_escapes_the_directory_is_not_this_family() {
        let v = classify(&files(&["deploy/../crates/x/src/lib.rs"]), 10, &policy());
        match v {
            GateVerdict::RequireApproval { kind, .. } => {
                assert_eq!(kind, PromotionGateKind::ApprovalCorePath)
            }
            other => panic!("expected RequireApproval, got {other:?}"),
        }
    }

    #[test]
    fn oversized_diff_requires_approval() {
        let v = classify(&files(&["docs/big.md"]), 501, &policy());
        match v {
            GateVerdict::RequireApproval { kind, reason } => {
                assert!(reason.contains("501"));
                assert_eq!(kind, PromotionGateKind::ApprovalDiffOverLimit);
            }
            other => panic!("expected RequireApproval, got {other:?}"),
        }
    }

    /// 没列过的路径族不再转人工：拒收，并点名是哪一族缺条件。
    #[test]
    fn unlisted_path_is_rejected_and_names_the_family() {
        let v = classify(&files(&["scripts/run.py"]), 10, &policy());
        match v {
            GateVerdict::Reject { kind, reason } => {
                assert_eq!(kind, PromotionGateKind::RejectUnclassified);
                assert!(reason.contains("scripts/run.py"), "{reason}");
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    /// 拒收那一格**有下一个动作**：给这一族一条条件（这里用白名单）就走进 L1。
    #[test]
    fn a_rejected_family_walks_the_auto_tier_once_it_has_a_condition() {
        let mut policy = PromotionGateConfig::default();
        policy.whitelist_prefixes.push("scripts/".into());
        let v = classify(&files(&["scripts/run.py"]), 10, &policy);
        assert_eq!(v, GateVerdict::AutoRollout);
    }

    /// 成员 crate 的源码族有可执行条件（编译 + 测试已在沙盒跑过），不需要人。
    #[test]
    fn a_member_crate_src_file_is_auto_rollout() {
        let v = classify(&files(&["crates/cog-memory/src/causal.rs"]), 10, &policy());
        assert_eq!(v, GateVerdict::AutoRollout);
    }

    /// 但 `src/` 之外的东西不沾这一族的光：分量相等才是边界。
    #[test]
    fn a_member_crate_test_file_is_not_in_the_src_family() {
        for path in [
            "crates/cog-memory/tests/causal.rs",
            "crates/cog-memory/src2/x.rs",
        ] {
            let v = classify(&files(&[path]), 10, &policy());
            assert!(matches!(v, GateVerdict::Reject { .. }), "{path}: {v:?}");
        }
    }

    #[test]
    fn config_path_exceeding_diff_limit_requires_approval() {
        let v = classify(&files(&["prompts/default.yaml"]), 9999, &policy());
        assert!(matches!(v, GateVerdict::RequireApproval { .. }), "{v:?}");
    }

    #[test]
    fn count_diff_lines_ignores_headers() {
        let change =
            "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n-old\n+new\n context\n";
        assert_eq!(count_diff_lines(change), 2);
    }

    #[test]
    fn is_auto_only_for_l0_l1() {
        assert!(GateVerdict::AutoConfig.is_auto());
        assert!(GateVerdict::AutoRollout.is_auto());
        assert!(!GateVerdict::Reject {
            kind: PromotionGateKind::RejectUnmeasured,
            reason: String::new(),
        }
        .is_auto());
        assert!(!GateVerdict::RequireApproval {
            kind: PromotionGateKind::ApprovalCorePath,
            reason: String::new(),
        }
        .is_auto());
    }
}
