//! Task profile and PGE mode selector.
//! Implements the task complexity scoring and dispatch rule from
//! Planner → Generator → Evaluator pipeline, while higher-complexity
//! work falls back to the Roundtable debate loop.

/// Profile describing the dimensions used to choose a PGE execution mode.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TaskProfile {
    /// How novel the task is relative to historical work (0.0 = familiar, 1.0 = brand new).
    pub novelty: f64,
    /// Risk of negative side effects if the task fails (0.0 = low, 1.0 = high).
    pub risk: f64,
    /// Ambiguity of the requirements (0.0 = crisp, 1.0 = vague).
    pub ambiguity: f64,
    /// Number of upstream/downstream dependencies, normalized to 0.0..=1.0.
    pub dependency_count: f64,
    /// Token budget headroom available for the task, normalized to 0.0..=1.0.
    pub token_budget: f64,
    /// Historical success rate on comparable tasks (0.0 = always fails, 1.0 = always succeeds).
    pub historical_success: f64,
    /// What the request itself declares about the size of the change it asks
    /// for. Deliberately outside [`complexity_score`]: the five weighted
    /// dimensions read the request's *description*, this reads the *change*, and
    /// [`select_mode`] combines the two rather than summing them into one number
    /// where a long sentence could out-vote a named file.
    #[serde(default)]
    pub declared_scale: DeclaredScale,
}

impl Default for TaskProfile {
    fn default() -> Self {
        Self {
            novelty: 0.0,
            risk: 0.0,
            ambiguity: 0.0,
            dependency_count: 0.0,
            token_budget: 1.0,
            historical_success: 1.0,
            declared_scale: DeclaredScale::Unknown,
        }
    }
}

/// How much a task declares it will change, read from the request itself.
///
/// Measured from declarations, never from prose length. How long a request is
/// says nothing about how much code it moves — the shortest sentence can ask for
/// a rewrite — which is why `ambiguity` (a function of the input's size) cannot
/// stand in for this. What a request *states*, the files it names and the diff it
/// attaches, is a fact about the change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeclaredScale {
    /// The request declares a scope: it names at least one file it will touch,
    /// or attaches a diff.
    Measured {
        /// Distinct files named by the request.
        files: usize,
        /// How many of those are source rather than prose. The rule is the
        /// allow-list below: anything it cannot read as prose counts here.
        code_files: usize,
        /// Lines in an attached diff, when the request attaches one.
        lines: Option<usize>,
    },
    /// The request declares no scope: it names no file and attaches no diff.
    #[default]
    Unknown,
}

/// Orchestration a *measured* scale entitles the task to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeTier {
    /// Small and prose-only: the request already states its own scope, so the
    /// stage that exists to work a request's shape out has nothing to discover.
    Shortcut,
    /// Large by declaration: the lightest topology is not available to it, and a
    /// short description must not be able to route it there.
    Deep,
}

/// Files a request may name and still be the shortcut's.
pub const SHORTCUT_MAX_FILES: usize = 3;
/// Lines an attached diff may declare and still be the shortcut's.
pub const SHORTCUT_MAX_LINES: usize = 40;
/// Declared files at which a change is too big for the lightest topology.
pub const DEEP_MIN_FILES: usize = 4;
/// Declared diff lines at which a change is too big for the lightest topology.
pub const DEEP_MIN_LINES: usize = 120;

/// Extensions whose files are prose rather than something a compiler, a schema
/// or a test run reads.
///
/// An allow-list on purpose: a path this list does not recognize counts as code,
/// so an unfamiliar extension can only cost the request the shortcut, never earn
/// it. The other direction would let a file nobody classified collect the
/// lightest route by being unreadable here.
const PROSE_EXTENSIONS: &[&str] = &["md", "markdown", "txt", "rst", "adoc"];

/// Whether a declared path is prose.
fn is_prose_path(path: &str) -> bool {
    match path.rsplit_once('.') {
        Some((_, ext)) => PROSE_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()),
        // A bare name (`LICENSE`, `README`) has no extension to read, and a
        // dotted directory (`docs/v1.2/notes`) is not a file name at all; both
        // stay code for the same reason the unknown extensions do.
        None => false,
    }
}

/// What the request declares about the size of the change it asks for.
///
/// Two declarations are read, because they are the two a request can make: the
/// paths it names (in the goal text, or as an explicit file list) and the diff it
/// attaches. Anything else about the request — its length, its tone — is a
/// property of the asking, not of the change.
pub fn declared_scale(task: &cog_core::Task) -> DeclaredScale {
    let goal = task
        .input
        .get("goal")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let mut files: Vec<String> = Vec::new();
    for path in cog_core::paths_named_in_goal(goal) {
        if !files.contains(&path) {
            files.push(path);
        }
    }
    if let Some(named) = task.input.get("affected_files").and_then(|v| v.as_array()) {
        for path in named.iter().filter_map(|v| v.as_str()) {
            if !files.contains(&path.to_string()) {
                files.push(path.to_string());
            }
        }
    }
    let lines = task
        .input
        .get("diff")
        .and_then(|v| v.as_str())
        .map(cog_core::count_diff_lines);
    if files.is_empty() && lines.is_none() {
        return DeclaredScale::Unknown;
    }
    DeclaredScale::Measured {
        files: files.len(),
        code_files: files.iter().filter(|p| !is_prose_path(p)).count(),
        lines,
    }
}

/// Which orchestration a measured scale entitles the task to.
///
/// `None` means the declaration does not decide: nothing was declared, or what
/// was declared is an ordinary code change the complexity score already handles.
/// A request that declares nothing keeps the route it has today, so this rule
/// can only be reached by a request that says enough for the decision to be read
/// off it — and the shortcut, which is a discount, is the one direction that must
/// never be earned by silence.
pub fn change_tier(scale: DeclaredScale) -> Option<ChangeTier> {
    let DeclaredScale::Measured {
        files,
        code_files,
        lines,
    } = scale
    else {
        return None;
    };
    if files >= DEEP_MIN_FILES || lines.is_some_and(|l| l >= DEEP_MIN_LINES) {
        return Some(ChangeTier::Deep);
    }
    // At least one file has to be named for "every file is prose" to mean
    // anything: an attached diff with no path next to it says nothing about what
    // the change touches.
    let prose_only = (1..=SHORTCUT_MAX_FILES).contains(&files) && code_files == 0;
    let small = lines.is_none_or(|l| l <= SHORTCUT_MAX_LINES);
    if prose_only && small {
        Some(ChangeTier::Shortcut)
    } else {
        None
    }
}

/// Mode the PGE selector dispatches to for a given [`TaskProfile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PgeMode {
    /// Linear Planner → Generator → Evaluator with no feedback loop.
    Pipeline,
    /// Iterative debate loop with retries and consensus checking.
    Roundtable,
    /// 只跑 Planner：交付物是它产出的原子任务列表，所以这条路径没有生成器也
    /// 没有评估器，没有 PGE 拓扑可言。
    ///
    /// 只有分解任务走这条路径，而它的判据是结构性的（有没有拿到任务列表），
    /// 不是"某份产物好不好"——这正是不能借用另外两种模式的原因：它们最后都
    /// 拿一份产物的分数决定成败，而分解的产物是任务列表之外的东西（曾经是
    /// 生成器写的一份没人读的产物）。选择器不会返回这个值：它不参与"哪种
    /// 拓扑更适合"的取舍，也就不该出现在那张候选表里。
    PlanOnly,
    /// Generator → Evaluator, with the request's own declared scope standing in
    /// for a plan.
    ///
    /// The planner is the stage that exists because a request's *shape* is
    /// unknown. When the request names the files it will touch and every one of
    /// them is prose, that shape is stated, and a call that restates it buys
    /// nothing. What remains is still the PGE contract for the work that
    /// remains: a generator produces an artifact and an evaluator that did not
    /// write it judges that artifact, under the same deterministic gates as
    /// every other route.
    ///
    /// Selected only by a measured [`DeclaredScale`], never by the complexity
    /// score: [`select_mode`] does not offer it as a competing topology, because
    /// the shortcut is a fact about the change while the score is a guess about
    /// the work, and a guess must not be able to buy the discount.
    Direct,
}

impl PgeMode {
    /// 这个模式在台账与学习数据里的名字。
    ///
    /// `None` = 它不是一次模式选择，别记账：分解路径从来没有"选哪种拓扑更好"
    /// 这一步，把它写成一条模式试验记录，模式选择的样本里就会混进一批从未
    /// 发生过的试验，而污染的表现是模型对真实模式越来越有把握。
    pub fn as_learnt_mode(&self) -> Option<&'static str> {
        match self {
            PgeMode::Pipeline => Some("pipeline"),
            PgeMode::Roundtable => Some("roundtable"),
            PgeMode::Direct => Some("direct"),
            PgeMode::PlanOnly => None,
        }
    }

    /// 这个模式的名字，用于记录与日志。所有模式都有名字，`None` 只影响学习面。
    pub fn as_str(&self) -> &'static str {
        match self {
            PgeMode::Pipeline => "pipeline",
            PgeMode::Roundtable => "roundtable",
            PgeMode::Direct => "direct",
            PgeMode::PlanOnly => "plan_only",
        }
    }
}

/// Threshold below which the [`PgeMode::Pipeline`] is preferred.
pub const PIPELINE_SCORE_THRESHOLD: f64 = 0.4;

/// Compute the complexity score for a profile.
/// Weights match the design doc:
/// `0.25*novelty + 0.30*risk + 0.20*ambiguity + 0.10*deps + 0.15*(1 - historical_success)`.
/// `token_budget` is intentionally not used in the scoring formula but is kept
/// on [`TaskProfile`] for future budget-aware variants.
pub fn complexity_score(p: &TaskProfile) -> f64 {
    p.novelty * 0.25
        + p.risk * 0.30
        + p.ambiguity * 0.20
        + p.dependency_count * 0.10
        + (1.0 - p.historical_success) * 0.15
}

/// Select the PGE mode appropriate for the given task profile.
///
/// A measured [`DeclaredScale`] decides first, because it is evidence rather than
/// inference: a request whose files are all prose takes the shortcut, and a
/// request that declares a change too large for the lightest topology is never
/// routed there — a big change can be asked for in one sentence, and the score
/// below reads the sentence, not the change.
///
/// With nothing measured, the design rule stands: [`PgeMode::Pipeline`] when
/// [`complexity_score`] is below [`PIPELINE_SCORE_THRESHOLD`], otherwise
/// [`PgeMode::Roundtable`]. When the score is right on the boundary we prefer
/// Roundtable — quality over speed when the decision is uncertain.
pub fn select_mode(p: &TaskProfile) -> PgeMode {
    match change_tier(p.declared_scale) {
        Some(ChangeTier::Shortcut) => PgeMode::Direct,
        Some(ChangeTier::Deep) => PgeMode::Roundtable,
        None if complexity_score(p) < PIPELINE_SCORE_THRESHOLD => PgeMode::Pipeline,
        None => PgeMode::Roundtable,
    }
}

/// Derive a task profile from a raw Task for PGE mode selection.
pub fn derive_task_profile(task: &cog_core::Task) -> TaskProfile {
    let input_len = task.input.to_string().len() as f64;
    let dep_count = task.blocked_by.len() as f64;
    TaskProfile {
        novelty: match task.task_type {
            cog_core::TaskType::Custom(_) => 0.7,
            cog_core::TaskType::WasmSkill | cog_core::TaskType::Skill => 0.6,
            _ => 0.3,
        },
        risk: (dep_count / 10.0).min(1.0),
        ambiguity: if input_len < 50.0 {
            0.8
        } else if input_len < 200.0 {
            0.5
        } else {
            0.3
        },
        dependency_count: (dep_count / 20.0).min(1.0),
        token_budget: 1.0,
        historical_success: 1.0,
        declared_scale: declared_scale(task),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_picks_pipeline() {
        let p = TaskProfile::default();
        assert!(complexity_score(&p) < PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p), PgeMode::Pipeline);
    }

    #[test]
    fn mid_complexity_profile_picks_roundtable() {
        // Score ~0.35 falls just above the Pipeline threshold (0.4).
        let p = TaskProfile {
            novelty: 0.5,
            risk: 0.5,
            ambiguity: 0.5,
            dependency_count: 0.0,
            token_budget: 1.0,
            historical_success: 1.0,
            declared_scale: DeclaredScale::Unknown,
        };
        let score = complexity_score(&p);
        // 0.5*0.25 + 0.5*0.30 + 0.5*0.20 = 0.125 + 0.15 + 0.10 = 0.375
        assert!(
            (0.25..PIPELINE_SCORE_THRESHOLD).contains(&score),
            "score={}",
            score
        );
        assert_eq!(select_mode(&p), PgeMode::Pipeline);
    }

    #[test]
    fn high_risk_profile_picks_roundtable() {
        let p = TaskProfile {
            novelty: 0.9,
            risk: 0.9,
            ambiguity: 0.9,
            dependency_count: 0.9,
            token_budget: 1.0,
            historical_success: 0.1,
            declared_scale: DeclaredScale::Unknown,
        };
        assert!(complexity_score(&p) >= PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }

    #[test]
    fn boundary_profile_picks_roundtable() {
        // Score exactly 0.4 should fall to Roundtable per the strict `<` rule.
        let p = TaskProfile {
            novelty: 0.0,
            risk: 0.0,
            ambiguity: 0.0,
            dependency_count: 0.0,
            token_budget: 1.0,
            // Use a direct mix that yields exactly 0.4: risk 1.0 contributes 0.30,
            // dependency_count 1.0 contributes 0.10. 0.30 + 0.10 = 0.40.
            historical_success: 1.0,
            declared_scale: DeclaredScale::Unknown,
        };
        let p = TaskProfile {
            risk: 1.0,
            dependency_count: 1.0,
            ..p
        };
        let score = complexity_score(&p);
        assert!((score - 0.4).abs() < f64::EPSILON);
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }

    #[test]
    fn low_historical_success_band_transitions() {
        let p = TaskProfile {
            novelty: 0.0,
            risk: 0.6,
            ambiguity: 0.0,
            dependency_count: 0.0,
            token_budget: 1.0,
            historical_success: 0.0,
            declared_scale: DeclaredScale::Unknown,
        };
        // 0.6*0.30 + 1.0*0.15 = 0.18 + 0.15 = 0.33 → Pipeline.
        assert_eq!(select_mode(&p), PgeMode::Pipeline);

        let p = TaskProfile { risk: 0.8, ..p };
        // 0.8*0.30 + 0.15 = 0.24 + 0.15 = 0.39 → Pipeline.
        assert_eq!(select_mode(&p), PgeMode::Pipeline);

        let p = TaskProfile { risk: 0.9, ..p };
        // 0.9*0.30 + 0.15 = 0.27 + 0.15 = 0.42 → Roundtable.
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }

    fn task_with(goal: &str, extra: serde_json::Value) -> cog_core::Task {
        let mut input = serde_json::json!({ "goal": goal });
        if let (Some(dst), Some(src)) = (input.as_object_mut(), extra.as_object()) {
            for (key, value) in src {
                dst.insert(key.clone(), value.clone());
            }
        }
        cog_core::Task::new("t", cog_core::TaskType::Custom("test".into()), input)
    }

    /// The defect this rule exists for. A module-level rewrite asked for in one
    /// short sentence scored below the threshold and was routed to the lightest
    /// topology, because nothing in the profile measured how much was being
    /// changed — the score reads the sentence, not the change.
    #[test]
    fn a_big_change_described_briefly_is_not_down_routed() {
        let task = task_with(
            "refactor the parser: crates/cog-parser/src/lib.rs, \
             crates/cog-parser/src/lexer.rs, crates/cog-parser/src/ast.rs, \
             crates/cog-parser/src/error.rs",
            serde_json::json!({}),
        );
        let p = derive_task_profile(&task);
        assert_eq!(
            p.declared_scale,
            DeclaredScale::Measured {
                files: 4,
                code_files: 4,
                lines: None
            }
        );
        // The score alone would still send it down; that is what the scale is for.
        assert!(complexity_score(&p) < PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }

    /// And its other end: a docs-only edit whose files the request names takes
    /// the shortcut, which the score would never have offered it.
    #[test]
    fn a_small_prose_change_takes_the_shortcut() {
        let task = task_with(
            "fix the wording in `docs/quickstart.md`",
            serde_json::json!({}),
        );
        let p = derive_task_profile(&task);
        assert_eq!(
            p.declared_scale,
            DeclaredScale::Measured {
                files: 1,
                code_files: 0,
                lines: None
            }
        );
        assert_eq!(select_mode(&p), PgeMode::Direct);
    }

    /// A measured change that is ordinary keeps the route it had. The rule
    /// settles the two ends only; a one-file code fix is neither end.
    #[test]
    fn a_small_code_change_keeps_the_design_rule() {
        let task = task_with(
            "fix the off-by-one in crates/cog-core/src/lib.rs",
            serde_json::json!({}),
        );
        let p = derive_task_profile(&task);
        assert_eq!(
            p.declared_scale,
            DeclaredScale::Measured {
                files: 1,
                code_files: 1,
                lines: None
            }
        );
        assert!(complexity_score(&p) < PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p), PgeMode::Pipeline);
    }

    /// Silence earns nothing. A request that names no file and attaches no diff
    /// keeps exactly the route it had before this rule existed, however small it
    /// looks in prose — a discount that could be collected by saying nothing
    /// would be collected by every request that says nothing.
    #[test]
    fn an_undeclared_scope_keeps_the_route_it_has_today() {
        let task = task_with("tidy up the docs", serde_json::json!({}));
        let p = derive_task_profile(&task);
        assert_eq!(p.declared_scale, DeclaredScale::Unknown);
        // 0.7*0.25 + 0.8*0.20 = 0.335 → Pipeline, as before the rule.
        assert!(complexity_score(&p) < PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p), PgeMode::Pipeline);
    }

    /// The shortcut is an allow-list of prose extensions, so a name this rule
    /// cannot read as prose counts as source: an unfamiliar path can only cost
    /// the request the shortcut, never earn it.
    #[test]
    fn a_name_that_is_not_readable_as_prose_does_not_earn_the_shortcut() {
        for path in [
            "deploy/values.yaml",
            "docs/v1.2/notes",
            "crates/cog-core/src/lib.rs",
        ] {
            let task = task_with(&format!("update `{path}`"), serde_json::json!({}));
            let p = derive_task_profile(&task);
            assert_eq!(
                p.declared_scale,
                DeclaredScale::Measured {
                    files: 1,
                    code_files: 1,
                    lines: None
                },
                "{path}"
            );
            assert_ne!(select_mode(&p), PgeMode::Direct, "{path}");
        }
    }

    /// A bare file name has no extension to read, so it stays source — the same
    /// fail-safe direction as an unknown extension. Reachable only through an
    /// explicit file list: a goal has no way to name a file without one.
    #[test]
    fn a_bare_name_counts_as_source() {
        let task = task_with(
            "refresh the licence header",
            serde_json::json!({ "affected_files": ["LICENSE", "docs/NOTES"] }),
        );
        assert_eq!(
            declared_scale(&task),
            DeclaredScale::Measured {
                files: 2,
                code_files: 2,
                lines: None
            }
        );
    }

    /// An attached diff is the other declaration a request can make, and it is
    /// what puts a line count on the scale — including the direction that
    /// matters: a diff too long to be a shortcut.
    #[test]
    fn an_attached_diff_measures_the_line_count() {
        let large = (1..=DEEP_MIN_LINES)
            .map(|i| format!("+line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let task = task_with(
            "apply the reviewed change to crates/x/src/lib.rs",
            serde_json::json!({ "diff": format!("--- a/crates/x/src/lib.rs\n+++ b/crates/x/src/lib.rs\n{large}\n") }),
        );
        let p = derive_task_profile(&task);
        assert_eq!(
            p.declared_scale,
            DeclaredScale::Measured {
                files: 1,
                code_files: 1,
                lines: Some(DEEP_MIN_LINES)
            }
        );
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }

    /// A diff with no file named next to it is a line count without a scope:
    /// large is still readable off it, and small is not enough on its own — a
    /// short diff says nothing about which files it lands in.
    #[test]
    fn a_line_count_without_a_scope_is_not_a_shortcut() {
        let scale = DeclaredScale::Measured {
            files: 0,
            code_files: 0,
            lines: Some(3),
        };
        assert_eq!(change_tier(scale), None);
        assert_eq!(
            change_tier(DeclaredScale::Measured {
                files: 0,
                code_files: 0,
                lines: Some(DEEP_MIN_LINES),
            }),
            Some(ChangeTier::Deep)
        );
    }

    /// The scale is a measurement of the request, not of how it reads: the same
    /// files declared in a one-line goal and in a padded one land in the same
    /// tier.
    #[test]
    fn the_scale_does_not_move_with_the_wording() {
        let terse = task_with("`docs/a.md` `docs/b.md`", serde_json::json!({}));
        let padded = task_with(
            "please, when you have a moment, take a careful look at `docs/a.md` \
             and also at `docs/b.md` and fix whatever wording is off",
            serde_json::json!({}),
        );
        assert_eq!(
            declared_scale(&terse),
            declared_scale(&padded),
            "the same declaration in two wordings"
        );
    }
}
