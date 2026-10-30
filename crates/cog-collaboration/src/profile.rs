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
    /// What the request itself declares about the size of the change it asks
    /// for. Deliberately outside [`complexity_score`]: the five weighted
    /// dimensions read the request's *description*, this reads the *change*, and
    /// [`select_mode`] combines the two rather than summing them into one number
    /// where a long sentence could out-vote a named file.
    #[serde(default)]
    pub declared_scale: DeclaredScale,
    /// Which of the request's declarations produced the scale above. Travels
    /// with it so the count and the provenance are never read off different
    /// readings of the same request.
    #[serde(default)]
    pub declaration_inputs: DeclarationInputs,
}

impl Default for TaskProfile {
    fn default() -> Self {
        Self {
            novelty: 0.0,
            risk: 0.0,
            ambiguity: 0.0,
            dependency_count: 0.0,
            token_budget: 1.0,
            declared_scale: DeclaredScale::Unknown,
            declaration_inputs: DeclarationInputs::empty(),
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
// cap: bound — 一个请求可以点出这么多文件，并且仍然算轻量捷径的
pub const SHORTCUT_MAX_FILES: usize = 3;
/// Lines an attached diff may declare and still be the shortcut's.
// cap: bound — 附带的 diff 可以自报这么多行，并且仍然算轻量捷径的
pub const SHORTCUT_MAX_LINES: usize = 40;
/// Declared files at which a change is too big for the lightest topology.
// cap: bound — 到这个声明文件数，变更就重出了最轻那套拓扑的份
pub const DEEP_MIN_FILES: usize = 4;
/// Declared diff lines at which a change is too big for the lightest topology.
// cap: bound — 到这个声明 diff 行数，变更就重出了最轻那套拓扑的份
pub const DEEP_MIN_LINES: usize = 120;

/// One of the request's own declarations the scale is read from.
///
/// The tiering reads three things and nothing else. Naming them is what makes
/// "this input has no producer in this deployment" a reading instead of a claim
/// only a source dive can settle: an input nothing supplies leaves the tier
/// cells that input alone could move sitting at zero — which is also exactly
/// what a quiet day looks like.
///
/// No serde form: the value is an index into a bitmask and never travels. Its
/// only outward word is the metric label [`Self::as_str`] produces, and a
/// derived spelling would have been a second one nobody compares with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclarationInput {
    /// Paths named in the goal text.
    GoalPaths,
    /// The explicit `affected_files` list.
    AffectedFiles,
    /// An attached diff, read for its line count.
    Diff,
}

impl DeclarationInput {
    /// Every declaration the tiering reads. The metric surface publishes one
    /// cell per entry, zeros included: a variant missing from here is an input
    /// whose use is counted nowhere.
    pub const ALL: [DeclarationInput; 3] = [
        DeclarationInput::GoalPaths,
        DeclarationInput::AffectedFiles,
        DeclarationInput::Diff,
    ];

    /// The task input field this declaration is read from. Kept as a method
    /// rather than a table so the reader and the name cannot be updated apart.
    pub const fn field(&self) -> &'static str {
        match self {
            DeclarationInput::GoalPaths => "goal",
            DeclarationInput::AffectedFiles => "affected_files",
            DeclarationInput::Diff => "diff",
        }
    }

    /// Position in [`Self::ALL`], and the bit this input occupies in
    /// [`DeclarationInputs`].
    pub const fn index(&self) -> usize {
        match self {
            DeclarationInput::GoalPaths => 0,
            DeclarationInput::AffectedFiles => 1,
            DeclarationInput::Diff => 2,
        }
    }

    /// The value published on the metric surface.
    pub const fn as_str(&self) -> &'static str {
        match self {
            DeclarationInput::GoalPaths => "goal_paths",
            DeclarationInput::AffectedFiles => "affected_files",
            DeclarationInput::Diff => "diff",
        }
    }
}

/// Which of the request's declarations were actually present.
///
/// A set rather than a count: "two of three" does not say which input went
/// missing, and which one is missing is the whole question — a request that
/// names files but attaches no diff keeps the file branch of the tiering alive
/// while the line branch stays unreachable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeclarationInputs(u8);

impl DeclarationInputs {
    pub const fn empty() -> Self {
        Self(0)
    }

    pub fn insert(&mut self, input: DeclarationInput) {
        self.0 |= 1 << input.index();
    }

    pub const fn contains(&self, input: DeclarationInput) -> bool {
        self.0 & (1 << input.index()) != 0
    }

    pub fn iter(&self) -> impl Iterator<Item = DeclarationInput> + '_ {
        DeclarationInput::ALL
            .into_iter()
            .filter(move |input| self.contains(*input))
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

/// Extensions whose files are prose rather than something a compiler, a schema
/// or a test run reads.
///
/// The list lives in the contract layer because the routing rule here and the
/// change pipeline's tiering both read it, and two copies would let the same
/// path be prose for one reader and code for the other.
pub use cog_core::is_prose_path;

/// What a request declares, read once.
///
/// Both the scale and its provenance are taken from here, so what was counted
/// and the naming of what was counted cannot drift apart — the same reason an
/// attached diff is measured by the one function the landing budget uses.
struct Declarations {
    goal_paths: Vec<String>,
    affected_files: Vec<String>,
    lines: Option<usize>,
}

/// Read the declarations a request carries. Every field of `task.input` the
/// tiering ever looks at is read here and nowhere else.
fn declarations(task: &cog_core::Task) -> Declarations {
    let goal = task
        .input
        .get("goal")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    Declarations {
        goal_paths: cog_core::paths_named_in_goal(goal),
        affected_files: task
            .input
            .get("affected_files")
            .and_then(|v| v.as_array())
            .map(|named| {
                named
                    .iter()
                    .filter_map(|v| v.as_str())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        lines: task
            .input
            .get("diff")
            .and_then(|v| v.as_str())
            .map(cog_core::count_diff_lines),
    }
}

impl Declarations {
    fn inputs(&self) -> DeclarationInputs {
        let mut inputs = DeclarationInputs::empty();
        if !self.goal_paths.is_empty() {
            inputs.insert(DeclarationInput::GoalPaths);
        }
        if !self.affected_files.is_empty() {
            inputs.insert(DeclarationInput::AffectedFiles);
        }
        if self.lines.is_some() {
            inputs.insert(DeclarationInput::Diff);
        }
        inputs
    }
}

/// What the request declares about the size of the change it asks for.
///
/// Two declarations are read, because they are the two a request can make: the
/// paths it names (in the goal text, or as an explicit file list) and the diff it
/// attaches. Anything else about the request — its length, its tone — is a
/// property of the asking, not of the change.
pub fn declared_scale(task: &cog_core::Task) -> DeclaredScale {
    let declared = declarations(task);
    let mut files: Vec<String> = Vec::new();
    for path in declared
        .goal_paths
        .iter()
        .chain(declared.affected_files.iter())
    {
        if !files.contains(path) {
            files.push(path.clone());
        }
    }
    if files.is_empty() && declared.lines.is_none() {
        return DeclaredScale::Unknown;
    }
    DeclaredScale::Measured {
        files: files.len(),
        code_files: files.iter().filter(|p| !is_prose_path(p)).count(),
        lines: declared.lines,
    }
}

/// Which declarations a request actually carries.
///
/// Published so that a judgement reading an input nothing supplies reports
/// itself as unreachable rather than as a rule with nothing to say: the tiering
/// can only be decided by the declarations that arrived, and a declaration no
/// caller sends is a branch of the rule that no deployment traffic reaches.
pub fn declaration_inputs(task: &cog_core::Task) -> DeclarationInputs {
    declarations(task).inputs()
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
///
/// One name per mode, in [`Self::as_str`]. The serde impls below go through it
/// rather than through the variant names, which spelled something else again
/// (`Pipeline` beside `pipeline`, `PlanOnly` beside `plan_only`): a record
/// written by one and counted by the other is a mode nobody selected, and a
/// topology whose cell is always zero reads the same as one that never ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Every mode this build can name, so a reader that counts decisions can
    /// publish a cell per mode rather than only the ones it happened to see.
    ///
    /// `PlanOnly` is in here although the selector never returns it: the
    /// decomposition path sets it directly, and a surface that omitted it could
    /// not tell "topology selection never chose it, correctly" from "the
    /// decomposition path stopped running".
    pub const ALL: [PgeMode; 4] = [
        PgeMode::Pipeline,
        PgeMode::Roundtable,
        PgeMode::PlanOnly,
        PgeMode::Direct,
    ];

    /// 这个模式在台账与学习数据里的名字。
    ///
    /// `None` = 它不是一次模式选择，别记账：分解路径从来没有"选哪种拓扑更好"
    /// 这一步，把它写成一条模式试验记录，模式选择的样本里就会混进一批从未
    /// 发生过的试验，而污染的表现是模型对真实模式越来越有把握。
    pub fn as_learnt_mode(&self) -> Option<&'static str> {
        match self {
            // 分解路径没有做过选择，别记成一次试验。
            PgeMode::PlanOnly => None,
            _ => Some(self.as_str()),
        }
    }

    /// 这个模式的名字，用于记录与日志。所有模式都有名字，`None` 只影响学习面。
    ///
    /// 这是这个枚举唯一一套拼写：字段、学习台账与序列化都从它取。
    pub fn as_str(&self) -> &'static str {
        match self {
            PgeMode::Pipeline => "pipeline",
            PgeMode::Roundtable => "roundtable",
            PgeMode::Direct => "direct",
            PgeMode::PlanOnly => "plan_only",
        }
    }

    /// `name` 指的那个模式，或 `None`：不在 [`Self::ALL`] 上的名字不猜成某个模式。
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|mode| mode.as_str() == name)
    }
}

impl serde::Serialize for PgeMode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// 认不出的名字是错误，不是某个默认模式：一个没见过的拓扑名，与"选了 Pipeline"
/// 是两件事，折成后者就让词典不一致这条事实消失在一次成功的解析里。
impl<'de> serde::Deserialize<'de> for PgeMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::parse(&name).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown pge mode {name:?}; declared modes are {:?}",
                PgeMode::ALL.map(|mode| mode.as_str())
            ))
        })
    }
}

/// Which rule decided a task's topology.
///
/// A type rather than a phrase inside the reason, because the two answer
/// different questions and only one of them may move. The reason is written for
/// a person and can be reworded any time; the stage is what the routing
/// observation surface counts by. Recovering it by parsing the reason would make
/// every rewording a silent change in what the metrics mean, and a stage whose
/// wording drifted would read exactly like a stage that never decided anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteStage {
    /// A measured [`DeclaredScale`] settled it on its own.
    DeclaredScale,
    /// A word in the goal.
    Keyword,
    /// Recorded outcomes for this request group, when the evidence cleared the
    /// configured margin.
    HistoricalPrior,
    /// The static complexity score.
    Score,
    /// The mode-selection agent's own judgement.
    Agent,
    /// Nothing decided it; quality-first default.
    Default,
}

impl RouteStage {
    /// Every stage this build can return.
    ///
    /// The observation surface publishes a cell per entry rather than only the
    /// stages that happened to fire: a series that is absent because a stage is
    /// dead and one that is absent because it was never wired up are the same
    /// reading otherwise, and the second is the one worth catching.
    pub const ALL: [RouteStage; 6] = [
        RouteStage::DeclaredScale,
        RouteStage::Keyword,
        RouteStage::HistoricalPrior,
        RouteStage::Score,
        RouteStage::Agent,
        RouteStage::Default,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            RouteStage::DeclaredScale => "declared_scale",
            RouteStage::Keyword => "keyword",
            RouteStage::HistoricalPrior => "historical_prior",
            RouteStage::Score => "score",
            RouteStage::Agent => "agent",
            RouteStage::Default => "default",
        }
    }
}

/// A routing decision together with the stage that made it.
#[derive(Debug, Clone)]
pub struct RouteDecision {
    pub mode: PgeMode,
    pub stage: RouteStage,
    /// For a person reading a log line. Never parsed back into a stage.
    pub reason: String,
}

/// Every name [`scale_label`] can return, so the routing surface can publish a
/// cell per name instead of only the ones that occurred.
pub const SCALE_LABELS: [&str; 4] = ["unknown", "none", "shortcut", "deep"];

/// The name a declared scale is counted under.
///
/// Four values, and the last two are the ones that make the tiering's input
/// face readable: `unknown` says the request declared nothing (so no tier could
/// have been earned), `none` says it declared a scope the tiering does not act
/// on. Without them a routing surface cannot tell "no prose-only request
/// arrived" from "requests stopped declaring anything at all".
pub fn scale_label(scale: DeclaredScale) -> &'static str {
    match change_tier(scale) {
        Some(ChangeTier::Shortcut) => "shortcut",
        Some(ChangeTier::Deep) => "deep",
        None if matches!(scale, DeclaredScale::Measured { .. }) => "none",
        None => "unknown",
    }
}

/// Threshold below which the [`PgeMode::Pipeline`] is preferred.
// cap: bound — 分数低于它才走 Pipeline 这条最轻的拓扑
pub const PIPELINE_SCORE_THRESHOLD: f64 = 0.4;

/// Compute the complexity score for a profile.
///
/// `0.25*novelty + 0.30*risk + 0.20*ambiguity + 0.10*deps`, so the score spans
/// 0.0..=0.85 rather than 0.0..=1.0 and [`PIPELINE_SCORE_THRESHOLD`] is 0.4 of
/// that same span — the numbers are unchanged for every profile it already
/// scored, because the fourth input below never varied.
///
/// A fifth weighted term used to sit here, `0.15*(1 - historical_success)`. It
/// was removed rather than kept: no writer ever measured a success rate, every
/// profile was built with the literal `1.0`, and the term therefore contributed
/// exactly zero to every decision ever taken. A weight that cannot vary does not
/// make the score more careful — it makes a reader believe the routing consults
/// history, and the routing does not. Reinstating it means finding the writer
/// that reads outcomes first; the type will refuse to compile until the field
/// comes back, which is the point.
///
/// `token_budget` is intentionally not used in the scoring formula but is kept
/// on [`TaskProfile`] for future budget-aware variants.
pub fn complexity_score(p: &TaskProfile) -> f64 {
    p.novelty * 0.25 + p.risk * 0.30 + p.ambiguity * 0.20 + p.dependency_count * 0.10
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
pub fn select_mode(p: &TaskProfile) -> RouteDecision {
    let score = complexity_score(p);
    let (mode, stage) = match change_tier(p.declared_scale) {
        Some(ChangeTier::Shortcut) => (PgeMode::Direct, RouteStage::DeclaredScale),
        Some(ChangeTier::Deep) => (PgeMode::Roundtable, RouteStage::DeclaredScale),
        None if score < PIPELINE_SCORE_THRESHOLD => (PgeMode::Pipeline, RouteStage::Score),
        None => (PgeMode::Roundtable, RouteStage::Score),
    };
    RouteDecision {
        mode,
        stage,
        reason: format!(
            "{} rule: declared_scale={:?}, complexity_score={score:.2} → {mode:?}",
            stage.as_str(),
            p.declared_scale
        ),
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
        declared_scale: declared_scale(task),
        declaration_inputs: declaration_inputs(task),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个模式只有一个名字：写出去的、读回来的、记进学习台账的都是 `as_str`。
    ///
    /// 下面这个 match 是刻意穷尽的，新增一个模式会让它编译不过，逼着人回到这份
    /// 断言，而不是只加变体、让 `ALL` 少一格——少的那一格在路由面上永远是零，
    /// 读起来与"这条路没人走过"一样，而它真正的意思是"这个模式从没被发布过"。
    #[test]
    fn every_mode_has_one_name_and_it_survives_the_wire() {
        let mut names = std::collections::BTreeSet::new();
        for mode in [
            PgeMode::Pipeline,
            PgeMode::Roundtable,
            PgeMode::PlanOnly,
            PgeMode::Direct,
        ] {
            match mode {
                PgeMode::Pipeline | PgeMode::Roundtable | PgeMode::PlanOnly | PgeMode::Direct => {}
            }
            assert!(
                PgeMode::ALL.contains(&mode),
                "{mode:?} 不在 ALL 上，路由面永远不会发布这一格"
            );
            assert!(
                names.insert(mode.as_str()),
                "两个模式共用一个名字 {:?}，台账上的两格会合并成一格",
                mode.as_str()
            );
            let json = serde_json::to_string(&mode).expect("mode serializes");
            assert_eq!(json, format!("\"{}\"", mode.as_str()));
            assert_eq!(serde_json::from_str::<PgeMode>(&json).ok(), Some(mode));
        }
        // 变体名不是名字：这一套曾经同时出现在两条面上。
        assert!(serde_json::from_str::<PgeMode>("\"Pipeline\"").is_err());
        // 学习面不记分解路径，另外三个记的就是同一个名字。
        assert_eq!(PgeMode::PlanOnly.as_learnt_mode(), None);
        for mode in PgeMode::ALL {
            if mode != PgeMode::PlanOnly {
                assert_eq!(mode.as_learnt_mode(), Some(mode.as_str()));
            }
        }
    }

    /// 每种声明输入在指标面上有自己的标签，且都在 `ALL` 上。
    #[test]
    fn every_declaration_input_has_its_own_label_on_the_list() {
        let mut labels = std::collections::BTreeSet::new();
        for input in [
            DeclarationInput::GoalPaths,
            DeclarationInput::AffectedFiles,
            DeclarationInput::Diff,
        ] {
            match input {
                DeclarationInput::GoalPaths
                | DeclarationInput::AffectedFiles
                | DeclarationInput::Diff => {}
            }
            assert!(
                DeclarationInput::ALL.contains(&input),
                "{input:?} 不在 ALL 上，指标面永远不会发布这一格"
            );
            assert!(
                labels.insert(input.as_str()),
                "两种输入共用一个标签 {:?}",
                input.as_str()
            );
        }
        assert_eq!(labels.len(), DeclarationInput::ALL.len());
    }

    #[test]
    fn default_profile_picks_pipeline() {
        let p = TaskProfile::default();
        assert!(complexity_score(&p) < PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p).mode, PgeMode::Pipeline);
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
            declared_scale: DeclaredScale::Unknown,
            declaration_inputs: DeclarationInputs::empty(),
        };
        let score = complexity_score(&p);
        // 0.5*0.25 + 0.5*0.30 + 0.5*0.20 = 0.125 + 0.15 + 0.10 = 0.375
        assert!(
            (0.25..PIPELINE_SCORE_THRESHOLD).contains(&score),
            "score={}",
            score
        );
        assert_eq!(select_mode(&p).mode, PgeMode::Pipeline);
    }

    #[test]
    fn high_risk_profile_picks_roundtable() {
        let p = TaskProfile {
            novelty: 0.9,
            risk: 0.9,
            ambiguity: 0.9,
            dependency_count: 0.9,
            token_budget: 1.0,
            declared_scale: DeclaredScale::Unknown,
            declaration_inputs: DeclarationInputs::empty(),
        };
        assert!(complexity_score(&p) >= PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p).mode, PgeMode::Roundtable);
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
            declared_scale: DeclaredScale::Unknown,
            declaration_inputs: DeclarationInputs::empty(),
        };
        let p = TaskProfile {
            risk: 1.0,
            dependency_count: 1.0,
            ..p
        };
        let score = complexity_score(&p);
        assert!((score - 0.4).abs() < f64::EPSILON);
        assert_eq!(select_mode(&p).mode, PgeMode::Roundtable);
    }

    /// 分数的量程就是那四个**被测过**的输入：满分 0.85，阈值落在量程之内。
    ///
    /// 这一条判的是「公式只能吃这四个输入」这件事本身。曾经还有第五项
    /// `0.15*(1 - historical_success)`，而没有任何一处量过成功率——每个 profile
    /// 都是用字面量 1.0 建出来的，那一项对每一次判定都恰好贡献零。它加进来的样子
    /// 就是「能到 1.0 而其中 0.15 没人量」，而量程是这条判据里唯一读得出那个差值
    /// 的数：谁再加一项常量，这里立刻对不上 0.85。
    #[test]
    fn the_score_spans_the_four_inputs_that_are_measured() {
        let maxed = TaskProfile {
            novelty: 1.0,
            risk: 1.0,
            ambiguity: 1.0,
            dependency_count: 1.0,
            ..TaskProfile::default()
        };
        let top = complexity_score(&maxed);
        assert!(
            (top - 0.85).abs() < 1e-9,
            "满分是 {top}，不是四个权重之和 0.85——多出来的那一段没人量过"
        );
        assert!(
            PIPELINE_SCORE_THRESHOLD < top,
            "阈值 {PIPELINE_SCORE_THRESHOLD} 不在量程 0..={top} 之内：任何输入都只会走同一个分支"
        );

        // 四个输入各自都动得动分数。谁被钉死成常量（历史长相与这一次那个字面量
        // 一模一样），这一条就对不上。
        let base = complexity_score(&TaskProfile::default());
        for (name, moved) in [
            (
                "novelty",
                TaskProfile {
                    novelty: 1.0,
                    ..TaskProfile::default()
                },
            ),
            (
                "risk",
                TaskProfile {
                    risk: 1.0,
                    ..TaskProfile::default()
                },
            ),
            (
                "ambiguity",
                TaskProfile {
                    ambiguity: 1.0,
                    ..TaskProfile::default()
                },
            ),
            (
                "dependency_count",
                TaskProfile {
                    dependency_count: 1.0,
                    ..TaskProfile::default()
                },
            ),
        ] {
            assert!(
                complexity_score(&moved) > base,
                "{name} 置 1.0 之后分数没有动：这个输入在公式里形同常量"
            );
        }
    }

    /// 从任务算出来的那一份，四个输入都跟着任务走，不是被谁填死的。
    ///
    /// 上面那条判的是公式，这条判的是**推导**：公式里的四个输入各自都得由任务本身
    /// 算出来。一个被写成字面量的字段（上一次是 `historical_success: 1.0`）在这条
    /// 断言下当场红——两个在所有维度上都不同的任务，推出同一个 profile。
    #[test]
    fn the_derived_profile_scores_only_what_the_task_carries() {
        let light = task_with("fix it", serde_json::json!({}));
        let mut heavy = task_with(
            "rewrite the parser so that every caller in the workspace keeps compiling while              the AST gains a second shape and the error type learns to carry spans",
            serde_json::json!({}),
        );
        heavy.blocked_by = vec!["a".into(), "b".into(), "c".into(), "d".into()];

        let a = derive_task_profile(&light);
        let b = derive_task_profile(&heavy);
        assert_ne!(a.risk, b.risk, "risk 没有跟着依赖数走");
        assert_ne!(
            a.dependency_count, b.dependency_count,
            "dependency_count 没有跟着依赖数走"
        );
        assert_ne!(a.ambiguity, b.ambiguity, "ambiguity 没有跟着输入长度走");
        assert!(complexity_score(&b) > complexity_score(&a));
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
        assert_eq!(select_mode(&p).mode, PgeMode::Roundtable);
    }

    /// The provenance names the declarations that arrived, and nothing else.
    ///
    /// Each case carries one declaration, so a set built from the wrong field
    /// (or from the request merely existing) shows up as a cell lit that no
    /// input lit.
    #[test]
    fn the_provenance_names_the_declarations_that_arrived() {
        let named_only = task_with("touch crates/cog-parser/src/lib.rs", serde_json::json!({}));
        assert_eq!(
            declaration_inputs(&named_only).iter().collect::<Vec<_>>(),
            vec![DeclarationInput::GoalPaths]
        );

        let listed_only = task_with(
            "make the change",
            serde_json::json!({ "affected_files": ["docs/quickstart.md"] }),
        );
        assert_eq!(
            declaration_inputs(&listed_only).iter().collect::<Vec<_>>(),
            vec![DeclarationInput::AffectedFiles]
        );

        let diff_only = task_with(
            "apply the patch",
            serde_json::json!({ "diff": "--- a/x.rs\n+++ b/x.rs\n@@ -1 +1 @@\n-a\n+b\n" }),
        );
        assert_eq!(
            declaration_inputs(&diff_only).iter().collect::<Vec<_>>(),
            vec![DeclarationInput::Diff]
        );
    }

    /// A request that declares nothing carries no provenance, so the surface
    /// reads the same as it does before any traffic — which is not the same
    /// reading as "this declaration has no producer".
    #[test]
    fn a_request_that_declares_nothing_names_no_input() {
        let task = task_with("make it better", serde_json::json!({}));
        let inputs = declaration_inputs(&task);
        assert!(inputs.is_empty(), "{inputs:?}");
        assert_eq!(declared_scale(&task), DeclaredScale::Unknown);
    }

    /// The set holds every declaration it was given at once, and each one lands
    /// on its own cell.
    #[test]
    fn each_declaration_has_its_own_cell() {
        let mut indices: Vec<usize> = DeclarationInput::ALL.iter().map(|i| i.index()).collect();
        indices.sort_unstable();
        assert_eq!(
            indices,
            (0..DeclarationInput::ALL.len()).collect::<Vec<_>>()
        );
        for input in DeclarationInput::ALL {
            let mut set = DeclarationInputs::empty();
            set.insert(input);
            assert_eq!(set.iter().collect::<Vec<_>>(), vec![input], "{input:?}");
        }
    }

    /// Every task input field the tiering reads is one the surface can count.
    ///
    /// The provenance series is complete only if it covers the reader: a field
    /// added to `declarations` without a [`DeclarationInput`] for it can decide
    /// a route while its cell stays at zero — the exact state this surface
    /// exists to make visible, and the one that is otherwise found by reading
    /// this file.
    #[test]
    fn every_field_the_tiering_reads_is_one_the_surface_counts() {
        let source = include_str!("profile.rs");
        let signature = "fn declarations(task: &cog_core::Task) -> Declarations {";
        let start = source
            .find(signature)
            .expect("the tiering no longer reads the task in a function named `declarations`");
        let body = &source[start..];
        let end = body
            .find("\n}\n")
            .expect("`declarations` has no closing brace at column 0");
        let body = &body[..end];

        let mut read: Vec<String> = Vec::new();
        let mut rest = body;
        while let Some(at) = rest.find(".get(\"") {
            rest = &rest[at + 6..];
            match rest.find('"') {
                Some(close) => {
                    read.push(rest[..close].to_string());
                    rest = &rest[close..];
                }
                None => break,
            }
        }
        assert!(
            !read.is_empty(),
            "the scan found no input field in `declarations`, so it confirms nothing"
        );

        let known: Vec<&str> = DeclarationInput::ALL.iter().map(|i| i.field()).collect();
        let uncounted: Vec<&String> = read
            .iter()
            .filter(|k| !known.contains(&k.as_str()))
            .collect();
        assert!(
            uncounted.is_empty(),
            "the tiering reads task fields nothing can count: {uncounted:?}; \
             known fields are {known:?}"
        );
        let unread: Vec<&&str> = known
            .iter()
            .filter(|f| !read.iter().any(|k| k == *f))
            .collect();
        assert!(
            unread.is_empty(),
            "these declarations name a field the tiering no longer reads, so their \
             cell can only ever be zero: {unread:?}"
        );
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
        assert_eq!(select_mode(&p).mode, PgeMode::Direct);
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
        assert_eq!(select_mode(&p).mode, PgeMode::Pipeline);
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
        assert_eq!(select_mode(&p).mode, PgeMode::Pipeline);
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
            assert_ne!(select_mode(&p).mode, PgeMode::Direct, "{path}");
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
        assert_eq!(select_mode(&p).mode, PgeMode::Roundtable);
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
