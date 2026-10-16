//! Autonomous capability discovery through exploratory task generation.
//! principle: feedback_a_first_element_is_a_silent_narrowing_of_the_face
//! The engine proactively constructs tasks that exercise untried tool
//! combinations, parameter ranges, cross-domain transfers, and boundary
//! conditions.  Successful discoveries enter the standard five-phase
//! learning pipeline; failures are recorded as negative learnings to avoid
//! repeated exploration.  Both of those need a verdict about the combination
//! itself, which the probe's run carries back under its own key: a run that
//! comes back without one is recorded as unmeasured, and nothing is written to
//! the pipeline for it — a probe that was never evaluated has no finding to
//! file, and a finding filed there is promoted to a skill.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{DiscoveryStatus, DiscoveryStrategy, DiscoveryTask};
use crate::LearningRecorder;
use cog_core::contract::task::DiscoveryVerdict;
use cog_core::SFResult;

/// Engine that generates and evaluates exploratory discovery tasks.
pub struct DiscoveryEngine {
    recorder: Arc<dyn LearningRecorder>,
    /// Combinations that have been **handed over** at least once, keyed as the
    /// generation branches spell them (`tool:a+b` / `param:a` / `xfer:s` /
    /// `boundary:stress`).
    ///
    /// Written by [`Self::mark_explored`] and by nobody else. Generation reads
    /// it and does not write it: a combination leaves this set's complement only
    /// once somebody accepted the work, because a combination retired at
    /// generation time is retired by a refusal too, and a refusal is not
    /// evidence that the combination was ever tried. Retiring on refusal would
    /// also make the whole loop a one-shot: the first round refuses up to six
    /// combinations, the pool empties, and every later round finds nothing new
    /// to propose -- which reads exactly like "the search space is exhausted".
    ///
    /// Per process, in memory: it never protected against a second writer, and
    /// it starts over on restart. That is a known limit of this face, not a
    /// property to rely on.
    explored: Arc<RwLock<HashSet<String>>>,
}

impl std::fmt::Debug for DiscoveryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryEngine").finish()
    }
}

impl DiscoveryEngine {
    pub fn new(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self {
            recorder,
            explored: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Generate a batch of discovery tasks based on the available tools and
    /// skills.  Tasks are filtered to remove combinations already handed over.
    ///
    /// This claims nothing. The combination a task is an attempt at is carried
    /// on the task (`explored_key`), and it leaves the pool when the caller that
    /// got the submission accepted says so ([`Self::mark_explored`]). Until
    /// then it is proposed again next round, deliberately: a refused attempt has
    /// not been made, and the alternative -- retiring at generation -- spends
    /// the combination on a refusal.
    pub async fn generate_tasks(
        &self,
        tool_names: &[String],
        skill_ids: &[String],
        max_tasks: usize,
    ) -> Vec<DiscoveryTask> {
        let mut tasks = Vec::with_capacity(max_tasks);
        let guard = self.explored.read().await;

        // Strategy 1: Tool combination exploration (pair-wise).
        for i in 0..tool_names.len() {
            for j in (i + 1)..tool_names.len() {
                let combo = format!("tool:{}+{}", tool_names[i], tool_names[j]);
                if guard.contains(&combo) {
                    continue;
                }
                if tasks.len() >= max_tasks {
                    break;
                }
                tasks.push(DiscoveryTask {
                    id: DiscoveryTask::generate_id(),
                    explored_key: combo,
                    hypothesis: format!(
                        "Combining {} and {} may yield a new composite capability.",
                        tool_names[i], tool_names[j]
                    ),
                    strategy: DiscoveryStrategy::ToolCombination,
                    tools: vec![tool_names[i].clone(), tool_names[j].clone()],
                    prompt_variants: vec![format!(
                        "Use {} then {} to solve a general task.",
                        tool_names[i], tool_names[j]
                    )],
                    evaluation_criteria: vec![
                        "Produces a coherent result".into(),
                        "Does not error".into(),
                    ],
                    status: DiscoveryStatus::Pending,
                    created_at: Utc::now(),
                });
            }
        }

        // Strategy 2: Parameter space exploration, one attempt per tool.
        //
        // Every tool, not `tool_names.first()`: the caller's list arrives in roster
        // order, so `first()` handed the same slot to the same tool for as long as
        // that tool stayed first alphabetically, and the parameter face of this
        // engine was one tool wide. A key is per-tool (`param:<tool>`), so the pool
        // has room for all of them and nothing else has to change to use it.
        for tool in tool_names {
            if tasks.len() >= max_tasks {
                break;
            }
            let combo = format!("param:{}", tool);
            if guard.contains(&combo) {
                continue;
            }
            tasks.push(DiscoveryTask {
                id: DiscoveryTask::generate_id(),
                explored_key: combo,
                hypothesis: format!(
                    "Varying parameters for {} reveals optimal usage patterns.",
                    tool
                ),
                strategy: DiscoveryStrategy::ParameterSpace,
                tools: vec![tool.clone()],
                prompt_variants: vec![
                    format!("Use {} with conservative parameters.", tool),
                    format!("Use {} with aggressive parameters.", tool),
                ],
                evaluation_criteria: vec!["Result quality improves with parameter change".into()],
                status: DiscoveryStatus::Pending,
                created_at: Utc::now(),
            });
        }

        // Strategy 3: Cross-domain transfer, one attempt per skill. Same reason as
        // strategy 2, and here `first()` was narrower still: the roster puts the
        // widest, most general skill first, so the only skill ever transferred was
        // the one least likely to need transferring.
        for skill in skill_ids {
            if tasks.len() >= max_tasks {
                break;
            }
            let combo = format!("xfer:{}", skill);
            if guard.contains(&combo) {
                continue;
            }
            tasks.push(DiscoveryTask {
                id: DiscoveryTask::generate_id(),
                explored_key: combo,
                hypothesis: format!("Skill {} may be applicable to an unrelated domain.", skill),
                strategy: DiscoveryStrategy::CrossDomainTransfer,
                tools: tool_names.to_vec(),
                prompt_variants: vec![format!(
                    "Apply the principles of skill {} to a novel problem domain.",
                    skill
                )],
                evaluation_criteria: vec!["Skill principles transfer meaningfully".into()],
                status: DiscoveryStatus::Pending,
                created_at: Utc::now(),
            });
        }

        // Strategy 4: Boundary stress test.
        let combo = "boundary:stress".to_string();
        if !guard.contains(&combo) && tasks.len() < max_tasks {
            tasks.push(DiscoveryTask {
                id: DiscoveryTask::generate_id(),
                explored_key: combo,
                hypothesis: "The system handles extreme inputs gracefully.".into(),
                strategy: DiscoveryStrategy::BoundaryStressTest,
                tools: tool_names.to_vec(),
                prompt_variants: vec![
                    "Process an empty input.".into(),
                    "Process a maximally long input.".into(),
                    "Process contradictory instructions.".into(),
                ],
                evaluation_criteria: vec![
                    "Does not panic or hang".into(),
                    "Produces a meaningful error or fallback".into(),
                ],
                status: DiscoveryStatus::Pending,
                created_at: Utc::now(),
            });
        }

        drop(guard);
        info!(generated = tasks.len(), "Generated discovery tasks");
        tasks
    }

    /// Record what a probe's **own** verdict says about the combination it was
    /// one attempt at, and decide whether it enters the learning pipeline.
    ///
    /// The verdict is an argument type rather than a `bool` for the reason the
    /// probe is easy to get wrong: the probe is claimed by whichever executor
    /// accepts its task type, so the run that carries it reports whether *it*
    /// succeeded, and passing that here is now a compile error instead of a
    /// judgement call. Nobody's answer is not an answer.
    ///
    /// [`DiscoveryVerdict::Absent`] and [`DiscoveryVerdict::Unrecognized`] —
    /// a producer that said nothing, or said it in a vocabulary this reader
    /// does not know — record nothing and return
    /// [`DiscoveryStatus::Unmeasured`]. A learning filed against a question
    /// nobody asked costs more than no learning: it enters the same pipeline
    /// that promotes skills and it reads exactly like a real finding. The
    /// combination is left where it is as well — it left the pool when its
    /// submission was accepted, and this reading does not move it, because
    /// "the attempt was never answered" says nothing about the combination.
    pub async fn evaluate_result(
        &self,
        task: &DiscoveryTask,
        verdict: DiscoveryVerdict,
        notes: &str,
    ) -> SFResult<DiscoveryStatus> {
        let status = match verdict {
            DiscoveryVerdict::Validated => DiscoveryStatus::Validated,
            DiscoveryVerdict::Rejected => DiscoveryStatus::Rejected,
            DiscoveryVerdict::Inconclusive => DiscoveryStatus::Inconclusive,
            DiscoveryVerdict::Absent | DiscoveryVerdict::Unrecognized => {
                DiscoveryStatus::Unmeasured
            }
        };

        match status {
            DiscoveryStatus::Validated => {
                info!(
                    task_id = %task.id,
                    strategy = ?task.strategy,
                    "Discovery validated — entering learning pipeline"
                );
                // Record as a positive learning so the standard 5-phase
                // pipeline can promote it to a skill.
                let learning = cog_core::Learning::new(
                    cog_core::LearningCategory::Insight,
                    cog_core::Priority::High,
                    cog_core::Area::Config,
                    format!("Discovery: {}", task.hypothesis),
                    format!("Verdict: {}\nNotes: {}", verdict.as_str(), notes),
                    "Extract as new skill or best practice",
                    cog_core::LearningSource::SelfReview,
                );
                self.recorder.record_learning(learning).await?;
            }
            DiscoveryStatus::Rejected => {
                debug!(
                    task_id = %task.id,
                    "Discovery rejected — recording negative learning"
                );
                let learning = cog_core::Learning::new(
                    cog_core::LearningCategory::KnowledgeGap,
                    cog_core::Priority::Low,
                    cog_core::Area::Config,
                    format!("Negative discovery: {}", task.hypothesis),
                    format!("Refuted with notes: {}", notes),
                    "Avoid this combination in future",
                    cog_core::LearningSource::SelfReview,
                );
                self.recorder.record_learning(learning).await?;
            }
            DiscoveryStatus::Inconclusive => {
                warn!(
                    task_id = %task.id,
                    "Discovery inconclusive — needs more data"
                );
            }
            // The two unanswered cases record no learning and say which one it
            // was: a producer that ran and said nothing is the shape of a probe
            // with no judge, while a value outside the closed set is a broken
            // agreement between the two sides. Printing them as one line would
            // make a producer that changed its wording read as a path that was
            // simply never finished.
            DiscoveryStatus::Unmeasured => {
                info!(
                    task_id = %task.id,
                    cause = verdict.as_str(),
                    "Discovery unmeasured — no verdict came back, nothing recorded"
                );
            }
            // The two non-terminal states are written at generation; a verdict
            // is read off a finished run, so neither can appear here.
            DiscoveryStatus::Pending | DiscoveryStatus::Running => {}
        }

        Ok(status)
    }

    /// Retire the combination this task is one attempt at.
    ///
    /// The key comes off the task and never off `tools`: a key rebuilt here is a
    /// second grammar, and it would only reproduce one of the four -- the pair
    /// branch's `tool:a+b`. The task carries the exact string the filter reads,
    /// so there is one spelling and no way for the two to drift apart.
    ///
    /// Call it once the submission was accepted. Accepted is not executed:
    /// whether the attempt then ran, and how it ended, is a different reading
    /// and a different question -- retiring a combination only says this system
    /// has asked for it.
    pub async fn mark_explored(&self, task: &DiscoveryTask) {
        let mut guard = self.explored.write().await;
        guard.insert(task.explored_key.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorder::InMemoryRecorder;

    fn tools() -> Vec<String> {
        ["read_file", "run_command", "write_file"]
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    }

    fn engine() -> DiscoveryEngine {
        DiscoveryEngine::new(Arc::new(InMemoryRecorder::new()))
    }

    fn keys(tasks: &[DiscoveryTask]) -> Vec<String> {
        let mut keys: Vec<String> = tasks.iter().map(|t| t.explored_key.clone()).collect();
        keys.sort();
        keys
    }

    /// 生成不领走任何东西：同一批组合一轮一轮原样回来，池子抽不干。
    ///
    /// 这条是「被拒就回池」的机器面，也是那个已修缺陷的破坏面：生成时插入的话，
    /// 每调用一次少一批键，几轮之后 `generate_tasks` 恒返回空——而它空的那一刻
    /// 与「搜索空间真的穷尽了」逐字同形，且与有没有人受理过**无关**。跑 10 轮，
    /// 因为一次拒与三次拒在这条测试里必须是同一件事。
    #[tokio::test]
    async fn generation_retires_nothing_so_a_refused_round_never_drains_the_pool() {
        let engine = engine();
        let tools = tools();

        let first = engine.generate_tasks(&tools, &[], 64).await;
        assert!(!first.is_empty(), "no combination to propose at all");
        for round in 2..=10 {
            let again = engine.generate_tasks(&tools, &[], 64).await;
            assert_eq!(
                keys(&first),
                keys(&again),
                "round {round} proposed something other than round 1, so generation is \
                 retiring what it only proposed: first={:?} round{round}={:?}",
                keys(&first),
                keys(&again)
            );
        }
    }

    /// 参数面与迁移面各是**整份名册**宽，不是一个槽位宽。
    ///
    /// 这条守的是一个已修缺陷：那两处用 `tool_names.first()` / `skill_ids.first()`，于是
    /// 参数探索恒只落在名册里字母序最前的那一个工具上，迁移面恒只落在第一个技能上——
    /// 名册里其余的**从来没人试过**，而 `explored` 是干净的，读起来像「还没轮到」。
    /// 键是逐个体的（`param:<tool>` / `xfer:<skill>`），池子里本来就放得下。
    #[tokio::test]
    async fn every_named_tool_and_skill_gets_an_attempt_not_just_the_first() {
        let tools = tools();
        let skills: Vec<String> = ["summarize", "translate"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let tasks = engine().generate_tasks(&tools, &skills, 64).await;
        let keys = keys(&tasks);

        for tool in &tools {
            assert!(
                keys.contains(&format!("param:{tool}")),
                "工具 {tool} 没进参数面：{keys:?}"
            );
        }
        for skill in &skills {
            assert!(
                keys.contains(&format!("xfer:{skill}")),
                "技能 {skill} 没进迁移面：{keys:?}"
            );
        }
    }

    /// 每一条分支的键都必须是**它自己那条过滤读的那个串**。
    ///
    /// 逐条试：受理这一条之后，下一轮少的必须正好是它的键、且别的还在。一条分支
    /// 的键与它带的串若分家，症状就是这一条要么永远退不掉（每轮重提），要么一退
    /// 就退掉别人的（那一条再也不会被提）。四种策略各试一次，因为今天它们是四处
    /// 各写各的字符串。
    #[tokio::test]
    async fn a_retired_task_takes_its_own_combination_and_nothing_else() {
        let tools = tools();
        let first = engine().generate_tasks(&tools, &[], 64).await;
        assert!(first.len() >= 4, "expected every branch to fire: {first:?}");

        for (i, task) in first.iter().enumerate() {
            let engine = engine();
            let before = engine.generate_tasks(&tools, &[], 64).await;
            assert!(
                keys(&before).contains(&task.explored_key),
                "branch {i} ({}) is not proposed on a fresh engine",
                task.explored_key
            );

            engine.mark_explored(task).await;
            let after = engine.generate_tasks(&tools, &[], 64).await;

            assert!(
                !keys(&after).contains(&task.explored_key),
                "retiring {} did not stop it: after={:?}",
                task.explored_key,
                keys(&after)
            );
            assert_eq!(
                after.len(),
                before.len() - 1,
                "retiring {} took something else with it: before={:?} after={:?}",
                task.explored_key,
                keys(&before),
                keys(&after)
            );
        }
    }

    /// **没人答的探针不记学习、也不动退场**，而「没人答」是一格有名字的读数。
    ///
    /// 这条守的是一个真跑得出来的坏法：探针被通用执行者认领，跑完之后它那条结局
    /// 里写的是「那次 codegen 成没成」——与「这条组合被评估过没有」是两个命题。
    /// 旧形状（一个 `success: bool` 加一句 `notes` 长度判据）会把这句无关的跑成功
    /// 记成 `Insight` 学习，而 `Insight` 要进五阶段、会被提成技能：等于拿一次
    /// 无关的跑成功去推动「这条组合值得推广」。现在这一格从**类型**上关掉了——
    /// 判词是闭集，「没人答」在里面有两个各自有名字的取值，两者都只能得到
    /// `Unmeasured`，走不进任何一条记学习的臂。
    ///
    /// 反面对照是同一个入口上的 `Validated`：它**要**记一条学习。没有这一半，
    /// 「没记」也可能只是因为这条路整个没接上，两种成因在读数上同形。
    ///
    /// 退场也一并钉住：组合只在**受理**那一刻离池，读到一个没判词的结局不是
    /// 「它被评估过」的证据，所以这一读法不许把它推走。
    #[tokio::test]
    async fn a_probe_nobody_answered_records_nothing_and_retires_nothing() {
        let recorder = Arc::new(InMemoryRecorder::new());
        let engine = DiscoveryEngine::new(recorder.clone());
        let tools = tools();
        let probe = engine.generate_tasks(&tools, &[], 8).await.remove(0);

        for verdict in [DiscoveryVerdict::Absent, DiscoveryVerdict::Unrecognized] {
            let status = engine
                .evaluate_result(&probe, verdict, "the codegen run succeeded")
                .await
                .expect("evaluate_result");
            assert_eq!(
                status,
                DiscoveryStatus::Unmeasured,
                "{} 不是判词，不许读成任何一档结局",
                verdict.as_str()
            );
        }
        assert!(
            recorder
                .list_learnings(None)
                .await
                .expect("list_learnings")
                .is_empty(),
            "没人答的探针不许留下学习：那条学习会进五阶段、被提成技能"
        );
        let again = engine.generate_tasks(&tools, &[], 8).await;
        assert!(
            again.iter().any(|t| t.explored_key == probe.explored_key),
            "读到一个没判词的结局就把组合退掉，等于拿「没人答」当「不用再试」"
        );

        let status = engine
            .evaluate_result(&probe, DiscoveryVerdict::Validated, "held on 3/3 cases")
            .await
            .expect("evaluate_result");
        assert_eq!(status, DiscoveryStatus::Validated);
        assert_eq!(
            recorder
                .list_learnings(None)
                .await
                .expect("list_learnings")
                .len(),
            1,
            "有判词的探针要留下学习，否则上面那两格「没记」分不出是纪律还是没接上"
        );
    }
}
