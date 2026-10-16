//! Skill effectiveness tracking with A/B comparison and auto-deprecation.
//! Every time an agent uses a skill extracted from reflection, the outcome
//! (success, self-review score, latency, token cost) is fed back into the
//! tracker.  After a minimum sample size the tracker computes a composite
//! effectiveness score and recommends one of four actions:
//! - **Strengthen** – skill is working well, bump its priority.
//! - **Deprecate** – skill is harmful or useless, remove it.
//! - **Refine** – skill is mediocre, trigger LLM-based refinement.
//! - **NoAction** – not enough data yet.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{EffectivenessAction, SkillEffectivenessRecord};
use crate::LearningRecorder;
use cog_core::SFResult;
use cog_core::SkillOutcome;

/// In-memory + recorder-backed tracker for skill effectiveness.
pub struct SkillEffectivenessTracker {
    records: Arc<RwLock<HashMap<(String, String), SkillEffectivenessRecord>>>,
    recorder: Arc<dyn LearningRecorder>,
}

impl std::fmt::Debug for SkillEffectivenessTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkillEffectivenessTracker")
            .field("recorder", &"<dyn LearningRecorder>")
            .finish()
    }
}

impl SkillEffectivenessTracker {
    pub fn new(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            recorder,
        }
    }

    /// Record a single skill usage outcome.
    pub async fn record_outcome(&self, outcome: SkillOutcome) -> SFResult<()> {
        let key = (outcome.skill_id.clone(), outcome.task_signature.clone());
        {
            let mut guard = self.records.write().await;
            let record = guard.entry(key.clone()).or_insert_with(|| {
                SkillEffectivenessRecord::new(&outcome.skill_id, &outcome.task_signature)
            });
            record.accumulate(&outcome);
            record.recompute_score();
        }

        // Persist to backend as a "custom" schema entry.
        self.persist_record(&key.0, &key.1).await?;

        info!(
            skill_id = %outcome.skill_id,
            task_sig = %outcome.task_signature,
            success = outcome.success,
            "recorded skill outcome"
        );
        Ok(())
    }

    /// Evaluate a skill across all task signatures it has been used for.
    pub async fn evaluate_skill(&self, skill_id: &str) -> Option<SkillEffectivenessRecord> {
        let guard = self.records.read().await;
        // Aggregate all task signatures for this skill into a single record.
        let mut aggregated: Option<SkillEffectivenessRecord> = None;
        for ((sid, _), record) in guard.iter() {
            if sid == skill_id {
                match aggregated.as_mut() {
                    Some(a) => {
                        a.used_count += record.used_count;
                        a.success_count += record.success_count;
                        a.total_score += record.total_score;
                        a.total_latency_ms += record.total_latency_ms;
                        a.total_token_cost += record.total_token_cost;
                    }
                    None => {
                        aggregated = Some(record.clone());
                    }
                }
            }
        }
        if let Some(ref mut a) = aggregated {
            a.recompute_score();
        }
        aggregated
    }

    /// Recommend an action for a skill based on its aggregated effectiveness.
    pub async fn recommend_action(&self, skill_id: &str) -> EffectivenessAction {
        match self.evaluate_skill(skill_id).await {
            Some(record) => record.recommend_action(),
            None => EffectivenessAction::NoAction,
        }
    }

    /// Apply the recommended action to a [`cog_core::SkillRegistry`].
    ///
    /// An id the registry does not hold is not acted on. The tracker records
    /// whatever it is told, and the engine reports two outcomes of its own
    /// under names that are not skills (`squad_execution`,
    /// `change_deployment`); an action aimed at one of those leaves selection
    /// untouched — the registry has no such entry — while still writing a
    /// learning that names a skill nobody can look up. Being tracked is not
    /// evidence of being registered, so the registry is asked before acting.
    pub async fn apply_action(
        &self,
        skill_id: &str,
        registry: Arc<RwLock<cog_core::SkillRegistry>>,
        llm: Option<Arc<dyn cog_core::LlmClient>>,
        refines: bool,
    ) -> SFResult<()> {
        let registered = registry.read().await.get_skill(skill_id).is_some();
        if !registered {
            debug!(
                skill_id,
                "tracked id is not a registered skill; nothing to act on"
            );
            return Ok(());
        }
        let action = self.recommend_action(skill_id).await;
        match action {
            EffectivenessAction::Strengthen => {
                info!(skill_id, "Strengthening skill — increasing priority");
                let mut reg = registry.write().await;
                let current = reg.get_priority(skill_id);
                reg.set_priority(skill_id, current + 1);
                drop(reg);
                let learning = cog_core::Learning::new(
                    cog_core::LearningCategory::BestPractice,
                    cog_core::Priority::High,
                    cog_core::Area::Config,
                    format!("Skill {} effectiveness high", skill_id),
                    format!(
                        "Effectiveness score warrants strengthening (priority {} -> {})",
                        current,
                        current + 1
                    ),
                    "Increase skill priority in selection",
                    cog_core::LearningSource::SelfReview,
                );
                self.recorder.record_learning(learning).await?;
            }
            EffectivenessAction::Deprecate => {
                info!(skill_id, "Deprecating skill — removing from registry");
                let mut reg = registry.write().await;
                reg.remove_skill_config(skill_id);
            }
            EffectivenessAction::Refine => {
                // The one branch with effects outside this process. Said out
                // loud rather than silently skipped: a process that is not
                // allowed to refine still *decides* the skill needs it, and a
                // reader seeing no refinement has to be able to tell "nobody is
                // allowed here" from "nobody judged it worth refining".
                if !refines {
                    info!(
                        skill_id,
                        "Refinement recommended; this process is not the one that runs it"
                    );
                } else {
                    info!(skill_id, "Refining skill — triggering evolution");
                    if let Some(llm) = llm {
                        let evolution =
                            crate::evolution::EvolutionEngine::new(llm, registry.clone(), None);
                        evolution.refine_skill(skill_id).await?;
                    } else {
                        warn!(skill_id, "Cannot refine skill: no LLM provider available");
                    }
                }
            }
            EffectivenessAction::NoAction => {
                // Nothing to do.
            }
        }
        Ok(())
    }

    /// Compare skill performance against a baseline (tasks executed without
    /// the skill).  Returns the delta in success rate.
    pub async fn compare_to_baseline(
        &self,
        skill_id: &str,
        baseline_registry: &HashMap<String, (u32, u32)>, // task_sig -> (attempts, successes)
    ) -> HashMap<String, f32> {
        let guard = self.records.read().await;
        let mut deltas = HashMap::new();
        for ((sid, task_sig), record) in guard.iter() {
            if sid != skill_id {
                continue;
            }
            let skill_rate = record.success_count as f32 / record.used_count.max(1) as f32;
            let (base_attempts, base_successes) =
                baseline_registry.get(task_sig).copied().unwrap_or((0, 0));
            let base_rate = base_successes as f32 / base_attempts.max(1) as f32;
            deltas.insert(task_sig.clone(), skill_rate - base_rate);
        }
        deltas
    }

    /// Return all unique skill IDs currently tracked.
    pub async fn tracked_skill_ids(&self) -> Vec<String> {
        let guard = self.records.read().await;
        let mut ids = std::collections::HashSet::new();
        for (sid, _) in guard.keys() {
            ids.insert(sid.clone());
        }
        ids.into_iter().collect()
    }

    async fn persist_record(&self, skill_id: &str, task_signature: &str) -> SFResult<()> {
        let guard = self.records.read().await;
        let record = guard
            .get(&(skill_id.to_string(), task_signature.to_string()))
            .cloned();
        drop(guard);

        if let Some(record) = record {
            let learning = cog_core::Learning::new(
                cog_core::LearningCategory::Insight,
                cog_core::Priority::Medium,
                cog_core::Area::Config,
                format!("Effectiveness {}:{}", skill_id, task_signature),
                serde_json::to_string(&record).unwrap_or_default(),
                "Track skill effectiveness",
                cog_core::LearningSource::SelfReview,
            );
            self.recorder.record_learning(learning).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(skill_id: &str) -> SkillOutcome {
        SkillOutcome {
            skill_id: skill_id.to_string(),
            task_signature: "role".to_string(),
            success: true,
            score: Some(1.0),
            latency_ms: 1,
            token_cost: 1,
            observed_at: chrono::Utc::now(),
        }
    }

    fn registry_holding(skill_id: &str) -> cog_core::SkillRegistry {
        let mut registry = cog_core::SkillRegistry::new();
        registry.insert_skill_config(cog_core::SkillConfig {
            skill_id: skill_id.to_string(),
            name: skill_id.to_string(),
            system_prompt: String::new(),
            tools: Vec::new(),
            max_iterations: 1,
            role_type: "role".to_string(),
        });
        registry
    }

    /// 名单上的 id 不等于注册表认识的技能。
    ///
    /// 引擎自己拿 `squad_execution` / `change_deployment` 两个**不是技能**的名字报
    /// 成绩（squad 那条与变更那条各一处），追踪器照单全收——名单从第一拍起就带着
    /// 它们，`tracked_skill_ids` 不看注册表。所以「有动作可做」不能只问名单，要问
    /// 注册表：否则动的是一个不存在的条目（选择照旧），却写出一条指名不存在技能的
    /// 学习。这条判据**两个方向都量**：真技能必须照常被动到，否则这个门就成了
    /// 「一律不动作」，而那种形状一样能让前半段断言通过。
    #[tokio::test]
    async fn an_action_is_never_taken_against_an_id_the_registry_does_not_hold() {
        let recorder = Arc::new(crate::InMemoryRecorder::new());
        let tracker = SkillEffectivenessTracker::new(recorder.clone());
        for _ in 0..10 {
            tracker.record_outcome(outcome("ghost")).await.unwrap();
            tracker.record_outcome(outcome("real")).await.unwrap();
        }
        let registry = Arc::new(RwLock::new(registry_holding("real")));

        tracker
            .apply_action("ghost", registry.clone(), None, true)
            .await
            .unwrap();
        tracker
            .apply_action("real", registry.clone(), None, true)
            .await
            .unwrap();

        let guard = registry.read().await;
        assert_eq!(
            guard.get_priority("ghost"),
            0,
            "注册表里没有的 id 不该被动到"
        );
        assert_eq!(
            guard.get_priority("real"),
            1,
            "注册表里的技能要照常被动到——头一条断言不该靠「一律不动作」通过"
        );
        drop(guard);

        let summaries: Vec<String> = recorder
            .list_learnings(None)
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.summary)
            .collect();
        assert!(
            !summaries.contains(&"Skill ghost effectiveness high".to_string()),
            "不该落下指名不存在技能的学习：{summaries:?}"
        );
        assert!(
            summaries.contains(&"Skill real effectiveness high".to_string()),
            "真技能那条动作学习应当照旧落下：{summaries:?}"
        );
    }

    /// 一条把 `recommend_action` 推到 `Refine` 的结局：成功率 0（0.40）、
    /// 质量分 1.0（0.30）、延迟与 token 都贴着软上限（各 0.15），合起来 0.60
    /// ——落在 `0.3..=0.8` 那一段里，而不是靠边（0.3 与 0.8 两侧都是别的动作，
    /// 贴着界写会让这条测试在浮点上变成掷硬币）。
    fn refinable_outcome(skill_id: &str) -> SkillOutcome {
        SkillOutcome {
            skill_id: skill_id.to_string(),
            task_signature: "role".to_string(),
            success: false,
            score: Some(1.0),
            latency_ms: 1,
            token_cost: 1,
            observed_at: chrono::Utc::now(),
        }
    }

    #[derive(Default)]
    struct CountingLlm {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for CountingLlm {
        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::text("{}")],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("这支动作只走 chat")
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("这支动作只走 chat")
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// `Refine` 是全仓唯一有跨进程副作用的一支动作：它调 LLM 再写技能配置。
    /// 不是那个唯一写者的进程**照样**要跑这条环、照样应用本进程的动作——它只是
    /// 不落这一支，因为两个进程各自问一次 LLM 会拿回两份答案写同一条技能。
    ///
    /// 两个方向都量：同一批结局在 `refines=false` 下一次 LLM 调用都不许有，
    /// 在 `refines=true` 下必须有一次。只量「不调」的话，一个把 `Refine` 判成
    /// 别的动作（或压根不动作）的实现也能通过。
    #[tokio::test]
    async fn a_process_that_is_not_the_writer_recommends_refinement_without_spending_it() {
        let mut calls_by_role = Vec::new();
        for refines in [false, true] {
            let recorder = Arc::new(crate::InMemoryRecorder::new());
            let tracker = SkillEffectivenessTracker::new(recorder.clone());
            for _ in 0..5 {
                tracker
                    .record_outcome(refinable_outcome("real"))
                    .await
                    .unwrap();
            }
            let registry = Arc::new(RwLock::new(registry_holding("real")));

            // 先钉住这批结局推的确实是 `Refine`。少了这一句，「一次 LLM 调用都
            // 没有」可以由「这条结局推的是别的动作」满足，而那种形状下这条测试
            // 一个字都没量到它要量的那件事。
            assert_eq!(
                tracker.recommend_action("real").await,
                EffectivenessAction::Refine,
                "这批结局应当推 Refine，否则下面量的是别的动作"
            );

            let llm = Arc::new(CountingLlm::default());
            tracker
                .apply_action("real", registry.clone(), Some(llm.clone()), refines)
                .await
                .unwrap();
            calls_by_role.push(llm.calls.load(std::sync::atomic::Ordering::SeqCst));
        }
        assert_eq!(
            calls_by_role,
            vec![0, 1],
            "非写者进程不许花掉这次精炼，写者进程必须花掉"
        );
    }
}
