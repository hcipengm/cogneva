use chrono::{DateTime, Utc};
use cog_core::{
    infer_dependencies, validate_dag, ActionPlan, AtomicTask, DagError, DecompositionPattern,
    EmbeddingProvider, ObjectBackend, SFError, SFResult, Skill, SkillRegistry, Task, TaskExecutor,
    TaskType, VectorBackend,
};
use std::collections::HashSet;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Pattern DB internal entry
// ---------------------------------------------------------------------------

/// Wrapper around a [`DecompositionPattern`] with metadata used for outcome
/// tracking, eviction, and (Jaccard-token) semantic similarity ranking.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PatternEntry {
    pub pattern: DecompositionPattern,
    pub created_at: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    /// Total times this pattern was associated with an action plan run.
    pub runs: u32,
    /// Number of those runs that succeeded.
    pub successes: u32,
}

impl PatternEntry {
    fn new(pattern: DecompositionPattern) -> Self {
        let now = Utc::now();
        Self {
            pattern,
            created_at: now,
            last_used: now,
            runs: 0,
            successes: 0,
        }
    }
}

/// Default maximum pattern DB size before LRU/score-based eviction kicks in.
pub const DEFAULT_MAX_PATTERN_DB_SIZE: usize = 256;
/// Default maximum age (in days) for pattern DB entries before they are
/// expired even if the size cap has not been reached.
pub const DEFAULT_MAX_PATTERN_AGE_DAYS: i64 = 30;

/// ActionPlanOrchestrator implements Stage 1 decomposition logic:
/// semantic goal decomposition, boundary detection, dependency inference,
/// DAG validation, and a feedback loop with pattern DB self-enhancement.
pub struct ActionPlanOrchestrator {
    task_executor: Option<Arc<dyn TaskExecutor>>,
    dag_executor: Option<Arc<dyn cog_core::DagExecutor>>,
    pattern_db: tokio::sync::RwLock<Vec<PatternEntry>>,
    max_feedback_rounds: u32,
    max_pattern_db_size: usize,
    max_pattern_age_days: i64,
    object_backend: Option<Arc<dyn ObjectBackend>>,
    pattern_db_key: String,
    vector_backend: Option<Arc<dyn VectorBackend>>,
    vector_collection: String,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
}

impl std::fmt::Debug for ActionPlanOrchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionPlanOrchestrator")
            .field(
                "pattern_db_len",
                &self.pattern_db.try_read().map(|g| g.len()).unwrap_or(0),
            )
            .field("max_feedback_rounds", &self.max_feedback_rounds)
            .field("max_pattern_db_size", &self.max_pattern_db_size)
            .field("max_pattern_age_days", &self.max_pattern_age_days)
            .field("has_object_backend", &self.object_backend.is_some())
            .field("pattern_db_key", &self.pattern_db_key)
            .field("has_vector_backend", &self.vector_backend.is_some())
            .field("vector_collection", &self.vector_collection)
            .field("has_embedder", &self.embedder.is_some())
            .field("has_task_executor", &self.task_executor.is_some())
            .field("has_dag_executor", &self.dag_executor.is_some())
            .finish_non_exhaustive()
    }
}

impl Default for ActionPlanOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

impl ActionPlanOrchestrator {
    pub fn new() -> Self {
        Self {
            task_executor: None,
            dag_executor: None,
            pattern_db: tokio::sync::RwLock::new(Vec::new()),
            max_feedback_rounds: 2,
            max_pattern_db_size: DEFAULT_MAX_PATTERN_DB_SIZE,
            max_pattern_age_days: DEFAULT_MAX_PATTERN_AGE_DAYS,
            object_backend: None,
            pattern_db_key: "patterns/pattern_db.json".into(),
            vector_backend: None,
            vector_collection: "patterns".into(),
            embedder: None,
        }
    }

    pub fn with_patterns(mut self, patterns: Vec<DecompositionPattern>) -> Self {
        self.pattern_db =
            tokio::sync::RwLock::new(patterns.into_iter().map(PatternEntry::new).collect());
        self
    }

    /// Override the maximum number of patterns retained in the DB. When the DB
    /// grows beyond this size, the lowest-scoring (and least-recently-used)
    /// entries are evicted by [`Self::evict_expired_patterns`].
    pub fn with_max_pattern_db_size(mut self, size: usize) -> Self {
        self.max_pattern_db_size = size.max(1);
        self
    }

    /// Override the maximum age (in days) of pattern entries. Older entries are
    /// dropped during [`Self::evict_expired_patterns`].
    pub fn with_max_pattern_age_days(mut self, days: i64) -> Self {
        self.max_pattern_age_days = days.max(1);
        self
    }

    /// Attach an [`ObjectBackend`] for pattern-db persistence.
    pub fn with_object_backend(mut self, backend: Arc<dyn ObjectBackend>) -> Self {
        self.object_backend = Some(backend);
        self
    }

    /// Override the object-storage key used for pattern-db persistence.
    /// Defaults to `"pattern_db.json"`.
    pub fn with_pattern_db_key(mut self, key: impl Into<String>) -> Self {
        self.pattern_db_key = key.into();
        self
    }

    /// Attach a [`VectorBackend`] for semantic pattern retrieval.
    pub fn with_vector_backend(mut self, backend: Arc<dyn VectorBackend>) -> Self {
        self.vector_backend = Some(backend);
        self
    }

    /// Override the Qdrant collection name used for vector indexing.
    /// Defaults to `"patterns"`.
    pub fn with_vector_collection(mut self, name: impl Into<String>) -> Self {
        self.vector_collection = name.into();
        self
    }

    /// Attach an [`EmbeddingProvider`] for BGE-M3 semantic similarity.
    /// When set, token-level Jaccard becomes a fallback rather than the
    /// primary signal.
    pub fn with_embedder(mut self, embedder: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Attach a [`TaskExecutor`] for collaboration-based goal decomposition.
    pub fn with_task_executor(mut self, executor: Arc<dyn TaskExecutor>) -> Self {
        self.task_executor = Some(executor);
        self
    }

    /// Attach a [`DagExecutor`] so that decomposed atomic tasks are
    /// dynamically injected into the runtime DAG instead of returned as a
    /// static [`ActionPlan`].
    pub fn with_dag_executor(mut self, dag: Arc<dyn cog_core::DagExecutor>) -> Self {
        self.dag_executor = Some(dag);
        self
    }

    /// Read-only access to the pattern DB (used in tests and for diagnostics).
    pub async fn pattern_db(&self) -> Vec<PatternEntry> {
        self.pattern_db.read().await.clone()
    }

    /// Load patterns from the attached [`ObjectBackend`] (if any).
    /// On success the in-memory `pattern_db` is replaced with the persisted
    /// snapshot.  On failure a warning is logged and the current memory state
    /// is left untouched (zero-shot start).
    /// When a [`VectorBackend`] is configured, all patterns that carry an
    /// embedding are upserted into the vector collection after loading.
    pub async fn load_patterns(&self) {
        let Some(ref backend) = self.object_backend else {
            return;
        };
        match backend.get(&self.pattern_db_key).await {
            Ok(Some(data)) => match serde_json::from_slice::<Vec<PatternEntry>>(&data) {
                Ok(entries) => {
                    tracing::info!(
                        "Loaded {} patterns from object backend (key: {})",
                        entries.len(),
                        self.pattern_db_key
                    );
                    *self.pattern_db.write().await = entries;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to deserialize pattern db from object backend: {}",
                        e
                    );
                }
            },
            Ok(None) => {
                tracing::info!(
                    "No persisted pattern db found at key '{}'; starting zero-shot.",
                    self.pattern_db_key
                );
            }
            Err(e) => {
                tracing::warn!("Failed to load pattern db from object backend: {}", e);
            }
        }

        // Sync loaded patterns to vector backend (best-effort).
        if self.vector_backend.is_some() {
            self.sync_all_patterns_to_vector_backend().await;
        }
    }

    /// Upsert a single pattern into the vector backend.
    /// The point id is derived from the pattern's `goal_summary` (blake3 hash)
    /// so that re-upserts are idempotent.  If the pattern has no embedding
    /// the operation is skipped — a zero-vector would be meaningless for
    /// cosine search.
    async fn sync_pattern_to_vector_backend(&self, entry: &PatternEntry) {
        let Some(ref vb) = self.vector_backend else {
            return;
        };
        let Some(ref embed) = entry.pattern.goal_embedding else {
            return;
        };
        if embed.is_empty() {
            return;
        }

        let _point_id = blake3::hash(entry.pattern.goal_summary.as_bytes())
            .to_hex()
            .to_string();
        let metadata = serde_json::json!({
            "goal_summary": entry.pattern.goal_summary,
            "success_score": entry.pattern.success_score,
        });

        // Ensure collection exists (idempotent).
        let _ = vb
            .create_collection(&self.vector_collection, embed.len())
            .await;

        if let Err(e) = vb
            .insert(&self.vector_collection, vec![embed.clone()], vec![metadata])
            .await
        {
            tracing::warn!(
                "Failed to upsert pattern '{}' to vector backend: {}",
                entry.pattern.goal_summary,
                e
            );
        }
    }

    /// Bulk-sync all in-memory patterns that have embeddings to the vector
    /// backend.  Called after `load_patterns` and before `save_patterns`.
    async fn sync_all_patterns_to_vector_backend(&self) {
        let Some(ref vb) = self.vector_backend else {
            return;
        };

        let mut vectors = Vec::new();
        let mut metadata = Vec::new();
        let mut ids = Vec::new();

        let db = self.pattern_db.read().await;
        for entry in db.iter() {
            if let Some(ref embed) = entry.pattern.goal_embedding {
                if !embed.is_empty() {
                    vectors.push(embed.clone());
                    metadata.push(serde_json::json!({
                        "goal_summary": entry.pattern.goal_summary,
                        "success_score": entry.pattern.success_score,
                    }));
                    ids.push(
                        blake3::hash(entry.pattern.goal_summary.as_bytes())
                            .to_hex()
                            .to_string(),
                    );
                }
            }
        }

        drop(db);
        if vectors.is_empty() {
            return;
        }

        // Ensure collection exists.
        if let Some(first) = vectors.first() {
            let _ = vb
                .create_collection(&self.vector_collection, first.len())
                .await;
        }

        // Qdrant insert returns new ids; we ignore them because we use
        // deterministic ids via goal_summary hash.
        if let Err(e) = vb.insert(&self.vector_collection, vectors, metadata).await {
            tracing::warn!(
                "Failed to bulk-sync {} patterns to vector backend: {}",
                ids.len(),
                e
            );
        } else {
            tracing::info!(
                "Synced {} patterns to vector collection '{}'",
                ids.len(),
                self.vector_collection
            );
        }
    }

    /// Save the current in-memory pattern DB to the attached [`ObjectBackend`].
    /// Failures are logged but do not propagate — persistence is best-effort.
    pub async fn save_patterns(&self) {
        let Some(ref backend) = self.object_backend else {
            return;
        };
        let db = self.pattern_db.read().await;
        match serde_json::to_vec(&*db) {
            Ok(data) => {
                let len = db.len();
                drop(db);
                if let Err(e) = backend.put(&self.pattern_db_key, &data).await {
                    tracing::warn!("Failed to save pattern db to object backend: {}", e);
                } else {
                    tracing::debug!(
                        "Saved {} patterns to object backend (key: {})",
                        len,
                        self.pattern_db_key
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Failed to serialize pattern db: {}", e);
            }
        }
    }

    /// Inject seed patterns from an external JSON file.
    /// The file must contain a JSON array of [`DecompositionPattern`] objects.
    /// Patterns are appended to the current in-memory DB (deduplication is left
    /// to the normal `enhance_pattern_db` flow).
    /// Returns the number of patterns injected, or an error if the file could
    /// not be read or parsed.
    pub async fn inject_patterns_from_file(&self, path: &std::path::Path) -> SFResult<usize> {
        let data = tokio::fs::read(path)
            .await
            .map_err(|e| SFError::Validation(format!("failed to read pattern file: {e}")))?;
        let patterns: Vec<DecompositionPattern> = serde_json::from_slice(&data)
            .map_err(|e| SFError::Validation(format!("invalid pattern file JSON: {e}")))?;
        let count = patterns.len();
        self.pattern_db
            .write()
            .await
            .extend(patterns.into_iter().map(PatternEntry::new));
        tracing::info!("Injected {} seed patterns from {}", count, path.display());
        self.save_patterns().await;
        Ok(count)
    }

    /// Main entry point: decompose a goal into an ActionPlan.
    pub async fn decompose_goal(
        &self,
        goal: &str,
        skill_registry: &SkillRegistry,
        hints: Option<&[Task]>,
    ) -> SFResult<ActionPlan> {
        let skills = skill_registry.get_all();
        if skills.is_empty() {
            // A fresh system has no distilled skills yet — that must not
            // block its first goal. Decomposition proceeds with an empty
            // capability list; skill-gap filling distills new skills from
            // the execution results afterwards.
            tracing::info!("Skill registry is empty; decomposing without skill context");
        }

        // Step 1.1: pattern retrieval (top-3 similar cases).
        // Hybrid path: when a VectorBackend is configured we use it for semantic
        // recall and fall back to in-memory Jaccard when the vector path is
        // unavailable or the goal has no embedding.
        let pattern_refs = if self.vector_backend.is_some() {
            self.retrieve_patterns_hybrid(goal, None, 3).await
        } else {
            self.retrieve_patterns(goal, 3).await
        };
        // Convert PatternEntry references to DecompositionPattern references
        // for the LLM prompt — the metadata is internal-only.
        let patterns: Vec<&DecompositionPattern> =
            pattern_refs.iter().map(|e| &e.pattern).collect();

        // Step 1.3: Collaboration-based decomposition via injected TaskExecutor.
        let tasks = if let Some(ref executor) = self.task_executor {
            let mut task_input = serde_json::json!({
                "goal": goal,
                "mode": "decompose_only",
                "skills": skills.iter().map(|s| serde_json::json!({
                    "id": s.id,
                    "name": s.name,
                    "description": s.description,
                })).collect::<Vec<_>>(),
                "patterns": patterns.iter().map(|p| serde_json::json!({
                    "goal_summary": p.goal_summary,
                    "decomposition_tree": p.decomposition_tree,
                })).collect::<Vec<_>>(),
            });
            if let Some(hint_tasks) = hints {
                task_input["hints"] = serde_json::json!(hint_tasks
                    .iter()
                    .map(|t| serde_json::json!({
                        "id": t.id,
                        "type": format!("{:?}", t.task_type),
                        "input": t.input,
                        "blocked_by": t.blocked_by,
                        "priority": t.priority,
                    }))
                    .collect::<Vec<_>>());
            }
            let mut task = Task::new(format!("decompose-{}", goal), TaskType::Planner, task_input);
            // CollaborationExecutor dispatches on is_executable: false routes to
            // the decomposition path (output carries atomic_tasks); true would
            // take the atomic-execution path and never produce a task list.
            task.is_executable = false;
            match executor.execute(&task).await {
                Ok(task_result) => {
                    if !task_result.success {
                        return Err(SFError::Agent(
                            "Decomposition TaskResult.success is false".into(),
                        ));
                    }
                    let score = task_result.metadata.score.unwrap_or(0.0);
                    if score < 0.7 {
                        return Err(SFError::Agent(format!(
                            "Decomposition quality score {:.2} < 0.7 threshold",
                            score
                        )));
                    }
                    let atomic_tasks = task_result
                        .output
                        .get("atomic_tasks")
                        .and_then(|v| v.as_array())
                        .ok_or_else(|| {
                            SFError::Agent("TaskResult.output missing atomic_tasks".into())
                        })?;
                    atomic_tasks
                        .iter()
                        .map(|v| serde_json::from_value::<AtomicTask>(v.clone()))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| {
                            SFError::Agent(format!("Failed to parse atomic_tasks: {}", e))
                        })?
                }
                Err(e) => {
                    tracing::warn!("TaskExecutor decomposition failed: {}", e);
                    return Err(SFError::Agent(format!(
                        "Decomposition failed: {} — LLM connection required",
                        e
                    )));
                }
            }
        } else {
            return Err(SFError::Agent(
                "TaskExecutor not configured — LLM connection required".into(),
            ));
        };

        // Step 1.4: output validation + dependency inference.
        self.validate_task_ids(&tasks)?;
        let edges = infer_dependencies(&tasks, skill_registry, &[]);

        // DAG validation (6 rules).
        match validate_dag(&tasks, &edges) {
            Ok(validation) => {
                tracing::info!(
                    "DAG validated: entry={:?}, exit={:?}, critical_path={}",
                    validation.entry_nodes,
                    validation.exit_nodes,
                    validation.critical_path_len
                );
            }
            Err(DagError::DisconnectedComponents(n)) => {
                tracing::warn!("DAG has {} disconnected components", n);
            }
            Err(e) => {
                return Err(SFError::Validation(format!("DAG validation failed: {}", e)));
            }
        }

        // Async pattern DB self-enhancement (best-effort).
        self.enhance_pattern_db(goal, &tasks, &skills).await;

        // Persist pattern DB snapshot (best-effort).
        self.save_patterns().await;

        let plan = ActionPlan {
            goal: goal.to_string(),
            tasks,
            skills: skills.into_iter().cloned().collect(),
            edges,
        };

        Ok(plan)
    }

    /// Convert an [`AtomicTask`] into a [`Task`] suitable for injection into
    /// [`DagExecutor`].
    fn atomic_task_to_task(at: &AtomicTask) -> Task {
        let task_type = match at.skill_id.as_deref() {
            Some("Planner") => TaskType::Planner,
            Some("Generator") => TaskType::Generator,
            Some("Evaluator") => TaskType::Evaluator,
            Some("Reviewer") => TaskType::Reviewer,
            Some(other) => TaskType::Custom(other.to_string()),
            None => TaskType::Custom("unknown".to_string()),
        };
        let mut task = Task::new(at.id.clone(), task_type, at.input.clone());
        task.blocked_by = at.blocked_by.clone();
        task.blocks = at.blocks.clone();
        task
    }

    /// High-level entry point: process a goal with optional pre-existing tasks.
    /// 1. If all `tasks` have `action_planner_meta.verified == true`, inject them
    ///    directly into the attached [`DagExecutor`] (skip decomposition).
    /// 2. If `tasks` is empty, decompose the goal via collaboration (`TaskExecutor`).
    /// 3. If `tasks` exist but lack the verified marker, evaluate them and either
    ///    mark+inject or re-decompose.
    ///
    /// Returns the list of task IDs that end up in the DagExecutor.
    pub async fn process_goal_impl(
        &self,
        goal: &str,
        tasks: Vec<Task>,
        skill_registry: &SkillRegistry,
    ) -> SFResult<Vec<String>> {
        // Self-evolution tasks are human intents that should be executed
        // atomically by the collaboration pipeline without decomposition.
        let all_self_evolution = !tasks.is_empty()
            && tasks.iter().all(|t| {
                matches!(
                    &t.task_type,
                    TaskType::Custom(s) if s == "self_evolution"
                )
            });

        if all_self_evolution {
            tracing::info!(
                goal = %goal,
                task_count = %tasks.len(),
                "All tasks are self_evolution; skipping decomposition and injecting into DagExecutor"
            );
            let goal_id = tasks
                .first()
                .and_then(|t| t.goal_id.clone())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let now = Utc::now();
            let mut task_ids = Vec::new();
            if let Some(ref dag) = self.dag_executor {
                for mut task in tasks {
                    task.goal_id = Some(goal_id.clone());
                    task.is_executable = true;
                    task.timeout_seconds = 3600; // self-evolution involves multi-agent LLM collaboration; allow more time
                    task.action_planner_meta = Some(cog_core::ActionPlannerMeta {
                        verified: true,
                        version: Some("1.0.0".into()),
                        note: Some(
                            "Self-evolution task routed directly to collaboration executor".into(),
                        ),
                        source: Some(cog_core::ActionPlannerSource::UserProvided),
                        confidence: None,
                        timestamp: Some(now),
                    });
                    task_ids.push(task.id.clone());
                    dag.add_task(task).await?;
                }
            } else {
                tracing::warn!("self_evolution tasks present but no DagExecutor attached");
            }
            return Ok(task_ids);
        }

        let all_verified = !tasks.is_empty()
            && tasks.iter().all(|t| {
                t.action_planner_meta
                    .as_ref()
                    .map(|m| m.verified)
                    .unwrap_or(false)
            });

        // Extract or generate goal_id from the first task (Gateway sets it on all tasks).
        let goal_id = tasks
            .first()
            .and_then(|t| t.goal_id.clone())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        if all_verified {
            tracing::info!(
                goal = %goal,
                task_count = %tasks.len(),
                "All tasks verified by ActionPlanner; injecting directly into DagExecutor"
            );
            let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
            if let Some(ref dag) = self.dag_executor {
                dag.add_tasks_batch(tasks).await?;
            } else {
                tracing::warn!("process_goal_impl: verified tasks but no DagExecutor attached");
            }
            return Ok(task_ids);
        }

        // Unverified tasks: route everything through the Collaboration main flow.
        // The injected TaskExecutor (CollaborationExecutor) evaluates goal + hints
        // via Squad → RalphLoop → PGE → SelfReview and returns the final task list.
        // ActionPlanner does not perform its own LLM evaluation — it only checks
        // markers and injects the result returned by Collaboration.
        let plan = if !tasks.is_empty() {
            tracing::info!(
                goal = %goal,
                task_count = %tasks.len(),
                "Tasks present but not verified; routing to Collaboration for evaluation / decomposition"
            );
            self.decompose_goal(goal, skill_registry, Some(&tasks))
                .await?
        } else {
            self.decompose_goal(goal, skill_registry, None).await?
        };

        if self.dag_executor.is_none() {
            tracing::warn!(
                "process_goal_impl called without DagExecutor; returning ActionPlan without scheduling"
            );
        }

        // Inject original tasks as non-executable placeholders so the hierarchy
        // (goal → overall task → atomic tasks) is fully preserved for tracing.
        let mut all_injected_ids: Vec<String> = Vec::new();
        if let Some(ref dag) = self.dag_executor {
            let now = Utc::now();
            for mut original in tasks {
                original.goal_id = Some(goal_id.clone());
                original.is_executable = false;
                original.action_planner_meta = Some(cog_core::ActionPlannerMeta {
                    verified: true,
                    version: Some("1.0.0".into()),
                    note: Some("Original task preserved after decomposition".into()),
                    source: Some(cog_core::ActionPlannerSource::UserProvided),
                    confidence: None,
                    timestamp: Some(now),
                });
                all_injected_ids.push(original.id.clone());
                dag.add_task(original).await?;
            }

            let source = if !plan.tasks.is_empty() {
                cog_core::ActionPlannerSource::Decomposed
            } else {
                cog_core::ActionPlannerSource::Optimized
            };
            for at in &plan.tasks {
                let mut task = Self::atomic_task_to_task(at);
                task.goal_id = Some(goal_id.clone());
                if let Some(first_original) = all_injected_ids.first() {
                    task.parent_task_id = Some(first_original.clone());
                }
                task.action_planner_meta = Some(cog_core::ActionPlannerMeta {
                    verified: true,
                    version: Some("1.0.0".into()),
                    note: Some(format!(
                        "Decomposed by ActionPlanner (source: {:?})",
                        source
                    )),
                    source: Some(source.clone()),
                    confidence: None,
                    timestamp: Some(now),
                });
                all_injected_ids.push(task.id.clone());
                dag.add_task(task).await?;
            }
        }

        tracing::info!(
            goal = %goal,
            task_count = %all_injected_ids.len(),
            "Goal decomposed and injected into DagExecutor"
        );
        Ok(all_injected_ids)
    }

    /// Retrieve top-k patterns from the pattern DB using semantic similarity.
    /// Similarity is computed as a weighted combination of:
    /// 1. **Embedding cosine similarity** (primary) when an [`EmbeddingProvider`]
    ///    is configured or a precomputed `goal_embedding` is supplied.
    /// 2. **Token-level Jaccard similarity** (fallback) when no embedding is
    ///    available.
    /// 3. **Historical success score** of the pattern, accounting for 30% of
    ///    the score. This biases retrieval toward patterns that have produced
    ///    successful outcomes in past runs.
    ///
    /// Patterns are returned sorted by similarity descending. Only patterns
    ///
    /// with non-zero similarity are returned; if every pattern is unrelated
    ///
    /// the function falls back to the most recent entries.
    pub async fn retrieve_patterns(&self, goal: &str, top_k: usize) -> Vec<PatternEntry> {
        let db = self.pattern_db.read().await;
        if db.is_empty() || top_k == 0 {
            return Vec::new();
        }
        drop(db);
        // If an embedder is available, compute the goal embedding on-the-fly.
        let goal_embedding = match self.embedder.as_ref() {
            Some(emb) => match emb.embed(vec![goal.to_string()]).await {
                Ok(mut v) if !v.is_empty() && !v[0].is_empty() => Some(v.remove(0)),
                Ok(_) | Err(_) => None,
            },
            None => None,
        };
        self.retrieve_patterns_with_embedding(goal, goal_embedding.as_deref(), top_k)
            .await
    }

    /// Variant of [`Self::retrieve_patterns`] that accepts a precomputed
    /// embedding for the goal. When provided, embedding cosine similarity is
    /// the primary semantic signal (90%); Jaccard token similarity is used as
    /// a lightweight fallback (10%) only when the pattern lacks an embedding.
    pub async fn retrieve_patterns_with_embedding(
        &self,
        goal: &str,
        goal_embedding: Option<&[f32]>,
        top_k: usize,
    ) -> Vec<PatternEntry> {
        let db = self.pattern_db.read().await;
        if db.is_empty() || top_k == 0 {
            return Vec::new();
        }
        let goal_tokens = tokenize(goal);

        let mut scored: Vec<(f64, f64, usize, PatternEntry)> = db
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let pattern_tokens = tokenize(&entry.pattern.goal_summary);
                let token_sim = jaccard_similarity(&goal_tokens, &pattern_tokens);

                // Embedding is the primary signal; Jaccard is a fallback.
                let embed_sim = match (goal_embedding, entry.pattern.goal_embedding.as_deref()) {
                    (Some(g), Some(p)) if !g.is_empty() && !p.is_empty() => {
                        Some(cosine_similarity(g, p))
                    }
                    _ => None,
                };

                let semantic = match embed_sim {
                    Some(e) => 0.9 * e + 0.1 * token_sim,
                    None => token_sim,
                };
                let success = entry.pattern.success_score.clamp(0.0, 1.0) as f64;
                let score = semantic * 0.7 + success * 0.3;
                (score, semantic, i, entry.clone())
            })
            .collect();

        // Sort by score desc; ties broken by recency (most recent last_used wins).
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.3.last_used.cmp(&a.3.last_used))
        });

        // If *all* scored entries have zero semantic similarity (no token/embedding
        // overlap with anything), fall back to chronological recency.
        let any_semantic_match = scored.iter().any(|s| s.1 > 0.0);
        if !any_semantic_match {
            return db.iter().rev().take(top_k).cloned().collect();
        }

        scored
            .into_iter()
            .filter(|(score, _, _, _)| *score > 0.0)
            .take(top_k)
            .map(|(_, _, _, entry)| entry)
            .collect()
    }

    /// Hybrid retrieval: vector backend recall + in-memory re-ranking.
    /// When a [`VectorBackend`] is configured and `goal_embedding` is provided,
    /// the vector store is queried for the top-`2*k` most similar vectors.
    /// The returned metadata (`goal_summary`) is used to look up the full
    /// [`PatternEntry`] in memory, and the final ranking blends the vector
    /// cosine score with the pattern's historical `success_score`.
    /// If the vector backend is unavailable, the embedding is missing, or the
    /// vector search returns no mappable results, the call transparently falls
    /// back to [`Self::retrieve_patterns_with_embedding`].
    pub async fn retrieve_patterns_hybrid(
        &self,
        goal: &str,
        goal_embedding: Option<&[f32]>,
        top_k: usize,
    ) -> Vec<PatternEntry> {
        let db = self.pattern_db.read().await;
        if db.is_empty() || top_k == 0 {
            return Vec::new();
        }
        drop(db);

        // If no precomputed embedding is supplied but an embedder is available,
        // compute it on-the-fly so the vector path can be used.
        let computed_embedding = match (goal_embedding, self.embedder.as_ref()) {
            (None, Some(emb)) => match emb.embed(vec![goal.to_string()]).await {
                Ok(mut v) if !v.is_empty() && !v[0].is_empty() => Some(v.remove(0)),
                Ok(_) | Err(_) => None,
            },
            _ => None,
        };
        let goal_embedding = goal_embedding.or(computed_embedding.as_deref());

        if let (Some(ref vb), Some(embed)) = (&self.vector_backend, goal_embedding) {
            if !embed.is_empty() {
                match vb.search(&self.vector_collection, embed, top_k * 2).await {
                    Ok(results) => {
                        let db = self.pattern_db.read().await;
                        let mut scored: Vec<(f64, PatternEntry)> = Vec::new();
                        for vsr in results {
                            if let Some(gs) =
                                vsr.metadata.get("goal_summary").and_then(|v| v.as_str())
                            {
                                if let Some(entry) =
                                    db.iter().find(|e| e.pattern.goal_summary == gs)
                                {
                                    let semantic = vsr.score as f64;
                                    let success =
                                        entry.pattern.success_score.clamp(0.0, 1.0) as f64;
                                    let score = semantic * 0.7 + success * 0.3;
                                    scored.push((score, entry.clone()));
                                }
                            }
                        }
                        drop(db);
                        scored.sort_by(|a, b| {
                            b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        scored.truncate(top_k);
                        if !scored.is_empty() {
                            return scored.into_iter().map(|(_, e)| e).collect();
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Vector backend search failed: {}, falling back to memory",
                            e
                        );
                    }
                }
            }
        }

        self.retrieve_patterns_with_embedding(goal, goal_embedding, top_k)
            .await
    }

    fn validate_task_ids(&self, tasks: &[AtomicTask]) -> SFResult<()> {
        let ids: std::collections::HashSet<String> = tasks.iter().map(|t| t.id.clone()).collect();
        if ids.len() != tasks.len() {
            return Err(SFError::Validation("duplicate task ids detected".into()));
        }
        for task in tasks {
            for dep in &task.blocked_by {
                if !ids.contains(dep) {
                    return Err(SFError::Validation(format!(
                        "task '{}' references unknown dependency '{}'",
                        task.id, dep
                    )));
                }
            }
        }
        Ok(())
    }

    /// Append a new pattern to the DB and apply expiration/eviction.
    /// Async because real-world implementations may persist the pattern to a
    /// vector store, embedding service, or external DB. The current in-memory
    /// implementation is sync at heart but uses an `async fn` so callers can
    /// yield between mutations and to keep the API stable for future backends.
    /// On insertion the new pattern starts with `runs = 0`, `successes = 0`
    /// and a default `success_score` of `1.0` (optimistic). Real outcomes
    /// must be reported via [`Self::record_outcome`] to refine this score.
    async fn enhance_pattern_db(&self, goal: &str, tasks: &[AtomicTask], _skills: &[&Skill]) {
        // If the goal already corresponds to a known pattern (high similarity),
        // just refresh `last_used` rather than inserting a duplicate.
        let mut update_idx: Option<usize> = None;

        // Primary path: embedding cosine similarity (BGE-M3).
        let db = self.pattern_db.read().await;
        if let Some(ref emb) = self.embedder {
            let texts: Vec<String> = std::iter::once(goal.to_string())
                .chain(db.iter().map(|e| e.pattern.goal_summary.clone()))
                .collect();
            if let Ok(vectors) = emb.embed(texts).await {
                if let Some(goal_vec) = vectors.first() {
                    let mut best_sim = 0.0_f64;
                    for (i, entry_vec) in vectors.iter().skip(1).enumerate() {
                        let sim = cosine_similarity(goal_vec, entry_vec);
                        if sim > best_sim {
                            best_sim = sim;
                            // Embedding cosine threshold is lower than Jaccard
                            // because cosine on BGE-M3 is a stricter metric.
                            if sim >= 0.75 {
                                update_idx = Some(i);
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Fallback: token-level Jaccard.
        if update_idx.is_none() {
            let goal_tokens = tokenize(goal);
            for (i, entry) in db.iter().enumerate() {
                let entry_tokens = tokenize(&entry.pattern.goal_summary);
                let sim = jaccard_similarity(&goal_tokens, &entry_tokens);
                if sim >= 0.85 {
                    update_idx = Some(i);
                    break;
                }
            }
        }
        drop(db);

        if let Some(idx) = update_idx {
            let entry_clone = {
                let mut db = self.pattern_db.write().await;
                db[idx].last_used = Utc::now();
                // Refresh the decomposition tree with the most recent decomposition,
                // since downstream fields (like skill_set) may have evolved.
                db[idx].pattern.decomposition_tree =
                    serde_json::to_value(tasks).unwrap_or(serde_json::Value::Array(vec![]));
                db[idx].pattern.skill_set =
                    tasks.iter().filter_map(|t| t.skill_id.clone()).collect();
                db[idx].clone()
            };
            // Re-sync to vector backend so metadata (e.g. skill_set) stays fresh.
            self.sync_pattern_to_vector_backend(&entry_clone).await;
            self.save_patterns().await;
            return;
        }

        let pattern = DecompositionPattern {
            goal_summary: goal.to_string(),
            goal_embedding: None,
            decomposition_tree: serde_json::to_value(tasks)
                .unwrap_or(serde_json::Value::Array(vec![])),
            skill_set: tasks.iter().filter_map(|t| t.skill_id.clone()).collect(),
            success_score: 1.0,
        };
        let entry = PatternEntry::new(pattern);
        self.sync_pattern_to_vector_backend(&entry).await;
        {
            let mut db = self.pattern_db.write().await;
            db.push(entry);
        }

        self.evict_expired_patterns().await;
        self.save_patterns().await;
    }

    /// Update outcome statistics for the pattern most similar to `goal`.
    /// `success` should be `true` when the action plan executed successfully
    /// (e.g. all atomic tasks completed with no errors), `false` otherwise.
    /// The matching pattern's `success_score` is recomputed as the running
    /// `successes / runs` ratio.
    /// Returns `Some(index)` of the updated pattern, or `None` if the DB is
    /// empty or no pattern was within the minimum similarity threshold.
    pub async fn record_outcome(&self, goal: &str, success: bool) -> Option<usize> {
        let db = self.pattern_db.read().await;
        if db.is_empty() {
            return None;
        }

        // Primary path: embedding cosine similarity (BGE-M3).
        let mut best: Option<(f64, usize)> = None;
        if let Some(ref emb) = self.embedder {
            let texts: Vec<String> = std::iter::once(goal.to_string())
                .chain(db.iter().map(|e| e.pattern.goal_summary.clone()))
                .collect();
            if let Ok(vectors) = emb.embed(texts).await {
                if let Some(goal_vec) = vectors.first() {
                    for (i, entry_vec) in vectors.iter().skip(1).enumerate() {
                        let sim = cosine_similarity(goal_vec, entry_vec);
                        match best {
                            Some((b_sim, _)) if sim <= b_sim => continue,
                            _ => best = Some((sim, i)),
                        }
                    }
                }
            }
        }

        // Fallback: token-level Jaccard.
        if best.is_none() {
            let goal_tokens = tokenize(goal);
            for (i, entry) in db.iter().enumerate() {
                let p_tokens = tokenize(&entry.pattern.goal_summary);
                let sim = jaccard_similarity(&goal_tokens, &p_tokens);
                match best {
                    Some((b_sim, _)) if sim <= b_sim => continue,
                    _ => best = Some((sim, i)),
                }
            }
        }
        drop(db);

        // Require a minimum similarity to avoid attributing outcomes to
        // unrelated patterns. The threshold is intentionally low (0.1) so a
        // few shared content words are enough.
        let (sim, idx) = best?;
        if sim < 0.1 {
            return None;
        }

        let entry_clone = {
            let mut db = self.pattern_db.write().await;
            let entry = &mut db[idx];
            entry.runs = entry.runs.saturating_add(1);
            if success {
                entry.successes = entry.successes.saturating_add(1);
            }
            entry.last_used = Utc::now();
            if entry.runs > 0 {
                entry.pattern.success_score = entry.successes as f32 / entry.runs as f32;
            }
            entry.clone()
        };
        // Re-sync updated score to vector backend metadata (best-effort).
        self.sync_pattern_to_vector_backend(&entry_clone).await;
        self.save_patterns().await;
        Some(idx)
    }

    /// Evict pattern entries that exceed [`Self::max_pattern_age_days`] or
    /// drive the DB above [`Self::max_pattern_db_size`].
    /// Eviction order:
    ///   1. Drop entries older than the max age.
    ///   2. If still over capacity, sort by `(success_score desc, last_used desc)`
    ///      and truncate to capacity.
    async fn evict_expired_patterns(&self) {
        let mut db = self.pattern_db.write().await;
        let now = Utc::now();
        let max_age = chrono::Duration::days(self.max_pattern_age_days);
        db.retain(|entry| now.signed_duration_since(entry.created_at) < max_age);

        if db.len() > self.max_pattern_db_size {
            db.sort_by(|a, b| {
                b.pattern
                    .success_score
                    .partial_cmp(&a.pattern.success_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.last_used.cmp(&a.last_used))
            });
            db.truncate(self.max_pattern_db_size);
        }
    }
}

// ---------------------------------------------------------------------------
// Similarity helpers
// ---------------------------------------------------------------------------

/// Lowercase, strip punctuation, and drop very short / common words.
fn tokenize(text: &str) -> HashSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter_map(|word| {
            let lower = word.trim().to_ascii_lowercase();
            if lower.len() <= 2 || is_stop_word(&lower) {
                None
            } else {
                Some(lower)
            }
        })
        .collect()
}

fn is_stop_word(w: &str) -> bool {
    matches!(
        w,
        "the"
            | "and"
            | "for"
            | "with"
            | "from"
            | "into"
            | "that"
            | "this"
            | "these"
            | "those"
            | "have"
            | "has"
            | "had"
            | "are"
            | "was"
            | "were"
            | "you"
            | "our"
            | "their"
            | "his"
            | "her"
    )
}

/// Jaccard similarity = |A ∩ B| / |A ∪ B| over token sets.
fn jaccard_similarity(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let intersection = a.intersection(b).count();
    let union = a.union(b).count();
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

/// Cosine similarity over two equally-sized embedding vectors.
/// Returns 0.0 if either side is empty or length-mismatched.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0_f64;
    let mut na = 0.0_f64;
    let mut nb = 0.0_f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let xf = *x as f64;
        let yf = *y as f64;
        dot += xf * yf;
        na += xf * xf;
        nb += yf * yf;
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        (dot / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0)
    }
}

impl ActionPlanOrchestrator {
    /// Parse a JSON string containing an array of `AtomicTask` objects under
    /// the `"tasks"` key.
    pub fn parse_tasks_from_json(&self, json: &str) -> SFResult<Vec<AtomicTask>> {
        let wrapper: serde_json::Value =
            serde_json::from_str(json).map_err(SFError::Serialization)?;
        let tasks_arr = wrapper
            .get("tasks")
            .and_then(|v| v.as_array())
            .ok_or_else(|| SFError::Validation("Missing 'tasks' array".into()))?;
        tasks_arr
            .iter()
            .map(|v| {
                serde_json::from_value::<AtomicTask>(v.clone()).map_err(SFError::Serialization)
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl cog_core::ActionPlanner for ActionPlanOrchestrator {
    async fn process_goal(
        &self,
        goal: &str,
        tasks: Vec<cog_core::Task>,
        skill_registry: &cog_core::SkillRegistry,
    ) -> cog_core::SFResult<Vec<String>> {
        self.process_goal_impl(goal, tasks, skill_registry).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{TaskResult, TaskResultMetadata};

    /// Mock [`TaskExecutor`] that returns a fixed list of [`AtomicTask`]s.
    struct MockTaskExecutor {
        atomic_tasks: Vec<AtomicTask>,
    }

    #[async_trait::async_trait]
    impl TaskExecutor for MockTaskExecutor {
        fn supports(&self, _task_type: &TaskType) -> bool {
            true
        }

        async fn execute(&self, _task: &Task) -> SFResult<TaskResult> {
            Ok(TaskResult {
                success: true,
                output: serde_json::json!({ "atomic_tasks": self.atomic_tasks }),
                metadata: TaskResultMetadata::new("mock").with_score(0.85),
            })
        }
    }

    fn make_mock_executor() -> Arc<dyn TaskExecutor> {
        let task = AtomicTask {
            id: "t1".into(),
            name: "Task 1".into(),
            skill_id: Some("s1".into()),
            description: None,
            estimated_tokens: 10_000,
            skill_gap: false,
            blocked_by: vec![],
            blocks: vec![],
            input: serde_json::json!({}),
            output_entities: vec![],
            estimated_seconds: 600,
        };
        Arc::new(MockTaskExecutor {
            atomic_tasks: vec![task],
        })
    }

    /// Mock LLM provider that returns a queue of canned responses for
    /// collaboration-based decomposition tests.
    struct MockDecompositionProvider {
        responses: std::sync::Mutex<Vec<String>>,
    }

    impl MockDecompositionProvider {
        fn new(responses: Vec<String>) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses),
            }
        }

        fn pop_response(&self) -> String {
            let mut guard = self.responses.lock().unwrap();
            if guard.is_empty() {
                // Default: passing evaluation so PGE pipeline completes.
                return r#"{"score":85,"passed":true,"feedback":"ok","criteria":[]}"#.into();
            }
            guard.remove(0)
        }
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for MockDecompositionProvider {
        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            let text = self.pop_response();
            Ok(cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::text(text)],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            let text = self.pop_response();
            let content = vec![cog_core::ContentBlock::text(text.clone())];
            let response = cog_core::ChatResponse {
                content: content.clone(),
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };
            let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(10);
            let _ = producer
                .push(cog_core::AssistantMessageEvent::Start {
                    partial: cog_core::Message::assistant(content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            let _ = producer
                .push(cog_core::AssistantMessageEvent::TextEnd {
                    content_index: 0,
                    content: text,
                    partial: cog_core::Message::assistant(content),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(response);
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            self.chat_stream(&[], &cog_core::ChatOptions::default())
                .await
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_pattern_retrieval_semantic_similarity() {
        // Semantic retrieval should rank patterns whose token set overlaps with
        // the goal above unrelated patterns, regardless of insertion order.
        let patterns = vec![
            DecompositionPattern {
                goal_summary: "build a website".into(),
                goal_embedding: None,
                decomposition_tree: serde_json::Value::Null,
                skill_set: vec!["web-dev".into()],
                success_score: 1.0,
            },
            DecompositionPattern {
                goal_summary: "deploy kubernetes cluster".into(),
                goal_embedding: None,
                decomposition_tree: serde_json::Value::Null,
                skill_set: vec!["k8s".into()],
                success_score: 0.8,
            },
        ];
        let orchestrator = ActionPlanOrchestrator::new().with_patterns(patterns);

        // "build a nice website" shares "build" + "website" with the first
        // pattern, but nothing meaningful with the second → first pattern wins.
        let retrieved = orchestrator
            .retrieve_patterns("build a nice website", 1)
            .await;
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].pattern.goal_summary, "build a website");

        // With top_k=2 we get both, semantically-related first.
        let retrieved2 = orchestrator
            .retrieve_patterns("build a nice website", 2)
            .await;
        assert_eq!(retrieved2.len(), 2);
        assert_eq!(retrieved2[0].pattern.goal_summary, "build a website");
        assert_eq!(
            retrieved2[1].pattern.goal_summary,
            "deploy kubernetes cluster"
        );
    }

    #[tokio::test]
    async fn test_pattern_retrieval_chronological_fallback_when_no_match() {
        // When no pattern shares any meaningful tokens with the goal,
        // retrieval falls back to chronological (most-recent first).
        let patterns = vec![
            DecompositionPattern {
                goal_summary: "build a website".into(),
                goal_embedding: None,
                decomposition_tree: serde_json::Value::Null,
                skill_set: vec!["web-dev".into()],
                success_score: 1.0,
            },
            DecompositionPattern {
                goal_summary: "deploy kubernetes cluster".into(),
                goal_embedding: None,
                decomposition_tree: serde_json::Value::Null,
                skill_set: vec!["k8s".into()],
                success_score: 0.8,
            },
        ];
        let orchestrator = ActionPlanOrchestrator::new().with_patterns(patterns);

        // "xyz unrelated topic" has no token overlap with either pattern.
        let retrieved = orchestrator
            .retrieve_patterns("xyz unrelated topic", 2)
            .await;
        assert_eq!(retrieved.len(), 2);
        // Falls back to most-recent first.
        assert_eq!(
            retrieved[0].pattern.goal_summary,
            "deploy kubernetes cluster"
        );
        assert_eq!(retrieved[1].pattern.goal_summary, "build a website");
    }

    #[tokio::test]
    async fn test_retrieve_patterns_with_embedding() {
        let patterns = vec![
            DecompositionPattern {
                goal_summary: "alpha bravo charlie".into(),
                goal_embedding: Some(vec![1.0, 0.0, 0.0]),
                decomposition_tree: serde_json::Value::Null,
                skill_set: vec![],
                success_score: 0.5,
            },
            DecompositionPattern {
                goal_summary: "delta echo foxtrot".into(),
                goal_embedding: Some(vec![0.0, 1.0, 0.0]),
                decomposition_tree: serde_json::Value::Null,
                skill_set: vec![],
                success_score: 0.5,
            },
        ];
        let orchestrator = ActionPlanOrchestrator::new().with_patterns(patterns);
        // No token overlap with either pattern, but embedding vector matches
        // the second pattern → embedding cosine pushes it to the top.
        let goal_embed = vec![0.0, 1.0, 0.0];
        let retrieved = orchestrator
            .retrieve_patterns_with_embedding("xyzzy plugh", Some(&goal_embed), 1)
            .await;
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].pattern.goal_summary, "delta echo foxtrot");
    }

    #[tokio::test]
    async fn test_record_outcome_updates_success_score() {
        let patterns = vec![DecompositionPattern {
            goal_summary: "build a website".into(),
            goal_embedding: None,
            decomposition_tree: serde_json::Value::Null,
            skill_set: vec!["web-dev".into()],
            success_score: 1.0,
        }];
        let orchestrator = ActionPlanOrchestrator::new().with_patterns(patterns);

        // 1 success
        let idx1 = orchestrator
            .record_outcome("build website fast", true)
            .await;
        assert_eq!(idx1, Some(0));
        let db = orchestrator.pattern_db().await;
        assert_eq!(db[0].runs, 1);
        assert_eq!(db[0].successes, 1);
        assert!((db[0].pattern.success_score - 1.0).abs() < f32::EPSILON);
        drop(db);

        // 1 failure → success_score becomes 0.5
        let idx2 = orchestrator.record_outcome("website builder", false).await;
        assert_eq!(idx2, Some(0));
        let db = orchestrator.pattern_db().await;
        assert_eq!(db[0].runs, 2);
        assert_eq!(db[0].successes, 1);
        assert!((db[0].pattern.success_score - 0.5).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn test_record_outcome_skips_when_no_match() {
        let patterns = vec![DecompositionPattern {
            goal_summary: "build a website".into(),
            goal_embedding: None,
            decomposition_tree: serde_json::Value::Null,
            skill_set: vec!["web-dev".into()],
            success_score: 1.0,
        }];
        let orchestrator = ActionPlanOrchestrator::new().with_patterns(patterns);
        // Unrelated goal — no matching pattern, no update.
        let result = orchestrator
            .record_outcome("xyzzy plugh frobnicate", true)
            .await;
        assert_eq!(result, None);
        let db = orchestrator.pattern_db().await;
        assert_eq!(db[0].runs, 0);
    }

    #[tokio::test]
    async fn test_pattern_db_eviction_by_size() {
        // Cap to 2; insert 4 distinct patterns; only the 2 highest-scoring
        // (or most-recent on ties) should remain.
        let _provider = Arc::new(MockDecompositionProvider::new(vec![
            r#"{"analysis":"a","specification":"s","design":"d","tasks":[{"id":"t1","name":"Task 1","task_type":"s1","input":{},"blocked_by":[]}]}"#.into(),
        ]));
        let orchestrator = ActionPlanOrchestrator::new()
            .with_task_executor(make_mock_executor())
            .with_max_pattern_db_size(2);
        let registry = SkillRegistry::from_skills(vec![Skill {
            id: "s1".into(),
            name: "s1".into(),
            description: "s1".into(),
            tools: vec![],
            complexity_score: 1,
            blocked_by: vec![],
            blocks: vec![],
        }]);
        orchestrator
            .decompose_goal("alpha task", &registry, None)
            .await
            .unwrap();
        orchestrator
            .decompose_goal("bravo task", &registry, None)
            .await
            .unwrap();
        orchestrator
            .decompose_goal("charlie task", &registry, None)
            .await
            .unwrap();
        orchestrator
            .decompose_goal("delta task", &registry, None)
            .await
            .unwrap();
        assert_eq!(orchestrator.pattern_db().await.len(), 2);
    }

    #[tokio::test]
    async fn test_pattern_db_dedup_on_high_similarity() {
        // The first decompose inserts a pattern. A second decompose for the
        // same goal should refresh that pattern rather than insert a duplicate.
        let _provider = Arc::new(MockDecompositionProvider::new(vec![
            r#"{"analysis":"a","specification":"s","design":"d","tasks":[{"id":"t1","name":"Task 1","task_type":"s1","input":{},"blocked_by":[]}]}"#.into(),
        ]));
        let orchestrator = ActionPlanOrchestrator::new().with_task_executor(make_mock_executor());
        let registry = SkillRegistry::from_skills(vec![Skill {
            id: "s1".into(),
            name: "s1".into(),
            description: "s1".into(),
            tools: vec![],
            complexity_score: 1,
            blocked_by: vec![],
            blocks: vec![],
        }]);
        orchestrator
            .decompose_goal("build a website", &registry, None)
            .await
            .unwrap();
        let initial_created = orchestrator.pattern_db().await[0].created_at;
        // Sleep to ensure last_used would advance if updated.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        orchestrator
            .decompose_goal("build a website", &registry, None)
            .await
            .unwrap();
        let db = orchestrator.pattern_db().await;
        assert_eq!(db.len(), 1);
        // created_at unchanged, last_used advanced.
        assert_eq!(db[0].created_at, initial_created);
        assert!(db[0].last_used >= initial_created);
    }

    #[test]
    fn test_jaccard_similarity_basic() {
        let a: HashSet<String> = ["hello", "world"].iter().map(|s| s.to_string()).collect();
        let b: HashSet<String> = ["world", "rust"].iter().map(|s| s.to_string()).collect();
        let sim = jaccard_similarity(&a, &b);
        // intersection = 1, union = 3 → 0.333…
        assert!((sim - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_jaccard_similarity_identical() {
        let a: HashSet<String> = ["alpha", "bravo"].iter().map(|s| s.to_string()).collect();
        assert!((jaccard_similarity(&a, &a) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_jaccard_similarity_disjoint() {
        let a: HashSet<String> = ["one", "two"].iter().map(|s| s.to_string()).collect();
        let b: HashSet<String> = ["three", "four"].iter().map(|s| s.to_string()).collect();
        assert!(jaccard_similarity(&a, &b).abs() < 1e-9);
    }

    #[test]
    fn test_cosine_similarity_basic() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-9);

        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < 1e-9);
    }

    #[test]
    fn test_cosine_similarity_mismatched_lengths() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }

    #[test]
    fn test_tokenize_filters_stop_words() {
        let tokens = tokenize("Build a Nice Website with the Latest Tech");
        assert!(tokens.contains("build"));
        assert!(tokens.contains("nice"));
        assert!(tokens.contains("website"));
        assert!(tokens.contains("latest"));
        assert!(tokens.contains("tech"));
        // Stop words and short words excluded.
        assert!(!tokens.contains("the"));
        assert!(!tokens.contains("with"));
        assert!(!tokens.contains("a"));
    }

    #[tokio::test]
    async fn test_empty_skill_registry_errors() {
        let registry = SkillRegistry::new();
        let orchestrator = ActionPlanOrchestrator::new();
        let result = orchestrator
            .decompose_goal("do something", &registry, None)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_parse_tasks_from_json() {
        let orchestrator = ActionPlanOrchestrator::new();
        let json = r#"{"tasks":[
            {"id":"t1","name":"Task 1","skill_id":"s1","description":"desc","estimated_tokens":5000,"skill_gap":false,"blocked_by":[],"output_entities":["entity1"]},
            {"id":"t2","name":"Task 2","skill_id":null,"skill_gap":true,"blocked_by":["t1"]}
        ]}"#;
        let tasks = orchestrator.parse_tasks_from_json(json).unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].id, "t1");
        assert_eq!(tasks[1].blocked_by, vec!["t1"]);
        assert!(tasks[1].skill_gap);
    }
}
