/// Explainability (AI decision tracking).
/// - Stores AI decision chains, confidence scores, reasoning
/// - Persisted in PostgreSQL JSONB (via pg.factory)
/// - Supports querying by session/task/agent/decision_type/time
///   **Human layer**: Explainability Service API for debugging AI behavior.
///   **Agent layer**: Decision records embedded in Snapshot for replay.
///   **Machine layer**: Structured data consumed by downstream analytics.
use crate::ExplainabilityRecord;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Explainability storage backend trait.
/// Phase 1: in-memory implementation.
/// Phase 2: PostgreSQL JSONB backend via pg.factory.
#[async_trait::async_trait]
pub trait ExplainabilityBackend: Send + Sync {
    async fn insert(&self, record: ExplainabilityRecord) -> anyhow::Result<()>;
    async fn query_by_task(&self, task_id: &str) -> anyhow::Result<Vec<ExplainabilityRecord>>;
    async fn query_by_agent(&self, agent_id: &str) -> anyhow::Result<Vec<ExplainabilityRecord>>;
    async fn query_by_decision_type(
        &self,
        decision_type: &str,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>>;
    async fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>>;
    async fn query_by_session(&self, session_id: &str)
        -> anyhow::Result<Vec<ExplainabilityRecord>>;
    async fn get_by_id(&self, record_id: &str) -> anyhow::Result<Option<ExplainabilityRecord>>;
    async fn count_by_task(&self, task_id: &str) -> anyhow::Result<usize>;
    async fn aggregate_confidence_by_model(&self) -> anyhow::Result<HashMap<String, f64>>;
}

/// In-memory explainability store for Phase 1.
/// Thread-safe via RwLock.  Auto-evicts oldest records when capacity
/// is exceeded, preserving the most recent decision history.
pub struct ExplainabilityStore {
    records: std::sync::RwLock<Vec<ExplainabilityRecord>>,
    capacity: usize,
}

impl ExplainabilityStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            records: std::sync::RwLock::new(Vec::with_capacity(capacity)),
            capacity,
        }
    }

    pub fn insert(&self, record: ExplainabilityRecord) {
        let mut recs = self.records.write().unwrap();
        if recs.len() >= self.capacity {
            recs.remove(0);
        }
        recs.push(record);
    }

    pub fn query_by_task(&self, task_id: &str) -> Vec<ExplainabilityRecord> {
        let recs = self.records.read().unwrap();
        recs.iter()
            .filter(|r| r.task_id.as_deref() == Some(task_id))
            .cloned()
            .collect()
    }

    pub fn query_by_agent(&self, agent_id: &str) -> Vec<ExplainabilityRecord> {
        let recs = self.records.read().unwrap();
        recs.iter()
            .filter(|r| r.agent_id.as_deref() == Some(agent_id))
            .cloned()
            .collect()
    }

    pub fn query_by_session(&self, session_id: &str) -> Vec<ExplainabilityRecord> {
        let recs = self.records.read().unwrap();
        recs.iter()
            .filter(|r| r.session_id.as_deref() == Some(session_id))
            .cloned()
            .collect()
    }

    pub fn query_by_decision_type(&self, decision_type: &str) -> Vec<ExplainabilityRecord> {
        let recs = self.records.read().unwrap();
        recs.iter()
            .filter(|r| r.decision_type == decision_type)
            .cloned()
            .collect()
    }

    pub fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<ExplainabilityRecord> {
        let recs = self.records.read().unwrap();
        recs.iter()
            .filter(|r| r.timestamp >= start && r.timestamp <= end)
            .cloned()
            .collect()
    }

    pub fn get_by_id(&self, record_id: &str) -> Option<ExplainabilityRecord> {
        let recs = self.records.read().unwrap();
        recs.iter().find(|r| r.record_id == record_id).cloned()
    }

    pub fn count_by_task(&self, task_id: &str) -> usize {
        let recs = self.records.read().unwrap();
        recs.iter()
            .filter(|r| r.task_id.as_deref() == Some(task_id))
            .count()
    }

    pub fn aggregate_confidence_by_model(&self) -> HashMap<String, f64> {
        let recs = self.records.read().unwrap();
        let mut sums: HashMap<String, f64> = HashMap::new();
        let mut counts: HashMap<String, u64> = HashMap::new();

        for r in recs.iter() {
            if r.confidence > 0.0 {
                *sums.entry(r.model.clone()).or_insert(0.0) += r.confidence;
                *counts.entry(r.model.clone()).or_insert(0) += 1;
            }
        }

        sums.into_iter()
            .map(|(model, sum)| {
                let count = counts.get(&model).copied().unwrap_or(1).max(1) as f64;
                (model, sum / count)
            })
            .collect()
    }

    pub fn all_records(&self) -> Vec<ExplainabilityRecord> {
        self.records.read().unwrap().clone()
    }

    pub fn len(&self) -> usize {
        self.records.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.read().unwrap().is_empty()
    }
}

impl Default for ExplainabilityStore {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[async_trait::async_trait]
impl ExplainabilityBackend for ExplainabilityStore {
    async fn insert(&self, record: ExplainabilityRecord) -> anyhow::Result<()> {
        self.insert(record);
        Ok(())
    }

    async fn query_by_task(&self, task_id: &str) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        Ok(self.query_by_task(task_id))
    }

    async fn query_by_agent(&self, agent_id: &str) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        Ok(self.query_by_agent(agent_id))
    }

    async fn query_by_decision_type(
        &self,
        decision_type: &str,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        Ok(self.query_by_decision_type(decision_type))
    }

    async fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        Ok(self.query_by_time_range(start, end))
    }

    async fn query_by_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        Ok(self.query_by_session(session_id))
    }

    async fn get_by_id(&self, record_id: &str) -> anyhow::Result<Option<ExplainabilityRecord>> {
        Ok(self.get_by_id(record_id))
    }

    async fn count_by_task(&self, task_id: &str) -> anyhow::Result<usize> {
        Ok(self.count_by_task(task_id))
    }

    async fn aggregate_confidence_by_model(&self) -> anyhow::Result<HashMap<String, f64>> {
        Ok(self.aggregate_confidence_by_model())
    }
}

/// Builder for explainability records.
/// # Example
/// ```rust,ignore
/// use cog_observability::explainability::ExplainabilityRecordBuilder;
/// let rec = ExplainabilityRecordBuilder::new("plan_generation")
///     .task_id("task-123")
///     .agent_id("agent-456")
///     .confidence(0.92)
///     .reasoning_step("Analyzed user intent")
///     .reasoning_step("Decomposed into sub-tasks")
///     .model("gpt-4")
///     .tokens(150, 320)
///     .metadata("temperature", serde_json::json!(0.7))
///     .build();
/// ```
pub struct ExplainabilityRecordBuilder {
    record: ExplainabilityRecord,
}

impl ExplainabilityRecordBuilder {
    pub fn new(decision_type: impl Into<String>) -> Self {
        Self {
            record: ExplainabilityRecord {
                record_id: uuid::Uuid::new_v4().to_string(),
                session_id: None,
                task_id: None,
                agent_id: None,
                decision_type: decision_type.into(),
                confidence: 0.0,
                reasoning_chain: Vec::new(),
                model: String::new(),
                input_tokens: 0,
                output_tokens: 0,
                metadata: HashMap::new(),
                timestamp: Utc::now(),
            },
        }
    }

    pub fn session_id(mut self, id: impl Into<String>) -> Self {
        self.record.session_id = Some(id.into());
        self
    }

    pub fn task_id(mut self, id: impl Into<String>) -> Self {
        self.record.task_id = Some(id.into());
        self
    }

    pub fn agent_id(mut self, id: impl Into<String>) -> Self {
        self.record.agent_id = Some(id.into());
        self
    }

    pub fn confidence(mut self, c: f64) -> Self {
        self.record.confidence = c.clamp(0.0, 1.0);
        self
    }

    pub fn reasoning_step(mut self, step: impl Into<String>) -> Self {
        self.record.reasoning_chain.push(step.into());
        self
    }

    pub fn reasoning_chain(mut self, steps: Vec<String>) -> Self {
        self.record.reasoning_chain = steps;
        self
    }

    pub fn model(mut self, m: impl Into<String>) -> Self {
        self.record.model = m.into();
        self
    }

    pub fn tokens(mut self, input: u64, output: u64) -> Self {
        self.record.input_tokens = input;
        self.record.output_tokens = output;
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.record.metadata.insert(key.into(), value);
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.record.timestamp = ts;
        self
    }

    pub fn build(self) -> ExplainabilityRecord {
        self.record
    }
}

/// Decision type taxonomy aligned with design doc.
/// These categories map to the 5 AI design dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionType {
    PlanGeneration,
    SkillSelection,
    ToolExecution,
    StateTransition,
    SelfReview,
    ConsensusReached,
    ConsensusRejected,
}

impl DecisionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DecisionType::PlanGeneration => "plan_generation",
            DecisionType::SkillSelection => "skill_selection",
            DecisionType::ToolExecution => "tool_execution",
            DecisionType::StateTransition => "state_transition",
            DecisionType::SelfReview => "self_review",
            DecisionType::ConsensusReached => "consensus_reached",
            DecisionType::ConsensusRejected => "consensus_rejected",
        }
    }
}
