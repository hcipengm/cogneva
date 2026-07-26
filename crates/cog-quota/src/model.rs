use std::collections::HashMap;

/// Supported task types with associated weight multipliers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskType {
    CodeGeneration,
    CodeReview,
    TextSummary,
    DataAnalysis,
    SimpleQA,
    Translation,
}

impl TaskType {
    /// Returns the cost weight multiplier for this task type.
    pub fn weight(&self) -> f64 {
        match self {
            TaskType::CodeGeneration => 1.5,
            TaskType::CodeReview => 1.3,
            TaskType::TextSummary => 1.0,
            TaskType::DataAnalysis => 1.2,
            TaskType::SimpleQA => 0.8,
            TaskType::Translation => 0.9,
        }
    }
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TaskType::CodeGeneration => "code_generation",
            TaskType::CodeReview => "code_review",
            TaskType::TextSummary => "text_summary",
            TaskType::DataAnalysis => "data_analysis",
            TaskType::SimpleQA => "simple_qa",
            TaskType::Translation => "translation",
        };
        write!(f, "{}", s)
    }
}

impl std::str::FromStr for TaskType {
    type Err = crate::QuotaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "code_generation" => Ok(TaskType::CodeGeneration),
            "code_review" => Ok(TaskType::CodeReview),
            "text_summary" => Ok(TaskType::TextSummary),
            "data_analysis" => Ok(TaskType::DataAnalysis),
            "simple_qa" => Ok(TaskType::SimpleQA),
            "translation" => Ok(TaskType::Translation),
            _ => Err(crate::QuotaError::InvalidTaskType(s.to_string())),
        }
    }
}

/// Configuration for a single LLM model including pricing.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelConfig {
    pub name: String,
    /// Input token price per 1K tokens.
    pub input_price: f64,
    /// Output token price per 1K tokens.
    pub output_price: f64,
    /// Maximum context window in tokens.
    pub context_window: u64,
    /// Currency code, e.g. "USD".
    pub currency: String,
}

impl ModelConfig {
    pub fn new(
        name: impl Into<String>,
        input_price: f64,
        output_price: f64,
        context_window: u64,
        currency: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            input_price,
            output_price,
            context_window,
            currency: currency.into(),
        }
    }

    /// Calculate estimated cost for given input and output tokens.
    pub fn estimate_cost(&self, input_tokens: u64, output_tokens: u64) -> f64 {
        let input_cost = (input_tokens as f64 / 1000.0) * self.input_price;
        let output_cost = (output_tokens as f64 / 1000.0) * self.output_price;
        input_cost + output_cost
    }
}

/// Registry of available models with lookup helpers.
#[derive(Debug, Clone)]
pub struct ModelRegistry {
    models: HashMap<String, ModelConfig>,
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelRegistry {
    pub fn new() -> Self {
        let mut models = HashMap::new();

        models.insert(
            "kimi-k1.5".to_string(),
            ModelConfig::new("kimi-k1.5", 0.012, 0.048, 128_000, "USD"),
        );
        models.insert(
            "deepseek-v3".to_string(),
            ModelConfig::new("deepseek-v3", 0.002, 0.008, 64_000, "USD"),
        );
        models.insert(
            "qwen-2.5-72b".to_string(),
            ModelConfig::new("qwen-2.5-72b", 0.004, 0.012, 128_000, "USD"),
        );
        models.insert(
            "gpt-4o".to_string(),
            ModelConfig::new("gpt-4o", 0.036, 0.108, 128_000, "USD"),
        );

        Self { models }
    }

    /// Look up a model by name.
    pub fn get(&self, name: &str) -> Option<&ModelConfig> {
        self.models.get(name)
    }

    /// List all registered models.
    pub fn list(&self) -> Vec<&ModelConfig> {
        self.models.values().collect()
    }

    /// Return the default model for a given task type.
    /// Chooses based on task characteristics.
    pub fn get_default_for_task(&self, task_type: TaskType) -> Option<&ModelConfig> {
        match task_type {
            TaskType::CodeGeneration => self.get("deepseek-v3"),
            TaskType::CodeReview => self.get("qwen-2.5-72b"),
            TaskType::TextSummary => self.get("kimi-k1.5"),
            TaskType::DataAnalysis => self.get("gpt-4o"),
            TaskType::SimpleQA => self.get("deepseek-v3"),
            TaskType::Translation => self.get("qwen-2.5-72b"),
        }
    }
}
