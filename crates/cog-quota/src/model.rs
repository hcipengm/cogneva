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
    /// An empty registry: this deployment's price list is the operator's to
    /// declare, not a library's to guess.
    ///
    /// It used to be seeded with four vendor models and their list prices. Two
    /// things were wrong with that. The numbers were never this deployment's
    /// contract -- a price read off a vendor's page is a guess about a bill that
    /// arrives in a different currency, with different cache rates and different
    /// negotiated terms -- and the names were vendor names, which the platform
    /// keeps out of everything except the gateway's own upstream list. Nothing
    /// in production ever called it, so the table only ever answered a question
    /// nobody asked, with numbers nobody checked.
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
        }
    }

    /// Add or replace the price entry for one model.
    pub fn register(&mut self, model: ModelConfig) {
        self.models.insert(model.name.clone(), model);
    }

    /// Look up a model by name.
    pub fn get(&self, name: &str) -> Option<&ModelConfig> {
        self.models.get(name)
    }

    /// List all registered models.
    pub fn list(&self) -> Vec<&ModelConfig> {
        self.models.values().collect()
    }
}
