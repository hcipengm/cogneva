use cog_llm::{
    model::{Model, ModelCost, Provider},
    ApiType, Usage,
};

fn test_model_for(provider: Provider) -> Model {
    match provider {
        Provider::OpenAI => Model {
            id: "gpt-4o".into(),
            name: "GPT-4o".into(),
            api: ApiType::OpenAICompletions,
            provider,
            base_url: "https://api.openai.com/v1".into(),
            context_window: 128_000,
            max_tokens: 4096,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: true,
            supports_reasoning: false,
            cost: ModelCost {
                input: 2.5,
                output: 10.0,
                cache_read: 1.25,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
        Provider::Anthropic => Model {
            id: "claude-sonnet-4-7".into(),
            name: "Claude Sonnet 4.7".into(),
            api: ApiType::AnthropicMessages,
            provider,
            base_url: "https://api.anthropic.com".into(),
            context_window: 200_000,
            max_tokens: 4096,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: true,
            supports_reasoning: true,
            cost: ModelCost {
                input: 3.0,
                output: 15.0,
                cache_read: 0.0,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
        Provider::Google => Model {
            id: "gemini-2.5-pro".into(),
            name: "Gemini 2.5 Pro".into(),
            api: ApiType::GoogleGenerativeAI,
            provider,
            base_url: "https://generativelanguage.googleapis.com/v1beta".into(),
            context_window: 1_000_000,
            max_tokens: 8192,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: true,
            supports_reasoning: true,
            cost: ModelCost {
                input: 1.25,
                output: 10.0,
                cache_read: 0.0,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
        Provider::Ollama => Model {
            id: "llama3.1".into(),
            name: "Llama 3.1".into(),
            api: ApiType::OllamaChat,
            provider,
            base_url: "http://localhost:11434".into(),
            context_window: 128_000,
            max_tokens: 4096,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: false,
            supports_reasoning: false,
            cost: ModelCost {
                input: 0.0,
                output: 0.0,
                cache_read: 0.0,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
    }
}

#[test]
fn test_calculate_cost_openai_gpt4o() {
    let model = test_model_for(Provider::OpenAI);
    let usage = Usage {
        input: 1000,
        output: 500,
        cache_read: 200,
        cache_write: 0,
        total_tokens: 1700,
        cost: Default::default(),
    };

    let cost = cog_llm::model::calculate_cost(&model, &usage);

    // GPT-4o: input=$2.5/M, output=$10/M, cache_read=$1.25/M
    let expected_input = (2.5 / 1_000_000.0) * 1000.0;
    let expected_output = (10.0 / 1_000_000.0) * 500.0;
    let expected_cache_read = (1.25 / 1_000_000.0) * 200.0;
    let expected_total = expected_input + expected_output + expected_cache_read;

    assert!(
        (cost.input - expected_input).abs() < 1e-9,
        "input cost mismatch: got {}, expected {}",
        cost.input,
        expected_input
    );
    assert!(
        (cost.output - expected_output).abs() < 1e-9,
        "output cost mismatch: got {}, expected {}",
        cost.output,
        expected_output
    );
    assert!(
        (cost.cache_read - expected_cache_read).abs() < 1e-9,
        "cache_read cost mismatch: got {}, expected {}",
        cost.cache_read,
        expected_cache_read
    );
    assert!(
        (cost.total - expected_total).abs() < 1e-9,
        "total cost mismatch: got {}, expected {}",
        cost.total,
        expected_total
    );
}

#[test]
fn test_calculate_cost_anthropic_claude() {
    let model = test_model_for(Provider::Anthropic);
    let usage = Usage {
        input: 2000,
        output: 1000,
        cache_read: 0,
        cache_write: 0,
        total_tokens: 3000,
        cost: Default::default(),
    };

    let cost = cog_llm::model::calculate_cost(&model, &usage);

    // Claude Sonnet: input=$3.0/M, output=$15/M
    let expected_input = (3.0 / 1_000_000.0) * 2000.0;
    let expected_output = (15.0 / 1_000_000.0) * 1000.0;
    let expected_total = expected_input + expected_output;

    assert!((cost.input - expected_input).abs() < 1e-9);
    assert!((cost.output - expected_output).abs() < 1e-9);
    assert!((cost.total - expected_total).abs() < 1e-9);
}

#[test]
fn test_calculate_cost_google_gemini() {
    let model = test_model_for(Provider::Google);
    let usage = Usage {
        input: 4000,
        output: 2000,
        cache_read: 0,
        cache_write: 0,
        total_tokens: 6000,
        cost: Default::default(),
    };

    let cost = cog_llm::model::calculate_cost(&model, &usage);

    // Gemini 2.5 Pro: input=$1.25/M, output=$10/M
    let expected_input = (1.25 / 1_000_000.0) * 4000.0;
    let expected_output = (10.0 / 1_000_000.0) * 2000.0;
    let expected_total = expected_input + expected_output;

    assert!((cost.input - expected_input).abs() < 1e-9);
    assert!((cost.output - expected_output).abs() < 1e-9);
    assert!((cost.total - expected_total).abs() < 1e-9);
}

#[test]
fn test_calculate_cost_ollama_free() {
    let model = test_model_for(Provider::Ollama);
    let usage = Usage {
        input: 10000,
        output: 5000,
        cache_read: 0,
        cache_write: 0,
        total_tokens: 15000,
        cost: Default::default(),
    };

    let cost = cog_llm::model::calculate_cost(&model, &usage);

    // Ollama: all costs are 0.0
    assert_eq!(cost.input, 0.0);
    assert_eq!(cost.output, 0.0);
    assert_eq!(cost.cache_read, 0.0);
    assert_eq!(cost.cache_write, 0.0);
    assert_eq!(cost.total, 0.0);
}

#[test]
fn test_calculate_cost_zero_usage() {
    let model = test_model_for(Provider::OpenAI);
    let usage = Usage {
        input: 0,
        output: 0,
        cache_read: 0,
        cache_write: 0,
        total_tokens: 0,
        cost: Default::default(),
    };

    let cost = cog_llm::model::calculate_cost(&model, &usage);

    assert_eq!(cost.input, 0.0);
    assert_eq!(cost.output, 0.0);
    assert_eq!(cost.cache_read, 0.0);
    assert_eq!(cost.cache_write, 0.0);
    assert_eq!(cost.total, 0.0);
}

#[test]
fn test_calculate_cost_custom_model() {
    let model = Model {
        id: "custom-model".into(),
        name: "Custom Model".into(),
        api: cog_llm::ApiType::OpenAICompletions,
        provider: Provider::OpenAI,
        base_url: "https://api.example.com".into(),
        context_window: 128_000,
        max_tokens: 4096,
        supports_tools: true,
        supports_streaming: true,
        supports_vision: false,
        supports_reasoning: false,
        cost: ModelCost {
            input: 5.0,
            output: 20.0,
            cache_read: 2.5,
            cache_write: 1.0,
        },
        headers: Default::default(),
    };

    let usage = Usage {
        input: 1000,
        output: 500,
        cache_read: 100,
        cache_write: 50,
        total_tokens: 1650,
        cost: Default::default(),
    };

    let cost = cog_llm::model::calculate_cost(&model, &usage);

    let expected_input = (5.0 / 1_000_000.0) * 1000.0;
    let expected_output = (20.0 / 1_000_000.0) * 500.0;
    let expected_cache_read = (2.5 / 1_000_000.0) * 100.0;
    let expected_cache_write = (1.0 / 1_000_000.0) * 50.0;
    let expected_total =
        expected_input + expected_output + expected_cache_read + expected_cache_write;

    assert!((cost.input - expected_input).abs() < 1e-9);
    assert!((cost.output - expected_output).abs() < 1e-9);
    assert!((cost.cache_read - expected_cache_read).abs() < 1e-9);
    assert!((cost.cache_write - expected_cache_write).abs() < 1e-9);
    assert!((cost.total - expected_total).abs() < 1e-9);
}
