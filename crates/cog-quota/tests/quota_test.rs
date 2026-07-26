use cog_quota::{BillingRecord, ModelConfig, ModelRegistry, QuotaMetrics, TaskType};

#[test]
fn test_model_registry_lookup() {
    let registry = ModelRegistry::new();

    let kimi = registry.get("kimi-k1.5").expect("kimi-k1.5 should exist");
    assert_eq!(kimi.input_price, 0.012);
    assert_eq!(kimi.output_price, 0.048);
    assert_eq!(kimi.context_window, 128_000);

    let deepseek = registry
        .get("deepseek-v3")
        .expect("deepseek-v3 should exist");
    assert_eq!(deepseek.input_price, 0.002);
    assert_eq!(deepseek.context_window, 64_000);

    let qwen = registry
        .get("qwen-2.5-72b")
        .expect("qwen-2.5-72b should exist");
    assert_eq!(qwen.input_price, 0.004);

    let gpt4o = registry.get("gpt-4o").expect("gpt-4o should exist");
    assert_eq!(gpt4o.input_price, 0.036);
    assert_eq!(gpt4o.output_price, 0.108);

    // List should return all 4 models
    let list = registry.list();
    assert_eq!(list.len(), 4);

    // Unknown model
    assert!(registry.get("unknown-model").is_none());
}

#[test]
fn test_model_registry_default_for_task() {
    let registry = ModelRegistry::new();

    assert!(registry
        .get_default_for_task(TaskType::CodeGeneration)
        .is_some());
    assert!(registry
        .get_default_for_task(TaskType::CodeReview)
        .is_some());
    assert!(registry
        .get_default_for_task(TaskType::TextSummary)
        .is_some());
    assert!(registry
        .get_default_for_task(TaskType::DataAnalysis)
        .is_some());
    assert!(registry.get_default_for_task(TaskType::SimpleQA).is_some());
    assert!(registry
        .get_default_for_task(TaskType::Translation)
        .is_some());
}

#[test]
fn test_model_estimate_cost() {
    let model = ModelConfig::new("test-model", 0.01, 0.03, 100_000, "USD");

    // 1000 input tokens at 0.01 per 1K = 0.01
    // 500 output tokens at 0.03 per 1K = 0.015
    let cost = model.estimate_cost(1000, 500);
    assert!((cost - 0.025).abs() < f64::EPSILON * 100.0);
}

#[test]
fn test_task_type_weights() {
    assert!((TaskType::CodeGeneration.weight() - 1.5).abs() < f64::EPSILON);
    assert!((TaskType::CodeReview.weight() - 1.3).abs() < f64::EPSILON);
    assert!((TaskType::TextSummary.weight() - 1.0).abs() < f64::EPSILON);
    assert!((TaskType::DataAnalysis.weight() - 1.2).abs() < f64::EPSILON);
    assert!((TaskType::SimpleQA.weight() - 0.8).abs() < f64::EPSILON);
    assert!((TaskType::Translation.weight() - 0.9).abs() < f64::EPSILON);
}

#[test]
fn test_task_type_display_and_parse() {
    let cases = vec![
        (TaskType::CodeGeneration, "code_generation"),
        (TaskType::CodeReview, "code_review"),
        (TaskType::TextSummary, "text_summary"),
        (TaskType::DataAnalysis, "data_analysis"),
        (TaskType::SimpleQA, "simple_qa"),
        (TaskType::Translation, "translation"),
    ];

    for (task_type, expected) in cases {
        assert_eq!(task_type.to_string(), expected);
        let parsed: TaskType = expected.parse().expect("should parse");
        assert_eq!(parsed, task_type);
    }
}

#[test]
fn test_billing_cost_calculation_with_weights() {
    let record = BillingRecord::new(
        "user-1",
        "task-1",
        "gpt-4o",
        TaskType::CodeGeneration,
        1000,
        500,
        0.1, // cost_before_weight
    );

    // CodeGeneration weight = 1.5
    assert!((record.cost - 0.15).abs() < f64::EPSILON * 100.0);
    assert!((record.weight_applied - 1.5).abs() < f64::EPSILON);
    assert_eq!(record.cost_before_weight, 0.1);

    let record2 = BillingRecord::new(
        "user-1",
        "task-2",
        "deepseek-v3",
        TaskType::SimpleQA,
        2000,
        1000,
        0.05,
    );

    // SimpleQA weight = 0.8
    assert!((record2.cost - 0.04).abs() < f64::EPSILON * 100.0);
    assert!((record2.weight_applied - 0.8).abs() < f64::EPSILON);
}

#[test]
fn test_billing_record_with_workspace() {
    let record = BillingRecord::new(
        "user-1",
        "task-1",
        "gpt-4o",
        TaskType::DataAnalysis,
        1000,
        500,
        0.1,
    )
    .with_workspace_id("ws-1");

    assert_eq!(record.workspace_id, Some("ws-1".to_string()));
}

#[test]
fn test_metrics_registration() {
    let metrics = QuotaMetrics::new().expect("should create metrics");

    // Record some values
    metrics.record_quota_used("user-1", 1000);
    metrics.set_quota_remaining("user-1", 5000);
    metrics.record_quota_exceeded("user-2");
    metrics.set_workspace_quota_used("ws-1", 2000);
    metrics.observe_task_cost("gpt-4o", 0.5);

    // Collect should produce non-empty output
    let output = metrics.collect().expect("should collect metrics");
    assert!(!output.is_empty());
    assert!(output.contains("quota_used_total"));
    assert!(output.contains("quota_remaining"));
    assert!(output.contains("quota_exceeded_total"));
    assert!(output.contains("workspace_quota_used"));
    assert!(output.contains("task_token_cost"));
}
