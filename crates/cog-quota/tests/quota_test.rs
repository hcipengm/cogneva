use cog_quota::{BillingRecord, ModelConfig, ModelRegistry, QuotaMetrics, TaskType};

/// 新建的价目表是空的：价目由本部署的运维声明，不由库猜。
///
/// 这条替代的是原先那张写死四个厂商型号与标价的表。它没有任何生产消费者，
/// 却在那里回答「这个型号多少钱」——读的人没法知道那是一次真实报价还是一次
/// 抄来的数字。空表至少诚实地说明「还没人声明」。
#[test]
fn a_fresh_registry_declares_no_price() {
    let registry = ModelRegistry::new();
    assert!(registry.list().is_empty(), "空表才是没声明过的形状");
    assert!(registry.get("gpt-4o").is_none());
    assert!(registry.get("deepseek-v3").is_none());
}

#[test]
fn a_registry_holds_what_the_operator_registers() {
    let mut registry = ModelRegistry::new();
    registry.register(ModelConfig::new(
        "upstream-a/model-x",
        0.01,
        0.03,
        100_000,
        "USD",
    ));

    let model = registry
        .get("upstream-a/model-x")
        .expect("声明过的型号要读得回来");
    assert_eq!(model.input_price, 0.01);
    assert_eq!(model.context_window, 100_000);
    assert_eq!(registry.list().len(), 1);
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
