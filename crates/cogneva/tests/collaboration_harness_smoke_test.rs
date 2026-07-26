use common::harness::TestHarness;
use std::time::Duration;

mod common;

#[tokio::test]
async fn harness_executes_squad_in_pipeline_mode_and_succeeds() {
    let harness = TestHarness::builder()
        .with_mock_llm()
        .with_mock_state_backend()
        .with_hook_engine()
        .with_squad_executor()
        .build();

    let task_id = "task:hello-world".to_string();
    let config = TestHarness::squad_config_for_goal("test goal: implement hello world");

    let result = harness
        .execute_squad(&task_id, config, Duration::from_secs(10))
        .await
        .expect("squad should complete within timeout");

    assert!(result.success, "squad should succeed");
}
