use cog_agent::ToolRegistry;
use cog_core::ToolRegistry as _;

#[tokio::test]
async fn test_tool_registry_empty() {
    let registry = ToolRegistry::new();
    assert!(registry.is_empty());
    assert!(registry.get("nonexistent").is_none());
}

#[tokio::test]
async fn test_tool_registry_builtins() {
    let registry = ToolRegistry::new();
    registry.register(cog_agent::tools::builtins::read_file());
    registry.register(cog_agent::tools::builtins::write_file());
    registry.register(cog_agent::tools::builtins::run_command());
    registry.register(cog_agent::tools::builtins::search_code());

    assert!(!registry.is_empty());
    assert_eq!(registry.list().len(), 4);
    assert!(registry.names().contains(&"read_file".to_string()));
    assert!(registry.names().contains(&"write_file".to_string()));
    assert!(registry.names().contains(&"run_command".to_string()));
    assert!(registry.names().contains(&"search_code".to_string()));
}

#[tokio::test]
async fn test_tool_registry_execute_not_found() {
    let registry = ToolRegistry::new();
    let result = registry.execute("nonexistent", serde_json::json!({})).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_tool_registry_dynamic_registration_visible_across_clones() {
    let registry_a = ToolRegistry::new();
    let registry_b = registry_a.clone();

    registry_a.register(cog_agent::tools::builtins::read_file());

    assert!(registry_b.get("read_file").is_some());
    assert_eq!(registry_b.list().len(), 1);

    // Register on B should also be visible on A (shared state).
    registry_b.register(cog_agent::tools::builtins::write_file());
    assert_eq!(registry_a.list().len(), 2);
}

#[tokio::test]
async fn test_tool_registry_register_replaces_existing() {
    let registry = ToolRegistry::new();
    registry.register(cog_agent::tools::builtins::read_file());
    registry.register(cog_agent::tools::builtins::read_file());
    assert_eq!(registry.list().len(), 1);
}
