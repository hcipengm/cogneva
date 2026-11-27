//! 网关与反射侧读口的普查：每个读口落到哪个存储/服务调用上。
//!
//! 这一层自己不读表（那道门在 `the_read_surface_layer_holds_no_sql_test.rs`）；每一口的答案
//! 来自它调用的那个读口。`delegates` 记的是**函数体里逐字存在的那个调用表达式**——门判它还在
//! 那个函数体里，所以搬走/改名会红，而不是让读者照着一份过时的清单去找。

/// 一个读口：函数名、文件、它落到的那个调用。
pub struct ReadSurface {
    pub function: &'static str,
    pub path: &'static str,
    /// 函数体里逐字存在的那个调用（例如 `state.llm_usage.priced_spend_between(`）；
    /// `none` 表示这个读口不调用任何东西（只在内存里算）。
    pub delegates: &'static str,
}

/// 全部读口。门会逐个函数核对。
pub const READ_SURFACES: &[ReadSurface] = &[
    ReadSurface {
        function: "get",
        path: "crates/cog-gateway/src/action_plan.rs",
        delegates: "store.hget(",
    },
    ReadSurface {
        function: "list",
        path: "crates/cog-gateway/src/action_plan.rs",
        delegates: "store.hvals(",
    },
    ReadSurface {
        function: "get_action_plan_handler",
        path: "crates/cog-gateway/src/action_plan.rs",
        delegates: "state.action_plan_store.get(",
    },
    ReadSurface {
        function: "list_action_plans_handler",
        path: "crates/cog-gateway/src/action_plan.rs",
        delegates: "state.action_plan_store.list(",
    },
    ReadSurface {
        function: "read_bound_session",
        path: "crates/cog-gateway/src/agents.rs",
        delegates: "none",
    },
    ReadSurface {
        function: "read_bound_user",
        path: "crates/cog-gateway/src/agents.rs",
        delegates: "none",
    },
    ReadSurface {
        function: "read_people",
        path: "crates/cog-gateway/src/agents.rs",
        delegates: "kv.hgetall(",
    },
    ReadSurface {
        function: "read_roster",
        path: "crates/cog-gateway/src/agents.rs",
        delegates: "kv.hgetall(",
    },
    ReadSurface {
        function: "get_handler",
        path: "crates/cog-gateway/src/agents.rs",
        delegates: "state.agent_registry.as_ref(",
    },
    ReadSurface {
        function: "list_handler",
        path: "crates/cog-gateway/src/agents.rs",
        delegates: "state.agent_registry.as_ref(",
    },
    ReadSurface {
        function: "list_audit_events_handler",
        path: "crates/cog-gateway/src/audit.rs",
        delegates: "query.agent_id.as_deref(",
    },
    ReadSurface {
        function: "list_audit_stream_handler",
        path: "crates/cog-gateway/src/audit.rs",
        delegates: "e.action.starts_with(",
    },
    ReadSurface {
        function: "get_me_handler",
        path: "crates/cog-gateway/src/auth/mod.rs",
        delegates: "state.user_store.as_ref(",
    },
    ReadSurface {
        function: "list_handler",
        path: "crates/cog-gateway/src/backup_admin.rs",
        delegates: "entries.flatten(",
    },
    ReadSurface {
        function: "read",
        path: "crates/cog-gateway/src/capabilities.rs",
        delegates: "state.tool_registry.as_ref(",
    },
    ReadSurface {
        function: "list_handler",
        path: "crates/cog-gateway/src/datasets.rs",
        delegates: "store.versions(",
    },
    ReadSurface {
        function: "get_handler",
        path: "crates/cog-gateway/src/datasets.rs",
        delegates: "store.find(",
    },
    ReadSurface {
        function: "list_eval_reports_handler",
        path: "crates/cog-gateway/src/eval_reports.rs",
        delegates: "dir.display(",
    },
    ReadSurface {
        function: "list_changes_handler",
        path: "crates/cog-gateway/src/evolution.rs",
        delegates: "admin.list_changes(",
    },
    ReadSurface {
        function: "get_file_handler",
        path: "crates/cog-gateway/src/files.rs",
        delegates: "backend.exists(",
    },
    ReadSurface {
        function: "list_hooks_handler",
        path: "crates/cog-gateway/src/hooks.rs",
        delegates: "state.hook_engine.as_ref(",
    },
    ReadSurface {
        function: "list_keys_handler",
        path: "crates/cog-gateway/src/keys_api.rs",
        delegates: "state.security_keys.clone(",
    },
    ReadSurface {
        function: "list_schema_handler",
        path: "crates/cog-gateway/src/memory.rs",
        delegates: "state.memory_backend.as_ref(",
    },
    ReadSurface {
        function: "list_summary_handler",
        path: "crates/cog-gateway/src/memory.rs",
        delegates: "state.memory_backend.as_ref(",
    },
    ReadSurface {
        function: "list_raw_handler",
        path: "crates/cog-gateway/src/memory.rs",
        delegates: "state.memory_backend.as_ref(",
    },
    ReadSurface {
        function: "get_raw_handler",
        path: "crates/cog-gateway/src/memory.rs",
        delegates: "state.memory_backend.as_ref(",
    },
    ReadSurface {
        function: "get_raw_content_handler",
        path: "crates/cog-gateway/src/memory.rs",
        delegates: "state.memory_backend.as_ref(",
    },
    ReadSurface {
        function: "get_raw_url_handler",
        path: "crates/cog-gateway/src/memory.rs",
        delegates: "state.memory_backend.as_ref(",
    },
    ReadSurface {
        function: "list_notifications_handler",
        path: "crates/cog-gateway/src/notifications.rs",
        delegates: "state.config.read(",
    },
    ReadSurface {
        function: "read",
        path: "crates/cog-gateway/src/output.rs",
        delegates: "state.evolution_admin.as_ref(",
    },
    ReadSurface {
        function: "read_progress",
        path: "crates/cog-gateway/src/progress.rs",
        delegates: "state.orchestrator.as_ref(",
    },
    ReadSurface {
        function: "list_projects_handler",
        path: "crates/cog-gateway/src/projects.rs",
        delegates: "claims.as_ref(",
    },
    ReadSurface {
        function: "list_raw_logs_handler",
        path: "crates/cog-gateway/src/raw_logs.rs",
        delegates: "store.query(",
    },
    ReadSurface {
        function: "list_sessions_handler",
        path: "crates/cog-gateway/src/sessions.rs",
        delegates: "state.chat_turns.as_ref(",
    },
    ReadSurface {
        function: "get_session_handler",
        path: "crates/cog-gateway/src/sessions.rs",
        delegates: "none",
    },
    ReadSurface {
        function: "list_messages_handler",
        path: "crates/cog-gateway/src/sessions.rs",
        delegates: "state.chat_turns.as_ref(",
    },
    ReadSurface {
        function: "get_task_handler",
        path: "crates/cog-gateway/src/tasks.rs",
        delegates: "state.orchestrator.get_task_checked(",
    },
    ReadSurface {
        function: "list_tasks_handler",
        path: "crates/cog-gateway/src/tasks.rs",
        delegates: "state.orchestrator.get_all_tasks_checked(",
    },
    ReadSurface {
        function: "get_ready_tasks_handler",
        path: "crates/cog-gateway/src/tasks.rs",
        delegates: "state.orchestrator.get_ready_tasks_checked(",
    },
    ReadSurface {
        function: "get_task_dependents_handler",
        path: "crates/cog-gateway/src/tasks.rs",
        delegates: "state.orchestrator.get_dependents_checked(",
    },
    ReadSurface {
        function: "get_task_dependencies_handler",
        path: "crates/cog-gateway/src/tasks.rs",
        delegates: "state.orchestrator.get_dependencies_checked(",
    },
    ReadSurface {
        function: "get_task_graph_handler",
        path: "crates/cog-gateway/src/tasks.rs",
        delegates: "state.orchestrator.get_graph_checked(",
    },
    ReadSurface {
        function: "list_connections",
        path: "crates/cog-gateway/src/websocket_protocol.rs",
        delegates: "self.connections.read(",
    },
    ReadSurface {
        function: "get_missed_event_ids",
        path: "crates/cog-gateway/src/websocket_protocol.rs",
        delegates: "self.event_cache.read(",
    },
    ReadSurface {
        function: "list_handler",
        path: "crates/cog-gateway/src/wiki.rs",
        delegates: "state.wiki_adapter.as_ref(",
    },
    ReadSurface {
        function: "get_document_handler",
        path: "crates/cog-gateway/src/wiki.rs",
        delegates: "state.wiki_adapter.as_ref(",
    },
    ReadSurface {
        function: "read",
        path: "crates/cog-gateway/src/workbench.rs",
        delegates: "state.llm_usage.as_ref(",
    },
    ReadSurface {
        function: "list_workspaces_handler",
        path: "crates/cog-gateway/src/workspaces.rs",
        delegates: "store.read(",
    },
    ReadSurface {
        function: "get_workspace_handler",
        path: "crates/cog-gateway/src/workspaces.rs",
        delegates: "store.read(",
    },
    ReadSurface {
        function: "list_workspace_members_handler",
        path: "crates/cog-gateway/src/workspaces.rs",
        delegates: "store.read(",
    },
    ReadSurface {
        function: "list_promoted",
        path: "crates/cog-reflection/src/baseline_port.rs",
        delegates: "out.lines(",
    },
    ReadSurface {
        function: "read_parent_pod",
        path: "crates/cog-reflection/src/change_execution.rs",
        delegates: "none",
    },
    ReadSurface {
        function: "read",
        path: "crates/cog-reflection/src/change_execution.rs",
        delegates: "config.kubectl_bin.clone(",
    },
    ReadSurface {
        function: "get_result",
        path: "crates/cog-reflection/src/evolution.rs",
        delegates: "self.results.lock(",
    },
    ReadSurface {
        function: "list_results",
        path: "crates/cog-reflection/src/evolution.rs",
        delegates: "self.results.lock(",
    },
    ReadSurface {
        function: "read_base_deployments",
        path: "crates/cog-reflection/src/live_revision.rs",
        delegates: "o.lines(",
    },
    ReadSurface {
        function: "load",
        path: "crates/cog-reflection/src/meta_learning.rs",
        delegates: "self.path.display(",
    },
    ReadSurface {
        function: "load_active",
        path: "crates/cog-reflection/src/policy_store.rs",
        delegates: "self.declared_active_version(",
    },
    ReadSurface {
        function: "load_version",
        path: "crates/cog-reflection/src/policy_store.rs",
        delegates: "self.version_path(",
    },
    ReadSurface {
        function: "list_versions",
        path: "crates/cog-reflection/src/policy_store.rs",
        delegates: "self.policy_dir(",
    },
    ReadSurface {
        function: "get",
        path: "crates/cog-reflection/src/registry_footprint.rs",
        delegates: "accept.is_empty(",
    },
    ReadSurface {
        function: "list",
        path: "crates/cog-reflection/src/workspace.rs",
        delegates: "self.git_bare(",
    },
];
