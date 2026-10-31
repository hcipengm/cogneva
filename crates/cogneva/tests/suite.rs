//! The assembly's test suite, as one binary.
//!
//! Two hundred files used to be two hundred test binaries. Each one linked the
//! whole assembly again -- seventy-seven megabytes of debug info and about
//! forty-nine seconds of linking -- so a change anywhere in the workspace paid
//! that cost two hundred times, and `target/debug/deps` held fifty gigabytes of
//! near-identical executables. A test file is a module: pulled in here by path,
//! every one of them runs in the same process and the assembly is linked once.
//!
//! What a file may not be:
//!
//! - one that writes a process-global environment variable, because a variable
//!   set by one file's test is visible to another's running at the same time;
//! - one carrying an `#[ignore]`d case, because `--ignored` is how CI runs those
//!   against a live database and a filter over the whole suite would pick up
//!   every other file's ignored case with it.
//!
//! What the merge cost, and what a file added here now pays: one compile error
//! anywhere in the tree stops every contract in it from running locally. Two
//! hundred binaries failed one at a time; one binary fails all at once. Adding a
//! file to `tests/` is therefore adding it to everyone's local verification, not
//! just to your own -- run `cargo test -p cogneva --test suite --no-run` before
//! leaving it broken.
//!
//! Both are read from the file, not listed: `the_suite_and_the_directory_agree`
//! fails when a file on disk is neither here nor justified by one of the two
//! properties above, and when a line here names a file that is gone.

#[path = "common/cells_written.rs"]
pub mod cells_written;
#[path = "common/closed_set.rs"]
pub mod closed_set;
#[path = "common/mod.rs"]
pub mod common;
#[path = "common/dashboard.rs"]
pub mod dashboard;
#[path = "common/derivation.rs"]
pub mod derivation;
#[path = "mock_provider.rs"]
pub mod mock_provider;
#[path = "common/producer.rs"]
pub mod producer;
#[path = "common/promql.rs"]
pub mod promql;
#[path = "router_state/mod.rs"]
pub mod router_state;
#[path = "common/unread.rs"]
pub mod unread;

#[path = "a2a_advertised_paths_are_served.rs"]
mod a2a_advertised_paths_are_served;
#[path = "a2a_rpc_round_trip.rs"]
mod a2a_rpc_round_trip;
#[path = "a_job_inner_wait_budget_widens_the_job_deadline_test.rs"]
mod a_job_inner_wait_budget_widens_the_job_deadline_test;
#[path = "a_label_name_is_the_same_wherever_it_is_declared_test.rs"]
mod a_label_name_is_the_same_wherever_it_is_declared_test;
#[path = "a_node_capacity_probe_is_written_once_test.rs"]
mod a_node_capacity_probe_is_written_once_test;
#[path = "a_relation_reading_is_one_query_test.rs"]
mod a_relation_reading_is_one_query_test;
#[path = "a_retention_knob_says_what_zero_means_test.rs"]
mod a_retention_knob_says_what_zero_means_test;
#[path = "a_shared_query_is_spelt_once_test.rs"]
mod a_shared_query_is_spelt_once_test;
#[path = "a_successful_write_rings_the_bell.rs"]
mod a_successful_write_rings_the_bell;
#[path = "a_waiting_reason_is_declared_once_test.rs"]
mod a_waiting_reason_is_declared_once_test;
#[path = "a_word_written_out_is_the_word_read_back_test.rs"]
mod a_word_written_out_is_the_word_read_back_test;
#[path = "agent_consumer_test.rs"]
mod agent_consumer_test;
#[path = "agent_context_test.rs"]
mod agent_context_test;
#[path = "agent_context_window_test.rs"]
mod agent_context_window_test;
#[path = "agent_conversation_is_recorded.rs"]
mod agent_conversation_is_recorded;
#[path = "agent_event_variants_are_constructed_somewhere_test.rs"]
mod agent_event_variants_are_constructed_somewhere_test;
#[path = "agent_lifecycle_test.rs"]
mod agent_lifecycle_test;
#[path = "agent_name_is_one_object_test.rs"]
mod agent_name_is_one_object_test;
#[path = "agent_retirement_test.rs"]
mod agent_retirement_test;
#[path = "agent_runtime_test.rs"]
mod agent_runtime_test;
#[path = "agent_snapshot_test.rs"]
mod agent_snapshot_test;
#[path = "agent_tools_test.rs"]
mod agent_tools_test;
#[path = "agent_wal_test.rs"]
mod agent_wal_test;
#[path = "agent_wrapper_test.rs"]
mod agent_wrapper_test;
#[path = "an_identifier_is_quoted_in_one_place_test.rs"]
mod an_identifier_is_quoted_in_one_place_test;
#[path = "anonymous_requests_cannot_tell_paths_apart.rs"]
mod anonymous_requests_cannot_tell_paths_apart;
#[path = "app_config_substitutions_contract_test.rs"]
mod app_config_substitutions_contract_test;
#[path = "assistant_event_tags_compared_are_tags_the_enum_writes_test.rs"]
mod assistant_event_tags_compared_are_tags_the_enum_writes_test;
#[path = "atomic_replace_is_written_once_test.rs"]
mod atomic_replace_is_written_once_test;
#[path = "auth_error_codes_are_one_vocabulary_test.rs"]
mod auth_error_codes_are_one_vocabulary_test;
#[path = "autonomous_executor_test.rs"]
mod autonomous_executor_test;
#[path = "background_loop_contract_test.rs"]
mod background_loop_contract_test;
#[path = "backup_package_name_is_one_shape_test.rs"]
mod backup_package_name_is_one_shape_test;
#[path = "backup_retention_is_counted_not_dated.rs"]
mod backup_retention_is_counted_not_dated;
#[path = "board_wip_limit_wiring_test.rs"]
mod board_wip_limit_wiring_test;
#[path = "boot_phase_marks_contract_test.rs"]
mod boot_phase_marks_contract_test;
#[path = "builtin_tools_have_one_list.rs"]
mod builtin_tools_have_one_list;
#[path = "capability_declarations_are_enumerated.rs"]
mod capability_declarations_are_enumerated;
#[path = "change_kind_vocabulary_test.rs"]
mod change_kind_vocabulary_test;
#[path = "change_ledger_read_face.rs"]
mod change_ledger_read_face;
#[path = "chart_declared_ports_reach_the_process_test.rs"]
mod chart_declared_ports_reach_the_process_test;
#[path = "chart_template_literals_contract_test.rs"]
mod chart_template_literals_contract_test;
#[path = "ci_gate_parity_contract_test.rs"]
mod ci_gate_parity_contract_test;
#[path = "claim_label_joins_the_deployed_rules_test.rs"]
mod claim_label_joins_the_deployed_rules_test;
#[path = "closed_vocabularies_in_sql_read_the_declaration_test.rs"]
mod closed_vocabularies_in_sql_read_the_declaration_test;
#[path = "cluster_overview_counts_live_agents.rs"]
mod cluster_overview_counts_live_agents;
#[path = "cmd_timeout_sentence_is_spelt_once_test.rs"]
mod cmd_timeout_sentence_is_spelt_once_test;
#[path = "collaboration_harness_smoke_test.rs"]
mod collaboration_harness_smoke_test;
#[path = "collaboration_roundtable_integration_test.rs"]
mod collaboration_roundtable_integration_test;
#[path = "collaboration_roundtable_test.rs"]
mod collaboration_roundtable_test;
#[path = "collaboration_self_review_test.rs"]
mod collaboration_self_review_test;
#[path = "complete_options_are_mapped_once_test.rs"]
mod complete_options_are_mapped_once_test;
#[path = "config_declaration_delivery_test.rs"]
mod config_declaration_delivery_test;
#[path = "config_section_reader_contract_test.rs"]
mod config_section_reader_contract_test;
#[path = "contract_default_body_must_not_answer_success_it_did_not_earn.rs"]
mod contract_default_body_must_not_answer_success_it_did_not_earn;
#[path = "core_boundary_contract_test.rs"]
mod core_boundary_contract_test;
#[path = "core_declared_strings_are_not_retyped_test.rs"]
mod core_declared_strings_are_not_retyped_test;
#[path = "crate_dependency_contract_test.rs"]
mod crate_dependency_contract_test;
#[path = "crate_source_membership_contract_test.rs"]
mod crate_source_membership_contract_test;
#[path = "credential_reading_loops_have_a_home.rs"]
mod credential_reading_loops_have_a_home;
#[path = "dag_executor_runtime_test.rs"]
mod dag_executor_runtime_test;
#[path = "dag_state_reclaimer_contract_test.rs"]
mod dag_state_reclaimer_contract_test;
#[path = "data_root_entries_are_declared_once_test.rs"]
mod data_root_entries_are_declared_once_test;
#[path = "data_root_subdirs.rs"]
mod data_root_subdirs;
#[path = "database_service_survives_an_unready_sidecar_test.rs"]
mod database_service_survives_an_unready_sidecar_test;
#[path = "dataset_routes.rs"]
mod dataset_routes;
#[path = "declared_prompts_match_code_fallbacks.rs"]
mod declared_prompts_match_code_fallbacks;
#[path = "deployed_rule_label_values_are_values_the_code_writes_test.rs"]
mod deployed_rule_label_values_are_values_the_code_writes_test;
#[path = "deployed_rule_labels_are_labels_the_code_emits_test.rs"]
mod deployed_rule_labels_are_labels_the_code_emits_test;
#[path = "deployed_rule_selector_keys_are_keys_the_emitter_writes_test.rs"]
mod deployed_rule_selector_keys_are_keys_the_emitter_writes_test;
#[path = "deployer_rbac_covers_its_reads_test.rs"]
mod deployer_rbac_covers_its_reads_test;
#[path = "deployment_namespace_literals_contract_test.rs"]
mod deployment_namespace_literals_contract_test;
#[path = "deployment_object_names_contract_test.rs"]
mod deployment_object_names_contract_test;
#[path = "e2e_integration.rs"]
mod e2e_integration;
#[path = "enumerations_name_every_variant_the_enum_declares_test.rs"]
mod enumerations_name_every_variant_the_enum_declares_test;
#[path = "env_names_are_declared_once_test.rs"]
mod env_names_are_declared_once_test;
#[path = "eval_report_is_readable_after_a_run.rs"]
mod eval_report_is_readable_after_a_run;
#[path = "eval_table_determinism.rs"]
mod eval_table_determinism;
#[path = "event_bus_metric_seed_test.rs"]
mod event_bus_metric_seed_test;
#[path = "every_agent_unhealthy_writer_consults_the_judgement.rs"]
mod every_agent_unhealthy_writer_consults_the_judgement;
#[path = "every_file_in_the_static_tree_is_delivered_or_declared_test.rs"]
mod every_file_in_the_static_tree_is_delivered_or_declared_test;
#[path = "every_rendered_profile_is_a_tree_the_deployer_delivers_test.rs"]
mod every_rendered_profile_is_a_tree_the_deployer_delivers_test;
#[path = "export_package_names_test.rs"]
mod export_package_names_test;
#[path = "gateway_answers_a_waited_prompt.rs"]
mod gateway_answers_a_waited_prompt;
#[path = "gateway_refuses_a_caller_named_reply_stream.rs"]
mod gateway_refuses_a_caller_named_reply_stream;
#[path = "governance_routes.rs"]
mod governance_routes;
#[path = "host_paths_are_not_baked_in_test.rs"]
mod host_paths_are_not_baked_in_test;
#[path = "image_layout_literals_contract_test.rs"]
mod image_layout_literals_contract_test;
#[path = "in_cluster_address_literals_contract_test.rs"]
mod in_cluster_address_literals_contract_test;
#[path = "install_face_creates_every_secret_the_apply_path_reads_test.rs"]
mod install_face_creates_every_secret_the_apply_path_reads_test;
#[path = "internal_callers_ask_for_served_paths_test.rs"]
mod internal_callers_ask_for_served_paths_test;
#[path = "knowledge_namespace_write_contract_test.rs"]
mod knowledge_namespace_write_contract_test;
#[path = "knowledge_retrieval_round_trip_test.rs"]
mod knowledge_retrieval_round_trip_test;
#[path = "knowledge_retrieval_series_test.rs"]
mod knowledge_retrieval_series_test;
#[path = "llm_actor_label_contract_test.rs"]
mod llm_actor_label_contract_test;
#[path = "llm_spend_verdict_contract_test.rs"]
mod llm_spend_verdict_contract_test;
#[path = "log_stream_selectors_are_labels_the_code_writes_test.rs"]
mod log_stream_selectors_are_labels_the_code_writes_test;
#[path = "mainline_rollout_dispatch_contract_test.rs"]
mod mainline_rollout_dispatch_contract_test;
#[path = "mcp_server_stdio.rs"]
mod mcp_server_stdio;
#[path = "memory_archive_on_a_filesystem_store.rs"]
mod memory_archive_on_a_filesystem_store;
#[path = "memory_backend_test.rs"]
mod memory_backend_test;
#[path = "memory_composite_test.rs"]
mod memory_composite_test;
#[path = "memory_core_backend_test.rs"]
mod memory_core_backend_test;
#[path = "memory_importance_ranking_test.rs"]
mod memory_importance_ranking_test;
#[path = "memory_metrics_backend_test.rs"]
mod memory_metrics_backend_test;
#[path = "memory_metrics_instrumented_test.rs"]
mod memory_metrics_instrumented_test;
#[path = "memory_object_backend_test.rs"]
mod memory_object_backend_test;
#[path = "memory_observability_gateway_test.rs"]
mod memory_observability_gateway_test;
#[path = "memory_search_all_test.rs"]
mod memory_search_all_test;
#[path = "memory_vector_backend_test.rs"]
mod memory_vector_backend_test;
#[path = "message_backend_multi_instance_test.rs"]
mod message_backend_multi_instance_test;
#[path = "metric_help_reports_the_wait_the_code_does_test.rs"]
mod metric_help_reports_the_wait_the_code_does_test;
#[path = "network_policy_subjects_name_a_component_the_code_writes_test.rs"]
mod network_policy_subjects_name_a_component_the_code_writes_test;
#[path = "notification_contract_matches_its_schema.rs"]
mod notification_contract_matches_its_schema;
#[path = "notification_delivery_over_a_socket_test.rs"]
mod notification_delivery_over_a_socket_test;
#[path = "notification_outlet_reachability_test.rs"]
mod notification_outlet_reachability_test;
#[path = "oauth_popup_keeps_its_opener_test.rs"]
mod oauth_popup_keeps_its_opener_test;
#[path = "observability_alert_face_literals_contract_test.rs"]
mod observability_alert_face_literals_contract_test;
#[path = "observability_alert_rule_contract_test.rs"]
mod observability_alert_rule_contract_test;
#[path = "observability_dashboard_contract_test.rs"]
mod observability_dashboard_contract_test;
#[path = "observability_endpoint_ports_contract_test.rs"]
mod observability_endpoint_ports_contract_test;
#[path = "observability_pool_freshness_contract_test.rs"]
mod observability_pool_freshness_contract_test;
#[path = "observability_reachability_probe_wiring_test.rs"]
mod observability_reachability_probe_wiring_test;
#[path = "observability_scrape_census_contract_test.rs"]
mod observability_scrape_census_contract_test;
#[path = "observability_stack_convergence_test.rs"]
mod observability_stack_convergence_test;
#[path = "observable_dimension_contract_test.rs"]
mod observable_dimension_contract_test;
#[path = "observable_reachability_contract_test.rs"]
mod observable_reachability_contract_test;
#[path = "one_value_has_one_name_inside_its_crate_test.rs"]
mod one_value_has_one_name_inside_its_crate_test;
#[path = "page_compared_words_are_owned_by_a_writer_test.rs"]
mod page_compared_words_are_owned_by_a_writer_test;
#[path = "page_component_nodes_are_components_the_gateway_reports_test.rs"]
mod page_component_nodes_are_components_the_gateway_reports_test;
#[path = "page_status_words_are_words_the_status_writes_test.rs"]
mod page_status_words_are_words_the_status_writes_test;
#[path = "plugin_init_dependencies.rs"]
mod plugin_init_dependencies;
#[path = "popup_handshake_words_are_the_words_the_other_page_sends_test.rs"]
mod popup_handshake_words_are_the_words_the_other_page_sends_test;
#[path = "probe_paths_are_routes_the_container_serves_test.rs"]
mod probe_paths_are_routes_the_container_serves_test;
#[path = "process_report_path_contract_test.rs"]
mod process_report_path_contract_test;
#[path = "project_contract.rs"]
mod project_contract;
#[path = "publish_paths_share_one_wiring_ledger.rs"]
mod publish_paths_share_one_wiring_ledger;
#[path = "raw_stream_words_are_words_both_sides_write_test.rs"]
mod raw_stream_words_are_words_both_sides_write_test;
#[path = "reading_series_have_a_producer_test.rs"]
mod reading_series_have_a_producer_test;
#[path = "redis_connection_contract_test.rs"]
mod redis_connection_contract_test;
#[path = "redis_streams_cluster_test.rs"]
mod redis_streams_cluster_test;
#[path = "repeated_path_components_are_named_once_test.rs"]
mod repeated_path_components_are_named_once_test;
#[path = "resource_governance_projection_test.rs"]
mod resource_governance_projection_test;
#[path = "role_names_are_declared_once_test.rs"]
mod role_names_are_declared_once_test;
#[path = "rollout_resource_reading_contract_test.rs"]
mod rollout_resource_reading_contract_test;
#[path = "root_climb_lands_on_the_repo_root_test.rs"]
mod root_climb_lands_on_the_repo_root_test;
#[path = "runtime_database_role_is_not_the_instance_superuser_test.rs"]
mod runtime_database_role_is_not_the_instance_superuser_test;
#[path = "runtime_roots_literals_contract_test.rs"]
mod runtime_roots_literals_contract_test;
#[path = "rust_capacity_constants_registry_test.rs"]
mod rust_capacity_constants_registry_test;
#[path = "scrape_targets_name_a_port_the_workload_provides_test.rs"]
mod scrape_targets_name_a_port_the_workload_provides_test;
#[path = "service_account_mount_literals_contract_test.rs"]
mod service_account_mount_literals_contract_test;
#[path = "skill_directories_contract_test.rs"]
mod skill_directories_contract_test;
#[path = "store_counter_window_contract_test.rs"]
mod store_counter_window_contract_test;
#[path = "store_tables_are_in_the_restore_bundle_test.rs"]
mod store_tables_are_in_the_restore_bundle_test;
#[path = "stress_message_backend_test.rs"]
mod stress_message_backend_test;
#[path = "supervisor_alert_words_are_words_the_face_map_knows_test.rs"]
mod supervisor_alert_words_are_words_the_face_map_knows_test;
#[path = "supervisor_control_plane_test.rs"]
mod supervisor_control_plane_test;
#[path = "supervisor_event_aggregator_test.rs"]
mod supervisor_event_aggregator_test;
#[path = "supervisor_health_checker_test.rs"]
mod supervisor_health_checker_test;
#[path = "supervisor_judges_the_live_agents.rs"]
mod supervisor_judges_the_live_agents;
#[path = "supervisor_lifecycle_coordinator_test.rs"]
mod supervisor_lifecycle_coordinator_test;
#[path = "supervisor_registry_test.rs"]
mod supervisor_registry_test;
#[path = "supervisor_respawner_test.rs"]
mod supervisor_respawner_test;
#[path = "supervisor_supervisor_test.rs"]
mod supervisor_supervisor_test;
#[path = "supervisor_task_rebalancer_test.rs"]
mod supervisor_task_rebalancer_test;
#[path = "supervisor_udp_heartbeat_test.rs"]
mod supervisor_udp_heartbeat_test;
#[path = "task_resume_wiring_contract_test.rs"]
mod task_resume_wiring_contract_test;
#[path = "the_fixture_reading_steps_are_written_once_test.rs"]
mod the_fixture_reading_steps_are_written_once_test;
#[path = "the_production_cut_is_defined_once_test.rs"]
mod the_production_cut_is_defined_once_test;
#[path = "the_repo_root_is_computed_in_one_place_test.rs"]
mod the_repo_root_is_computed_in_one_place_test;
#[path = "trace_header_names_contract_test.rs"]
mod trace_header_names_contract_test;
#[path = "tunable_registry_contract_test.rs"]
mod tunable_registry_contract_test;
#[path = "unwired_modules_contract_test.rs"]
mod unwired_modules_contract_test;
#[path = "volume_footprint_declaration_test.rs"]
mod volume_footprint_declaration_test;
#[path = "wal_event_round_trip_test.rs"]
mod wal_event_round_trip_test;
#[path = "web_chat_frames_read_fields_the_socket_writes_test.rs"]
mod web_chat_frames_read_fields_the_socket_writes_test;
#[path = "web_client_api_functions_are_called_test.rs"]
mod web_client_api_functions_are_called_test;
#[path = "web_client_branch_words_are_words_the_server_writes_test.rs"]
mod web_client_branch_words_are_words_the_server_writes_test;
#[path = "web_client_query_names_are_read_by_the_gateway_test.rs"]
mod web_client_query_names_are_read_by_the_gateway_test;
#[path = "web_client_response_fields_are_written_somewhere_test.rs"]
mod web_client_response_fields_are_written_somewhere_test;
#[path = "web_client_routes_are_served_test.rs"]
mod web_client_routes_are_served_test;
#[path = "web_client_word_lists_are_declared_test.rs"]
mod web_client_word_lists_are_declared_test;
#[path = "web_event_payloads_are_the_fields_the_server_writes_test.rs"]
mod web_event_payloads_are_the_fields_the_server_writes_test;
#[path = "web_event_vocabulary_contract_test.rs"]
mod web_event_vocabulary_contract_test;
#[path = "web_payload_reads_are_scoped_to_their_variant_test.rs"]
mod web_payload_reads_are_scoped_to_their_variant_test;
#[path = "web_request_body_keys_are_parsed_by_the_handler_test.rs"]
mod web_request_body_keys_are_parsed_by_the_handler_test;
#[path = "web_resource_face_keys_are_served_routes_test.rs"]
mod web_resource_face_keys_are_served_routes_test;
#[path = "web_storage_keys_are_declared_once_test.rs"]
mod web_storage_keys_are_declared_once_test;
#[path = "web_websocket_channels_are_published_test.rs"]
mod web_websocket_channels_are_published_test;
#[path = "web_websocket_envelope_vocabulary_test.rs"]
mod web_websocket_envelope_vocabulary_test;
#[path = "wiki_indexer_test.rs"]
mod wiki_indexer_test;
#[path = "wiki_integration_test.rs"]
mod wiki_integration_test;
#[path = "wiki_manager_test.rs"]
mod wiki_manager_test;
#[path = "wiki_search_test.rs"]
mod wiki_search_test;
#[path = "wiki_vector_index_test.rs"]
mod wiki_vector_index_test;
#[path = "wire_words_are_not_derived_from_debug_test.rs"]
mod wire_words_are_not_derived_from_debug_test;
#[path = "word_constants_with_the_same_name_say_the_same_word_test.rs"]
mod word_constants_with_the_same_name_say_the_same_word_test;
#[path = "workload_label_pairs_contract_test.rs"]
mod workload_label_pairs_contract_test;
/// Whether a file can share this process with the others.
///
/// Two properties, both read from the file: it must not write a process-global
/// environment variable, and it must not carry an `#[ignore]`d case. A file on
/// disk that is not listed here must have one of them -- that is the whole of
/// the exemption, and it is derived from the file rather than kept in a list
/// beside it, so a file that starts doing either stops being eligible in the
/// same change that makes it do so.
fn cannot_share_a_process(text: &str) -> Option<&'static str> {
    if text.contains("set_var(") {
        return Some("writes a process-global environment variable");
    }
    if text.contains("#[ignore") {
        return Some("carries an #[ignore]d case that CI runs on its own");
    }
    None
}

/// The suite and the directory say the same thing.
///
/// This is the maintenance side of merging two hundred binaries into one: the
/// list above cannot be generated by the compiler, so a new test file would
/// silently not run. Reading both sides -- the `#[path]` lines in this file and
/// the files on disk -- makes forgetting one a failure rather than a quiet
/// drop, and the exemption is the same property the merge itself was decided
/// on, re-checked per file.
#[test]
fn the_suite_and_the_directory_agree() {
    const FLOOR: usize = 150;
    let tests = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let list = include_str!("suite.rs");
    let listed: Vec<&str> = list
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("#[path = \"")
                .and_then(|rest| rest.strip_suffix("\"]"))
                .map(|file| file.trim_end_matches(".rs"))
        })
        .collect();
    assert!(
        listed.len() >= FLOOR,
        "the list names {} files, fewer than the {FLOOR} this suite is known to hold; \
         the scan has gone wrong and everything below it would pass over the empty set",
        listed.len()
    );

    for name in &listed {
        let path = tests.join(format!("{name}.rs"));
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{}: listed but unreadable: {e}", path.display()));
        if let Some(why) = cannot_share_a_process(&text) {
            panic!("{name}.rs is in the merged suite but {why}");
        }
    }

    let mut on_disk = 0_usize;
    for entry in std::fs::read_dir(&tests).expect("tests directory is readable") {
        let path = entry.expect("directory entry").path();
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let name = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name == "suite" {
            continue;
        }
        on_disk += 1;
        if listed.contains(&name.as_str()) {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{}: unreadable: {e}", path.display()));
        assert!(
            cannot_share_a_process(&text).is_some(),
            "{name}.rs is neither in the merged suite nor a file that cannot share a process: \
             add it to the suite (or the reason it cannot be there is wrong)"
        );
    }
    assert!(
        on_disk >= FLOOR,
        "the census read {on_disk} files on disk, fewer than the {FLOOR} this suite is known to \
         hold; the directory scan has gone wrong"
    );
}
