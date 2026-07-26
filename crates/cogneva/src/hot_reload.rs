//! Hot-reload configuration application logic.
//! Extracted from `main.rs` so it can be unit-tested independently.

/// Apply hot-reloaded gateway configuration, detecting immutable port changes.
/// Returns `(applied_items, restart_required_items)`.
/// Port fields (`http_port`, `ws_port`, `metrics_port`) are compared but
/// intentionally **not** written — the caller must log the change and
/// either reject the reload or restart the process.
pub fn apply_gateway_config_update(
    current: &std::sync::RwLock<cog_core::GatewayConfig>,
    new_gateway: &cog_core::GatewayConfig,
    request_timeout_secs: &std::sync::atomic::AtomicU64,
    sandbox_task_timeout_secs: &std::sync::atomic::AtomicU64,
) -> (Vec<String>, Vec<String>) {
    let mut applied = Vec::new();
    let mut need_restart = Vec::new();

    let (old_http, old_ws, old_metrics) = {
        let cfg = current.read().unwrap_or_else(|e| e.into_inner());
        (cfg.http_port, cfg.ws_port, cfg.metrics_port)
    };
    let ports_changed = old_http != new_gateway.http_port
        || old_ws != new_gateway.ws_port
        || old_metrics != new_gateway.metrics_port;

    if ports_changed {
        need_restart.push(format!(
            "ports_changed: http {}→{}, ws {}→{}, metrics {}→{}",
            old_http,
            new_gateway.http_port,
            old_ws,
            new_gateway.ws_port,
            old_metrics,
            new_gateway.metrics_port
        ));
    }

    // Update non-port fields only.
    {
        let mut cfg = current.write().unwrap_or_else(|e| e.into_inner());
        cfg.websocket_timeout_secs = new_gateway.websocket_timeout_secs;
        cfg.websocket_inactivity_timeout_secs = new_gateway.websocket_inactivity_timeout_secs;
        cfg.websocket_tick_secs = new_gateway.websocket_tick_secs;
        cfg.request_timeout_secs = new_gateway.request_timeout_secs;
        cfg.sandbox_task_timeout_secs = new_gateway.sandbox_task_timeout_secs;
        cfg.notification_limit = new_gateway.notification_limit;
        // NOTE: http_port / ws_port / metrics_port are intentionally
        // preserved so the already-bound sockets remain valid.
    }
    request_timeout_secs.store(
        new_gateway.request_timeout_secs,
        std::sync::atomic::Ordering::Relaxed,
    );
    sandbox_task_timeout_secs.store(
        new_gateway.sandbox_task_timeout_secs,
        std::sync::atomic::Ordering::Relaxed,
    );
    applied.push("gateway_timeouts_updated".to_string());

    (applied, need_restart)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::GatewayConfig;

    #[test]
    fn test_port_change_detected_and_rejected() {
        let current = std::sync::RwLock::new(GatewayConfig {
            http_port: 8080,
            ws_port: 8081,
            metrics_port: 8082,
            cors_origins: vec![],
            websocket_timeout_secs: 30,
            websocket_inactivity_timeout_secs: 90,
            websocket_tick_secs: 5,
            notification_limit: 50,
            sandbox_task_timeout_secs: 30,
            request_timeout_secs: 30,
            notification_webhook_url: None,
            ..Default::default()
        });

        let new_config = GatewayConfig {
            http_port: 9090,
            ws_port: 9091,
            metrics_port: 9092,
            websocket_timeout_secs: 60,
            websocket_inactivity_timeout_secs: 120,
            websocket_tick_secs: 10,
            notification_limit: 100,
            sandbox_task_timeout_secs: 45,
            request_timeout_secs: 45,
            notification_webhook_url: None,
            ..Default::default()
        };

        let request_timeout = std::sync::atomic::AtomicU64::new(30);
        let sandbox_timeout = std::sync::atomic::AtomicU64::new(30);
        let (applied, need_restart) =
            apply_gateway_config_update(&current, &new_config, &request_timeout, &sandbox_timeout);

        assert!(
            need_restart.iter().any(|s| s.contains("ports_changed")),
            "Port change should be detected and reported as needing restart"
        );
        assert!(
            applied.iter().any(|s| s == "gateway_timeouts_updated"),
            "Non-port fields should be marked as applied"
        );

        let cfg = current.read().unwrap_or_else(|e| e.into_inner());
        // Old ports must be preserved.
        assert_eq!(cfg.http_port, 8080, "http_port must not change");
        assert_eq!(cfg.ws_port, 8081, "ws_port must not change");
        assert_eq!(cfg.metrics_port, 8082, "metrics_port must not change");

        // Non-port fields must be updated.
        assert_eq!(cfg.websocket_timeout_secs, 60);
        assert_eq!(cfg.websocket_inactivity_timeout_secs, 120);
        assert_eq!(cfg.websocket_tick_secs, 10);
        assert_eq!(cfg.notification_limit, 100);
        assert_eq!(cfg.sandbox_task_timeout_secs, 45);
        assert_eq!(cfg.request_timeout_secs, 45);
        assert_eq!(
            request_timeout.load(std::sync::atomic::Ordering::Relaxed),
            45
        );
        assert_eq!(
            sandbox_timeout.load(std::sync::atomic::Ordering::Relaxed),
            45
        );
    }

    #[test]
    fn test_same_ports_no_restart_needed() {
        let current = std::sync::RwLock::new(GatewayConfig {
            http_port: 8080,
            ws_port: 8081,
            metrics_port: 8082,
            cors_origins: vec![],
            websocket_timeout_secs: 30,
            websocket_inactivity_timeout_secs: 90,
            websocket_tick_secs: 5,
            notification_limit: 50,
            sandbox_task_timeout_secs: 30,
            request_timeout_secs: 30,
            notification_webhook_url: None,
            ..Default::default()
        });

        let new_config = GatewayConfig {
            http_port: 8080,
            ws_port: 8081,
            metrics_port: 8082,
            websocket_timeout_secs: 60,
            websocket_inactivity_timeout_secs: 90,
            websocket_tick_secs: 5,
            notification_limit: 50,
            sandbox_task_timeout_secs: 30,
            request_timeout_secs: 30,
            notification_webhook_url: None,
            ..Default::default()
        };

        let request_timeout = std::sync::atomic::AtomicU64::new(30);
        let sandbox_timeout = std::sync::atomic::AtomicU64::new(30);
        let (applied, need_restart) =
            apply_gateway_config_update(&current, &new_config, &request_timeout, &sandbox_timeout);

        assert!(
            need_restart.is_empty(),
            "No restart should be needed when ports stay the same"
        );
        assert!(
            applied.iter().any(|s| s == "gateway_timeouts_updated"),
            "Timeout update should still be applied"
        );

        let cfg = current.read().unwrap_or_else(|e| e.into_inner());
        assert_eq!(cfg.websocket_timeout_secs, 60);
        assert_eq!(cfg.http_port, 8080);
        assert_eq!(
            request_timeout.load(std::sync::atomic::Ordering::Relaxed),
            30
        );
        assert_eq!(
            sandbox_timeout.load(std::sync::atomic::Ordering::Relaxed),
            30
        );
    }
}
