//! Configuration loader for the cogneva binary.
//! Implements the 5-layer config stack:
//! Layer 0: Hard-coded defaults (`Config::default()`)
//! Layer 1: Base config file (`/etc/cogneva/cogneva.json` or `COGNEVA_CONFIG_PATH`)
//! Layer 2: Environment-specific config (`cogneva.{env}.json`)
//! Layer 3: Environment variables (`Config::from_env()`)
//! Layer 4: Runtime overrides (future)
//! This module lives in the binary crate because file I/O does not belong in
//! `cog-core`.
//! **架构定位**：`cog-core::Config` 只保留领域层通用配置；业务 crate 特有的
//! 配置（supervisor、hook_engine、metrics）定义在
//! 本模块的 `AppConfig` 中，通过 `#[serde(flatten)]` 与 core 配置平铺共存，
//! 既保持了 `cog-core` 的纯净，又保留了 `cogneva.json` 的动态丰富配置能力。

use cog_core::{Config, SFResult, SecretProvider};
use std::collections::HashMap;
use std::ops::Deref;

/// Default system-wide config path (FHS).
pub const DEFAULT_CONFIG_PATH: &str = "/etc/cogneva/cogneva.json";

// ---------------------------------------------------------------------------
// AppConfig — thin wrapper around cog-core::Config for the binary layer
// ---------------------------------------------------------------------------

/// Binary-layer config wrapper.
/// `core` contains the full `cog-core::Config` (now including all business
/// sections).  `#[serde(flatten)]` keeps the JSON/TOML representation flat.
/// `Deref<Target = Config>` lets existing `config.app.name` etc. keep working.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct AppConfig {
    #[serde(flatten)]
    pub core: Config,
}

impl Deref for AppConfig {
    type Target = Config;
    fn deref(&self) -> &Config {
        &self.core
    }
}

/// Default env mappings — kept in one place so they are easy to audit.
fn default_env_mappings() -> HashMap<String, String> {
    let mut m = HashMap::new();
    // app
    m.insert("COGNEVA_APP_NAME".into(), "app.name".into());
    m.insert("COGNEVA_APP_VERSION".into(), "app.version".into());
    m.insert("COGNEVA_LOG_LEVEL".into(), "app.log_level".into());
    m.insert("COGNEVA_DATA_DIR".into(), "app.data_dir".into());
    m.insert("COGNEVA_CONFIG_DIR".into(), "app.config_dir".into());
    m.insert("COGNEVA_APP_DIR".into(), "app.app_dir".into());
    // providers
    m.insert("COGNEVA_DB_PROVIDER".into(), "providers.db.provider".into());
    m.insert("COGNEVA_PG_PROVIDER".into(), "providers.pg.provider".into());
    m.insert(
        "COGNEVA_VECTOR_PROVIDER".into(),
        "providers.vector.provider".into(),
    );
    m.insert(
        "COGNEVA_MEDIA_PROVIDER".into(),
        "providers.media.provider".into(),
    );
    m.insert(
        "COGNEVA_STORAGE_PROVIDER".into(),
        "providers.storage.provider".into(),
    );
    m.insert(
        "COGNEVA_WIKI_PROVIDER".into(),
        "providers.wiki.provider".into(),
    );
    // dag_executor / gateway
    m.insert("COGNEVA_REDIS_URL".into(), "dag_executor.redis_url".into());
    m.insert("COGNEVA_NATS_URL".into(), "dag_executor.nats.urls.0".into());
    m.insert(
        "COGNEVA_NATS_USERNAME".into(),
        "dag_executor.nats.auth.username".into(),
    );
    m.insert(
        "COGNEVA_NATS_PASSWORD".into(),
        "dag_executor.nats.auth.password".into(),
    );
    m.insert(
        "COGNEVA_NATS_TOKEN".into(),
        "dag_executor.nats.auth.token".into(),
    );
    m.insert(
        "COGNEVA_NATS_TLS_ENABLED".into(),
        "dag_executor.nats.tls.enabled".into(),
    );
    m.insert(
        "COGNEVA_WORKSPACE_ID".into(),
        "dag_executor.workspace_id".into(),
    );
    m.insert(
        "COGNEVA_CONSUMER_GROUP".into(),
        "dag_executor.consumer_group".into(),
    );
    m.insert(
        "COGNEVA_ARCHIVE_ENABLED".into(),
        "dag_executor.archive_enabled".into(),
    );
    m.insert(
        "COGNEVA_ARCHIVE_AFTER_SECS".into(),
        "dag_executor.archive_after_secs".into(),
    );
    m.insert(
        "COGNEVA_ARCHIVE_POLL_INTERVAL_SECS".into(),
        "dag_executor.archive_poll_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_DECOMPOSITION_MAX_ATTEMPTS".into(),
        "dag_executor.decomposition_max_attempts".into(),
    );
    m.insert(
        "COGNEVA_DECOMPOSITION_ORPHAN_WATCH_ENABLED".into(),
        "dag_executor.decomposition_orphan_watch_enabled".into(),
    );
    m.insert(
        "COGNEVA_DECOMPOSITION_ORPHAN_POLL_INTERVAL_SECS".into(),
        "dag_executor.decomposition_orphan_poll_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_DECOMPOSITION_ORPHAN_STALL_AFTER_SECS".into(),
        "dag_executor.decomposition_orphan_stall_after_secs".into(),
    );
    m.insert(
        "COGNEVA_DECOMPOSITION_ORPHAN_ALERT_DWELL_SECS".into(),
        "dag_executor.decomposition_orphan_alert_dwell_secs".into(),
    );
    m.insert("COGNEVA_HTTP_PORT".into(), "gateway.http_port".into());
    m.insert("COGNEVA_WS_PORT".into(), "gateway.ws_port".into());
    m.insert("COGNEVA_METRICS_PORT".into(), "gateway.metrics_port".into());
    // Throwaway demo images flip the no-user-store demo login via env;
    // production installs leave it unset (false).
    m.insert(
        "COGNEVA_DEMO_LOGIN_ENABLED".into(),
        "gateway.demo_login_enabled".into(),
    );
    // raw_logger
    m.insert(
        "COGNEVA_RAW_LOGGER_ENABLED".into(),
        "raw_logger.enabled".into(),
    );
    m.insert(
        "COGNEVA_RAW_LOGGER_BASE_DIR".into(),
        "raw_logger.base_dir".into(),
    );
    m.insert(
        "COGNEVA_RAW_LOGGER_BUFFER_SIZE".into(),
        "raw_logger.max_buffer_size".into(),
    );
    // memory
    // tier_migrator
    m.insert(
        "COGNEVA_TIER_MIGRATOR_ENABLED".into(),
        "tier_migrator.enabled".into(),
    );
    m.insert(
        "COGNEVA_TIER_HOT_DURATION_SECS".into(),
        "tier_migrator.hot_duration_secs".into(),
    );
    m.insert(
        "COGNEVA_TIER_WARM_DURATION_SECS".into(),
        "tier_migrator.warm_duration_secs".into(),
    );
    m.insert(
        "COGNEVA_TIER_SCAN_INTERVAL_SECS".into(),
        "tier_migrator.scan_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_TIER_COLD_KEY_PREFIX".into(),
        "tier_migrator.cold_key_prefix".into(),
    );
    // observability exporters
    // business-specific
    m.insert("COGNEVA_METRICS_ENABLED".into(), "metrics.enabled".into());
    m.insert(
        "COGNEVA_SUPERVISOR_HEALTH_INTERVAL_SECS".into(),
        "supervisor.health_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_SUPERVISOR_QUOTA_INTERVAL_SECS".into(),
        "supervisor.quota_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_HOOK_ENGINE_DEDUP_WINDOW_SECS".into(),
        "hook_engine.dedup_window_secs".into(),
    );
    // self_evolution
    m.insert(
        "COGNEVA_SELF_EVOLUTION_ENABLED".into(),
        "self_evolution.enabled".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_EXECUTOR_ENABLED".into(),
        "self_evolution.executor_enabled".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_AUTO_APPLY".into(),
        "self_evolution.auto_apply".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_AUTO_DEPLOY".into(),
        "self_evolution.auto_deploy".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_SANDBOX_MODE".into(),
        "self_evolution.sandbox_mode".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_FORCE_AUTONOMOUS".into(),
        "self_evolution.force_autonomous".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_MANUAL_APPROVE".into(),
        "self_evolution.manual_approve".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_SWITCH_MODE".into(),
        "self_evolution.switch_mode".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_CHANGE_DIR".into(),
        "self_evolution.change_dir".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_BINARY_DIR".into(),
        "self_evolution.binary_dir".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_BACKUP_DIR".into(),
        "self_evolution.backup_dir".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_POLL_INTERVAL_SECS".into(),
        "self_evolution.poll_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_TEST_TIMEOUT_SECS".into(),
        "self_evolution.test_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_BUILD_TIMEOUT_SECS".into(),
        "self_evolution.build_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_WORKSPACES_ROOT".into(),
        "self_evolution.workspaces.root".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_WORKSPACES_TARGET_DIR".into(),
        "self_evolution.workspaces.target_dir".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_WORKSPACES_EPHEMERAL_TTL_SECS".into(),
        "self_evolution.workspaces.ephemeral_ttl_secs".into(),
    );
    m.insert(
        "COGNEVA_SELF_EVOLUTION_WORKSPACES_ORPHAN_BRANCH_TTL_SECS".into(),
        "self_evolution.workspaces.orphan_branch_ttl_secs".into(),
    );
    // system
    m.insert(
        "COGNEVA_SKILL_HOT_RELOAD_INTERVAL_SECS".into(),
        "system.skill_hot_reload_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_TOOL_TIMEOUT_SECS".into(),
        "system.tool_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_GRPC_RECONNECT_INTERVAL_SECS".into(),
        "system.grpc_reconnect_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_PROBE_TIMEOUT_SECS".into(),
        "system.probe_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_HTTP_TIMEOUT_SECS".into(),
        "system.http_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_PARTITION_MAINTENANCE_INTERVAL_SECS".into(),
        "system.partition_maintenance_interval_secs".into(),
    );
    m.insert(
        "COGNEVA_PATTERN_DB_MAX_SIZE".into(),
        "system.pattern_db_max_size".into(),
    );
    m.insert(
        "COGNEVA_PATTERN_MAX_AGE_DAYS".into(),
        "system.pattern_max_age_days".into(),
    );
    // gateway
    m.insert(
        "COGNEVA_WEBSOCKET_TIMEOUT_SECS".into(),
        "gateway.websocket_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_WEBSOCKET_INACTIVITY_TIMEOUT_SECS".into(),
        "gateway.websocket_inactivity_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_WEBSOCKET_TICK_SECS".into(),
        "gateway.websocket_tick_secs".into(),
    );
    m.insert(
        "COGNEVA_NOTIFICATION_LIMIT".into(),
        "gateway.notification_limit".into(),
    );
    m.insert(
        "COGNEVA_SANDBOX_TASK_TIMEOUT_SECS".into(),
        "gateway.sandbox_task_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_REQUEST_TIMEOUT_SECS".into(),
        "gateway.request_timeout_secs".into(),
    );
    m.insert(
        "COGNEVA_NOTIFICATION_WEBHOOK_URL".into(),
        "gateway.notification_webhook_url".into(),
    );
    // tuning
    // agent_pool
    // multi_backend_consumer
    m.insert(
        "COGNEVA_MULTI_BACKEND_CONSUMER_ENABLED".into(),
        "multi_backend_consumer.enabled".into(),
    );
    m.insert(
        "COGNEVA_MULTI_BACKEND_CONSUMER_CHANNEL".into(),
        "multi_backend_consumer.channel".into(),
    );
    m.insert(
        "COGNEVA_MULTI_BACKEND_CONSUMER_GROUP".into(),
        "multi_backend_consumer.group".into(),
    );
    m.insert(
        "COGNEVA_MULTI_BACKEND_CONSUMER_RETRY_INTERVAL_SECS".into(),
        "multi_backend_consumer.retry_interval_secs".into(),
    );
    // github_integration
    m
}

impl Default for AppConfig {
    fn default() -> Self {
        let core = Config {
            env: default_env_mappings(),
            ..Default::default()
        };
        Self { core }
    }
}

// ---------------------------------------------------------------------------
// Conversions to business-crate config types
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Load functions
// ---------------------------------------------------------------------------

/// Read one config file layer.
///
/// A file that is simply absent contributes nothing and is not an error — that
/// is the normal state of optional layers. A file that **exists** but cannot be
/// read or parsed is an error: reporting it as an empty layer would let a
/// truncated write (editors and `File::create` both truncate before writing)
/// or a half-swapped configmap volume masquerade as a valid, near-empty
/// configuration.
fn read_config_layer(path: &str) -> SFResult<Option<serde_json::Value>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(cog_core::SFError::Config(format!(
                "config file {path} is unreadable: {e}"
            )))
        }
    };
    let mut value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
        cog_core::SFError::Config(format!("config file {path} is not valid JSON: {e}"))
    })?;
    interpolate_env_vars(&mut value);
    Ok(Some(value))
}

/// Apply one layer's JSON on top of the config accumulated so far.
fn apply_config_layer(
    config: AppConfig,
    path: &str,
    value: serde_json::Value,
) -> SFResult<AppConfig> {
    let merged = merge_config_value(serde_json::to_value(&config).unwrap_or_default(), value)
        .map_err(|e| {
            cog_core::SFError::Config(format!("config file {path} cannot be merged: {e}"))
        })?;
    serde_json::from_value::<AppConfig>(merged).map_err(|e| {
        cog_core::SFError::Config(format!("config file {path} does not match the schema: {e}"))
    })
}

/// Load configuration using the full 5-layer stack, reporting a config file
/// that exists but cannot be used.
///
/// Callers that must distinguish "loaded" from "fell back to defaults" — the
/// hot-reload watcher in particular — use this. Boot-time callers that only
/// need a usable config use [`load`].
pub fn try_load() -> SFResult<AppConfig> {
    let mut config = AppConfig::default();

    // Layer 1: base config file
    let base_path =
        std::env::var("COGNEVA_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.into());
    if let Some(base_value) = read_config_layer(&base_path)? {
        config = apply_config_layer(config, &base_path, base_value)?;
    }

    // Layer 2: environment-specific config
    let env = std::env::var("COGNEVA_ENV").unwrap_or_else(|_| "development".into());
    let env_path = base_path.replace(".json", &format!(".{env}.json"));
    if env_path != base_path {
        if let Some(env_value) = read_config_layer(&env_path)? {
            config = apply_config_layer(config, &env_path, env_value)?;
        }
    }

    // Layer 3: environment variables — apply directly so unset vars do not
    // overwrite values already loaded from files.
    apply_env_overrides(&mut config);

    Ok(config)
}

/// Load configuration using the full 5-layer stack, falling back to defaults
/// when no usable config is found. The fallback is logged: a silent default is
/// indistinguishable from a configured one downstream.
pub fn load() -> AppConfig {
    match try_load() {
        Ok(config) => config,
        Err(e) => {
            tracing::warn!("Falling back to default configuration: {e}");
            // Layer 3 still applies: env vars are the last-resort configuration
            // surface and must survive an unusable file.
            let mut config = AppConfig::default();
            apply_env_overrides(&mut config);
            config
        }
    }
}

/// Build an [`AppConfig`] from environment variables only.
#[allow(dead_code)]
pub fn from_env() -> AppConfig {
    let mut config = AppConfig::default();
    apply_env_overrides(&mut config);
    config
}

/// Apply environment-variable overrides to an existing [`AppConfig`].
/// Reads the `env` field (env-var name → JSON dot-path), fetches each
/// variable from the process environment and writes it into the matching
/// config path.  No env-var names are hard-coded here — the mapping lives
/// entirely in `cogneva.json` (or `default_env_mappings` when no file is
/// present).
pub fn apply_env_overrides(config: &mut AppConfig) {
    let mut value = match serde_json::to_value(&*config) {
        Ok(v) => v,
        Err(_) => return,
    };

    // If the loaded config has no env mapping (e.g. base file missing), fall
    // back to the binary's built-in mappings so env overrides still work.
    let mappings: Vec<(String, String)> = if config.env.is_empty() {
        default_env_mappings().into_iter().collect()
    } else {
        config
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    for (env_key, path) in mappings {
        if let Ok(env_val) = std::env::var(&env_key) {
            cog_core::config::set_json_path(&mut value, &path, &env_val);
        }
    }

    // Special case: if wiki provider was set via env, auto-enable it.
    if let Some(serde_json::Value::Object(wiki)) =
        value.get_mut("providers").and_then(|p| p.get_mut("wiki"))
    {
        if wiki
            .get("provider")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false)
        {
            wiki.insert("enabled".into(), serde_json::Value::Bool(true));
        }
    }

    if let Ok(new_config) = serde_json::from_value::<AppConfig>(value) {
        *config = new_config;
    }
}

/// Load configuration from a JSON file.
/// Partial configs are supported: missing fields use hard-coded defaults.
/// `${ENV_VAR}` placeholders inside string values are resolved at load time.
#[allow(dead_code)]
pub fn from_json_file<P: AsRef<std::path::Path>>(path: P) -> SFResult<AppConfig> {
    let content = std::fs::read_to_string(path)?;
    let mut file_value: serde_json::Value =
        serde_json::from_str(&content).map_err(|e| cog_core::SFError::Config(e.to_string()))?;
    interpolate_env_vars(&mut file_value);
    let merged = merge_config_value(
        serde_json::to_value(AppConfig::default())
            .map_err(|e| cog_core::SFError::Config(e.to_string()))?,
        file_value,
    )?;
    let config: AppConfig =
        serde_json::from_value(merged).map_err(|e| cog_core::SFError::Config(e.to_string()))?;
    Ok(config)
}

fn merge_config_value(a: serde_json::Value, b: serde_json::Value) -> SFResult<serde_json::Value> {
    match (a, b) {
        (serde_json::Value::Object(mut a_map), serde_json::Value::Object(b_map)) => {
            for (k, v) in b_map {
                let entry = a_map.entry(k).or_insert(serde_json::Value::Null);
                *entry = merge_config_value(entry.take(), v)?;
            }
            Ok(serde_json::Value::Object(a_map))
        }
        // For non-object values, b overrides a (including arrays)
        (_, b) => Ok(b),
    }
}

// ---------------------------------------------------------------------------
// Environment-variable interpolation inside JSON values
// ---------------------------------------------------------------------------

/// Recursively replace `${VAR}` placeholders inside every JSON string value
/// with the corresponding environment variable.  Unset variables are left
/// as-is so the caller can spot missing values.
fn interpolate_env_vars(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(s) => {
            *s = substitute_env_vars(s);
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                interpolate_env_vars(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                interpolate_env_vars(v);
            }
        }
        _ => {}
    }
}

/// Replace all `${VAR}` occurrences in `input` with the value of the
/// environment variable `VAR`.  If the variable is not set the placeholder
/// is preserved unchanged.
fn substitute_env_vars(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            loop {
                match chars.next() {
                    Some('}') => break,
                    Some(ch) => var_name.push(ch),
                    None => {
                        // Unclosed ${ — preserve literally and abort.
                        result.push('$');
                        result.push('{');
                        result.push_str(&var_name);
                        return result;
                    }
                }
            }
            if let Ok(val) = std::env::var(&var_name) {
                result.push_str(&val);
            } else {
                result.push_str("${");
                result.push_str(&var_name);
                result.push('}');
            }
        } else {
            result.push(c);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Secret references: secret://env/VAR, secret://file/<path>, secret://vault/<mount/path#field>
// ---------------------------------------------------------------------------

/// 解析配置中所有 `secret://` 引用（审计 3.3）。
///
/// - `secret://env/VAR_NAME` —— 环境变量
/// - `secret://file/<abs-path>` —— 文件（K8s Secret 挂载卷）
/// - `secret://vault/<mount/path#field>` —— HashiCorp Vault KV v2（需 VAULT_ADDR/VAULT_TOKEN）
///
/// 未解析出的引用保持原样（与 `${VAR}` 行为一致），由 validate-config 暴露。
pub async fn resolve_secret_refs(config: &mut AppConfig) -> cog_core::SFResult<()> {
    let mut value =
        serde_json::to_value(&*config).map_err(|e| cog_core::SFError::Config(e.to_string()))?;
    resolve_secret_refs_value(&mut value).await?;
    *config =
        serde_json::from_value(value).map_err(|e| cog_core::SFError::Config(e.to_string()))?;
    Ok(())
}

async fn resolve_secret_refs_value(value: &mut serde_json::Value) -> cog_core::SFResult<()> {
    match value {
        serde_json::Value::String(s) => {
            if let Some(rest) = s.strip_prefix("secret://") {
                if let Some(resolved) = resolve_secret_ref(rest).await? {
                    *s = resolved;
                }
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                Box::pin(resolve_secret_refs_value(v)).await?;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                Box::pin(resolve_secret_refs_value(v)).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn resolve_secret_ref(reference: &str) -> cog_core::SFResult<Option<String>> {
    let (scheme, path) = reference.split_once('/').ok_or_else(|| {
        cog_core::SFError::Validation(format!("malformed secret reference: secret://{reference}"))
    })?;
    match scheme {
        "env" => cog_core::EnvSecretProvider.get(path).await,
        "file" => {
            // file 引用使用绝对路径：secret://file//var/run/secrets/x → /var/run/secrets/x
            let abs = format!("/{path}");
            let root = std::path::Path::new("/");
            cog_core::FileSecretProvider::new(root)
                .get(abs.trim_start_matches('/'))
                .await
        }
        "vault" => match cog_net::VaultSecretProvider::from_env() {
            Some(provider) => provider.get(path).await,
            None => Err(cog_core::SFError::Config(
                "secret://vault reference requires VAULT_ADDR and VAULT_TOKEN".into(),
            )),
        },
        other => Err(cog_core::SFError::Validation(format!(
            "unknown secret scheme '{other}' (expected env|file|vault)"
        ))),
    }
}

#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub(crate) struct EnvGuard {
    key: String,
    old: Option<String>,
}

#[cfg(test)]
impl EnvGuard {
    pub(crate) fn set(key: &str, value: &str) -> Self {
        let old = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self {
            key: key.to_string(),
            old,
        }
    }
    pub(crate) fn remove(key: &str) -> Self {
        let old = std::env::var(key).ok();
        std::env::remove_var(key);
        Self {
            key: key.to_string(),
            old,
        }
    }
}

#[cfg(test)]
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.old {
            Some(v) => std::env::set_var(&self.key, v),
            None => std::env::remove_var(&self.key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_set_json_path_indexes_arrays() {
        let mut value = serde_json::json!({
            "llm_routing": {
                "backends": [
                    {"provider": "backend-a", "base_url": "https://a.example/v1"},
                    {"provider": "backend-b", "base_url": "https://b.example/v3"}
                ]
            }
        });

        cog_core::config::set_json_path(
            &mut value,
            "llm_routing.backends.0.base_url",
            "http://cogneva-security-gateway:8081/v1",
        );
        assert_eq!(
            value["llm_routing"]["backends"][0]["base_url"],
            "http://cogneva-security-gateway:8081/v1"
        );
        // 其余元素不受影响
        assert_eq!(
            value["llm_routing"]["backends"][1]["base_url"],
            "https://b.example/v3"
        );

        // 越界下标不扩容、不 panic
        cog_core::config::set_json_path(&mut value, "llm_routing.backends.5.base_url", "x");
        assert_eq!(
            value["llm_routing"]["backends"].as_array().unwrap().len(),
            2
        );

        // 对象路径语义不变
        cog_core::config::set_json_path(&mut value, "gateway.http_port", "9090");
        assert_eq!(value["gateway"]["http_port"], 9090);
    }

    #[test]
    fn test_load_without_files_uses_defaults() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _g1 = EnvGuard::set("COGNEVA_CONFIG_PATH", "/nonexistent/cogneva.json");
        let _g2 = EnvGuard::remove("COGNEVA_ENV");

        let config = load();
        assert_eq!(config.app.name, "");
        assert_eq!(config.gateway.http_port, 0);
        assert!(config.metrics.enabled);
        assert_eq!(config.supervisor.health_interval_secs, 10);
    }

    /// Provider settings live in a nested `"options"` object, and consumers read
    /// them with `options.get(...)`. Round-tripping through the merge pipeline
    /// must not lift those keys out of the object, or an endpoint such as the
    /// wiki host silently falls back to a default nobody configured.
    #[test]
    fn test_provider_options_survive_the_load_pipeline() {
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        let json = r#"{
            "providers": {
                "pg": { "provider": "postgres", "enabled": true,
                        "options": { "connection": "postgres://db/cogneva" } },
                "wiki": { "provider": "meilisearch", "enabled": true,
                          "options": { "host": "http://meili:7700", "index": "wiki" } }
            }
        }"#;
        tmpfile.write_all(json.as_bytes()).unwrap();

        let config = from_json_file(tmpfile.path()).unwrap();
        assert_eq!(
            config
                .providers
                .pg
                .options
                .get("connection")
                .and_then(|v| v.as_str()),
            Some("postgres://db/cogneva")
        );
        let wiki = config
            .providers
            .wiki
            .as_ref()
            .expect("wiki provider present");
        assert_eq!(
            wiki.options.get("host").and_then(|v| v.as_str()),
            Some("http://meili:7700")
        );
        assert_eq!(
            wiki.options.get("index").and_then(|v| v.as_str()),
            Some("wiki")
        );
    }

    #[test]
    fn test_from_json_file() {
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        let json = r#"{
            "app": { "name": "test-app", "log_level": "debug" },
            "gateway": { "http_port": 9090 },
            "supervisor": { "health_interval_secs": 20 }
        }"#;
        tmpfile.write_all(json.as_bytes()).unwrap();

        let config = from_json_file(tmpfile.path()).unwrap();
        assert_eq!(config.app.name, "test-app");
        assert_eq!(config.app.log_level, "debug");
        assert_eq!(config.gateway.http_port, 9090);
        assert_eq!(config.app.version, "");
        assert_eq!(config.supervisor.health_interval_secs, 20);
        assert_eq!(config.supervisor.quota_interval_secs, 60);
    }

    #[test]
    fn test_load_with_base_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("cogneva.json");
        let mut file = std::fs::File::create(&base_path).unwrap();
        file.write_all(
            br#"{
                "app": { "name": "from-file", "log_level": "warn" },
                "gateway": { "http_port": 7777 }
            }"#,
        )
        .unwrap();

        let _g1 = EnvGuard::set("COGNEVA_CONFIG_PATH", base_path.to_str().unwrap());
        let _g2 = EnvGuard::remove("COGNEVA_ENV");

        let config = load();
        assert_eq!(config.app.name, "from-file");
        assert_eq!(config.app.log_level, "warn");
        assert_eq!(config.gateway.http_port, 7777);
        assert_eq!(config.gateway.ws_port, 0);
    }

    #[test]
    fn test_load_env_file_overrides_base() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("cogneva.json");
        let env_path = dir.path().join("cogneva.staging.json");

        std::fs::write(
            &base_path,
            br#"{ "app": { "name": "base-app" }, "gateway": { "http_port": 1111 } }"#,
        )
        .unwrap();
        std::fs::write(
            &env_path,
            br#"{ "app": { "name": "staging-app" }, "gateway": { "ws_port": 2222 } }"#,
        )
        .unwrap();

        let _g1 = EnvGuard::set("COGNEVA_CONFIG_PATH", base_path.to_str().unwrap());
        let _g2 = EnvGuard::set("COGNEVA_ENV", "staging");

        let config = load();
        assert_eq!(config.app.name, "staging-app");
        assert_eq!(config.gateway.ws_port, 2222);
        assert_eq!(config.gateway.http_port, 1111);
    }

    #[test]
    fn test_load_env_vars_override_files() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("cogneva.json");
        std::fs::write(
            &base_path,
            br#"{ "app": { "name": "file-app", "log_level": "error" } }"#,
        )
        .unwrap();

        let _g1 = EnvGuard::set("COGNEVA_CONFIG_PATH", base_path.to_str().unwrap());
        let _g2 = EnvGuard::set("COGNEVA_APP_NAME", "env-app");
        let _g3 = EnvGuard::remove("COGNEVA_ENV");

        let config = load();
        assert_eq!(config.app.name, "env-app");
        assert_eq!(config.app.log_level, "error");
    }

    #[test]
    fn test_business_config_from_json() {
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        let json = r#"{
            "metrics": { "enabled": false, "interval_secs": 30 },
            "supervisor": {
                "health_interval_secs": 5,
                "health_checker": { "suspect_after_secs": 10 }
            },
            "hook_engine": { "dedup_window_secs": 2, "default_rate_limit": { "burst": 50 } }
        }"#;
        tmpfile.write_all(json.as_bytes()).unwrap();

        let config = from_json_file(tmpfile.path()).unwrap();
        assert!(!config.metrics.enabled);
        assert_eq!(config.metrics.interval_secs, 30);
        assert_eq!(config.supervisor.health_interval_secs, 5);
        assert_eq!(config.supervisor.health_checker.suspect_after_secs, 10);
        assert_eq!(config.hook_engine.dedup_window_secs, 2);
        assert_eq!(config.hook_engine.default_rate_limit.burst, 50);
    }

    #[test]
    fn test_supervisor_config_conversion() {
        let cfg = cog_core::SupervisorConfig {
            health_interval_secs: 7,
            quota_interval_secs: 70,
            rebalance_interval_secs: 350,
            event_window_secs: 35,
            broadcast_capacity: 512,
            quota_threshold: 2000,
            health_checker: cog_core::HealthCheckerConfig {
                suspect_after_secs: 20,
                dead_after_secs: 90,
                stuck_after_secs: 900,
            },
            task_rebalancer: cog_core::TaskRebalancerConfig {
                max_tasks_per_agent: 8,
                max_assignments_per_pass: 64,
            },
            behavior_history_max: 20,
            heartbeat_history_max: 1000,
            alert_history_max: 10000,
            control_plane_interval_secs: 30,
            control_plane_url: None,
        };
        let business: cog_supervisor::SupervisorConfig = cfg.into();
        assert_eq!(business.health_interval, std::time::Duration::from_secs(7));
        assert_eq!(business.broadcast_capacity, 512);
        assert_eq!(business.quota_threshold, 2000);
    }

    #[test]
    fn test_hook_engine_config_conversion() {
        let cfg = cog_core::HookEngineConfig {
            dedup_window_secs: 3,
            default_rate_limit: cog_core::RateLimitConfig {
                burst: 200,
                per_second: 200,
            },
            hook_timeout_secs: 30,
        };
        let business: cog_agent::HookEngineConfig = cfg.into();
        assert_eq!(business.dedup_window, std::time::Duration::from_secs(3));
        assert_eq!(business.default_rate_limit.burst, 200);
    }

    #[test]
    fn test_agent_loop_config_conversion() {
        let cfg = cog_agent::AgentLoopConfig {
            agent_id: "agent-42".into(),
            role: "generator".into(),
            max_iterations: 7,
            context_window_size: 8192,
            skill_cache_ttl_secs: 30,
            think_stall_timeout_secs: 240,
        };
        let business: cog_core::RuntimeConfig = cfg.into();
        assert_eq!(business.agent_id, "agent-42");
        assert_eq!(business.role, "generator");
        assert_eq!(business.max_iterations, 7);
        assert_eq!(business.context_window_size, 8192);
    }

    #[test]
    fn test_substitute_env_vars() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _g1 = EnvGuard::set("TEST_DB_USER", "alice");
        let _g2 = EnvGuard::set("TEST_DB_PASS", "secret123");

        assert_eq!(
            substitute_env_vars("postgres://${TEST_DB_USER}:${TEST_DB_PASS}@localhost/db"),
            "postgres://alice:secret123@localhost/db"
        );
        assert_eq!(
            substitute_env_vars("single-${TEST_DB_USER}-value"),
            "single-alice-value"
        );
        assert_eq!(substitute_env_vars("no placeholder"), "no placeholder");
        // Unset variable is preserved unchanged
        assert_eq!(
            substitute_env_vars("${TEST_NONEXISTENT_VAR}"),
            "${TEST_NONEXISTENT_VAR}"
        );
    }

    #[test]
    fn test_json_env_interpolation() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _g1 = EnvGuard::set("TEST_APP_NAME", "interpolated-app");

        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        let json = r#"{ "app": { "name": "${TEST_APP_NAME}", "log_level": "info", "data_dir": "/tmp", "config_dir": "/tmp", "app_dir": "/tmp" } }"#;
        tmpfile.write_all(json.as_bytes()).unwrap();

        let config = from_json_file(tmpfile.path()).unwrap();
        assert_eq!(config.app.name, "interpolated-app");
    }

    #[tokio::test]
    async fn test_resolve_secret_ref_env() {
        let _g1 = {
            let _lock = ENV_LOCK.lock().unwrap();
            EnvGuard::set("TEST_SECRET_API_KEY", "resolved-key-123")
        };

        let mut config = AppConfig::default();
        config.core.dag_executor.redis_url = "secret://env/TEST_SECRET_API_KEY".into();
        resolve_secret_refs(&mut config).await.unwrap();
        assert_eq!(config.core.dag_executor.redis_url, "resolved-key-123");
    }

    #[tokio::test]
    async fn test_resolve_secret_ref_file() {
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(b"file-secret-456\n").unwrap();

        let mut config = AppConfig::default();
        config.core.dag_executor.redis_url = format!("secret://file/{}", tmpfile.path().display());
        resolve_secret_refs(&mut config).await.unwrap();
        assert_eq!(config.core.dag_executor.redis_url, "file-secret-456");
    }

    #[tokio::test]
    async fn test_resolve_secret_ref_unresolved_stays() {
        let _g1 = {
            let _lock = ENV_LOCK.lock().unwrap();
            EnvGuard::remove("DEFINITELY_MISSING_SECRET")
        };

        let mut config = AppConfig::default();
        config.core.dag_executor.redis_url = "secret://env/DEFINITELY_MISSING_SECRET".into();
        resolve_secret_refs(&mut config).await.unwrap();
        assert_eq!(
            config.core.dag_executor.redis_url,
            "secret://env/DEFINITELY_MISSING_SECRET"
        );
    }

    #[tokio::test]
    async fn test_resolve_secret_ref_vault_without_env_errors() {
        let (_g1, _g2) = {
            let _lock = ENV_LOCK.lock().unwrap();
            (
                EnvGuard::remove("VAULT_ADDR"),
                EnvGuard::remove("VAULT_TOKEN"),
            )
        };

        let err = resolve_secret_ref("vault/secret/data/llm#api_key")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("VAULT_ADDR"));
    }
    #[test]
    fn test_demo_login_env_override() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvGuard::set("COGNEVA_DEMO_LOGIN_ENABLED", "true");
        let config = from_env();
        assert!(config.gateway.demo_login_enabled);
    }

    /// The main app pod runs as a control plane: it keeps
    /// `self_evolution.enabled` true (so it still publishes the admin API and
    /// runs the cluster-side GitOps puller) but sets EXECUTOR_ENABLED=false to
    /// stop spawning change-execution loops. That override must survive the env
    /// layer — otherwise the main app keeps running an evolution cycle every
    /// poll and races the dedicated evolution worker over the shared instance
    /// fingerprint, bare repo, and workspace worktrees (the "missing but
    /// already registered worktree" failure that never lands a change).
    #[test]
    fn test_self_evolution_executor_enabled_env_override() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _g_enabled = EnvGuard::set("COGNEVA_SELF_EVOLUTION_ENABLED", "true");
        let _g_exec = EnvGuard::set("COGNEVA_SELF_EVOLUTION_EXECUTOR_ENABLED", "false");
        let config = from_env();
        assert!(
            config.self_evolution.enabled,
            "control plane stays mounted: enabled must remain true"
        );
        assert!(
            !config.self_evolution.executor_enabled,
            "EXECUTOR_ENABLED=false must reach self_evolution.executor_enabled"
        );
    }

    /// With no env override the executor stays on, so a single-binary / non-K8s
    /// host keeps evolving with zero extra wiring.
    #[test]
    fn test_self_evolution_executor_enabled_defaults_true() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _g = EnvGuard::remove("COGNEVA_SELF_EVOLUTION_EXECUTOR_ENABLED");
        let config = from_env();
        assert!(config.self_evolution.executor_enabled);
    }

    /// A config file that exists but is not valid JSON is a reportable failure,
    /// not an empty layer: `load()` used to swallow it and hand back defaults.
    #[test]
    fn test_try_load_reports_a_present_but_unparsable_file() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cogneva.json");
        // The exact state a truncate-then-write leaves behind on a modify event.
        std::fs::File::create(&path).unwrap();
        let _g = EnvGuard::set("COGNEVA_CONFIG_PATH", &path.to_string_lossy());

        let err = try_load().expect_err("an unparsable config must not load as Ok");
        let msg = err.to_string();
        assert!(
            msg.contains("not valid JSON"),
            "the error must name the cause, got: {msg}"
        );
        assert!(
            msg.contains("cogneva.json"),
            "the error must name the file, got: {msg}"
        );
    }

    /// An absent file is the normal state of an optional layer, not a failure.
    #[test]
    fn test_try_load_treats_an_absent_file_as_no_layer() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        let _g = EnvGuard::set("COGNEVA_CONFIG_PATH", &path.to_string_lossy());

        let config = try_load().expect("an absent config file is not an error");
        // Layer 0 defaults still stand.
        assert_eq!(config.app.name, AppConfig::default().app.name);
    }

    /// The boot-time entry point keeps its old contract: unusable file, still
    /// returns a usable config, env overrides still applied.
    #[test]
    fn test_load_falls_back_to_defaults_and_still_applies_env() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cogneva.json");
        std::fs::write(&path, b"{ this is not json").unwrap();
        let _g = EnvGuard::set("COGNEVA_CONFIG_PATH", &path.to_string_lossy());
        let _g2 = EnvGuard::set("COGNEVA_APP_NAME", "from-env");

        let config = load();
        assert_eq!(
            config.app.name, "from-env",
            "env is the last-resort surface and must survive an unusable file"
        );
    }
}
