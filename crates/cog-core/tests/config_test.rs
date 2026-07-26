use cog_core::Config;

#[test]
fn test_config_default() {
    let config = Config::default();
    // Config::default() returns zero-values; real defaults come from config files
    assert_eq!(config.app.name, "");
    assert_eq!(config.app.version, "");
    assert_eq!(config.app.log_level, "");
    assert_eq!(config.app.data_dir, "");
    assert_eq!(config.app.config_dir, "");
    assert_eq!(config.app.app_dir, "");
    assert_eq!(config.gateway.http_port, 0);
    assert!(!config.raw_logger.enabled);
}
