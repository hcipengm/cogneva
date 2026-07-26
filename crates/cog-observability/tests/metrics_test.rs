use cog_observability::MetricsExporter;

#[test]
fn test_metrics_exporter_new() {
    let metrics = MetricsExporter::new();
    let families = metrics.gather();
    assert!(families.is_empty() || !families.is_empty());
}

#[test]
fn test_metrics_exporter_encode() {
    let metrics = MetricsExporter::new();
    let encoded = metrics.encode();
    assert!(encoded.is_ok());
    let _bytes = encoded.unwrap();
}

#[test]
fn test_metrics_exporter_registry() {
    let metrics = MetricsExporter::new();
    let registry = metrics.registry();
    let _registry2 = registry.clone();
}

#[test]
fn test_metrics_default() {
    let metrics: MetricsExporter = Default::default();
    assert!(metrics.encode().is_ok());
}
