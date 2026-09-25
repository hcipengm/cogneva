//! The delivered configuration document has a judge, and the judge runs.
//!
//! The judgement is deliberately not a configured rule — a rule saying the rule
//! set did not arrive would travel in the rule set that did not arrive — so none
//! of the alert tables fail when the call disappears. This gate is what stands
//! in for them: the four links of the chain are read out of the sources, because
//! every one of them can be dropped by an edit that compiles.
//!
//! The links are: the reference document is the one the deployment delivers, the
//! path is the one the process reads, the reading is judged, and the result
//! reaches the alert row. A check missing any of them reports nothing while
//! looking installed.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// The reference side is the document the deployment delivers.
///
/// It is embedded from the chart's copy rather than a copy inside the crate, so
/// the reference and what the ConfigMap carries are the same file: a second copy
/// would drift, and a drift here reports differences that are not there.
#[test]
fn the_reference_document_is_the_one_the_deployment_delivers() {
    let source = read("crates/cog-observability/src/config.rs");
    let needle = r#"include_str!("../../../deploy/helm/cogneva/files/cogneva.json")"#;
    assert!(
        source.contains(needle),
        "声明的配置文档不再从部署实际下发的那份嵌入（找不到 {needle}）"
    );
    let delivered = repo_root().join("deploy/helm/cogneva/files/cogneva.json");
    assert!(delivered.is_file(), "{} 不存在", delivered.display());
    let text = std::fs::read_to_string(&delivered).unwrap();
    let document: serde_json::Value = serde_json::from_str(&text).expect("部署下发的文档是 JSON");
    assert!(document.is_object());
}

/// Both delivery carriers are the same document, by the judge's own criterion.
///
/// The check compares a delivered document against the chart's copy, and the
/// k3s carrier is the other document the deployment can deliver. Field parity
/// already holds them together; this reads the second one through the same
/// comparison the runtime uses, so the criterion itself is exercised on a real
/// document rather than only on fixtures.
#[test]
fn the_k3s_carrier_is_the_declared_document() {
    let carrier = read("deploy/k3s/cogneva-json-configmap.yaml");
    let (_, body) = carrier
        .split_once("cogneva.json: |")
        .expect("k3s 清单里没有 cogneva.json 块标量");
    let indented: Vec<&str> = body.lines().skip(1).collect();
    let indent = indented
        .iter()
        .find(|line| !line.trim().is_empty())
        .map(|line| line.len() - line.trim_start().len())
        .expect("块标量为空");
    let document: String = indented
        .iter()
        .map(|line| line.get(indent..).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");

    let declared = read("deploy/helm/cogneva/files/cogneva.json");
    assert_eq!(
        cog_core::config_sections::judge_delivery(Some(&document), &declared),
        cog_core::config_sections::DocumentDelivery::Matches,
        "k3s 载体与 chart 声明的文档不是同一份"
    );
}

/// The judged path is the path this process reads.
///
/// One resolution, used by the loader and by the check: two copies of the path
/// let the check judge a file the process never reads, and pass while the
/// running configuration is something else entirely.
#[test]
fn the_judged_path_is_the_one_the_process_reads() {
    let source = read("crates/cog-observability/src/config.rs");
    assert!(source.contains("pub fn config_path()"));
    assert!(
        source.contains("Self::load_from(&config_path())"),
        "配置加载不再走与投递判据同一个 path 解析"
    );
    let delivery = read("crates/cog-observability/src/config_delivery.rs");
    assert!(
        delivery.contains("crate::config::config_path()"),
        "投递判据自己解析路径，而没走加载器用的那一处"
    );
}

/// The reading is judged, and the judgement reaches the alert row.
///
/// The judge itself is covered by unit tests on both sides; what cannot be
/// covered there is whether anything calls it, which is what this link is.
#[test]
fn the_plugin_runs_the_check_and_persists_its_verdict() {
    let plugin = read("crates/cog-observability/src/plugin.rs");
    assert!(
        plugin.contains("run_config_declaration_check("),
        "插件不再启动投递判据：文档没到这件事重新变得无人观测"
    );
    assert!(
        plugin.contains("self.alert_store.clone()"),
        "投递判据拿不到 alert store，判定只会进日志"
    );
    assert!(
        plugin.contains("config_declaration"),
        "投递判据的节奏不再来自配置面"
    );
}

/// The row this check raises is the one the human surface serves.
///
/// The durable rows reach `/api/v1/alerts/active` through the source the alert
/// store publishes; a rule name that the gateway filtered out would leave the
/// check reporting into nothing. The merge is by rule name only, so this asserts
/// the producer side is wired, not a name list on the consumer side.
#[test]
fn the_alert_store_is_published_to_the_surface_that_serves_it() {
    let plugin = read("crates/cog-observability/src/plugin.rs");
    assert!(
        plugin.contains("ctx.publish_service(source)") && plugin.contains("ActiveAlertSource"),
        "alert store 不再作为 ActiveAlertSource 发布，持久化的行到不了人看的那一面"
    );
}
