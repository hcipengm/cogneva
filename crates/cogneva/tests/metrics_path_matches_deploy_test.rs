//! 部署侧拼 `/metrics` 的 URL 与 Rust 常量必须指向同一个值。
//!
//! 生产源里路径只声明一次（`cog-core::contract::observability::METRICS_PATH`），
//! 但 Helm 读不到 Rust 常量，部署侧的 URL 只能从 chart 的值拼。两边各写一份时，
//! 改路径不会报错：Rust 在旧路径上答、抓取端在新路径上拿 404，读数看起来只是
//! 「这个 Pod 很安静」。所以这里把 chart 的值与 Rust 常量逐字比对，并把三个
//! 拼 URL 的站点钉在同一个 values 键上——部署侧不做 import，靠的就是这条门。
//!
//! `templates/postgres.yaml` 的路径属于 Postgres exporter 自己的面，不在此列；
//! 渲染产物由 `render-deploy.sh --check` 判新鲜度，也不在这里重复。

use crate::common;

use common::routes::read_repo;
use common::yaml::values_entry;

const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const VALUES_BLOCK: &str = "observability:";
const VALUES_KEY: &str = "metricsPath:";
const CHART_VALUE: &str = ".Values.observability.metricsPath";

const EVOLUTION: &str = "deploy/helm/cogneva/templates/evolution.yaml";
const GATEWAY: &str = "deploy/helm/cogneva/templates/gateway.yaml";
const APP_CONFIG: &str = "deploy/helm/cogneva/templates/app-config.yaml";

#[test]
fn the_chart_and_the_rust_constant_name_the_same_metrics_path() {
    let chart_path = values_entry(VALUES_YAML, VALUES_BLOCK, VALUES_KEY).unwrap_or_else(|| {
        panic!(
            "values.yaml 的 {VALUES_BLOCK} 段里没有 {VALUES_KEY}：引用不存在的键会渲染成空串，\
             抓取地址会静默落回旧路径"
        )
    });
    assert_eq!(
        chart_path,
        cog_core::contract::observability::METRICS_PATH,
        "chart 的 {VALUES_KEY} 与 Rust 常量不是同一个值：部署侧会去抓旧路径，\
         而进程在另一个路径上答"
    );
}

#[test]
fn every_deploy_side_scrape_url_reads_the_chart_value() {
    for (file, marker) in [
        (EVOLUTION, "COGNEVA_CLAIM_STORE_METRICS_URL"),
        (GATEWAY, "COGNEVA_GITOPS_METRICS_URL"),
        (APP_CONFIG, "\"localhost:8080/metrics\""),
    ] {
        let text = read_repo(file);
        let lines: Vec<&str> = text.lines().collect();
        let index = lines
            .iter()
            .position(|line| line.contains(marker))
            .unwrap_or_else(|| {
                panic!("{file} 里找不到 {marker}：抓取地址被搬走或改名，本门盯着的站点不在了")
            });
        // env 名与它的值不在同一行：名字是 `- name:`，取值是下一行。两行一起看，
        // 但不越到更远的地方——越远就可能读到隔壁无关的 `.Values` 引用。
        let site = lines[index..lines.len().min(index + 2)].join("\n");
        assert!(
            site.contains(CHART_VALUE),
            "{file} 的 {marker} 没有从 {CHART_VALUE} 拼（这一处是 {site:?}）：\
             改 chart 的值不会改到这个 URL"
        );
    }
}

#[test]
fn the_templates_do_not_compose_a_second_metrics_path() {
    for file in [EVOLUTION, GATEWAY] {
        let text = read_repo(file);
        let offenders: Vec<String> = text
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim_start().starts_with('#'))
            .filter(|(_, line)| line.contains("/metrics"))
            .map(|(number, line)| format!("{file}:{}: {line}", number + 1))
            .collect();
        assert!(
            offenders.is_empty(),
            "部署侧还在自己拼指标路径；改 chart 的值不会改到它们：\n{}",
            offenders.join("\n")
        );
    }

    // app-config 的端口替换表要按 `files/cogneva.json` 里的字面量做 `from` 匹配，
    // 所以那一行会带一个源字面量；允许它，是因为同一行右侧的 `to` 是从 chart 取的。
    let text = read_repo(APP_CONFIG);
    let offenders: Vec<String> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim_start().starts_with('#'))
        .filter(|(_, line)| line.contains("/metrics"))
        .filter(|(_, line)| !line.contains(CHART_VALUE))
        .map(|(number, line)| format!("{APP_CONFIG}:{}: {line}", number + 1))
        .collect();
    assert!(
        offenders.is_empty(),
        "app-config 的指标路径没有全部从 chart 取：\n{}",
        offenders.join("\n")
    );
}
