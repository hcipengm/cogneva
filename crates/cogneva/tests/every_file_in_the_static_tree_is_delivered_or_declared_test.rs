//! principle: feedback_no_hardcoding_config_surface
//! `deploy/k3s/kustomization.yaml` 的 `resources` 列表是这棵树交付的对象集，不是
//! 一份用途不明的索引：`kubectl apply -k` 按它投递，parity 门禁按 `kubectl kustomize`
//! 渲染它再与 chart 比字段，痕迹探测面的 kind 也从它读。目录里多出一份 `.yaml` 而
//! 没进这份列表时，三个面会一起看不见它——apply 不投递它、parity 不与 chart 比它、
//! 探测面不为它的种类兜底——而「文件写在树上」看起来完全像「已经交付」。
//!
//! 这个洞是静默的：漏投递一份清单不会让任何门变红，只会让集群里少一个对象，少的那
//! 个对象与「这一版本来就不需要它」在读数上分不开。所以盘上的每一份清单都要在这里
//! 表态：要么进 `resources`，要么进下面那张注明理由的非交付表。

mod common;

use common::routes::repo_root;

use std::collections::BTreeSet;

/// 盘上不是交付清单的文件，逐个写理由。
///
/// 两张表都只登记「盘上真有这个文件」的项：一条理由留在这里而文件已经没了，说明
/// 这行在替一个不存在的决定辩护，下一次真正需要表态的新文件会照着它的形状被漏掉。
const NOT_DELIVERED: [(&str, &str); 2] = [
    (
        "kustomization.yaml",
        "这张列表本身；它是读法，不是被读的对象",
    ),
    (
        "ingress-nginx-values.yaml",
        "ingress-nginx 控制器的参数集，由 helm 读；本树从未装过该控制器，\
         交付面不该 apply 一份没有对象体的 values",
    ),
];

fn resources(k3s: &std::path::Path) -> BTreeSet<String> {
    let text = std::fs::read_to_string(k3s.join("kustomization.yaml"))
        .unwrap_or_else(|e| panic!("读不到 deploy/k3s/kustomization.yaml：{e}"));
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&text).expect("deploy/k3s/kustomization.yaml 不可解析");
    doc["resources"]
        .as_sequence()
        .expect("kustomization.yaml 没有 resources 列表——这一趟判的不是交付集")
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .expect("resources 里有一项不是文件名")
                .to_string()
        })
        .collect()
}

#[test]
fn every_yaml_in_the_static_tree_is_delivered_or_declared_not_delivered() {
    let k3s = repo_root().join("deploy/k3s");
    let declared = resources(&k3s);

    let on_disk: BTreeSet<String> = std::fs::read_dir(&k3s)
        .unwrap_or_else(|e| panic!("{}: {e}", k3s.display()))
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                return None;
            }
            Some(path.file_name()?.to_string_lossy().into_owned())
        })
        .collect();

    assert!(
        on_disk.len() >= 40 && declared.len() >= 40,
        "deploy/k3s 只读出 {} 份 yaml、resources 只读出 {} 项——数量这么少，\
         这一趟大概率什么都没比",
        on_disk.len(),
        declared.len()
    );

    let excused: BTreeSet<&str> = NOT_DELIVERED.iter().map(|(file, _)| *file).collect();
    let undelivered: Vec<&String> = on_disk
        .iter()
        .filter(|file| !declared.contains(*file) && !excused.contains(file.as_str()))
        .collect();
    assert!(
        undelivered.is_empty(),
        "这些清单在 deploy/k3s 下，却不在 kustomization 的 resources 里，也没在\
         NOT_DELIVERED 表上表态：{undelivered:?}——apply -k 不投递它们，parity 不\
         拿它们与 chart 比，痕迹探测面也不认它们声明的种类；集群里少这几个对象\
         不会让任何门禁变红"
    );

    let missing: Vec<&String> = declared
        .iter()
        .filter(|file| !k3s.join(*file).is_file())
        .collect();
    assert!(
        missing.is_empty(),
        "resources 点名了这些文件，盘上却没有：{missing:?}——`apply -k` 会在这里\
         报错，而报的头一个错是一句路径，不是「少了一份清单」"
    );

    let stale: Vec<&str> = NOT_DELIVERED
        .iter()
        .map(|(file, _)| *file)
        .filter(|file| !on_disk.contains(*file))
        .collect();
    assert!(
        stale.is_empty(),
        "NOT_DELIVERED 里登记的文件已经不在 deploy/k3s 下了：{stale:?}——留着这条\
         理由，下一个真要表态的文件会以为这张表不要求它出现在盘上"
    );
}
