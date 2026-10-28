//! principle: feedback_no_hardcoding_config_surface
//! Code that asks "are this workload's pods ready" or "restart the workload that
//! serves the images" selects by two pod labels. A selector naming a value no
//! manifest carries matches nothing, and `kubectl get -l` answers an empty list
//! as an ordinary result: the readiness reading says "no pods yet", the restart
//! says it succeeded, and neither distinguishes a workload that has not started
//! from a label the chart renamed two commits ago. So the pairs are declared
//! once in `cog_core::topology` and checked here against the rendered manifests
//! the deployment actually applies.
//!
//! The pairs are read from the declaration rather than written into this file: a
//! list here would be the same kind of second copy, and it would be the copy
//! nobody updates when a workload is renamed.
//!
//! The object keys take the same treatment, and the annotation key is the one
//! whose rename is quietest: the jobs two crates create carry their trace id in
//! it, and a key spelled differently at one writer does not error — the stage
//! simply stops appearing in the trace, so the chain reads as shorter instead of
//! broken. So a source census keeps every crate typing the label keys and the
//! annotation key from the declaration.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

mod common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, relative_name};

/// The `(name, component)` pairs the code selects a workload's pods by.
fn declared_pairs() -> Vec<(&'static str, &'static str)> {
    use cog_core::topology as t;
    vec![
        (t::WORKLOAD_NAME, t::COMPONENT_GATEWAY),
        (t::WORKLOAD_NAME, t::COMPONENT_SECURITY_GATEWAY),
        (t::WORKLOAD_NAME, t::COMPONENT_SANDBOX_EXECUTOR),
        (t::EVOLUTION, t::COMPONENT_EVOLUTION),
        (t::WORKLOAD_NAME, t::COMPONENT_REGISTRY),
    ]
}

fn yaml_files(dir: &Path, out: &mut Vec<PathBuf>) {
    // 读不出来就 panic：递归遍历里跳过一个子目录不会让任何断言变红，而剩下的那份
    // 普查往往仍高于下限——「少读了一块」与「那一块都合规」在判词上分不开。
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| {
        panic!(
            "{} 读不出来（{e}）：这棵树的普查面少了一块，而用剩下的那份判出来的\
             结论与「整片区都合规」同形",
            dir.display()
        )
    });
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            yaml_files(&path, out);
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("yaml") | Some("yml")
        ) {
            out.push(path);
        }
    }
}

/// One label value out of a workload document's Pod template.
fn label<'a>(doc: &'a serde_yaml::Value, key: &str) -> Option<&'a str> {
    doc.get("spec")?
        .get("template")?
        .get("metadata")?
        .get("labels")?
        .get(key)?
        .as_str()
}

/// Every `(name, component)` pair the documents in this text carry on a Pod
/// template, and how many documents had a Pod template at all.
fn pairs_in(text: &str) -> (usize, BTreeSet<(String, String)>) {
    let mut found = BTreeSet::new();
    let mut templates = 0;
    for doc in serde_yaml::Deserializer::from_str(text) {
        let Ok(doc) = serde_yaml::Value::deserialize(doc) else {
            continue;
        };
        if doc.get("spec").and_then(|s| s.get("template")).is_none() {
            continue;
        }
        templates += 1;
        if let (Some(name), Some(component)) = (
            label(&doc, cog_core::topology::LABEL_NAME),
            label(&doc, cog_core::topology::LABEL_COMPONENT),
        ) {
            found.insert((name.to_string(), component.to_string()));
        }
    }
    (templates, found)
}

/// The declared pairs this set does not carry.
fn missing<'a>(
    declared: &[(&'a str, &'a str)],
    carried: &BTreeSet<(String, String)>,
) -> Vec<(&'a str, &'a str)> {
    declared
        .iter()
        .copied()
        .filter(|(name, component)| !carried.contains(&(name.to_string(), component.to_string())))
        .collect()
}

#[test]
fn every_pair_the_code_selects_on_is_one_the_manifests_carry() {
    let mut profiles: Vec<PathBuf> = std::fs::read_dir(repo_root().join("deploy/rendered"))
        .expect("deploy/rendered 不在仓库里")
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    profiles.sort();
    assert!(
        !profiles.is_empty(),
        "deploy/rendered/ 下一个形态都没有，这条判据没有题目"
    );

    let declared = declared_pairs();
    for profile in profiles {
        let mut files = Vec::new();
        yaml_files(&profile, &mut files);
        let mut carried = BTreeSet::new();
        let mut templates = 0;
        for path in &files {
            let text =
                std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let (count, pairs) = pairs_in(&text);
            templates += count;
            carried.extend(pairs);
        }
        assert!(
            templates > 0,
            "{} 里没有一个带 Pod 模板的工作负载，这条判据没有题目",
            profile.display()
        );
        let absent = missing(&declared, &carried);
        assert!(
            absent.is_empty(),
            "{} 里没有这些标签对：{absent:?}。代码按它们选 Pod——选择器匹配不到任何东西时\
             返回的是空名单，与「这个工作负载还没起来」同形，所以改名不会报错，只会让\
             就绪读数一直是空的、重启说它成功了。",
            profile.display()
        );
    }
}

#[test]
fn the_deployer_defaults_select_by_the_declared_pairs() {
    let declared: BTreeSet<(String, String)> = declared_pairs()
        .into_iter()
        .map(|(name, component)| (name.to_string(), component.to_string()))
        .collect();
    let targets = cog_reflection::MainlineDeployerConfig::default().targets;
    assert!(!targets.is_empty(), "默认滚动目标为空，这条判据没有题目");
    for target in &targets {
        let pair = (target.name.clone(), target.component.clone());
        assert!(
            declared.contains(&pair),
            "滚动目标 {} 按 {pair:?} 选 Pod，而声明里没有这一对：\
             选择器会匹配不到任何东西，而后端走到的是「等就绪」那条路——\
             等到超时，判败，回滚。要改的是 cog_core::topology 里的声明，不是这里的默认值。",
            target.deployment
        );
    }
}

/// The one file allowed to spell the object keys.
const DECLARATION: &str = "crates/cog-core/src/topology.rs";

fn offences(text: &str) -> Vec<String> {
    let mut hits = Vec::new();
    for line in production_lines(text) {
        for key in [
            cog_core::topology::LABEL_NAME,
            cog_core::topology::LABEL_COMPONENT,
            cog_core::topology::TRACEPARENT_ANNOTATION,
        ] {
            if line.contains(key) {
                hits.push(line.trim().to_string());
                break;
            }
        }
    }
    hits
}

#[test]
fn only_the_declaration_spells_an_object_key() {
    let root = repo_root();
    let mut hits = Vec::new();
    for path in production_sources(&root) {
        let rel = relative_name(&root, &path);
        if rel == DECLARATION {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        hits.extend(
            offences(&text)
                .into_iter()
                .map(|hit| format!("{rel}: {hit}")),
        );
    }
    assert!(
        hits.is_empty(),
        "这些地方自己写了一遍对象键（标签键或 trace 注解键），没读 {DECLARATION} 里的声明。\
         键写错一个字符时选择器只是匹配不到东西：返回空名单、退出码 0，\
         读出来是「没有这个工作负载」；而注解键写错的那一半只是让某一段从追踪链上\
         消失——两种都不是「键写错了」。\n{}",
        hits.join("\n")
    );
}

#[test]
fn the_census_reddens_on_code_and_ignores_comments_and_fixtures() {
    let key = cog_core::topology::LABEL_COMPONENT;
    let text = format!(
        "/// 说明里提到 {key} 不算。\n\
         pub fn selector() -> String {{ format!(\"{key}={{}}\", \"x\") }}\n\
         // {key} 出现在注释里也不算\n\
         #[cfg(test)]\n\
         mod tests {{ const K: &str = \"{key}\"; }}\n"
    );
    let hits = offences(&text);
    assert_eq!(hits.len(), 1, "应当只留下那行代码，实得 {hits:?}");
    assert!(
        hits[0].contains("pub fn selector()"),
        "留下的应当是代码行，实得 {hits:?}"
    );

    // 注解键走同一条判据：两个写者在一个 crate 里、读者在另一个 crate 里，
    // 单改一处不会报错，只会让那一段从链上消失。
    let annotation = cog_core::topology::TRACEPARENT_ANNOTATION;
    let text = format!("pub fn annotate() {{ let _ = \"{{}}\"; }}\n// {annotation}\n");
    let hits = offences(&text.replace("let _ = \"{}\"", &format!("let _ = \"{annotation}\"")));
    assert_eq!(hits.len(), 1, "注解键也应当留下那行代码，实得 {hits:?}");
}

#[test]
fn a_pair_no_manifest_carries_is_reported() {
    let text = "\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cogneva-security-gateway
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/name: cogneva
        app.kubernetes.io/component: security-gateway
";
    let (templates, carried) = pairs_in(text);
    assert_eq!(templates, 1, "这份文档有一个 Pod 模板");
    let declared = declared_pairs();
    let absent = missing(&declared, &carried);
    assert_eq!(
        absent.len(),
        declared.len() - 1,
        "只带了安全网关那一对，其余都该报出来：{absent:?}"
    );
    assert!(
        !absent
            .iter()
            .any(|(_, component)| *component == cog_core::topology::COMPONENT_SECURITY_GATEWAY),
        "带上了的那一对不该报：{absent:?}"
    );
}
