//! 原则密封包的**格式与完整性**：正文＋索引，按内容哈希取回时先核哈希。
//!
//! 签名与加密是外层的事（发布 CI 用发布密钥签、平台公钥加；平台启动时验签解密），
//! 这一层只回答「解出来的这包字节，是不是我们按哈希点名的那一包」。分开是因为两件
//! 事会各自坏：哈希对而签名不对是「包被换过」，签名对而哈希不对是「取的包坏了」。
//!
//! 包里的每一段正文与 memory 的原文件逐字相同；`carrier_sha256` 是那段正文的哈希，
//! 与声明事件里的同一格对得上——包的版本换了，哈希不变的那些原则仍是同一段文字。

use serde::{Deserialize, Serialize};

/// 这一版代码认识的包版本。
pub const BUNDLE_VERSION: u32 = 1;

/// 一条原则在包里的样子。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundledPrinciple {
    pub slug: String,
    /// 哪一份读数命名了它（`list-principles.sh` 之类）。
    pub source: String,
    /// 规则自己的那一句。
    pub lead: String,
    /// 载体在 memory 里的路径。
    pub carrier_path: String,
    /// 载体正文的哈希；与声明事件里的同一格是同一个值。
    pub carrier_sha256: String,
    /// 门在哪：主机侧注册表里的名字。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_gate: Option<String>,
    /// 门在树里的位置：文件、测试或命令。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_red: Option<String>,
    /// 检查方式（shape/tree/live/judgement）；`None` 是没声明。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_kind: Option<String>,
    /// 优先级词（unstated/p0..p3）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
    /// 主题词：命中要改的路径时用它决定取不取这一条；空表示没有主题。
    #[serde(default)]
    pub themes: Vec<String>,
    /// 正文原文，逐字。
    pub body: String,
}

/// 一个密封包：版本、构建时刻、逐条原则。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleBundle {
    pub version: u32,
    /// 构建那一刻（RFC 3339）；只用于人读与告警，不参与判定。
    pub built_at: String,
    pub principles: Vec<BundledPrinciple>,
}

/// 一段文字里点到的树内路径（`crates/…`、`deploy/…`、`scripts/…`）。
pub fn paths_in(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    // 中文标点也要切：原则正文是中文，`deploy/helm/x.yaml，不改…` 里的全角逗号
    // 不切开时，抽出来的“路径”后面会拖着半句中文，读者按路径去查就查不到。
    for token in text.split(|c: char| {
        c.is_whitespace()
            || matches!(c, '`' | '\'' | '"' | ',' | '(' | ')' | ':')
            || matches!(c, '，' | '、' | '；' | '。' | '：' | '（' | '）')
    }) {
        let token = token.trim_end_matches(|c: char| c == '.' || c == ';');
        if token.starts_with("crates/")
            || token.starts_with("deploy/")
            || token.starts_with("scripts/")
        {
            if !out.iter().any(|seen| seen == token) {
                out.push(token.to_string());
            }
        }
    }
    out
}

/// 这一段正文里命中的原则：主题词出现在某个路径里，或门的目录与某个路径同前缀。
pub fn select_for_paths<'a>(
    bundle: &'a PrincipleBundle,
    touched: &[String],
) -> Vec<&'a BundledPrinciple> {
    bundle
        .principles
        .iter()
        .filter(|item| {
            let by_theme = !item.themes.is_empty()
                && touched.iter().any(|path| {
                    item.themes
                        .iter()
                        .any(|theme| !theme.is_empty() && path.contains(theme))
                });
            let by_gate = item
                .where_red
                .as_deref()
                .map(|gate| {
                    let gate_dir = gate.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(gate);
                    touched
                        .iter()
                        .any(|path| path.starts_with(gate_dir) || gate.starts_with(path.as_str()))
                })
                .unwrap_or(false);
            by_theme || by_gate
        })
        .collect()
}

impl PrincipleBundle {
    /// 按 slug 找一条；没有就是没有，不编一条空的。
    pub fn principle(&self, slug: &str) -> Option<&BundledPrinciple> {
        self.principles.iter().find(|item| item.slug == slug)
    }
}

/// 解包并核哈希：字节的 sha256 必须等于调用方点名的那个。
///
/// 三件事分开报：哈希不符、不是 JSON、版本不认识。合成一句「包坏了」会让三种修法
/// 变成一个。
pub fn parse_bundle(bytes: &[u8], expected_sha256: &str) -> Result<PrincipleBundle, String> {
    use sha2::{Digest, Sha256};
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual != expected_sha256 {
        return Err(format!(
            "bundle bytes hash to {actual}, not the named {expected_sha256}"
        ));
    }
    let bundle: PrincipleBundle =
        serde_json::from_slice(bytes).map_err(|e| format!("bundle is not JSON: {e}"))?;
    if bundle.version != BUNDLE_VERSION {
        return Err(format!(
            "bundle version {} is not the {BUNDLE_VERSION} this build reads",
            bundle.version
        ));
    }
    for item in &bundle.principles {
        if item.slug.trim().is_empty() {
            return Err("a bundled principle has no slug".into());
        }
        if item.body.is_empty() {
            return Err(format!("bundled principle {} has no body", item.slug));
        }
        use sha2::{Digest, Sha256};
        let body_hash = format!("{:x}", Sha256::digest(item.body.as_bytes()));
        if body_hash != item.carrier_sha256 {
            return Err(format!(
                "bundled principle {} carries a body whose sha256 is {body_hash}, not {}",
                item.slug, item.carrier_sha256
            ));
        }
    }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle() -> PrincipleBundle {
        let body = "数值不硬编码。".to_string();
        use sha2::{Digest, Sha256};
        let sha = format!("{:x}", Sha256::digest(body.as_bytes()));
        PrincipleBundle {
            version: BUNDLE_VERSION,
            built_at: "2026-11-25T00:00:00Z".into(),
            principles: vec![BundledPrinciple {
                slug: "no-hardcoded-knobs".into(),
                source: "list-principles.sh".into(),
                lead: "数值不硬编码".into(),
                carrier_path: "memory/principles/x.md".into(),
                carrier_sha256: sha,
                host_gate: Some("tunable_registry".into()),
                where_red: Some("crates/x/tests/y.rs".into()),
                check_kind: Some("tree".into()),
                priority: Some("p1".into()),
                themes: vec!["context".into()],
                body,
            }],
        }
    }

    fn bytes_of(bundle: &PrincipleBundle) -> (Vec<u8>, String) {
        use sha2::{Digest, Sha256};
        let bytes = serde_json::to_vec(bundle).unwrap();
        let sha = format!("{:x}", Sha256::digest(&bytes));
        (bytes, sha)
    }

    #[test]
    fn paths_are_extracted_only_inside_the_tree() {
        let text = "改 `crates/cog-agent/src/context.rs` 与 deploy/helm/x.yaml，不改 /etc/passwd";
        let paths = paths_in(text);
        assert!(paths.contains(&"crates/cog-agent/src/context.rs".to_string()));
        assert!(paths.contains(&"deploy/helm/x.yaml".to_string()));
        assert!(!paths.iter().any(|p| p.contains("/etc/")));
    }

    #[test]
    fn a_well_formed_bundle_parses_and_is_findable_by_slug() {
        let (bytes, sha) = bytes_of(&bundle());
        let parsed = parse_bundle(&bytes, &sha).expect("a clean bundle parses");
        assert_eq!(
            parsed.principle("no-hardcoded-knobs").unwrap().lead,
            "数值不硬编码"
        );
        assert!(parsed.principle("nope").is_none());
    }

    #[test]
    fn a_swapped_bundle_is_named_as_a_hash_mismatch() {
        let (bytes, _) = bytes_of(&bundle());
        let err = parse_bundle(&bytes, &"0".repeat(64)).unwrap_err();
        assert!(err.contains("hash"), "哈希不符要单独报出来：{err}");
    }

    #[test]
    fn a_body_that_does_not_match_its_hash_is_refused() {
        let mut bundle = bundle();
        bundle.principles[0].body.push_str("（改过）");
        let (bytes, sha) = bytes_of(&bundle);
        let err = parse_bundle(&bytes, &sha).unwrap_err();
        assert!(err.contains("sha256"), "正文与哈希不符要单独报出来：{err}");
    }

    #[test]
    fn an_unknown_version_is_not_read_as_this_one() {
        let mut bundle = bundle();
        bundle.version = 99;
        let (bytes, sha) = bytes_of(&bundle);
        let err = parse_bundle(&bytes, &sha).unwrap_err();
        assert!(err.contains("version"), "版本不认识要单独报出来：{err}");
    }
}
