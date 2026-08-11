//! LLM 驱动的 Wiki 维护者 — LLM_WIKI 理念的核心落地。
//!
//! 页面持久化策略：
//! - `raw/{source}` 原始来源（不可变，ingest 时写入）
//! - `pages/{id}.json` 页面结构（WikiPage 序列化，机器可读真源）
//! - `pages/{id}.md` 页面渲染（人类可读）
//! - `index.md` 内容目录（每次 ingest 后重建）
//! - `log.md` 操作时间线（append-only）

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio::sync::Mutex;

use cog_core::{
    execute_structured, ChatOptions, LlmClient, Message, SFError, SFResult, WikiBackend,
};

use crate::maintainer::{
    ContradictionReport, ContradictionSeverity, DataGap, IngestReport, LintReport, MissingCrossRef,
    OutdatedPage, QueryResult, WikiMaintainer,
};
use crate::page::{WikiPage, WikiPageType};

/// 孤立页/过时页判定阈值（天）。
const OUTDATED_THRESHOLD_DAYS: i64 = 30;

pub struct LlmWikiMaintainer {
    backend: Arc<dyn WikiBackend>,
    llm: Arc<dyn LlmClient>,
    /// 串行化写操作，避免并发 ingest 互相覆盖 index/log。
    write_lock: Mutex<()>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct EntityExtract {
    /// 实体/概念名称
    name: String,
    /// "entity" 或 "concept"
    kind: String,
    /// 一句话概述
    summary: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SourceAnalysis {
    /// 摘要页标题
    summary_title: String,
    /// 来源摘要（markdown）
    summary: String,
    /// 主题标签
    tags: Vec<String>,
    /// 来源中提到的关键实体与概念
    entities: Vec<EntityExtract>,
    /// 来源做出的关键主张（用于矛盾检测）
    claims: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ContradictionItem {
    existing_claim: String,
    new_claim: String,
    /// low / medium / high / critical
    severity: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ContradictionScan {
    contradictions: Vec<ContradictionItem>,
}

impl LlmWikiMaintainer {
    pub fn new(backend: Arc<dyn WikiBackend>, llm: Arc<dyn LlmClient>) -> Self {
        Self {
            backend,
            llm,
            write_lock: Mutex::new(()),
        }
    }

    // ── 页面持久化 ──────────────────────────────────────────────

    fn page_json_path(id: &str) -> String {
        format!("pages/{id}.json")
    }

    fn page_md_path(id: &str) -> String {
        format!("pages/{id}.md")
    }

    async fn load_pages(&self) -> SFResult<Vec<WikiPage>> {
        let docs = match self.backend.list_documents().await {
            Ok(d) => d,
            Err(_) => return Ok(vec![]),
        };
        let mut pages = Vec::new();
        for doc in docs {
            if doc.path.starts_with("pages/") && doc.path.ends_with(".json") {
                if let Ok(page) = serde_json::from_str::<WikiPage>(&doc.content) {
                    pages.push(page);
                }
            }
        }
        Ok(pages)
    }

    async fn save_page(&self, page: &WikiPage) -> SFResult<()> {
        let json = serde_json::to_string_pretty(page).map_err(SFError::from)?;
        self.backend
            .update_document(&Self::page_json_path(&page.id), &json)
            .await?;
        self.backend
            .update_document(&Self::page_md_path(&page.id), &render_markdown(page))
            .await?;
        Ok(())
    }

    // ── index.md / log.md ───────────────────────────────────────

    async fn rebuild_index(&self, pages: &[WikiPage]) -> SFResult<()> {
        let mut out = String::from("# Wiki Index\n\n");
        for (label, filter) in [
            ("摘要页 Summary", WikiPageType::Summary),
            ("实体页 Entity", WikiPageType::Entity),
            ("概念页 Concept", WikiPageType::Concept),
            ("对比页 Comparison", WikiPageType::Comparison),
            ("综合页 Synthesis", WikiPageType::Synthesis),
        ] {
            let mut group: Vec<&WikiPage> =
                pages.iter().filter(|p| p.page_type == filter).collect();
            if group.is_empty() {
                continue;
            }
            group.sort_by(|a, b| a.title.cmp(&b.title));
            out.push_str(&format!("## {label}\n"));
            for p in group {
                out.push_str(&format!(
                    "- [[{}]] — {}（更新于 {}）\n",
                    p.title,
                    p.id,
                    p.last_updated.format("%Y-%m-%d")
                ));
            }
            out.push('\n');
        }
        self.backend.update_document("index.md", &out).await
    }

    async fn append_log(&self, op: &str, details: &str) -> SFResult<String> {
        let existing = self
            .backend
            .read_document("log.md")
            .await
            .map(|d| d.content)
            .unwrap_or_else(|_| "# Wiki Log\n\n".to_string());
        let entry_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let line = format!(
            "- {} | {} | {} | {}\n",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            op,
            entry_id,
            details
        );
        self.backend
            .update_document("log.md", &format!("{existing}{line}"))
            .await?;
        Ok(entry_id)
    }

    // ── LLM 调用 ────────────────────────────────────────────────

    async fn analyze_source(&self, source_path: &str, content: &str) -> SFResult<SourceAnalysis> {
        let truncated: String = content.chars().take(12_000).collect();
        let prompt = format!(
            "阅读以下来源文档（路径：{source_path}），完成：\n\
             1. 写一段 markdown 摘要；\n\
             2. 抽取关键实体（人物/组织/产品）与概念；\n\
             3. 列出文档做出的关键主张（每条一句，便于与既有知识核对矛盾）。\n\n\
             ---\n{truncated}"
        );
        execute_structured(
            self.llm.as_ref(),
            &[Message::user(prompt)],
            &ChatOptions::default(),
        )
        .await
    }

    async fn scan_contradictions(
        &self,
        existing_page: &WikiPage,
        new_claims: &[String],
    ) -> SFResult<Vec<ContradictionItem>> {
        if new_claims.is_empty() {
            return Ok(vec![]);
        }
        let existing: String = existing_page.content.chars().take(6_000).collect();
        let claims = new_claims.join("\n- ");
        let prompt = format!(
            "以下是 wiki 中关于「{}」的既有内容，以及新来源提出的主张。\n\
             找出新主张中与既有内容相矛盾的条目；没有矛盾则返回空列表。\n\n\
             ## 既有内容\n{existing}\n\n## 新主张\n- {claims}",
            existing_page.title
        );
        let scan: ContradictionScan = execute_structured(
            self.llm.as_ref(),
            &[Message::user(prompt)],
            &ChatOptions::default(),
        )
        .await?;
        Ok(scan.contradictions)
    }
}

fn slugify(text: &str) -> String {
    let mut slug = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.ends_with('-') && !slug.is_empty() {
            slug.push('-');
        }
    }
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        uuid::Uuid::new_v4().to_string()[..8].to_string()
    } else {
        slug.chars().take(48).collect()
    }
}

fn render_markdown(page: &WikiPage) -> String {
    let mut out = format!("# {}\n\n", page.title);
    out.push_str(&format!(
        "> 类型：{:?} | 更新：{} | 原因：{}\n\n",
        page.page_type,
        page.last_updated.format("%Y-%m-%d %H:%M"),
        page.update_reason
    ));
    out.push_str(&page.content);
    out.push('\n');
    if !page.source_refs.is_empty() {
        out.push_str("\n## 相关来源\n");
        for s in &page.source_refs {
            out.push_str(&format!("- {s}\n"));
        }
    }
    if !page.outgoing_links.is_empty() {
        out.push_str("\n## 相关页面\n");
        for l in &page.outgoing_links {
            out.push_str(&format!("- [[{l}]]\n"));
        }
    }
    out
}

fn parse_severity(s: &str) -> ContradictionSeverity {
    match s.to_ascii_lowercase().as_str() {
        "medium" => ContradictionSeverity::Medium,
        "high" => ContradictionSeverity::High,
        "critical" => ContradictionSeverity::Critical,
        _ => ContradictionSeverity::Low,
    }
}

#[async_trait]
impl WikiMaintainer for LlmWikiMaintainer {
    async fn ingest_source(
        &self,
        source_path: &str,
        source_content: &str,
    ) -> SFResult<IngestReport> {
        let _guard = self.write_lock.lock().await;

        // 1. 存原始来源（不可变层）
        let raw_path = format!("raw/{}", source_path.trim_start_matches('/'));
        self.backend
            .ingest_document(&raw_path, source_content)
            .await?;

        // 2. LLM 阅读来源
        let analysis = self.analyze_source(source_path, source_content).await?;

        // 3. 写摘要页
        let summary_id = format!("summary-{}", slugify(&analysis.summary_title));
        let summary_page =
            WikiPage::new(&summary_id, WikiPageType::Summary, &analysis.summary_title)
                .with_content(&analysis.summary)
                .with_tags(analysis.tags.clone())
                .with_source_refs(vec![raw_path.clone()])
                .with_update_reason(format!("new source: {source_path}"));
        self.save_page(&summary_page).await?;

        // 4. 更新实体/概念页 + 矛盾检测
        let mut pages = self.load_pages().await?;
        let mut created = Vec::new();
        let mut updated = Vec::new();
        let mut contradictions = Vec::new();

        for entity in &analysis.entities {
            let page_type = if entity.kind.eq_ignore_ascii_case("concept") {
                WikiPageType::Concept
            } else {
                WikiPageType::Entity
            };
            let existing_idx = pages
                .iter()
                .position(|p| p.title.eq_ignore_ascii_case(&entity.name));

            if let Some(idx) = existing_idx {
                let mut page = pages[idx].clone();
                // 矛盾检测：既有内容 vs 新主张
                for item in self.scan_contradictions(&page, &analysis.claims).await? {
                    contradictions.push(ContradictionReport {
                        existing_page_id: page.id.clone(),
                        existing_claim: item.existing_claim,
                        new_source_path: source_path.to_string(),
                        new_claim: item.new_claim,
                        severity: parse_severity(&item.severity),
                    });
                }
                page.content.push_str(&format!(
                    "\n\n## 更新（{}，来源 {source_path}）\n{}\n",
                    Utc::now().format("%Y-%m-%d"),
                    entity.summary
                ));
                if !page.source_refs.contains(&raw_path) {
                    page.source_refs.push(raw_path.clone());
                }
                if !page.outgoing_links.contains(&summary_page.title) {
                    page.outgoing_links.push(summary_page.title.clone());
                }
                page = page.touch(format!("updated by source: {source_path}"));
                self.save_page(&page).await?;
                pages[idx] = page;
                updated.push(pages[idx].id.clone());
            } else {
                let id = format!(
                    "{}-{}",
                    if page_type == WikiPageType::Concept {
                        "concept"
                    } else {
                        "entity"
                    },
                    slugify(&entity.name)
                );
                let page = WikiPage::new(&id, page_type, &entity.name)
                    .with_content(format!("## 概述\n{}\n", entity.summary))
                    .with_source_refs(vec![raw_path.clone()])
                    .with_outgoing_links(vec![summary_page.title.clone()])
                    .with_update_reason(format!("created from source: {source_path}"));
                self.save_page(&page).await?;
                created.push(id.clone());
                pages.push(page);
            }
        }

        // 摘要页回链实体页（双向）
        let mut summary_page = summary_page;
        summary_page.outgoing_links = analysis.entities.iter().map(|e| e.name.clone()).collect();
        self.save_page(&summary_page).await?;

        // 重建 backlinks
        let mut pages = self.load_pages().await?;
        rebuild_backlinks(&mut pages);
        for p in &pages {
            self.save_page(p).await?;
        }

        // 5. 更新 index.md + log.md
        self.rebuild_index(&pages).await?;
        let log_details = format!(
            "source={} summary={} created={} updated={} contradictions={}",
            source_path,
            summary_id,
            created.len(),
            updated.len(),
            contradictions.len()
        );
        let log_entry_id = self.append_log("ingest", &log_details).await?;

        Ok(IngestReport {
            source_path: source_path.to_string(),
            summary_page_id: summary_id,
            entity_pages_created: created,
            entity_pages_updated: updated,
            contradictions_flagged: contradictions,
            index_updated: true,
            log_entry_id,
        })
    }

    async fn query(&self, question: &str, archive: bool) -> SFResult<QueryResult> {
        let results = self.backend.search(question, 5).await.unwrap_or_default();
        let mut context = String::new();
        let mut sources = Vec::new();
        for r in &results {
            if r.document.path.starts_with("pages/") && r.document.path.ends_with(".json") {
                continue; // 跳过机器可读副本，只用 markdown 渲染
            }
            sources.push(r.document.path.clone());
            let snippet: String = r.document.content.chars().take(2_000).collect();
            context.push_str(&format!("## 来源 {}\n{}\n\n", r.document.path, snippet));
        }

        let prompt = if context.is_empty() {
            format!("wiki 中没有检索到相关内容。请直接回答问题：{question}")
        } else {
            format!("基于以下 wiki 页面内容回答问题，并在末尾列出引用的来源：\n\n{context}\n## 问题\n{question}")
        };
        let resp = self
            .llm
            .chat(&[Message::user(prompt)], &ChatOptions::default())
            .await?;
        let answer = resp
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("");

        let archived_page_id = if archive {
            let page = self.archive_answer(question, &answer, &sources).await?;
            Some(page.id)
        } else {
            None
        };

        let _ = self
            .append_log(
                "query",
                &format!(
                    "question={} archived={:?}",
                    &question[..question.len().min(80)],
                    archived_page_id
                ),
            )
            .await;

        Ok(QueryResult {
            answer,
            sources_consulted: sources,
            archived_page_id,
        })
    }

    async fn lint(&self) -> SFResult<LintReport> {
        let pages = self.load_pages().await?;
        let now: DateTime<Utc> = Utc::now();
        let threshold = Duration::days(OUTDATED_THRESHOLD_DAYS);

        let orphan_pages = pages
            .iter()
            .filter(|p| {
                p.backlinks.is_empty()
                    && !matches!(p.page_type, WikiPageType::Index | WikiPageType::Log)
            })
            .map(|p| p.id.clone())
            .collect();

        let outdated_pages = pages
            .iter()
            .filter(|p| now - p.last_updated > threshold)
            .map(|p| OutdatedPage {
                page_id: p.id.clone(),
                last_updated: p.last_updated,
                reason: format!("超过 {OUTDATED_THRESHOLD_DAYS} 天未更新"),
            })
            .collect();

        let mut missing_cross_references = Vec::new();
        for p in &pages {
            for other in &pages {
                if p.id == other.id || other.title.len() < 2 {
                    continue;
                }
                if p.content.contains(&other.title) && !p.outgoing_links.contains(&other.title) {
                    missing_cross_references.push(MissingCrossRef {
                        from_page: p.id.clone(),
                        to_page: other.id.clone(),
                        suggested_link_text: format!("[[{}]]", other.title),
                    });
                }
            }
        }

        // 数据空白：多个页面共用的标签没有对应概念页
        let mut tag_count: std::collections::HashMap<&str, usize> = Default::default();
        for p in &pages {
            for t in &p.tags {
                *tag_count.entry(t.as_str()).or_default() += 1;
            }
        }
        let data_gaps = tag_count
            .into_iter()
            .filter(|(tag, n)| {
                *n >= 2
                    && !pages.iter().any(|p| {
                        p.page_type == WikiPageType::Concept && p.title.eq_ignore_ascii_case(tag)
                    })
            })
            .map(|(tag, n)| DataGap {
                topic: tag.to_string(),
                missing_entity: tag.to_string(),
                suggestion: format!("{n} 个页面使用标签「{tag}」但缺少对应概念页，建议创建"),
            })
            .collect();

        let _ = self.append_log("lint", "completed").await;

        Ok(LintReport {
            checked_at: now,
            orphan_pages,
            outdated_pages,
            missing_cross_references,
            data_gaps,
        })
    }

    async fn update_cross_references(&self) -> SFResult<()> {
        let _guard = self.write_lock.lock().await;
        let mut pages = self.load_pages().await?;
        let link_re = regex::Regex::new(r"\[\[([^\]]+)\]\]").expect("valid regex");
        let titles: Vec<(String, String)> = pages
            .iter()
            .map(|p| (p.title.clone(), p.id.clone()))
            .collect();
        for page in pages.iter_mut() {
            let mut links: Vec<String> = Vec::new();
            for cap in link_re.captures_iter(&page.content.clone()) {
                let title = cap[1].trim().to_string();
                if titles.iter().any(|(t, _)| t.eq_ignore_ascii_case(&title))
                    && !links.contains(&title)
                {
                    links.push(title);
                }
            }
            page.outgoing_links = links;
        }
        rebuild_backlinks(&mut pages);
        for p in &pages {
            self.save_page(p).await?;
        }
        let _ = self
            .append_log("update_cross_references", &format!("pages={}", pages.len()))
            .await;
        Ok(())
    }

    async fn archive_answer(
        &self,
        question: &str,
        answer: &str,
        sources: &[String],
    ) -> SFResult<WikiPage> {
        let _guard = self.write_lock.lock().await;
        let id = format!("synthesis-{}", slugify(question));
        let title: String = question.chars().take(60).collect();
        let content = format!("## 问题\n{question}\n\n## 答案\n{answer}\n");
        let page = WikiPage::new(&id, WikiPageType::Synthesis, &title)
            .with_content(content)
            .with_source_refs(sources.to_vec())
            .with_update_reason("archived query answer");
        self.save_page(&page).await?;
        let pages = self.load_pages().await?;
        self.rebuild_index(&pages).await?;
        let _ = self
            .append_log("archive_answer", &format!("page={id}"))
            .await;
        Ok(page)
    }
}

/// 由 outgoing_links（按标题）重建全量 backlinks。
fn rebuild_backlinks(pages: &mut [WikiPage]) {
    let mut backs: std::collections::HashMap<String, Vec<String>> = Default::default();
    for p in pages.iter() {
        for link in &p.outgoing_links {
            backs
                .entry(link.to_lowercase())
                .or_default()
                .push(p.title.clone());
        }
    }
    for p in pages.iter_mut() {
        p.backlinks = backs.remove(&p.title.to_lowercase()).unwrap_or_default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugify_basic() {
        assert_eq!(slugify("Hello World!"), "hello-world");
        assert_eq!(slugify("---").len(), 8); // 非字母数字兜底为 8 位随机 id
        assert!(!slugify("").is_empty());
    }

    #[test]
    fn backlinks_rebuilt_bidirectionally() {
        let mut a = WikiPage::new("a", WikiPageType::Entity, "Alpha");
        a.outgoing_links = vec!["Beta".into()];
        let b = WikiPage::new("b", WikiPageType::Entity, "Beta");
        let mut pages = vec![a, b];
        rebuild_backlinks(&mut pages);
        assert_eq!(pages[1].backlinks, vec!["Alpha".to_string()]);
        assert!(pages[0].backlinks.is_empty());
    }

    #[test]
    fn render_contains_sections() {
        let page = WikiPage::new("x", WikiPageType::Entity, "X")
            .with_content("body")
            .with_source_refs(vec!["raw/a.md".into()])
            .with_outgoing_links(vec!["Y".into()]);
        let md = render_markdown(&page);
        assert!(md.contains("# X"));
        assert!(md.contains("## 相关来源"));
        assert!(md.contains("[[Y]]"));
    }
}
