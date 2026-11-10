//! 账上的运行时原则接到内核的读面上。
//!
//! 内核只认两件事：这一轮拿哪几条规则、交出去之后记一笔。账在存储插件里，
//! 两边的类型也是分开的（`cog_core::RuntimePrinciple` 是账上的一行，
//! `cog_agent::principles::InForce` 是交给模型的那一段），这里只做搬运，不
//! 重新解释：账上说生效的，就是这里交出去的。
//!
//! 运一条坏行不拖垮整轮：一段空着的判据读起来像「这一条今天没有内容」，与其
//! 把它当成没有约束交给模型，不如跳过它并记一条 warn——它明天还在账上，读数
//! 与账仍然对得上（`used` 只数真正交出去的那几条）。

use std::sync::Arc;

use async_trait::async_trait;

use cog_agent::principles::{InForce, PrincipleSource};
use cog_core::{RuntimePrincipleStore, SFResult};

/// 把存储插件发布的运行时原则账接到内核读面上。
pub struct AccountPrinciples {
    store: Arc<dyn RuntimePrincipleStore>,
}

impl AccountPrinciples {
    pub fn new(store: Arc<dyn RuntimePrincipleStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl PrincipleSource for AccountPrinciples {
    async fn in_force(&self) -> SFResult<Vec<InForce>> {
        let rows = self.store.in_force().await?;
        let mut rules = Vec::with_capacity(rows.len());
        for row in rows {
            let slug = row.slug.clone();
            match InForce::new(row.slug, row.trigger, row.criterion, row.action) {
                Ok(rule) => rules.push(rule),
                Err(e) => tracing::warn!(
                    error = %e,
                    slug = %slug,
                    "runtime principle is in force but cannot be handed over; skipped for this run"
                ),
            }
        }
        Ok(rules)
    }

    async fn note_used(&self, slugs: &[String]) -> SFResult<()> {
        self.store.note_used(slugs).await
    }
}
