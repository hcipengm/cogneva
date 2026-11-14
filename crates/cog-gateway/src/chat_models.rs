//! 对话里能选哪几只模型。
//!
//! 清单只有一份来源：平台已经配置好的那一份。两个来源按部署形态各答一半——
//!
//! - 集群里，真上游只在安全网关：池里每一家报的是 `base_url|model`，模型名字从
//!   那里取。主应用零凭证，读的正是这条派生读数（与设置页同一个调用）。
//! - 集群外，本进程读得到自己那份 `llm_routing.backends`，取其中启用的那几只。
//!
//! 两处都是「平台现在就配着的东西」，不是一张内置的型号表：内置表会在配置换掉
//! 之后继续列出一只已经调不通的模型，而人照着它选，下一轮就失败。
//!
//! 一个都没有的时候，答案是**未配置**加一句为什么，而不是一只默认模型：这正是
//! 界面要画的那句话——「还没配」与「配了但没模型」是两句不同的话，只有前一句
//! 在告诉人去哪儿修。

/// 一段对话能选的模型，以及这份清单的来源与可信度。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalogue {
    /// 去重后的模型名，保持来源里的顺序。
    pub models: Vec<String>,
    /// 清单是从哪儿读到的：`security-gateway` / `local-config` / 空。
    pub source: &'static str,
    /// 池或本地配置说「有东西被配置了」。
    pub configured: bool,
    /// 为什么清单是空的（或为什么只读到一半），给界面直接显示。
    pub reason: Option<String>,
}

impl Catalogue {
    fn empty(reason: impl Into<String>) -> Self {
        Self {
            models: Vec::new(),
            source: "",
            configured: false,
            reason: Some(reason.into()),
        }
    }

    /// 这只模型在不在清单里。清单本身为空（没配任何上游）时一律不在——没有
    /// 清单就没有「照清单选」这回事，选一只不在上游上的名字只会让下一轮失败。
    pub fn offers(&self, model: &str) -> bool {
        self.models.iter().any(|m| m == model)
    }
}

/// 池成员名 `base_url|model` 里的模型那一段。
///
/// 按最后一个 `|` 切：`base_url` 是地址，不认识的地址里出现一个 `|` 也不会
/// 把模型名切走；而模型名里出现 `|` 不是这个名字的写法。
fn model_of_member(name: &str) -> Option<String> {
    let (_, model) = name.rsplit_once('|')?;
    let model = model.trim();
    (!model.is_empty()).then(|| model.to_string())
}

/// 平台现在配着的那几只模型。
pub async fn catalogue() -> Catalogue {
    // 先问池：集群里那是唯一说了算的那一份。
    match crate::llm_admin::forwarded_llm_status().await {
        Ok(pool) => {
            let mut models: Vec<String> = Vec::new();
            for name in &pool.names {
                if let Some(model) = model_of_member(name) {
                    if !models.contains(&model) {
                        models.push(model);
                    }
                }
            }
            if !models.is_empty() {
                return Catalogue {
                    models,
                    source: cog_core::topology::COMPONENT_SECURITY_GATEWAY,
                    configured: true,
                    reason: None,
                };
            }
            if pool.configured {
                return Catalogue {
                    models,
                    source: cog_core::topology::COMPONENT_SECURITY_GATEWAY,
                    configured: true,
                    reason: Some(
                        "安全网关说池里有上游，但一条模型名都没报回来：选不了，先看一眼池的状态。"
                            .into(),
                    ),
                };
            }
            // 池说「没配」：接着看本地那一份，集群外的部署走的是它。
        }
        Err(error) => {
            // 问不到网关不是「没配」：本地可能配着。接着看本地，并把原因带上。
            let local = local_catalogue();
            if local.models.is_empty() {
                return Catalogue {
                    reason: Some(format!(
                        "问不到安全网关的模型池（{error}），本地配置里也没有可用的模型。"
                    )),
                    ..local
                };
            }
            return local;
        }
    }

    let local = local_catalogue();
    if local.models.is_empty() {
        return Catalogue::empty(
            "平台还没有配置任何模型：先在设置里接一个上游，对话里才有可选的模型。",
        );
    }
    local
}

/// 本进程读得到的那份配置里、启用了的模型。
fn local_catalogue() -> Catalogue {
    let mut models: Vec<String> = Vec::new();
    for backend in crate::llm_admin::local_backends() {
        if !backend
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        if let Some(model) = backend
            .get("model")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|m| !m.is_empty())
        {
            if !models.iter().any(|m| m == model) {
                models.push(model.to_string());
            }
        }
    }
    let configured = !models.is_empty();
    Catalogue {
        models,
        source: "local-config",
        configured,
        reason: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{model_of_member, Catalogue};

    /// 池成员名里取模型那一段：地址在前、模型在后，中间一个 `|`。
    #[test]
    fn the_model_is_the_tail_of_a_pool_member_name() {
        assert_eq!(
            model_of_member("http://llm.internal/v1|claude-sonnet-5-5"),
            Some("claude-sonnet-5-5".to_string())
        );
        // 没有分隔的名字不是这一族：宁可少列一只，也不把整条地址当模型名。
        assert_eq!(model_of_member("claude-sonnet-5-5"), None);
        assert_eq!(model_of_member("http://x|"), None);
    }

    /// 「能选」只看清单：空清单不承诺任何名字。
    #[test]
    fn an_empty_catalogue_offers_nothing() {
        let empty = Catalogue::empty("没有配上游");
        assert!(!empty.offers("anything"));
        let one = Catalogue {
            models: vec!["m-1".into()],
            source: "local-config",
            configured: true,
            reason: None,
        };
        assert!(one.offers("m-1"));
        assert!(!one.offers("m-2"));
    }
}
