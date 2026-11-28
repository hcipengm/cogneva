//! 斜杠命令：输入框里 `/` 打头的那一句不是发给模型的一句话。
//!
//! 平台上的「自定义命令」与「技能」是同一件东西的两面：一份 Markdown 说明
//! （`SKILL.md`）放进技能目录，热加载把它读进外部技能注册表，人就能用 `/名字`
//! 点它。这里只做三件事：
//!
//! 1. 把注册表列成一张打得出来的名单（前端输入 `/` 时列的就是它）；
//! 2. 把点名的那一份展开成这一轮要说的话；
//! 3. 认不出名字时给一句人话。
//!
//! 第三件是本模块存在的主要理由：把 `/foo` 当成一句话交给模型，屏幕上是「模型
//! 答得奇怪」，而真正发生的是平台不认识这个词——模型被要求回答一个它无从回答的
//! 问题，人却在等一个不存在的命令执行。

use std::sync::Arc;

use cog_core::ExternalSkillRegistry;

/// 平台自己实现的那一条。它不是技能：没有说明要展开，答的就是这份名单本身。
pub const HELP: &str = "help";

/// 名单里的一条。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CommandInfo {
    /// 打出来的名字，不带 `/`。
    pub name: String,
    pub description: String,
    /// `skill` 是有人放进技能目录的一份说明；`builtin` 由平台自己实现。
    pub kind: &'static str,
}

/// 名单本身，连同「为什么它可能不全」。
///
/// `note` 是那句「没读到 / 不全」的解释，`None` 才是「列全了」。两者在界面上要
/// 分得开：一个是去查技能插件，一个是去放一份 `SKILL.md`。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Catalogue {
    pub commands: Vec<CommandInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// 人点的那一条命令：名字（不带 `/`）与名字之后的整段话。
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct Invocation {
    pub name: String,
    #[serde(default)]
    pub arguments: String,
}

impl Invocation {
    /// 人打的那一行。回声、记录、界面上「用户说了什么」用的都是它——不是展开
    /// 之后的那一份。
    pub fn line(&self) -> String {
        let arguments = self.arguments.trim();
        if arguments.is_empty() {
            format!("/{}", self.name)
        } else {
            format!("/{} {}", self.name, arguments)
        }
    }
}

/// 命令名认不认：字母、数字、`-` 与 `_`。
///
/// 这条判据挡的不是恶意输入，而是「一句以 `/` 开头的话」：`/usr/local/bin 不见了`
/// 要被当成一句话，`/web-research 查一下` 要被当成一条命令。两者的分别在于名字
/// 后面紧跟的是空白还是另一个 `/`，而这条判据只认词本身——所以 `/usr/bin` 里
/// 那个 `usr` 后面跟的是 `/`，它不是命令名的一部分，整句于是照旧是一句话。
pub fn is_command_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// 现有的命令：技能目录里读得到的每一份，加上平台自己实现的那一条。
pub async fn catalogue(skills: Option<&Arc<dyn ExternalSkillRegistry>>) -> Catalogue {
    let mut commands = vec![CommandInfo {
        name: HELP.to_string(),
        description: "列出这一段对话能用的命令".to_string(),
        kind: "builtin",
    }];
    let Some(registry) = skills else {
        return Catalogue {
            commands,
            note: Some("这个部署没有装技能表：只列得出平台自己的命令".to_string()),
        };
    };
    let mut note = None;
    match registry.list().await {
        Ok(listed) => {
            let mut skipped = 0usize;
            for skill in listed {
                if !is_command_name(&skill.id) {
                    skipped += 1;
                    continue;
                }
                commands.push(CommandInfo {
                    name: skill.id,
                    description: skill.description,
                    kind: "skill",
                });
            }
            commands.sort_by(|a, b| {
                // `help` 排第一：它是唯一一条「不知道有什么」时不用先知道名字的命令。
                (a.name != HELP, &a.name).cmp(&(b.name != HELP, &b.name))
            });
            if skipped > 0 {
                note = Some(format!(
                    "有 {skipped} 份技能的名字不能用 `/` 打出来（名字只能用字母、数字、- 与 _），\
                     它们没有列在这里"
                ));
            }
        }
        Err(e) => {
            note = Some(format!("读技能表失败：{e}"));
        }
    }
    Catalogue { commands, note }
}

/// 把一次点名展开成这一轮要说的话。
///
/// 展开的是**整份说明**，不是一句摘要：说明里的规程、边界与止损条件都是模型这一
/// 轮要照着做的部分，截一段等于给它一份缺页的规程。太长就拒——拒绝是看得见的，
/// 截断不是。
pub async fn expand(
    skills: Option<&Arc<dyn ExternalSkillRegistry>>,
    invocation: &Invocation,
    threshold_tokens: usize,
) -> Result<String, String> {
    if !is_command_name(&invocation.name) {
        return Err(format!(
            "「{}」不是一个命令名：`/` 后面的名字只能用字母、数字、- 与 _。\
             要是这一句本来就是一串路径，把它当普通消息发出去就行。",
            invocation.name
        ));
    }
    if invocation.name == HELP {
        return Ok(help_text(&catalogue(skills).await));
    }
    let Some(registry) = skills else {
        return Err(format!(
            "这个部署没有装技能表，`/{}` 点不了。换一句话让它直接做，或者先把技能装上。",
            invocation.name
        ));
    };
    // 先看名单再看内容：名单里没有这个名字是「不认识」，名单里有而读不出来是
    // 「认得，但这一份坏了」——两种要对人说的话不一样。
    let listed = catalogue(Some(registry)).await;
    let known = listed.commands.iter().any(|c| c.name == invocation.name);
    if !known {
        let names: Vec<String> = listed
            .commands
            .iter()
            .map(|c| format!("/{}", c.name))
            .collect();
        let mut reason = format!(
            "没有叫「{}」的命令。这一段对话现在能用的是：{}。",
            invocation.name,
            names.join(" ")
        );
        if let Some(note) = listed.note {
            reason.push(' ');
            reason.push_str(&note);
        }
        return Err(reason);
    }
    let definition = registry
        .resolve(&invocation.name)
        .await
        .map_err(|e| format!("技能「{}」认得，但这一份读不出来：{e}", invocation.name))?;
    let body = without_frontmatter(&definition.skill_md);
    let arguments = invocation.arguments.trim();
    let text = if arguments.is_empty() {
        format!(
            "[用户在这一轮点了技能「{}」，没有补充要求；照着下面这份说明自己判断要做\
             什么]\n\n{body}",
            invocation.name
        )
    } else {
        format!(
            "[用户在这一轮点了技能「{}」，要求是这一段话后面那几句；先读完说明，再按\
             说明做]\n\n{body}\n\n[用户接着说]\n\n{arguments}",
            invocation.name
        )
    };
    let tokens = cog_core::estimate_tokens(&text);
    if tokens > threshold_tokens {
        return Err(format!(
            "技能「{}」的说明约 {tokens} token，超过这一轮能带的量（约 {threshold_tokens}），\
             没有发出去。把说明拆短，或者把要它做的事写成一句话。",
            invocation.name
        ));
    }
    Ok(text)
}

/// `/help` 的回答：名单本身。它进对话流，和一条回答一样。
fn help_text(catalogue: &Catalogue) -> String {
    let mut text = String::from("这一段对话能用的命令：");
    for command in &catalogue.commands {
        text.push_str(&format!("\n/{} — {}", command.name, command.description));
    }
    if let Some(note) = &catalogue.note {
        text.push_str(&format!("\n\n{note}"));
    }
    text
}

/// 去掉开头那段 frontmatter：`name` / `description` 是给名单看的元信息，不是给
/// 模型读的说明。没有 frontmatter 就原样返回。
fn without_frontmatter(skill_md: &str) -> &str {
    let mut lines = skill_md.lines();
    if lines.next().map(str::trim) != Some("---") {
        return skill_md;
    }
    let mut offset = skill_md.find('\n').map_or(skill_md.len(), |at| at + 1);
    for line in lines {
        let advance = line.len() + 1;
        if line.trim() == "---" {
            return skill_md[(offset + advance).min(skill_md.len())..].trim_start();
        }
        offset += advance;
    }
    // 收尾的 `---` 不见了：那不是一份能读的说明，把原文交出去比交一段空白好——
    // 模型至少看得见它写了什么。
    skill_md
}

/// 命令名单。前端在输入框打 `/` 时读它。
pub async fn catalogue_handler(
    axum::extract::State(state): axum::extract::State<Arc<crate::GatewayState>>,
) -> axum::Json<Catalogue> {
    axum::Json(catalogue(state.external_skill_registry.as_ref()).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{SFError, SFResult, SkillDef, SkillFrontmatter, SkillMetadata};

    const BODY: &str = "# 联网调研\n\n先搜，再读。";

    struct Skills {
        listed: Vec<SkillMetadata>,
        body: String,
    }

    #[async_trait::async_trait]
    impl ExternalSkillRegistry for Skills {
        async fn resolve_metadata(&self, skill_id: &str) -> SFResult<SkillMetadata> {
            self.listed
                .iter()
                .find(|s| s.id == skill_id)
                .cloned()
                .ok_or_else(|| SFError::Agent(format!("no skill {skill_id}")))
        }

        async fn resolve(&self, skill_id: &str) -> SFResult<SkillDef> {
            let body = &self.body;
            Ok(SkillDef {
                metadata: self.resolve_metadata(skill_id).await?,
                skill_md: format!("---\nname: {skill_id}\ndescription: 规程\n---\n\n{body}"),
                frontmatter: SkillFrontmatter {
                    name: skill_id.into(),
                    description: "规程".into(),
                },
            })
        }

        async fn list(&self) -> SFResult<Vec<SkillMetadata>> {
            Ok(self.listed.clone())
        }

        async fn load_resource(&self, _: &str, _: &str) -> SFResult<String> {
            Ok(String::new())
        }

        async fn list_scripts(&self, _: &str) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn script_path(&self, _: &str, _: &str) -> SFResult<std::path::PathBuf> {
            Ok(std::path::PathBuf::new())
        }

        async fn download(&self, _: &str, _: cog_core::DownloadOptions) -> SFResult<()> {
            Ok(())
        }
    }

    fn skill(id: &str) -> SkillMetadata {
        SkillMetadata {
            id: id.into(),
            name: id.into(),
            description: format!("{id} 的说明"),
            version: "1".into(),
        }
    }

    fn registry(listed: Vec<SkillMetadata>) -> Arc<dyn ExternalSkillRegistry> {
        Arc::new(Skills {
            listed,
            body: BODY.into(),
        })
    }

    /// 名字的形状：字母、数字、`-`、`_`。
    ///
    /// 「一句以 `/` 开头的话」不靠这一条挡住——`/usr/local/bin 不见了` 里那个
    /// `usr` 本身是个合法名字，挡住它的是协议那一边的判据：名字后面紧跟空白或
    /// 行尾才算一条命令，`/usr/bin` 于是整句照旧是一句话。
    #[test]
    fn a_command_name_is_made_of_letters_digits_dashes_and_underscores() {
        for name in ["a.b", "路径", "a b", ""] {
            assert!(!is_command_name(name), "「{name}」被当成了命令名");
        }
        for name in ["help", "web-research", "pge_planner", "v2"] {
            assert!(is_command_name(name), "「{name}」是一个正常名字");
        }
    }

    /// 名单里先有平台自己的那一条，再有技能；技能按名字排，`help` 永远在最前。
    #[tokio::test]
    async fn the_list_names_the_builtin_first_and_every_skill_after_it() {
        let skills = registry(vec![skill("web-research"), skill("frontend-design")]);
        let listed = catalogue(Some(&skills)).await;
        let names: Vec<&str> = listed.commands.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["help", "frontend-design", "web-research"]);
        assert_eq!(listed.commands[0].kind, "builtin");
        assert_eq!(listed.commands[1].kind, "skill");
        assert_eq!(listed.note, None, "读全了却报了一句「不全」");
    }

    /// 没装技能表：列得出平台自己的那条，并说清楚为什么只有它。
    #[tokio::test]
    async fn a_deployment_without_a_skill_table_says_so_instead_of_listing_nothing() {
        let listed = catalogue(None).await;
        assert_eq!(listed.commands.len(), 1);
        assert!(listed
            .note
            .as_deref()
            .is_some_and(|note| note.contains("没有装技能表")));
    }

    /// 名字打不出来的技能不算静默消失：名单给一句「有几份没列出来」。
    #[tokio::test]
    async fn a_skill_whose_name_cannot_be_typed_is_counted_not_silently_dropped() {
        let skills = registry(vec![skill("联网调研"), skill("web-research")]);
        let listed = catalogue(Some(&skills)).await;
        let names: Vec<&str> = listed.commands.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["help", "web-research"]);
        assert!(listed
            .note
            .as_deref()
            .is_some_and(|note| note.contains("1 份技能")));
    }

    /// 展开的是整份说明，正文不含 frontmatter，人的要求排在它后面。
    #[tokio::test]
    async fn a_named_skill_expands_into_its_whole_instructions() {
        let skills = registry(vec![skill("web-research")]);
        let text = expand(
            Some(&skills),
            &Invocation {
                name: "web-research".into(),
                arguments: "  查一下 Rust 1.90 的发布说明  ".into(),
            },
            100_000,
        )
        .await
        .expect("认得的技能展不开");
        assert!(text.contains("先搜，再读"), "说明正文没有展开：{text}");
        assert!(
            !text.contains("description: 规程"),
            "frontmatter 也铺进去了：{text}"
        );
        let body_at = text.find("先搜，再读").unwrap();
        let ask_at = text.find("查一下 Rust 1.90 的发布说明").unwrap();
        assert!(body_at < ask_at, "人的要求排在了说明前面：{text}");
    }

    /// 认不出名字：给一句人话，且把现有的名字列出来——不是让模型去猜。
    #[tokio::test]
    async fn an_unknown_name_is_answered_here_not_by_the_model() {
        let skills = registry(vec![skill("web-research")]);
        let reason = expand(
            Some(&skills),
            &Invocation {
                name: "websearch".into(),
                arguments: String::new(),
            },
            100_000,
        )
        .await
        .expect_err("不认识的名字展得开");
        assert!(reason.contains("没有叫「websearch」的命令"), "{reason}");
        assert!(
            reason.contains("/web-research"),
            "没有列出能用的名字：{reason}"
        );
        assert!(reason.contains("/help"), "没有指出 /help：{reason}");
    }

    /// 认得、但读不出来：说的是「这一份坏了」，不是「没有这个命令」。
    #[tokio::test]
    async fn a_skill_that_lists_but_cannot_be_read_says_which_half_failed() {
        struct Broken;
        #[async_trait::async_trait]
        impl ExternalSkillRegistry for Broken {
            async fn resolve_metadata(&self, _: &str) -> SFResult<SkillMetadata> {
                Ok(skill("web-research"))
            }
            async fn resolve(&self, _: &str) -> SFResult<SkillDef> {
                Err(SFError::IO("SKILL.md 半途丢行".into()))
            }
            async fn list(&self) -> SFResult<Vec<SkillMetadata>> {
                Ok(vec![skill("web-research")])
            }
            async fn load_resource(&self, _: &str, _: &str) -> SFResult<String> {
                Ok(String::new())
            }
            async fn list_scripts(&self, _: &str) -> SFResult<Vec<String>> {
                Ok(Vec::new())
            }
            async fn script_path(&self, _: &str, _: &str) -> SFResult<std::path::PathBuf> {
                Ok(std::path::PathBuf::new())
            }
            async fn download(&self, _: &str, _: cog_core::DownloadOptions) -> SFResult<()> {
                Ok(())
            }
        }
        let skills: Arc<dyn ExternalSkillRegistry> = Arc::new(Broken);
        let reason = expand(
            Some(&skills),
            &Invocation {
                name: "web-research".into(),
                arguments: String::new(),
            },
            100_000,
        )
        .await
        .expect_err("读不出来的技能展得开");
        assert!(reason.contains("认得"), "{reason}");
        assert!(reason.contains("读不出来"), "{reason}");
    }

    /// 说明超过这一轮能带的量：拒，而不是截一半交给模型。
    #[tokio::test]
    async fn instructions_beyond_the_window_are_refused_not_cut() {
        let skills = registry(vec![skill("web-research")]);
        let reason = expand(
            Some(&skills),
            &Invocation {
                name: "web-research".into(),
                arguments: String::new(),
            },
            1,
        )
        .await
        .expect_err("超过阈值的说明照样展开了");
        assert!(reason.contains("超过这一轮能带的量"), "{reason}");
    }

    /// `/help` 答的是名单本身，且它进对话流（返回的是正文，不是错误）。
    #[tokio::test]
    async fn help_answers_with_the_list_itself() {
        let skills = registry(vec![skill("web-research")]);
        let text = expand(
            Some(&skills),
            &Invocation {
                name: HELP.into(),
                arguments: String::new(),
            },
            100_000,
        )
        .await
        .expect("/help 答不出来");
        assert!(text.contains("/help — 列出这一段对话能用的命令"), "{text}");
        assert!(
            text.contains("/web-research — web-research 的说明"),
            "{text}"
        );
    }

    /// 人打的那一行：名字前面补 `/`，没有要求时不留一个尾随空格。
    #[test]
    fn the_line_a_person_typed_is_rebuilt_without_a_stray_space() {
        let line = |name: &str, arguments: &str| {
            Invocation {
                name: name.into(),
                arguments: arguments.into(),
            }
            .line()
        };
        assert_eq!(line("help", ""), "/help");
        assert_eq!(line("help", "  "), "/help");
        assert_eq!(line("web-research", " 查一下 "), "/web-research 查一下");
    }
}
