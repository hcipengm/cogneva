//! 仓库里随镜像发布的技能目录（`skills/`）必须真的装得起来。
//!
//! 技能坏起来是静悄悄的：目录名对、`SKILL.md` 也在，只有 frontmatter 少了
//! `description`、或者正文空了——部署脚本照样把它复制进 `/opt/cogneva/skills`，
//! 注册表要在 Pod 起来之后才 `warn!` 一行，而那一行不会让任何人为之停下来。
//! 所以这里在构建期就把这份目录逐条装一遍。
//!
//! 另外单独钉住 `web-research`：它把联网检索的操作规程（先搜、再读、搜不到时的
//! 引擎直达模板）交给模型，是「不靠付费搜索接口也能查资料」这条能力在代码里的
//! 唯一载体。目录被删或改名，用户侧的表现是模型又开始瞎猜版本号——没有这条断言，
//! 没有人会把它和一次目录整理联系起来。

use std::path::{Path, PathBuf};

fn shipped_skills_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills")
}

#[tokio::test]
async fn every_shipped_skill_loads_with_a_name_and_a_body() {
    let dir = shipped_skills_dir();
    assert!(
        dir.is_dir(),
        "仓库里的技能目录不在：{}（镜像靠它把技能复制进 /opt/cogneva/skills）",
        dir.display()
    );

    let discovered = cog_skill::discovery::discover_skills(&dir)
        .await
        .expect("扫描技能目录失败");
    assert!(
        !discovered.is_empty(),
        "{} 下没有任何含 SKILL.md 的技能目录：发布进镜像的技能树是空的",
        dir.display()
    );

    for path in discovered {
        let id = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("<非法目录名>")
            .to_string();
        let def = cog_skill::loader::load_skill(&path)
            .await
            .unwrap_or_else(|e| panic!("技能 {id} 装不起来：{e}"));

        assert_eq!(def.metadata.id, id, "技能 id 必须等于目录名");
        assert!(
            !def.frontmatter.name.trim().is_empty(),
            "技能 {id} 的 frontmatter 没有 name"
        );
        assert!(
            !def.frontmatter.description.trim().is_empty(),
            "技能 {id} 的 frontmatter 没有 description：模型在选择技能时看不到它"
        );
        assert!(
            def.metadata.description.len() >= 20,
            "技能 {id} 的 description 只有 {} 个字符，短到无法据此判断是否该用",
            def.metadata.description.len()
        );
        // 正文是真正注入 prompt 的部分；只有 frontmatter 的技能等于一个标题。
        let body = def
            .skill_md
            .split("\n---")
            .nth(1)
            .unwrap_or("")
            .trim()
            .to_string();
        assert!(
            body.chars().count() > 200,
            "技能 {id} 的正文只有 {} 个字符，装起来也不会改变任何行为",
            body.chars().count()
        );
    }
}

#[tokio::test]
async fn web_research_skill_tells_the_model_how_to_search_without_paid_apis() {
    let path = shipped_skills_dir().join("web-research");
    assert!(
        path.join("SKILL.md").is_file(),
        "缺 skills/web-research/SKILL.md：联网检索的操作规程没有随镜像发布"
    );

    let def = cog_skill::loader::load_skill(&path)
        .await
        .expect("web-research 装不起来");
    let text = def.skill_md.as_str();

    for needle in [
        // 两个工具的名字必须出现，否则模型不知道去调什么。
        "web_search",
        "web_fetch",
        // 必须给出搜不到时的直达路径，否则「元搜索没结果」会被读成「网上没有」。
        "https://cn.bing.com/search?q=",
        "https://www.so.com/s?q=",
        // 必须写明止损，否则一次查不到会拖成十几轮抓取。
        "止损",
    ] {
        assert!(
            text.contains(needle),
            "skills/web-research/SKILL.md 里没有 {needle}"
        );
    }
}
