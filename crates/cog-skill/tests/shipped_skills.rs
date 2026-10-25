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

/// 设计规程只有一个家，而且原文是原文、本仓库的落点是另一份。
///
/// 这份规程原先躺在 `.claude/skills/` 里——那是给某个编辑器会话读的目录，
/// 不是平台的技能目录：镜像不复制它，注册表不扫描它，模型在选择技能时看不到
/// 它，于是「界面该怎么做」这件事只对一个碰巧在那个目录下干活的会话生效。
/// 技能的正本在 `skills/`，`Dockerfile` 把它整棵复制进 `/opt/cogneva/skills`，
/// 系统提示里按 description 列出，任何一次前端改动都读得到同一份。
///
/// 正文是 Anthropic 的 `frontend-design` 原文，逐字保留。它被改写过一次：
/// 转成中文、压成要点、删掉一整节——每一处单看都像整理，合起来就是把唯一
/// 经过验证的部分换成了一份没有人验证过的转述。所以这里钉的是指纹，不是要点：
/// 少一个字、多一句本地补充，都会红。
///
/// 本仓库自己的落点写在同目录的 `COGNEVA.md`，不并进原文。上游更新时整篇
/// 替换 `SKILL.md` 即可，两件事不会互相绊住。
#[tokio::test]
async fn the_design_skill_is_the_upstream_text_verbatim_and_the_local_notes_live_beside_it() {
    use sha2::{Digest, Sha256};

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");

    let stray = root.join(".claude/skills");
    assert!(
        !stray.exists(),
        "{} 又出现了：设计规程只许有一个家（skills/），第二份会与正本漂开",
        stray.display()
    );

    let path = root.join("skills/frontend-design");
    let skill_md = std::fs::read_to_string(path.join("SKILL.md"))
        .expect("skills/frontend-design/SKILL.md 不在");

    // 上游 anthropics/skills 的 frontend-design/SKILL.md 的 sha256。改它之前先问：
    // 这是上游真的更新了，还是有人在原文里改了几笔？后者就在这里改回去。
    const UPSTREAM_SHA256: &str =
        "d91970639e9f5c37682ac7ab60094d35f1c7c1f38d731bd56396563aee10c1d3";
    let digest = format!("{:x}", Sha256::digest(skill_md.as_bytes()));
    assert_eq!(
        digest, UPSTREAM_SHA256,
        "skills/frontend-design/SKILL.md 与上游原文不一致。上游更新了就把这个\n\
         指纹一起改掉并说明；只是本地编辑的话，请改回原文——本仓库的补充写进\n\
         同目录的 COGNEVA.md。"
    );

    // 指纹之外再按形状钉一道：指纹挡住的是「改了」，这几条挡住的是「改得看起来
    // 像原文」——一次中文改写可以被指纹拦下，但下次换个方式压缩就又得靠人看。
    for section in [
        "## Ground your designs in the subject matter",
        "## Design principles",
        "## Process: plan, review against the brief, build, critique",
        "## Restraint and self-critique",
        "## More on writing in design",
    ] {
        assert!(
            skill_md.contains(section),
            "skills/frontend-design/SKILL.md 少了上游的一节：{section}"
        );
    }

    let def = cog_skill::loader::load_skill(&path)
        .await
        .expect("skills/frontend-design 装不起来");
    assert_eq!(def.metadata.id, "frontend-design", "技能 id 必须等于目录名");

    let notes = std::fs::read_to_string(path.join("COGNEVA.md"))
        .expect("skills/frontend-design/COGNEVA.md 不在：本仓库的落点没有载体");

    let tokens = root.join("web/src/styles/tokens.css");
    assert!(
        tokens.is_file(),
        "落点指向的令牌表不在：{}（表搬了，落点没跟上，读者只会以为是自己找不到）",
        tokens.display()
    );
    assert!(
        notes.contains("web/src/styles/tokens.css"),
        "COGNEVA.md 没有点名令牌表的路径，读者只能自己猜颜色该从哪儿取"
    );
    assert!(
        notes.contains("--brass"),
        "COGNEVA.md 没有点名唯一的强调色：不写清楚，每个视图都会自己造一个强调色"
    );
}
