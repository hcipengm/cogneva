//! principle: feedback_no_hardcoding_config_surface
//! A word list in the WebUI is a copy of a vocabulary the server declares, and
//! nothing compares the two at build time: the client ships compiled, the server
//! answers whatever it is asked. A word the server does not know comes back as
//! the empty answer, which is what "nothing there" looks like -- so the operator
//! reads a broken panel as a quiet system.
//!
//! Two lists are checked here against their declarations: the task view's
//! filter tabs against `cog_core::TaskFilter`, and the git identity state union
//! against the gateway's `IdentityState`.
//!
//! 实测（2026-10-27）：修前 `web/src/views/TasksView.tsx` 的 `ready` 标签把
//! `status=ready` 发给服务端，而 `TaskStatus` 里没有这个词，标签恒空——表头却
//! 数着「就绪待领 N」。消融对照两条：把 `running` 改成 `running_now`，本门红并
//! 点名该文件与该词；把 `IdentityState::Ready` 的词改成 `configured`，《状态词》
//! 那条红并点名前端联合类型；各自改回即绿。

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The quoted words inside the first `[...]` that follows `start`.
fn array_words(source: &str, start: &str) -> Vec<String> {
    let from = source
        .find(start)
        .unwrap_or_else(|| panic!("{start:?} is gone; this gate reads the tab list through it"))
        + start.len();
    let rest = &source[from..];
    let end = rest.find(']').expect("the array literal is not closed");
    let mut words = Vec::new();
    let bytes = rest.as_bytes();
    let mut i = 0;
    while i < end {
        let quote = bytes[i];
        if quote == b'\'' || quote == b'"' {
            let close = rest[i + 1..end]
                .find(quote as char)
                .unwrap_or_else(|| panic!("unclosed quote at byte {i} of the tab list"));
            words.push(rest[i + 1..i + 1 + close].to_string());
            i += close + 2;
        } else {
            i += 1;
        }
    }
    words
}

/// The word the view starts on: the tab that sends no filter at all.
fn initial_tab(source: &str) -> String {
    let start = "useState<(typeof FILTERS)[number]>('";
    let from = source.find(start).unwrap_or_else(|| {
        panic!("the tab state is not declared as {start:?}; this gate reads the no-filter tab through it")
    }) + start.len();
    let rest = &source[from..];
    let end = rest.find('\'').expect("the initial tab is not closed");
    rest[..end].to_string()
}

#[test]
fn every_task_view_tab_is_a_word_the_endpoints_accept() {
    let path = repo_root().join("web/src/views/TasksView.tsx");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let tabs = array_words(&source, "const FILTERS = [");
    let no_filter = initial_tab(&source);

    assert!(
        tabs.len() >= 3,
        "the tab list was read as {tabs:?}; a list this short means the parse, not the view, changed"
    );
    assert!(
        tabs.contains(&no_filter),
        "the initial selection {no_filter:?} is not one of the tabs {tabs:?}"
    );
    assert!(
        cog_core::TaskFilter::parse(&no_filter).is_none(),
        "{no_filter:?} is the no-filter tab: if it were also a filter word the view would be \
         sending a word it means as 'everything'"
    );

    let filter_tabs: Vec<&String> = tabs.iter().filter(|tab| **tab != no_filter).collect();
    assert!(
        !filter_tabs.is_empty(),
        "the view offers no filtered tab at all"
    );

    for tab in filter_tabs {
        let filter = cog_core::TaskFilter::parse(tab).unwrap_or_else(|| {
            panic!(
                "tab {tab:?} is not a word the task endpoints accept; they take {:?}",
                cog_core::TaskFilter::ALL.map(|f| f.as_str())
            )
        });
        assert_eq!(
            filter.as_str(),
            tab.as_str(),
            "tab {tab:?} is a filter word in the wrong spelling: the endpoint matches \
             case-insensitively, but the row it returns carries {:?}",
            filter.as_str()
        );
    }
}

/// The words of the union type that follows `start`, e.g. `state: 'ready' |
/// 'pending'`.
fn union_words(source: &str, start: &str) -> Vec<String> {
    let from = source
        .find(start)
        .unwrap_or_else(|| panic!("{start:?} is gone; this gate reads the state union through it"))
        + start.len();
    let rest = &source[from..];
    let end = rest.find(';').expect("the type alias is not closed");
    let body = &rest[..end];
    let mut words = Vec::new();
    for part in body.split('|') {
        let part = part.trim();
        if let Some(word) = part.strip_prefix('\'').and_then(|p| p.strip_suffix('\'')) {
            words.push(word.to_string());
        }
    }
    words
}

/// 网关能报的状态与面板认的状态必须是同一组词。少一个：面板把它落到通用分支，
/// 「读不到」与「没配」看起来一样；多一个：面板为一个永远不会到来的词留着一支
/// 从没跑过的显示。
#[test]
fn the_git_identity_states_the_panel_knows_are_the_ones_the_gateway_reports() {
    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let mut panel = union_words(&source, "export interface GitIdentityStatus {\n  state:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "the state union was read as {panel:?}; a list this short means the parse changed"
    );

    let mut declared: Vec<String> = cog_gateway::git_identity::IdentityState::ALL
        .iter()
        .map(|state| state.as_str().to_string())
        .collect();
    declared.sort();

    assert_eq!(
        panel, declared,
        "the panel and the gateway must know the same identity states"
    );
}

/// 项目行上的 `kind` 同理：面板按它决定这一行能不能点进任务图，所以它是响应的
/// 意思的一部分，不是标签。服务端多一个词，那一行就点不进去；少一个词，面板指着
/// 一个永远不会到来的种类留着一支死分支。
#[test]
fn the_project_kinds_the_panel_knows_are_the_ones_the_gateway_reports() {
    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    // `kind:` also names unions in other interfaces, so the field is read
    // inside `ProjectRow`'s own body rather than by the first match in the file.
    let body_start = source
        .find("export interface ProjectRow {")
        .expect("the project row type is gone; this gate reads its kind through it");
    let body = &source[body_start..];
    let body_end = body.find("\n}").expect("the interface body is not closed");
    let mut panel = union_words(&body[..body_end], "  kind:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "the project kind union was read as {panel:?}; that is too short to be a parse of it"
    );

    let mut declared: Vec<String> = cog_gateway::projects::ProjectKind::ALL
        .iter()
        .map(|kind| kind.as_str().to_string())
        .collect();
    declared.sort();
    assert_eq!(
        panel, declared,
        "the panel and the gateway must know the same project kinds"
    );
}

/// 对照：本门的判据认得出一个不是词的东西。少了这条，`TaskFilter::parse` 哪天
/// 退化成「什么词都收」，上面那条仍然全绿。
#[test]
fn the_acceptance_check_rejects_a_word_no_row_carries() {
    for word in ["ready_to_run", "全部", "", "pending ", "cancelled!"] {
        assert_eq!(
            cog_core::TaskFilter::parse(word),
            None,
            "{word:?} is not a declared filter word"
        );
    }
    for word in ["configured", "Ready", "unknown ", "missing"] {
        assert_eq!(
            cog_gateway::git_identity::IdentityState::parse(word),
            None,
            "{word:?} is not a declared identity state"
        );
    }
    for state in cog_gateway::git_identity::IdentityState::ALL {
        assert_eq!(
            cog_gateway::git_identity::IdentityState::parse(state.as_str()),
            Some(state),
            "{} is declared but not readable",
            state.as_str()
        );
    }
    for filter in cog_core::TaskFilter::ALL {
        assert_eq!(
            cog_core::TaskFilter::parse(filter.as_str()),
            Some(filter),
            "{} is declared but not readable",
            filter.as_str()
        );
    }
}
