//! principle: feedback_no_hardcoding_config_surface
//! The task view's tabs carry the words that go onto the query string, and the
//! endpoint decides which rows those words select. Nothing compares the two at
//! build time, so a tab can name a word no row carries -- and the empty list it
//! gets back is the same body an empty state returns. The operator reading it
//! sees "nothing waiting", which is exactly the reading the tab was opened for.
//!
//! So every tab word is checked against [`TaskFilter`], the declaration the
//! endpoints read their own words from. The one word that is not a filter is
//! the view's "no filter" tab, and it is identified structurally -- it is the
//! list's initial selection -- rather than by name, so renaming the label does
//! not turn this red.
//!
//! 实测（2026-10-27）：修前 `web/src/views/TasksView.tsx` 的 `ready` 标签把
//! `status=ready` 发给服务端，而 `TaskStatus` 里没有这个词，标签恒空——表头却
//! 数着「就绪待领 N」。消融对照：把 `running` 改成 `running_now`，本门红并点名
//! 该文件与该词；改回即绿。

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
    for filter in cog_core::TaskFilter::ALL {
        assert_eq!(
            cog_core::TaskFilter::parse(filter.as_str()),
            Some(filter),
            "{} is declared but not readable",
            filter.as_str()
        );
    }
}
