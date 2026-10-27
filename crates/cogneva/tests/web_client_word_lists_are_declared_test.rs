//! principle: feedback_no_hardcoding_config_surface
//! A word list in the WebUI is a copy of a vocabulary the server declares, and
//! nothing compares the two at build time: the client ships compiled, the server
//! answers whatever it is asked. A word the server does not know comes back as
//! the empty answer, which is what "nothing there" looks like -- so the operator
//! reads a broken panel as a quiet system.
//!
//! Every list a panel selects on is read here against the declaration the server
//! keeps: the task view's filter tabs against `cog_core::TaskFilter`, the git
//! identity state union against the gateway's `IdentityState`, the project kinds
//! against `ProjectKind`, the backend health words against `BackendState` (plus
//! their colour map), and the cluster verdict against `cog_core::ClusterHealth`.
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

/// 后端健康矩阵的状态词：面板按它选颜色，网关按它说「有没有东西坏了」。两边
/// 各自写一份时，服务端多一个词，那一行落进通用分支（灰色，与「没探出结论」
/// 同形）；少一个词，面板为一支永远不会到来的结论留着一支死分支。
///
/// 实测（2026-10-27）：修前探针把失败文本拼进状态词（`"down: connection
/// refused"`），面板的 `=== 'up'` 与色表都认不出它，那一行读作中性。消融对照：
/// 把 `BackendState::Down` 的词改成 `down!`，本门红并点名前端联合类型。
#[test]
fn the_backend_states_the_panel_knows_are_the_ones_the_gateway_reports() {
    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let mut declared: Vec<String> = cog_gateway::backend_health::BackendState::ALL
        .iter()
        .map(|state| state.as_str().to_string())
        .collect();
    declared.sort();

    for (interface, field) in [
        ("export interface BackendHealth {", "  status:"),
        ("export interface BackendHealthMatrix {", "  overall:"),
    ] {
        let body_start = source
            .find(interface)
            .unwrap_or_else(|| panic!("{interface} is gone; this gate reads its union through it"));
        let body = &source[body_start..];
        let body_end = body.find("\n}").expect("the interface body is not closed");
        let mut panel = union_words(&body[..body_end], field);
        panel.sort();
        assert!(
            panel.len() >= 2,
            "{interface} 的 {field} 读出来是 {panel:?}；这么短说明解析变了，不是树变干净了"
        );
        assert_eq!(
            panel, declared,
            "面板与网关必须认识同一组后端状态词（{interface}）"
        );
    }

    // 每个声明的词在状态色表里都要有一格：缺一格时那一行画成中性的「一般般」，
    // 与「没探出结论」同色，读的人分不出这两种。
    let primitives = repo_root().join("web/src/ui/primitives.tsx");
    let source = std::fs::read_to_string(&primitives)
        .unwrap_or_else(|e| panic!("{}: {e}", primitives.display()));
    let start = source
        .find("const TONES: Record<string, string> = {")
        .expect("the status colour map is gone; this gate reads the tones through it");
    let tones = &source[start..];
    let end = tones.find("\n};").expect("the tone map is not closed");
    let tones = &tones[..end];
    assert!(
        tones.len() > 100,
        "色表只读出 {} 字节，这不是它",
        tones.len()
    );
    for state in cog_gateway::backend_health::BackendState::ALL {
        assert!(
            tones.contains(&format!("\n  {}: '", state.as_str())),
            "状态词 {:?} 在色表里没有颜色：那一行会画成中性的，与「没探出结论」同形",
            state.as_str()
        );
    }
}

/// 集群判词的三个词：网关决定它、两个存储后端默认答它、面板按它选颜色并决定
/// 要不要报一条问题。三处各写一份时，服务端多一个词，状态栏与问题列表都落进
/// 「不是 healthy 也不是 degraded」的分支——状态栏画成告警色、问题列表给它
/// `error`，读起来像一个从没验证过的结论；少一个词，面板为一支永远不会到来的
/// 判词留着一支死分支。
#[test]
fn the_cluster_verdicts_the_panel_knows_are_the_ones_the_gateway_declares() {
    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let body_start = source
        .find("export interface ClusterOverview {")
        .expect("the cluster overview type is gone; this gate reads its verdict through it");
    let body = &source[body_start..];
    let body_end = body.find("\n}").expect("the interface body is not closed");
    let mut panel = union_words(&body[..body_end], "  cluster_health:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "判词联合类型读出来是 {panel:?}；这么短说明解析变了，不是树变干净了"
    );

    let mut declared: Vec<String> = cog_core::ClusterHealth::ALL
        .iter()
        .map(|health| health.as_str().to_string())
        .collect();
    declared.sort();
    assert_eq!(panel, declared, "面板与网关必须认识同一组集群判词");
}

/// 读数板的四个判词：端点按它说「要不要现在动手」，面板按它选颜色、配中文名。
///
/// 两处各写一份时，服务端多一个词，那一行落进 `?? 'var(--paper-dim)'` 与
/// `?? verdict` 的兜底——灰点、英文原词，读起来像「一般般」；少一个词，面板为
/// 一支永远不会到来的判词留着颜色与中文名。面板这一侧即便联合类型对了，把两张
/// 表写成 `Record<string, string>` 也会让「缺一行」从编译错误退化成兜底色，所以
/// 键类型本身也在这里钉住。
#[test]
fn the_reading_verdicts_the_board_knows_are_the_ones_the_endpoint_declares() {
    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let body_start = source
        .find("export interface ObservabilityReading {")
        .expect("the reading type is gone; this gate reads its verdict through it");
    let body = &source[body_start..];
    let body_end = body.find("\n}").expect("the interface body is not closed");
    let mut panel = union_words(&body[..body_end], "  verdict:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "判词联合类型读出来是 {panel:?}；这么短说明解析变了，不是树变干净了"
    );

    let mut declared: Vec<String> = cog_gateway::readings::ReadingVerdict::ALL
        .iter()
        .map(|verdict| verdict.as_str().to_string())
        .collect();
    declared.sort();
    assert_eq!(panel, declared, "面板与端点必须认识同一组读数判词");

    // 两张表都以判词为键：`Record<string, string>` 会让漏掉的一行退化成兜底
    // 颜色，而这里要的是一行不落。
    let board = repo_root().join("web/src/observability/ReadingsBoard.tsx");
    let board =
        std::fs::read_to_string(&board).unwrap_or_else(|e| panic!("{}: {e}", board.display()));
    for table in ["VERDICT_TONE", "VERDICT_WORD"] {
        assert!(
            board.contains(&format!("const {table}: Record<VerdictWord, string> = {{")),
            "{table} 不再以判词为键：缺一行只会画成兜底色，编译期不会红"
        );
    }
    assert!(
        board.contains("type VerdictWord = ObservabilityReading['verdict'];"),
        "判词键类型必须来自端点类型本身，不能另写一份"
    );
}

/// 一句发出去的话落定的五种结局：存储层按它写列、面板按它配那一行的话与颜色。
///
/// 多一个词：会话窗口把英文原词贴在灰字里，读起来像一句没人写过的说明；少一个
/// 词：面板为一支永远不会写入的结局留着一句死文案。面板那张表写成
/// `Record<string, …>` 时，缺一行只会落到兜底分支，编译期也不会红。
#[test]
fn the_send_outcomes_the_panel_knows_are_the_ones_the_store_writes() {
    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let body_start = source
        .find("export interface AgentConversationLine {")
        .expect("the conversation line type is gone; this gate reads its outcome through it");
    let body = &source[body_start..];
    let body_end = body.find("\n}").expect("the interface body is not closed");
    let mut panel = union_words(&body[..body_end], "  outcome:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "结局联合类型读出来是 {panel:?}；这么短说明解析变了，不是树变干净了"
    );

    let mut declared: Vec<String> = cog_storage::SendOutcome::ALL
        .iter()
        .map(|outcome| outcome.as_str().to_string())
        .collect();
    declared.sort();
    assert_eq!(panel, declared, "面板与存储层必须认识同一组结局词");

    let overlay = repo_root().join("web/src/overlay/AgentsOverlay.tsx");
    let overlay =
        std::fs::read_to_string(&overlay).unwrap_or_else(|e| panic!("{}: {e}", overlay.display()));
    assert!(
        overlay.contains(
            "const OUTCOME_LABEL: Record<SendOutcomeWord, { text: string; className: string }> = {"
        ),
        "结局文案表不再以结局为键：缺一行只会贴出英文原词，编译期不会红"
    );
    assert!(
        overlay.contains("type SendOutcomeWord = AgentConversationLine['outcome'];"),
        "结局键类型必须来自端点类型本身，不能另写一份"
    );
}

/// 面的判词的九个词，以及每一个应当画成哪一档。
///
/// 四档不是界面自己分的：`DriftState` 的 `is_red` / `is_yellow` / `is_gray`
/// 判它挡不挡变更，颜色只是那个判定的翻译。色表里缺一行的后果不是画错颜色，
/// 是画成中性的 `--paper-dim`——`no_owner`、`unread` 与「一切正常」同色，而
/// 这两档恰恰是最该被看见的。
#[test]
fn the_drift_verdicts_the_panel_colours_are_the_ones_the_registry_declares() {
    use cog_core::contract::face_registry::DriftState;

    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let body_start = source
        .find("export interface FaceDriftReading {")
        .expect("the drift reading type is gone; this gate reads its state through it");
    let body = &source[body_start..];
    let body_end = body.find("\n}").expect("the interface body is not closed");
    let mut panel = union_words(&body[..body_end], "  state:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "判词联合类型读出来是 {panel:?}；这么短说明解析变了，不是树变干净了"
    );

    let mut declared: Vec<String> = DriftState::ALL
        .iter()
        .map(|state| state.as_str().to_string())
        .collect();
    declared.sort();
    assert_eq!(panel, declared, "面板与注册表必须认识同一组面的判词");

    let primitives = repo_root().join("web/src/ui/primitives.tsx");
    let primitives = std::fs::read_to_string(&primitives)
        .unwrap_or_else(|e| panic!("{}: {e}", primitives.display()));
    let start = primitives
        .find("const TONES: Record<string, string> = {")
        .expect("the status colour map is gone; this gate reads the tones through it");
    let tones = &primitives[start..];
    let end = tones.find("\n};").expect("the tone map is not closed");
    let tones = &tones[..end];

    for state in DriftState::ALL {
        let (tier, expected) = if state.is_red() {
            ("红", "var(--danger)")
        } else if state.is_yellow() {
            ("黄", "var(--warn)")
        } else if state.is_gray() {
            ("灰", "var(--muted)")
        } else {
            ("通过", "var(--ok)")
        };
        assert!(
            tones.contains(&format!("\n  {}: '{}',", state.as_str(), expected)),
            "判词 {:?} 在色表里不是 {expected}：注册表判它为{tier}档，界面必须画成同一档",
            state.as_str()
        );
    }
}

/// 所有者的两个来源：注册表说这一格是声明的还是量出来的。
///
/// 面板原来写 `source === 'declared' ? '声明' : '实测'`，联合类型又是 `| string`：
/// 认不出的来源一律读作「实测」——把「没人报过这个名字」说成「有人量过」。
/// 现在这一列只认声明的两个词，各有一句话，多一个词就落到没有句子的分支。
#[test]
fn the_owner_sources_the_panel_names_are_the_ones_the_registry_declares() {
    use cog_core::contract::face_registry::OwnerSource;

    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let body_start = source
        .find("export interface FaceDriftReading {")
        .expect("the drift reading type is gone; this gate reads its owner source through it");
    let body = &source[body_start..];
    let body_end = body.find("\n}").expect("the interface body is not closed");
    let mut panel = union_words(&body[..body_end], "  owner_source:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "来源联合类型读出来是 {panel:?}；这么短说明解析变了，不是树变干净了"
    );

    // 词取自 serde 自己，不在这条测试里再写一遍：面板要匹配的就是线上的拼写。
    let mut declared: Vec<String> = OwnerSource::ALL
        .iter()
        .map(|source| {
            serde_json::to_value(source)
                .expect("an owner source must be serialisable")
                .as_str()
                .expect("the wire form is a plain word")
                .to_string()
        })
        .collect();
    declared.sort();
    assert_eq!(panel, declared, "面板与注册表必须认识同一组所有者来源");

    // 穷尽 match：新增一个来源而没加进 `ALL`，这里就编译不过——`ALL` 与变体
    // 集合的同步由编译器看着，不靠人记得。
    fn every_variant_is_in_all(source: OwnerSource) -> bool {
        match source {
            OwnerSource::Declared => OwnerSource::ALL.contains(&source),
            OwnerSource::Measured => OwnerSource::ALL.contains(&source),
        }
    }
    for source in OwnerSource::ALL {
        assert!(every_variant_is_in_all(source), "{source:?} 不在 ALL 里");
    }

    let view = repo_root().join("web/src/views/GovernanceView.tsx");
    let view = std::fs::read_to_string(&view).unwrap_or_else(|e| panic!("{}: {e}", view.display()));
    assert!(
        view.contains("const OWNER_SOURCE_WORD: Record<OwnerSourceWord, string> = {"),
        "来源文案表不再以来源为键：认不出的一律落到兜底句子，编译期不会红"
    );
    assert!(
        view.contains("type OwnerSourceWord = FaceDriftReading['owner_source'];"),
        "来源键类型必须来自端点类型本身，不能另写一份"
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
    // 修前探针写的就是这一种：状态词后面拼着失败文本。
    for word in ["down: connection refused", "connected", "Up "] {
        assert_eq!(
            cog_gateway::backend_health::BackendState::parse(word),
            None,
            "{word:?} is not a declared backend state"
        );
    }
    for state in cog_gateway::backend_health::BackendState::ALL {
        assert_eq!(
            cog_gateway::backend_health::BackendState::parse(state.as_str()),
            Some(state),
            "{} is declared but not readable",
            state.as_str()
        );
    }
}
