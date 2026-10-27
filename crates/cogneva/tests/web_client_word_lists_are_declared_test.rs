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
//! The board is the same shape three times over: its column names, its column
//! labels and its move branches are each a copy of `cog_core::TaskStatus`. The
//! roster's state cell is `AgentState`'s wire spelling plus the three words the
//! client derives from lifecycle frames itself. The shortest list of all -- who
//! said a line -- is held in both directions: the client's `ChatRole` against
//! the roles the chat socket puts on `message.start` frames and the roles the
//! stored transcript can carry.
//!
//! 实测（2026-10-27）：修前 `web/src/views/TasksView.tsx` 的 `ready` 标签把
//! `status=ready` 发给服务端，而 `TaskStatus` 里没有这个词，标签恒空——表头却
//! 数着「就绪待领 N」。消融对照两条：把 `running` 改成 `running_now`，本门红并
//! 点名该文件与该词；把 `IdentityState::Ready` 的词改成 `configured`，《状态词》
//! 那条红并点名前端联合类型；各自改回即绿。
//!
//! 实测（2026-10-27，看板那三条）：把 `BOARD_COLUMNS` 的 `scheduled` 写成
//! `planned`，列名那条红并列出两组词；删掉 `nextColumns` 的 `case 'failed'`
//! 分支，挪卡那条红并点名 `failed`——那张卡从此一个按钮都画不出来，而屏幕上
//! 它与「这张卡没有合法下一列」一模一样。各自改回即绿。
//!
//! 实测（2026-10-27，在岗名单那一格）：`AgentState` 没有 `ALL`，词表按枚举源码的
//! 变体名加 `rename_all = "snake_case"` 折出来，所以核心加一个状态也会红。把
//! `STATE_LABEL` 的 `suspect` 删掉，本门红并列出两侧的词。
//!
//! 实测（2026-10-27，谁说的那一句）：这张表在客户端被抄了四遍，其中三份还带着
//! `system`——`cog_core::Message::role()` 会返回它，可聊天套接字与落盘对话行
//! 两个写者都只写 `user`/`assistant`，界面那支「系统」从写下来起一次都没亮过。
//! 现在只剩 `types/events.ts` 一处声明，另外三处改成从它取。消融对照两条：
//! 把 `'system'` 加回联合类型，本门红并列出两侧的词；把 `chat.rs` 里一处
//! `"user"` 改成 `"system"`，帧与落盘那张表先红——写者能说、存不下的角色，
//! 重开这段对话时会换一种身份回来。各自改回即绿。
//!
//! 实测（2026-10-27，两处抄件）：网关身份状态那组词在界面里还有第二份
//! （凭据浮层的 `STATE_LABEL`），它不读联合类型、只把词抄了一遍；在岗名单的
//! 两张色表是第三、第四份，稀疏但必须落在状态词里、且两张说的是同一组词。
//! 消融对照两条：浮层把 `pending` 写成 `unconfirmed`，身份那条红并列出两组词；
//! `STATE_TONE` 加一个 `zombie`，在岗名单那条红并点名这个词。各自改回即绿。

mod common;

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

    // 同一组词在界面里还有第二份：凭据浮层的 `STATE_LABEL` 按状态给出那一句
    // 中文。它不读那个联合类型，只把词抄了一遍，所以联合类型改了它不会跟着改——
    // 改漏的那个状态在浮层上落到兜底，读者看到的是另一个词的意思。
    let overlay_path = repo_root().join("web/src/overlay/ContributionOverlay.tsx");
    let overlay = std::fs::read_to_string(&overlay_path)
        .unwrap_or_else(|e| panic!("{}: {e}", overlay_path.display()));
    let mut labelled = table_keys(&overlay, "const STATE_LABEL: Record<string, string> = {");
    labelled.sort();
    assert_eq!(
        labelled, declared,
        "凭据浮层的状态说法必须正好盖住网关声明的状态词"
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

/// 面与横切线是分开数的两类：普查把「十三个面 + 五条线」报成两组数，混成一个
/// 总数会让「丢了一个面、多了一条线」照样对得上。
///
/// 面板原来把 `kind` 写成 `'face' | 'cross' | string`：联合类型只是注释，多出来
/// 的一类照样通过类型检查，落进「类型」那一列的原文里，与两类之一长得一样。
#[test]
fn the_face_kinds_the_panel_shows_are_the_ones_the_registry_declares() {
    use cog_core::contract::face_registry::FaceKind;

    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let body_start = source
        .find("export interface GovernanceFaceSpec {")
        .expect("the face spec type is gone; this gate reads its kind through it");
    let body = &source[body_start..];
    let body_end = body.find("\n}").expect("the interface body is not closed");
    let mut panel = union_words(&body[..body_end], "  kind:");
    panel.sort();
    assert!(
        panel.len() >= 2,
        "种类联合类型读出来是 {panel:?}；这么短说明解析变了，不是树变干净了"
    );

    let mut declared: Vec<String> = FaceKind::ALL
        .iter()
        .map(|kind| kind.as_str().to_string())
        .collect();
    declared.sort();
    assert_eq!(panel, declared, "面板与注册表必须认识同一组面的种类");

    // 每一种都要能原样读回来：`parse` 里少一支，那一类面就整行读不进来。
    for kind in FaceKind::ALL {
        assert_eq!(FaceKind::parse(kind.as_str()), Some(kind));
    }
    assert_eq!(FaceKind::parse("cross_cutting"), None);
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

/// The keys of a `const X: Record<...> = {` table, one per line, up to `};`.
fn table_keys(source: &str, start: &str) -> Vec<String> {
    let from = source
        .find(start)
        .unwrap_or_else(|| panic!("{start:?} is gone; this gate reads the table through it"))
        + start.len();
    let rest = &source[from..];
    let end = rest.find("\n};").expect("the object literal is not closed");
    rest[..end]
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let key: String = line
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            (line[key.len()..].trim_start().starts_with(':') && !key.is_empty()).then_some(key)
        })
        .collect()
}

/// 看板那六列就是 `TaskStatus` 的六个拼写，客户端把它们又抄了一遍：列名一份
/// (`BOARD_COLUMNS`，决定 `moveTask` 把哪个词发给服务端)、列标题一份
/// (`COLUMN_LABEL`)。两处都没有形状让编译器去比对——多一个词，服务端没有那一
/// 列，挪卡发出去的是一个它不认识的状态；少一个词，那个状态的卡片落在没有标题
/// 的一列，画出来与「这一列本来就是空的」一模一样。
#[test]
fn the_board_columns_are_the_task_statuses_the_server_stores() {
    let path = repo_root().join("web/src/api/client.ts");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let mut columns = array_words(&source, "export const BOARD_COLUMNS = [");
    columns.sort();
    assert!(
        columns.len() >= 4,
        "看板列名读出来是 {columns:?}；这么短说明解析变了，不是树变干净了"
    );

    let mut declared: Vec<String> = cog_core::TaskStatus::ALL
        .iter()
        .map(|status| status.as_str().to_string())
        .collect();
    declared.sort();
    assert_eq!(
        columns, declared,
        "看板的列名与任务状态必须是同一组词：列名就是 `moveTask` 发给服务端的那个词"
    );

    let board_path = repo_root().join("web/src/projects/projectWrite.tsx");
    let board = std::fs::read_to_string(&board_path)
        .unwrap_or_else(|e| panic!("{}: {e}", board_path.display()));
    let mut labelled = table_keys(&board, "const COLUMN_LABEL: Record<string, string> = {");
    labelled.sort();
    assert_eq!(
        labelled, declared,
        "列标题表与任务状态必须是同一组词：缺一行时那一列没有中文名，"
    );
}

/// 挪卡的准入是同一套词的第三份抄写：`nextColumns` 的 `switch` 按状态给出下一列。
/// `default: return []` 让「不认识的词」与「终态」长得一样——卡片上一个按钮都不
/// 画，读作「这张卡不能挪」。所以这里两个方向都问：每个 `case` 词必须是任务状态，
/// 每个任务状态必须有分支（除了 `NO_NEXT` 里写明理由的两个终态）。
#[test]
fn the_board_moves_are_spelled_with_the_task_statuses() {
    const NO_NEXT: &[(&str, &str)] = &[
        (
            "completed",
            "完成了就没有下一列：再往下要么是一条新任务，要么是重开",
        ),
        (
            "cancelled",
            "取消了就没有下一列：要复活是新建一条，不是把这张卡挪回去",
        ),
    ];

    let board_path = repo_root().join("web/src/projects/projectWrite.tsx");
    let board = std::fs::read_to_string(&board_path)
        .unwrap_or_else(|e| panic!("{}: {e}", board_path.display()));
    let start = board
        .find("export function nextColumns(")
        .expect("nextColumns 不在了；本门通过它读挪卡的准入");
    let body = &board[start..];
    let end = body.find("\n}\n").expect("nextColumns 的函数体没有闭合");
    let body = &body[..end];

    let mut arms: Vec<String> = Vec::new();
    let mut from = 0;
    while let Some(at) = body[from..].find("case '") {
        let start = from + at + "case '".len();
        from = start;
        let word: String = body[start..].chars().take_while(|c| *c != '\'').collect();
        arms.push(word);
    }
    arms.sort();
    arms.dedup();
    assert!(
        arms.len() >= 3,
        "nextColumns 只认出 {arms:?} 这几个分支；这么少说明解析变了"
    );

    let mut declared: Vec<String> = cog_core::TaskStatus::ALL
        .iter()
        .map(|status| status.as_str().to_string())
        .collect();
    declared.sort();

    for arm in &arms {
        assert!(
            declared.contains(arm),
            "nextColumns 认一个不是任务状态的词 {arm:?}：这条分支永远不会被走到"
        );
    }
    for word in &declared {
        if NO_NEXT.iter().any(|(terminal, _)| terminal == word) {
            continue;
        }
        assert!(
            arms.contains(word),
            "状态 {word:?} 在 nextColumns 里没有分支：这个状态的卡片一个按钮都画不出来，\
             而「没有合法下一列」与「代码里没写这一支」在屏幕上是同一个样子"
        );
    }
    // 登记的两个词确实还没有下一列：`nextColumns` 对它们只走 default。
    for (terminal, reason) in NO_NEXT {
        assert!(
            !arms.contains(&terminal.to_string()),
            "{terminal:?} 已经有分支了，这条登记（{reason}）该删掉"
        );
    }
}

/// 把 `AgentState` 的变体名按 `rename_all = "snake_case"` 折成线上写法。
fn pascal_to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    for (index, c) in name.chars().enumerate() {
        if c.is_uppercase() && index > 0 {
            out.push('_');
        }
        out.extend(c.to_lowercase());
    }
    out
}

/// 在岗名单上那一格状态：`AgentsView` 的 `STATE_LABEL` 是 `AgentState` 线上写法
/// 的一份抄写（serde 的 snake_case），另加三个由客户端自己按帧翻出来的词——
/// `agent_start` → `started`、`agent_end` → `ended`、`agent_error` → `error`。
///
/// 少一个词：那个状态的执行者显示英文原词，而注释里「照原样显示」的本意是让
/// 「核心新加了一个状态」被看见；多一个词：为一段永远不会到来的状态留着一句
/// 中文，读的人以为那一格会亮。两个方向在这里都要对得上。
#[test]
fn the_agent_states_the_roster_labels_are_the_ones_the_core_serialises() {
    let core_path = repo_root().join("crates/cog-core/src/types/agent_state.rs");
    let core = std::fs::read_to_string(&core_path)
        .unwrap_or_else(|e| panic!("{}: {e}", core_path.display()));
    assert!(
        core.contains("#[serde(rename_all = \"snake_case\")]"),
        "AgentState 的线上写法不再由 rename_all 决定：本门按 snake_case 折变体名，\
         改了就得跟着改这里"
    );
    let start = core
        .find("pub enum AgentState {")
        .expect("AgentState 不在了；本门通过它读执行者状态的线上写法")
        + "pub enum AgentState {".len();
    let body = &core[start..];
    let end = body.find("\n}").expect("AgentState 的枚举体没有闭合");
    let mut declared: Vec<String> = body[..end]
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//") && !line.starts_with("#["))
        .map(|line| {
            let variant: String = line.chars().take_while(|c| c.is_alphanumeric()).collect();
            pascal_to_snake(&variant)
        })
        .collect();
    declared.sort();
    assert_eq!(
        declared.len(),
        8,
        "AgentState 读出来是 {declared:?}；变体数变了就要看客户端那一侧跟没跟上"
    );

    let view_path = repo_root().join("web/src/views/AgentsView.tsx");
    let view = std::fs::read_to_string(&view_path)
        .unwrap_or_else(|e| panic!("{}: {e}", view_path.display()));
    let mut labelled = table_keys(&view, "const STATE_LABEL: Record<string, string> = {");
    labelled.sort();
    assert!(
        labelled.len() >= 8,
        "状态格读出来是 {labelled:?}；这么短说明解析变了，不是树变干净了"
    );

    // 客户端自己翻出来的三个词，出处必须是 `liveAgents.ts` 的那个 switch。
    const CLIENT_WORDS: &[(&str, &str)] = &[
        ("started", "state: 'started'"),
        ("ended", "state: 'ended'"),
        ("error", "state: 'error'"),
    ];
    let live_path = repo_root().join("web/src/store/liveAgents.ts");
    let live = std::fs::read_to_string(&live_path)
        .unwrap_or_else(|e| panic!("{}: {e}", live_path.display()));
    for (word, needle) in CLIENT_WORDS {
        assert!(
            live.contains(needle),
            "`{word}` 被当成客户端自己的词放行了，但 {live_path:?} 里没有 {needle:?}：\
             这一条登记得撤掉，或者那个帧分支改了而标签表没跟上"
        );
    }

    let mut expected: Vec<String> = declared.clone();
    expected.extend(CLIENT_WORDS.iter().map(|(word, _)| word.to_string()));
    expected.sort();
    assert_eq!(
        labelled, expected,
        "状态格的中文名必须正好盖住 `AgentState` 的线上写法加客户端自己翻出来的三个词"
    );

    // 两张色表是稀疏的：只给需要区分的状态上色，其余走兜底，所以这里要求的是
    // 键落在状态词里、不是盖满。键留着一个已经不存在（或被改名）的状态，那一格
    // 就是一支永远上不了的色；两张表说的不是同一组词，同一个状态在状态格与
    // 分布条上就会是两种颜色，而颜色正是这两张表唯一的作用。
    let tone = table_keys(&view, "const STATE_TONE: Record<string, string> = {");
    let plot = table_keys(&view, "const STATE_PLOT_TONE: Record<string, Tone> = {");
    for (table, keys) in [("STATE_TONE", &tone), ("STATE_PLOT_TONE", &plot)] {
        assert!(
            !keys.is_empty(),
            "{table} 一个键都没读到：解析坏了，不是表空了"
        );
        for key in keys {
            assert!(
                expected.contains(key),
                "{table} 里的 `{key}` 不是一个状态词：色表为一个永远到不了的状态\
                 留着一支从没上过的色，而真到了的那个状态走兜底——两者画出来一样"
            );
        }
    }
    let mut tone_sorted = tone.clone();
    let mut plot_sorted = plot.clone();
    tone_sorted.sort();
    plot_sorted.sort();
    assert_eq!(
        tone_sorted, plot_sorted,
        "状态格与分布条必须是同一组词：同一个状态在两张图上不同色，读者会当成两种处境"
    );
}

/// 一段文本里每次 `callee(…)` 的括号内容，声明那一处不算。
///
/// 按括号配平读，不按行：三个调用点目前都写在一行里，但一条只认单行的规则在
/// 换行排版那天就会悄悄少读一个调用——少读的那个角色会被当成「没人写」。
///
/// 声明本身也要跳过：`async fn emit_message_start(` 的形参表长得和调用一模一样，
/// 把它当成一次调用会读出一个 `role: &str`。
fn call_arguments<'a>(text: &'a str, callee: &str, out: &mut Vec<&'a str>) {
    let bytes = text.as_bytes();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find(callee) {
        let name_at = cursor + at;
        let start = name_at + callee.len();
        cursor = start;
        if bytes.get(start) != Some(&b'(') {
            continue;
        }
        let mut depth = 0i32;
        let mut i = start;
        while i < bytes.len() {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        if !text[..name_at].trim_end().ends_with("fn") {
            out.push(&text[start + 1..i.min(bytes.len())]);
        }
        cursor = (i + 1).min(bytes.len());
    }
}

/// 实参表里的最后一格（顶层的逗号才分隔实参）。
fn last_argument(args: &str) -> &str {
    let mut depth = 0i32;
    let mut last = 0usize;
    for (i, ch) in args.char_indices() {
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => last = i + 1,
            _ => {}
        }
    }
    args[last..].trim()
}

/// 一段代码里出现的带引号字面量。
fn quoted(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('"') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('"') else { break };
        words.push(after[..close].to_string());
        rest = &after[close + 1..];
    }
    words
}

/// 「这句话是谁说的」是全平台最短的一张词表，而它被写了四遍：聊天帧的联合类型、
/// 落盘对话行的 `ChatTurnRole`、聊天套接字 `message.start` 帧的实参，以及界面
/// 按角色选称呼的那几支。第四份就写在客户端里，也就是本门读的这一份。
///
/// 三个可能的写者里有两个是真写者：套接字帧（`emit_message_start`）与落盘行
/// （`ChatTurnRole::as_str`）。第三处 `cog_core::Message::role()` 也能返回角色，
/// 但它服务的是模型请求的组装，聊天面上一格都到不了——它的 `"tool"` 从来没在
/// 这两个通道上出现过，所以不在这张表里；真有一天到了，写者那一侧先红。
///
/// 这里曾经多一个 `system`：三个人各自抄了一遍联合类型，都把 `Message::role()`
/// 的词表当成了聊天面的词表。聊天帧上它一格都到不了，界面那支「系统」永远不亮。
#[test]
fn the_chat_roles_the_client_names_are_the_roles_the_writers_write() {
    let events_path = repo_root().join("web/src/types/events.ts");
    let events = std::fs::read_to_string(&events_path)
        .unwrap_or_else(|e| panic!("{}: {e}", events_path.display()));
    let mut client = union_words(&events, "export type ChatRole =");
    client.sort();
    assert!(
        client.len() >= 2,
        "ChatRole 读出来是 {client:?}：词表解析坏了，不是词变少了"
    );

    // 写者一：聊天套接字上 `message.start` 帧里的角色。
    let chat_path = repo_root().join("crates/cog-gateway/src/chat.rs");
    let chat = std::fs::read_to_string(&chat_path)
        .unwrap_or_else(|e| panic!("{}: {e}", chat_path.display()));
    let production = common::rust_production::production_text(&chat);
    let mut calls = Vec::new();
    call_arguments(&production, "emit_message_start", &mut calls);
    assert!(
        calls.len() >= 2,
        "`emit_message_start` 只读到 {} 处调用：这条判据靠它的实参枚举角色，\
         少读一处就等于把一个角色裁掉",
        calls.len()
    );
    let mut frames: Vec<String> = Vec::new();
    for call in calls {
        let role = last_argument(call);
        let word = role
            .strip_prefix('"')
            .and_then(|r| r.strip_suffix('"'))
            .unwrap_or_else(|| {
                panic!(
                    "`emit_message_start` 的角色实参 {role:?} 不是字面量：本门看不见它写的是什么词，\
                     写者换成变量就得给这里加一条读法，而不是让判据空转"
                )
            });
        frames.push(word.to_string());
    }
    frames.sort();
    frames.dedup();

    // 写者二：落盘的一行是谁说的。
    let store_path = repo_root().join("crates/cog-storage/src/postgres/chat_turn_store.rs");
    let store = std::fs::read_to_string(&store_path)
        .unwrap_or_else(|e| panic!("{}: {e}", store_path.display()));
    let at = store
        .find("pub fn as_str(self)")
        .expect("`ChatTurnRole::as_str` 不在了；本门通过它读落盘对话行的角色词");
    let body = &store[at..];
    let end = body
        .find("\n    }")
        .expect("`ChatTurnRole::as_str` 的函数体没有闭合");
    let mut stored: Vec<String> = quoted(&body[..end])
        .into_iter()
        .filter(|word| !word.is_empty())
        .collect();
    stored.sort();

    assert_eq!(
        frames, stored,
        "聊天帧上的角色与落盘对话行的角色不是同一组词：帧说得出、存不下的角色，\
         重开这段对话时那一行会按另一种角色回来"
    );
    assert_eq!(
        client, frames,
        "客户端认的角色与写者写的不是同一组词：多一个＝为永远不会到来的词留着一支\
         从没跑过的显示；少一个＝那个角色的消息落到兜底分支，与「谁都没说」一样"
    );
}
