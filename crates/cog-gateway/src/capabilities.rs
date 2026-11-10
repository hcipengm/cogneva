//! `GET /api/v1/capabilities` —— 这个平台此刻真的做得到的动作，按「项 → 源」列开。
//!
//! **能力是一件可以被点单的动作，不是一个清单文件。** 一项要算数，三条缺一不可：
//! 有名字（别人能用它点单）、有调用点（这个名字走到一处真实执行）、有边界（要什么
//! 参数、做砸了怎么算）。按这个定义，下面这些东西都**不是**能力，只是能力的来源或
//! 者影子：
//!
//! - **插件清单**是来源之一：一个插件声明若干能力，也可能一个都不声明。把「装了几
//!   个插件」读成「会几件事」，是把供给当成了能力；
//! - **Markdown 技能（SKILL.md）**是一份说明书，不是动作。它的能力体现在读它的智能
//!   体能不能真的做出来——`cog-core` 的 `skill.rs` 开头就警告过这个词指两样东西；
//! - **执行手表（`SkillConfig`）**是分工表：这一块活可以交给哪一种手。分解时读它，
//!   它空着是合法状态，不是「没配好」。
//!
//! **不新建任何存储。** 这张表由四个已经在跑的读取面拼出来，各有各的负责人：
//!
//! - 内建动作：`cog_core::ToolRegistry` 的三个读法——`names()`（有什么）、
//!   `definitions()`（要什么参数）与 `calls()`（点过几次），由 `cog-agent` 发布成服务；
//! - 插件动作：`PluginRegistry::discover()` 读磁盘上的清单（工作台那一格读的就是本
//!   文件，不另读一遍磁盘）；
//! - 远端动作：`McpClient::list_tools()`，与 `/api/v1/protocol/mcp/tools` 同一个源；
//! - 谁能点它：共享技能表（`SkillRegistry`）里每个角色声明的 `tools`。读法与运行时
//!   收窄一轮时一样——`skill_for_role` 决定一个角色的多行里哪一行算数——所以这一栏
//!   不会报出运行时并不给的许可。没有声明 `tools` 的角色是「不设边界，拿整张注册
//!   表」，单独一栏列出，不摊到每个动作上；
//! - 持有人：`AgentRegistry::list()` 里每一行自报的 `capabilities` 与心跳。
//!
//! **使用次数只在有计数器的那一层是读数。** 内建这一层由 `cog-agent` 在派发点计
//! （`execute_scoped_checked` 是每一次调用的唯一漏斗），所以那一栏的 0 是真的没人
//! 点过。插件层与远端层没有计数器，那一栏印 `null` 并说清是哪一层缺——0 与 null 要
//! 采取的动作相反：一个去查「为什么没人用」，一个去接计数器。
//!
//! **有名字不等于能调。** 三要件里的「有调用点」这一条今天三层各不相同，所以每一项
//! 都带 `reach`：内建名字来自 `ToolRegistry` 自己的登记（`register` 与 `execute` 是
//! 同一张表），MCP 名字来自对端的 `tools/list` 而网关有 `/protocol/mcp/tools/call`
//! 这条路，这两层点了会真的跑；插件清单里声明的工具**没有任何代码把它登记进执行侧**
//! （全树引用 `PluginCapability::Tool` 的只有本文件），所以它只有名字与参数，点了会
//! 落空。这一条正是本文件开头警告的那个错，只不过换了一个方向：把声明当能力。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::error::ApiError;
use crate::GatewayState;

/// 一项动作从哪来。同名不同源要分开列：同名的两项来自两个源时，删掉其中一个源
/// 并不会让这项能力消失，而按名字合并的表会说它消失了。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// 编译进进程的内建工具。
    Builtin,
    /// 磁盘清单声明的工具。
    Plugin,
    /// 远端 MCP 服务器暴露的工具。
    Mcp,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Builtin => "builtin",
            Source::Plugin => "plugin",
            Source::Mcp => "mcp",
        }
    }
}

/// 这一项此刻能不能真的被调起来。
///
/// 不是布尔：`declared_only` 不是「查了没有」，是「查了，确实没有」——名字与参数都在
/// 清单上，而没有任何一处执行侧登记它。将来接上了，这个值要靠一次有意的改动翻过来。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reach {
    /// 名字在执行侧有登记：按名字点单会走到一处真实执行。
    Callable,
    /// 只有声明：名字存在，点了会落空。
    DeclaredOnly,
}

impl Reach {
    pub fn as_str(self) -> &'static str {
        match self {
            Reach::Callable => "callable",
            Reach::DeclaredOnly => "declared_only",
        }
    }
}

/// 一层来源的名字能不能调。
///
/// 每一层单列一条，是因为它们会分叉：同一份清单里，内建与 MCP 的行可以调，插件的行
/// 不行。把它写成一个函数而不是在拼装处各写一遍，是为了让「接上插件工具」这件事必须
/// 在这里改一次——下面有一条测试钉着当前的值。
fn reach_of(source: Source) -> Reach {
    match source {
        Source::Builtin | Source::Mcp => Reach::Callable,
        Source::Plugin => Reach::DeclaredOnly,
    }
}

/// 一个来源这一拍读到了没有。
///
/// 三态而不是一个数：`measured` 印条数，`unwired` 说清缺哪一层，`unread` 说清这一
/// 拍为什么读失败。它们要的动作不同——缺一层要去接，读失败要去查，条数才是结论。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SourceReading {
    /// 这一层供了多少个名字，其中几个点了会真的跑。
    ///
    /// 两个数都要：只报条数会把「声明了但没人接」读成「会做这件事」，只报可调数则看
    /// 不出这一层到底声明了多少。
    Measured {
        entries: usize,
        callable: usize,
    },
    Unwired {
        because: String,
    },
    Unread {
        because: String,
    },
}

/// 一次调用要带什么。这是三要件里的「边界」。
///
/// 只读声明里的两样：要几个参数、哪几个必填。参数各自的类型与说明留在原始 schema
/// 里——这一面答的是「这一项能不能被正确地点」，不是一份接口文档。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Boundary {
    /// 必填参数名，按名字排。
    pub required: Vec<String>,
    /// schema 里声明了多少个参数，含可选。
    pub params: usize,
}

impl Boundary {
    /// 一句话说法：`3 个参数，其中 2 个必填`。页面上原样印这一句。
    pub fn label(&self) -> String {
        if self.params == 0 {
            return "不要参数".into();
        }
        if self.required.is_empty() {
            return format!("{} 个参数，都可选", self.params);
        }
        format!(
            "{} 个参数，其中 {} 个必填",
            self.params,
            self.required.len()
        )
    }
}

/// 一份 JSON Schema 里的参数边界。
///
/// `None` 不是「零个参数」，是「没有 schema 可读」——插件与远端都可能只报一个名字。
/// 两者混成一个「0 个参数」，读的人会以为这一项点了就能跑。
fn boundary_of(schema: Option<&serde_json::Value>) -> Option<Boundary> {
    let schema = schema?.as_object()?;
    let params = schema
        .get("properties")
        .and_then(|v| v.as_object())
        .map(|p| p.len())
        .unwrap_or(0);
    let mut required: Vec<String> = schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    required.sort();
    Some(Boundary { required, params })
}

/// 这一项在哪里跑。
///
/// 三要件里的「做砸了怎么算」在读数上就只有这一个抓手：同一个名字，在进程里跑失败
/// 是一个错误返回值，在 WASM 沙箱里跑是超时或被内存上限掐掉，经远端执行器跑是一个
/// 不再应答的对端。要说清失败长什么样，先得说清它在哪一层。
///
/// 只对读得到实现的内建工具成立：插件清单与远端对端只报名字，它们在哪跑这一面读
/// 不到，那一栏是 `None` 而不是猜一个。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunsIn {
    /// 编译进这个进程的闭包：失败是一次调用返回错误。
    Native,
    /// 插件 WASM：失败是沙箱超时或被上限掐掉。
    Wasm,
    /// 经远端执行器：失败是一个不再应答的对端。
    Shell,
}

/// 一项能力。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Entry {
    pub name: String,
    pub source: Source,
    /// 这一项此刻能不能真的调起来。见 [`Reach`]。
    pub reach: Reach,
    /// 声明方写的用途。内建工具与 MCP 工具各有自己的措辞，能取到就带上。
    pub description: Option<String>,
    /// 调这一项要带什么。`None` 是这一层没给 schema，不是「不用参数」。
    pub boundary: Option<Boundary>,
    /// 这一项在哪一层里跑，也就是它失败时是什么样子。`None` 是这一层没报实现。
    pub runs_in: Option<RunsIn>,
    /// 这一台进程里被点过几次。
    ///
    /// `None` 是这一层没有计数器，不是「一次都没用过」：那个 0 只在有计数器的那
    /// 几层才是读数。今天只有内建这一层记（`cog-agent` 在派发点计），插件声明的
    /// 动作本来就没人能调，远端 MCP 的调用走的是网关自己的那条路由，不经过这张
    /// 登记表。
    pub calls: Option<u64>,
    /// 分工表里点名允许调它的角色，按角色名排。
    ///
    /// `None` 是这一拍没读到分工表（部署没发布共享技能表），不是「没有角色被
    /// 允许」——后者是 `Some(空)`。两种情形要的动作相反：一个去接表，一个是
    /// 「这一项没有角色点名」，读的人要能分清。
    pub allowed_roles: Option<Vec<String>>,
    /// 自报了这一项的单位。空集合不是「没人会用」，是「没人这么自报」——这两句
    /// 之间差着一次改名，读的人要能分清。
    pub holders: Vec<Holder>,
}

/// 一个自报了某项能力的单位，以及它此刻在不在岗。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Holder {
    pub unit: String,
    pub live: bool,
}

/// 一台机器上「会什么」的全貌。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Inventory {
    pub generated_at: chrono::DateTime<chrono::Utc>,
    /// 按来源排好的读数。顺序固定：内建、插件、MCP。
    pub sources: Vec<(Source, SourceReading)>,
    /// 全部项，去重后按名字排。同名来自两个源的各占一行。
    pub entries: Vec<Entry>,
    /// 自报过能力的单位数。0 且名册非空，说明这一栏没人填——那是接线缺口，不是
    /// 「没有人有能力」。
    ///
    /// `None` 是名册这一拍没读到（没接上，或读失败了）：那时「没有人自报」这句话
    /// 无从说起，而它与「0 个单位自报」要采取的动作相反，所以在读数上就分开。
    pub declaring_units: Option<usize>,
    /// 名册这一拍没读到时的原因。
    ///
    /// `None` 是读到了（含「读到但没有单位自报」）。名册读不到时，持有人那一栏
    /// 无从判断，而「为什么无从判断」要能说出来——否则读者只知道这一栏空着，分
    /// 不清是该去接名册还是该去查读失败。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roster_caveat: Option<String>,
    /// 没有声明 `tools` 的角色，按角色名排。
    ///
    /// 分工表里空表的意思是「不设边界，拿整张注册表」，所以这些角色能调每一项。
    /// 把它们摊到每一行上会把同一份名单印几十遍；放在这里一次，页面上一次说清。
    /// `None` 是分工表没读到，不是「没有这种角色」——后者是 `Some(空)`。
    pub unbounded_roles: Option<Vec<String>>,
}

/// 一份「什么都没接」的清单。
///
/// 存在的理由只有一个：调用方（工作台的夹具、别的装配）要能说「这一拍没有能力
/// 读数」而不必自己编一份。它不含任何来源读数，所以工作台那一格会印「这一层没有报
/// 读数」——那正是空清单该说的话，不是「零项能力」。
impl Default for Inventory {
    fn default() -> Self {
        Self {
            generated_at: chrono::Utc::now(),
            sources: Vec::new(),
            entries: Vec::new(),
            declaring_units: None,
            roster_caveat: None,
            unbounded_roles: None,
        }
    }
}

impl Inventory {
    /// 表里一共有几项。按层拆开的数在 `sources` 里，这里只有一个总数。
    ///
    /// 不叫 `len`：`len` 会招来「有没有 `is_empty`」那一问，而这份清单「空」的
    /// 含义有两层（三层都没接 vs 三层都读到了但一项没有），一个布尔答不清。
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// 表里真的有调用点的那几项。工作台那一格印这个数：那一格答「会做什么」，而
    /// 声明了没人接的名字不构成「会做」。「声明了几项」留在 [`Self::entry_count`]，
    /// 两个数各答一个问题。
    pub fn callable_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.reach == Reach::Callable)
            .count()
    }
}

/// 拼出这份清单。任何一个来源读不到都不影响其余来源——缺的那一层在 `sources` 里
/// 自报，其余照常给出。
pub async fn read(state: &GatewayState) -> Inventory {
    let mut entries: Vec<Entry> = Vec::new();
    let mut sources: Vec<(Source, SourceReading)> = Vec::new();

    // ── 内建 ──
    let builtin = match state.tool_registry.as_ref() {
        None => SourceReading::Unwired {
            because: "这个部署没有发布工具注册表".into(),
        },
        Some(registry) => {
            // 名字、说明、参数、调用次数四样都从注册表这一张表上取：这一面要答的
            // 「会不会做、怎么点、有没有人在点」是同一件事的四个面，分开去别的
            // 地方读第二个版本，就会有一处漂。
            let calls = registry.calls();
            let definitions = registry.definitions();
            for tool in &definitions {
                entries.push(Entry {
                    name: tool.name.clone(),
                    source: Source::Builtin,
                    reach: reach_of(Source::Builtin),
                    description: Some(tool.description.clone()),
                    boundary: boundary_of(Some(&tool.parameters)),
                    runs_in: Some(match &tool.implementation {
                        cog_core::ToolImplementation::Native(_) => RunsIn::Native,
                        cog_core::ToolImplementation::Wasm { .. } => RunsIn::Wasm,
                        cog_core::ToolImplementation::Shell(_) => RunsIn::Shell,
                    }),
                    calls: calls.get(&tool.name).copied(),
                    allowed_roles: None,
                    holders: Vec::new(),
                });
            }
            SourceReading::Measured {
                entries: definitions.len(),
                callable: definitions.len(),
            }
        }
    };
    sources.push((Source::Builtin, builtin));

    // ── 插件 ──
    let plugin = match state.plugin_registry.as_ref() {
        None => SourceReading::Unwired {
            because: "这个部署没有插件注册表".into(),
        },
        Some(registry) => {
            match registry
                .discover(&cog_core::paths::app_path_str("plugins"))
                .await
            {
                Ok(manifests) => {
                    let mut count = 0usize;
                    for manifest in &manifests {
                        for capability in &manifest.capabilities {
                            // 只有 `Tool` 是一项可点单的动作。`Skill` 是说明书，
                            // `Backend` 是一种接入方式，它们都不是能力本身——列进来
                            // 会把「有这种接入」说成「会做这件事」。
                            if let cog_core::PluginCapability::Tool {
                                name,
                                description,
                                parameters,
                            } = capability
                            {
                                entries.push(Entry {
                                    name: name.clone(),
                                    source: Source::Plugin,
                                    reach: reach_of(Source::Plugin),
                                    description: Some(description.clone()),
                                    boundary: boundary_of(Some(parameters)),
                                    // 插件清单只报名字、说明与参数，不报实现：它在哪里
                                    // 跑这一面读不到，所以这一栏是 `None`，不是猜一个。
                                    runs_in: None,
                                    // 这层没有计数器，而且今天也没有调用点：印 0 会
                                    // 读成「量过，没人用」。
                                    calls: None,
                                    allowed_roles: None,
                                    holders: Vec::new(),
                                });
                                count += 1;
                            }
                        }
                    }
                    // 声明了几项就是几项，可调数由 `finish` 按这一层每行的 `reach` 重
                    // 算——两个数必须同源，否则「插件 3」而表里三行都说不能调，读的人
                    // 会以为接口坏了。
                    SourceReading::Measured {
                        entries: count,
                        callable: 0,
                    }
                }
                Err(e) => SourceReading::Unread {
                    because: format!("读插件清单失败：{e}"),
                },
            }
        }
    };
    sources.push((Source::Plugin, plugin));

    // ── MCP ──
    let mcp = match state.mcp_client.as_ref() {
        None => SourceReading::Unwired {
            because: "这个部署没有配置 MCP 对端（COGNEVA_MCP_ENDPOINT / _COMMAND）".into(),
        },
        Some(client) => match client.list_tools().await {
            Ok(tools) => {
                for tool in &tools {
                    let Some(name) = tool.get("name").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    entries.push(Entry {
                        name: name.to_string(),
                        source: Source::Mcp,
                        reach: reach_of(Source::Mcp),
                        description: tool
                            .get("description")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        // MCP 的参数表在 `inputSchema` 里，与内建工具同一个形状
                        // （JSON Schema），所以用同一段读法。没报 `inputSchema`
                        // 的对端不少，那时是 `None`，不是「不用参数」。
                        boundary: boundary_of(tool.get("inputSchema")),
                        // 远端工具跑在对端，不在这个进程里；对端没报实现方式，
                        // 这一栏是 `None`。
                        runs_in: None,
                        // 远端调用走网关的 `/protocol/mcp/tools/call`，不经过内建
                        // 那张登记表，所以这一层没有数可报。
                        calls: None,
                        allowed_roles: None,
                        holders: Vec::new(),
                    });
                }
                SourceReading::Measured {
                    entries: tools.len(),
                    callable: tools.len(),
                }
            }
            Err(e) => SourceReading::Unread {
                because: format!("列 MCP 工具失败：{e}"),
            },
        },
    };
    sources.push((Source::Mcp, mcp));

    // ── 谁能点它 ──
    // 从角色分工表读，不从名册自报读：名册那一栏写的是角色名，这一面要的是动作
    // 名，两边说的不是同一组词（下面 `holders` 就是这件事的现场）。读法与运行时
    // 收窄一轮时一样——`skill_configs` 数出有哪些角色，`skill_for_role` 决定同一个
    // 角色有多行时哪一行算数。用别的读法，这一栏会报出运行时并不会给的许可。
    let (allowed, unbounded_roles) = match state.skill_registry.as_ref() {
        None => (None, None),
        Some(registry) => {
            let registry = registry.read().await;
            let (per_tool, unbounded) = callers_by_tool(&registry);
            (Some(per_tool), Some(unbounded))
        }
    };

    // ── 持有人 ──
    // 名册读不到时 `declaring_units` 是 `None`，不是 0：那时两种情形在读数上同形，
    // 而它们要的动作相反——一栏没人填是接线缺口，名册没读到是这一拍读失败。
    let (declared, declaring_units, roster_caveat) = match state.agent_registry.as_ref() {
        None => (
            BTreeMap::new(),
            None,
            Some("这个部署没有员工名册".to_string()),
        ),
        Some(registry) => match registry.list().await {
            Ok(agents) => {
                let mut declared: BTreeMap<String, Vec<Holder>> = BTreeMap::new();
                let mut declaring = 0usize;
                let now = chrono::Utc::now();
                for agent in agents {
                    if agent.capabilities.is_empty() {
                        continue;
                    }
                    declaring += 1;
                    // 心跳多久算在岗：用的是面主那套判据，不另造一个界。同一个
                    // 「这人还在不在」在两张表上给出两个答案，读的人只会记住先
                    // 看到的那个。
                    let live = (now - agent.last_heartbeat).num_seconds()
                        < cog_core::contract::face_registry::OWNER_IDLE_AFTER_SECS;
                    for capability in &agent.capabilities {
                        declared
                            .entry(capability.clone())
                            .or_default()
                            .push(Holder {
                                unit: agent.agent_id.clone(),
                                live,
                            });
                    }
                }
                (declared, Some(declaring), None)
            }
            Err(e) => (BTreeMap::new(), None, Some(format!("读员工名册失败：{e}"))),
        },
    };
    for entry in entries.iter_mut() {
        if let Some(holders) = declared.get(&entry.name) {
            entry.holders = holders.clone();
        }
        if let Some(allowed) = allowed.as_ref() {
            // 分工表读到了，这一项有没有角色点名都要写成 `Some`：空表是读数
            // （没有角色点名它），`None` 才是没读到。
            entry.allowed_roles = Some(allowed.get(&entry.name).cloned().unwrap_or_default());
        }
    }

    finish(
        entries,
        sources,
        declaring_units,
        roster_caveat,
        unbounded_roles,
    )
}

/// 分工表里「谁能点它」：没有角色声明边界，所以每个角色都能点每一项。
///
/// 工具只有一套：手上有什么由**进程装了什么**决定，不由「它是哪种手」决定。技能行
/// 上曾经有一张 `tools` 白名单，按角色收窄一轮的工具表；那一栏现在没有来源了，
/// 于是这里只报一份角色名单，全部落在「不设边界」那一侧。
///
/// 空表这一档不是「什么都不能调」，是「拿整张注册表」：把它读成禁止，恰好把最宽
/// 的那个角色读成最窄的。
///
/// 单独一段是为了能判：它只吃一份技能表，不需要一棵接了插件的网关状态。
fn callers_by_tool(
    registry: &cog_core::SkillRegistry,
) -> (BTreeMap<String, Vec<String>>, Vec<String>) {
    let roles: BTreeSet<String> = registry
        .skill_configs()
        .into_iter()
        .map(|config| config.role_type.clone())
        .collect();
    // 角色名已经按 `BTreeSet` 排过：同一项的两个角色，顺序不随哈希历史变。
    (BTreeMap::new(), roles.into_iter().collect())
}

/// 把三路拼到的东西收成一份清单：排序、同一来源里同名的合成一行、每层条数与行数同源。
///
/// 单独一段是为了能判：它只吃几个已经拼好的值，不需要一棵接了插件的网关状态。
fn finish(
    mut entries: Vec<Entry>,
    mut sources: Vec<(Source, SourceReading)>,
    declaring_units: Option<usize>,
    roster_caveat: Option<String>,
    unbounded_roles: Option<Vec<String>>,
) -> Inventory {
    entries.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.source.as_str().cmp(b.source.as_str()))
    });
    // 同一来源里同名的合成一行：两个插件各自声明一个 `search`，是这一层有两个来源
    // 提供同一个动作，不是两件不同的动作。按名字点单的人只认那个词。留下的那一行
    // 保留先读到的那条说明。
    entries.dedup_by(|a, b| a.name == b.name && a.source == b.source);

    // 每一层报的条数必须与表里这一层的行数同源：拼装时数的是声明条数，去重之后的
    // 行会少一行。两个数落在同一屏上而各说一套，读者只会记住先看到的那个。可调数同
    // 理——它是这一层行里 `reach == Callable` 的行数，不从拼装时那个数推。
    for (source, reading) in sources.iter_mut() {
        if let SourceReading::Measured {
            entries: n,
            callable,
        } = reading
        {
            let of_this_layer = || entries.iter().filter(|e| e.source == *source);
            *n = of_this_layer().count();
            *callable = of_this_layer()
                .filter(|e| e.reach == Reach::Callable)
                .count();
        }
    }

    Inventory {
        generated_at: chrono::Utc::now(),
        sources,
        entries,
        declaring_units,
        roster_caveat,
        unbounded_roles,
    }
}

pub async fn capabilities_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Inventory>, ApiError> {
    Ok(Json(read(&state).await))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 同名不同源各占一行：按名字合并会让「删掉一个源」看起来像「这项能力没了」。
    #[test]
    fn the_same_name_from_two_sources_is_two_rows() {
        let entry = |source, description: Option<&str>| Entry {
            name: "search".into(),
            source,
            reach: reach_of(source),
            description: description.map(str::to_string),
            boundary: None,
            runs_in: None,
            calls: None,
            allowed_roles: None,
            holders: Vec::new(),
        };
        let inventory = finish(
            vec![
                entry(Source::Plugin, Some("插件版的检索")),
                entry(Source::Builtin, None),
            ],
            vec![
                (
                    Source::Builtin,
                    SourceReading::Measured {
                        entries: 1,
                        callable: 1,
                    },
                ),
                (
                    Source::Plugin,
                    SourceReading::Measured {
                        entries: 1,
                        callable: 0,
                    },
                ),
            ],
            Some(0),
            None, // roster_caveat
            None, // unbounded_roles
        );
        assert_eq!(inventory.entry_count(), 2);
        assert_eq!(inventory.entries[0].source, Source::Builtin);
        assert_eq!(inventory.entries[1].source, Source::Plugin);
    }

    /// 三态在线上就是三个词：`measured` / `unwired` / `unread`，各带自己的字段。
    ///
    /// 前端按 `state` 分三支（印数、印破折号并说缺哪层、印破折号并说为什么读失败）。
    /// 改一个变体名在这里红，在前端不会红——那一支落空之后，读的人看到的是一根
    /// 破折号加一句 `undefined`，看上去只是这一层没接上。
    #[test]
    fn the_three_states_keep_their_names_on_the_wire() {
        let measured = serde_json::to_value(SourceReading::Measured {
            entries: 7,
            callable: 5,
        })
        .unwrap();
        assert_eq!(measured["state"], "measured");
        assert_eq!(measured["entries"], 7);
        assert_eq!(measured["callable"], 5);

        let unwired = serde_json::to_value(SourceReading::Unwired {
            because: "这个部署没有插件注册表".into(),
        })
        .unwrap();
        assert_eq!(unwired["state"], "unwired");
        assert_eq!(unwired["because"], "这个部署没有插件注册表");

        let unread = serde_json::to_value(SourceReading::Unread {
            because: "读插件清单失败：权限不足".into(),
        })
        .unwrap();
        assert_eq!(unread["state"], "unread");
        assert_eq!(unread["because"], "读插件清单失败：权限不足");
    }

    /// 同一来源里同名的合成一行，且这一层报的条数跟着变成 1。
    ///
    /// 两个插件各自声明一个 `search` 时，拼装那一步数到两条、表里只剩一行。两个数
    /// 落在同一屏上而各说一套（「插件 2」而表里一行），读者只会记住先看到的那个。
    #[test]
    fn a_name_declared_twice_by_one_layer_is_one_row_and_one_count() {
        let entry = |description: &str| Entry {
            name: "search".into(),
            source: Source::Plugin,
            reach: reach_of(Source::Plugin),
            description: Some(description.into()),
            boundary: None,
            runs_in: None,
            calls: None,
            allowed_roles: None,
            holders: Vec::new(),
        };
        let inventory = finish(
            vec![entry("第一个插件"), entry("第二个插件")],
            vec![(
                Source::Plugin,
                SourceReading::Measured {
                    entries: 2,
                    callable: 0,
                },
            )],
            Some(0),
            None, // roster_caveat
            None, // unbounded_roles
        );
        assert_eq!(inventory.entry_count(), 1);
        assert_eq!(
            inventory.sources,
            vec![(
                Source::Plugin,
                SourceReading::Measured {
                    entries: 1,
                    callable: 0,
                }
            )]
        );
        // 留下的是先读到的那条说明，不是某一轮排序恰好排在前面的那条。
        assert_eq!(
            inventory.entries[0].description.as_deref(),
            Some("第一个插件")
        );
    }

    /// 来源的名字也是线上一部分：前端按 `builtin` / `plugin` / `mcp` 给它们安中文名。
    #[test]
    fn each_source_serializes_to_the_name_the_ui_knows() {
        for (source, expected) in [
            (Source::Builtin, "builtin"),
            (Source::Plugin, "plugin"),
            (Source::Mcp, "mcp"),
        ] {
            assert_eq!(serde_json::to_value(source).unwrap(), expected);
        }
    }

    /// 「点了会不会真的跑」也要有线上的名字：前端按 `callable` / `declared_only` 分两支。
    #[test]
    fn reach_keeps_its_names_on_the_wire() {
        assert_eq!(serde_json::to_value(Reach::Callable).unwrap(), "callable");
        assert_eq!(
            serde_json::to_value(Reach::DeclaredOnly).unwrap(),
            "declared_only"
        );
    }

    /// 插件清单里声明的工具今天没有调用点：全树只有本文件读 `PluginCapability::Tool`，
    /// 没有任何代码把它登记进 `ToolRegistry`，也没有按名字把它派给 WASM 运行时。所以
    /// 它算「有名字、有参数」，不算「会做这件事」。
    ///
    /// 这条测试是那条接线的绊线：将来谁把插件工具接进执行侧，`reach_of` 必须跟着改，
    /// 在这里红一次，逼一次「清单上那一栏也一起翻过来」的决定。否则真实读数会一直说
    /// 「不能调」，而系统其实已经能调——读数比现状旧，比没有读数更糟。
    #[test]
    fn the_plugin_layer_is_declared_only_until_something_registers_it() {
        assert_eq!(reach_of(Source::Builtin), Reach::Callable);
        assert_eq!(reach_of(Source::Mcp), Reach::Callable);
        assert_eq!(reach_of(Source::Plugin), Reach::DeclaredOnly);
    }

    /// 参数边界从 schema 里读：几个参数、哪几个必填，必填名按名字排。
    ///
    /// 排序不是装饰：这一栏印在表上，两份 schema 里 `required` 的书写顺序不同时，
    /// 同一份清单每次刷新换一个顺序，读的人会以为参数变了。
    #[test]
    fn the_boundary_is_read_from_the_schema_and_sorted() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "limit": {"type": "integer"},
                "offset": {"type": "integer"}
            },
            "required": ["path", "offset"]
        });
        let boundary = boundary_of(Some(&schema)).expect("有 schema 就该读出边界");
        assert_eq!(boundary.params, 3);
        assert_eq!(boundary.required, vec!["offset", "path"]);
        assert_eq!(boundary.label(), "3 个参数，其中 2 个必填");
    }

    /// 空 schema 是「不要参数」，缺 schema 是「没读到」——两者在读数上要分得开。
    ///
    /// 一个插件只报名字不报参数时，「0 个参数」会让读的人以为这一项点了就能跑；
    /// 它真正要说的是「这一层没说清楚要什么」。
    #[test]
    fn an_empty_schema_is_no_parameters_while_a_missing_one_is_no_boundary() {
        let empty = serde_json::json!({});
        assert_eq!(
            boundary_of(Some(&empty)),
            Some(Boundary {
                required: Vec::new(),
                params: 0
            })
        );
        assert_eq!(boundary_of(Some(&empty)).unwrap().label(), "不要参数");
        assert_eq!(boundary_of(None), None);
        // 非对象的 schema（远端回了 null 或一个字符串）不是一个参数表。
        assert_eq!(boundary_of(Some(&serde_json::json!(null))), None);
    }

    /// 在哪一层跑也要有线上的名字：前端按这三个词说清失败长什么样。
    ///
    /// 改一个变体名在这里红，在前端不会红——那一支落空之后，读的人看到的是一格空
    /// 白，看上去只是这一层没有报。
    #[test]
    fn where_an_action_runs_keeps_its_name_on_the_wire() {
        for (kind, expected) in [
            (RunsIn::Native, "native"),
            (RunsIn::Wasm, "wasm"),
            (RunsIn::Shell, "shell"),
        ] {
            assert_eq!(serde_json::to_value(kind).unwrap(), expected);
        }
    }

    /// 「这一层没有计数器」与「有计数器、数是 0」在线上要分得开。
    ///
    /// 内建这一层量到 0 是真的没人点过；插件与远端那一栏是 `null`，因为没有任何
    /// 东西在数。合成一个 0 之后，读的人会去查「为什么这个工具没人用」，而实际要
    /// 修的是「这里根本没人记」。
    #[test]
    fn a_layer_without_a_counter_reports_null_rather_than_zero() {
        let uncounted = serde_json::to_value(Entry {
            name: "search".into(),
            source: Source::Plugin,
            reach: Reach::DeclaredOnly,
            description: None,
            boundary: None,
            runs_in: None,
            calls: None,
            allowed_roles: None,
            holders: Vec::new(),
        })
        .unwrap();
        assert!(uncounted["calls"].is_null());

        let counted = serde_json::to_value(Entry {
            name: "read_file".into(),
            source: Source::Builtin,
            reach: Reach::Callable,
            description: None,
            boundary: None,
            runs_in: Some(RunsIn::Native),
            calls: Some(0),
            allowed_roles: None,
            holders: Vec::new(),
        })
        .unwrap();
        assert_eq!(counted["calls"], 0);
    }

    /// 每一层的「其中几个能调」按行算，不照抄拼装时那个数。
    ///
    /// 拼装那一步只能数「声明了几项」——它看不到去重，也看不到 `reach`。两个数分开算
    /// 就会漂：一层声明三项、去重后两行、其中一行真能调，而面板上写着「3 项，3 项
    /// 能调」。这里故意喂一个错的可调数，`finish` 必须把它改对。
    #[test]
    fn the_callable_count_comes_from_the_rows_not_from_the_input() {
        let row = |name: &str, reach| Entry {
            name: name.into(),
            source: Source::Mcp,
            reach,
            description: None,
            boundary: None,
            runs_in: None,
            calls: None,
            allowed_roles: None,
            holders: Vec::new(),
        };
        let inventory = finish(
            vec![
                row("search", Reach::Callable),
                row("delete", Reach::DeclaredOnly),
                // 与第一行同源同名：去重之后这一行不该再被数一次。
                row("search", Reach::Callable),
            ],
            vec![(
                Source::Mcp,
                SourceReading::Measured {
                    entries: 3,
                    callable: 3,
                },
            )],
            Some(0),
            None, // roster_caveat
            None, // unbounded_roles
        );
        assert_eq!(inventory.entry_count(), 2);
        assert_eq!(
            inventory.sources,
            vec![(
                Source::Mcp,
                SourceReading::Measured {
                    entries: 2,
                    callable: 1,
                }
            )]
        );
    }

    fn registry_with(json: &str) -> cog_core::SkillRegistry {
        let mut registry = cog_core::SkillRegistry::new();
        registry
            .load_skills_from_json(json)
            .expect("夹具里的技能表可解析");
        registry
    }

    /// 点了名允许调它的角色按动作列开：这一栏答的是「运行时会不会放它进去」。
    ///
    /// 名册那一栏（`holders`）写的是角色名或自由文本，这一栏要的是动作名；两边
    /// 没有角色声明边界之后，每个角色都落在「不设边界」那一侧。
    ///
    /// 这一栏从分工表读而不是从名册自报读，理由没变：两处说的不是同一组词。变的是
    /// 分工表里不再有边界——工具只有一套，谁拿到的都是整张注册表。
    #[test]
    fn every_role_is_unbounded_since_no_row_declares_a_boundary() {
        let registry = registry_with(
            r#"[
                {"skill_id":"planner","name":"Planner","role_type":"planner"},
                {"skill_id":"generator","name":"Generator","role_type":"generator"}
            ]"#,
        );
        let (per_tool, unbounded) = callers_by_tool(&registry);
        assert!(
            per_tool.is_empty(),
            "有动作被摊到了某个角色头上：分工表里已经没有任何边界可以声明它"
        );
        assert_eq!(
            unbounded,
            vec!["generator".to_string(), "planner".to_string()],
            "每个角色都该落进「拿整张注册表」那一栏，而不是被读成什么都不能调"
        );
    }

    /// 同一个角色有多行时，这一栏只列它一次。
    ///
    /// 这一栏答的是「有哪些角色」，不是「有哪些行」：按行列，同一个角色会因为
    /// 表里多出一行而在读者那里变成两个人。
    #[test]
    fn a_role_with_several_rows_is_listed_once() {
        let registry = registry_with(
            r#"[
                {"skill_id":"a-generator","name":"A","role_type":"generator"},
                {"skill_id":"b-generator","name":"B","role_type":"generator"}
            ]"#,
        );
        let (_, unbounded) = callers_by_tool(&registry);
        assert_eq!(unbounded, vec!["generator".to_string()]);
    }

    /// 「分工表没读到」与「读到了、没有角色点名它」在线上要分得开。
    #[test]
    fn a_missing_role_table_reports_null_rather_than_no_caller() {
        let absent = serde_json::to_value(Entry {
            name: "read_file".into(),
            source: Source::Builtin,
            reach: Reach::Callable,
            description: None,
            boundary: None,
            runs_in: None,
            calls: Some(0),
            allowed_roles: None,
            holders: Vec::new(),
        })
        .unwrap();
        assert!(absent["allowed_roles"].is_null());

        let named_none = serde_json::to_value(Entry {
            name: "read_file".into(),
            source: Source::Builtin,
            reach: Reach::Callable,
            description: None,
            boundary: None,
            runs_in: None,
            calls: Some(0),
            allowed_roles: Some(Vec::new()),
            holders: Vec::new(),
        })
        .unwrap();
        assert_eq!(named_none["allowed_roles"], serde_json::json!([]));
    }
}
