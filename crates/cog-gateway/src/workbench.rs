//! `GET /api/v1/workbench` —— 工作台的四格与一条状态灯。
//!
//! 工作台答四问：花了多少（投入）、换回什么（产出）、会干什么（能力）、为谁做
//! （项目）。它们一次读、一个时刻：拆成四次请求时四份响应各有各的时刻，拼出来的
//! 东西不属于任何一瞬间，也没人说得清它读的是哪一刻。
//!
//! **布局意图写在数据里，不写在界面里。** 每格带 `priority` 与 `span`（12 列网格
//! 下宽、中、窄三档各跨几列），前端只做「格 → 网格位置」的确定性映射，不跑「找最
//! 短列」那种由客户端事后发明版式的算法——那部分版式是界面自己编的信息，数据里没有。
//!
//! **读数三态**：`measured`（量到了）/ `not_wired`（这条还没接上，写清缺哪一层）/
//! `unread`（接了但这一拍读失败，带原因）。没接上的分项一律写 `not_wired`，不印 0
//! ——0 是一个读数，会让人以为量过了。同一格里的主数可以量到、某一分项却还没接上，
//! 这时它们各自带自己的态，不互相顶替。
//!
//! 任务不单独占格：每个任务都恰好落在一个项目桶里（有 goal 的用 goal id，没有的落
//! 到 `ungrouped`），所以项目是容器、任务是明细。两格并列时印的是同一批数的两种切
//! 法，读的人会在两处看到「在跑 3」却看不出它们是同一件事。

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::error::ApiError;
use crate::observability::SpendTotals;
use crate::GatewayState;

/// 工作台上的一格。名字与前端的面一一对应，顺序不代表优先级（那是 `priority`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellId {
    Invest,
    Output,
    Capability,
    Project,
}

/// 一份读数量到了没有。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadingState {
    /// 量到了。
    Measured,
    /// 这条还没接上：`because` 说清缺的是哪一层。
    NotWired,
    /// 接了，但这一拍读失败：`because` 带原因。
    Unread,
}

/// 一格在 12 列网格里横跨几列，按三档宽度各说一次。
///
/// 三档而不是一个数：同一个格在窄屏上满宽、在宽屏上占三分之一，都是这一格自己的
/// 版式意图，不该由前端按断点反推。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Span {
    /// 最宽一档跨几列。
    pub wide: u8,
    /// 中间一档。
    pub mid: u8,
    /// 最窄一档；通常满宽。
    pub narrow: u8,
}

/// 一个数的单位。数的意义由后端说，前端只按它选词与位数。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    Count,
    Hours,
    Calls,
}

/// 一份读数：态、值、以及没量到时的那句话。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Value {
    /// 钱。两种币种是两笔钱，不是一个汇率。
    Money { totals: SpendTotals },
    /// 一个数，带它自己的单位。
    Number { n: f64, unit: Unit },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Reading {
    pub state: ReadingState,
    /// `measured` 时是量到的值；另外两态是 `None`，并配 `because`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// `not_wired` 时说缺哪一层；`unread` 时说哪一步读失败了。
    ///
    /// `measured` 时也可以有：它是这句话的限定语（例如「只数了进程内」），不是
    /// 替掉值的理由。有限定语的值与完整读数是两个读数，读者要看得见差别。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub because: Option<String>,
}

impl Reading {
    fn number(n: f64, unit: Unit) -> Self {
        Self {
            state: ReadingState::Measured,
            value: Some(Value::Number { n, unit }),
            because: None,
        }
    }

    fn money(totals: SpendTotals) -> Self {
        Self {
            state: ReadingState::Measured,
            value: Some(Value::Money { totals }),
            because: None,
        }
    }

    fn not_wired(because: impl Into<String>) -> Self {
        Self {
            state: ReadingState::NotWired,
            value: None,
            because: Some(because.into()),
        }
    }

    fn unread(because: impl Into<String>) -> Self {
        Self {
            state: ReadingState::Unread,
            value: None,
            because: Some(because.into()),
        }
    }

    /// 量到了，但这句话不完整：带上限定语。
    fn qualified(mut self, because: impl Into<String>) -> Self {
        self.because = Some(because.into());
        self
    }
}

/// 格子里的一行：一个分项读数。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Row {
    pub label: &'static str,
    pub reading: Reading,
    /// 这个数是不是「该处理了」。它只影响配色，不改变数的意义。
    pub warn: bool,
}

/// 工作台上的一格。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Cell {
    pub id: CellId,
    pub title: &'static str,
    /// 越小越靠前。顺序是数据说的，不是前端数组下标说的。
    pub priority: u8,
    pub span: Span,
    /// 点进去的那一面（前端 store 里的面 id）。
    pub href: &'static str,
    pub main: Reading,
    pub rows: Vec<Row>,
}

/// 状态灯的一段：读的是「该处理了没有」，不是规模。
///
/// 它不做格子：一出现就该被处理掉的事与「此刻规模多大」抢注意力时，读者会先读规
/// 模。灯亮不亮由数决定，不另存一份状态。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Lamp {
    pub id: &'static str,
    pub label: &'static str,
    pub reading: Reading,
    pub href: &'static str,
}

/// 工作台的全部读数。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Workbench {
    pub generated_at: DateTime<Utc>,
    /// 投入与产出对账的那个窗口（小时）。上一窗是同样长的前一段。
    pub window_hours: i64,
    pub cells: Vec<Cell>,
    pub lamps: Vec<Lamp>,
}

/// 花销账两段窗口的读数。`None` 是「这个部署没有用量账」——那不是零花销。
#[derive(Debug, Clone, Default)]
pub struct SpendWindows {
    pub current: Option<Result<cog_core::PricedSpend, String>>,
    pub previous: Option<Result<cog_core::PricedSpend, String>>,
}

/// 告警那一拍的读数：几条亮着，以及读的时候丢没丢一半。
///
/// 耐久源读失败时 `count` 只含进程内的事件，`caveat` 必须说出来：一个更短的名单
/// 读起来是「那些已经好了」，而那正是它最不能表示的意思。
#[derive(Debug, Clone)]
pub struct AlertReading {
    pub count: u64,
    pub caveat: Option<String>,
}

/// 这一拍的四格要从哪儿读。
///
/// 每个来源缺席都有它自己的意义，所以它们是 `Option` 而不是空值：没挂账、账里没
/// 东西、读账失败是三件事，折成两个会让其中一件无处可说。
pub struct Inputs<'a> {
    pub generated_at: DateTime<Utc>,
    pub window_hours: i64,
    pub spend: SpendWindows,
    /// 任务图。`None` 是这一拍没读到。
    pub tasks: Option<&'a [cog_core::Task]>,
    /// 任务计划基线；`None` 是这一拍没读到。
    pub plans: Option<&'a BTreeMap<String, cog_core::TaskPlan>>,
    /// 变更账那一段。外层 `None` 是这个部署没有变更账。
    pub changes: Option<Result<crate::progress::ChangeProgress, String>>,
    pub alerts: Option<AlertReading>,
    /// 「会做什么」那一格的清单，与 `/api/v1/capabilities` 读的是同一份。
    pub capabilities: crate::capabilities::Inventory,
    /// 「回来了什么」那一格的读数，与 `/api/v1/output` 读的是同一份。
    ///
    /// 构建跑次不再单列一格原料：它已经是这份产出的一层，两处各读一次会在同一次
    /// 请求里给出两个版本。
    pub outputs: crate::output::Outputs,
}

// ── 投入 ────────────────────────────────────────────────────────────────────

fn invest_cell(window_hours: i64, spend: &SpendWindows) -> Cell {
    let main = match &spend.current {
        None => Reading::not_wired("这个部署没有用量账（llm_usage ledger）"),
        Some(Ok(s)) => Reading::money(crate::observability::spend_totals(s)),
        Some(Err(e)) => Reading::unread(format!("读本窗花销失败：{e}")),
    };

    let previous = match &spend.previous {
        None => Reading::not_wired("这个部署没有用量账（llm_usage ledger）"),
        Some(Ok(s)) => Reading::money(crate::observability::spend_totals(s)),
        Some(Err(e)) => Reading::unread(format!("读上一窗花销失败：{e}")),
    };

    // 没算价的笔数是本窗唯一的「钱准不准」的判据：它不为零时主数是下界。
    let unpriced = match &spend.current {
        None => Reading::not_wired("这个部署没有用量账（llm_usage ledger）"),
        Some(Ok(s)) => {
            let totals = crate::observability::spend_totals(s);
            Reading::number(totals.unpriced_calls as f64, Unit::Calls)
        }
        Some(Err(e)) => Reading::unread(format!("读本窗花销失败：{e}")),
    };
    let unpriced_warn = matches!(&unpriced.value, Some(Value::Number { n, .. }) if *n > 0.0);

    Cell {
        id: CellId::Invest,
        title: "投入",
        priority: 1,
        span: Span {
            wide: 5,
            mid: 6,
            narrow: 12,
        },
        href: "token",
        main,
        rows: vec![
            Row {
                label: "上一窗",
                reading: previous,
                warn: false,
            },
            Row {
                label: "没算价的笔数",
                reading: unpriced,
                warn: unpriced_warn,
            },
            Row {
                label: "窗口",
                reading: Reading::number(window_hours as f64, Unit::Hours),
                warn: false,
            },
        ],
    }
}

// ── 产出 ────────────────────────────────────────────────────────────────────

/// 「回来了什么」这一格：数的是这一窗口结束的活、出来的产物、带提交落地的变更。
///
/// 它**不**读变更账的结算数。变更账记的是过程（谁提了什么、走到哪一步），一条变更
/// 可以走完全程而没有产出，一件产出也可以不经过变更账；把结算数当产出的主数，会把
/// 「忙了一场」读成「交出了东西」。这一格与 `/api/v1/output` 用的是同一份读数，所以
/// 点进去看到的数与格子上印的数不可能各说一套。
fn output_cell(outputs: &crate::output::Outputs) -> Cell {
    // 三态从产出面那一份读数里原样搬过来：格子与页面共用一次读，状态也就只有一份。
    let (main, with_result, failed) = match &outputs.work {
        crate::output::Reading::Measured { value } => (
            Reading::number(value.completed as f64, Unit::Count),
            Reading::number(value.with_result as f64, Unit::Count),
            Reading::number(value.failed as f64, Unit::Count),
        ),
        crate::output::Reading::Unwired { because } => {
            let one = || Reading::not_wired(because.clone());
            (one(), one(), one())
        }
        crate::output::Reading::Unread { because } => {
            let one = || Reading::unread(because.clone());
            (one(), one(), one())
        }
    };

    let builds = match &outputs.builds {
        crate::output::Reading::Measured { value } => {
            let reading = Reading::number(value.built as f64, Unit::Count);
            match &value.note {
                Some(note) => reading.qualified(note.clone()),
                None => reading,
            }
        }
        crate::output::Reading::Unwired { because } => Reading::not_wired(because.clone()),
        crate::output::Reading::Unread { because } => Reading::unread(because.clone()),
    };

    // 这一行不叫「提交」：它数的是带着提交落地的变更，同一个提交送了几条就计几次。
    // 叫「提交」会让读的人以为那是提交条数，而两个数差着一次除法的量级。
    let commits = match &outputs.commits {
        crate::output::Reading::Measured { value } => {
            Reading::number(value.delivered as f64, Unit::Count)
                .qualified(format!("最近 {} 天按 UTC 天记账", value.days))
        }
        crate::output::Reading::Unwired { because } => Reading::not_wired(because.clone()),
        crate::output::Reading::Unread { because } => Reading::unread(because.clone()),
    };

    Cell {
        id: CellId::Output,
        title: "产出",
        priority: 2,
        span: Span {
            wide: 4,
            mid: 6,
            narrow: 12,
        },
        // 产出有自己的面：落在变更面上，点进去是一张讲变更怎么走的表，与格子上
        // 「这一窗回来了多少」不是同一件事。
        href: "output",
        main,
        rows: vec![
            // 完成不等于交付：这一行是「其中交回了东西的」。两个数合成一个，读的人
            // 会以为每一条完成都带回了结果。
            Row {
                label: "带结果",
                reading: with_result,
                warn: false,
            },
            Row {
                label: "失败",
                reading: failed,
                warn: false,
            },
            Row {
                label: "构建产物",
                reading: builds,
                warn: false,
            },
            Row {
                label: "带提交落地",
                reading: commits,
                warn: false,
            },
        ],
    }
}

// ── 项目 ────────────────────────────────────────────────────────────────────

/// 项目那一格：按任务图的桶算，计划完成时间按每个桶里最晚的一条排期算。
fn project_cell(
    tasks: Option<&[cog_core::Task]>,
    plans: Option<&BTreeMap<String, cog_core::TaskPlan>>,
    now: DateTime<Utc>,
) -> Cell {
    let Some(tasks) = tasks else {
        let unread = || Reading::unread("这一拍没读到任务图");
        return Cell {
            id: CellId::Project,
            title: "项目",
            priority: 4,
            span: Span {
                wide: 12,
                mid: 12,
                narrow: 12,
            },
            href: "projects",
            main: unread(),
            rows: ["推进中", "越过计划完成时间", "排队", "在跑", "失败"]
                .iter()
                .map(|label| Row {
                    label,
                    reading: unread(),
                    warn: false,
                })
                .collect(),
        };
    };

    // 每个任务恰好落在一个桶里，所以桶的个数就是项目数——含 `ungrouped`：那份
    // 没归属的活也是活，把它漏掉会让桶加起来不等于总数。
    let mut keys: BTreeSet<&str> = BTreeSet::new();
    let mut advancing: BTreeSet<&str> = BTreeSet::new();
    let mut queued = 0u64;
    let mut running = 0u64;
    let mut failed = 0u64;
    for task in tasks {
        let key = task.project_key();
        keys.insert(key);
        match task.status {
            cog_core::TaskStatus::Pending | cog_core::TaskStatus::Scheduled => queued += 1,
            cog_core::TaskStatus::Running => running += 1,
            cog_core::TaskStatus::Failed => failed += 1,
            cog_core::TaskStatus::Completed | cog_core::TaskStatus::Cancelled => {}
        }
        if !task.status.is_terminal() {
            advancing.insert(key);
        }
    }

    let overdue = match plans {
        None => Reading::unread("这一拍没读到排期基线"),
        Some(plans) => {
            let mut any_plan = false;
            let mut past_due: BTreeSet<&str> = BTreeSet::new();
            let mut latest_end: BTreeMap<&str, DateTime<Utc>> = BTreeMap::new();
            for task in tasks {
                let Some(plan) = plans.get(&task.id) else {
                    continue;
                };
                let Some(end) = plan.planned_end else {
                    continue;
                };
                any_plan = true;
                let key = task.project_key();
                latest_end
                    .entry(key)
                    .and_modify(|held| *held = (*held).max(end))
                    .or_insert(end);
            }
            for (key, end) in latest_end {
                if end < now && advancing.contains(key) {
                    past_due.insert(key);
                }
            }
            if any_plan {
                Reading::number(past_due.len() as f64, Unit::Count)
            } else {
                // 一条计划完成时间都没有：这个数算不出来，而不是「没有延期」。
                Reading::not_wired("任务图上没有任何一条计划完成时间")
            }
        }
    };

    Cell {
        id: CellId::Project,
        title: "项目",
        priority: 4,
        span: Span {
            wide: 12,
            mid: 12,
            narrow: 12,
        },
        href: "projects",
        main: Reading::number(keys.len() as f64, Unit::Count),
        rows: vec![
            Row {
                label: "推进中",
                reading: Reading::number(advancing.len() as f64, Unit::Count),
                warn: false,
            },
            Row {
                label: "越过计划完成时间",
                reading: overdue,
                warn: false,
            },
            Row {
                label: "排队",
                reading: Reading::number(queued as f64, Unit::Count),
                warn: false,
            },
            Row {
                label: "在跑",
                reading: Reading::number(running as f64, Unit::Count),
                warn: false,
            },
            Row {
                label: "失败",
                reading: Reading::number(failed as f64, Unit::Count),
                warn: failed > 0,
            },
        ],
    }
}

// ── 能力 ────────────────────────────────────────────────────────────────────

/// 「会做什么」这一格：数的是**动作**，不是装了几个插件。
///
/// 以前这里数的是插件数，主数读起来像「这个系统会这么多件事」，而一件工具都没声明
/// 的插件也算了一件。现在它读的是能力清单本身（与 `/api/v1/capabilities` 同一份）：
/// 主数是「能被点单的动作有几个」，分项按来源拆开——一个来源读不到时，缺的是哪一层
/// 在分项上说，不让主数被别的来源撑住而看不出缺了一层。
fn capability_cell(inventory: &crate::capabilities::Inventory) -> Cell {
    use crate::capabilities::Source;

    let reading_of = |source: Source| -> Reading {
        match inventory
            .sources
            .iter()
            .find(|(s, _)| *s == source)
            .map(|(_, r)| r)
        {
            None => Reading::not_wired("这一层没有报读数"),
            Some(crate::capabilities::SourceReading::Measured { entries, callable }) => {
                // 印能调的那几个：这一格答「会做什么」。声明了没人接的名字另附一句，
                // 否则「插件 3」会被读成三件事都能做——那正是这一格以前犯的错。
                let reading = Reading::number(*callable as f64, Unit::Count);
                if callable == entries {
                    reading
                } else {
                    reading.qualified(format!(
                        "这一层声明了 {entries} 项，其中 {callable} 项有调用点"
                    ))
                }
            }
            Some(crate::capabilities::SourceReading::Unwired { because }) => {
                Reading::not_wired(because.clone())
            }
            Some(crate::capabilities::SourceReading::Unread { because }) => {
                Reading::unread(because.clone())
            }
        }
    };

    // 一层都没报出读数时不印 0：0 是一个读数（「查过了，一件都不会」），而这时真话
    // 是「没量过」。主数由各层撑起来，所以它必须和各层一起说这句话。
    let main = if inventory
        .sources
        .iter()
        .any(|(_, r)| matches!(r, crate::capabilities::SourceReading::Measured { .. }))
    {
        Reading::number(inventory.callable_count() as f64, Unit::Count)
    } else {
        Reading::not_wired("三个来源都没有报出读数")
    };

    Cell {
        id: CellId::Capability,
        title: "能力",
        priority: 3,
        span: Span {
            wide: 3,
            mid: 6,
            narrow: 12,
        },
        // 这一格的数来自能力清单，就该落在能力那一面；落在治理面上，点进去看到的
        // 是一张讲责任人的表，与点它时看到的那几个数毫无关系。
        href: "capabilities",
        main,
        rows: vec![
            Row {
                label: "内建",
                reading: reading_of(Source::Builtin),
                warn: false,
            },
            Row {
                label: "插件",
                reading: reading_of(Source::Plugin),
                warn: false,
            },
            Row {
                label: "MCP",
                reading: reading_of(Source::Mcp),
                warn: false,
            },
            Row {
                label: "使用次数",
                reading: Reading::not_wired("能力还没有使用计数：工具跑完不写任何读数"),
                warn: false,
            },
        ],
    }
}

// ── 状态灯 ──────────────────────────────────────────────────────────────────

fn lamps(
    changes: Option<&Result<crate::progress::ChangeProgress, String>>,
    alerts: Option<&AlertReading>,
) -> Vec<Lamp> {
    let alert_reading = match alerts {
        None => Reading::not_wired("这个部署没有告警源"),
        Some(a) => {
            let reading = Reading::number(a.count as f64, Unit::Count);
            match &a.caveat {
                Some(caveat) => reading.qualified(caveat.clone()),
                None => reading,
            }
        }
    };

    let change_reading = match changes {
        None => Reading::not_wired("这个部署没有变更账"),
        Some(Err(e)) => Reading::unread(format!("读变更账失败：{e}")),
        Some(Ok(p)) => Reading::number(p.outstanding as f64, Unit::Count),
    };

    vec![
        Lamp {
            id: "alerts",
            label: "告警",
            reading: alert_reading,
            href: "alerts",
        },
        Lamp {
            id: "changes",
            label: "扣着主线",
            reading: change_reading,
            href: "changes",
        },
        // 「版本未滚」要读部署版本与交付版本各是哪一版，今天没有这一份读数。
        //
        // `href` 必须是左栏真有的那一面：写 `deploy` 时点进去是「没有叫 deploy
        // 的这一页」——构建与部署在界面上叫 `delivery`。
        Lamp {
            id: "rollout",
            label: "版本未滚",
            reading: Reading::not_wired("版本滚动读数还没接上"),
            href: "delivery",
        },
    ]
}

/// 四格与一条灯，按 `priority` 排好。
pub fn build(input: Inputs<'_>) -> Workbench {
    let Inputs {
        generated_at,
        window_hours,
        spend,
        tasks,
        plans,
        changes,
        alerts,
        capabilities,
        outputs,
    } = input;
    let mut cells = vec![
        invest_cell(window_hours, &spend),
        output_cell(&outputs),
        capability_cell(&capabilities),
        project_cell(tasks, plans, generated_at),
    ];
    cells.sort_by_key(|c| c.priority);
    Workbench {
        generated_at,
        window_hours,
        lamps: lamps(changes.as_ref(), alerts.as_ref()),
        cells,
    }
}

// ── 这一拍从部署里读 ─────────────────────────────────────────────────────────

/// 这一拍的四格。
pub async fn read(state: &GatewayState) -> Workbench {
    let now = Utc::now();
    let window_hours = crate::observability::spend_window_hours();

    let spend = match state.llm_usage.as_ref() {
        None => SpendWindows::default(),
        Some(ledger) => {
            // 两段等长的相邻窗口。上一窗的终点就是本窗的起点：中间不留缝，也不
            // 重叠——留缝时两段加起来不等于一整段，重叠时同一笔钱被数两次。
            let current_from = now - chrono::Duration::hours(window_hours);
            let previous_from = current_from - chrono::Duration::hours(window_hours);
            let (current, previous) = tokio::join!(
                ledger.priced_spend_between(current_from, now),
                ledger.priced_spend_between(previous_from, current_from),
            );
            SpendWindows {
                current: Some(current.map_err(|e| e.to_string())),
                previous: Some(previous.map_err(|e| e.to_string())),
            }
        }
    };

    let tasks = crate::progress::merged_tasks(state).await.ok();
    let plans = crate::projects::readable_plans(&state.orchestrator).await;

    let changes = match state.change_ledger.as_ref() {
        None => None,
        Some(ledger) => Some(
            ledger
                .census()
                .await
                .map(|census| crate::progress::change_progress(&census))
                .map_err(|e| e.to_string()),
        ),
    };

    let alerts = crate::read_active_alerts(state)
        .await
        .map(|(rows, durable)| AlertReading {
            count: rows.len() as u64,
            caveat: match durable {
                Some(false) => Some("耐久源读失败，只数了进程内".to_string()),
                _ => None,
            },
        });

    // 插件清单不再单独读一遍：能力清单（`crate::capabilities::read`）已经把三个
    // 来源一起读了，两处各读一次会让同一份磁盘在同一次请求里给出两个版本。
    let capabilities = crate::capabilities::read(state).await;

    let builds = match state.evolution_admin.as_ref() {
        None => None,
        Some(admin) => Some(
            admin
                .build_runs(crate::evolution::BUILD_RUN_ROWS_ASKED)
                .await
                .map_err(|e| e.to_string()),
        ),
    };

    // 漏斗与产出面读的是同一栏，天数也从同一个函数取：读的是一周、格子上标的是三天
    // 时，两个数覆盖的范围不一样，而两边都不会报错。
    let funnel = match state.change_ledger.as_ref() {
        None => None,
        Some(ledger) => Some(
            ledger
                .funnel(crate::output::funnel_days(window_hours))
                .await,
        ),
    };

    // 产出那一格与产出面共用这一次读：任务图与构建跑次各读两遍，会在同一次请求里
    // 给出两个版本，而格子与页面就在同一屏上。
    let outputs = crate::output::build(crate::output::Inputs {
        now,
        window_hours,
        tasks: tasks.as_deref(),
        builds: builds.as_ref(),
        funnel: funnel.as_ref(),
    });

    build(Inputs {
        generated_at: now,
        window_hours,
        spend,
        tasks: tasks.as_deref(),
        plans: plans.as_ref(),
        changes,
        alerts,
        capabilities,
        outputs,
    })
}

pub async fn workbench_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Workbench>, ApiError> {
    Ok(Json(read(&state).await))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Task, TaskStatus, TaskType};

    fn task(id: &str, goal: Option<&str>, status: TaskStatus) -> Task {
        let mut task = Task::new(
            id,
            TaskType::Custom("platform_agent_task".into()),
            serde_json::json!({}),
        );
        task.goal_id = goal.map(str::to_string);
        task.status = status;
        task
    }

    fn plan(task_id: &str, end: DateTime<Utc>) -> cog_core::TaskPlan {
        cog_core::TaskPlan {
            workspace_id: "ws".into(),
            task_id: task_id.into(),
            planned_start: Some(end - chrono::Duration::hours(1)),
            planned_end: Some(end),
            budget_tokens: 0,
            baseline_rev: "rev-1".into(),
            updated_at: end,
        }
    }

    fn spend_of(rows: Vec<cog_core::PricedSpendRow>) -> cog_core::PricedSpend {
        cog_core::PricedSpend {
            covered_since: None,
            covered_until: None,
            rows,
        }
    }

    fn spend_row(
        price_book: Option<&str>,
        calls: i64,
        priced: i64,
        micros: Option<i64>,
    ) -> cog_core::PricedSpendRow {
        cog_core::PricedSpendRow {
            actor: "agent:1".into(),
            origin: "http".into(),
            upstream: "main".into(),
            api_style: "anthropic".into(),
            price_book: price_book.map(str::to_string),
            calls,
            failed_calls: 0,
            tokens_input: 10,
            tokens_cached: 0,
            tokens_output: 5,
            cost_micros: micros,
            priced_calls: priced,
        }
    }

    fn empty_inputs(now: DateTime<Utc>) -> Inputs<'static> {
        Inputs {
            generated_at: now,
            window_hours: 24,
            spend: SpendWindows::default(),
            tasks: None,
            plans: None,
            changes: None,
            alerts: None,
            capabilities: Default::default(),
            outputs: Default::default(),
        }
    }

    fn cell(bench: &Workbench, id: CellId) -> &Cell {
        bench
            .cells
            .iter()
            .find(|c| c.id == id)
            .unwrap_or_else(|| panic!("工作台上没有 {id:?} 这一格"))
    }

    fn number_of(reading: &Reading) -> Option<f64> {
        match &reading.value {
            Some(Value::Number { n, .. }) => Some(*n),
            _ => None,
        }
    }

    /// 四格就是全部：任务不是其中一格，它是项目格里的明细。这条守住「任务与项目
    /// 各占一格」那种又冒出第五格的做法——两格并列时印的是同一批数的两种切法。
    #[test]
    fn the_workbench_is_four_cells_and_a_task_is_not_one_of_them() {
        let now = Utc::now();
        let tasks = vec![
            task("t1", Some("goal-a"), TaskStatus::Running),
            task("t2", Some("goal-a"), TaskStatus::Pending),
            task("t3", None, TaskStatus::Failed),
        ];
        let bench = build(Inputs {
            tasks: Some(&tasks),
            ..empty_inputs(now)
        });

        let ids: Vec<CellId> = bench.cells.iter().map(|c| c.id).collect();
        assert_eq!(
            ids,
            vec![
                CellId::Invest,
                CellId::Output,
                CellId::Capability,
                CellId::Project
            ],
            "四格的顺序由 priority 说，不由数组下标说"
        );
        assert!(
            !bench.cells.iter().any(|c| c.title == "任务"),
            "任务不单独占格：它是项目容器里的明细"
        );
        // 版式意图写在数据里：前三格在最宽一档合计正好是网格的 12 列。
        let wide: u8 = bench
            .cells
            .iter()
            .filter(|c| c.id != CellId::Project)
            .map(|c| c.span.wide)
            .sum();
        assert_eq!(wide, 12);
        assert_eq!(cell(&bench, CellId::Project).span.wide, 12);
    }

    /// 没接上的分项写「缺哪一层」，不印 0。0 是一个读数，会让人以为量过了。
    #[test]
    fn a_reading_that_is_not_wired_says_what_is_missing_instead_of_reading_zero() {
        let bench = build(empty_inputs(Utc::now()));
        for id in [CellId::Invest, CellId::Output, CellId::Capability] {
            let readings = std::iter::once(&cell(&bench, id).main).chain(
                cell(&bench, id)
                    .rows
                    .iter()
                    // 窗口不在这条判据里：它是窗口期的常量，不依赖任何数据源（下面单独判它）。
                    .filter(|r| r.label != "窗口")
                    .map(|r| &r.reading),
            );
            for reading in readings {
                assert_ne!(
                    reading.state,
                    ReadingState::Measured,
                    "{id:?} 在没有数据源时不该量到一个数"
                );
                assert!(
                    reading.value.is_none(),
                    "{id:?} 没量到就不许带值——带 0 会被读成「查过了，是零」"
                );
                assert!(
                    reading
                        .because
                        .as_deref()
                        .is_some_and(|why| !why.is_empty()),
                    "{id:?} 没量到必须说清缺的是哪一层"
                );
            }
        }
        // 窗口是常量，不依赖任何数据源，所以它照常量得到。
        let window = cell(&bench, CellId::Invest)
            .rows
            .iter()
            .find(|r| r.label == "窗口")
            .expect("投入格必须有窗口这一行");
        assert_eq!(number_of(&window.reading), Some(24.0));
    }

    /// 项目是容器：主数是桶的个数（含 `ungrouped`），明细是桶里的活按状态摊开。
    #[test]
    fn the_project_cell_counts_the_container_and_the_work_inside_it() {
        let now = Utc::now();
        let tasks = vec![
            task("t1", Some("goal-a"), TaskStatus::Running),
            task("t2", Some("goal-a"), TaskStatus::Pending),
            task("t3", None, TaskStatus::Failed),
            task("t4", Some("goal-b"), TaskStatus::Completed),
        ];
        let bench = build(Inputs {
            tasks: Some(&tasks),
            ..empty_inputs(now)
        });
        let project = cell(&bench, CellId::Project);
        assert_eq!(
            number_of(&project.main),
            Some(3.0),
            "goal-a、goal-b、ungrouped 三个桶"
        );
        let row = |label: &str| {
            project
                .rows
                .iter()
                .find(|r| r.label == label)
                .unwrap_or_else(|| panic!("项目格缺 {label} 这一行"))
        };
        // 只有 goal-a 还有没走完的活在推进。ungrouped 那一桶只剩一条失败的活——失败是
        // 历史，不是还站着的活，所以它不进「推进中」，只在「失败」那一行里出现。
        assert_eq!(number_of(&row("推进中").reading), Some(1.0));
        assert_eq!(number_of(&row("排队").reading), Some(1.0));
        assert_eq!(number_of(&row("在跑").reading), Some(1.0));
        assert_eq!(number_of(&row("失败").reading), Some(1.0));
        assert!(row("失败").warn, "有失败就该亮出来");
    }

    /// 越过计划完成时间只数「还开着」的桶：已经交完的项目迟到过，不等于现在延期。
    #[test]
    fn a_project_past_its_planned_end_counts_only_while_it_still_has_open_work() {
        let now = Utc::now();
        let tasks = vec![
            task("t1", Some("goal-late"), TaskStatus::Running),
            task("t2", Some("goal-done"), TaskStatus::Completed),
        ];
        let mut plans = BTreeMap::new();
        plans.insert(
            "t1".to_string(),
            plan("t1", now - chrono::Duration::days(1)),
        );
        plans.insert(
            "t2".to_string(),
            plan("t2", now - chrono::Duration::days(1)),
        );
        let bench = build(Inputs {
            tasks: Some(&tasks),
            plans: Some(&plans),
            ..empty_inputs(now)
        });
        let overdue = cell(&bench, CellId::Project)
            .rows
            .iter()
            .find(|r| r.label == "越过计划完成时间")
            .unwrap();
        assert_eq!(number_of(&overdue.reading), Some(1.0));
    }

    /// 一条排期都没有时，延期是「算不出来」而不是「没有延期」。
    #[test]
    fn no_schedule_at_all_is_not_wired_rather_than_zero_overdue() {
        let now = Utc::now();
        let tasks = vec![task("t1", Some("goal-a"), TaskStatus::Running)];
        let plans = BTreeMap::new();
        let bench = build(Inputs {
            tasks: Some(&tasks),
            plans: Some(&plans),
            ..empty_inputs(now)
        });
        let overdue = cell(&bench, CellId::Project)
            .rows
            .iter()
            .find(|r| r.label == "越过计划完成时间")
            .unwrap();
        assert_eq!(overdue.reading.state, ReadingState::NotWired);
        assert!(overdue.reading.value.is_none());
    }

    /// 钱按币种分开，没算价的笔数单独成行并且在大于零时亮出来：主数是下界时，
    /// 读的人要当场看见它是下界。
    #[test]
    fn money_is_per_currency_and_unpriced_calls_are_flagged_beside_it() {
        let now = Utc::now();
        let spend = SpendWindows {
            current: Some(Ok(spend_of(vec![
                spend_row(Some("CNY@2026-11-01"), 3, 3, Some(1_500_000)),
                spend_row(Some("USD@2026-11-01"), 2, 2, Some(800_000)),
                spend_row(None, 4, 0, None),
            ]))),
            previous: Some(Ok(spend_of(vec![spend_row(
                Some("CNY@2026-11-01"),
                1,
                1,
                Some(500_000),
            )]))),
        };
        let bench = build(Inputs {
            spend,
            ..empty_inputs(now)
        });
        let invest = cell(&bench, CellId::Invest);
        match &invest.main.value {
            Some(Value::Money { totals }) => {
                assert_eq!(
                    totals.by_currency.len(),
                    2,
                    "两种币种是两笔钱，不是一个汇率"
                );
                assert_eq!(totals.unpriced_calls, 4);
            }
            other => panic!("投入格的主数应当是钱：{other:?}"),
        }
        let unpriced = invest
            .rows
            .iter()
            .find(|r| r.label == "没算价的笔数")
            .unwrap();
        assert_eq!(number_of(&unpriced.reading), Some(4.0));
        assert!(unpriced.warn);
        let previous = invest.rows.iter().find(|r| r.label == "上一窗").unwrap();
        assert!(matches!(previous.reading.value, Some(Value::Money { .. })));
    }
}
