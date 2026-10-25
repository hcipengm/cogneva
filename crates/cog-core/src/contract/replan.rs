//! 绩效越界之后由谁来动手。
//!
//! 挣值这三个量只有一处算得出来：读得到逐笔 token 账的那一侧。而能把一条新活
//! 放进任务图的是编排侧。两边各在一地，回注要么在中间加一条旁路通知（图上看不到
//! 的那条），要么把「越界了，该造一条重规划」这件事变成一个跨 crate 的交接。这里
//! 是那个交接：判定那一侧交的是一份**读数**，造任务的那一侧交的是**结局**。
//!
//! 交接只带事实，不带命令：`BreachReport` 说的是「哪个指数、量到多少、差多少」，
//! 不含判据。判据留在造任务的那一侧（它要按这一版基线去重、要按验收门下判），
//! 因为同一个越界在两种部署下该不该造活，取决于那一侧读不读得到基线——把判据抄到
//! 判定侧，两边就会各自演一遍，谁也不知道哪一遍是真的。

use async_trait::async_trait;

use crate::SFResult;

/// 绩效的两个指数。闭集，因为每一个都对应一条重规划任务要写的验收标准。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PerformanceIndex {
    /// 进度绩效 = 挣值 / 计划价值。
    Schedule,
    /// 成本绩效 = 挣值 / 有基线任务的实际成本。
    Cost,
}

impl PerformanceIndex {
    /// 全部指数。判决与测试都按它遍历，新增一项时两处一起看见。
    pub const ALL: [Self; 2] = [Self::Schedule, Self::Cost];

    /// 写进任务 id、任务输入与判决日志的稳定标识。改它等于改判据的身份：
    /// 已经落在图上的重规划行按这个名字认得出自己是给哪个指数造的。
    pub fn code(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Cost => "cost",
        }
    }

    /// 这个指数在量什么，一句给人读的话。
    pub fn what(self) -> &'static str {
        match self {
            Self::Schedule => "进度绩效（挣值 / 计划价值）",
            Self::Cost => "成本绩效（挣值 / 有基线任务的实际成本）",
        }
    }

    /// 越界的判据：低于 1.0。
    ///
    /// 1.0 是这里唯一不是编出来的线——挣值等于计划价值就是恰好按计划，等于实际
    /// 成本就是恰好花在预算上。别的数都得先替「落后多少算落后」挑一个容差，而
    /// 没有任何一项测量支持那个容差；挑出来的容差会安静地把真正的落后读成正常。
    /// 少造/多造这一类噪声由去重那一侧管（一版基线一条重规划），不靠调这条线。
    pub fn breaching(self, measured: f64) -> bool {
        measured < 1.0
    }

    /// 分母说的那一侧叫什么：进度是计划价值，成本是有基线任务的实际成本。
    pub fn denominator(self) -> &'static str {
        match self {
            Self::Schedule => "计划价值",
            Self::Cost => "有基线任务的实际成本",
        }
    }
}

/// 一处越界：哪一个指数、量到多少、差多少。
///
/// 分子分母一起交，因为「差多少」必须能折成 token 说出来：一个 0.62 的指数，
/// 差的是几万 token 还是一次调用，重规划要做的事完全不同。
#[derive(Debug, Clone, PartialEq)]
pub struct BreachReport {
    /// 挂到哪个项目根下（任务图里的 goal 根 id）。
    pub project: String,
    pub index: PerformanceIndex,
    /// 指数本身：分子分母的比值。
    pub measured: f64,
    /// 分子：挣值，单位 token。
    pub earned: u64,
    /// 分母，见 [`PerformanceIndex::denominator`]。
    pub against: u64,
}

impl BreachReport {
    /// 差多少：分母减去分子。
    ///
    /// 两个指数下它说的是同一件事——「按此刻这把尺子，差的这一截值多少 token」：
    /// 进度是还没挣到的计划价值，成本是已经超出的花销。分子比分母还大的组合
    /// 不构成越界，取零而不是回绕。
    pub fn gap_tokens(&self) -> u64 {
        self.against.saturating_sub(self.earned)
    }
}

/// 一条越界最终怎么了。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplanOutcome {
    /// 造出来了，这是它的任务 id。
    Minted { task_id: String },
    /// 这一版基线已经有过重规划，没有再造。`open` 说的是那一条还没走完——
    /// 与「造过、已经结掉」是两件事：前者在等结论，后者该看它的结论。
    AlreadyMinted { task_id: String, open: bool },
    /// 生成处拒绝。判词必须自己说清缺哪一件——一个只写「拒绝」的拒绝，谁也没法
    /// 从它推出该补什么。
    Refused { reason: String },
}

/// 一条越界的结局，带着它是给谁的。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplanMinting {
    pub project: String,
    pub index: PerformanceIndex,
    pub outcome: ReplanOutcome,
}

/// 把越界回注成任务图里的活。
///
/// 实现要求两点，都在造任务的那一侧成立：
///
/// 1. **幂等**：同一条越界重问一次不产生第二条活。判定侧每轮都会把同一处越界
///    再报一遍（读数没变），因此重复是常态而不是意外。
/// 2. **走同一套状态机**：产出是一条普通任务行，不是通知。它要能被调度、被认领、
///    有验收标准、有终态——图上多一条活，读者才知道这件事有人在管。
#[async_trait]
pub trait ReplanMinter: Send + Sync {
    /// 为这批越界逐一决定造不造、造出哪一条。一次调用只应扫一遍任务图：
    /// 判定侧一轮报上来的是一批，不是一条。
    async fn mint_replans(&self, breaches: &[BreachReport]) -> SFResult<Vec<ReplanMinting>>;
}
