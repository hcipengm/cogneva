//! 「这张表保留多少天」这个旋钮怎么读。
//!
//! 三张归档表各有一个这样的旋钮，各读自己的环境变量：
//! `COGNEVA_TASK_EVENT_KEEP_DAYS`（任务事件流）、`COGNEVA_LLM_EXCHANGE_KEEP_DAYS`
//! （提示与回包）、`COGNEVA_LLM_USAGE_KEEP_DAYS`（用量账）。三份解析一字不差，
//! 只有**读到 0** 那一条分支不同——而那条分支恰恰是它们唯一真正需要各自表态的
//! 地方。抄写不携带表态：谁把哪一句改成另一句，编译不失败、测试不失败，那个旋钮
//! 就从「关掉清扫」变成「按默认三十天清扫」或者反过来，而且没有任何读数会说出这
//! 件事发生过。
//!
//! 所以读的规矩只有一份（[`keep_days_or_default`] 与 [`keep_days_or_disable`]
//! 共用下面那条解析），**0 算什么意思**变成选哪个函数——写在调用点上，一个词，
//! 不需要读者去比对两段几乎相同的代码。两种含义今天都在树上，各有各的理由：
//!
//! - 落回默认（[`keep_days_or_default`]）：0 视为写坏了。保留期是零天意味着每一趟
//!   都把表清空，而「一张被清空的表」与「保留策略正常工作」在读数上同形——那正是
//!   这个旋钮要防的事，不该由它自己制造。
//! - 关掉清扫（[`keep_days_or_disable`]）：0 是显式关掉这一趟清扫。读者从偏移零
//!   开始回放整条流，删掉前缀就答成一个看起来像「这次运行刚开始」的缺口，于是这个
//!   旋钮宁可让人明说「不扫」，也不接受一个近似零的天数。
//!
//! 这两句对同一个输入给出相反的处置，本模块不替任何一方裁决；它保证的是这件事被
//! 写成一个写在调用点上的词，而不是一次抄写留下的巧合。要统一含义，改的是那张登记
//! 表（`a_retention_knob_says_what_zero_means_test`）与被登记的那一处，不是这里的
//! 读法。
//!
//! 两条共用的规矩：两头空白剥掉；读不出数字、或低于 `min_days`（负数都在内）都落
//! 回 `default_days`。旋钮写坏了不该把一张表清空。

/// 用量账的保留期旋钮：变量名与它那两个有意义的数。
///
/// 放在这里而不是跟着清扫循环走，是因为**读的那一侧也要它**。清扫循环在
/// `cog-observability` 里，读口在 `cog-gateway` 里，两者互不依赖；旋钮只在清扫
/// 那一侧声明时，读口那边只剩两条路——抄一个数字，或者抄一个上限常量。抄数字会
/// 在旋钮被改的那天静默撒谎；抄上限常量的情形今天就在树上：账本按旋钮留三十天，
/// 接口按一个写死的七天截止，于是二十三天一直在表里、从接口读不到，而两条读数
/// 都看起来正常。
///
/// 旋钮的意思（读到 0 落回默认）由 [`llm_usage_keep_days_from`] 定，与清扫循环
/// 用的是同一个 [`keep_days_or_default`]，所以「清扫留几天」与「读口能覆盖几天」
/// 不是两个承诺。
pub const LLM_USAGE_KEEP_DAYS_ENV: &str = "COGNEVA_LLM_USAGE_KEEP_DAYS";

/// 用量账的保留天数：三十天，与它旁边那本提示归档的承诺一致。
// cap: bound — 与旁边那本提示归档同一个承诺，不是从某次读数推出来的
pub const LLM_USAGE_KEEP_DAYS_DEFAULT: i64 = 30;

/// 用量账最短的保留期：一天。更短会在每一趟里把当前小时折赖以计算的行走删掉。
// cap: bound — 再短就会删掉当前小时那次折赖以计算的行走；没有测量能把它调小
pub const LLM_USAGE_KEEP_DAYS_MIN: i64 = 1;

/// 用量账的保留天数，按 [`LLM_USAGE_KEEP_DAYS_ENV`] 读。
///
/// 返回值永远是一个生效的天数：读不出来、低于下限、读到 0 都落回默认。读口因此
/// 不必处理「没有保留期」这一支，「没有保留期」在账本上本来也不是一个状态。
pub fn llm_usage_keep_days() -> i64 {
    let published = EFFECTIVE_LLM_USAGE_KEEP_DAYS.load(std::sync::atomic::Ordering::Relaxed);
    if published > 0 {
        return published;
    }
    llm_usage_keep_days_seed()
}

/// 本进程最近一拍清扫算出的保留天数。
///
/// 零是「还没有一拍算过」，不是「保留零天」；那时读的是部署声明的 seed。清扫
/// 循环每拍把实测值写进来，读口（花销账的追溯上限）读到的就是同一句话——两条
/// 各说各的，正是「账本留三十天、接口只覆盖七天」那种静默漂移的来源。
static EFFECTIVE_LLM_USAGE_KEEP_DAYS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

/// 清扫循环把这一拍算出的保留天数写进进程，读口下一次调用就跟着它走。
pub fn publish_llm_usage_keep_days(days: i64) {
    EFFECTIVE_LLM_USAGE_KEEP_DAYS.store(days.max(0), std::sync::atomic::Ordering::Relaxed);
}

/// 部署声明的保留天数：清扫循环的 seed。
///
/// 循环读的是这一个，不是 [`llm_usage_keep_days`]：后者会把它自己上一拍的输出
/// 当成下一拍的输入，回路就变成了自己的回音。
pub fn llm_usage_keep_days_seed() -> i64 {
    llm_usage_keep_days_from(std::env::var(LLM_USAGE_KEEP_DAYS_ENV).ok().as_deref())
}

/// [`llm_usage_keep_days`] 对一个显式读数，好让这条规矩不靠进程环境就能测。
pub fn llm_usage_keep_days_from(raw: Option<&str>) -> i64 {
    keep_days_or_default(raw, LLM_USAGE_KEEP_DAYS_DEFAULT, LLM_USAGE_KEEP_DAYS_MIN)
}

/// 把原始读数读成一个数：剥空白，读不出来就是 `None`。
///
/// 这一条是两条策略唯一的共同底座；`0` 在这里还是 `Some(0)`，怎么处置由上面那两
/// 个函数各自决定。
fn read_days(raw: Option<&str>) -> Option<i64> {
    raw.and_then(|v| v.trim().parse::<i64>().ok())
}

/// 一个保留天数旋钮：读不出、低于下限、或者读到 0，都落回默认值。
///
/// 给「0 是写坏了」的那一类旋钮：返回值永远是一个生效的保留天数，调用方不必处理
/// 「没有天数」这一支。
///
/// 0 由**这里**挡住，不由 `min_days` 挡：`days > 0` 写在闸上，所以「0 落回默认」
/// 是这个函数的承诺，而不是它与调用点传进来的下限之间的默契。一个允许「极小保留
/// 期」的旋钮很自然会传 `min_days = 0`，那时若靠下限挡零，`Some(0)` 会原样通过，
/// 于是每一趟清空这张表——正是这个函数名字里说要防的那件事，而编译与它自己的测试
/// 都不会响。
pub fn keep_days_or_default(raw: Option<&str>, default_days: i64, min_days: i64) -> i64 {
    match read_days(raw) {
        Some(days) if days > 0 && days >= min_days => days,
        _ => default_days,
    }
}

/// 一个保留天数旋钮：读到 0 是显式关掉，返回 `None`；其余同上。
///
/// 给「不扫」本身是一个正当状态的那一类旋钮。关掉之后读数照发——这张表现在有
/// 多大、最老一行多旧，仍然答得出来，所以「关掉」不等于「不知道」。
///
/// 正数与下限的关系同上：`days > 0` 在这一层，负数与零都不会走到「按天数清扫」
/// 那一支。
pub fn keep_days_or_disable(raw: Option<&str>, default_days: i64, min_days: i64) -> Option<i64> {
    match read_days(raw) {
        Some(0) => None,
        Some(days) if days > 0 && days >= min_days => Some(days),
        _ => Some(default_days),
    }
}

/// 数据库卷的声明大小，由部署声明、由这一侧读成字节。
///
/// 保留清扫要回答的不是「表够不够大」，而是「这张表能占这张卷的多少」。没有卷
/// 的大小，每行字节测得再准也换不出一个行数；反过来，每行字节不量，卷再大也只
/// 能换成一个与此刻的数据无关的常数。
///
/// 卷的大小只在**改卷**的时候变：要么重装，要么容量回路把声明抬上去——两者都
/// 是一条重启之外的路径。所以它是回路的结构输入（读入一次、跟着重启更新），而
/// 回路每拍重算的是「这个预算买得到多少行／多少天」，用的是当拍的实测值。
pub const DATABASE_STORAGE_ENV: &str = "COGNEVA_DATABASE_STORAGE_BYTES";

/// 一张受保留回路管理的表最多占数据库卷的百分之几。
///
/// 一张卷上坐着不止一本账：检查点、可观测事件、任务事件、提示归档、用量账各
/// 自清扫。每张表都按「整张卷」算预算，它们合起来就会把卷填满；份额是它们之间
/// 的分配约定，只能由部署来定，测不出来。回路继承这个数，每拍用它乘出预算、再
/// 除以当拍实测的每行字节／日写入量。
// cap: bound — 多张表共用一张卷，份额是分配约定而不是测出来的尺寸；改它是一次产品决策
pub const RETENTION_TABLE_SHARE_PERCENT: u64 = 5;

/// 把部署声明的卷大小读成字节；读不出来就是 `None`。
///
/// `"10Gi"` 这类写法与卷声明、资源量同一套数量词，解析器只有一份
/// （[`cog_core::claim_footprint::quantity_bytes`]）：两边各写一份，改了一边之后
/// 另一边的读数会静默地差一个数量级。
pub fn database_storage_bytes(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|text| crate::claim_footprint::quantity_bytes(text))
        .filter(|bytes| bytes.is_finite() && *bytes >= 1.0)
        .map(|bytes| bytes as u64)
}

/// 一张表在这张卷上能用的字节预算。
pub fn table_budget_bytes(volume_bytes: u64) -> u64 {
    volume_bytes.saturating_mul(RETENTION_TABLE_SHARE_PERCENT) / 100
}

/// 一个字节预算，按这一拍实测的「持有件数与其字节数」，装得下多少件。
///
/// 这是「容量换件数」那一条律本身：预算 ÷ (实测字节 ÷ 实测件数) = 容量。调用方
/// 只决定预算取哪张卷的多少（数据库表是卷的一份份额，整张卷归一种东西时就是整张
/// 卷），不重写这条除法。件成本与持有量都量得到时容量由实测决定（两个方向都跟：
/// 单件变大容量就小，单件变小容量就大）；量不到时落回 seed。seed 为 0 表示这个
/// 部署关掉了这一趟回收，实测不会把它重新打开。返回目标与它这一拍的来源标签。
pub fn budget_against_measured_items(
    seed: i64,
    budget_bytes: Option<u64>,
    held: i64,
    bytes: Option<u64>,
) -> (i64, &'static str) {
    if seed <= 0 {
        return (0, SOURCE_SEED);
    }
    match (budget_bytes, bytes) {
        (Some(budget), Some(bytes)) if held > 0 && bytes > 0 => {
            let per_item = bytes as f64 / held as f64;
            let items = rows_that_fit(budget, per_item).min(i64::MAX as u64);
            ((items as i64).max(1), SOURCE_DERIVED)
        }
        _ => (seed, SOURCE_SEED),
    }
}

/// 一张表的行容量：这一拍实测的持有行数与表字节，加上部署声明的卷大小。
///
/// 预算取的是表在卷上的那份份额（[`table_budget_bytes`]），除法本身与别的回收
/// 回路共用 [`budget_against_measured_items`]。
pub fn row_budget_against_volume(
    seed: i64,
    volume_bytes: Option<u64>,
    held: i64,
    bytes: Option<u64>,
) -> (i64, &'static str) {
    budget_against_measured_items(seed, volume_bytes.map(table_budget_bytes), held, bytes)
}

/// 一张归档表的保留天数：这一拍实测的表字节与最老一行年龄，加上声明的卷大小。
///
/// 表自上次清扫以来留下的字节数除以它覆盖的天数，就是一天的成本；卷的份额除以
/// 这个成本，就是预算买得到的天数。量不到（没有卷、表还空、体积读不出）时落回
/// seed。返回天数与「是否来自实测」。
pub fn keep_days_against_volume(
    seed_days: i64,
    volume_bytes: Option<u64>,
    oldest_age_secs: i64,
    bytes: Option<u64>,
) -> (i64, bool) {
    let Some(volume) = volume_bytes else {
        return (seed_days, false);
    };
    match bytes {
        Some(bytes) if oldest_age_secs > 0 && bytes > 0 => {
            let bytes_per_day = bytes as f64 / oldest_age_secs as f64 * 86_400.0;
            let days = days_that_fit(table_budget_bytes(volume), bytes_per_day);
            (days.min(i64::MAX as u64) as i64, true)
        }
        _ => (seed_days, false),
    }
}

/// 保留回路的读数面：每条回路这一拍算出的目标、它量的单位、它的输入是不是实测。
pub const TARGET_METRIC: &str = "cogneva_retention_target";

/// 每条回路算过几拍。输入不变时目标可以不变，这一条不能不变。
pub const EVALUATIONS_METRIC: &str = "cogneva_retention_evaluations_total";

/// 读数上点名回路的那一维；取值就是登记表里的 key。
pub const KNOB_LABEL: &str = "knob";

/// 目标值的量纲：`rows`、`days`、`items`、`seconds`。
pub const UNIT_LABEL: &str = "unit";

/// 这一拍的目标是实测算出来的（`derived`）还是还没量到、仍用部署声明的值（`seed`）。
pub const SOURCE_LABEL: &str = "source";
/// 见 [`SOURCE_LABEL`]。
pub const SOURCE_SEED: &str = "seed";
/// 见 [`SOURCE_LABEL`]。
pub const SOURCE_DERIVED: &str = "derived";

/// 目标以行为单位。
pub const UNIT_ROWS: &str = "rows";
/// 目标以天为单位。
pub const UNIT_DAYS: &str = "days";
/// 目标以份数为单位。
pub const UNIT_ITEMS: &str = "items";
/// 目标以秒为单位。
pub const UNIT_SECONDS: &str = "seconds";

#[derive(Clone, Debug)]
struct Reading {
    target: f64,
    evaluations: u64,
    unit: &'static str,
    derived: bool,
}

/// 一个进程里所有保留回路的读数面。
///
/// 每条回路每算一拍就 [`evaluate`](Self::evaluate) 一次——即使这一拍的目标与
/// 上一拍相同。目标相同是正确行为（同样的输入得出同样的值），所以「这个回路还在
/// 算」只能由求值计数来答；没有它，一个停摆的清扫与一个安静的表在读数上同形。
#[derive(Debug, Default)]
pub struct RetentionReadings {
    states: std::sync::Mutex<std::collections::HashMap<String, Reading>>,
}

impl RetentionReadings {
    /// 一块空的读数面。回路持有它，`/metrics` 收集它。
    pub fn new() -> Self {
        Self::default()
    }

    /// 记下这一拍算出的目标，返回同一个值。
    ///
    /// `derived` 说的是目标的**输入**：`false` 表示回路还没有量到该量的东西，
    /// 目标仍是部署声明的 seed。它每拍都会被重新判断，所以量到之后同一条序列的
    /// 标签会从 `seed` 变成 `derived`，而计数继续往上走。
    pub fn evaluate(&self, knob: &str, target: f64, unit: &'static str, derived: bool) -> f64 {
        let mut states = self.states.lock().unwrap();
        let state = states.entry(knob.to_string()).or_insert(Reading {
            target,
            evaluations: 0,
            unit,
            derived,
        });
        state.target = target;
        state.unit = unit;
        state.derived = derived;
        state.evaluations = state.evaluations.saturating_add(1);
        target
    }

    /// 这条回路最近一拍的目标、算过的拍数、以及目标是否来自实测。
    pub fn reading(&self, knob: &str) -> Option<(f64, u64, bool)> {
        self.states
            .lock()
            .unwrap()
            .get(knob)
            .map(|state| (state.target, state.evaluations, state.derived))
    }

    /// 这条回路算过几拍。零是「还没算过」，不是「算出来是零」。
    pub fn evaluations(&self, knob: &str) -> u64 {
        self.reading(knob).map(|(_, evaluations, _)| evaluations).unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl crate::observability::Observable for RetentionReadings {
    async fn collect_metrics(
        &self,
        _dimension: &str,
    ) -> crate::SFResult<Vec<crate::observability::RawMetric>> {
        let states = self.states.lock().unwrap();
        let mut out = Vec::new();
        for (knob, state) in states.iter() {
            out.push(
                crate::observability::RawMetric::new(TARGET_METRIC, state.target)
                    .with_label(KNOB_LABEL, knob.clone())
                    .with_label(UNIT_LABEL, state.unit)
                    .with_label(
                        SOURCE_LABEL,
                        if state.derived { SOURCE_DERIVED } else { SOURCE_SEED },
                    ),
            );
            out.push(
                crate::observability::RawMetric::new(EVALUATIONS_METRIC, state.evaluations as f64)
                    .with_label(KNOB_LABEL, knob.clone()),
            );
        }
        Ok(out)
    }

    async fn collect_trace(
        &self,
        _task_id: &str,
    ) -> crate::SFResult<Vec<crate::observability::TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<crate::observability::DimensionSpec> {
        Vec::new()
    }
}

/// 一个字节预算，按实测的一行字节，买得到多少行。
///
/// 未测到的行成本（零、负数、非数）只买到一行：一个量不出来的成本不能反过来
/// 授权无限多行——那正是这个回路要防的那件事。调用方应当先测再算，这个返回值
/// 只是让「测不到」落在一个安全的一侧。
pub fn rows_that_fit(budget_bytes: u64, bytes_per_row: f64) -> u64 {
    if !bytes_per_row.is_finite() || bytes_per_row <= 0.0 {
        return 1;
    }
    let rows = budget_bytes as f64 / bytes_per_row;
    if !rows.is_finite() {
        return u64::MAX;
    }
    (rows.floor() as u64).max(1)
}

/// 一个字节预算，按实测的日写入量，买得到多少天。
///
/// 与 [`rows_that_fit`] 同一条律、同一侧安全边界：写不出来的日写入量只买到一天。
pub fn days_that_fit(budget_bytes: u64, bytes_per_day: f64) -> u64 {
    if !bytes_per_day.is_finite() || bytes_per_day <= 0.0 {
        return 1;
    }
    let days = budget_bytes as f64 / bytes_per_day;
    if !days.is_finite() {
        return u64::MAX;
    }
    (days.floor() as u64).max(1)
}

/// 一份按预算轮转的保底寿命，秒。
///
/// 到达方每秒钟带来 `arrivals_per_day / 86_400` 件、每件 `bytes_per_item` 字节，
/// 预算被填满所需的时间就是一件东西平均能活多久；取一半，是让清扫发生在再次填满
/// 之前、而不是恰好撞在填满的那一刻。到达率或单件体积量不出来时返回零，由调用方
/// 落回它自己的 seed——一个「没有到达」的回路不该自己发明寿命。
pub fn ttl_for_half_turnover(
    budget_bytes: u64,
    arrivals_per_day: f64,
    bytes_per_item: f64,
) -> std::time::Duration {
    if !arrivals_per_day.is_finite()
        || arrivals_per_day <= 0.0
        || !bytes_per_item.is_finite()
        || bytes_per_item <= 0.0
    {
        return std::time::Duration::ZERO;
    }
    let bytes_per_second = arrivals_per_day / 86_400.0 * bytes_per_item;
    if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
        return std::time::Duration::ZERO;
    }
    let secs = budget_bytes as f64 / bytes_per_second / 2.0;
    if !secs.is_finite() || secs <= 0.0 {
        return std::time::Duration::ZERO;
    }
    std::time::Duration::from_secs_f64(secs)
}

/// 一个字节预算，按实测的每件体积，留得下最新几件。
///
/// `sizes` 从最新到最旧；从最新那件开始累计，装不下就停——最新的一件永远留下，
/// 即使它一件就超预算：它是要写出去或刚写下的那一件，删掉它不能省下一个仍在
/// 发生的写入，只会让「保留」这件事失去意义。
pub fn items_that_fit(budget_bytes: u64, sizes: &[u64]) -> u64 {
    let mut used = 0u64;
    let mut kept = 0u64;
    for size in sizes {
        if kept > 0 && used.saturating_add(*size) > budget_bytes {
            break;
        }
        used = used.saturating_add(*size);
        kept += 1;
    }
    kept.max(1).min(sizes.len().max(1) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT: i64 = 30;
    const MIN: i64 = 1;

    /// 两种表态只在 `0` 上分道：同一个正数读数必须给出同一个天数。
    #[test]
    fn the_two_policies_disagree_on_zero_and_nowhere_else() {
        for raw in [
            None,
            Some(""),
            Some(" 7 "),
            Some("1"),
            Some("90"),
            Some("-7"),
            Some("not a number"),
            Some("1.5"),
        ] {
            assert_eq!(
                Some(keep_days_or_default(raw, DEFAULT, MIN)),
                keep_days_or_disable(raw, DEFAULT, MIN),
                "{raw:?} 只在 0 上允许两种表态不同"
            );
        }
    }

    /// 那个唯一的零点：两侧各写死一条，改哪一条都会红。
    #[test]
    fn zero_means_whatever_the_knob_says_it_means() {
        assert_eq!(
            keep_days_or_default(Some("0"), DEFAULT, MIN),
            DEFAULT,
            "落回默认的那一类：0 天等于每一趟清空，与「保留正常工作」在读数上同形"
        );
        assert_eq!(
            keep_days_or_disable(Some("0"), DEFAULT, MIN),
            None,
            "关掉的那一类：0 是显式关掉，不是「按最短保留期清扫」"
        );
    }

    /// 一个读得出来的正数按它自己的数走，包括它恰好等于下限。
    #[test]
    fn a_readable_retention_is_honoured() {
        assert_eq!(keep_days_or_default(Some("7"), DEFAULT, MIN), 7);
        assert_eq!(keep_days_or_default(Some(" 90 "), DEFAULT, MIN), 90);
        assert_eq!(keep_days_or_default(Some("1"), DEFAULT, MIN), 1);
        assert_eq!(keep_days_or_disable(Some("7"), DEFAULT, MIN), Some(7));
        assert_eq!(keep_days_or_disable(Some(" 90 "), DEFAULT, MIN), Some(90));
    }

    /// 读不出来与低于下限落回默认，在任何表态下都不是「关掉」的另一种写法。
    #[test]
    fn a_value_that_cannot_be_read_falls_back() {
        for raw in [None, Some(""), Some("-7"), Some("nope")] {
            assert_eq!(keep_days_or_default(raw, DEFAULT, MIN), DEFAULT);
            assert_eq!(keep_days_or_disable(raw, DEFAULT, MIN), Some(DEFAULT));
        }
    }

    /// 下限是调用方给的：低于它的正数落回默认，而不是被当成合法的小保留期。
    #[test]
    fn the_floor_belongs_to_the_knob() {
        assert_eq!(keep_days_or_default(Some("2"), DEFAULT, 3), DEFAULT);
        assert_eq!(keep_days_or_default(Some("3"), DEFAULT, 3), 3);
        assert_eq!(keep_days_or_disable(Some("2"), DEFAULT, 3), Some(DEFAULT));
        assert_eq!(keep_days_or_disable(Some("3"), DEFAULT, 3), Some(3));
    }

    /// 用量账那三个数只有一个出处：清扫循环与读口都把变量名与天数指向这里。
    /// 抄一份到读口那一侧，旋钮被改的那天就只有一边跟着动——账本留三十天、接口
    /// 覆盖七天，而两边各自的读数都对。
    #[test]
    fn the_usage_ledger_knob_is_declared_once() {
        assert_eq!(LLM_USAGE_KEEP_DAYS_ENV, "COGNEVA_LLM_USAGE_KEEP_DAYS");
        assert_eq!(LLM_USAGE_KEEP_DAYS_DEFAULT, 30);
        assert_eq!(LLM_USAGE_KEEP_DAYS_MIN, 1);
        // 读口要拿它当上限用，所以它必须永远是一个生效的天数。
        assert_eq!(llm_usage_keep_days_from(Some("7")), 7);
        assert_eq!(
            llm_usage_keep_days_from(Some("0")),
            LLM_USAGE_KEEP_DAYS_DEFAULT
        );
        assert_eq!(llm_usage_keep_days_from(None), LLM_USAGE_KEEP_DAYS_DEFAULT);
        assert_eq!(
            llm_usage_keep_days_from(Some("-3")),
            LLM_USAGE_KEEP_DAYS_DEFAULT
        );
    }

    /// 读口跟着清扫循环的最后一拍：发布之前读 seed，发布之后读同一句话，
    /// 发布零（还没有一拍算过）又回到 seed。
    #[test]
    fn the_reader_follows_the_sweeps_last_beat() {
        publish_llm_usage_keep_days(0);
        assert_eq!(llm_usage_keep_days(), llm_usage_keep_days_seed());
        publish_llm_usage_keep_days(12);
        assert_eq!(llm_usage_keep_days(), 12);
        assert_eq!(
            llm_usage_keep_days_seed(),
            llm_usage_keep_days_from(std::env::var(LLM_USAGE_KEEP_DAYS_ENV).ok().as_deref()),
            "the loop still reads the deployment's seed, not its own output"
        );
        publish_llm_usage_keep_days(0);
    }

    /// 零不是「下限恰好是 0 时的合法天数」：一个允许极小保留期的旋钮传
    /// `min_days = 0` 时，零仍然落回默认（或者关掉），否则它是一张每趟被清空的表。
    #[test]
    fn a_zero_floor_does_not_let_zero_days_through() {
        assert_eq!(keep_days_or_default(Some("0"), DEFAULT, 0), DEFAULT);
        assert_eq!(keep_days_or_disable(Some("0"), DEFAULT, 0), None);
        assert_eq!(keep_days_or_default(Some("-1"), DEFAULT, 0), DEFAULT);
        assert_eq!(keep_days_or_disable(Some("-1"), DEFAULT, 0), Some(DEFAULT));
    }
}

#[cfg(test)]
mod read_windows {
    use super::*;

    /// 读者问得更宽，下一拍的地板跟着变宽；问得比声明窄，声明仍是生效值。
    #[test]
    fn the_floor_follows_the_longest_span_a_reader_asked_for() {
        let day = OBSERVABILITY_EVENTS_READ_WINDOW_DEFAULT_SECS;
        assert_eq!(read_window_secs(day, None), (day, false));
        assert_eq!(
            read_window_secs(day, Some(3_600)),
            (day, false),
            "a narrower question does not shrink a floor"
        );
        assert_eq!(
            read_window_secs(day, Some(7 * day)),
            (7 * day, true),
            "a week-long question widens the floor and the reading says so"
        );
    }

    /// 读数只增不减：后来没人再问七天，也不把已经答应过的七天收回来。
    #[test]
    fn a_recorded_span_is_a_high_water_mark() {
        const KNOB: &str = "test.readWindowSeconds.high_water";
        assert_eq!(longest_read_span(KNOB), None);
        record_read_span(KNOB, 3_600);
        assert_eq!(longest_read_span(KNOB), Some(3_600));
        record_read_span(KNOB, 600);
        assert_eq!(longest_read_span(KNOB), Some(3_600));
        record_read_span(KNOB, 7_200);
        assert_eq!(longest_read_span(KNOB), Some(7_200));
        record_read_span(KNOB, 0);
        assert_eq!(longest_read_span(KNOB), Some(7_200), "a zero span is not a question");
    }
}

#[cfg(test)]
mod fits {
    use super::*;
    use std::time::Duration;

    /// 预算是卷的份额，不是卷本身：多张表共用一张卷，任何一张按整卷算都会把
    /// 其余几张挤出去。
    #[test]
    fn a_table_budget_is_a_share_of_the_volume() {
        let volume = 10 * 1024u64.pow(3);
        assert_eq!(
            table_budget_bytes(volume),
            volume * RETENTION_TABLE_SHARE_PERCENT / 100
        );
        assert_eq!(table_budget_bytes(0), 0);
    }

    /// 卷的声明按同一套数量词读；读不出来是 `None`，不是一个猜出来的体积。
    #[test]
    fn the_volume_is_read_in_the_same_units_as_every_other_claim() {
        assert_eq!(database_storage_bytes(Some("10Gi")), Some(10 * 1024u64.pow(3)));
        assert_eq!(database_storage_bytes(Some(" 512Mi ")), Some(512 * 1024u64.pow(2)));
        assert_eq!(database_storage_bytes(Some("0")), None);
        assert_eq!(database_storage_bytes(Some("")), None);
        assert_eq!(database_storage_bytes(Some("ten")), None);
        assert_eq!(database_storage_bytes(None), None);
    }

    /// 行数是当拍实测行成本的函数：同一个预算下，每行变大，容量就变小。
    /// 这是「改输入、不重启、下一拍读数跟上」的那一半，在律上可证。
    #[test]
    fn rows_follow_the_measured_cost_of_a_row() {
        let budget = 512 * 1024 * 1024;
        assert_eq!(rows_that_fit(budget, 1024.0), 524_288);
        assert_eq!(rows_that_fit(budget, 4096.0), 131_072);
        assert_eq!(rows_that_fit(budget, 1024.0 * 8.0), 65_536);
    }

    /// 量不出来的行成本只买到一行：回路的另一侧必须是「更少」，不是「更多」。
    #[test]
    fn an_unmeasured_cost_never_licenses_unbounded_rows() {
        for cost in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(rows_that_fit(512 * 1024 * 1024, cost), 1, "{cost}");
        }
    }

    /// 天数是日写入量的函数：写得越快，同样的预算买到的天数越少。
    #[test]
    fn days_follow_the_measured_daily_write() {
        let budget = 512 * 1024 * 1024;
        assert_eq!(days_that_fit(budget, 10.0 * 1024.0 * 1024.0), 51);
        assert_eq!(days_that_fit(budget, 100.0 * 1024.0 * 1024.0), 5);
        assert_eq!(days_that_fit(budget, 0.0), 1);
        assert_eq!(days_that_fit(budget, f64::NAN), 1);
    }

    /// 寿命是到达率与单件体积的函数；到达量不出来时返回零，由调用方落回 seed。
    #[test]
    fn the_turnover_ttl_follows_arrivals_and_item_size() {
        let budget = 1024 * 1024 * 1024;
        // 每天十件、每件 1 MiB：填满预算要 102.4 天，一半是 51.2 天。
        let ttl = ttl_for_half_turnover(budget, 10.0, 1024.0 * 1024.0);
        assert_eq!(ttl.as_secs(), (102.4 * 86_400.0 / 2.0) as u64);
        // 到达翻倍，寿命减半——同一个预算里能同时活着的件数不翻倍。
        let faster = ttl_for_half_turnover(budget, 20.0, 1024.0 * 1024.0);
        assert_eq!(faster.as_secs(), ttl.as_secs() / 2);
        // 单件变大，寿命同比例变短。
        let bigger = ttl_for_half_turnover(budget, 10.0, 2.0 * 1024.0 * 1024.0);
        assert_eq!(bigger.as_secs(), ttl.as_secs() / 2);
        assert_eq!(ttl_for_half_turnover(budget, 0.0, 1024.0), Duration::ZERO);
        assert_eq!(ttl_for_half_turnover(budget, 10.0, 0.0), Duration::ZERO);
    }


    /// 行容量是当拍实测的函数：同样是这张卷，每行变大容量就变小；卷变小容量
    /// 也变小。seed 只在量不到的时候顶着（没有卷、表还空、行成本读不出）。
    #[test]
    fn the_row_budget_follows_the_measured_cost_and_the_volume() {
        let volume = 10 * 1024u64.pow(3);
        // 5% 的卷 = 512 MiB。1000 行占 1 MiB，即每行 1048.576 字节。
        assert_eq!(
            row_budget_against_volume(40_000, Some(volume), 1_000, Some(1024 * 1024)),
            (512_000, SOURCE_DERIVED)
        );
        // 行成本翻倍，容量减半——改的是同一次实测的输入。
        assert_eq!(
            row_budget_against_volume(40_000, Some(volume), 1_000, Some(2 * 1024 * 1024)),
            (256_000, SOURCE_DERIVED)
        );
        // 卷减半，容量减半。
        assert_eq!(
            row_budget_against_volume(40_000, Some(volume / 2), 1_000, Some(1024 * 1024)),
            (256_000, SOURCE_DERIVED)
        );
        // 量不到：卷没声明、表还空、行成本读不出，都落回 seed。
        assert_eq!(
            row_budget_against_volume(40_000, None, 1_000, Some(1024 * 1024)),
            (40_000, SOURCE_SEED)
        );
        assert_eq!(
            row_budget_against_volume(40_000, Some(volume), 0, Some(0)),
            (40_000, SOURCE_SEED)
        );
        assert_eq!(
            row_budget_against_volume(40_000, Some(volume), 1_000, Some(0)),
            (40_000, SOURCE_SEED)
        );
        // 关掉清扫的部署不会被实测把闸重新打开。
        assert_eq!(
            row_budget_against_volume(0, Some(volume), 1_000, Some(1024 * 1024)),
            (0, SOURCE_SEED)
        );
    }


    /// 整张卷归一种东西（registry 的 rev）时预算就是整张卷：同一条除法，不乘
    /// 表份额。每件成本的输入是「实测总量 ÷ 实测件数」，两个方向都跟。
    #[test]
    fn a_whole_volume_budget_counts_what_fits_at_the_measured_item_cost() {
        let volume = 10 * 1024u64.pow(3);
        // 20 个 rev 共占 1 GiB 的独有字节：每 rev 51.2 MiB，整卷买 200 个。
        assert_eq!(
            budget_against_measured_items(
                20,
                Some(volume),
                20,
                Some(1024 * 1024 * 1024)
            ),
            (200, SOURCE_DERIVED)
        );
        // 独有字节翻倍，容量减半；持有件数翻倍（同样的字节摊到更多 rev 上），
        // 每件成本减半，容量翻倍。
        assert_eq!(
            budget_against_measured_items(20, Some(volume), 20, Some(2 * 1024 * 1024 * 1024)),
            (100, SOURCE_DERIVED)
        );
        assert_eq!(
            budget_against_measured_items(20, Some(volume), 40, Some(1024 * 1024 * 1024)),
            (400, SOURCE_DERIVED)
        );
        // 同一张卷、同一份字节，表要走 5% 的份额：预算不同，律是同一条。
        assert_eq!(
            row_budget_against_volume(20, Some(volume), 20, Some(1024 * 1024 * 1024)),
            (10, SOURCE_DERIVED)
        );
        // 量不到与关掉：落回 seed，且不标 derived。
        assert_eq!(
            budget_against_measured_items(20, None, 20, Some(1024)),
            (20, SOURCE_SEED)
        );
        assert_eq!(
            budget_against_measured_items(20, Some(volume), 0, Some(1024)),
            (20, SOURCE_SEED)
        );
        assert_eq!(
            budget_against_measured_items(20, Some(volume), 20, None),
            (20, SOURCE_SEED)
        );
        assert_eq!(
            budget_against_measured_items(0, Some(volume), 20, Some(1024)),
            (0, SOURCE_SEED)
        );
    }

    /// 保留天数是当拍实测的函数：同样的跨度里写了两倍字节，能留的天数就减半；
    /// 卷减半同样减半；量不到时 seed 顶着。
    #[test]
    fn the_kept_days_follow_the_measured_daily_write_and_the_volume() {
        let volume = 10 * 1024u64.pow(3);
        // 10 天里写了 512 MiB，即 51.2 MiB/天；5% 的卷 = 512 MiB，买 10 天。
        assert_eq!(
            keep_days_against_volume(30, Some(volume), 10 * 86_400, Some(512 * 1024 * 1024)),
            (10, true)
        );
        assert_eq!(
            keep_days_against_volume(30, Some(volume), 10 * 86_400, Some(1024 * 1024 * 1024)),
            (5, true)
        );
        assert_eq!(
            keep_days_against_volume(30, Some(volume / 2), 10 * 86_400, Some(512 * 1024 * 1024)),
            (5, true)
        );
        assert_eq!(
            keep_days_against_volume(30, None, 10 * 86_400, Some(512 * 1024 * 1024)),
            (30, false)
        );
        assert_eq!(keep_days_against_volume(30, Some(volume), 0, Some(512)), (30, false));
        assert_eq!(keep_days_against_volume(30, Some(volume), 86_400, None), (30, false));
    }

    /// 最新的一件永远留下；再往前的按体积装，装不下就停。
    #[test]
    fn items_keep_the_newest_and_stop_when_the_budget_is_full() {
        let sizes = [4, 4, 4, 4, 4];
        assert_eq!(items_that_fit(12, &sizes), 3);
        assert_eq!(items_that_fit(11, &sizes), 2);
        assert_eq!(items_that_fit(1, &sizes), 1);
        // 最新那件本身超预算也留下：它是仍在发生的那一笔写入。
        assert_eq!(items_that_fit(0, &sizes), 1);
        assert_eq!(items_that_fit(12, &[100]), 1);
        assert_eq!(items_that_fit(12, &[]), 1);
    }
}

#[cfg(test)]
mod readings {
    use super::*;
    use crate::observability::Observable;

    /// 同一个输入两拍：目标不变是正确行为，变的是求值计数——「这一拍算过」只能
    /// 由它来答。输入的来源也从 seed 变成 derived，标签跟着走。
    #[tokio::test]
    async fn the_published_series_carry_the_target_the_unit_and_the_beats() {
        let readings = RetentionReadings::new();
        assert_eq!(readings.evaluations("checkpointStore.maxRows"), 0);
        readings.evaluate("checkpointStore.maxRows", 40_000.0, UNIT_ROWS, false);
        let series = readings.collect_metrics("").await.expect("collect");
        let target = series
            .iter()
            .find(|m| m.name == TARGET_METRIC)
            .expect("target series");
        assert_eq!(target.value, 40_000.0);
        assert_eq!(target.labels.get(KNOB_LABEL), Some(&"checkpointStore.maxRows".to_string()));
        assert_eq!(target.labels.get(UNIT_LABEL), Some(&UNIT_ROWS.to_string()));
        assert_eq!(target.labels.get(SOURCE_LABEL), Some(&SOURCE_SEED.to_string()));

        readings.evaluate("checkpointStore.maxRows", 40_000.0, UNIT_ROWS, true);
        let series = readings.collect_metrics("").await.expect("collect");
        let target = series.iter().find(|m| m.name == TARGET_METRIC).unwrap();
        assert_eq!(target.value, 40_000.0, "same input, same target");
        assert_eq!(target.labels.get(SOURCE_LABEL), Some(&SOURCE_DERIVED.to_string()));
        let evaluations = series
            .iter()
            .find(|m| m.name == EVALUATIONS_METRIC)
            .expect("evaluation series");
        assert_eq!(evaluations.value, 2.0, "the second beat still beats");
        assert_eq!(
            evaluations.labels.get(KNOB_LABEL),
            Some(&"checkpointStore.maxRows".to_string())
        );
    }

    /// 两条回路各占一条序列：一个 knobs 的目标不会覆盖另一个的。
    #[tokio::test]
    async fn each_knob_keeps_its_own_reading() {
        let readings = RetentionReadings::new();
        readings.evaluate("checkpointStore.maxRows", 10.0, UNIT_ROWS, true);
        readings.evaluate("taskEventRetention.keepDays", 30.0, UNIT_DAYS, true);
        let series = readings.collect_metrics("").await.expect("collect");
        let targets: Vec<_> = series.iter().filter(|m| m.name == TARGET_METRIC).collect();
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().any(|m| m.labels.get(UNIT_LABEL).map(String::as_str) == Some(UNIT_ROWS)));
        assert!(targets.iter().any(|m| m.labels.get(UNIT_LABEL).map(String::as_str) == Some(UNIT_DAYS)));
    }
}

/// 本进程声明的数据库卷字节数；没声明就是 `None`。
pub fn database_volume_from_env() -> Option<u64> {
    database_storage_bytes(std::env::var(DATABASE_STORAGE_ENV).ok().as_deref())
}

/// 观测事件读口的读窗口旋钮与它的部署声明。
///
/// 读窗口是「清扫不许越过的地板」，不是第二条保留期。它由**读者实际问过的
/// 最长跨度**决定：历史事件查询每答一次就把这次的跨度记进来，清扫下一拍按
/// 已见的最长跨度重算。放在这里而不是跟着清扫循环走，是因为读者在网关里、
/// 清扫在主应用里，两者互不依赖；旋钮只在一侧声明时，另一侧就只剩抄数字。
pub const OBSERVABILITY_EVENTS_READ_WINDOW_ENV: &str =
    "COGNEVA_OBSERVABILITY_EVENT_READ_WINDOW_SECS";

/// 观测事件读口在保留读数面上的旋钮名。
pub const OBSERVABILITY_EVENTS_READ_WINDOW_KNOB: &str = "observability.eventLog.readWindowSeconds";

/// 观测事件读口的部署声明：一天，与历史事件查询的默认窗同值。
// cap: bound — 与历史事件查询的默认窗同值，是读者与清扫共同约定的地板，不是某次测量
pub const OBSERVABILITY_EVENTS_READ_WINDOW_DEFAULT_SECS: u64 = 86_400;

/// 本进程读者实际问过的最长跨度，按旋钮名分开记。
///
/// 只增不减：一条被问过七天的查询不能因为后来没人再问七天就被缩回去，否则
/// 地板会随读者的作息移动，而它保护的正是「问过的人下次还能读到」这件事。
static LONGEST_READ_SPAN: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<&'static str, u64>>,
> = std::sync::OnceLock::new();

/// 记下一次读者实际问过的跨度。零跨度不算一次提问。
pub fn record_read_span(knob: &'static str, span_secs: u64) {
    if span_secs == 0 {
        return;
    }
    let spans = LONGEST_READ_SPAN
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut spans = spans.lock().unwrap();
    let seen = spans.entry(knob).or_insert(0);
    *seen = (*seen).max(span_secs);
}

/// 本进程读者问过的最长跨度；还没有人问过就是 `None`。
pub fn longest_read_span(knob: &'static str) -> Option<u64> {
    LONGEST_READ_SPAN
        .get()
        .and_then(|spans| spans.lock().unwrap().get(knob).copied())
        .filter(|seen| *seen > 0)
}

/// 部署声明的读窗口：读不出来、读到 0 都落回默认。
pub fn read_window_seed(raw: Option<&str>, default_secs: u64) -> u64 {
    raw.map(str::trim)
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(default_secs)
}

/// 地板 = 部署声明与实测最长跨度里更宽的那条。
///
/// 实测跨度比声明更窄时，声明仍是生效值——地板只许变宽。第二个返回值说的是
/// **生效值是否被实测推动过**：只有实测比声明更宽时才标 `derived`，否则读数
/// 如实显示 `seed`。
pub fn read_window_secs(seed_secs: u64, longest_span_secs: Option<u64>) -> (u64, bool) {
    match longest_span_secs {
        Some(span) if span > seed_secs => (span, true),
        _ => (seed_secs, false),
    }
}
