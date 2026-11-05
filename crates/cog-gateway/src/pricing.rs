//! 把一行的 token 折算成钱。
//!
//! 账本（`gateway_llm_usage`）记的是 token，不是钱。这两件事之间的桥就是价目：
//! 一个上游条目自己带一份，网关在落账时按当时那一刻算出一个数写进同一行。
//! 没有这座桥时，「这次改动省了多少钱」只能拿 token 乘一个从别处抄来的单价
//! 现算——而抄来的单价没有版本、没有出处、也不会有人去核，它过期的那天没有任何
//! 读数会喊。
//!
//! 三条纪律：
//!
//! * 算不出来就写 `NULL`，绝不写 0。缺价目、时段落在没声明的窗口里、上游根本没
//!   报 token（`usage_outcome != read`）——这三种都是「没算」，与「算出来是零」
//!   是两件事。写 0 会让一段没定价的时间在账单上读作「免费」。
//! * 价目跟着上游条目走，不跟着厂商名走。同一家厂商的不同型号价格不同；把价格
//!   挂在厂商上等于假设一个模型的价格代表它全家的。
//! * 每一行都记下用的是哪一版价目（[`Cost::book`]）。价格变了以后，历史行的金额
//!   不会被新价目重新解释——它带着自己那一刻的那一版。

use serde_json::Value;

/// 一份价目。价格用整数「微元 / unit_tokens」存，避免浮点在账上累积误差。
///
/// 微元是 1e-6 元（或 [`Self::currency`] 对应币种的 1e-6 单位）。用这个刻度是
/// 因为主流厂商的人民币价目最细到每百万 token 两分钱（¥0.02/M ⇒ 20,000 微元/单位），
/// 再细的刻度没有意义，再粗的刻度会把 ¥0.02 抹成 0。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceBook {
    /// 币种代码（`CNY` / `USD`）。只作标识，不做换算——换汇是读者的事，
    /// 账本只记上游自己计价的那个币种。
    pub currency: String,
    /// 一「单位」是多少 token。主流价目按百万 token 标价，但这是个可声明的数，
    /// 不写死。
    pub unit_tokens: u64,
    /// 价目出处（官方定价页 URL）。这是这份价目能被人核对的前提。
    pub source: String,
    /// 核对日期（`YYYY-MM-DD`）：这份数字是哪一天从 [`Self::source`] 抄下来的。
    pub checked_at: String,
    /// 分档价目。一档带窗口，另一档不带窗口作兜底。
    pub tiers: Vec<PriceTier>,
}

/// 一档价目：同一组单价适用的时段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceTier {
    /// 档名（`off_peak` / `peak` / `standard`），写进读数让人看得出这一行按哪一档算。
    pub name: String,
    /// 缓存未命中的输入，每 [`PriceBook::unit_tokens`] 的微元数。
    pub cache_miss_micros: u64,
    /// 缓存命中的输入，同上。
    pub cache_hit_micros: u64,
    /// 输出，同上。
    pub output_micros: u64,
    /// 这一档适用的时段窗口。空 = 兜底档（其余所有时段）。
    pub windows: Vec<PriceWindow>,
}

/// 一个时段窗口：一天里的分钟区间，可限定周几。
///
/// 只按分钟分档是不够的：厂商的高峰价常常只在**工作日**收（DeepSeek 就是周一至
/// 周五两个半天），周末与假日半价。只按钟点分档的话，周六上午会被按高峰计价，
/// 账上每天多算两档差价——而它读起来完全正常，没有任何一列会红。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceWindow {
    /// 一分钟刻度上的起点（含）。
    pub start_minute: u16,
    /// 终点（不含）。`start > end` 表跨午夜。
    pub end_minute: u16,
    /// 适用的周几，`1` = 周一 … `7` = 周日。空 = 每天。
    ///
    /// 只认「星期几」，不认法定节假日：厂商的原文是「周一至周五（不含中国法定
    /// 节假日）」，节假日表不在这一份价目里，也不该由一个价目去猜。没建模的代价
    /// 是那些日子里的峰时小时仍按峰价算——方向是**多算**，不会把花费算少，而
    /// 影响的量级是全年十几天里每天几个小时的价差。
    ///
    /// 跨午夜的窗口按**被问的那一分钟自己所属的周几**判，不按窗口起点那一天：
    /// 「周五 23:00 到周六 02:00」在这条规则下只覆盖周五 23:00–23:59 与周六
    /// 00:00–01:59 里的周五部分——两端各自按自己的日子判。价目里写窗口的人
    /// 因此不必去想「这一档到底算是哪一天开始的」。
    pub weekdays: Vec<u8>,
}

/// 一行算出来的钱。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cost {
    /// [`PriceBook::currency`] 的 1e-6 单位。
    pub micros: i64,
    /// 用的是哪一版价目：`<currency>@<checked_at>`。写进账本那一行。
    pub book: String,
    /// 落在哪一档。
    pub tier: String,
}

impl PriceBook {
    /// 从上游条目里那份 `pricing` 对象解析。任何一处不合法都返回 `None` 并带原因，
    /// 调用方把它打进日志——半份价目比没有价目更坏：它会让一部分调用有价、另一部分
    /// 没价，而「没价」在有价的行旁边会被读成「便宜」。
    pub fn parse(raw: &Value) -> Result<Self, String> {
        let obj = raw.as_object().ok_or("pricing 不是对象")?;
        let currency = obj
            .get("currency")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or("pricing.currency 缺失")?
            .to_string();
        let unit_tokens = obj
            .get("unit_tokens")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .ok_or("pricing.unit_tokens 缺失或为 0")?;
        let source = obj
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let checked_at = obj
            .get("checked_at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let tiers_raw = obj
            .get("tiers")
            .and_then(|v| v.as_array())
            .ok_or("pricing.tiers 不是数组")?;
        if tiers_raw.is_empty() {
            return Err("pricing.tiers 是空的".into());
        }
        let mut tiers = Vec::with_capacity(tiers_raw.len());
        for (i, t) in tiers_raw.iter().enumerate() {
            tiers.push(PriceTier::parse(t).map_err(|e| format!("tiers[{i}]: {e}"))?);
        }
        // 兜底档最多一个：两档都声称「其余所有时段」时，取哪一档取决于数组顺序，
        // 而顺序是配置里一次无心编辑就能改的东西。价目不能按顺序解释。
        if tiers.iter().filter(|t| t.windows.is_empty()).count() > 1 {
            return Err("pricing.tiers 里有多于一个不带 windows 的兜底档".into());
        }
        Ok(Self {
            currency,
            unit_tokens,
            source,
            checked_at,
            tiers,
        })
    }

    /// 落进账本那一行的价目标识。
    pub fn label(&self) -> String {
        if self.checked_at.is_empty() {
            self.currency.clone()
        } else {
            format!("{}@{}", self.currency, self.checked_at)
        }
    }

    /// 按 UTC 的一天里的第几分钟、以及星期几（`1` = 周一 … `7` = 周日）选档。
    /// 带窗口的档优先，兜底档其次；都没有返回 `None`——那说明价目没覆盖这一刻，
    /// 算不出来就是算不出来。
    pub fn tier_at(&self, utc_minute_of_day: u16, weekday: u8) -> Option<&PriceTier> {
        let m = utc_minute_of_day % 1440;
        if let Some(hit) = self.tiers.iter().find(|t| {
            t.windows
                .iter()
                .any(|w| covers(w.start_minute, w.end_minute, m) && w.applies_on(weekday))
        }) {
            return Some(hit);
        }
        self.tiers.iter().find(|t| t.windows.is_empty())
    }

    /// 把一次调用的 token 折算成钱。
    ///
    /// `input` 只算缓存未命中那部分、`cached` 是命中那部分——与账本两列的含义
    /// 一致（两者互补，加起来才是整个 prompt）。三个数全为 0 时返回 `None`：
    /// 那不是「这次调用免费」，是「这次调用没有读数」，让它读作零会把未计量的
    /// 调用混进总花费的每一次求和中。
    pub fn cost(
        &self,
        input: u64,
        cached: u64,
        output: u64,
        utc_minute_of_day: u16,
        weekday: u8,
    ) -> Option<Cost> {
        if input == 0 && cached == 0 && output == 0 {
            return None;
        }
        let tier = self.tier_at(utc_minute_of_day, weekday)?;
        let unit = self.unit_tokens as u128;
        // 四舍五入到 1 微元。截断会让每次调用少算不到 1 微元，而调用次数是百万级
        // 的，那一方向的偏差会累积成「账上永远比账单少一点」，且永远找不到是哪一行。
        let raw = (input as u128) * (tier.cache_miss_micros as u128)
            + (cached as u128) * (tier.cache_hit_micros as u128)
            + (output as u128) * (tier.output_micros as u128);
        let micros = (raw + unit / 2) / unit;
        let micros = i64::try_from(micros).ok()?;
        Some(Cost {
            micros,
            book: self.label(),
            tier: tier.name.clone(),
        })
    }
}

impl PriceTier {
    fn parse(raw: &Value) -> Result<Self, String> {
        let obj = raw.as_object().ok_or("不是对象")?;
        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or("name 缺失")?
            .to_string();
        let num = |k: &str| -> Result<u64, String> {
            obj.get(k)
                .and_then(|v| v.as_u64())
                .ok_or_else(|| format!("{k} 缺失或不是非负整数"))
        };
        let cache_miss_micros = num("cache_miss_micros")?;
        let cache_hit_micros = num("cache_hit_micros")?;
        let output_micros = num("output_micros")?;
        let mut windows = Vec::new();
        if let Some(arr) = obj.get("windows") {
            let arr = arr.as_array().ok_or("windows 不是数组")?;
            for w in arr {
                let s = w
                    .get("utc_start_minute")
                    .and_then(|v| v.as_u64())
                    .filter(|n| *n < 1440)
                    .ok_or("windows[].utc_start_minute 缺失或越界")?;
                let e = w
                    .get("utc_end_minute")
                    .and_then(|v| v.as_u64())
                    .filter(|n| *n <= 1440)
                    .ok_or("windows[].utc_end_minute 缺失或越界")?;
                if s as u16 == e as u16 {
                    return Err("窗口起止相同，长度为 0".into());
                }
                // 周几缺省 = 每天。给了就必须是 1..=7：0 与 8 这类笔误若被
                // 夹成「每天」或者被忽略，结果都是一档本来只在工作日收的价
                // 变成天天收，而账上看不出区别。
                let mut weekdays = Vec::new();
                if let Some(days) = w.get("weekdays") {
                    let days = days.as_array().ok_or("windows[].weekdays 不是数组")?;
                    for d in days {
                        let d = d
                            .as_u64()
                            .filter(|n| (1..=7).contains(n))
                            .ok_or("windows[].weekdays 的取值必须是 1..=7（1 = 周一）")?;
                        let d = d as u8;
                        if !weekdays.contains(&d) {
                            weekdays.push(d);
                        }
                    }
                    if weekdays.is_empty() {
                        return Err("windows[].weekdays 是空数组：要「每天」就别写这个键".into());
                    }
                }
                windows.push(PriceWindow {
                    start_minute: s as u16,
                    end_minute: e as u16,
                    weekdays,
                });
            }
        }
        Ok(Self {
            name,
            cache_miss_micros,
            cache_hit_micros,
            output_micros,
            windows,
        })
    }
}

impl PriceWindow {
    /// 这个窗口在 `weekday` 这一天是否成立。空 `weekdays` = 天天成立。
    fn applies_on(&self, weekday: u8) -> bool {
        self.weekdays.is_empty() || self.weekdays.contains(&weekday)
    }
}

/// `[start, end)` 是否覆盖第 `m` 分钟；`start > end` 表跨午夜。
fn covers(start: u16, end: u16, m: u16) -> bool {
    if start <= end {
        start <= m && m < end
    } else {
        m >= start || m < end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn book(tiers: Value) -> PriceBook {
        PriceBook::parse(&json!({
            "currency": "CNY",
            "unit_tokens": 1_000_000,
            "source": "https://api-docs.deepseek.com/zh-cn/quick_start/pricing/",
            "checked_at": "2026-11-05",
            "tiers": tiers,
        }))
        .expect("价目应当解析得出来")
    }

    /// 一份人民币价目：谷时 ¥1 / ¥0.02 / ¥4，峰时 ¥2 / ¥0.04 / ¥8。
    /// 峰时窗口只在工作日（与 DeepSeek 官方中文页一致：北京时间周一至周五
    /// 9:00–12:00、14:00–18:00，即 UTC 01:00–04:00、06:00–10:00）。
    fn deepseek_like() -> PriceBook {
        book(json!([
            {
                "name": "peak",
                "cache_miss_micros": 2_000_000,
                "cache_hit_micros": 40_000,
                "output_micros": 8_000_000,
                "windows": [
                    {"utc_start_minute": 60, "utc_end_minute": 240, "weekdays": [1, 2, 3, 4, 5]},
                    {"utc_start_minute": 360, "utc_end_minute": 600, "weekdays": [1, 2, 3, 4, 5]}
                ]
            },
            {
                "name": "off_peak",
                "cache_miss_micros": 1_000_000,
                "cache_hit_micros": 20_000,
                "output_micros": 4_000_000
            }
        ]))
    }

    /// 周五：工作日，峰时窗口成立。
    const FRIDAY: u8 = 5;
    /// 周六：同一个钟点，峰时窗口不成立。
    const SATURDAY: u8 = 6;

    /// 一次调用在谷时的钱要按谷价算，峰时按峰价算——同一份 token，两个时段两个数。
    /// 这条是整座桥的核心：不按时段取档，夜间与白天的钱会被算成同一个数，
    /// 而峰谷差价正是「在谷时跑长任务」那类省钱手段的读数来源。
    #[test]
    fn the_same_tokens_cost_more_in_a_peak_window() {
        let b = deepseek_like();
        // 三个数都取整到能整除的档（1,000 / 200 / 1,000）：两档各自四舍五入到
        // 微元时，带零头的数会让「峰时正好是谷时的两倍」差那么一微元，而那是
        // 取整的必然结果，不是判据。
        let tokens = (1_000, 200, 1_000);
        // 05:00 UTC —— 360 分钟，落在第二个峰时窗口 [360, 600) 的起点。
        let peak = b
            .cost(tokens.0, tokens.1, tokens.2, 360, FRIDAY)
            .expect("峰时应当算得出");
        // 08:00 UTC —— 480 分钟，仍在峰时窗口里。
        let still_peak = b
            .cost(tokens.0, tokens.1, tokens.2, 480, FRIDAY)
            .expect("峰时应当算得出");
        // 12:00 UTC —— 720 分钟，两个峰时窗口之外，落到兜底档。
        let off = b
            .cost(tokens.0, tokens.1, tokens.2, 720, FRIDAY)
            .expect("谷时应当算得出");

        assert_eq!(peak.tier, "peak");
        assert_eq!(still_peak, peak);
        assert_eq!(off.tier, "off_peak");
        assert_eq!(peak.micros, off.micros * 2);
    }

    /// 窗口按左闭右开切：终点那一分钟已经属于下一档。用闭区间会让峰谷交界那
    /// 一分钟同时属于两档，而取哪一档取决于数组顺序——价目不能按顺序解释。
    #[test]
    fn a_window_ends_exclusively() {
        let b = deepseek_like();
        assert_eq!(
            b.tier_at(239, FRIDAY).map(|t| t.name.as_str()),
            Some("peak")
        );
        assert_eq!(
            b.tier_at(240, FRIDAY).map(|t| t.name.as_str()),
            Some("off_peak")
        );
        assert_eq!(
            b.tier_at(359, FRIDAY).map(|t| t.name.as_str()),
            Some("off_peak")
        );
        assert_eq!(
            b.tier_at(360, FRIDAY).map(|t| t.name.as_str()),
            Some("peak")
        );
        assert_eq!(
            b.tier_at(600, FRIDAY).map(|t| t.name.as_str()),
            Some("off_peak")
        );
    }

    /// 只在工作日收的峰价，到了周末不许生效。
    ///
    /// 只按钟点取档时，周六上午会按峰价算，一天的账就这样多出两档差价——而每
    /// 一列都还在、都看起来对。价目里的周几不是修饰。
    #[test]
    fn a_weekday_only_window_does_not_apply_at_the_weekend() {
        let b = deepseek_like();
        assert_eq!(
            b.tier_at(120, FRIDAY).map(|t| t.name.as_str()),
            Some("peak")
        );
        assert_eq!(
            b.tier_at(120, SATURDAY).map(|t| t.name.as_str()),
            Some("off_peak"),
            "周六同一个钟点必须是谷价"
        );
        assert_eq!(b.tier_at(120, 7).map(|t| t.name.as_str()), Some("off_peak"));
        let fri = b.cost(1_000, 0, 1_000, 120, FRIDAY).unwrap();
        let sat = b.cost(1_000, 0, 1_000, 120, SATURDAY).unwrap();
        assert_eq!(fri.micros, sat.micros * 2);
    }

    /// 周几写错（0、8、非整数、空数组）整份拒收：夹成「每天」或者当作没写，
    /// 都会让一档本来只在工作日收的价变成天天收，而账上看不出区别。
    #[test]
    fn a_malformed_weekday_is_refused_whole() {
        for bad in [json!([0]), json!([8]), json!(["mon"]), json!([])] {
            let parsed = PriceBook::parse(&json!({
                "currency": "CNY",
                "unit_tokens": 1_000_000,
                "tiers": [{
                    "name": "peak",
                    "cache_miss_micros": 1, "cache_hit_micros": 1, "output_micros": 1,
                    "windows": [{"utc_start_minute": 60, "utc_end_minute": 240, "weekdays": bad}]
                }]
            }));
            assert!(parsed.is_err(), "weekdays = {bad} 应当整份拒收");
        }
    }

    /// 跨午夜的窗口：`start > end` 时覆盖两端。不支持跨午夜会把一个「23:00 起
    /// 优惠」的价目读成「23:00–23:59 之外全价」，而它读起来完全像一个正常的价目。
    #[test]
    fn a_window_may_wrap_past_midnight() {
        let b = book(json!([
            {
                "name": "discount",
                "cache_miss_micros": 1,
                "cache_hit_micros": 1,
                "output_micros": 1,
                "windows": [{"utc_start_minute": 1380, "utc_end_minute": 120}]
            },
            {"name": "standard", "cache_miss_micros": 2, "cache_hit_micros": 2, "output_micros": 2}
        ]));
        assert_eq!(
            b.tier_at(1380, FRIDAY).map(|t| t.name.as_str()),
            Some("discount")
        );
        assert_eq!(
            b.tier_at(1439, FRIDAY).map(|t| t.name.as_str()),
            Some("discount")
        );
        assert_eq!(
            b.tier_at(0, FRIDAY).map(|t| t.name.as_str()),
            Some("discount")
        );
        assert_eq!(
            b.tier_at(119, FRIDAY).map(|t| t.name.as_str()),
            Some("discount")
        );
        assert_eq!(
            b.tier_at(120, FRIDAY).map(|t| t.name.as_str()),
            Some("standard")
        );
        assert_eq!(
            b.tier_at(1379, FRIDAY).map(|t| t.name.as_str()),
            Some("standard")
        );
    }

    /// 算出来的钱要带着它自己那一版价目的标识。价格改了以后，历史行的金额不能被
    /// 新价目重新解释——它当时就是按那一版扣的。
    #[test]
    fn a_cost_carries_the_book_it_was_priced_from() {
        let b = deepseek_like();
        let c = b.cost(1_000, 0, 0, 720, FRIDAY).unwrap();
        assert_eq!(c.book, "CNY@2026-11-05");
        assert_eq!(c.micros, 1_000); // ¥1/M × 1,000 token = 1,000 微元 = ¥0.001
    }

    /// 三个 token 数全为 0 时不算价：那不是免费，是没有读数。这一条与账本里
    /// `usage_outcome` 那条纪律同源——未计量的调用不许混进任何一次求和。
    #[test]
    fn zero_tokens_price_to_nothing_rather_than_to_zero() {
        let b = deepseek_like();
        assert_eq!(b.cost(0, 0, 0, 720, FRIDAY), None);
    }

    /// 价目没覆盖这一刻（只有带窗口的档、这一刻不在任何窗口里）时返回 `None`，
    /// 不是 0。这是「算不出来就写 NULL」那条纪律在折算这一层的落点。
    #[test]
    fn an_uncovered_instant_prices_to_nothing() {
        let b = book(json!([{
            "name": "peak_only",
            "cache_miss_micros": 2_000_000,
            "cache_hit_micros": 40_000,
            "output_micros": 8_000_000,
            "windows": [{"utc_start_minute": 60, "utc_end_minute": 240}]
        }]));
        assert!(b.cost(1_000, 0, 1_000, 60, FRIDAY).is_some());
        assert_eq!(b.cost(1_000, 0, 1_000, 0, FRIDAY), None);
    }

    /// 半份价目整份拒收。只有一部分档解析得出时若认下，会出现「白天的行有价、
    /// 夜里的行没价」，而没价的行在有价的行旁边读起来像便宜的。
    #[test]
    fn a_half_readable_book_is_rejected_whole() {
        let bad = json!({
            "currency": "CNY",
            "unit_tokens": 1_000_000,
            "tiers": [
                {"name": "off_peak", "cache_miss_micros": 1_000_000, "cache_hit_micros": 20_000, "output_micros": 4_000_000},
                {"name": "peak", "cache_miss_micros": 2_000_000}
            ]
        });
        assert!(PriceBook::parse(&bad).is_err());
    }

    /// 两个兜底档同时存在时整份拒收：取哪一档取决于数组顺序，而顺序是一次无心
    /// 编辑就能改的东西。
    #[test]
    fn two_fallback_tiers_are_rejected() {
        let bad = json!({
            "currency": "CNY",
            "unit_tokens": 1_000_000,
            "tiers": [
                {"name": "a", "cache_miss_micros": 1, "cache_hit_micros": 1, "output_micros": 1},
                {"name": "b", "cache_miss_micros": 2, "cache_hit_micros": 2, "output_micros": 2}
            ]
        });
        assert!(PriceBook::parse(&bad).is_err());
    }

    /// 四舍五入而不是截断。¥0.02/M 的缓存命中价下，一次 782 token 的调用是
    /// 15.64 微元：截断成 15 会让每一行都少算一点点，而这是百万行级的账。
    #[test]
    fn fractions_round_rather_than_truncate() {
        let b = deepseek_like();
        // 782 × 20,000 / 1,000,000 = 15.64 → 16
        let c = b.cost(0, 782, 0, 720, FRIDAY).unwrap();
        assert_eq!(c.micros, 16);
    }
}
