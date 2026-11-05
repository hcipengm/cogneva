//! 官方价目页的核对：配置里那份价目，与厂商今天挂出来的那一版是不是同一份。
//!
//! [`crate::pricing`] 那份价目是**配置里的一份抄本**，抄的时候有出处、有日期。
//! 它不会自己更新：厂商调价的那天，账本会继续按旧价算钱，而旧价与新价在读数上
//! 完全同形——每一行的 `cost_micros` 都还在，只是全都算错了。
//!
//! 所以这里定时把官方页读一遍，与配置比：
//!
//! * 数字变了 → 报一条漂移，让人去改配置（**不自动改**：价格是钱的基准，让它
//!   自己动，历史行与未来行就不可比了，而且没人会去看它为什么变）。
//! * 页面读不到、结构改了、解析不出来 → **同样报一条**，而且是另一种原因。
//!   这两种都必须报：「解析失败」若静默，漂移检查就退化成「永远不会报」，
//!   而它读起来与「价格没变」一模一样。
//!
//! 解析只认三处：两个输入价、一个输出价，各分空闲与高峰。页面结构变了就解析
//! 失败——这是设计里预期的失败方式，不是意外。

use crate::pricing::{PriceBook, PriceTier};
use regex::Regex;

/// 从价目页读到的三档价，单位是「每百万 token 的人民币元」。
///
/// 用 `f64` 而不是微元整数：这一层比的是「页面上的数字和配置里的数字一样
/// 吗」，页面写的是 `4.5` 这种小数，先按原样接住再在比较时落到微元。
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedPrices {
    pub cache_hit_idle: f64,
    pub cache_hit_peak: f64,
    pub cache_miss_idle: f64,
    pub cache_miss_peak: f64,
    pub output_idle: f64,
    pub output_peak: f64,
    /// 官方页是不是把高峰限在工作日（页面上写的是「周一至周五」）。
    pub peak_weekdays_only: bool,
    /// 官方页声明的高峰时段，已经换算成 UTC 分钟窗口。
    ///
    /// 页面上写的是北京时间，而配置里存的是 UTC：这一处换算是价目能不能对上
    /// 的地方，写错一小时就是每天有几个小时按错档算钱，账上没有任何一列会红。
    pub peak_windows: Vec<(u16, u16)>,
}

/// 一处漂移：一行人读得懂的话。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drift {
    pub what: String,
    pub configured: String,
    pub observed: String,
}

/// 把价目页的 HTML 摊平成一行文本。
///
/// 只用正则剥标签，不引 HTML 解析器：这里要的不是页面结构，是「那三行数字」，
/// 而结构一变我们本来就该解析失败、报警。引一个解析器会让「结构变了」变成
/// 「解析器还在、但取到了别的数字」——那是唯一一种比失败更坏的结局。
fn flatten(html: &str) -> String {
    let script = Regex::new(r"(?is)<script.*?</script>").expect("静态正则");
    let style = Regex::new(r"(?is)<style.*?</style>").expect("静态正则");
    let tags = Regex::new(r"(?s)<[^>]+>").expect("静态正则");
    let ws = Regex::new(r"\s+").expect("静态正则");
    let text = script.replace_all(html, " ");
    let text = style.replace_all(&text, " ");
    let text = tags.replace_all(&text, " ");
    ws.replace_all(&text, " ").trim().to_string()
}

/// 在 `section` 里找 `anchor` 之后出现的第一个「<数字>元」。
fn yuan_after(section: &str, anchor: &str) -> Option<f64> {
    let at = section.find(anchor)?;
    let rest = &section[at + anchor.len()..];
    let re = Regex::new(r"([0-9]+(?:\.[0-9]+)?)\s*元").expect("静态正则");
    re.captures(rest)?.get(1)?.as_str().parse().ok()
}

/// 从价目页读三档价的空闲与高峰两面。
///
/// 页面的表格是「命中 / 未命中 / 输出」三行，每行两列（本型号 + 另一型号）。
/// 只取每段的**第一个**数字：第一个是这一行的主型号，后面的数字属于同一张表里
/// 的其它型号，把它们混进来会让比对标错型号。
pub fn parse_pricing_page(html: &str) -> Result<ObservedPrices, String> {
    let text = flatten(html);
    let hit_at = text.find("缓存命中").ok_or("页面上找不到「缓存命中」")?;
    let miss_at = text
        .find("缓存未命中")
        .ok_or("页面上找不到「缓存未命中」")?;
    let out_at = text
        .find("百万tokens输出")
        .ok_or("页面上找不到「百万tokens输出」")?;
    if !(hit_at < miss_at && miss_at < out_at) {
        return Err("页面上的三段价顺序变了，按位置切段会取错行".into());
    }
    let hit = &text[hit_at..miss_at];
    let miss = &text[miss_at..out_at];
    let out = &text[out_at..];
    let read = |section: &str, what: &str| -> Result<(f64, f64), String> {
        let idle =
            yuan_after(section, "空闲时段").ok_or_else(|| format!("{what}：找不到空闲时段价"))?;
        let peak =
            yuan_after(section, "高峰时段").ok_or_else(|| format!("{what}：找不到高峰时段价"))?;
        Ok((idle, peak))
    };
    let (cache_hit_idle, cache_hit_peak) = read(hit, "缓存命中")?;
    let (cache_miss_idle, cache_miss_peak) = read(miss, "缓存未命中")?;
    let (output_idle, output_peak) = read(out, "输出")?;
    let (peak_weekdays_only, peak_windows) = peak_schedule(&text);
    Ok(ObservedPrices {
        cache_hit_idle,
        cache_hit_peak,
        cache_miss_idle,
        cache_miss_peak,
        output_idle,
        output_peak,
        peak_weekdays_only,
        peak_windows,
    })
}

/// 北京时间的分钟数换成 UTC 的分钟数。
///
/// 页面上所有时刻都是北京时间（UTC+8），配置里存的是 UTC：这一步换错一小时，
/// 就是每天有几个小时按错档算钱，而账上没有任何一列会红。
fn beijing_to_utc(minute: u16) -> u16 {
    (minute + 1440 - 8 * 60) % 1440
}

/// 从页面正文里读高峰时段的形状：限不限工作日、是几个小时区间。
///
/// 读不出来不算失败（`peak_windows` 空 = 这一面不判），因为这几句是解释性文字，
/// 措辞比价目表更容易变；但读出来的部分必须与配置一致，否则按它算的钱就是错的。
fn peak_schedule(text: &str) -> (bool, Vec<(u16, u16)>) {
    let weekdays_only = text.contains("周一至周五");
    let mut windows = Vec::new();
    if let Some(at) = text.find("北京时间") {
        // 只看这一段之后的一小截：整页别处也可能有时刻，取全页会把它们混进来。
        let seg: String = text[at..].chars().take(200).collect();
        let re = Regex::new(r"([0-9]{1,2}):([0-9]{2})\s*[-–—]\s*([0-9]{1,2}):([0-9]{2})")
            .expect("静态正则");
        for c in re.captures_iter(&seg) {
            let h = |i: usize| c[i].parse::<u16>().ok();
            let (Some(sh), Some(sm), Some(eh), Some(em)) = (h(1), h(2), h(3), h(4)) else {
                continue;
            };
            let start = sh * 60 + sm;
            let end = eh * 60 + em;
            if start >= 1440 || end > 1440 || start == end {
                continue;
            }
            windows.push((beijing_to_utc(start), beijing_to_utc(end % 1440)));
        }
    }
    (weekdays_only, windows)
}

/// 元/百万 token 换成配置里的微元/单位。
///
/// 页面按百万 token 标价，配置按「一单位多少微元」存。一单位是 `unit_tokens` 个
/// token，一元是一百万微元：
///
/// `微元/单位 = 元/百万 × 1e6 微元/元 × unit_tokens / 1e6 token = 元/百万 × unit_tokens`
///
/// 恒等式的那个 1e6 恰好只 在 `unit_tokens == 1e6` 时抵消，所以不能省——写成
/// `yuan * 1e6 / unit_tokens` 在单位一百万时会把 ¥0.02 算成 0 微元，而那会让
/// 缓存命中这一档的对账永远说「没变化」。
fn micros_per_unit(yuan_per_million: f64, unit_tokens: u64) -> u64 {
    let per_unit = yuan_per_million * unit_tokens as f64;
    per_unit.round().max(0.0) as u64
}

fn tier_named<'a>(book: &'a PriceBook, name: &str) -> Option<&'a PriceTier> {
    book.tiers.iter().find(|t| t.name == name)
}

/// 比一份配置价目与页面上读到的价。
///
/// 只比「能对上的档」：页面上永远分空闲与高峰，所以配置要么按 `off_peak` /
/// `peak` 两档声明，要么就是没分——**没分本身就是漂移**（按一个价算两段，
/// 白天的钱永远比账单少一半）。只有一档且档名不是这两个之一时，报这一条。
pub fn compare(observed: &ObservedPrices, book: &PriceBook) -> Vec<Drift> {
    let unit = book.unit_tokens;
    let mut out = Vec::new();
    let has_idle = tier_named(book, "off_peak").is_some();
    let has_peak = tier_named(book, "peak").is_some();
    if !has_idle && !has_peak {
        out.push(Drift {
            what: "价目没有分峰谷".to_string(),
            configured: book
                .tiers
                .iter()
                .map(|t| t.name.clone())
                .collect::<Vec<_>>()
                .join(","),
            observed: "官方页分了空闲与高峰两档".to_string(),
        });
        return out;
    }
    let mut check = |tier_name: &str, label: &str, want: f64, got: u64| {
        if tier_named(book, tier_name).is_none() {
            out.push(Drift {
                what: format!("配置缺 {tier_name} 档"),
                configured: "-".to_string(),
                observed: format!("{label} {want} 元/百万"),
            });
            return;
        }
        let expect = micros_per_unit(want, unit);
        // 差一分钱（10,000 微元/单位）以上才算漂移：页面写 `4.5`、配置写
        // 4,500,000 微元，这是同一个数，不该报。
        if expect.abs_diff(got) > 10_000 {
            out.push(Drift {
                what: format!("{tier_name} 档的{label}价"),
                configured: format!("{got} 微元/单位"),
                observed: format!("{want} 元/百万 ⇒ {expect} 微元/单位"),
            });
        }
    };
    if let Some(t) = tier_named(book, "off_peak") {
        check(
            "off_peak",
            "缓存命中",
            observed.cache_hit_idle,
            t.cache_hit_micros,
        );
        check(
            "off_peak",
            "缓存未命中",
            observed.cache_miss_idle,
            t.cache_miss_micros,
        );
        check("off_peak", "输出", observed.output_idle, t.output_micros);
    }
    if let Some(t) = tier_named(book, "peak") {
        check(
            "peak",
            "缓存命中",
            observed.cache_hit_peak,
            t.cache_hit_micros,
        );
        check(
            "peak",
            "缓存未命中",
            observed.cache_miss_peak,
            t.cache_miss_micros,
        );
        check("peak", "输出", observed.output_peak, t.output_micros);
        // 价对不代表算对：同样两个数字，「工作日两个半天」与「天天这两个半天」
        // 差的是每周两天半的钱。这一条查的是窗口的形状，不是数字。
        if observed.peak_weekdays_only && t.windows.iter().any(|w| w.weekdays.is_empty()) {
            out.push(Drift {
                what: "peak 档的窗口没有限定工作日".to_string(),
                configured: t
                    .windows
                    .iter()
                    .map(|w| {
                        format!(
                            "{}-{}{}",
                            w.start_minute,
                            w.end_minute,
                            if w.weekdays.is_empty() {
                                "（每天）"
                            } else {
                                ""
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
                observed: "官方页把高峰限在周一至周五".to_string(),
            });
        }
        if !observed.peak_windows.is_empty() {
            let mut got: Vec<(u16, u16)> = t
                .windows
                .iter()
                .map(|w| (w.start_minute, w.end_minute))
                .collect();
            let mut want = observed.peak_windows.clone();
            got.sort_unstable();
            want.sort_unstable();
            want.dedup();
            if got != want {
                out.push(Drift {
                    what: "peak 档的时段".to_string(),
                    configured: format!("{got:?}（UTC 分钟）"),
                    observed: format!("{want:?}（UTC 分钟，页面写的是北京时间）"),
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 官方中文价目页在 2026-11-05 那一版的正文（剥掉标签后那一段）。
    /// 夹具是死的，页面变了它不会自己更新——这正是 `parse_pricing_page`
    /// 在运行时失败时要报警的原因：夹具证明的是解析器认这个形状，不是证明
    /// 今天的页面还是这个形状。
    const PAGE_2026_11_05: &str = r#"<html><body>
      <script>var x = "缓存命中 999元";</script>
      <p>deepseek-flash (1) deepseek-v4-pro</p>
      <p>思考模式 支持非思考与思考模式（默认）</p>
      <p>百万tokens输入 （缓存命中） 空闲时段 0.02元 0.15元 高峰时段 0.04元 0.30元</p>
      <p>百万tokens输入 （缓存未命中） 空闲时段 1元 4.5元 高峰时段 2元 9.0元</p>
      <p>百万tokens输出 空闲时段 4元 13.5元 高峰时段 8元 27.0元</p>
      <p>北京时间周一至周五（不含中国法定节假日）9:00 - 12:00、14:00 - 18:00 为高峰时段；其余时段，包括周末及中国法定节假日全天均为空闲时段。</p>
    </body></html>"#;

    #[test]
    fn the_page_reads_as_three_prices_on_two_schedules() {
        let p = parse_pricing_page(PAGE_2026_11_05).expect("这一版的页面应当解析得出来");
        assert_eq!(p.cache_hit_idle, 0.02);
        assert_eq!(p.cache_hit_peak, 0.04);
        assert_eq!(p.cache_miss_idle, 1.0);
        assert_eq!(p.cache_miss_peak, 2.0);
        assert_eq!(p.output_idle, 4.0);
        assert_eq!(p.output_peak, 8.0);
    }

    /// 只有第一个数字属于主型号。同一行后面的数字是表里另一列（另一个型号）
    /// 的价——把它们当成本型号的价会让对账在另一家调价时误报。
    #[test]
    fn the_first_number_on_a_row_belongs_to_the_main_model() {
        let p = parse_pricing_page(PAGE_2026_11_05).unwrap();
        // 第二列是 4.5 / 9.0 / 13.5 / 27.0，一个都不该被读到。
        assert_ne!(p.cache_miss_idle, 4.5);
        assert_ne!(p.output_peak, 27.0);
    }

    /// `<script>` 里的文本不算数。页面脚本文本里出现「缓存命中」时若剥不掉，
    /// 解析会从脚本里取价——那是比解析失败更坏的一种错：它取到的数看起来
    /// 完全正常。
    #[test]
    fn script_text_is_not_a_price_row() {
        let html = r#"<script>百万tokens输入 （缓存命中） 空闲时段 99元</script>
            <p>百万tokens输入 （缓存命中） 空闲时段 0.02元 高峰时段 0.04元</p>
            <p>百万tokens输入 （缓存未命中） 空闲时段 1元 高峰时段 2元</p>
            <p>百万tokens输出 空闲时段 4元 高峰时段 8元</p>"#;
        let p = parse_pricing_page(html).unwrap();
        assert_eq!(p.cache_hit_idle, 0.02);
    }

    /// 三段价的顺序变了就拒解析，不按位置硬切。顺序变了还硬切，会把「未命中」
    /// 那行的数字读成「命中」那一行的——两份都读成一个数，账上却看不出错。
    #[test]
    fn a_reordered_table_is_refused_rather_than_sliced_by_position() {
        let html = r#"<p>百万tokens输出 空闲时段 4元 高峰时段 8元</p>
            <p>百万tokens输入 （缓存命中） 空闲时段 0.02元 高峰时段 0.04元</p>
            <p>百万tokens输入 （缓存未命中） 空闲时段 1元 高峰时段 2元</p>"#;
        assert!(parse_pricing_page(html).is_err());
    }

    /// 页面缺一段就整份拒收。缺「输出」那一行时若只解析前两段，对账会在
    /// 输出价上永远说「没变化」——而那正是最贵的一档。
    #[test]
    fn a_page_missing_a_row_is_refused_whole() {
        let html = r#"<p>百万tokens输入 （缓存命中） 空闲时段 0.02元 高峰时段 0.04元</p>
            <p>百万tokens输入 （缓存未命中） 空闲时段 1元 高峰时段 2元</p>"#;
        assert!(parse_pricing_page(html).is_err());
    }

    fn book_with(tiers: serde_json::Value) -> PriceBook {
        PriceBook::parse(&json!({
            "currency": "CNY",
            "unit_tokens": 1_000_000,
            "checked_at": "2026-11-05",
            "tiers": tiers,
        }))
        .expect("价目应当解析得出来")
    }

    /// 配置与页面一致时一条漂移都不该报——这是对账器的日常结局，也是它不被人
    /// 关掉的原因。
    #[test]
    fn a_matching_book_has_no_drift() {
        let book = book_with(json!([
            {"name": "off_peak", "cache_miss_micros": 1_000_000, "cache_hit_micros": 20_000,
             "output_micros": 4_000_000},
            {"name": "peak", "cache_miss_micros": 2_000_000, "cache_hit_micros": 40_000,
             "output_micros": 8_000_000,
             "windows": [
                 {"utc_start_minute": 60, "utc_end_minute": 240, "weekdays": [1, 2, 3, 4, 5]},
                 {"utc_start_minute": 360, "utc_end_minute": 600, "weekdays": [1, 2, 3, 4, 5]}
             ]}
        ]));
        let observed = parse_pricing_page(PAGE_2026_11_05).unwrap();
        assert!(compare(&observed, &book).is_empty());
    }

    /// 官方调价就报出来，并且说清是哪一档的哪一项。只报「有漂移」不够——
    /// 看的人要能直接改配置。
    #[test]
    fn a_changed_output_price_is_named() {
        let book = book_with(json!([
            {"name": "off_peak", "cache_miss_micros": 1_000_000, "cache_hit_micros": 20_000,
             "output_micros": 5_000_000},
            {"name": "peak", "cache_miss_micros": 2_000_000, "cache_hit_micros": 40_000,
             "output_micros": 8_000_000,
             "windows": [
                 {"utc_start_minute": 60, "utc_end_minute": 240, "weekdays": [1, 2, 3, 4, 5]},
                 {"utc_start_minute": 360, "utc_end_minute": 600, "weekdays": [1, 2, 3, 4, 5]}
             ]}
        ]));
        let observed = parse_pricing_page(PAGE_2026_11_05).unwrap();
        let drift = compare(&observed, &book);
        assert_eq!(drift.len(), 1, "只有输出价那一项变了：{drift:?}");
        assert!(drift[0].what.contains("off_peak"));
        assert!(drift[0].what.contains("输出"));
    }

    /// 价格数字全对，但峰时窗口没限工作日：必须报。
    ///
    /// 这是最容易被漏掉的一种错，因为价目表上每一个数字都对得上。它错的是
    /// **什么时候**按那个价收：官方只在周一至周五收峰价，配置写成天天收，
    /// 每周多算两天半的钱，而账上看不出任何异常。
    #[test]
    fn a_peak_tier_that_forgets_the_weekday_limit_is_named() {
        let book = book_with(json!([
            {"name": "off_peak", "cache_miss_micros": 1_000_000, "cache_hit_micros": 20_000,
             "output_micros": 4_000_000},
            {"name": "peak", "cache_miss_micros": 2_000_000, "cache_hit_micros": 40_000,
             "output_micros": 8_000_000,
             "windows": [
                 {"utc_start_minute": 60, "utc_end_minute": 240},
                 {"utc_start_minute": 360, "utc_end_minute": 600}
             ]}
        ]));
        let observed = parse_pricing_page(PAGE_2026_11_05).unwrap();
        let drift = compare(&observed, &book);
        assert_eq!(drift.len(), 1, "只有窗口形状这一项不对：{drift:?}");
        assert!(drift[0].what.contains("工作日"), "{drift:?}");
    }

    /// 峰时时刻按北京时间写在页面上，配置里存的是 UTC。少一个区间、或者换算
    /// 差几个小时，都要报——每一个数字都对、只是时段写错，是另一种算错钱。
    #[test]
    fn peak_hours_are_compared_after_converting_beijing_time_to_utc() {
        let observed = parse_pricing_page(PAGE_2026_11_05).unwrap();
        assert_eq!(
            observed.peak_windows,
            vec![(60, 240), (360, 600)],
            "北京时间 9:00-12:00、14:00-18:00 换成 UTC 是 01:00-04:00、06:00-10:00"
        );
        assert!(observed.peak_weekdays_only);

        // 只写了上午那一档。
        let short = book_with(json!([
            {"name": "off_peak", "cache_miss_micros": 1_000_000, "cache_hit_micros": 20_000,
             "output_micros": 4_000_000},
            {"name": "peak", "cache_miss_micros": 2_000_000, "cache_hit_micros": 40_000,
             "output_micros": 8_000_000,
             "windows": [{"utc_start_minute": 60, "utc_end_minute": 240, "weekdays": [1, 2, 3, 4, 5]}]}
        ]));
        let drift = compare(&observed, &short);
        assert_eq!(drift.len(), 1, "{drift:?}");
        assert!(drift[0].what.contains("时段"), "{drift:?}");
    }

    /// 配置只有一档（不分峰谷）时，对账必须报——它按一个价算两段，白天的钱
    /// 永远比账单少一半。这一条不是「配置风格不同」，是算错钱。
    #[test]
    fn a_single_tier_book_drifts_against_a_two_tier_page() {
        let book = book_with(json!([
            {"name": "standard", "cache_miss_micros": 1_000_000, "cache_hit_micros": 20_000,
             "output_micros": 4_000_000}
        ]));
        let observed = parse_pricing_page(PAGE_2026_11_05).unwrap();
        let drift = compare(&observed, &book);
        assert_eq!(drift.len(), 1);
        assert!(drift[0].what.contains("没有分峰谷"));
    }
}
