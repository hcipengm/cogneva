//! 通知出口的投递读数。
//!
//! 一个通知发出去了还是没发出去，从进程外面读是**同一个样子**：都没有人收到。
//! 出口配错了地址、被 NetworkPolicy 拦掉、平台以"关键词不匹配"这类自己的错误码
//! 拒收——三种成因在调用方那侧全都只表现为"连接器返回成功"（[`crate::MultiDispatcher`]
//! 过去把每个子出口的失败都吞掉返 `Ok(())`），于是告警发不出去这件事只能靠人去
//! 翻某一台容器的日志。
//!
//! 这里的读数就是补上这一面：每个出口每次投递的结果各计一次，
//! `result` 把三种成因分开——它们的原因与处置人不同（网络路径 / 接收方拒绝 /
//! 平台以报文里的错误码拒收），合并成一个标量会让最需要区分的那次失败变成
//! "投递失败"四个字。
//!
//! 读数按进程计数（与 [`cog_core::loop_health`] 同形），所以判据读的是**增量**
//! 而不是绝对值：一条"失败总数 > 0"的规则在第一次失败后会永远响下去，而这条
//! 路径是能自愈的（改地址、放行 egress 之后就该消停）。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use cog_core::{DimensionSpec, Observable, RawMetric, SFResult, TraceFragment};

/// 每次投递尝试落进的计数器，无论结果。
///
/// 两个标签：`outlet`（`webhook` / `dingtalk` / `feishu` / `wechat-work`）与
/// `result`（[`DeliveryResult`] 的四种）。标签名是**写死在渲染处的字面量**而不是
/// 常量：部署侧那条"规则摘要只许点名存在的标签"的门禁按 `.with_label("...")` 的
/// 字面量收集词汇表，写成常量它收集不到——一个拼错的标签名会一路走到读者面前变成
/// `{result}` 这样的空占位符，而列表里没有它就没人发现。
pub const DELIVERY_TOTAL: &str = "cogneva_notification_delivery_total";

/// 一次投递尝试的结果。
///
/// 四种取值对应四种不同的成因与处置，标签里只放这个分类，不放平台返回的自由
/// 文本：文本无界，而标签是身份。平台自己那句话进日志。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeliveryResult {
    /// 接收方收下了。
    Ok,
    /// 没有得到任何答复：连接失败、DNS、TLS、超时——包括被 egress 策略拦掉。
    Unreachable,
    /// 接收方答复了，但不是 2xx（鉴权、权限、限流、服务端故障）。
    HttpError,
    /// 接收方答复了 2xx，但**它自己的报文**说这次消息没被接受（机器人平台
    /// 的 `errcode` / `code` 非 0，例如"关键词不匹配"、"签名校验失败"）。
    /// 这一种最像"已送达"：HTTP 层全绿，而消息根本没进群。
    EnvelopeError,
}

impl DeliveryResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Unreachable => "unreachable",
            Self::HttpError => "http_error",
            Self::EnvelopeError => "envelope_error",
        }
    }
}

impl std::fmt::Display for DeliveryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 一次投递结果的全部取值。渲染时每个出口把四种都报出来（没发生的是 0），
/// 序列从第一次抓取就存在——判据读增量，而增量要有个起点。
pub const ALL_RESULTS: [DeliveryResult; 4] = [
    DeliveryResult::Ok,
    DeliveryResult::Unreachable,
    DeliveryResult::HttpError,
    DeliveryResult::EnvelopeError,
];

/// 本进程每个出口每种结果的累计次数。
///
/// 一个进程一份（[`registry`]），因为读数要与被读的投递动作同寿命：换成按插件
/// 各自持有的计数，同一个出口在两个插件里各记一份，判据读到的就是两份都不全
/// 的数。
#[derive(Debug, Default)]
pub struct DeliveryHealth {
    counts: Mutex<BTreeMap<(&'static str, DeliveryResult), u64>>,
}

impl DeliveryHealth {
    /// 记一次投递结果。
    pub fn record(&self, outlet: &'static str, result: DeliveryResult) {
        let mut counts = self.counts.lock().expect("delivery counter lock");
        *counts.entry((outlet, result)).or_insert(0) += 1;
    }

    /// 某个出口某个结果当前的计数，0 表示还没发生过。
    pub fn count(&self, outlet: &str, result: DeliveryResult) -> u64 {
        let counts = self.counts.lock().expect("delivery counter lock");
        counts.get(&(outlet, result)).copied().unwrap_or_default()
    }

    /// 渲染的出口集合：声明过的出口 ∪ 真记录过的出口。
    ///
    /// 并集而不是只按声明渲染，是因为"新加了出口而渲染侧抄的是旧清单"会让新
    /// 出口的读数永远不出现（产出侧加了东西，什么都不报错）——手写清单静默吃掉
    /// 新产出正是这条路径要修的缺陷本身。
    fn outlets(&self) -> Vec<&'static str> {
        let mut outlets: Vec<&'static str> = crate::plugin::outlet_names();
        {
            let counts = self.counts.lock().expect("delivery counter lock");
            for (outlet, _) in counts.keys() {
                if !outlets.contains(outlet) {
                    outlets.push(outlet);
                }
            }
        }
        outlets.sort_unstable();
        outlets.dedup();
        outlets
    }
}

/// 本进程的投递读数表。
pub fn registry() -> Arc<DeliveryHealth> {
    static REGISTRY: OnceLock<Arc<DeliveryHealth>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| Arc::new(DeliveryHealth::default()))
        .clone()
}

/// 记一次投递结果，落到本进程的表里。
pub fn record(outlet: &'static str, result: DeliveryResult) {
    registry().record(outlet, result);
}

/// 本进程读数表的可发布形态（插件在 `init` 里发布它）。
pub fn observable() -> Arc<dyn Observable> {
    let registry: Arc<DeliveryHealth> = registry();
    registry
}

#[async_trait::async_trait]
impl Observable for DeliveryHealth {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = Vec::new();
        for outlet in self.outlets() {
            for result in ALL_RESULTS {
                out.push(
                    RawMetric::new(DELIVERY_TOTAL, self.count(outlet, result) as f64)
                        .with_label("outlet", outlet)
                        .with_label("result", result.as_str()),
                );
            }
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// 出口是一个固定的小集合，读数不随维度变：采集侧采一次就够。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn metrics_of(health: &DeliveryHealth) -> Vec<(String, String, f64)> {
        health
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .map(|m| {
                let label = |key: &str| m.labels.get(key).cloned().unwrap_or_default();
                (label("outlet"), label("result"), m.value)
            })
            .collect()
    }

    /// 每个声明过的出口、每种结果都从第一次抓取起就有序列——哪怕一次都没发生。
    /// 判据读的是增量，增量要有起点；只报发生过的组合会让"这个出口从没投递过"
    /// 和"这个出口不存在"在读数上同形。
    #[tokio::test]
    async fn every_outlet_reports_every_result_before_anything_happens() {
        let health = DeliveryHealth::default();
        let metrics = metrics_of(&health).await;
        let declared = crate::plugin::outlet_names();
        assert_eq!(metrics.len(), declared.len() * ALL_RESULTS.len());
        for outlet in declared {
            for result in ALL_RESULTS {
                assert!(
                    metrics
                        .iter()
                        .any(|(o, r, v)| o == outlet && r == result.as_str() && *v == 0.0),
                    "{outlet}/{result} 没有零值序列: {metrics:?}"
                );
            }
        }
    }

    /// 结果分开计：同一个出口的三种失败互不覆盖，成功也不加进失败那一格。
    #[tokio::test]
    async fn results_are_counted_apart() {
        let health = DeliveryHealth::default();
        health.record("dingtalk", DeliveryResult::Ok);
        health.record("dingtalk", DeliveryResult::Unreachable);
        health.record("dingtalk", DeliveryResult::Unreachable);
        health.record("dingtalk", DeliveryResult::EnvelopeError);

        assert_eq!(health.count("dingtalk", DeliveryResult::Ok), 1);
        assert_eq!(health.count("dingtalk", DeliveryResult::Unreachable), 2);
        assert_eq!(health.count("dingtalk", DeliveryResult::HttpError), 0);
        assert_eq!(health.count("dingtalk", DeliveryResult::EnvelopeError), 1);

        let metrics = metrics_of(&health).await;
        let value = |outlet: &str, result: &str| {
            metrics
                .iter()
                .find(|(o, r, _)| o == outlet && r == result)
                .map(|(_, _, v)| *v)
                .unwrap_or_else(|| panic!("no series for {outlet}/{result}"))
        };
        assert_eq!(value("dingtalk", "unreachable"), 2.0);
        assert_eq!(value("dingtalk", "ok"), 1.0);
        assert_eq!(value("feishu", "unreachable"), 0.0);
    }

    /// 记录过一个没声明过的出口，它也得出现在读数里。漏掉它会得到一个"新出口
    /// 的失败在读数上不存在"的盲区，而那正是本轮要修的形态。
    #[tokio::test]
    async fn an_undeclared_outlet_still_gets_a_series() {
        let health = DeliveryHealth::default();
        health.record("some-new-outlet", DeliveryResult::HttpError);
        let metrics = metrics_of(&health).await;
        assert!(
            metrics
                .iter()
                .any(|(o, r, v)| o == "some-new-outlet" && r == "http_error" && *v == 1.0),
            "{metrics:?}"
        );
    }

    /// 结果标签的取值域是闭集，四种各一个拼写，不重不漏。
    #[test]
    fn the_result_label_domain_is_a_closed_set() {
        let mut seen: Vec<&str> = ALL_RESULTS.iter().map(|r| r.as_str()).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), ALL_RESULTS.len());
    }

    /// 渲染出来的序列只带那两个标签，名字就是规则摘要里用的那两个。改名要在这里
    /// 或部署侧那条摘要门禁上撞一次，不能等到告警消息里出现一个空占位符。
    #[tokio::test]
    async fn the_rendered_series_carry_the_two_named_labels() {
        let health = DeliveryHealth::default();
        health.record("dingtalk", DeliveryResult::HttpError);
        for metric in health.collect_metrics("").await.unwrap() {
            assert_eq!(metric.name, DELIVERY_TOTAL);
            let mut keys: Vec<&str> = metric.labels.keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(keys, vec!["outlet", "result"], "{metric:?}");
        }
    }
}
