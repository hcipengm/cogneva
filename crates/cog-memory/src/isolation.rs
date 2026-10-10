//! 评测/基准数据的准入排除——这份数据不进记忆的任何一层。
//!
//! 为什么是「拒收」而不是「先存后删」：拒收发生在归档与抽取**之前**，所以
//! 那一份载荷既不会落进 raw 层，也不会被送进抽取器（真抽取器是一次模型调用，
//! 送出去的内容已经离开了本系统）。事后清理只能减小残留在盘上的量，清理本身
//! 也是一次读，且「清干净了没有」要靠一次次证明——而这里要的是一条不成立的
//! 路径，不是一条清理得及时的通路。
//!
//! 三条规则各自有输入，因为它们各自覆盖一种投递方式：
//!
//! - 命名空间：评测数据有专用桶时，桶名由部署给出，写侧不必做任何事。
//! - 标签：写侧（provision 任务）知道这条 raw 属于评测集，却可能写进共享
//!   命名空间——标签是那种情形下唯一还在的声明。
//! - 载荷标记：前两条都要求写侧**带着牌子**来；一条经过普通任务链路、落在
//!   默认命名空间的载荷两条都不会命中。评测集自带的标记串（canary）随数据
//!   本身走，所以它是唯一能覆盖「牌子丢了」的规则。
//!
//! `canary` 在本仓库另有含义（金丝雀部署，指一小股流量的新版本），这里指的是
//! 数据集自带的标记串。配置键带 `benchmark_` 前缀就是为了不让两者在同一个
//! 词上撞车。

use cog_core::RawSource;

/// 一条 raw 被哪条规则挡下。
///
/// 分成三种而不是一个布尔：三条规则的修法不同（改桶名/补标签/看载荷），
/// 合成一个判词会抹掉「是哪一条在说」。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkRefusal {
    /// 命名空间是评测数据专用桶。
    Namespace,
    /// raw 上带着 [`cog_core::RAW_TAG_BENCHMARK`] 一族的标签。
    Tag,
    /// 载荷里出现评测数据集自带的标记串。
    Canary,
}

/// 被命名空间规则挡下的次数。
pub const BENCHMARK_NAMESPACE_REFUSED_OPERATION: &str = "benchmark_namespace_refused";
/// 被标签规则挡下的次数。
pub const BENCHMARK_TAG_REFUSED_OPERATION: &str = "benchmark_tag_refused";
/// 被载荷标记规则挡下的次数。
pub const BENCHMARK_CANARY_REFUSED_OPERATION: &str = "benchmark_canary_refused";

impl BenchmarkRefusal {
    /// 这次拒收记在 `memory_operations_total` 的哪一格上。
    ///
    /// 与 `bus_claim`/`dlq_written` 一族同一个指标名：这是摄取这条路上的一次
    /// 具名结果，新开指标名要为一次「这一拍做了什么」付告警规则普查的代价。
    pub const fn operation(self) -> &'static str {
        match self {
            Self::Namespace => BENCHMARK_NAMESPACE_REFUSED_OPERATION,
            Self::Tag => BENCHMARK_TAG_REFUSED_OPERATION,
            Self::Canary => BENCHMARK_CANARY_REFUSED_OPERATION,
        }
    }

    /// 日志里点名的规则。写清是哪一条，否则看日志的人只能挨个试。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Namespace => "namespace is an eval-dedicated bucket",
            Self::Tag => "raw carries the benchmark tag",
            Self::Canary => "payload carries a benchmark canary marker",
        }
    }
}

/// 排除面：命名空间与载荷标记两张表，加上标签那一条固定规则。
///
/// 两张表都由部署给出（`memory.ingest` 段），代码里不写任何具体的桶名或标记
/// 串：它们是部署事实与数据集事实，写死在代码里等于让两者只能靠改代码对齐。
/// 两张表为空时这份隔离是**惰性的**——标签规则仍然生效，另外两条不生效。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BenchmarkIsolation {
    namespaces: Vec<String>,
    markers: Vec<String>,
}

impl BenchmarkIsolation {
    pub fn new(namespaces: Vec<String>, markers: Vec<String>) -> Self {
        Self {
            namespaces,
            markers,
        }
    }

    /// 配了哪些桶名（生效面读数用）。
    pub fn namespaces(&self) -> &[String] {
        &self.namespaces
    }

    /// 配了哪些载荷标记（生效面读数用）。
    pub fn markers(&self) -> &[String] {
        &self.markers
    }

    /// 生效面的那句话，装配处各打一次。
    ///
    /// 「这一趟没配桶名与标记串」必须是一句可读的事实而不是要靠推断的状态：
    /// 两张表都空时，这条通路看起来与「守住了」完全一样，而它其实一条规则
    /// 都没在判（标签那一条除外）——把惰性写进启动日志，读日志的人不必去猜
    /// 这份部署到底有没有排除面。
    pub fn describe(&self) -> String {
        format!(
            "{} eval-bucket namespace(s), {} canary marker(s); the benchmark tag rule is unconditional",
            self.namespaces.len(),
            self.markers.len()
        )
    }

    /// 这张 raw 该不该被拒收，及拒在谁手上。
    ///
    /// 顺序是固定且要写明的：桶名 → 标签 → 载荷标记。两条规则同时命中时记的
    /// 是**先判的那一条**，所以顺序也是「读者先该去看哪一处」的顺序。桶名与
    /// 标签都在归档前就能判，载荷标记要扫一遍字节，最贵，放最后；两条命中时
    /// 记前者也少一次扫描。
    pub fn refusal(&self, raw: &RawSource) -> Option<BenchmarkRefusal> {
        if self.namespaces.iter().any(|ns| ns == &raw.namespace) {
            return Some(BenchmarkRefusal::Namespace);
        }
        if raw
            .tags
            .iter()
            .any(|tag| cog_core::tag_declares_benchmark(tag))
        {
            return Some(BenchmarkRefusal::Tag);
        }
        if self
            .markers
            .iter()
            .any(|marker| !marker.is_empty() && payload_contains(&raw.payload, marker.as_bytes()))
        {
            return Some(BenchmarkRefusal::Canary);
        }
        None
    }
}

/// 按字节找子串，不先做 `String::from_utf8_lossy`：载荷是任意字节，一次转
/// 换会把整份载荷复制一遍，而这只是为了一次查找。空 needle 会 panic，调用方
/// 已在上面挡掉——空标记串匹配一切，那不叫标记。
fn payload_contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(namespace: &str, tags: &[&str], payload: &[u8]) -> RawSource {
        RawSource::new("raw-1", namespace, "text/plain", payload.to_vec())
            .with_tags(tags.iter().map(|t| t.to_string()).collect())
    }

    /// 桶名相等即拒，不需要写侧做任何事——这条是「评测数据放专用桶」唯一
    /// 依赖的机制。
    #[test]
    fn an_eval_bucket_is_refused_by_namespace() {
        let isolation = BenchmarkIsolation::new(vec!["benchmark".into()], Vec::new());
        assert_eq!(
            isolation.refusal(&raw("benchmark", &[], b"anything")),
            Some(BenchmarkRefusal::Namespace)
        );
        assert_eq!(isolation.refusal(&raw("default", &[], b"anything")), None);
    }

    /// 桶名不相等就不算命中：前缀相同、大小写不同都各自是另一个桶，
    /// 拿「像不像」当判据会把同名的非评测桶一起挡掉。
    #[test]
    fn only_an_exact_namespace_counts_as_the_bucket() {
        let isolation = BenchmarkIsolation::new(vec!["benchmark".into()], Vec::new());
        assert_eq!(isolation.refusal(&raw("benchmark-hle", &[], b"x")), None);
        assert_eq!(isolation.refusal(&raw("Benchmark", &[], b"x")), None);
    }

    /// 标签规则不依赖配置：写侧带了牌子就拒，桶名没配也一样。
    #[test]
    fn the_tag_rule_stands_without_any_configured_bucket() {
        let isolation = BenchmarkIsolation::default();
        assert_eq!(
            isolation.refusal(&raw("default", &[cog_core::RAW_TAG_BENCHMARK], b"x")),
            Some(BenchmarkRefusal::Tag)
        );
        assert_eq!(
            isolation.refusal(&raw("default", &["benchmark=1"], b"x")),
            Some(BenchmarkRefusal::Tag)
        );
        assert_eq!(
            isolation.refusal(&raw("default", &["benchmark"], b"x")),
            Some(BenchmarkRefusal::Tag)
        );
        assert_eq!(
            isolation.refusal(&raw("default", &["custom_agent:benchmark"], b"x")),
            None,
            "a tag that merely contains the word is not a declaration"
        );
    }

    /// 显式否定不是声明：`benchmark=false` 说的是相反的事，把它当命中会让一条
    /// 明确标注过的正常记忆被丢。
    #[test]
    fn an_explicit_negative_tag_is_not_a_declaration() {
        assert!(!cog_core::tag_declares_benchmark("benchmark=false"));
        assert!(!cog_core::tag_declares_benchmark("benchmark=0"));
        assert!(!cog_core::tag_declares_benchmark(" benchmark = no "));
        assert_eq!(
            BenchmarkIsolation::default().refusal(&raw("default", &["benchmark=false"], b"x")),
            None
        );
    }

    /// 载荷规则要能命中不落在切分边界上的标记，也要能吃下非 UTF-8 的载荷——
    /// 工具输出、抓回来的页面都不是文本。
    #[test]
    fn a_marker_inside_a_non_utf8_payload_is_found() {
        let isolation =
            BenchmarkIsolation::new(Vec::new(), vec!["CANARY-26b5c67b".into(), "".into()]);
        let mut payload = vec![0xff, 0xfe, 0x00, b'x'];
        payload.extend_from_slice(b"prefix CANARY-26b5c67b suffix");
        assert_eq!(
            isolation.refusal(&raw("default", &[], &payload)),
            Some(BenchmarkRefusal::Canary)
        );
        assert_eq!(
            isolation.refusal(&raw("default", &[], b"CANARY-26b5c67")),
            None,
            "a prefix of the marker is not the marker"
        );
    }

    /// 两条规则同时命中时记先判的那条，且顺序是固定的。合成一条会让「桶名配错
    /// 了」和「载荷里真有标记」读起来一样。
    #[test]
    fn the_reading_names_the_first_rule_that_matched() {
        let isolation = BenchmarkIsolation::new(vec!["benchmark".into()], vec!["CANARY".into()]);
        assert_eq!(
            isolation.refusal(&raw("benchmark", &[cog_core::RAW_TAG_BENCHMARK], b"CANARY")),
            Some(BenchmarkRefusal::Namespace)
        );
        assert_eq!(
            isolation.refusal(&raw("default", &[cog_core::RAW_TAG_BENCHMARK], b"CANARY")),
            Some(BenchmarkRefusal::Tag)
        );
    }

    /// 空配置不是「匹配一切」：惰性隔离必须放过普通载荷，否则一次没配好的
    /// 部署会把全部记忆挡在门外。
    #[test]
    fn an_unconfigured_isolation_refuses_nothing() {
        let isolation = BenchmarkIsolation::default();
        assert!(isolation.namespaces().is_empty());
        assert!(isolation.markers().is_empty());
        assert_eq!(
            isolation.refusal(&raw("default", &["agent_id:x"], b"ordinary text")),
            None
        );
    }

    /// 三条规则各自的读数格子互不相同。
    #[test]
    fn each_rule_owns_its_own_reading_cell() {
        let cells = [
            BenchmarkRefusal::Namespace.operation(),
            BenchmarkRefusal::Tag.operation(),
            BenchmarkRefusal::Canary.operation(),
        ];
        let unique: std::collections::HashSet<_> = cells.iter().collect();
        assert_eq!(unique.len(), cells.len());
    }
}
