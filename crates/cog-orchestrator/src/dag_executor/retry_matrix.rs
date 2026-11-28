use cog_core::TaskType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Backoff strategy for retries.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries.
    Fixed { interval_ms: u64 },
    /// Linear backoff: delay = base + increment * attempt.
    Linear { base_ms: u64, increment_ms: u64 },
    /// Exponential backoff: delay = min(base * multiplier^attempt, max).
    Exponential {
        base_ms: u64,
        max_ms: u64,
        multiplier: f64,
    },
}

impl BackoffStrategy {
    /// Compute the delay for a given retry attempt (0-indexed).
    pub fn delay(&self, attempt: u32) -> Duration {
        match *self {
            BackoffStrategy::Fixed { interval_ms } => Duration::from_millis(interval_ms),
            BackoffStrategy::Linear {
                base_ms,
                increment_ms,
            } => Duration::from_millis(base_ms.saturating_add(increment_ms * attempt as u64)),
            BackoffStrategy::Exponential {
                base_ms,
                max_ms,
                multiplier,
            } => {
                let delay =
                    (base_ms as f64 * multiplier.powi(attempt as i32)).min(max_ms as f64) as u64;
                Duration::from_millis(delay)
            }
        }
    }
}

/// 指数退避的倍增：每一档是前一档的两倍。
///
/// 这不是挑出来的数，是指数退避的定义本身——把它写成 `2.0` 只是把这个定义
/// 抄进了每一处表项。
// cap: fixed — 倍增：指数退避的定义，不是这台机器量的
// not-a-bound: 倍增：指数退避的定义，不是这台机器量的
pub const DOUBLING: f64 = 2.0;

/// Circuit breaker configuration per task type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Duration the circuit stays open before transitioning to half-open.
    pub recovery_timeout_ms: u64,
    /// Max calls allowed in half-open state.
    pub half_open_max_calls: u32,
}

// 未分类的任务（四个角色 Planner/Generator/Evaluator/Reviewer 与 `Custom`）
// 没有专门的档，走这两个 `Default`。它们的失败多是上游抖动而不是活本身坏了，
// 所以熔断比节点档宽：多两次失败才判死。
// cap: fixed — 连续五次失败才开熔断：角色失败多是上游抖动，三次判死会把它锁在 DLQ
// keep: 删掉后三次判死把一个上游抖动的角色锁在 DLQ；由「连续五次才开熔断」兜住
const UNCLASSIFIED_FAILURE_THRESHOLD: u32 = 5;
// cap: fixed — 半开放三次探测：一次分不清抖动与真坏，三次全失败才判死
// keep: 删掉后一次探测分不清抖动与真坏；由「三次全失败才判死」兜住
const UNCLASSIFIED_HALF_OPEN_CALLS: u32 = 3;
// 三次重试，与节点档同一个次数；等待更长（1s 起步、30s 封顶）是因为不知道失败在哪。
// 这个数只在一处定义：任务行上那一列的初值也是它（`cog_core::types::task::DEFAULT_MAX_RETRIES`），
// 判决与落库读同一个名字，改一边不会悄悄让另一边说别的。
const UNCLASSIFIED_RETRIES: u32 = cog_core::types::task::DEFAULT_MAX_RETRIES;

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: UNCLASSIFIED_FAILURE_THRESHOLD,
            recovery_timeout_ms: (30 * cog_core::day::MILLIS_PER_SECOND) as u64,
            half_open_max_calls: UNCLASSIFIED_HALF_OPEN_CALLS,
        }
    }
}

/// Retry configuration for a specific task type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub backoff: BackoffStrategy,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: UNCLASSIFIED_RETRIES,
            backoff: BackoffStrategy::Exponential {
                base_ms: cog_core::day::MILLIS_PER_SECOND as u64,
                max_ms: (30 * cog_core::day::MILLIS_PER_SECOND) as u64,
                multiplier: DOUBLING,
            },
            circuit_breaker: Some(CircuitBreakerConfig::default()),
        }
    }
}

/// Per-task-type retry policy matrix.
/// Provides differentiated retry + backoff + circuit-breaker policies
/// based on the operational type of the task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryMatrix {
    default: RetryConfig,
    by_task_type: HashMap<TaskType, RetryConfig>,
}

impl Default for RetryMatrix {
    fn default() -> Self {
        Self::defaults()
    }
}

/// 出厂策略表里的每个数都有名字。
///
/// 这张表以前把数字直接写在结构体字面量里，而它既不是 `impl Default` 也不是
/// `fn default_*`：D5 那道门看不见它，容量登记表只认 `const` 也看不见它。
/// 形状换了，数还是数——名字给出来之后，每个数至少有一条判词和一个能拦住
/// 「只能往下调」的簿记。
// LLM 调用：一次调用里重试三次
// cap: fixed — 退避三档 1s/2s/4s；三次之后每一次都只是把同一个失败拖得更久
// keep: 删掉后重试无界，同一个失败被拖得越来越久；由「三次之后每一次都只是拖」兜住
const LLM_CALL_RETRIES: u32 = 3;
// cap: fixed — 连续五次失败才开熔断：三次重试各自失败都算在内，第五次才说明这条路真不通
// keep: 删掉后三次重试都失败还不开熔断，这条路真不通时每轮都去撞；由「第五次才说明真不通」兜住
const LLM_CALL_FAILURE_THRESHOLD: u32 = 5;
// cap: fixed — 半开时放三次探测：一次分不清抖动与真坏，三次仍全失败才判死
// keep: 删掉后半开只放一次，抖动与真坏分不开；由「三次仍全失败才判死」兜住
const LLM_CALL_HALF_OPEN_CALLS: u32 = 3;

// 工具调用：外部服务抖动，等一等再试
// cap: fixed — 工具失败多是抖动：两次重试足够越过一次瞬时故障
// keep: 删掉后一次瞬时故障就让工具失败；由「两次重试越过一次瞬时故障」兜住
const TOOL_CALL_RETRIES: u32 = 2;
// cap: fixed — 连续三次失败开熔断：比 LLM 调用更早停，因为工具重试本身是等待而不是推理
// keep: 删掉后工具的路真断时还每轮去等，把等待当成推理；由「比 LLM 更早停」兜住
const TOOL_CALL_FAILURE_THRESHOLD: u32 = 3;
// cap: fixed — 半开放两次探测
// keep: 删掉后半开只放一次，抖动与真坏分不开；由「半开放两次探测」兜住
const TOOL_CALL_HALF_OPEN_CALLS: u32 = 2;

// 文件操作：立刻重试一次
// cap: fixed — 本地文件失败再等也不会变，只给一次不留等待的重试
// keep: 删掉后本地文件失败连一次重试都没有；由「再等也不会变」兜住
const FILE_OP_RETRIES: u32 = 1;

// 数据库事务：线性退避
// cap: fixed — 连接池争用是秒级的事：三次、每次加一百毫秒，等太久只是让锁持有更久
// keep: 删掉后锁争用被无限重试，锁持有被拖得更久；由「三次、每次加一百毫秒」兜住
const DB_RETRIES: u32 = 3;
// cap: fixed — 十次才开熔断：事务失败要区分「这次撞上锁」与「这条 SQL 坏了」，前者会自己好
// keep: 删掉后撞锁与 SQL 坏了分不开，前者被当成后者进熔断；由「十次才开熔断」兜住
const DB_FAILURE_THRESHOLD: u32 = 10;
// cap: fixed — 半开放三次探测
// keep: 删掉后半开只放一次，抖动与真坏分不开；由「半开放三次探测」兜住
const DB_HALF_OPEN_CALLS: u32 = 3;

// 网络请求：对端过载时第一次重试就该更慢
// cap: fixed — 三次：0.5s/1s/2s，超过两秒仍被拒就不是过载而是不可达
// keep: 删掉后不可达被当成过载无限重试；由「超过两秒仍被拒就是不可达」兜住
const NETWORK_RETRIES: u32 = 3;
// cap: fixed — 连续五次失败开熔断
// keep: 删掉后网络真不可达时无限撞；由「连续五次失败开熔断」兜住
const NETWORK_FAILURE_THRESHOLD: u32 = 5;
// cap: fixed — 半开放三次探测
// keep: 删掉后网络真坏时分不清抖动与真坏，半开探测永远放出错的请求；由「三次仍全失败才判死」兜住
const NETWORK_HALF_OPEN_CALLS: u32 = 3;

// WASM 技能：沙箱内的失败多是初始化竞态
// cap: fixed — 两次：竞态在一次重试之内就过去，第三次开始只是在等一个坏掉的初始化
// keep: 删掉后竞态过去之后还在等一个坏掉的初始化；由「竞态在一次重试之内过去」兜住
const WASM_SKILL_RETRIES: u32 = 2;

// 技能：立刻重试一次
// cap: fixed — 一次不留等待的重试：技能本身失败再等也不会变
// keep: 删掉后技能本身失败被无限重试；由「再等也不会变」兜住
const SKILL_RETRIES: u32 = 1;

// DAG 节点：同一节点连败到熔断阈值即进 DLQ
// cap: fixed — 连续三次失败即进 DLQ
// keep: 删掉后节点一直坏却一直调度；由「连续三次失败即进 DLQ」兜住
const DAG_NODE_FAILURE_THRESHOLD: u32 = 3;
// cap: fixed — 半开只放一次探测：节点一坏往往就是一直坏，多放几次只是多花几次额度
// keep: 删掉后多放几次只是多花几次额度；由「节点一坏往往一直坏」兜住
const DAG_NODE_HALF_OPEN_CALLS: u32 = 1;

impl RetryMatrix {
    /// Build a [`RetryMatrix`] pre-populated with the standard
    pub fn defaults() -> Self {
        let mut by_task_type = HashMap::new();

        // LLM 调用：指数退避，熔断了就不必再试——次数、等待与熔断阈值都写在下面这一项里
        by_task_type.insert(
            TaskType::LlmCall,
            RetryConfig {
                max_retries: LLM_CALL_RETRIES,
                backoff: BackoffStrategy::Exponential {
                    base_ms: cog_core::day::MILLIS_PER_SECOND as u64,
                    max_ms: (8 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    multiplier: DOUBLING,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: LLM_CALL_FAILURE_THRESHOLD,
                    recovery_timeout_ms: (60 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    half_open_max_calls: LLM_CALL_HALF_OPEN_CALLS,
                }),
            },
        );

        // 工具调用：固定间隔；工具失败多是外部服务抖动，等一等再试
        by_task_type.insert(
            TaskType::ToolCall,
            RetryConfig {
                max_retries: TOOL_CALL_RETRIES,
                backoff: BackoffStrategy::Fixed {
                    interval_ms: (3 * cog_core::day::MILLIS_PER_SECOND) as u64,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: TOOL_CALL_FAILURE_THRESHOLD,
                    recovery_timeout_ms: (30 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    half_open_max_calls: TOOL_CALL_HALF_OPEN_CALLS,
                }),
            },
        );

        // 文件操作：立刻重试一次；本地文件失败再等也不会变，不设熔断
        by_task_type.insert(
            TaskType::FileOp,
            RetryConfig {
                max_retries: FILE_OP_RETRIES,
                backoff: BackoffStrategy::Fixed { interval_ms: 0 },
                circuit_breaker: None,
            },
        );

        // 数据库事务：线性退避；连接池争用是秒级的事，等太久只是让锁持有更久
        by_task_type.insert(
            TaskType::DbTransaction,
            RetryConfig {
                max_retries: DB_RETRIES,
                backoff: BackoffStrategy::Linear {
                    base_ms: (cog_core::day::MILLIS_PER_SECOND / 10) as u64,
                    increment_ms: (cog_core::day::MILLIS_PER_SECOND / 10) as u64,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: DB_FAILURE_THRESHOLD,
                    recovery_timeout_ms: (120 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    half_open_max_calls: DB_HALF_OPEN_CALLS,
                }),
            },
        );

        // 网络请求：指数退避；对端过载时，第一次重试就该比上一次慢一倍
        by_task_type.insert(
            TaskType::NetworkRequest,
            RetryConfig {
                max_retries: NETWORK_RETRIES,
                backoff: BackoffStrategy::Exponential {
                    base_ms: (cog_core::day::MILLIS_PER_SECOND / 2) as u64,
                    max_ms: (2 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    multiplier: DOUBLING,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: NETWORK_FAILURE_THRESHOLD,
                    recovery_timeout_ms: (30 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    half_open_max_calls: NETWORK_HALF_OPEN_CALLS,
                }),
            },
        );

        // WASM 技能：固定间隔一次；沙箱内的失败多是初始化竞态
        by_task_type.insert(
            TaskType::WasmSkill,
            RetryConfig {
                max_retries: WASM_SKILL_RETRIES,
                backoff: BackoffStrategy::Fixed {
                    interval_ms: cog_core::day::MILLIS_PER_SECOND as u64,
                },
                circuit_breaker: None,
            },
        );

        // 技能：立刻重试一次，不设熔断
        by_task_type.insert(
            TaskType::Skill,
            RetryConfig {
                max_retries: SKILL_RETRIES,
                backoff: BackoffStrategy::Fixed { interval_ms: 0 },
                circuit_breaker: None,
            },
        );

        // DAG 节点：指数退避，等待从秒级起步；同一节点连败到熔断阈值即进 DLQ
        //
        // 次数读执行器自己那份声明（`dag_executor.max_retries`）的出厂值：
        // 部署声明了这个数，这里再写一个就是同一件事的第二个说法，而且写的
        // 那个永远不生效——真正跑的执行器用部署的值，这里只补没人声明时的
        // 兜底。
        by_task_type.insert(
            TaskType::DagNode,
            RetryConfig {
                max_retries: cog_core::DagExecutorConfig::default().max_retries,
                backoff: BackoffStrategy::Exponential {
                    base_ms: (5 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    max_ms: (20 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    multiplier: DOUBLING,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: DAG_NODE_FAILURE_THRESHOLD,
                    recovery_timeout_ms: (30 * cog_core::day::MILLIS_PER_SECOND) as u64,
                    half_open_max_calls: DAG_NODE_HALF_OPEN_CALLS,
                }),
            },
        );

        Self {
            default: RetryConfig::default(),
            by_task_type,
        }
    }

    /// Look up the retry configuration for a given task type.
    pub fn lookup(&self, task_type: &TaskType) -> &RetryConfig {
        self.by_task_type.get(task_type).unwrap_or(&self.default)
    }

    /// Register or override a retry configuration for a task type.
    pub fn set(&mut self, task_type: TaskType, config: RetryConfig) {
        self.by_task_type.insert(task_type, config);
    }

    /// Compute the backoff delay for a given task type and attempt.
    pub fn delay(&self, task_type: &TaskType, attempt: u32) -> Duration {
        self.lookup(task_type).backoff.delay(attempt)
    }

    /// The wait before the next attempt, raised to the wait the upstream named
    /// when it named a longer one.
    ///
    /// The policy delay is derived from our own guess at how long a refusal
    /// lasts; a stated wait is the upstream measuring its own recovery. When
    /// the measurement is longer, retrying at the guessed moment just buys
    /// another refusal, so the measurement governs. When it is shorter — or
    /// absent — the policy stays in charge: an upstream asking for less than
    /// our policy does not license us to retry harder than we otherwise would.
    pub fn delay_with_hint(
        &self,
        task_type: &TaskType,
        attempt: u32,
        upstream_hint: Option<u64>,
    ) -> Duration {
        let policy = self.delay(task_type, attempt);
        match upstream_hint {
            Some(secs) => policy.max(Duration::from_secs(secs)),
            None => policy,
        }
    }

    /// Get the max retries for a given task type.
    pub fn max_retries(&self, task_type: &TaskType) -> u32 {
        self.lookup(task_type).max_retries
    }

    /// Get the circuit breaker config for a given task type (if any).
    pub fn circuit_breaker(&self, task_type: &TaskType) -> Option<&CircuitBreakerConfig> {
        self.lookup(task_type).circuit_breaker.as_ref()
    }

    /// Check if a task type has a circuit breaker configured.
    pub fn has_circuit_breaker(&self, task_type: &TaskType) -> bool {
        self.lookup(task_type).circuit_breaker.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_matrix_defaults_llm_call() {
        let matrix = RetryMatrix::defaults();
        let cfg = matrix.lookup(&TaskType::LlmCall);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.backoff.delay(0).as_secs(), 1);
        assert_eq!(cfg.backoff.delay(1).as_secs(), 2);
        assert_eq!(cfg.backoff.delay(2).as_secs(), 4);
        assert_eq!(cfg.backoff.delay(10).as_secs(), 8); // capped
        assert!(cfg.circuit_breaker.is_some());
        let cb = cfg.circuit_breaker.unwrap();
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.recovery_timeout_ms, 60000);
    }

    #[test]
    fn test_retry_matrix_defaults_db_transaction() {
        let matrix = RetryMatrix::defaults();
        let cfg = matrix.lookup(&TaskType::DbTransaction);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.backoff.delay(0).as_millis(), 100);
        assert_eq!(cfg.backoff.delay(1).as_millis(), 200);
        assert_eq!(cfg.backoff.delay(2).as_millis(), 300);
        assert_eq!(cfg.backoff.delay(5).as_millis(), 600); // linear, no cap needed
        assert!(cfg.circuit_breaker.is_some());
    }

    #[test]
    fn test_retry_matrix_defaults_file_op() {
        let matrix = RetryMatrix::defaults();
        let cfg = matrix.lookup(&TaskType::FileOp);
        assert_eq!(cfg.max_retries, 1);
        assert_eq!(cfg.backoff.delay(0).as_millis(), 0);
        assert!(cfg.circuit_breaker.is_none());
    }

    #[test]
    fn test_retry_matrix_lookup_unknown_uses_default() {
        let matrix = RetryMatrix::defaults();
        // Planner is not explicitly configured in defaults → falls back
        let cfg = matrix.lookup(&TaskType::Planner);
        assert_eq!(cfg, &RetryConfig::default());
    }

    /// 未分类任务那一档的三个数各只有一处声明：两个 `Default` 读常量，
    /// 常量上面写着它由什么钉住。
    ///
    /// 两面一起钉：运行时默认值等于常量（改常量就改默认值），源码面两个
    /// `impl Default` 的字段里不再有数字（下一笔把数写回裸字面量就红）。
    #[test]
    fn the_unclassified_defaults_read_their_names() {
        let breaker = CircuitBreakerConfig::default();
        assert_eq!(breaker.failure_threshold, UNCLASSIFIED_FAILURE_THRESHOLD);
        assert_eq!(breaker.half_open_max_calls, UNCLASSIFIED_HALF_OPEN_CALLS);
        assert_eq!(RetryConfig::default().max_retries, UNCLASSIFIED_RETRIES);

        let source = include_str!("retry_matrix.rs");
        let mut bare: Vec<&str> = Vec::new();
        for head in [
            "impl Default for CircuitBreakerConfig",
            "impl Default for RetryConfig",
        ] {
            let body = source
                .split(head)
                .nth(1)
                .expect("the impl is in this file")
                .split("\n}\n")
                .next()
                .unwrap();
            bare.extend(body.lines().filter(|line| {
                let line = line.trim();
                let Some((field, value)) = line.split_once(':') else {
                    return false;
                };
                let named_field = !field.is_empty()
                    && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                named_field && value.trim().starts_with(|c: char| c.is_ascii_digit())
            }));
        }
        assert!(bare.is_empty(), "默认值要读具名常量：{bare:?}");
    }

    #[test]
    fn test_retry_matrix_set_override() {
        let mut matrix = RetryMatrix::defaults();
        let custom = RetryConfig {
            max_retries: 5,
            backoff: BackoffStrategy::Fixed { interval_ms: 500 },
            circuit_breaker: None,
        };
        matrix.set(TaskType::Planner, custom.clone());
        let cfg = matrix.lookup(&TaskType::Planner);
        assert_eq!(cfg.max_retries, 5);
    }

    #[test]
    fn test_retry_matrix_delay_wrapper() {
        let matrix = RetryMatrix::defaults();
        assert_eq!(matrix.delay(&TaskType::LlmCall, 1).as_secs(), 2);
        assert_eq!(matrix.max_retries(&TaskType::ToolCall), 2);
    }

    /// 上游明说的等待只在**比策略更长**时接管；更短或没说都由策略说了算。
    /// 让它接管更短的一档，等于上游要求放慢而我们加速。
    #[test]
    fn a_stated_wait_governs_only_when_it_outlasts_the_policy() {
        let matrix = RetryMatrix::defaults();

        // LlmCall 第 1 次退避是 2s。
        assert_eq!(
            matrix
                .delay_with_hint(&TaskType::LlmCall, 1, None)
                .as_secs(),
            2
        );
        assert_eq!(
            matrix
                .delay_with_hint(&TaskType::LlmCall, 1, Some(1))
                .as_secs(),
            2,
            "上游要的更短，不缩短我们的退避"
        );
        assert_eq!(
            matrix
                .delay_with_hint(&TaskType::LlmCall, 1, Some(2))
                .as_secs(),
            2
        );
        assert_eq!(
            matrix
                .delay_with_hint(&TaskType::LlmCall, 1, Some(90))
                .as_secs(),
            90,
            "上游要的更久，等满它说的时长"
        );

        // 策略退避有封顶，明说的等待不受封顶约束：封顶是我们的猜测，明说的
        // 是上游的测量。
        let capped = matrix.delay(&TaskType::DagNode, 10);
        assert_eq!(capped.as_secs(), 20);
        assert_eq!(
            matrix
                .delay_with_hint(&TaskType::DagNode, 10, Some(120))
                .as_secs(),
            120
        );
    }

    /// 回归样例：2026-10-04 上游停供那三次尝试的时间戳链——18:24:28.08 →
    /// 18:24:33.17 → 18:24:37.94，两段间距 5.09s 与 4.77s。那几秒就是退避回落
    /// 成策略 1s/2s/4s 的指纹：网关当时自报的是 282 秒，而它在角色输出那一跳被
    /// 拍成了字符串（读回来的那半在 `cog_core::SFError::retry_after_secs` 的
    /// 回归测试里）。这条把两半钉在一起：同一个上游答案，现在算出来的等待是上游
    /// 自己说的那个数，不是策略的秒级退避。
    #[test]
    fn the_incidents_five_second_gaps_become_the_stated_wait() {
        let attempts = [
            "2026-10-04T18:24:28.080Z",
            "2026-10-04T18:24:33.170Z",
            "2026-10-04T18:24:37.940Z",
        ]
        .map(|t| {
            t.parse::<chrono::DateTime<chrono::Utc>>()
                .expect("a real timestamp")
        });

        for pair in attempts.windows(2) {
            let gap = pair[1] - pair[0];
            assert!(
                gap < chrono::Duration::seconds(60),
                "样例记的就是退避落到秒级策略时的间距，{gap}"
            );
        }

        let matrix = RetryMatrix::defaults();
        assert_eq!(
            matrix
                .delay_with_hint(&TaskType::LlmCall, 1, Some(282))
                .as_secs(),
            282,
            "上游明说的等待接管之后，同一次重试等的是 282 秒，不是策略的 2 秒"
        );
    }
}
