use crate::contract::llm::UpstreamFailure;

/// 统一错误类型。参考 pi-ai 的契约：所有错误编码在流中，不直接抛出。
#[derive(Debug, thiserror::Error)]
pub enum SFError {
    /// LLM 服务没有服务这次调用：不可达、被拒（配额/鉴权）、超时，或没有返回
    /// 可用答复。
    ///
    /// 答复拿到了但解析不了、校验不过，是 [`Self::Serialization`] /
    /// [`Self::Validation`]，不是这个变体。两者混用会让"模型吐了垃圾"被读成
    /// "上游挂了"，下游于是把一条永远抽不出来的消息一直延后重投。
    #[error("LLM provider error: {0}")]
    LLM(String),

    /// 上游在协议层明确拒绝了这次调用，原因取自传输信号（HTTP 状态码）而不是
    /// 上游自由文本。与 [`Self::LLM`] 的区别是**原因有类型**：调用方可以据此
    /// 判断该延后重试、该按意图退避、还是该换钥匙，不必再去解析 message。
    #[error("LLM upstream refused ({cause}): {reason}")]
    Upstream {
        cause: UpstreamFailure,
        reason: String,
        /// The wait the upstream itself named, when it named one. Kept beside
        /// the cause rather than inside it: the cause is a closed enum every
        /// consumer matches exhaustively, and a stated wait is an optional
        /// measurement, not a seventh kind of refusal. `None` means the
        /// upstream said nothing, and the caller's own policy governs.
        retry_after_secs: Option<u64>,
    },

    #[error("Agent execution error: {0}")]
    Agent(String),

    #[error("Dag-executor error: {0}")]
    DagExecutor(String),

    #[error("Adapter error ({provider}): {message}")]
    Adapter { provider: String, message: String },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("IO error: {0}")]
    IO(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Redis error: {0}")]
    Redis(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Task execution failed: {task_id} - {reason}")]
    TaskFailed { task_id: String, reason: String },

    #[error("Backpressure: channel capacity exceeded")]
    Backpressure,

    /// The host could not supply what this call needed right now -- for
    /// instance a build slot, when the build gate is already at its bound.
    ///
    /// Environmental by construction: the work is unchanged and the same call
    /// succeeds once the host frees up. A caller that cannot tell this apart
    /// from the work failing would retire a change because the machine was
    /// busy, so it is classified with the other environment failures below.
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Timeout")]
    Timeout,

    #[error("Aborted")]
    Aborted,

    #[error("Auth error: {0}")]
    Auth(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Internal error: {0}")]
    Internal(String),

    /// 这次发布的消息比传输层肯接受的字节数还大：**同一份输入重试多少次都是
    /// 同一个结果**，因为它的大小不随重试改变。
    ///
    /// 与其余变体的区别不是严重程度而是性质：那些是「现在不行」，这个是
    /// 「这份东西不行」。分开类型是为了让重试的那一侧能做出正确决定——
    /// 把永久错和瞬时错一起重试，一次拒绝就会把队列堵在这里，而重试的次数
    /// 换不来任何东西；把瞬时错当永久错丢掉，则会在总线抖一下时把事件扔了。
    ///
    /// `size` 永远是量出来的那串字节。`limit` 是**这一路能声明的那条界**，所以
    /// 是 `Option`：本地按服务器声明的上限先判的那一路报得出来；服务器自己回来
    /// 拒绝的那一路只知道它拒了、不知道它按哪条界拒的——本地那条界正是因为它
    /// 不够紧才放它过去，拿它充数会是一条假读数。
    #[error(
        "Payload exceeds the transport's limit: {size} bytes ({})",
        render_payload_limit(.limit)
    )]
    PayloadTooLarge { size: usize, limit: Option<usize> },
}

/// 上限那一侧的措辞，比 `Some(1048576)` 这种 Debug 输出更贴近它要回答的问题。
fn render_payload_limit(limit: &Option<usize>) -> String {
    match limit {
        Some(limit) => format!("limit {limit}"),
        None => "limit not known on this path".to_string(),
    }
}

pub type SFResult<T> = Result<T, SFError>;

impl SFError {
    /// 这次失败属于环境，还是属于这条输入本身。
    ///
    /// 判据只看错误的**类型**，不看错误文本：文本随上游改写、随语言变化，
    /// 拿它分类等于把判定权交给一个不受控的字符串。环境类失败的共性是"同一
    /// 调用换个时刻重试可能成功"，所以调用方该做的是把活延后、而不是把这条
    /// 输入终结掉；内容类失败（答复解析不了、校验不过、任务本身失败）重试多
    /// 少次都是同一个结果，才是死信。
    pub fn is_environment_failure(&self) -> bool {
        match self {
            SFError::LLM(_) | SFError::Timeout | SFError::ResourceExhausted(_) => true,
            // 上游明确拒绝：限流、配额、鉴权、服务端故障、连不上。请求本身不
            // 合法那一档不算——那是我们自己的请求写错了，归到环境里就又是一次
            // 把自身缺陷记成外部故障。
            SFError::Upstream { cause, .. } => cause.is_environment_failure(),
            _ => false,
        }
    }

    /// 这次失败是不是"宿主当时给不出一个构建槽"。
    ///
    /// 故意比 [`Self::is_environment_failure`] 窄：那一类还包含超时与上游故障，
    /// 而这里要回答的是一个更具体的判定——**这次构建根本没跑**。调用方拿它决定
    /// 要不要回滚一个已提交的变更：把宿主的忙记到变更头上，等于用机器负载给代码
    /// 定罪，变更会因此消失，而其内容一次都没被评判过。
    pub fn is_build_slot_refused(&self) -> bool {
        matches!(self, SFError::ResourceExhausted(_))
    }

    /// 这次失败是"重试同一请求在外部窗口复位或凭证更换前不可能成功"，还是
    /// "过一会儿再试可能就成了"。
    ///
    /// 判据同样只看类型。只有上游明确说了配额耗尽或鉴权被拒才算终止性——限流
    /// 是分钟级信号，服务端 5xx 和传输层故障本身就是瞬时的，把这几档也按终止
    /// 处理会让系统在一条本来能恢复的路上睡死。
    pub fn is_terminal_upstream_failure(&self) -> bool {
        matches!(self, SFError::Upstream { cause, .. } if cause.is_terminal())
    }

    /// 这次失败是不是「同一份输入再试多少次都一样」。
    ///
    /// 刻意比「不是环境类失败」窄得多。判据放宽到那一档会把库、网络、内部错
    /// 一起判成永久错——那些换一次调用完全可能成功，丢掉它们等于拿一次抖动
    /// 换一份耐久记录的消失。这里只承认有类型化依据的那一种：载荷超过传输层
    /// 的字节上限。判据看类型不看文本，理由同 [`Self::is_environment_failure`]。
    ///
    /// 调用方的决定是这个判据存在的理由：重试的那一侧对永久错应**当次消费掉**
    /// 而不是无限重试，否则一个再也不会变小的载荷会把队头永久占住。
    pub fn is_permanent_rejection(&self) -> bool {
        matches!(self, SFError::PayloadTooLarge { .. })
    }

    /// 这次失败若带有类型化原因，取出来。调用方拿它做判断，不必回头去解析
    /// message。
    pub fn upstream_failure(&self) -> Option<UpstreamFailure> {
        match self {
            SFError::Upstream { cause, .. } => Some(*cause),
            _ => None,
        }
    }

    /// The wait the upstream named, if it named one.
    ///
    /// The typed field first, then the rendering. The fallback is not a
    /// convenience: this error is flattened into a string by the layers between
    /// the transport and the retry decision, and every wrapper in between
    /// (`Agent`, `LLM`, `DagExecutor`) rebuilds it from text alone. Reading only
    /// the field makes the wait vanish exactly on the path it was measured for,
    /// and the retry then falls back to a policy backoff measured in seconds
    /// against an upstream that asked for minutes.
    ///
    /// `None` still means the upstream said nothing — the caller's policy
    /// governs, which is not the same as a zero-length wait.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            SFError::Upstream {
                retry_after_secs,
                reason,
                ..
            } => retry_after_secs.or_else(|| crate::contract::llm::retry_after_hint_in(reason)),
            other => crate::contract::llm::retry_after_hint_in(&other.to_string()),
        }
    }
}

impl SFError {
    /// Build a typed upstream refusal with no stated wait.
    ///
    /// The common case, and the one every caller that has only a status code
    /// wants; spelling it out keeps the stated-wait case visibly different at
    /// the construction site instead of one more `None` in a field list.
    pub fn upstream_refused(cause: UpstreamFailure, reason: impl Into<String>) -> Self {
        Self::Upstream {
            cause,
            reason: reason.into(),
            retry_after_secs: None,
        }
    }

    /// Build a typed upstream refusal that carries the wait the upstream named.
    ///
    /// The wait is rendered into `reason` as well as kept in the field. That is
    /// deliberate duplication: the field is the reading for anyone who receives
    /// this error, and the rendering is the reading for the layers in between,
    /// which flatten the error into a string and carry the reason out with them
    /// (a role output that reports "the prompt never reached its upstream"
    /// writes the cause into its own content, and the pipeline wraps that in a
    /// marker of its own). By the time the retry decision runs, the text is the
    /// only thing left of this error, so the wait has to be in it.
    pub fn upstream_refused_after(
        cause: UpstreamFailure,
        reason: impl Into<String>,
        retry_after_secs: Option<u64>,
    ) -> Self {
        let reason = match retry_after_secs {
            Some(secs) => format!(
                "{} {}",
                reason.into(),
                crate::contract::llm::render_retry_after(secs)
            ),
            None => reason.into(),
        };
        Self::Upstream {
            cause,
            reason,
            retry_after_secs,
        }
    }
}

impl From<std::io::Error> for SFError {
    fn from(e: std::io::Error) -> Self {
        SFError::IO(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::llm::UpstreamFailure;

    /// 分类落在类型上：文本里写满 timeout / quota 也不改变它的类别，反之环境
    /// 类失败也不靠文本被认出来。
    #[test]
    fn environment_class_follows_the_variant_not_the_text() {
        assert!(SFError::LLM("HTTP 503 from upstream".into()).is_environment_failure());
        assert!(SFError::LLM("plain reason".into()).is_environment_failure());
        assert!(SFError::Timeout.is_environment_failure());

        assert!(
            !SFError::Validation("quota exceeded in the payload".into()).is_environment_failure()
        );
        assert!(!SFError::Agent("upstream timeout after 30s".into()).is_environment_failure());
        assert!(
            !SFError::Serialization(serde_json::from_str::<i32>("not json").unwrap_err())
                .is_environment_failure()
        );
    }

    /// 上游拒绝这条路径上，环境类与「我们自己请求写错了」必须分开：后者的
    /// message 里写满 quota 也只是措辞，类别由 status 翻出来的取值决定。
    #[test]
    fn upstream_refusal_is_environment_unless_our_request_was_malformed() {
        let refused = |cause| SFError::upstream_refused(cause, "quota exceeded");

        assert!(refused(UpstreamFailure::RateLimited).is_environment_failure());
        assert!(refused(UpstreamFailure::QuotaExhausted).is_environment_failure());
        assert!(refused(UpstreamFailure::Auth).is_environment_failure());
        assert!(refused(UpstreamFailure::ServerError).is_environment_failure());
        assert!(refused(UpstreamFailure::Transport).is_environment_failure());

        assert!(!refused(UpstreamFailure::BadRequest).is_environment_failure());
    }

    /// 终止性只认"外部窗口复位或换钥匙才会有不同结果"的两档；限流与 5xx 是
    /// 瞬时信号，按终止处理会让恢复侧睡死。
    #[test]
    fn terminal_covers_only_quota_and_credentials() {
        let refused = |cause| SFError::upstream_refused(cause, String::new());

        assert!(refused(UpstreamFailure::QuotaExhausted).is_terminal_upstream_failure());
        assert!(refused(UpstreamFailure::Auth).is_terminal_upstream_failure());

        assert!(!refused(UpstreamFailure::RateLimited).is_terminal_upstream_failure());
        assert!(!refused(UpstreamFailure::ServerError).is_terminal_upstream_failure());
        assert!(!refused(UpstreamFailure::Transport).is_terminal_upstream_failure());
        assert!(!refused(UpstreamFailure::BadRequest).is_terminal_upstream_failure());

        // 没有状态码可依的失败不猜：LLM(_) 只说明上游没服务这次调用。
        assert!(!SFError::LLM("quota exceeded".into()).is_terminal_upstream_failure());
    }

    /// 上游明说的等待时长与原因并行传递，不是原因的第七种取值：没有说就是
    /// `None`，由调用方自己的策略兜底；说了就原样带出去，别再猜一遍。
    #[test]
    fn a_stated_wait_travels_beside_the_cause() {
        let stated =
            SFError::upstream_refused_after(UpstreamFailure::RateLimited, "slow down", Some(42));
        assert_eq!(stated.retry_after_secs(), Some(42));
        assert_eq!(
            stated.upstream_failure(),
            Some(UpstreamFailure::RateLimited)
        );

        assert_eq!(
            SFError::upstream_refused(UpstreamFailure::RateLimited, "slow down").retry_after_secs(),
            None
        );
        // A failure with no typed cause cannot have named a wait either.
        assert_eq!(SFError::Timeout.retry_after_secs(), None);
    }

    /// 回归：一次上游停供的三次尝试在 10 秒内烧完，退避回到了策略的
    /// 1s/2s/4s，因为 `retry_after_secs` 在角色输出那一跳被拍成了字符串。
    ///
    /// 这条测试复刻的是那一跳：网关 503 带着 `retry-after` 进来到类型上，
    /// 角色把它写进自己的 content（`environment_error: {e}`），PGE 再套一层
    /// 终止性标记，最后整条链以 `SFError::Agent` 结束。到达重试判定的错误
    /// 已经不含任何类型，只有文本 —— 所以等待时长要么在文本里，要么就没了。
    #[test]
    fn a_stated_wait_survives_the_flattening_into_a_wrapped_agent_error() {
        let refused = SFError::upstream_refused_after(
            UpstreamFailure::ServerError,
            "LLM stream error: API error (HTTP 503): {\"error\":\"所有 LLM 上游当前不可用\",\
             \"quota_window_secs\":18000,\"retry_after_seconds\":282}",
            Some(282),
        );
        // 角色输出那一跳：类型没了，文本留下。
        let as_role_content = format!("environment_error: {refused}");
        // PGE 那一跳：终止性标记套在外面。
        let flattened = SFError::Agent(format!("terminal_env_failure: {as_role_content}"));

        assert_eq!(
            flattened.upstream_failure(),
            None,
            "类型确实在这一跳丢了，否则这条测试测的不是它要测的那一跳"
        );
        assert_eq!(
            flattened.retry_after_secs(),
            Some(282),
            "上游说的等待时长必须跟着文本过这一跳，否则退避回落成秒级策略"
        );
        assert!(
            !crate::contract::outcome::is_deterministic_failure(&flattened.to_string()),
            "套上终止性标记之后仍要按它自报的类型判定：server error 是环境自己会清掉的，\
             保留重试"
        );
    }

    /// 拍在文本里的等待时长是量出来的，不是判出来的：取不到就是没说，由调用
    /// 方自己的策略兜底；读出天文数字也不能真的把任务停在那里。
    #[test]
    fn a_wait_read_out_of_text_is_capped_and_absent_means_unstated() {
        assert_eq!(
            SFError::Agent("Agent execution error: nothing about a wait".into()).retry_after_secs(),
            None,
            "没有这句话时返回 None，而不是 0：0 会读成「立刻重试」"
        );
        assert_eq!(
            SFError::Agent(format!(
                "refused {}",
                crate::contract::llm::render_retry_after(9_999_999)
            ))
            .retry_after_secs(),
            Some(crate::contract::llm::MAX_RECOVERED_RETRY_AFTER_SECS),
            "从散文里读出来的数不能比它可能的来源活得更久"
        );
    }

    /// 永久这一档刻意很窄：库、网络、内部错换一次调用完全可能成功，把「不是环境
    /// 类」当判据会把它们一起判成永久，丢掉它们等于拿一次抖动换一份耐久记录的消失。
    #[test]
    fn permanent_rejection_covers_only_the_payload_that_cannot_shrink() {
        let too_big = SFError::PayloadTooLarge {
            size: 2_000_000,
            limit: Some(1_048_576),
        };
        assert!(too_big.is_permanent_rejection());
        // 它也不属于环境类：大小不随时刻改变，延后重投不是这条失败的解。
        assert!(!too_big.is_environment_failure());

        for other in [
            SFError::DagExecutor("bus down".into()),
            SFError::Agent("upstream timeout".into()),
            SFError::Timeout,
            SFError::LLM("provider unavailable".into()),
            SFError::Validation("payload malformed".into()),
            SFError::IO("disk full".into()),
        ] {
            assert!(
                !other.is_permanent_rejection(),
                "{other} 等一等可能就好，按永久错丢掉是拿抖动换记录的消失"
            );
        }
    }

    /// 界那一侧的读数分两种来源，报出来的东西必须跟着来源走。本地前置检查读的是
    /// 服务器自己声明的上限，报得出来；服务器回来拒绝那一路只知道它拒了、不知道
    /// 按哪条界拒的——本地那条界正因不够紧才放它过去，填上去就是一条假读数。
    #[test]
    fn a_payload_rejection_names_the_limit_only_when_this_path_knew_it() {
        let judged_locally = SFError::PayloadTooLarge {
            size: 2_000_000,
            limit: Some(1_048_576),
        };
        assert!(
            judged_locally.to_string().contains("limit 1048576"),
            "本地判的那一路，界要跟着一起报出来：{}",
            judged_locally
        );

        let refused_by_server = SFError::PayloadTooLarge {
            size: 2_000_000,
            limit: None,
        };
        assert!(
            refused_by_server
                .to_string()
                .contains("limit not known on this path"),
            "服务器判的那一路报的是「不知道」，不是本地那个不够紧的数：{}",
            refused_by_server
        );
        assert!(
            refused_by_server.to_string().contains("2000000 bytes"),
            "字节数仍然是量出来的那一串：{refused_by_server}"
        );
    }

    /// 状态码到原因的翻译是纯函数，边界的取值要落在预期的档位上。
    #[test]
    fn status_maps_to_the_expected_cause() {
        assert_eq!(
            UpstreamFailure::from_status(429),
            UpstreamFailure::RateLimited
        );
        assert_eq!(
            UpstreamFailure::from_status(402),
            UpstreamFailure::QuotaExhausted
        );
        assert_eq!(UpstreamFailure::from_status(401), UpstreamFailure::Auth);
        assert_eq!(UpstreamFailure::from_status(403), UpstreamFailure::Auth);
        assert_eq!(
            UpstreamFailure::from_status(500),
            UpstreamFailure::ServerError
        );
        assert_eq!(
            UpstreamFailure::from_status(503),
            UpstreamFailure::ServerError
        );
        assert_eq!(
            UpstreamFailure::from_status(400),
            UpstreamFailure::BadRequest
        );
        assert_eq!(
            UpstreamFailure::from_status(422),
            UpstreamFailure::BadRequest
        );
        // 没见过的码不往环境上靠。
        assert_eq!(
            UpstreamFailure::from_status(418),
            UpstreamFailure::BadRequest
        );
    }
}
