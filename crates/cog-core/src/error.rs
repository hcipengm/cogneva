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
        matches!(self, SFError::LLM(_) | SFError::Timeout)
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
}
