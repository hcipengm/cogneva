//! 唯一的写口：把一条已经拼好的信封追加到那条流上。
//!
//! 意图、待办、员工、消息、任务转移、调用——每一族都有自己的载荷构造器，但**落库
//! 只有这一条路**。此前每族各带一份 SQL、一份事件 id 工厂、一份 `ON CONFLICT`：
//! 四份一样的代码，改一处漏三处，而且「哪一族能写」取决于插件里 publish 了几次。
//!
//! 形状因此是：构造器在 [`crate`] 里拼 [`EnvelopeFields`]，写口只做三件事——发一个
//! 全局唯一的 id、过一遍信封检查、按同一个幂等键写进去。因果与批号也在信封里，
//! 不再走第二条入口：`append_caused` 那条旁路删除之后，一条事件的每一列都只看一个
//! 地方。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{EnvelopeFields, SFResult};

/// 一条只追加流的写口。
#[async_trait]
pub trait EventLog: Send + Sync {
    /// 这个进程写下的每一条事实的写者名（部署名），与幂等键一起构成唯一约束。
    fn writer(&self) -> &str;

    /// 下一个事件 id。由写口发，构造器只接收——两处各生成一次，就会有两套 ULID
    /// 形状，而主键只认其中一套。
    fn next_event_id(&self) -> String;

    /// 追加一条。同一 `(writer, idempotency_key)` 重投不写第二行，也不报错。
    ///
    /// 因果、批号、尝试次数、载荷版本都在 `fields` 里；写口不再收第二组参数。
    async fn append(&self, fields: EnvelopeFields, at: DateTime<Utc>) -> SFResult<()>;
}
