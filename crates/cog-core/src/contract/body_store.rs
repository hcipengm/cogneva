//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//!
//! 流式正文的落点：每收到一块就写一块，结束时封口。
//!
//! 契约在 core（网关与存储各自依赖它，谁也不依赖谁）；实现在 `cog-storage`（PG 大对象），
//! 接线在装配层。
//!
//! 三条形状：
//! - `open` 拿到一个句柄；句柄活着的期间每一块都写进去；
//! - `seal` 封口并给出 `(sha256, bytes)`——**这两个数是「无损」的判据**：与上游字节算出的
//!   哈希相等才算没有丢；
//! - 没有 `seal` 的句柄就是「未封口」：崩溃留下的半段在读面上与完整正文分得开。

use async_trait::async_trait;

use crate::SFResult;

/// 一段正文的句柄。
#[async_trait]
pub trait BodyWriter: Send + Sync {
    /// 写一块。块就是这一拍收到的字节，不合并、不切分。
    async fn write(&self, chunk: &[u8]) -> SFResult<()>;

    /// 这段大对象的 oid：行里存的指针就是它。
    fn oid(&self) -> u32;

    /// 封口：返回 `(sha256, 总字节)`。
    async fn seal(&self) -> SFResult<(String, u64)>;

    /// 丢掉这段没写成的正文。
    ///
    /// 只在**写入口自己知道这一份不完整、而且字节另外还有一份**时调用（写入失败退回
    /// 内存攒着的那条路）。不调它，库里就留一段谁也不引用的字节，而孤儿扫描会把
    /// 「进程崩在中途」与「一次被处理掉的写失败」读成同一件事。
    ///
    /// 崩溃不走这条路：进程死掉时没人调它，已到达的字节留在库里，那正是要留下的现场。
    async fn discard(&self) -> SFResult<()>;
}

/// 开一段正文。
#[async_trait]
pub trait BodyStore: Send + Sync {
    async fn open(&self) -> SFResult<Box<dyn BodyWriter>>;
}
