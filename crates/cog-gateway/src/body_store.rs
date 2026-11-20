//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//!
//! 流式正文的落点：每收到一块就写一块，结束时封口。网关只认这个接口，实现由装配层给。
//!
//! 为什么是一个 trait 而不是直接调库：网关进程只依赖契约，而"写大对象"是存储侧的实现
//! （`cog-storage` 的 `large_object`）。契约在这里，实现在那里，接线在装配层。
//!
//! 三条形状：
//! - `open` 拿到一个句柄；句柄活着的期间每一块都写进去；
//! - `seal` 封口并给出 `(sha256, bytes)`——**这两个数是「无损」的判据**：与上游字节算出的
//!   哈希相等才算没有丢；
//! - 没有 `seal` 的句柄就是"未封口"：崩溃留下的半段在读面上与完整正文分得开。

use async_trait::async_trait;

use cog_core::SFResult;

/// 一段正文的句柄。
#[async_trait]
pub trait BodyWriter: Send + Sync {
    /// 写一块。块就是这一拍收到的字节，不合并、不切分。
    async fn write(&self, chunk: &[u8]) -> SFResult<()>;

    /// 封口：返回 `(sha256, 总字节)`。
    async fn seal(&self) -> SFResult<(String, u64)>;
}

/// 开一段正文。
#[async_trait]
pub trait BodyStore: Send + Sync {
    async fn open(&self) -> SFResult<Box<dyn BodyWriter>>;
}
