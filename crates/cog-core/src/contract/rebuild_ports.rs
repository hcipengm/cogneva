//! 重建一个投影时要向外面借的两样东西。
//!
//! 事实里存的是正文的**指针**：附件是对象存储的 key，工具调用是 `artifact://`。
//! 重建要把指针换回字节，但它不该知道对象存储或 memory API 在哪——那两个端口由
//! 装配层实现，存储层只按接口取字节。这样重建命令与「正文存在哪」解耦，换存储
//! 不动重建。

use async_trait::async_trait;

use crate::SFResult;

/// 按对象存储的 key 取回字节。
#[async_trait]
pub trait AttachmentBytes: Send + Sync {
    async fn get(&self, key: &str) -> SFResult<Vec<u8>>;
}

/// 按 `artifact://<namespace>/<id>` 取回正文。
#[async_trait]
pub trait ArchivedBodyReader: Send + Sync {
    async fn read(&self, uri: &str) -> SFResult<String>;
}
