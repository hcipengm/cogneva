//! 一份归档进记忆的原始素材，两份记忆后端契约都要它。
//!
//! 两处各写了一遍同一个 `RawSource` 字面量，连命名空间与内容类型都逐字相同。
//! 这不是「各自造各自的数据」：两份契约问的是同一个后端对同一条素材的回答，一边
//! 改了内容类型或命名空间而另一边没改，两份判据说的就不是同一件事了，而两边都还是绿的。

use cog_core::{RawSource, SourceRef};

/// `id` 那条素材：默认命名空间下的一份对话转写，正文是 `text` 的字节。
//
// 每个测试二进制各编一次 `common`，只有两份夹具用它。
#[allow(dead_code)]
pub fn raw(id: &str, text: &str) -> RawSource {
    RawSource::new(
        id,
        "default",
        "conversation/transcript",
        text.as_bytes().to_vec(),
    )
}

/// 指向 `id` 那条素材的引用：`memory://<id>`，版本 `test/v1`。
#[allow(dead_code)]
pub fn source_ref(id: &str) -> SourceRef {
    SourceRef::new(format!("memory://{}", id), "test/v1")
}
