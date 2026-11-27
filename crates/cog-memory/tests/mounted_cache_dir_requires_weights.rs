//! 空缓存目录必须在任何网络请求之前被拒绝。
//!
//! 2026-11-19 现场：单节点 k3s 上 profile 打开权重，`modelWeights.hostPath` 是个空
//! 目录，init 卡在 fastembed 向 model hub 的无超时拉取上，Pod 一直 not ready，日志里
//! 没有一句「权重不在」。这条用例把「空目录 = 立刻 Err」钉住：它跑得快，正是因为没
//! 有发生任何下载——实现一旦退回联网，CI 上这条会真的去拉整份权重而慢到红。
//!
//! 嵌入与重排两条构造口各判一次：它们读同一个目录、踩同一个坑，只修一条等于留一条。

use cog_memory::{FastEmbedProvider, FastEmbedRerankerProvider};

fn empty_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cog-memory-empty-weights-{tag}-{}",
        std::process::id()
    ));
    cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

#[test]
fn an_empty_embedding_cache_dir_is_refused_before_any_fetch() {
    let dir = empty_dir("embedding");
    let err = FastEmbedProvider::try_new_with_cache_dir(Some(dir.clone()))
        .err()
        .expect("一个空目录不能被当成可用的权重目录");
    assert!(
        err.contains("no ONNX weights"),
        "要说出「目录里没有权重」而不是一句泛泛的加载失败：{err}"
    );
    cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
}

#[test]
fn an_empty_reranker_cache_dir_is_refused_before_any_fetch() {
    let dir = empty_dir("reranker");
    let err = FastEmbedRerankerProvider::try_new_with_cache_dir(Some(dir.clone()))
        .err()
        .expect("一个空目录不能被当成可用的权重目录");
    assert!(
        err.contains("no ONNX weights"),
        "要说出「目录里没有权重」而不是一句泛泛的加载失败：{err}"
    );
    cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
}
