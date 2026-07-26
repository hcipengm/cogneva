use cog_core::ObjectBackend;
use cog_storage::MemoryObjectBackend;

#[tokio::test]
async fn test_memory_put_and_get() {
    let backend = MemoryObjectBackend::new();
    let uri = backend.put("test/key", b"hello world").await.unwrap();
    assert_eq!(uri, "memory://test/key");

    let data = backend.get("test/key").await.unwrap();
    assert_eq!(data, Some(b"hello world".to_vec()));
}

#[tokio::test]
async fn test_memory_get_missing() {
    let backend = MemoryObjectBackend::new();
    let data = backend.get("missing").await.unwrap();
    assert_eq!(data, None);
}

#[tokio::test]
async fn test_memory_delete() {
    let backend = MemoryObjectBackend::new();
    backend.put("del/key", b"data").await.unwrap();
    assert!(backend.exists("del/key").await.unwrap());

    backend.delete("del/key").await.unwrap();
    assert!(!backend.exists("del/key").await.unwrap());
    assert_eq!(backend.get("del/key").await.unwrap(), None);
}

#[tokio::test]
async fn test_memory_presign_url() {
    let backend = MemoryObjectBackend::new();
    let url = backend.presign_url("sig/key", 3600).await.unwrap();
    assert_eq!(url, "memory://sig/key");
}

#[tokio::test]
async fn test_memory_list_all() {
    let backend = MemoryObjectBackend::new();
    backend.put("a/1", b"1").await.unwrap();
    backend.put("a/2", b"2").await.unwrap();
    backend.put("b/1", b"3").await.unwrap();

    let mut keys = backend.list(None).await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a/1", "a/2", "b/1"]);
}

#[tokio::test]
async fn test_memory_list_with_prefix() {
    let backend = MemoryObjectBackend::new();
    backend.put("prefix/alpha", b"1").await.unwrap();
    backend.put("prefix/beta", b"2").await.unwrap();
    backend.put("other/gamma", b"3").await.unwrap();

    let mut keys = backend.list(Some("prefix/")).await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["prefix/alpha", "prefix/beta"]);
}
