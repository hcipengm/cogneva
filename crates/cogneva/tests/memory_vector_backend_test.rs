use cog_core::VectorBackend;
use cog_storage::MemoryVectorBackend;

fn vec_f32(vals: &[f32]) -> Vec<f32> {
    vals.to_vec()
}

#[tokio::test]
async fn test_memory_create_and_delete_collection() {
    let backend = MemoryVectorBackend::new();
    assert!(!backend.collection_exists("docs").await.unwrap());

    backend.create_collection("docs", 3).await.unwrap();
    assert!(backend.collection_exists("docs").await.unwrap());

    backend.delete_collection("docs").await.unwrap();
    assert!(!backend.collection_exists("docs").await.unwrap());
}

#[tokio::test]
async fn test_memory_insert_and_search() {
    let backend = MemoryVectorBackend::new();
    backend.create_collection("docs", 3).await.unwrap();

    let ids = backend
        .insert(
            "docs",
            vec![
                vec_f32(&[1.0, 0.0, 0.0]),
                vec_f32(&[0.0, 1.0, 0.0]),
                vec_f32(&[0.0, 0.0, 1.0]),
            ],
            vec![
                serde_json::json!({"text": "x-axis"}),
                serde_json::json!({"text": "y-axis"}),
                serde_json::json!({"text": "z-axis"}),
            ],
        )
        .await
        .unwrap();

    assert_eq!(ids.len(), 3);

    let results = backend.search("docs", &[1.0, 0.1, 0.1], 2).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, ids[0]); // x-axis is closest
    assert!(results[0].score > results[1].score);
}

#[tokio::test]
async fn test_memory_search_top_k_limits_results() {
    let backend = MemoryVectorBackend::new();
    backend.create_collection("items", 2).await.unwrap();

    let ids = backend
        .insert(
            "items",
            vec![
                vec_f32(&[1.0, 0.0]),
                vec_f32(&[0.0, 1.0]),
                vec_f32(&[1.0, 1.0]),
            ],
            vec![
                serde_json::json!(null),
                serde_json::json!(null),
                serde_json::json!(null),
            ],
        )
        .await
        .unwrap();

    let results = backend.search("items", &[1.0, 0.0], 1).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, ids[0]);
}

#[tokio::test]
async fn test_memory_delete_vectors() {
    let backend = MemoryVectorBackend::new();
    backend.create_collection("docs", 2).await.unwrap();

    let ids = backend
        .insert(
            "docs",
            vec![vec_f32(&[1.0, 0.0]), vec_f32(&[0.0, 1.0])],
            vec![serde_json::json!(null), serde_json::json!(null)],
        )
        .await
        .unwrap();

    backend.delete("docs", &[ids[0].clone()]).await.unwrap();

    let results = backend.search("docs", &[1.0, 0.0], 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, ids[1]);
}

#[tokio::test]
async fn test_memory_search_returns_metadata() {
    let backend = MemoryVectorBackend::new();
    backend.create_collection("docs", 2).await.unwrap();

    backend
        .insert(
            "docs",
            vec![vec_f32(&[1.0, 0.0])],
            vec![serde_json::json!({"title": "Hello"})],
        )
        .await
        .unwrap();

    let results = backend.search("docs", &[1.0, 0.0], 1).await.unwrap();
    assert_eq!(results[0].metadata, serde_json::json!({"title": "Hello"}));
}

#[tokio::test]
async fn test_memory_search_missing_collection_fails() {
    let backend = MemoryVectorBackend::new();
    let err = backend.search("missing", &[1.0], 1).await.unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("missing"));
}

#[tokio::test]
async fn test_memory_insert_length_mismatch_fails() {
    let backend = MemoryVectorBackend::new();
    backend.create_collection("docs", 2).await.unwrap();

    let err = backend
        .insert(
            "docs",
            vec![vec_f32(&[1.0, 0.0])],
            vec![serde_json::json!(null), serde_json::json!(null)],
        )
        .await
        .unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("mismatch"));
}
