//! Production-grade Qdrant [`VectorBackend`] implementation.
//! Uses the `qdrant-client` crate to talk to a Qdrant cluster.
//! Each collection stores dense vectors with cosine distance.
//! Metadata is serialised to a JSON string and stored in the point payload
//! under the key `metadata_json` so that it survives round-trips.

use async_trait::async_trait;
use cog_core::{SFError, SFResult, VectorBackend, VectorSearchResult};
use qdrant_client::qdrant::{
    CollectionExistsRequest, CreateCollection, DeleteCollection, DeletePoints, Distance,
    PointStruct, PointsIdsList, PointsSelector, SearchPoints, SparseIndexConfig, SparseIndices,
    SparseVectorConfig, SparseVectorParams, UpsertPoints, Value as QdrantValue, Vector,
    VectorParams, VectorParamsMap, Vectors, VectorsConfig,
};
use serde_json::Value;
use sha2::Digest;
use std::collections::HashMap;

/// Qdrant point ids only accept a UUID or an unsigned integer, while callers
/// hand us arbitrary strings (e.g. `summary-<bounded agent id>`). Derive a
/// stable UUID from the caller id so re-inserts upsert instead of erroring,
/// and deletes resolve to the same point. The caller id itself stays in
/// `metadata_json.id`, which is what the read path correlates on.
fn point_id_for(caller_id: &str) -> String {
    if uuid::Uuid::parse_str(caller_id).is_ok() || caller_id.parse::<u64>().is_ok() {
        return caller_id.to_string();
    }
    let digest = sha2::Sha256::digest(caller_id.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4 layout
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
    uuid::Uuid::from_bytes(bytes).to_string()
}

/// Qdrant-backed [`VectorBackend`].
/// Talks to a Qdrant cluster over gRPC/HTTP.  Collections are created on
/// demand via [`VectorBackend::create_collection`].
pub struct QdrantVectorBackend {
    client: qdrant_client::Qdrant,
}

impl QdrantVectorBackend {
    /// Connect to a Qdrant server.
    /// `qdrant_url` is typically `http://localhost:6334`.
    pub async fn try_new(qdrant_url: &str) -> SFResult<Self> {
        let client = qdrant_client::Qdrant::new(
            qdrant_client::config::QdrantConfig::from_url(qdrant_url).skip_compatibility_check(),
        )
        .map_err(|e| SFError::Validation(format!("qdrant connect failed: {e}")))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl VectorBackend for QdrantVectorBackend {
    async fn create_collection(&self, collection: &str, dimension: usize) -> SFResult<()> {
        let exists = self
            .client
            .collection_exists(CollectionExistsRequest {
                collection_name: collection.to_string(),
            })
            .await
            .map_err(|e| SFError::Validation(format!("qdrant collection check failed: {e}")))?;

        if exists {
            return Ok(());
        }

        let dense_params = VectorParams {
            size: dimension as u64,
            distance: Distance::Cosine as i32,
            ..Default::default()
        };

        let sparse_params = SparseVectorParams {
            index: Some(SparseIndexConfig {
                ..Default::default()
            }),
            ..Default::default()
        };

        let vectors_config = VectorsConfig {
            config: Some(qdrant_client::qdrant::vectors_config::Config::ParamsMap(
                VectorParamsMap {
                    map: HashMap::from([("dense".to_string(), dense_params)]),
                },
            )),
        };

        let sparse_vectors_config = SparseVectorConfig {
            map: HashMap::from([("sparse".to_string(), sparse_params)]),
        };

        let create = CreateCollection {
            collection_name: collection.to_string(),
            vectors_config: Some(vectors_config),
            sparse_vectors_config: Some(sparse_vectors_config),
            ..Default::default()
        };

        self.client
            .create_collection(create)
            .await
            .map_err(|e| SFError::Validation(format!("qdrant create collection failed: {e}")))?;

        Ok(())
    }

    async fn delete_collection(&self, collection: &str) -> SFResult<()> {
        let delete = DeleteCollection {
            collection_name: collection.to_string(),
            ..Default::default()
        };
        self.client
            .delete_collection(delete)
            .await
            .map_err(|e| SFError::Validation(format!("qdrant delete collection failed: {e}")))?;
        Ok(())
    }

    async fn insert(
        &self,
        collection: &str,
        vectors: Vec<Vec<f32>>,
        metadata: Vec<Value>,
    ) -> SFResult<Vec<String>> {
        if vectors.len() != metadata.len() {
            return Err(SFError::Agent(
                "vectors and metadata length mismatch".into(),
            ));
        }

        let mut points = Vec::with_capacity(vectors.len());
        let mut ids = Vec::with_capacity(vectors.len());

        for (vec, meta) in vectors.into_iter().zip(metadata) {
            let id = meta
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            ids.push(id.clone());

            let mut payload: HashMap<String, QdrantValue> = HashMap::new();
            payload.insert(
                "metadata_json".to_string(),
                QdrantValue::from(meta.to_string()),
            );

            let vectors_map = HashMap::from([("dense".to_string(), Vector::new_dense(vec))]);

            let point = PointStruct {
                id: Some(point_id_for(&id).into()),
                payload,
                vectors: Some(Vectors::from(vectors_map)),
            };
            points.push(point);
        }

        let upsert = UpsertPoints {
            collection_name: collection.to_string(),
            points,
            ..Default::default()
        };

        self.client
            .upsert_points(upsert)
            .await
            .map_err(|e| SFError::Validation(format!("qdrant upsert failed: {e}")))?;

        Ok(ids)
    }

    async fn search(
        &self,
        collection: &str,
        vector: &[f32],
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        let request = SearchPoints {
            collection_name: collection.to_string(),
            vector: vector.to_vec(),
            vector_name: Some("dense".to_string()),
            limit: top_k as u64,
            with_payload: Some(true.into()),
            ..Default::default()
        };

        let response = self
            .client
            .search_points(request)
            .await
            .map_err(|e| SFError::Validation(format!("qdrant search failed: {e}")))?;

        let mut results = Vec::new();
        for scored_point in response.result {
            let score = scored_point.score;
            let id = match scored_point.id {
                Some(id) => match id.point_id_options {
                    Some(qdrant_client::qdrant::point_id::PointIdOptions::Uuid(u)) => u,
                    Some(qdrant_client::qdrant::point_id::PointIdOptions::Num(n)) => n.to_string(),
                    None => continue,
                },
                None => continue,
            };

            let metadata = scored_point
                .payload
                .get("metadata_json")
                .and_then(|v| match &v.kind {
                    Some(qdrant_client::qdrant::value::Kind::StringValue(s)) => {
                        serde_json::from_str(s).ok()
                    }
                    _ => None,
                })
                .unwrap_or(Value::Null);

            results.push(VectorSearchResult {
                id,
                score,
                metadata,
            });
        }

        Ok(results)
    }

    async fn delete(&self, collection: &str, ids: &[String]) -> SFResult<()> {
        let id_values: Vec<qdrant_client::qdrant::PointId> =
            ids.iter().map(|id| point_id_for(id).into()).collect();

        let delete = DeletePoints {
            collection_name: collection.to_string(),
            points: Some(PointsSelector {
                points_selector_one_of: Some(
                    qdrant_client::qdrant::points_selector::PointsSelectorOneOf::Points(
                        PointsIdsList { ids: id_values },
                    ),
                ),
            }),
            ..Default::default()
        };

        self.client
            .delete_points(delete)
            .await
            .map_err(|e| SFError::Validation(format!("qdrant delete failed: {e}")))?;
        Ok(())
    }

    async fn insert_sparse(
        &self,
        collection: &str,
        sparse: Vec<cog_core::SparseEmbedding>,
        metadata: Vec<Value>,
    ) -> SFResult<Vec<String>> {
        if sparse.len() != metadata.len() {
            return Err(SFError::Agent("sparse and metadata length mismatch".into()));
        }

        let mut points = Vec::with_capacity(sparse.len());
        let mut ids = Vec::with_capacity(sparse.len());

        for (s, meta) in sparse.into_iter().zip(metadata) {
            let id = meta
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            ids.push(id.clone());

            let mut payload: HashMap<String, QdrantValue> = HashMap::new();
            payload.insert(
                "metadata_json".to_string(),
                QdrantValue::from(meta.to_string()),
            );

            let vectors_map = HashMap::from([(
                "sparse".to_string(),
                Vector::new_sparse(s.indices, s.values),
            )]);

            let point = PointStruct {
                id: Some(point_id_for(&id).into()),
                payload,
                vectors: Some(Vectors::from(vectors_map)),
            };
            points.push(point);
        }

        let upsert = UpsertPoints {
            collection_name: collection.to_string(),
            points,
            ..Default::default()
        };

        self.client
            .upsert_points(upsert)
            .await
            .map_err(|e| SFError::Validation(format!("qdrant sparse upsert failed: {e}")))?;

        Ok(ids)
    }

    async fn search_sparse(
        &self,
        collection: &str,
        sparse: &cog_core::SparseEmbedding,
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        let request = SearchPoints {
            collection_name: collection.to_string(),
            vector: sparse.values.clone(),
            sparse_indices: Some(SparseIndices {
                data: sparse.indices.clone(),
            }),
            vector_name: Some("sparse".to_string()),
            limit: top_k as u64,
            with_payload: Some(true.into()),
            ..Default::default()
        };

        let response = self
            .client
            .search_points(request)
            .await
            .map_err(|e| SFError::Validation(format!("qdrant sparse search failed: {e}")))?;

        let mut results = Vec::new();
        for scored_point in response.result {
            let score = scored_point.score;
            let id = match scored_point.id {
                Some(id) => match id.point_id_options {
                    Some(qdrant_client::qdrant::point_id::PointIdOptions::Uuid(u)) => u,
                    Some(qdrant_client::qdrant::point_id::PointIdOptions::Num(n)) => n.to_string(),
                    None => continue,
                },
                None => continue,
            };

            let metadata = scored_point
                .payload
                .get("metadata_json")
                .and_then(|v| match &v.kind {
                    Some(qdrant_client::qdrant::value::Kind::StringValue(s)) => {
                        serde_json::from_str(s).ok()
                    }
                    _ => None,
                })
                .unwrap_or(Value::Null);

            results.push(VectorSearchResult {
                id,
                score,
                metadata,
            });
        }

        Ok(results)
    }

    async fn collection_exists(&self, collection: &str) -> SFResult<bool> {
        let exists = self
            .client
            .collection_exists(CollectionExistsRequest {
                collection_name: collection.to_string(),
            })
            .await
            .map_err(|e| SFError::Validation(format!("qdrant collection check failed: {e}")))?;
        Ok(exists)
    }
}

#[cfg(test)]
mod tests {
    use super::point_id_for;

    /// 生产里真实撞墙的 id 形状：摄取器把整条 issue 标题绑进 agent id，
    /// summary id 是 `summary-` 前缀加绑定的超长字符串，Qdrant 只认
    /// UUID/u64，直接拒绝 upsert，summary 通路整体写不进去。
    const LIVE_SUMMARY_ID: &str = "summary-agent-squad_decompose-Fix_gitee_issue__\
        34295240_______________________________________-1789347585888-ba4e032b";

    #[test]
    fn arbitrary_caller_id_maps_to_stable_valid_uuid() {
        let first = point_id_for(LIVE_SUMMARY_ID);
        assert!(
            uuid::Uuid::parse_str(&first).is_ok(),
            "派生结果必须是 Qdrant 接受的 UUID：{first}"
        );
        assert_eq!(first, point_id_for(LIVE_SUMMARY_ID), "同一 id 必须幂等");
        assert_ne!(
            first,
            point_id_for("summary-agent-other"),
            "不同 id 不许撞同一个 point"
        );
    }

    #[test]
    fn already_valid_point_ids_pass_through() {
        let uuid = uuid::Uuid::new_v4().to_string();
        assert_eq!(point_id_for(&uuid), uuid, "UUID 原样透传");
        assert_eq!(point_id_for("42"), "42", "u64 原样透传");
    }
}
