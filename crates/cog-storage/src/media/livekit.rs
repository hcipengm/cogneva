#![cfg(feature = "livekit")]
//! LiveKit media backend — WebRTC rooms, token generation, and recording.
//! Uses the LiveKit Server REST API (HTTP + protobuf/JSON) for room management
//! and the Egress API for recording.  Participant access tokens are standard
//! HS256 JWTs with LiveKit `video` grants.

use async_trait::async_trait;
use cog_core::{
    HttpRequest, MediaBackend, MediaBackendConfig, MediaRoom, RecordingSession, SFError, SFResult,
};
use std::sync::Arc;
use tracing::{info, warn};

/// LiveKit media backend implementation.
#[derive(Clone)]
pub struct LiveKitMediaBackend {
    config: MediaBackendConfig,
    client: Option<Arc<dyn cog_core::HttpClient>>,
    server_url: String,
}

impl LiveKitMediaBackend {
    /// Create a new LiveKit backend from configuration.
    pub fn new(config: MediaBackendConfig) -> Self {
        let server_url = config.server_url.trim_end_matches('/').to_string();
        Self {
            config,
            client: None,
            server_url,
        }
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn client(&self) -> SFResult<&Arc<dyn cog_core::HttpClient>> {
        self.client.as_ref().ok_or_else(|| SFError::Adapter {
            provider: "livekit".into(),
            message: "no HttpClient configured".into(),
        })
    }

    // ─── JWT Token Generation ───

    fn generate_service_token(&self) -> SFResult<String> {
        let header = base64url_encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "video": {
                "roomCreate": true,
                "roomList": true,
                "roomRecord": true,
                "roomAdmin": true,
                "ingressAdmin": true,
            },
            "iss": self.config.api_key,
            "nbf": now,
            "exp": now + 86400,
        });
        let payload = base64url_encode(&claims.to_string());
        let signing_input = format!("{}.{}", header, payload);
        let signature = hmac_sha256(&self.config.api_secret, &signing_input);
        Ok(format!("{}.{}.{}", header, payload, signature))
    }

    fn generate_participant_token(&self, room: &str, identity: &str) -> SFResult<String> {
        let header = base64url_encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "video": {
                "room": room,
                "roomJoin": true,
                "canPublish": true,
                "canSubscribe": true,
                "canPublishData": true,
            },
            "iss": self.config.api_key,
            "sub": identity,
            "nbf": now,
            "exp": now + 3600,
        });
        let payload = base64url_encode(&claims.to_string());
        let signing_input = format!("{}.{}", header, payload);
        let signature = hmac_sha256(&self.config.api_secret, &signing_input);
        Ok(format!("{}.{}.{}", header, payload, signature))
    }

    fn auth_header(&self) -> SFResult<String> {
        let token = self.generate_service_token()?;
        Ok(format!("Bearer {}", token))
    }

    fn room_svc_url(&self, method: &str) -> String {
        format!("{}/twirp/livekit.RoomService/{}", self.server_url, method)
    }

    fn egress_svc_url(&self, method: &str) -> String {
        format!("{}/twirp/livekit.Egress/{}", self.server_url, method)
    }
}

#[async_trait]
impl MediaBackend for LiveKitMediaBackend {
    async fn create_room(&self, name: &str) -> SFResult<String> {
        let token = self.auth_header()?;
        let body = serde_json::json!({ "name": name });
        let req = HttpRequest::post(self.room_svc_url("CreateRoom"))
            .header("Authorization", token)
            .json(&body)
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("create_room serialize failed: {}", e),
            })?
            .timeout(30);
        let resp = self
            .client()?
            .execute(req)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("create_room request failed: {}", e),
            })?;

        if !resp.is_success() {
            let text = resp.text().unwrap_or_default();
            return Err(SFError::Adapter {
                provider: "livekit".into(),
                message: format!("create_room failed: {}", text),
            });
        }

        let json: serde_json::Value = resp.json().map_err(|e| SFError::Adapter {
            provider: "livekit".into(),
            message: format!("create_room parse failed: {}", e),
        })?;
        let sid = json
            .get("sid")
            .and_then(|v| v.as_str())
            .unwrap_or(name)
            .to_string();
        info!("LiveKit room created: {} (sid={})", name, sid);
        Ok(sid)
    }

    async fn delete_room(&self, name: &str) -> SFResult<()> {
        let token = self.auth_header()?;
        let body = serde_json::json!({ "room": name });
        let req = HttpRequest::post(self.room_svc_url("DeleteRoom"))
            .header("Authorization", token)
            .json(&body)
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("delete_room serialize failed: {}", e),
            })?
            .timeout(30);
        let resp = self
            .client()?
            .execute(req)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("delete_room request failed: {}", e),
            })?;

        if !resp.is_success() {
            let text = resp.text().unwrap_or_default();
            return Err(SFError::Adapter {
                provider: "livekit".into(),
                message: format!("delete_room failed: {}", text),
            });
        }
        info!("LiveKit room deleted: {}", name);
        Ok(())
    }

    async fn list_rooms(&self) -> SFResult<Vec<MediaRoom>> {
        let token = self.auth_header()?;
        let req = HttpRequest::post(self.room_svc_url("ListRooms"))
            .header("Authorization", token)
            .json(&serde_json::json!({}))
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("list_rooms serialize failed: {}", e),
            })?
            .timeout(30);
        let resp = self
            .client()?
            .execute(req)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("list_rooms request failed: {}", e),
            })?;

        if !resp.is_success() {
            let text = resp.text().unwrap_or_default();
            return Err(SFError::Adapter {
                provider: "livekit".into(),
                message: format!("list_rooms failed: {}", text),
            });
        }

        let json: serde_json::Value = resp.json().map_err(|e| SFError::Adapter {
            provider: "livekit".into(),
            message: format!("list_rooms parse failed: {}", e),
        })?;

        let rooms = json
            .get("rooms")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|r| {
                        Some(MediaRoom {
                            name: r.get("name")?.as_str()?.to_string(),
                            sid: r.get("sid")?.as_str()?.to_string(),
                            participant_count: r.get("numParticipants")?.as_u64()? as u32,
                            created_at: chrono::Utc::now(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(rooms)
    }

    async fn generate_token(&self, room: &str, identity: &str) -> SFResult<String> {
        let token = self.generate_participant_token(room, identity)?;
        Ok(token)
    }

    async fn start_recording(&self, room: &str) -> SFResult<String> {
        let token = self.auth_header()?;
        let recording_id = format!("rec-{room}-{}", uuid::Uuid::new_v4());
        let body = serde_json::json!({
            "roomName": room,
            "fileOutputs": [{
                "filepath": format!("/recordings/{}.mp4", recording_id),
            }],
        });
        let req = HttpRequest::post(self.egress_svc_url("StartRoomCompositeEgress"))
            .header("Authorization", token)
            .json(&body)
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("start_recording serialize failed: {}", e),
            })?
            .timeout(30);
        let resp = self
            .client()?
            .execute(req)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("start_recording request failed: {}", e),
            })?;

        if !resp.is_success() {
            let text = resp.text().unwrap_or_default();
            return Err(SFError::Adapter {
                provider: "livekit".into(),
                message: format!("start_recording failed: {}", text),
            });
        }

        info!(
            "LiveKit recording started for room {}: id={}",
            room, recording_id
        );
        Ok(recording_id)
    }

    async fn stop_recording(&self, recording_id: &str) -> SFResult<()> {
        let token = self.auth_header()?;
        let body = serde_json::json!({ "egressId": recording_id });
        let req = HttpRequest::post(self.egress_svc_url("StopEgress"))
            .header("Authorization", token)
            .json(&body)
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("stop_recording serialize failed: {}", e),
            })?
            .timeout(30);
        let resp = self
            .client()?
            .execute(req)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "livekit".into(),
                message: format!("stop_recording request failed: {}", e),
            })?;

        if !resp.is_success() {
            let text = resp.text().unwrap_or_default();
            return Err(SFError::Adapter {
                provider: "livekit".into(),
                message: format!("stop_recording failed: {}", text),
            });
        }
        info!("LiveKit recording stopped: {}", recording_id);
        Ok(())
    }

    async fn list_recordings(&self) -> SFResult<Vec<RecordingSession>> {
        // LiveKit Egress API does not have a direct "list all" endpoint;
        // we return an empty list here and expect callers to track IDs locally.
        warn!("list_recordings is not supported by LiveKit Egress API; returning empty list");
        Ok(Vec::new())
    }
}

// ─── Helpers ───

fn base64url_encode(input: &str) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    URL_SAFE_NO_PAD.encode(input.as_bytes())
}

fn hmac_sha256(secret: &str, data: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any size");
    mac.update(data.as_bytes());
    let result = mac.finalize();
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    URL_SAFE_NO_PAD.encode(result.into_bytes())
}
