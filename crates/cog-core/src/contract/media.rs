//!Media backend trait — audio/video rooms, streaming, and recording.

use crate::SFResult;
use async_trait::async_trait;

/// A room descriptor returned by the media backend.
#[derive(Debug, Clone)]
pub struct MediaRoom {
    pub name: String,
    pub sid: String,
    pub participant_count: u32,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// A recording session descriptor.
#[derive(Debug, Clone)]
pub struct RecordingSession {
    pub id: String,
    pub room_name: String,
    pub status: RecordingStatus,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecordingStatus {
    Starting,
    Active,
    Stopped,
    Failed,
}

/// Configuration for connecting to a media server.
#[derive(Debug, Clone)]
pub struct MediaBackendConfig {
    pub api_key: String,
    pub api_secret: String,
    pub server_url: String,
}

/// Media backend — room management, token generation, and recording.
#[async_trait]
pub trait MediaBackend: Send + Sync {
    /// Create a new room. Returns the room SID.
    async fn create_room(&self, name: &str) -> SFResult<String>;

    /// Delete a room by name.
    async fn delete_room(&self, name: &str) -> SFResult<()>;

    /// List active rooms.
    async fn list_rooms(&self) -> SFResult<Vec<MediaRoom>>;

    /// Generate an access token for a participant.
    async fn generate_token(&self, room: &str, identity: &str) -> SFResult<String>;

    /// Start recording a room. Returns the recording session ID.
    async fn start_recording(&self, room: &str) -> SFResult<String>;

    /// Stop a recording session.
    async fn stop_recording(&self, recording_id: &str) -> SFResult<()>;

    /// List active recordings.
    async fn list_recordings(&self) -> SFResult<Vec<RecordingSession>>;
}
