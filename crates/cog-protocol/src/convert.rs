//! Conversion bridges between Rust-native cog-core types and protobuf wire types.

use bytes::{Buf, BufMut, BytesMut};
use chrono::{DateTime, TimeZone};
use prost::Message;
use uuid::Uuid;

use cog_core::raw_logger::{RawContext, RawMeta, RawPayload, RawRecord};
use cog_core::wal::{WalError, WalEventType, WalRecord};
use cog_core::{SFError, SFResult};

use super::raw;
use super::wal as wal_proto;

pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

// ─── RawRecord ↔ raw::RawRecord conversions ───

impl From<&RawRecord> for raw::RawRecord {
    fn from(rec: &RawRecord) -> Self {
        let payload_json = serde_json::to_vec(&rec.payload).unwrap_or_default();
        raw::RawRecord {
            meta: Some(raw::RawMeta {
                version: rec.meta.version.clone(),
                stream: rec.meta.stream.clone(),
                recorded_at_unix_nanos: rec.meta.recorded_at.timestamp_nanos_opt().unwrap_or(0),
                recorded_by: rec.meta.recorded_by.clone(),
                sequence: rec.meta.sequence,
                trace_id: rec.meta.trace_id.clone(),
                span_id: rec.meta.span_id.clone(),
            }),
            context: Some(raw::RawContext {
                session_id: rec.context.session_id.map(|u| u.to_string()),
                user_id: rec.context.user_id.map(|u| u.to_string()),
                workspace_id: rec.context.workspace_id.map(|u| u.to_string()),
                agent_id: rec.context.agent_id.clone(),
                task_id: rec.context.task_id.map(|u| u.to_string()),
            }),
            payload_json,
        }
    }
}

impl TryFrom<raw::RawRecord> for RawRecord {
    type Error = SFError;

    fn try_from(p: raw::RawRecord) -> Result<Self, Self::Error> {
        let meta = p
            .meta
            .ok_or_else(|| SFError::Agent("proto RawRecord missing meta".into()))?;
        let context = p.context.unwrap_or_default();

        let recorded_at = chrono::Utc.timestamp_nanos(meta.recorded_at_unix_nanos);

        let payload: RawPayload = if p.payload_json.is_empty() {
            RawPayload {
                direction: String::new(),
                transport: String::new(),
                format: None,
                raw: serde_json::Value::Null,
            }
        } else {
            serde_json::from_slice(&p.payload_json)
                .map_err(|e| SFError::Agent(format!("invalid payload_json: {}", e)))?
        };

        Ok(RawRecord {
            meta: RawMeta {
                version: meta.version,
                stream: meta.stream,
                recorded_at,
                recorded_by: meta.recorded_by,
                sequence: meta.sequence,
                trace_id: meta.trace_id,
                span_id: meta.span_id,
            },
            context: RawContext {
                session_id: parse_uuid(context.session_id)?,
                user_id: parse_uuid(context.user_id)?,
                workspace_id: parse_uuid(context.workspace_id)?,
                agent_id: context.agent_id,
                task_id: parse_uuid(context.task_id)?,
            },
            payload,
        })
    }
}

fn parse_uuid(s: Option<String>) -> SFResult<Option<Uuid>> {
    match s {
        Some(v) if !v.is_empty() => Uuid::parse_str(&v)
            .map(Some)
            .map_err(|e| SFError::Agent(format!("invalid uuid '{}': {}", v, e))),
        _ => Ok(None),
    }
}

// ─── Single-record encode/decode ───

/// Encode one [`RawRecord`] to its protobuf wire bytes.
pub fn encode_record(record: &RawRecord) -> Vec<u8> {
    let p: raw::RawRecord = record.into();
    let mut buf = Vec::with_capacity(p.encoded_len());
    p.encode(&mut buf).expect("encoding into Vec cannot fail");
    buf
}

/// Decode one [`RawRecord`] from raw protobuf wire bytes (no length prefix).
pub fn decode_record(bytes: &[u8]) -> SFResult<RawRecord> {
    let p = raw::RawRecord::decode(bytes)
        .map_err(|e| SFError::Agent(format!("proto decode failed: {}", e)))?;
    RawRecord::try_from(p)
}

// ─── Length-delimited stream encode/decode ───

/// Append a single record to `buf` using prost's length-delimited framing
/// (varint length prefix + payload).
pub fn append_delimited(buf: &mut Vec<u8>, record: &RawRecord) -> SFResult<()> {
    let p: raw::RawRecord = record.into();
    p.encode_length_delimited(buf)
        .map_err(|e| SFError::Agent(format!("proto length-delimited encode failed: {}", e)))
}

/// Decode every record from a length-delimited byte stream produced by
/// [`append_delimited`].
pub fn decode_all_delimited(mut bytes: &[u8]) -> SFResult<Vec<RawRecord>> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let mut tmp = BytesMut::with_capacity(bytes.len());
        tmp.put_slice(bytes);
        match raw::RawRecord::decode_length_delimited(&mut tmp) {
            Ok(p) => {
                out.push(RawRecord::try_from(p)?);
                let consumed = bytes.len() - tmp.remaining();
                bytes = &bytes[consumed..];
            }
            Err(e) => {
                return Err(SFError::Agent(format!(
                    "proto delimited decode failed: {}",
                    e
                )));
            }
        }
    }
    Ok(out)
}

// ─── zstd helpers ───

/// Compress a byte stream with zstd at the given level (3 = balanced).
pub fn zstd_compress(bytes: &[u8], level: i32) -> SFResult<Vec<u8>> {
    zstd::stream::encode_all(bytes, level)
        .map_err(|e| SFError::Agent(format!("zstd compress failed: {}", e)))
}

/// Decompress a zstd frame produced by [`zstd_compress`].
pub fn zstd_decompress(bytes: &[u8]) -> SFResult<Vec<u8>> {
    zstd::stream::decode_all(bytes)
        .map_err(|e| SFError::Agent(format!("zstd decompress failed: {}", e)))
}

// ─── WalRecord ↔ wal_proto::WalRecord conversions ───

impl From<&WalEventType> for wal_proto::WalEventType {
    fn from(ty: &WalEventType) -> Self {
        match ty {
            WalEventType::AgentStart => wal_proto::WalEventType::AgentStart,
            WalEventType::AgentEnd => wal_proto::WalEventType::AgentEnd,
            WalEventType::TurnStart => wal_proto::WalEventType::TurnStart,
            WalEventType::TurnEnd => wal_proto::WalEventType::TurnEnd,
            WalEventType::MessageStart => wal_proto::WalEventType::MessageStart,
            WalEventType::MessageDelta => wal_proto::WalEventType::MessageDelta,
            WalEventType::MessageEnd => wal_proto::WalEventType::MessageEnd,
            WalEventType::ToolExecutionStart => wal_proto::WalEventType::ToolExecutionStart,
            WalEventType::ToolExecutionDelta => wal_proto::WalEventType::ToolExecutionDelta,
            WalEventType::ToolExecutionEnd => wal_proto::WalEventType::ToolExecutionEnd,
            WalEventType::StateChange => wal_proto::WalEventType::StateChange,
            WalEventType::TaskStatusChange => wal_proto::WalEventType::TaskStatusChange,
            WalEventType::SelfReview => wal_proto::WalEventType::SelfReview,
            WalEventType::ReActStepStart => wal_proto::WalEventType::ReactStepStart,
            WalEventType::ReActStepEnd => wal_proto::WalEventType::ReactStepEnd,
            WalEventType::AgentError => wal_proto::WalEventType::AgentError,
            WalEventType::ResourceAlert => wal_proto::WalEventType::ResourceAlert,
            WalEventType::Heartbeat => wal_proto::WalEventType::Heartbeat,
            WalEventType::CheckpointSaved => wal_proto::WalEventType::CheckpointSaved,
            WalEventType::Custom { .. } => wal_proto::WalEventType::Custom,
        }
    }
}

impl From<wal_proto::WalEventType> for WalEventType {
    fn from(ty: wal_proto::WalEventType) -> Self {
        match ty {
            wal_proto::WalEventType::AgentStart => WalEventType::AgentStart,
            wal_proto::WalEventType::AgentEnd => WalEventType::AgentEnd,
            wal_proto::WalEventType::TurnStart => WalEventType::TurnStart,
            wal_proto::WalEventType::TurnEnd => WalEventType::TurnEnd,
            wal_proto::WalEventType::MessageStart => WalEventType::MessageStart,
            wal_proto::WalEventType::MessageDelta => WalEventType::MessageDelta,
            wal_proto::WalEventType::MessageEnd => WalEventType::MessageEnd,
            wal_proto::WalEventType::ToolExecutionStart => WalEventType::ToolExecutionStart,
            wal_proto::WalEventType::ToolExecutionDelta => WalEventType::ToolExecutionDelta,
            wal_proto::WalEventType::ToolExecutionEnd => WalEventType::ToolExecutionEnd,
            wal_proto::WalEventType::StateChange => WalEventType::StateChange,
            wal_proto::WalEventType::TaskStatusChange => WalEventType::TaskStatusChange,
            wal_proto::WalEventType::SelfReview => WalEventType::SelfReview,
            wal_proto::WalEventType::ReactStepStart => WalEventType::ReActStepStart,
            wal_proto::WalEventType::ReactStepEnd => WalEventType::ReActStepEnd,
            wal_proto::WalEventType::AgentError => WalEventType::AgentError,
            wal_proto::WalEventType::ResourceAlert => WalEventType::ResourceAlert,
            wal_proto::WalEventType::Heartbeat => WalEventType::Heartbeat,
            wal_proto::WalEventType::CheckpointSaved => WalEventType::CheckpointSaved,
            wal_proto::WalEventType::Custom => WalEventType::Custom {
                name: String::new(),
            },
            wal_proto::WalEventType::Unspecified => WalEventType::Custom {
                name: "unspecified".into(),
            },
        }
    }
}

impl From<&WalRecord> for wal_proto::WalRecord {
    fn from(rec: &WalRecord) -> Self {
        wal_proto::WalRecord {
            seq: rec.seq,
            session_id: rec.session_id.clone(),
            event_type: wal_proto::WalEventType::from(&rec.event_type) as i32,
            payload_json: serde_json::to_vec(&rec.payload).unwrap_or_default(),
            timestamp_rfc3339: rec.timestamp.to_rfc3339(),
            checksum: rec.checksum.clone(),
        }
    }
}

impl TryFrom<wal_proto::WalRecord> for WalRecord {
    type Error = WalError;

    fn try_from(p: wal_proto::WalRecord) -> Result<Self, Self::Error> {
        let payload = if p.payload_json.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&p.payload_json).map_err(WalError::Serialization)?
        };

        let timestamp = DateTime::parse_from_rfc3339(&p.timestamp_rfc3339)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());

        let event_type = wal_proto::WalEventType::try_from(p.event_type)
            .map(WalEventType::from)
            .unwrap_or(WalEventType::Custom {
                name: "unknown".into(),
            });

        Ok(WalRecord {
            seq: p.seq,
            session_id: p.session_id,
            event_type,
            payload,
            timestamp,
            checksum: p.checksum,
        })
    }
}

/// WAL encode/decode helpers.
pub mod wal_codec {
    use super::*;

    /// Encode a [`WalRecord`] to protobuf wire bytes (no length prefix).
    pub fn encode(record: &WalRecord) -> Vec<u8> {
        let p: wal_proto::WalRecord = record.into();
        let mut buf = Vec::with_capacity(p.encoded_len());
        p.encode(&mut buf).expect("encoding into Vec cannot fail");
        buf
    }

    /// Decode a [`WalRecord`] from protobuf wire bytes (no length prefix).
    pub fn decode(bytes: &[u8]) -> Result<WalRecord, WalError> {
        let p = wal_proto::WalRecord::decode(bytes)
            .map_err(|e| WalError::Backend(format!("proto decode failed: {}", e)))?;
        WalRecord::try_from(p)
    }

    /// Encode using length-delimited protobuf framing.
    pub fn encode_length_delimited(record: &WalRecord) -> Result<Vec<u8>, WalError> {
        let p: wal_proto::WalRecord = record.into();
        let mut buf = Vec::with_capacity(p.encoded_len() + 8);
        p.encode_length_delimited(&mut buf)
            .map_err(|e| WalError::Backend(format!("proto encode failed: {}", e)))?;
        Ok(buf)
    }

    /// Decode a length-delimited record; returns (record, bytes_consumed).
    pub fn decode_length_delimited(bytes: &[u8]) -> Result<(WalRecord, usize), WalError> {
        let mut buf = bytes::Bytes::copy_from_slice(bytes);
        let p = wal_proto::WalRecord::decode_length_delimited(&mut buf)
            .map_err(|e| WalError::Backend(format!("proto decode failed: {}", e)))?;
        let consumed = bytes.len() - buf.remaining();
        let rec = WalRecord::try_from(p)?;
        Ok((rec, consumed))
    }
}

/// Zero-sized type that implements the core codec traits using prost protobuf.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProtoCodec;

impl cog_core::WalCodec for ProtoCodec {
    fn encode_length_delimited(&self, record: &WalRecord) -> Result<Vec<u8>, WalError> {
        wal_codec::encode_length_delimited(record)
    }

    fn decode_length_delimited(&self, bytes: &[u8]) -> Result<(WalRecord, usize), WalError> {
        wal_codec::decode_length_delimited(bytes)
    }
}

impl cog_core::RawRecordCodec for ProtoCodec {
    fn append_delimited(
        &self,
        buf: &mut Vec<u8>,
        record: &cog_core::raw_logger::RawRecord,
    ) -> cog_core::SFResult<()> {
        append_delimited(buf, record)
    }

    fn decode_all_delimited(
        &self,
        bytes: &[u8],
    ) -> cog_core::SFResult<Vec<cog_core::raw_logger::RawRecord>> {
        decode_all_delimited(bytes)
    }

    fn decode_record(&self, bytes: &[u8]) -> cog_core::SFResult<cog_core::raw_logger::RawRecord> {
        decode_record(bytes)
    }
}
