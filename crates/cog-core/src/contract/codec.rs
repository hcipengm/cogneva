/// Codec trait for WAL protobuf length-delimited encoding/decoding.
/// Implementations live in `cog-protocol` so that `cog-core` stays free of
/// `prost` dependencies, while `cog-storage` can depend only on `cog-core`.
pub trait WalCodec: Send + Sync + std::fmt::Debug {
    fn encode_length_delimited(
        &self,
        record: &crate::WalRecord,
    ) -> Result<Vec<u8>, crate::WalError>;
    fn decode_length_delimited(
        &self,
        bytes: &[u8],
    ) -> Result<(crate::WalRecord, usize), crate::WalError>;
}

/// Codec trait for raw-record protobuf encoding/decoding.
pub trait RawRecordCodec: Send + Sync + std::fmt::Debug {
    fn append_delimited(
        &self,
        buf: &mut Vec<u8>,
        record: &crate::storage::RawRecord,
    ) -> crate::SFResult<()>;
    fn decode_all_delimited(&self, bytes: &[u8])
        -> crate::SFResult<Vec<crate::storage::RawRecord>>;
    fn decode_record(&self, bytes: &[u8]) -> crate::SFResult<crate::storage::RawRecord>;
}
