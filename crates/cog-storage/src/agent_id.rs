//! Agent id generation — moved from `cog-core` so the domain-kernel
//! stays free of hashing logic.

/// Generate a deterministic agent_id from the four input dimensions.
pub fn generate_agent_id(hostname: &str, pod_ip: &str, role: &str, uuid: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(hostname.as_bytes());
    hasher.update(b"|");
    hasher.update(pod_ip.as_bytes());
    hasher.update(b"|");
    hasher.update(role.as_bytes());
    hasher.update(b"|");
    hasher.update(uuid.as_bytes());
    let hash = hasher.finalize();
    hash.to_hex().to_string()
}
