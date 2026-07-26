//! Ed25519 signature verification for plugin integrity.

use cog_core::{PluginManifest, SFResult};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

/// Verify that `bytes` is signed by the public key embedded in `manifest.signature`.
pub fn verify(manifest: &PluginManifest, bytes: &[u8]) -> SFResult<()> {
    // MVP: signature field is expected to be "hex_pubkey:hex_signature".
    let parts: Vec<&str> = manifest.signature.split(':').collect();
    if parts.len() != 2 {
        return Err(cog_core::SFError::Agent(
            "invalid signature format, expected 'pubkey:sig'".into(),
        ));
    }

    let pubkey_bytes = hex::decode(parts[0])
        .map_err(|e| cog_core::SFError::Agent(format!("invalid pubkey hex: {}", e)))?;
    let sig_bytes = hex::decode(parts[1])
        .map_err(|e| cog_core::SFError::Agent(format!("invalid signature hex: {}", e)))?;

    let verifying_key = VerifyingKey::from_bytes(
        &pubkey_bytes
            .try_into()
            .map_err(|_| cog_core::SFError::Agent("pubkey length invalid".into()))?,
    )
    .map_err(|e| cog_core::SFError::Agent(format!("pubkey parse error: {}", e)))?;

    let signature = Signature::from_bytes(
        &sig_bytes
            .try_into()
            .map_err(|_| cog_core::SFError::Agent("signature length invalid".into()))?,
    );

    verifying_key
        .verify(bytes, &signature)
        .map_err(|e| cog_core::SFError::Agent(format!("signature verification failed: {}", e)))?;

    Ok(())
}
