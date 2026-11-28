//! 密封包的外层：签名信封。
//!
//! 包里有一层「是不是按哈希点名的那一包」（`principle_bundle`），外面这一层回答
//! 「这包是不是发布者签的」。分开两层：哈希对而签名不对是包被换过；签名对而哈希
//! 不对是取回来的字节坏了。加密交给对象存储的静态加密（SSE/KMS）——那是部署的
//! 结构约定，不在应用层再造一套对称密钥。
//!
//! 算法只认 ed25519；`alg` 不认识就拒，不猜。

use base64::{engine::general_purpose::STANDARD, Engine as _};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

/// 挂在包外面的签名信封。四个字段都是 base64（`alg` 除外）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedBundle {
    /// 签名算法；这一版只认 `ed25519`。
    pub alg: String,
    /// 哪把钥匙签的：页面与告警都按它点名。
    pub key_id: String,
    /// 包的原文（JSON 字节）。
    pub payload: String,
    /// 对 `payload` 原文的签名。
    pub signature: String,
}

/// 验签并交出包的原文。
pub fn open_sealed(envelope: &[u8], verifying_key_b64: &str) -> Result<Vec<u8>, String> {
    let sealed: SealedBundle =
        serde_json::from_slice(envelope).map_err(|e| format!("sealed bundle is not JSON: {e}"))?;
    if sealed.alg != "ed25519" {
        return Err(format!("unknown signature algorithm {}", sealed.alg));
    }
    let key_bytes = STANDARD
        .decode(&verifying_key_b64)
        .map_err(|e| format!("verifying key is not base64: {e}"))?;
    let key_bytes: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| "verifying key is not 32 bytes".to_string())?;
    let key =
        VerifyingKey::from_bytes(&key_bytes).map_err(|e| format!("bad verifying key: {e}"))?;
    let sig_bytes = STANDARD
        .decode(&sealed.signature)
        .map_err(|e| format!("signature is not base64: {e}"))?;
    let sig_bytes: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| "signature is not 64 bytes".to_string())?;
    let signature = Signature::from_bytes(&sig_bytes);
    let payload = STANDARD
        .decode(&sealed.payload)
        .map_err(|e| format!("payload is not base64: {e}"))?;
    key.verify(&payload, &signature)
        .map_err(|_| format!("bundle is not signed by key {}", sealed.key_id))?;
    Ok(payload)
}

/// 两步一起做：先验签，再按内容哈希与逐条正文哈希解包。
///
/// `expected_sha256` 是调用方点名的**包原文**哈希（不是信封的）：签名证明是谁发的，
/// 哈希证明取回来的是哪一包。
pub fn load_bundle(
    envelope: &[u8],
    verifying_key_b64: &str,
    expected_sha256: &str,
) -> Result<super::principle_bundle::PrincipleBundle, String> {
    let payload = open_sealed(envelope, verifying_key_b64)?;
    super::principle_bundle::parse_bundle(&payload, expected_sha256)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn sealed_with(key: &SigningKey, payload: &[u8], key_id: &str) -> SealedBundle {
        SealedBundle {
            alg: "ed25519".into(),
            key_id: key_id.into(),
            payload: STANDARD.encode(payload),
            signature: STANDARD.encode(key.sign(payload).to_bytes()),
        }
    }

    #[test]
    fn a_signed_bundle_opens_with_its_key() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let sealed = sealed_with(&key, b"{\"version\":1}", "release-1");
        let bytes = serde_json::to_vec(&sealed).unwrap();
        let opened = open_sealed(&bytes, &STANDARD.encode(key.verifying_key().to_bytes()))
            .expect("a bundle signed by this key opens");
        assert_eq!(opened, b"{\"version\":1}");
    }

    #[test]
    fn a_tampered_payload_is_refused() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let mut sealed = sealed_with(&key, b"{\"version\":1}", "release-1");
        sealed.payload = STANDARD.encode(b"{\"version\":2}");
        let bytes = serde_json::to_vec(&sealed).unwrap();
        let err =
            open_sealed(&bytes, &STANDARD.encode(key.verifying_key().to_bytes())).unwrap_err();
        assert!(err.contains("not signed by key"), "{err}");
    }

    #[test]
    fn another_key_cannot_open_it() {
        let signer = SigningKey::from_bytes(&[7u8; 32]);
        let other = SigningKey::from_bytes(&[9u8; 32]);
        let sealed = sealed_with(&signer, b"{}", "release-1");
        let bytes = serde_json::to_vec(&sealed).unwrap();
        assert!(open_sealed(&bytes, &STANDARD.encode(other.verifying_key().to_bytes())).is_err());
    }

    #[test]
    fn loading_composes_signature_and_hash_checks() {
        use sha2::{Digest, Sha256};
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let body = "数值不硬编码。".to_string();
        let bundle = super::super::principle_bundle::PrincipleBundle {
            version: super::super::principle_bundle::BUNDLE_VERSION,
            built_at: "2026-11-25T00:00:00Z".into(),
            principles: vec![super::super::principle_bundle::BundledPrinciple {
                slug: "s".into(),
                source: "list-principles.sh".into(),
                lead: "l".into(),
                carrier_path: "m.md".into(),
                carrier_sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
                host_gate: None,
                where_red: None,
                check_kind: None,
                priority: None,
                themes: Vec::new(),
                body,
            }],
        };
        let payload = serde_json::to_vec(&bundle).unwrap();
        let sha = format!("{:x}", Sha256::digest(&payload));
        let sealed = sealed_with(&key, &payload, "release-1");
        let envelope = serde_json::to_vec(&sealed).unwrap();
        let key_b64 = STANDARD.encode(key.verifying_key().to_bytes());
        assert!(load_bundle(&envelope, &key_b64, &sha).is_ok());
        assert!(load_bundle(&envelope, &key_b64, &"0".repeat(64)).is_err());
    }

    #[test]
    fn an_unknown_algorithm_is_refused_by_name() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let mut sealed = sealed_with(&key, b"{}", "release-1");
        sealed.alg = "rsa".into();
        let bytes = serde_json::to_vec(&sealed).unwrap();
        let err =
            open_sealed(&bytes, &STANDARD.encode(key.verifying_key().to_bytes())).unwrap_err();
        assert!(err.contains("rsa"), "{err}");
    }
}
