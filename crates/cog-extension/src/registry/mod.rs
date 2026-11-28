//! Extension registry — plugin discovery, signature-checked fetching, and
//! registration.

pub mod discovery;
pub mod signature;

use cog_core::{PluginManifest, PluginRegistry, SFResult};

/// Concrete implementation of [`PluginRegistry`].
pub struct PluginRegistryImpl {
    local_dir: String,
    _remote_url: Option<String>,
}

impl PluginRegistryImpl {
    pub fn new(local_dir: String, remote_url: Option<String>) -> Self {
        Self {
            local_dir,
            _remote_url: remote_url,
        }
    }
}

#[async_trait::async_trait]
impl PluginRegistry for PluginRegistryImpl {
    async fn discover(&self, source: &str) -> SFResult<Vec<PluginManifest>> {
        discovery::discover_local(source).await
    }

    async fn fetch(&self, manifest: &PluginManifest) -> SFResult<Vec<u8>> {
        let path = std::path::Path::new(&self.local_dir).join(&manifest.entry);
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("plugin fetch failed: {}", e)))?;
        // 契约那一行写的是「下载并校验签名」。校验搬到这里而不是留在原处：
        // 它原先只被 `load` 调用，而 `load` 已经不在了——把没校验过的字节
        // 交出去，清单里的 `signature` 字段就只是一个没人读的约定。
        signature::verify(manifest, &bytes)?;
        Ok(bytes)
    }

    async fn fetch_by_id(&self, plugin_id: &str) -> SFResult<Vec<u8>> {
        let manifests = self.discover(&self.local_dir).await?;
        let manifest = manifests
            .into_iter()
            .find(|m| m.name == plugin_id)
            .ok_or_else(|| cog_core::SFError::Agent(format!("plugin not found: {}", plugin_id)))?;
        self.fetch(&manifest).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// 签名域按 `hex 公钥:hex 签名` 写。密钥固定，测试不用随机数就能重放同一份签名。
    fn signature_over(bytes: &[u8]) -> String {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let signature = key.sign(bytes);
        format!(
            "{}:{}",
            hex::encode(key.verifying_key().to_bytes()),
            hex::encode(signature.to_bytes())
        )
    }

    fn manifest(entry: &str, signature: String) -> PluginManifest {
        PluginManifest {
            name: "demo".into(),
            version: "0.1.0".into(),
            author: "test".into(),
            capabilities: vec![],
            entry: entry.into(),
            signature,
            sandbox: None,
        }
    }

    /// 取回来的字节必须就是签名覆盖的那一份：签名不覆盖的字节不许交出去。
    #[tokio::test]
    async fn fetch_hands_back_bytes_only_under_the_manifest_signature() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bytes = b"\0asm\x01\0\0\0".to_vec();
        tokio::fs::write(dir.path().join("plugin.wasm"), &bytes)
            .await
            .expect("entry file is written");
        let registry = PluginRegistryImpl::new(dir.path().to_string_lossy().into_owned(), None);

        let signed = manifest("plugin.wasm", signature_over(&bytes));
        assert_eq!(
            registry
                .fetch(&signed)
                .await
                .expect("签名覆盖的就是这些字节"),
            bytes
        );

        let mut tampered = signed.clone();
        tampered.signature = signature_over(b"another module");
        let error = registry
            .fetch(&tampered)
            .await
            .expect_err("签名不覆盖这份字节，就不许交出去");
        assert!(
            error.to_string().contains("verification failed"),
            "拒绝要说清是签名对不上：{error}"
        );
    }
}
