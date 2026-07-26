//! HashiCorp Vault 密钥提供者（KV v2）。
//!
//! `reference` 格式：`mount/path#field`，例如 `secret/data/llm#api_key`。
//! 认证使用 `VAULT_TOKEN` 环境变量或构造时显式传入的 token。

use cog_core::{SFError, SFResult, SecretProvider};

/// Vault KV v2 密钥提供者。
pub struct VaultSecretProvider {
    client: reqwest::Client,
    addr: String,
    token: String,
}

impl VaultSecretProvider {
    /// `addr` 例如 `http://127.0.0.1:8200`；`token` 为 Vault token。
    pub fn new(addr: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            addr: addr.into().trim_end_matches('/').to_string(),
            token: token.into(),
        }
    }

    /// 从环境变量构造：`VAULT_ADDR` + `VAULT_TOKEN`。
    pub fn from_env() -> Option<Self> {
        let addr = std::env::var("VAULT_ADDR").ok().filter(|v| !v.is_empty())?;
        let token = std::env::var("VAULT_TOKEN")
            .ok()
            .filter(|v| !v.is_empty())?;
        Some(Self::new(addr, token))
    }
}

#[async_trait::async_trait]
impl SecretProvider for VaultSecretProvider {
    fn name(&self) -> &'static str {
        "vault"
    }

    async fn get(&self, reference: &str) -> SFResult<Option<String>> {
        let (path, field) = reference.split_once('#').ok_or_else(|| {
            SFError::Validation(format!(
                "vault reference must be 'mount/path#field', got: {reference}"
            ))
        })?;
        if path.split('/').any(|seg| seg == "..") {
            return Err(SFError::Validation(format!(
                "vault reference escapes mount: {reference}"
            )));
        }

        let url = format!("{}/v1/{}", self.addr, path);
        let response = self
            .client
            .get(&url)
            .header("X-Vault-Token", &self.token)
            .send()
            .await
            .map_err(|e| SFError::Config(format!("vault request failed: {e}")))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(SFError::Config(format!(
                "vault returned status {}",
                response.status()
            )));
        }

        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| SFError::Config(format!("vault response parse failed: {e}")))?;
        let value = body
            .pointer(&format!("/data/data/{}", field.replace('.', "/")))
            .or_else(|| body.pointer(&format!("/data/{}", field)))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_reference_without_field() {
        let provider = VaultSecretProvider::new("http://127.0.0.1:1", "token");
        let err = provider.get("secret/data/llm").await.unwrap_err();
        assert!(err.to_string().contains("mount/path#field"));
    }

    #[tokio::test]
    async fn rejects_path_escape() {
        let provider = VaultSecretProvider::new("http://127.0.0.1:1", "token");
        let err = provider.get("secret/../admin#key").await.unwrap_err();
        assert!(err.to_string().contains("escapes mount"));
    }

    #[test]
    fn from_env_requires_both_vars() {
        std::env::remove_var("VAULT_ADDR");
        std::env::remove_var("VAULT_TOKEN");
        assert!(VaultSecretProvider::from_env().is_none());
        std::env::set_var("VAULT_ADDR", "http://127.0.0.1:8200");
        assert!(VaultSecretProvider::from_env().is_none());
        std::env::set_var("VAULT_TOKEN", "tok");
        assert!(VaultSecretProvider::from_env().is_some());
        std::env::remove_var("VAULT_ADDR");
        std::env::remove_var("VAULT_TOKEN");
    }
}
