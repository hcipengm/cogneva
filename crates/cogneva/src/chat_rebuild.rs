//! `cogneva chat-rebuild <session>`：照着流把一段会话的 `chat_turns` 重建回来。
//!
//! 轮的根是意图事件；附件按对象存储的 key 取字节，工具调用按 `artifact://` 取正文。
//! 这两样从哪里取由这个装配层决定，重建本身（存储层）只认端口。

use std::sync::Arc;

use async_trait::async_trait;

use cog_core::{ArchivedBodyReader, AttachmentBytes, ObjectBackend, SFResult};

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let session = std::env::args()
        .skip_while(|arg| arg != "chat-rebuild")
        .nth(1)
        .ok_or("usage: cogneva chat-rebuild <session-id>")?;
    let config = crate::config_loader::load();
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let pool = sqlx::PgPool::connect(&url).await?;

    let http: Option<Arc<dyn cog_core::HttpClient>> = match cog_net::HttpClientConfig::load()
        .and_then(|config| cog_net::ReqwestHttpClient::from_config(&config))
    {
        Ok(client) => Some(Arc::new(client)),
        Err(e) => {
            tracing::warn!(error = %e, "chat-rebuild: no HTTP client; archived tool bodies cannot be read");
            None
        }
    };
    let backend = cog_storage::build_object_backend(
        &config.providers.storage,
        &config.app.data_dir,
        http.clone(),
    )?;
    let agent_loop = cog_agent::AgentLoopConfig::load()?;
    let ports = Ports {
        objects: backend,
        http,
        memory_base: agent_loop.memory_api_base.clone(),
    };
    let written = cog_storage::rebuild_chat_turns(&pool, &session, &ports, &ports).await?;
    println!("chat-rebuild: {written} turn(s) rebuilt for {session}");
    Ok(())
}

/// 两个端口由同一次装配给出：对象存储与 memory 读回口用的是同一份配置。
struct Ports {
    objects: Arc<dyn ObjectBackend>,
    http: Option<Arc<dyn cog_core::HttpClient>>,
    memory_base: Option<String>,
}

#[async_trait]
impl AttachmentBytes for Ports {
    async fn get(&self, key: &str) -> SFResult<Vec<u8>> {
        self.objects
            .get(key)
            .await?
            .ok_or_else(|| cog_core::SFError::Database(format!("no object at {key}")))
    }
}

#[async_trait]
impl ArchivedBodyReader for Ports {
    async fn read(&self, uri: &str) -> SFResult<String> {
        let client = self.http.as_ref().ok_or_else(|| {
            cog_core::SFError::Config(
                "no HTTP client is configured: an artifact cannot be read back".into(),
            )
        })?;
        cog_agent::archive::read_artifact(client.as_ref(), self.memory_base.as_deref(), uri).await
    }
}
