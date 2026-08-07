//! Prompt 加载器 — 从文件系统加载 prompt，支持热重载。
//! 文件格式（YAML）：
//! ```yaml
//! prompts:
//!   self_review:critique:
//!     content: "Critique the following..."
//!     version: "1.0.0"
//!     description: "..."
//!     tags: ["agent", "review"]
//!     active: true
//! ```

use crate::registry::{PromptEntry, PromptRegistry, PromptSource};
use std::path::PathBuf;
use std::sync::RwLock;

/// 热重载模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchMode {
    /// 启动时加载一次，之后不监控文件变化。
    None,
    /// 监控文件变化，自动热重载。
    HotReload,
}

/// Prompt 加载器 trait。
#[async_trait::async_trait]
pub trait PromptLoader: Send + Sync {
    /// 加载所有 prompt 到注册中心。
    async fn load_all(&self, registry: &mut PromptRegistry) -> anyhow::Result<()>;

    /// 启动文件监控（热重载）。
    async fn watch(&self, registry: Arc<RwLock<PromptRegistry>>) -> anyhow::Result<()>;
}

use std::sync::Arc;

/// 文件系统加载器。
pub struct FileSystemLoader {
    root: PathBuf,
}

impl FileSystemLoader {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn scan_files(&self) -> Vec<PathBuf> {
        let mut files = vec![];
        if let Ok(entries) = std::fs::read_dir(&self.root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let ext = path.extension().and_then(|e| e.to_str());
                    if ext == Some("yaml") || ext == Some("yml") || ext == Some("json") {
                        files.push(path);
                    }
                }
            }
        }
        files
    }
}

#[async_trait::async_trait]
impl PromptLoader for FileSystemLoader {
    async fn load_all(&self, registry: &mut PromptRegistry) -> anyhow::Result<()> {
        let files = self.scan_files();
        for path in files {
            let content = tokio::fs::read_to_string(&path).await?;
            let ext = path.extension().and_then(|e| e.to_str());

            let entries: Vec<PromptEntry> = match ext {
                Some("json") => {
                    let wrapper: PromptFileWrapper = serde_json::from_str(&content)?;
                    wrapper.into_entries(&path)
                }
                _ => {
                    let wrapper: PromptFileWrapper = serde_yaml::from_str(&content)?;
                    wrapper.into_entries(&path)
                }
            };

            registry.batch_register(entries);
            tracing::info!(file = %path.display(), "Loaded prompts");
        }
        Ok(())
    }

    async fn watch(&self, registry: Arc<RwLock<PromptRegistry>>) -> anyhow::Result<()> {
        use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
        use std::sync::mpsc::channel;

        let (tx, rx) = channel::<Result<Event, notify::Error>>();
        let mut watcher = RecommendedWatcher::new(
            move |res| {
                let _ = tx.send(res);
            },
            Config::default(),
        )?;

        watcher.watch(&self.root, RecursiveMode::NonRecursive)?;
        tracing::info!(dir = %self.root.display(), "Prompt hot-reload watcher started");

        loop {
            // 不能用 rx.recv() 阻塞等待：本函数跑在 tokio worker 上，
            // 主 Pod CPU limit=1 时 tokio 只有 1 个 worker，阻塞 recv
            // 会冻住整个运行时（进程零日志假死，2026-08-06 实测定位）。
            let event = match rx.try_recv() {
                Ok(ev) => ev,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    tracing::warn!("Prompt watch channel disconnected");
                    break;
                }
            };
            match event {
                Ok(event) => {
                    let relevant = event.kind.is_modify()
                        || event.kind.is_create()
                        || event.kind.is_remove()
                        || matches!(event.kind, notify::EventKind::Any);
                    if !relevant {
                        continue;
                    }
                    // K8s configmap 挂载更新走 `..data` 符号链接整体切换，
                    // 事件路径不带 .yaml 后缀；逐文件热更捕捉不到。凡是出现
                    // 非 prompt 文件路径的事件（symlink 交换、目录重命名）
                    // 一律全量重扫，保证 configmap 更新一定触发热加载。
                    let has_prompt_file = event.paths.iter().any(|p| {
                        matches!(
                            p.extension().and_then(|e| e.to_str()),
                            Some("yaml") | Some("yml") | Some("json")
                        )
                    });
                    if !has_prompt_file {
                        // 先读+解析（await 段），再取写锁注册——写锁不是
                        // Send，持锁跨 await 会让整个 future 失去 Send。
                        let files = self.scan_files();
                        let mut parsed_entries = Vec::new();
                        for path in files {
                            match tokio::fs::read_to_string(&path).await {
                                Ok(content) => {
                                    let ext = path.extension().and_then(|e| e.to_str());
                                    let parsed = if ext == Some("json") {
                                        serde_json::from_str::<PromptFileWrapper>(&content)
                                            .map_err(|e| e.to_string())
                                    } else {
                                        serde_yaml::from_str::<PromptFileWrapper>(&content)
                                            .map_err(|e| e.to_string())
                                    };
                                    match parsed {
                                        Ok(w) => {
                                            parsed_entries.extend(w.into_entries(&path));
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "Failed to parse {}: {}",
                                                path.display(),
                                                e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to read {}: {}", path.display(), e);
                                }
                            }
                        }
                        let mut reg = registry.write().unwrap();
                        for entry in parsed_entries {
                            tracing::info!(
                                key = %entry.key,
                                "Hot-reloading prompt (configmap swap)"
                            );
                            reg.register(entry);
                        }
                        continue;
                    }
                    if event.kind.is_modify() || event.kind.is_create() {
                        for path in event.paths {
                            let ext = path.extension().and_then(|e| e.to_str());
                            if ext != Some("yaml") && ext != Some("yml") && ext != Some("json") {
                                continue;
                            }
                            match tokio::fs::read_to_string(&path).await {
                                Ok(content) => {
                                    let ext = path.extension().and_then(|e| e.to_str());
                                    let entries: Vec<PromptEntry> = match ext {
                                        Some("json") => {
                                            match serde_json::from_str::<PromptFileWrapper>(
                                                &content,
                                            ) {
                                                Ok(w) => w.into_entries(&path),
                                                Err(e) => {
                                                    tracing::warn!(
                                                        "Failed to parse {}: {}",
                                                        path.display(),
                                                        e
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        _ => {
                                            match serde_yaml::from_str::<PromptFileWrapper>(
                                                &content,
                                            ) {
                                                Ok(w) => w.into_entries(&path),
                                                Err(e) => {
                                                    tracing::warn!(
                                                        "Failed to parse {}: {}",
                                                        path.display(),
                                                        e
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                    };

                                    let mut reg = registry.write().unwrap();
                                    for entry in entries {
                                        tracing::info!(key = %entry.key, "Hot-reloading prompt");
                                        reg.register(entry);
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to read {}: {}", path.display(), e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Watch error: {}", e);
                }
            }
        }

        Ok(())
    }
}

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct PromptFileWrapper {
    prompts: std::collections::HashMap<String, PromptFileEntry>,
}

#[derive(Debug, Deserialize)]
struct PromptFileEntry {
    content: String,
    #[serde(default = "default_version")]
    version: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_active")]
    active: bool,
    #[serde(default)]
    ab_group: Option<String>,
}

fn default_version() -> String {
    "1.0.0".into()
}

fn default_active() -> bool {
    true
}

impl PromptFileWrapper {
    fn into_entries(self, path: &std::path::Path) -> Vec<PromptEntry> {
        let source = PromptSource::FileSystem {
            path: path.display().to_string(),
        };
        self.prompts
            .into_iter()
            .map(|(key, entry)| PromptEntry {
                key,
                content: entry.content,
                version: entry.version,
                description: entry.description,
                tags: entry.tags,
                source: source.clone(),
                active: entry.active,
                ab_group: entry.ab_group,
            })
            .collect()
    }
}
