//! 工作区文件面：树、读、写。
//!
//! 这是 WebUI 里那个「文件」面板的后端。它读写的是**每个用户自己的一个目录**，
//! 不是进程的当前目录，也不是整个文件系统：容器里能读到的东西（配置、密钥挂载、
//! `/proc`）和用户在编辑器里该看到的东西，是两套完全不同的东西。
//!
//! 根目录取 `COGNEVA_WORKSPACE_ROOT`；没设时落在应用数据卷上
//! （`$COGNEVA_DATA_DIR/workspace/users`，默认 `/var/lib/cogneva-data/...`）——
//! 工作区必须跨 Pod 重启存活，而容器根文件系统是一次性的。每个用户在下面有一个
//! 以自己 UUID 命名的子目录，第一次打开时创建。按用户分而不是共用一个根：共用根时，
//! A 在树里能看见 B 的草稿、能覆盖 B 的文件，而这不会有任何报错。
//!
//! 路径判据是这条路面上唯一的攻击面，所以它只认一种形状：**根之下的相对路径**。
//! 绝对路径、`..`、以及任何能把人带出根的符号链接都拒绝。符号链接的检查不能只看
//! 字面量——`root/link -> /etc` 字面上完全规矩——所以落到盘上的每一步都做一次真实
//! 路径前缀比对，写新文件时比对它的父目录。

use axum::{
    extract::{Extension, Query},
    Json,
};
use cog_core::Claims;
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

use crate::error::ApiError;

/// 工作区根的父目录；用户子目录建在它下面。
pub const WORKSPACE_ROOT_ENV: &str = "COGNEVA_WORKSPACE_ROOT";

/// 应用数据目录（与身份文件、数仓同一个卷）。
const DATA_DIR_ENV: &str = "COGNEVA_DATA_DIR";
const DEFAULT_DATA_DIR: &str = "/var/lib/cogneva-data";
/// 单个文件的上限。编辑器不是上传通道：超过这个量的东西该走附件那条路。
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
/// 一次列目录最多回多少条，防止一个百万文件的目录把响应撑爆。
const MAX_TREE_ENTRIES: usize = 2000;
/// 判断二进制时看的前缀长度，也是这类文件永不返回正文的理由。
const BINARY_PROBE_BYTES: usize = 8192;

#[derive(Debug, Deserialize)]
pub struct TreeQuery {
    #[serde(default)]
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct FileQuery {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct WriteBody {
    pub path: String,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct TreeEntry {
    /// 相对用户根的路径，前端拿它当 key，也拿它当下一次请求的参数。
    pub path: String,
    pub name: String,
    pub dir: bool,
    pub size: u64,
    pub modified_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct TreeResponse {
    pub path: String,
    pub entries: Vec<TreeEntry>,
    /// 目录里的条数超过了上限，这里只列了前一部分。
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct FileResponse {
    pub path: String,
    pub size: u64,
    /// 二进制文件不回正文——把它当文本塞进编辑器会得到一屏替换字符。
    pub binary: bool,
    /// 超过上限时只回前一段，`truncated` 说明后面还有。
    pub truncated: bool,
    pub text: Option<String>,
    pub modified_ms: Option<u64>,
}

/// 用户根目录：`<COGNEVA_WORKSPACE_ROOT>/<user uuid>`。
fn user_root(state_root: &Path, claims: &Claims) -> Result<PathBuf, ApiError> {
    let id = claims
        .user_id()
        .map_err(|e| ApiError::forbidden(format!("token does not name a user: {e}")))?;
    Ok(state_root.join(id.to_string()))
}

/// 进程内配置的工作区根。
fn configured_root() -> PathBuf {
    if let Some(explicit) = non_empty_env(WORKSPACE_ROOT_ENV) {
        return PathBuf::from(explicit);
    }
    let data = non_empty_env(DATA_DIR_ENV).unwrap_or_else(|| DEFAULT_DATA_DIR.to_string());
    PathBuf::from(data).join("workspace").join("users")
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
}

/// 把客户端给的相对路径落到根下面。
///
/// 拒绝的东西一律回 403 而不是 404：调用方要能分清「这里没有这个文件」和
/// 「这个路径本来就不许出现」，否则换一个路径重试就成了唯一线索。
fn resolve_under(root: &Path, rel: &str) -> Result<PathBuf, ApiError> {
    let rel = rel.trim();
    if rel.is_empty() {
        return Ok(root.to_path_buf());
    }
    for component in Path::new(rel).components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {
                return Err(ApiError::forbidden(
                    "path must not contain '.' segments; send a normalised relative path",
                ))
            }
            _ => {
                return Err(ApiError::forbidden(
                    "path must be relative to the workspace root and must not contain '..'",
                ))
            }
        }
    }

    let joined = root.join(rel);
    let root_real = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    // 存在的路径：解析完符号链接之后仍要在根下面。
    // 不存在的路径（新建文件）：检查它最近的已存在祖先，因为符号链接可以出现在
    // 中间任何一层，只比字面量会把 `root/link -> /etc` 放过去。
    let mut probe = joined.as_path();
    loop {
        match probe.canonicalize() {
            Ok(real) => {
                if !real.starts_with(&root_real) {
                    return Err(ApiError::forbidden(
                        "path resolves outside the workspace root",
                    ));
                }
                return Ok(joined);
            }
            Err(_) => match probe.parent() {
                Some(parent) if parent != probe => probe = parent,
                _ => return Ok(joined),
            },
        }
    }
}

fn modified_ms(meta: &std::fs::Metadata) -> Option<u64> {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
}

fn to_response_path(rel: &str) -> String {
    rel.replace('\\', "/")
}

/// 列一层目录。不做递归：整棵树一次拉完在真实工作区上就是几万条，前端也画不动。
async fn read_tree(dir: &Path, rel: &str) -> Result<TreeResponse, ApiError> {
    let mut reader = tokio::fs::read_dir(dir).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ApiError::not_found(format!("no such directory: {rel}"))
        } else {
            ApiError::bad_request(format!("cannot read directory {rel}: {e}"))
        }
    })?;

    let mut entries = Vec::new();
    let mut truncated = false;
    while let Some(entry) = reader
        .next_entry()
        .await
        .map_err(|e| ApiError::internal(format!("cannot read directory {rel}: {e}")))?
    {
        if entries.len() >= MAX_TREE_ENTRIES {
            truncated = true;
            break;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let meta = match entry.metadata().await {
            Ok(m) => m,
            // 目录在这一刻被删掉了：跳过，不因为它让整棵树失败。
            Err(_) => continue,
        };
        let child_rel = if rel.is_empty() {
            name.clone()
        } else {
            format!("{rel}/{name}")
        };
        entries.push(TreeEntry {
            path: to_response_path(&child_rel),
            name,
            dir: meta.is_dir(),
            size: meta.len(),
            modified_ms: modified_ms(&meta),
        });
    }

    // 目录在前、同类按名字排：文件树里混着排序没人能扫。
    entries.sort_by(|a, b| b.dir.cmp(&a.dir).then_with(|| a.name.cmp(&b.name)));

    Ok(TreeResponse {
        path: to_response_path(rel),
        entries,
        truncated,
    })
}

/// 读一个文件。二进制与超限都不报错，而是把「为什么没有正文」写在响应里。
async fn read_file(path: &Path, rel: &str) -> Result<FileResponse, ApiError> {
    let meta = tokio::fs::metadata(path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ApiError::not_found(format!("no such file: {rel}"))
        } else {
            ApiError::bad_request(format!("cannot stat {rel}: {e}"))
        }
    })?;
    if meta.is_dir() {
        return Err(ApiError::bad_request(format!(
            "{rel} is a directory; list it with the tree endpoint"
        )));
    }

    let size = meta.len();
    let probe_len = size.min(BINARY_PROBE_BYTES as u64) as usize;
    let mut probe = vec![0u8; probe_len];
    {
        use tokio::io::AsyncReadExt;
        let mut handle = tokio::fs::File::open(path)
            .await
            .map_err(|e| ApiError::internal(format!("cannot open {rel}: {e}")))?;
        handle
            .read_exact(&mut probe)
            .await
            .map_err(|e| ApiError::internal(format!("cannot read {rel}: {e}")))?;
    }
    if probe.contains(&0) {
        return Ok(FileResponse {
            path: to_response_path(rel),
            size,
            binary: true,
            truncated: false,
            text: None,
            modified_ms: modified_ms(&meta),
        });
    }

    let read_len = size.min(MAX_FILE_BYTES) as usize;
    let mut bytes = vec![0u8; read_len];
    {
        use tokio::io::AsyncReadExt;
        let mut handle = tokio::fs::File::open(path)
            .await
            .map_err(|e| ApiError::internal(format!("cannot open {rel}: {e}")))?;
        handle
            .read_exact(&mut bytes)
            .await
            .map_err(|e| ApiError::internal(format!("cannot read {rel}: {e}")))?;
    }
    // 截断可能切在多字节字符中间：用 from_utf8 的容错解码，不要拒绝整页。
    let text = String::from_utf8_lossy(&bytes).to_string();

    Ok(FileResponse {
        path: to_response_path(rel),
        size,
        binary: false,
        truncated: size > MAX_FILE_BYTES,
        text: Some(text),
        modified_ms: modified_ms(&meta),
    })
}

/// 原子写入：先写同目录临时文件再 rename。
///
/// 直接截断重写的话，写一半失败（磁盘满、进程被杀）会留下一个前半段是新内容、
/// 后半段是旧内容的文件，而这种文件看起来完全正常。
async fn write_file(path: &Path, text: &str) -> Result<u64, ApiError> {
    let bytes = text.as_bytes();
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(ApiError::bad_request(format!(
            "file is {} bytes; the editor writes at most {} bytes",
            bytes.len(),
            MAX_FILE_BYTES
        )));
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| ApiError::bad_request(format!("cannot create parent directory: {e}")))?;
    }

    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| ApiError::bad_request("path must end in a file name"))?;
    let tmp = path.with_file_name(format!(".{name}.tmp-{}", std::process::id()));
    tokio::fs::write(&tmp, bytes)
        .await
        .map_err(|e| ApiError::internal(format!("cannot write: {e}")))?;
    match tokio::fs::rename(&tmp, path).await {
        Ok(()) => Ok(bytes.len() as u64),
        Err(e) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            Err(ApiError::internal(format!("cannot replace file: {e}")))
        }
    }
}

/// 按需建立用户根：第一次打开文件面板的人不该看到一个「目录不存在」的错误。
async fn ensure_user_root(claims: &Claims) -> Result<PathBuf, ApiError> {
    let root = configured_root();
    let user = user_root(&root, claims)?;
    tokio::fs::create_dir_all(&user)
        .await
        .map_err(|e| ApiError::internal(format!("cannot create workspace: {e}")))?;
    Ok(user)
}

pub async fn tree_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<TreeResponse>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let dir = resolve_under(&root, &query.path)?;
    Ok(Json(read_tree(&dir, &query.path).await?))
}

pub async fn file_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<FileQuery>,
) -> Result<Json<FileResponse>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let path = resolve_under(&root, &query.path)?;
    Ok(Json(read_file(&path, &query.path).await?))
}

pub async fn write_handler(
    Extension(claims): Extension<Claims>,
    Json(body): Json<WriteBody>,
) -> Result<Json<FileResponse>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let path = resolve_under(&root, &body.path)?;
    // 写之前先把「写的是谁、写到哪」记下来：文件面出问题要能回答这个问题，
    // 而响应体不会被任何人留档。
    tracing::info!(
        user = %claims.sub,
        path = %body.path,
        bytes = body.text.len(),
        "workspace file written"
    );
    write_file(&path, &body.text).await?;
    Ok(Json(read_file(&path, &body.path).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("temp dir")
    }

    #[test]
    fn absolute_paths_and_parent_traversal_are_refused() {
        let dir = tempdir();
        let root = dir.path();
        for bad in ["/etc/passwd", "../escape", "a/../../b", "..", "./x"] {
            let err = resolve_under(root, bad).expect_err(bad);
            assert_eq!(err.error, "Forbidden", "{bad} 必须被拒绝");
        }
    }

    #[test]
    fn ordinary_relative_paths_stay_under_the_root() {
        let dir = tempdir();
        let root = dir.path();
        std::fs::create_dir_all(root.join("src")).unwrap();
        let resolved = resolve_under(root, "src/main.rs").unwrap();
        assert_eq!(resolved, root.join("src/main.rs"));
        // 空路径就是根本身。
        assert_eq!(resolve_under(root, "").unwrap(), root);
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_pointing_outside_the_root_is_refused() {
        let dir = tempdir();
        let root = dir.path().join("root");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret.txt"), "not yours").unwrap();

        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();

        let err = resolve_under(&root, "link/secret.txt").expect_err("符号链接必须被拦在根里");
        assert_eq!(err.error, "Forbidden");
    }

    #[tokio::test]
    async fn write_then_read_round_trips_and_reports_size() {
        let dir = tempdir();
        let root = dir.path();
        let path = resolve_under(root, "notes/todo.md").unwrap();

        let written = write_file(&path, "第一行\n第二行\n").await.unwrap();
        assert_eq!(written, "第一行\n第二行\n".len() as u64);

        let read = read_file(&path, "notes/todo.md").await.unwrap();
        assert!(!read.binary);
        assert!(!read.truncated);
        assert_eq!(read.text.as_deref(), Some("第一行\n第二行\n"));
        assert_eq!(read.path, "notes/todo.md");
    }

    #[tokio::test]
    async fn binary_files_come_back_without_text() {
        let dir = tempdir();
        let root = dir.path();
        let path = root.join("blob.bin");
        tokio::fs::write(&path, [0u8, 1, 2, 3, 0u8]).await.unwrap();

        let read = read_file(&path, "blob.bin").await.unwrap();
        assert!(read.binary, "含 NUL 的文件必须判成二进制");
        assert!(read.text.is_none(), "二进制文件不返回正文");
    }

    #[tokio::test]
    async fn writes_above_the_cap_are_refused_before_touching_the_disk() {
        let dir = tempdir();
        let root = dir.path();
        let path = root.join("big.txt");
        let huge = "x".repeat(MAX_FILE_BYTES as usize + 1);

        let err = write_file(&path, &huge)
            .await
            .expect_err("超过上限必须拒绝");
        assert_eq!(err.error, "BadRequest");
        assert!(!path.exists(), "被拒绝的写入不该在盘上留下文件");
    }

    #[tokio::test]
    async fn a_directory_read_through_the_file_endpoint_says_so() {
        let dir = tempdir();
        let root = dir.path();
        tokio::fs::create_dir_all(root.join("src")).await.unwrap();

        let err = read_file(&root.join("src"), "src")
            .await
            .expect_err("目录要单独报");
        assert_eq!(err.error, "BadRequest");
        assert!(err.message.contains("directory"));
    }
}
