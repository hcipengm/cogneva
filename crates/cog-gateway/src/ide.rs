//! 工作区文件面：树、读、写。
//!
//! 这是 WebUI 里那个「文件」面板的后端。它读写的是**每个用户自己的一个目录**，
//! 不是进程的当前目录，也不是整个文件系统：容器里能读到的东西（配置、密钥挂载、
//! `/proc`）和用户在编辑器里该看到的东西，是两套完全不同的东西。
//!
//! 根目录取 `COGNEVA_WORKSPACE_ROOT`；没设时落在应用数据卷上
//! （`$COGNEVA_DATA_DIR/workspace`，默认 `/var/lib/cogneva-data/workspace`）——
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
/// 上传 / 下载的上限。比编辑器那条宽——整份构建产物、一张截图也该能搬进搬出，
/// 但必须落在路由层请求体的封顶之下并留出余量：超过那条线的大请求在到达这里
/// 之前就被拒，本层再给一个更大的数只会让两边说法不一致。
const TRANSFER_MAX_BYTES: usize = 8 * 1024 * 1024;
/// 一次搜索最多走多少个文件、读多少字节：搜索是同步回答的请求，没有一个
/// 上界时一个装满构建产物的根目录会让这一次请求把连接占满。
const SEARCH_MAX_FILES: usize = 5_000;
const SEARCH_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// 一次搜索默认回多少条匹配，以及调用方能要到多少。
const SEARCH_DEFAULT_LIMIT: usize = 100;
const SEARCH_MAX_LIMIT: usize = 500;
/// 每个用户工作区的容量上限（字节）。取一个部署能改的默认值：编辑器与上传是同一块
/// 持久卷，没有上限时一个用户把卷写满，代价由同一卷上的其他状态一起付。
const WORKSPACE_MAX_BYTES_ENV: &str = "COGNEVA_WORKSPACE_MAX_BYTES";
const DEFAULT_WORKSPACE_MAX_BYTES: u64 = 1024 * 1024 * 1024;

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

#[derive(Debug, Deserialize)]
pub struct MkdirBody {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct RenameBody {
    #[serde(rename = "from")]
    pub from: String,
    #[serde(rename = "to")]
    pub to: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    /// 要找的文字。按字面量、不区分 ASCII 大小写，不当正则：搜索框里打一个
    /// `(` 不该回 400，也不该把一次误输入变成一次昂贵的正则执行。
    pub q: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct SearchMatch {
    pub path: String,
    /// 从 1 开始的行号与列号（列按字符算，不按字节：中文一列一个字）。
    pub line: usize,
    pub column: usize,
    /// 那一行的原文，未截断——读者要能看见命中的上下文。
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub matches: Vec<SearchMatch>,
    /// 走过了多少文件 / 字节：读者据此知道「没找到」是真的没有，还是没走完。
    pub scanned_files: usize,
    pub scanned_bytes: u64,
    /// 条数或扫描面到了上限，结果被截断。
    pub truncated: bool,
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
    // 只拼一级：每个用户的 UUID 目录直接落在数据根的 workspace 下，再多一层
    // 就多一个要在数据根台账里登记的子目录，而它并不承载独立语义。
    PathBuf::from(data).join("workspace")
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

/// 一次工作区的用量读数。
#[derive(Debug, Serialize)]
pub struct UsageResponse {
    pub files: u64,
    /// 按文件大小相加：这是「这块盘被我占了多少」，不是「块占了多少」。
    pub bytes: u64,
    /// 上限。`0` 表示部署把上限关掉了（不限），不是「一点也不能写」。
    pub limit_bytes: u64,
}

fn workspace_limit_bytes() -> u64 {
    match non_empty_env(WORKSPACE_MAX_BYTES_ENV).and_then(|raw| raw.parse::<u64>().ok()) {
        Some(0) => 0,
        Some(limit) => limit,
        None => DEFAULT_WORKSPACE_MAX_BYTES,
    }
}

/// 走一遍用户根，累加文件数与字节。不跟符号链接：链接可以指向根外，把它算进
/// 用量等于让用户用一条链接占别人的空间。
async fn measure_usage(root: &Path) -> Result<(u64, u64), ApiError> {
    let mut files = 0u64;
    let mut bytes = 0u64;
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut reader = match tokio::fs::read_dir(&dir).await {
            Ok(reader) => reader,
            Err(_) => continue,
        };
        while let Some(entry) = reader
            .next_entry()
            .await
            .map_err(|e| ApiError::internal(format!("cannot measure the workspace: {e}")))?
        {
            let file_type = match entry.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                if let Ok(meta) = entry.metadata().await {
                    files += 1;
                    bytes += meta.len();
                }
            }
        }
    }
    Ok((files, bytes))
}

/// 写之前先问「写完还装得下吗」。
///
/// `replacing` 是这次写入会覆盖掉的那份字节数（新建文件传 0）：用它把「改一个
/// 已经存在的文件」与「再塞一份新的」分开，否则每次保存都要凭空多算一遍自己。
async fn check_capacity(
    root: &Path,
    limit: u64,
    adding: u64,
    replacing: u64,
) -> Result<(), ApiError> {
    if limit == 0 {
        return Ok(());
    }
    let (_, used) = measure_usage(root).await?;
    let after = used.saturating_sub(replacing).saturating_add(adding);
    if after > limit {
        return Err(ApiError::bad_request(format!(
            "工作区放不下：这次写入后是 {after} 字节，上限 {limit} 字节。先删一些文件，\
             或让部署调高 {WORKSPACE_MAX_BYTES_ENV}"
        )));
    }
    Ok(())
}

/// 建目录（含父目录）。返回这一条在树里的形状。
async fn make_dir(root: &Path, rel: &str) -> Result<TreeEntry, ApiError> {
    let path = resolve_under(root, rel)?;
    tokio::fs::create_dir_all(&path)
        .await
        .map_err(|e| ApiError::bad_request(format!("cannot create {rel}: {e}")))?;
    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|e| ApiError::internal(format!("cannot stat {rel}: {e}")))?;
    Ok(TreeEntry {
        path: to_response_path(rel.trim()),
        name: path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string(),
        dir: true,
        size: meta.len(),
        modified_ms: modified_ms(&meta),
    })
}

/// 改名 / 移动。两端都在同一个用户根里，且目标已存在时拒绝——覆盖式改名在
/// 文件管理器里是「手滑一次就没了一个文件」，而这里没有撤销。
async fn move_entry(root: &Path, from_rel: &str, to_rel: &str) -> Result<(), ApiError> {
    let from = resolve_under(root, from_rel)?;
    let to = resolve_under(root, to_rel)?;
    if tokio::fs::symlink_metadata(&from).await.is_err() {
        return Err(ApiError::not_found(format!("no such file: {from_rel}")));
    }
    if tokio::fs::symlink_metadata(&to).await.is_ok() {
        return Err(ApiError::bad_request(format!(
            "{to_rel} already exists; refusing to overwrite"
        )));
    }
    if let Some(parent) = to.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| ApiError::bad_request(format!("cannot create parent directory: {e}")))?;
    }
    tokio::fs::rename(&from, &to)
        .await
        .map_err(|e| ApiError::bad_request(format!("cannot rename: {e}")))
}

/// 删除文件或空目录。非空目录要先把内容删掉：这条规矩让「删一个目录」不会
/// 连带删掉读者没看见的东西。
async fn remove_entry(root: &Path, rel: &str) -> Result<(), ApiError> {
    let path = resolve_under(root, rel)?;
    let meta = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|_| ApiError::not_found(format!("no such file: {rel}")))?;
    if meta.is_dir() {
        let mut reader = tokio::fs::read_dir(&path)
            .await
            .map_err(|e| ApiError::internal(format!("cannot read directory: {e}")))?;
        if reader
            .next_entry()
            .await
            .map_err(|e| ApiError::internal(format!("cannot read directory: {e}")))?
            .is_some()
        {
            return Err(ApiError::bad_request(format!(
                "{rel} is not empty; delete its contents first"
            )));
        }
        tokio::fs::remove_dir(&path)
            .await
            .map_err(|e| ApiError::internal(format!("cannot remove directory: {e}")))
    } else {
        tokio::fs::remove_file(&path)
            .await
            .map_err(|e| ApiError::internal(format!("cannot remove file: {e}")))
    }
}

/// 原样读取字节。编辑器那条路会把文本转成 UTF-8 并截断，二进制文件更是没有
/// 正文；搬文件要的是字节本身。
async fn read_raw(root: &Path, rel: &str) -> Result<(PathBuf, Vec<u8>), ApiError> {
    let path = resolve_under(root, rel)?;
    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|_| ApiError::not_found(format!("no such file: {rel}")))?;
    if meta.is_dir() {
        return Err(ApiError::bad_request(format!(
            "{rel} is a directory; listing it is the tree endpoint"
        )));
    }
    if meta.len() > TRANSFER_MAX_BYTES as u64 {
        return Err(ApiError::bad_request(format!(
            "{rel} is {} bytes; transfers stop at {TRANSFER_MAX_BYTES}",
            meta.len()
        )));
    }
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| ApiError::internal(format!("cannot read {rel}: {e}")))?;
    Ok((path, bytes))
}

/// 原样写入字节（上传）。不截断、不做编码转换：上传的东西就是盘上该有的东西。
async fn write_bytes(root: &Path, rel: &str, bytes: &[u8]) -> Result<(), ApiError> {
    if bytes.len() > TRANSFER_MAX_BYTES {
        return Err(ApiError::bad_request(format!(
            "upload is {} bytes; the limit is {TRANSFER_MAX_BYTES}",
            bytes.len()
        )));
    }
    let path = resolve_under(root, rel)?;
    if tokio::fs::metadata(&path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(format!("{rel} is a directory")));
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| ApiError::bad_request(format!("cannot create parent directory: {e}")))?;
    }
    tokio::fs::write(&path, bytes)
        .await
        .map_err(|e| ApiError::internal(format!("cannot write {rel}: {e}")))
}

/// 在工作区里按字面量找，一行一条匹配。
///
/// 三条判据写在这里而不是交给调用方：
/// - 只走自己的根。目录项是符号链接就跳过——链接可以指向根外，顺着它走等于让
///   被搜索的内容决定搜索能看见什么。
/// - 二进制与超大文件不读。命中一行替换字符组成的「匹配」比没有匹配更糟。
/// - 扫描面有上界，并且把「被截断」写进回答：读者要能分出「确实没有」和
///   「没走到那儿」。
async fn search_workspace(
    root: &Path,
    q: &str,
    limit: Option<usize>,
) -> Result<SearchResponse, ApiError> {
    let needle = q.trim();
    if needle.is_empty() {
        return Err(ApiError::bad_request(
            "search needs a non-empty q; an empty query matches every line",
        ));
    }
    let limit = limit
        .unwrap_or(SEARCH_DEFAULT_LIMIT)
        .clamp(1, SEARCH_MAX_LIMIT);
    // `to_ascii_lowercase` 逐字节保长，所以找到的字节下标就是原文的下标；
    // 非 ASCII 的字节原样保留，中文按字面量比较。
    let needle_lower = needle.to_ascii_lowercase();

    let mut response = SearchResponse {
        query: needle.to_string(),
        matches: Vec::new(),
        scanned_files: 0,
        scanned_bytes: 0,
        truncated: false,
    };
    let mut stack: Vec<(PathBuf, String)> = vec![(root.to_path_buf(), String::new())];

    while let Some((dir, rel)) = stack.pop() {
        let mut reader = match tokio::fs::read_dir(&dir).await {
            Ok(reader) => reader,
            // 目录在这一刻被删掉、或读不动：跳过它，不因为它让整次搜索失败。
            Err(_) => continue,
        };
        while let Some(entry) = reader
            .next_entry()
            .await
            .map_err(|e| ApiError::internal(format!("cannot walk the workspace: {e}")))?
        {
            if response.scanned_files >= SEARCH_MAX_FILES
                || response.scanned_bytes >= SEARCH_MAX_BYTES
            {
                response.truncated = true;
                return Ok(response);
            }
            // `file_type` 不跟随链接；`metadata` 会跟随，两者别混用。
            let file_type = match entry.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if file_type.is_symlink() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let child_rel = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            if file_type.is_dir() {
                stack.push((entry.path(), child_rel));
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let path = entry.path();
            let meta = match entry.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.len() > MAX_FILE_BYTES {
                continue;
            }
            let bytes = match tokio::fs::read(&path).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            response.scanned_files += 1;
            response.scanned_bytes += bytes.len() as u64;
            let probe_len = bytes.len().min(BINARY_PROBE_BYTES);
            if bytes[..probe_len].contains(&0) {
                continue;
            }
            let text = String::from_utf8_lossy(&bytes);
            for (index, line) in text.lines().enumerate() {
                let Some(byte_pos) = line.to_ascii_lowercase().find(&needle_lower) else {
                    continue;
                };
                response.matches.push(SearchMatch {
                    path: to_response_path(&child_rel),
                    line: index + 1,
                    column: line[..byte_pos].chars().count() + 1,
                    text: line.to_string(),
                });
                if response.matches.len() >= limit {
                    response.truncated = true;
                    return Ok(response);
                }
            }
        }
    }
    Ok(response)
}

pub async fn search_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let response = search_workspace(&root, &query.q, query.limit).await?;
    tracing::info!(
        user = %claims.sub,
        query = %response.query,
        matches = response.matches.len(),
        scanned_files = response.scanned_files,
        truncated = response.truncated,
        "workspace searched"
    );
    Ok(Json(response))
}

pub async fn mkdir_handler(
    Extension(claims): Extension<Claims>,
    Json(body): Json<MkdirBody>,
) -> Result<Json<TreeEntry>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let entry = make_dir(&root, &body.path).await?;
    tracing::info!(user = %claims.sub, path = %body.path, "workspace directory created");
    Ok(Json(entry))
}

pub async fn rename_handler(
    Extension(claims): Extension<Claims>,
    Json(body): Json<RenameBody>,
) -> Result<Json<FileResponse>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    move_entry(&root, &body.from, &body.to).await?;
    tracing::info!(user = %claims.sub, from = %body.from, to = %body.to, "workspace entry renamed");
    read_file(&resolve_under(&root, &body.to)?, &body.to)
        .await
        .map(Json)
}

pub async fn delete_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<FileQuery>,
) -> Result<axum::http::StatusCode, ApiError> {
    let root = ensure_user_root(&claims).await?;
    remove_entry(&root, &query.path).await?;
    tracing::info!(user = %claims.sub, path = %query.path, "workspace entry deleted");
    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn raw_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<FileQuery>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;

    let root = ensure_user_root(&claims).await?;
    let (path, bytes) = read_raw(&root, &query.path).await?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .replace('"', "");
    Ok((
        [
            ("content-type", "application/octet-stream".to_string()),
            (
                "content-disposition",
                format!("attachment; filename=\"{name}\""),
            ),
        ],
        bytes,
    )
        .into_response())
}

/// 上传：请求体就是文件内容，路径由查询参数给出。
///
/// 不用 multipart：这一条只搬一个文件，多出来的边界解析与表单字段没有一处会
/// 用到，而它正是这类接口出过问题的地方。
pub async fn upload_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<FileQuery>,
    body: axum::body::Bytes,
) -> Result<Json<FileResponse>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let target = resolve_under(&root, &query.path)?;
    let replacing = tokio::fs::metadata(&target)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    check_capacity(&root, workspace_limit_bytes(), body.len() as u64, replacing).await?;
    write_bytes(&root, &query.path, &body).await?;
    tracing::info!(user = %claims.sub, path = %query.path, bytes = body.len(), "workspace file uploaded");
    let path = resolve_under(&root, &query.path)?;
    match read_file(&path, &query.path).await {
        Ok(file) => Ok(Json(file)),
        // 二进制附件读不出正文，但上传确实成功了：回一条没有正文的记录，
        // 而不是把一次成功的写入报成失败。
        Err(_) => Ok(Json(FileResponse {
            path: to_response_path(query.path.trim()),
            size: body.len() as u64,
            binary: true,
            truncated: false,
            text: None,
            modified_ms: None,
        })),
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

pub async fn usage_handler(
    Extension(claims): Extension<Claims>,
) -> Result<Json<UsageResponse>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let (files, bytes) = measure_usage(&root).await?;
    Ok(Json(UsageResponse {
        files,
        bytes,
        limit_bytes: workspace_limit_bytes(),
    }))
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
    let replacing = tokio::fs::metadata(&path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    check_capacity(
        &root,
        workspace_limit_bytes(),
        body.text.len() as u64,
        replacing,
    )
    .await?;
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
    async fn directories_are_created_including_missing_parents() {
        let dir = tempdir();
        let root = dir.path();

        let entry = make_dir(root, "notes/2026").await.unwrap();
        assert!(entry.dir);
        assert_eq!(entry.path, "notes/2026");
        assert!(root.join("notes/2026").is_dir());
    }

    #[tokio::test]
    async fn rename_moves_within_the_root_and_refuses_to_overwrite() {
        let dir = tempdir();
        let root = dir.path();
        std::fs::write(root.join("a.txt"), "one").unwrap();
        std::fs::write(root.join("b.txt"), "two").unwrap();

        move_entry(root, "a.txt", "c.txt").await.unwrap();
        assert!(root.join("c.txt").exists());
        assert!(!root.join("a.txt").exists());

        let err = move_entry(root, "c.txt", "b.txt")
            .await
            .expect_err("目标已存在时必须拒绝");
        assert_eq!(err.error, "BadRequest");
        assert_eq!(std::fs::read_to_string(root.join("b.txt")).unwrap(), "two");
    }

    #[tokio::test]
    async fn rename_cannot_reach_outside_the_root() {
        let dir = tempdir();
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.txt"), "x").unwrap();

        let err = move_entry(&root, "a.txt", "../escaped.txt")
            .await
            .expect_err("出根的改名必须拒绝");
        assert_eq!(err.error, "Forbidden");
        assert!(root.join("a.txt").exists(), "被拒绝的改名不能动原文件");
    }

    #[tokio::test]
    async fn a_non_empty_directory_is_not_deleted() {
        let dir = tempdir();
        let root = dir.path();
        std::fs::create_dir_all(root.join("keep")).unwrap();
        std::fs::write(root.join("keep/inside.txt"), "x").unwrap();

        let err = remove_entry(root, "keep")
            .await
            .expect_err("非空目录必须拒绝");
        assert_eq!(err.error, "BadRequest");
        assert!(root.join("keep/inside.txt").exists());

        remove_entry(root, "keep/inside.txt").await.unwrap();
        remove_entry(root, "keep").await.unwrap();
        assert!(!root.join("keep").exists());
    }

    #[tokio::test]
    async fn raw_transfers_keep_bytes_exactly() {
        let dir = tempdir();
        let root = dir.path();
        let payload = [0u8, 159, 146, 150, 255];

        write_bytes(root, "blob.bin", &payload).await.unwrap();
        let (path, bytes) = read_raw(root, "blob.bin").await.unwrap();
        assert_eq!(path, root.join("blob.bin"));
        assert_eq!(bytes, payload, "原样搬运不能经过任何编码转换");
    }

    #[tokio::test]
    async fn uploads_above_the_transfer_cap_are_refused() {
        let dir = tempdir();
        let root = dir.path();
        let big = vec![b'x'; TRANSFER_MAX_BYTES + 1];

        let err = write_bytes(root, "big.bin", &big)
            .await
            .expect_err("超过传输上限必须拒绝");
        assert_eq!(err.error, "BadRequest");
        assert!(!root.join("big.bin").exists());
    }

    #[tokio::test]
    async fn search_reports_path_line_and_column_for_each_hit() {
        let dir = tempdir();
        let root = dir.path();
        tokio::fs::create_dir_all(root.join("notes")).await.unwrap();
        tokio::fs::write(root.join("notes/todo.md"), "第一行\nTODO: 写搜索\n")
            .await
            .unwrap();
        tokio::fs::write(root.join("main.rs"), "fn main() {}\n// TODO: 还债\n")
            .await
            .unwrap();

        let found = search_workspace(root, "todo", None).await.unwrap();

        assert_eq!(found.matches.len(), 2, "{found:?}");
        let md = found
            .matches
            .iter()
            .find(|m| m.path == "notes/todo.md")
            .expect("中文同一行里的 TODO 也要命中");
        assert_eq!(md.line, 2);
        assert_eq!(md.column, 1);
        let rs = found
            .matches
            .iter()
            .find(|m| m.path == "main.rs")
            .expect("注释里的 TODO 也要命中");
        assert_eq!(rs.line, 2);
        // 列按字符算：「// TODO」里的 T 是这一行的第 4 个字符。
        assert_eq!(rs.column, 4);
        assert_eq!(found.scanned_files, 2);
        assert!(!found.truncated);
    }

    #[tokio::test]
    async fn search_does_not_read_binary_files() {
        let dir = tempdir();
        let root = dir.path();
        // 前面是文本、中间一个 NUL：按文本读回来就是一屏替换字符。
        let mut body = b"needle in the head\n".to_vec();
        body.push(0);
        body.extend_from_slice(b"needle after the nul\n");
        tokio::fs::write(root.join("blob.bin"), &body)
            .await
            .unwrap();
        tokio::fs::write(root.join("plain.txt"), "needle here\n")
            .await
            .unwrap();

        let found = search_workspace(root, "needle", None).await.unwrap();

        assert_eq!(found.matches.len(), 1, "{found:?}");
        assert_eq!(found.matches[0].path, "plain.txt");
        assert_eq!(found.scanned_files, 2, "两个文件都走到了，只是二进制不读");
    }

    #[tokio::test]
    async fn search_stops_at_the_limit_and_says_it_was_truncated() {
        let dir = tempdir();
        let root = dir.path();
        let body: String = (0..20).map(|i| format!("needle {i}\n")).collect();
        tokio::fs::write(root.join("many.txt"), body).await.unwrap();

        let found = search_workspace(root, "needle", Some(5)).await.unwrap();

        assert_eq!(found.matches.len(), 5);
        assert!(
            found.truncated,
            "到了条数上限就必须说，否则「只有 5 处」是假读数"
        );
    }

    #[tokio::test]
    async fn search_does_not_follow_a_symlink_out_of_the_root() {
        let dir = tempdir();
        let root = dir.path().join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let outside = dir.path().join("outside");
        tokio::fs::create_dir_all(&outside).await.unwrap();
        tokio::fs::write(outside.join("secret.txt"), "needle outside\n")
            .await
            .unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();

        let found = search_workspace(&root, "needle", None).await.unwrap();

        assert!(
            found.matches.is_empty(),
            "根外的内容不能因为一条链接被搜出来：{found:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_query_is_refused_rather_than_matching_every_line() {
        let dir = tempdir();
        let err = search_workspace(dir.path(), "   ", None)
            .await
            .expect_err("空查询必须拒绝");
        assert_eq!(err.error, "BadRequest");
    }

    #[tokio::test]
    async fn usage_counts_files_and_bytes_but_not_through_a_symlink() {
        let dir = tempdir();
        let root = dir.path().join("root");
        tokio::fs::create_dir_all(root.join("sub")).await.unwrap();
        tokio::fs::write(root.join("sub/a.txt"), "12345")
            .await
            .unwrap();
        tokio::fs::write(root.join("b.txt"), "67").await.unwrap();
        let outside = dir.path().join("outside");
        tokio::fs::create_dir_all(&outside).await.unwrap();
        tokio::fs::write(outside.join("big.bin"), vec![0u8; 4096])
            .await
            .unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();

        let (files, bytes) = measure_usage(&root).await.unwrap();

        assert_eq!(files, 2, "链接后面的文件不算我的用量");
        assert_eq!(bytes, 7);
    }

    #[tokio::test]
    async fn a_write_that_would_pass_the_cap_never_reaches_the_disk() {
        let dir = tempdir();
        let root = dir.path();
        tokio::fs::write(root.join("a.txt"), vec![b'x'; 60])
            .await
            .unwrap();

        let err = check_capacity(root, 100, 60, 0)
            .await
            .expect_err("60 + 60 > 100 必须拒绝");
        assert_eq!(err.error, "BadRequest");
        assert!(
            err.message.contains("COGNEVA_WORKSPACE_MAX_BYTES"),
            "拒绝里要写出改哪个部署开关：{}",
            err.message
        );
        assert!(!root.join("b.txt").exists(), "拒绝发生在写之前");
    }

    #[tokio::test]
    async fn rewriting_a_file_counts_its_old_bytes_out_of_the_cap() {
        let dir = tempdir();
        let root = dir.path();
        tokio::fs::write(root.join("a.txt"), vec![b'x'; 60])
            .await
            .unwrap();

        // 覆盖同一份 60 字节：占用不变，不该被拦。
        check_capacity(root, 100, 60, 60)
            .await
            .expect("改一个已经存在的文件不该多算一遍自己");
        // 再写一份新的 60 字节：这时才真的放不下。
        let err = check_capacity(root, 100, 60, 0)
            .await
            .expect_err("再塞一份 60 字节确实超了");
        assert_eq!(err.error, "BadRequest");
    }

    #[tokio::test]
    async fn a_cap_of_zero_means_unlimited_rather_than_read_only() {
        let dir = tempdir();
        let huge = u64::MAX / 2;
        check_capacity(dir.path(), 0, huge, 0)
            .await
            .expect("0 是「不限」，不是「一点也不能写」");
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
