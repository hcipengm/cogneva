//! CN 模式换 apt 源。
//!
//! 不换源的后果是安装卡死而不是报错：`apt-get update` 直连 archive.ubuntu.com /
//! deb.debian.org，国内几十 KB/s 甚至一直超时，装 buildah / git 的步骤看起来
//! 只是"很慢"，没有任何一行日志说清它在等什么。
//!
//! 两条路径都要这一步：源码构建路径由 bootstrap.sh 的 ensure_cc 兜住，预编译
//! 引导器路径不经 shell，所以这里再实现一份。两份的宿主清单必须一致，由
//! `apt_host_list_matches_shell` 测试盯着。

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{info, warn};

/// 国内 apt 镜像候选。与 bootstrap.sh 的候选表同源（同样由测试盯着）。
pub const APT_MIRROR_HOSTS: &[&str] = &[
    "https://mirrors.tuna.tsinghua.edu.cn",
    "https://mirrors.ustc.edu.cn",
    "https://mirrors.aliyun.com",
    "https://mirrors.huaweicloud.com",
];

/// 需要改写的官方源主机（连路径）。`&str` 的第二项是镜像站上的相对路径。
const APT_HOST_REWRITES: &[(&str, &str)] = &[
    ("archive.ubuntu.com/ubuntu", "ubuntu"),
    ("security.ubuntu.com/ubuntu", "ubuntu"),
    ("ports.ubuntu.com/ubuntu", "ubuntu"),
    ("deb.debian.org/debian", "debian"),
    ("security.debian.org/debian-security", "debian-security"),
];

/// 换源标记文件：记录已切到哪个基址，命中即跳过（幂等）。
const MIRROR_MARK: &str = "/etc/apt/.cogneva-cn-mirror";

/// 纯函数：把官方 apt 源主机换成镜像基址，其余条目（第三方 PPA、内网源）原样保留。
/// `base` 形如 `https://mirrors.tuna.tsinghua.edu.cn/ubuntu`（已含发行版路径）。
pub fn rewrite_apt_sources(text: &str, base: &str) -> String {
    let mut out = text.to_string();
    for (host, _) in APT_HOST_REWRITES {
        let repl = if host.starts_with("security.debian.org") {
            // debian-security 在镜像站的路径是 debian-security，不是 debian/debian-security
            format!("{base}-security")
        } else {
            base.to_string()
        };
        for scheme in ["http", "https"] {
            out = out.replace(&format!("{scheme}://{host}"), &repl);
        }
    }
    out
}

/// 按 /etc/os-release 读出（发行版 id, 版本代号）。代号用于探活目标路径。
fn distro_id_and_codename() -> (String, String) {
    let text = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
    let field = |key: &str| -> Option<String> {
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{key}=")))
            .map(|v| v.trim().trim_matches('"').to_string())
            .filter(|s| !s.is_empty())
    };
    (
        field("ID").unwrap_or_else(|| "ubuntu".into()),
        field("VERSION_CODENAME").unwrap_or_else(|| "stable".into()),
    )
}

/// 探活出可用的镜像基址（含发行版路径）。探测目标取 `dists/<codename>/Release`：
/// 镜像站有没有收录这个发行版一看便知，比探首页准。
async fn pick_mirror() -> Option<String> {
    let (id, codename) = distro_id_and_codename();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    for host in APT_MIRROR_HOSTS {
        let base = format!("{host}/{id}");
        let probe = format!("{base}/dists/{codename}/Release");
        if client.get(&probe).send().await.is_ok() {
            return Some(base);
        }
        warn!("apt 镜像站不可达，换下一个: {probe}");
    }
    None
}

/// apt 源文件清单：一行式 sources.list 与 deb822 风格的 *.sources 都要覆盖。
fn source_files() -> Vec<PathBuf> {
    let mut files = vec![PathBuf::from("/etc/apt/sources.list")];
    if let Ok(entries) = std::fs::read_dir("/etc/apt/sources.list.d") {
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if matches!(ext, "list" | "sources") && path.is_file() {
                files.push(path);
            }
        }
    }
    files
}

fn read_mark() -> Option<String> {
    std::fs::read_to_string(MIRROR_MARK)
        .ok()
        .map(|s| s.trim().to_string())
}

/// 应用换源：原地改写 + 留 `*.cogneva-orig` 备份；已切到同一基址则跳过。
pub fn apply_apt_mirror(base: &str) -> Result<()> {
    if read_mark().as_deref() == Some(base) {
        return Ok(());
    }
    let mut changed = 0;
    for path in source_files() {
        let Ok(original) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rewritten = rewrite_apt_sources(&original, base);
        if rewritten == original {
            continue;
        }
        let backup = PathBuf::from(format!("{}.cogneva-orig", path.display()));
        if !backup.exists() {
            std::fs::copy(&path, &backup)
                .with_context(|| format!("备份 {} 失败", path.display()))?;
        }
        std::fs::write(&path, rewritten)
            .with_context(|| format!("写入 {} 失败", path.display()))?;
        changed += 1;
    }
    std::fs::write(MIRROR_MARK, base).with_context(|| format!("写入 {MIRROR_MARK} 失败"))?;
    if changed > 0 {
        info!("apt 源已切到 {base}（{changed} 个文件，原件备份为 *.cogneva-orig）");
    }
    Ok(())
}

/// CN 模式下确保 apt 源已换到国内镜像。任何一步不成只是慢，不该让安装失败，
/// 所以探测不到镜像时告警后沿用原有源。
pub async fn ensure_cn_apt_mirror(cn: bool) -> Result<()> {
    if !cn || !Path::new("/usr/bin/apt-get").exists() {
        return Ok(());
    }
    match pick_mirror().await {
        Some(base) => apply_apt_mirror(&base),
        None => {
            warn!("国内 apt 镜像站均不可达，沿用原有源（apt 步骤可能很慢）");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UBUNTU: &str = "https://mirrors.tuna.tsinghua.edu.cn/ubuntu";

    #[test]
    fn rewrites_official_hosts_only() {
        // 一行式
        assert_eq!(
            rewrite_apt_sources("deb http://archive.ubuntu.com/ubuntu noble main", UBUNTU),
            format!("deb {UBUNTU} noble main")
        );
        // deb822
        assert_eq!(
            rewrite_apt_sources("URIs: http://archive.ubuntu.com/ubuntu", UBUNTU),
            format!("URIs: {UBUNTU}")
        );
        // security 主机同路径
        assert_eq!(
            rewrite_apt_sources(
                "deb http://security.ubuntu.com/ubuntu noble-security main",
                UBUNTU
            ),
            format!("deb {UBUNTU} noble-security main")
        );
        // 第三方 PPA / 内网源原样保留
        let third = "deb https://ppa.example.com/ubuntu noble main";
        assert_eq!(rewrite_apt_sources(third, UBUNTU), third);
        // 已经是镜像源的文本不变（幂等的判据）
        let already = format!("deb {UBUNTU} noble main");
        assert_eq!(rewrite_apt_sources(&already, UBUNTU), already);
    }

    #[test]
    fn debian_security_keeps_its_own_path() {
        let base = "https://mirrors.ustc.edu.cn/debian";
        assert_eq!(
            rewrite_apt_sources(
                "deb http://security.debian.org/debian-security bookworm-security main",
                base
            ),
            "deb https://mirrors.ustc.edu.cn/debian-security bookworm-security main"
        );
        assert_eq!(
            rewrite_apt_sources("deb http://deb.debian.org/debian bookworm main", base),
            format!("deb {base} bookworm main")
        );
    }

    /// 两份实现（shell 与 Rust）改的是同一批主机。shell 里是 sed 正则（点号转义过），
    /// 所以两种写法都认；改了宿主清单忘了另一侧，这里红。
    #[test]
    fn apt_host_list_matches_shell() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../bootstrap.sh");
        let shell =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("读取 {path} 失败: {e}"));
        for (host, _) in APT_HOST_REWRITES {
            let escaped = host.replace('.', "\\.");
            assert!(
                shell.contains(host) || shell.contains(&escaped),
                "bootstrap.sh 未改写宿主 {host}（Rust 与 shell 的 apt 宿主清单已漂移）"
            );
        }
        for m in APT_MIRROR_HOSTS {
            assert!(
                shell.contains(m),
                "bootstrap.sh 未列 apt 镜像候选 {m}（两侧候选表已漂移）"
            );
        }
    }
}
