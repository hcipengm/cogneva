//! 全部出网动作共用的超时纪律。
//!
//! 过去每个下载点各写各的超时：镜像包有、helm 只有连接超时、firecracker 与
//! K3s 安装脚本完全没有、取 `.sha256` 文本走 reqwest 默认（**没有**总超时，
//! 连连接超时也是默认值）。没有总超时的下载在墙前不是"失败"而是"永远不返回"
//! ——装机卡在某一步、日志停在那里，看起来和死机一模一样，而它对使用者完全
//! 不可区分。
//!
//! 所以这里把超时按**用途**收成常量，谁下载都从这里取，不再各写各的。

use anyhow::{bail, Context, Result};
use std::path::Path;
use std::time::Duration;
use tokio::process::Command;
use tracing::warn;

/// 一组下载超时。`total` 是**总**时限，不是"两个字节之间"的间隔——只有连接
/// 超时的下载在墙前可以无限期地以极低速率拖着。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    pub connect: u64,
    pub total: u64,
    pub retry: u32,
}

impl Timeouts {
    /// curl 参数。`--max-time` 必须始终在——这正是过去缺的那一项。
    pub fn curl_args(&self) -> Vec<String> {
        vec![
            "-fsSL".into(),
            "--connect-timeout".into(),
            self.connect.to_string(),
            "--max-time".into(),
            self.total.to_string(),
            "--retry".into(),
            self.retry.to_string(),
        ]
    }
}

/// 装不下去的东西：K3s 安装脚本、helm、预编译镜像的校验文本。
pub const MANDATORY: Timeouts = Timeouts {
    connect: 15,
    total: 900,
    retry: 2,
};

/// 大件：镜像包（数百 MB，慢网下允许更久）。
pub const LARGE: Timeouts = Timeouts {
    connect: 15,
    total: 3600,
    retry: 2,
};

/// 可选组件：装不上就降级走别的形态，不该拖着装机流程（firecracker 即此类）。
pub const OPTIONAL: Timeouts = Timeouts {
    connect: 5,
    total: 60,
    retry: 0,
};

/// 可达性探测的时限（与 shell 侧 `probe_reachable` 的 5s 对齐）。
pub const PROBE: Duration = Duration::from_secs(5);

/// 下载到文件。任何失败返回 Err，调用方决定是回退还是降级。
pub async fn curl_to_file(url: &str, dest: &Path, t: Timeouts) -> Result<()> {
    let dest = dest.to_string_lossy().into_owned();
    let mut args = t.curl_args();
    args.push("-o".into());
    args.push(dest);
    args.push(url.to_string());
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    run_curl(&refs, url).await
}

/// 下载到内存（小文件：校验文本、配置）。
pub async fn curl_to_string(url: &str, t: Timeouts) -> Result<String> {
    let mut args = t.curl_args();
    args.push(url.to_string());
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let out = Command::new("curl")
        .args(&refs)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .with_context(|| format!("无法执行 curl 下载 {url}"))?;
    if !out.status.success() {
        bail!("下载失败 {url}（curl 退出码 {:?}）", out.status.code());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

async fn run_curl(args: &[&str], url: &str) -> Result<()> {
    let status = Command::new("curl")
        .args(args)
        .stdin(std::process::Stdio::null())
        .status()
        .await
        .with_context(|| format!("无法执行 curl 下载 {url}"))?;
    if !status.success() {
        bail!("下载失败 {url}（curl 退出码 {:?}）", status.code());
    }
    Ok(())
}

/// 可达性探测：拿到任何 HTTP 响应即算通（401/403 也说明路通到目标了），
/// 只有连接失败/超时才算不通。判据用状态码而不是退出码，`-f` 会把 401 当成失败，
/// 于是把网络正常的机器误判成受限。
pub async fn probe(url: &str, within: Duration) -> bool {
    let client = match reqwest::Client::builder().timeout(within).build() {
        Ok(c) => c,
        Err(_) => return false,
    };
    client.get(url).send().await.is_ok()
}

/// 探活候选表，返回第一个可达项的值；全不可达回退第一项并告警。
/// 表项为（选用值，探活 URL）。
pub async fn pick_alive(candidates: &[(&str, &str)]) -> String {
    for (value, probe_url) in candidates {
        if probe(probe_url, PROBE).await {
            return (*value).to_string();
        }
        warn!("镜像不可达，换下一个: {probe_url}");
    }
    candidates[0].0.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 每一档都必须带总超时。这条断言是往事：漏掉 `--max-time` 的下载点不会
    /// 报错，它只会永远不返回。
    #[test]
    fn every_profile_carries_a_total_timeout() {
        for t in [MANDATORY, LARGE, OPTIONAL] {
            let args = t.curl_args();
            assert!(args.contains(&"--max-time".to_string()), "{t:?} 缺总超时");
            assert!(args.contains(&"--connect-timeout".to_string()));
            assert!(t.total > 0 && t.connect > 0);
            // 连接超时不能大于总时限（否则总时限形同虚设）
            assert!(t.connect <= t.total, "{t:?} 连接超时大于总时限");
        }
    }

    /// 可选组件的超时必须明显短于命门组件：装不上要快速降级，而不是陪着一起等。
    /// 写成编译期判据——这组关系只依赖常量本身，不需要跑测试才发现被改坏。
    const _: () = {
        assert!(OPTIONAL.total < MANDATORY.total);
        assert!(OPTIONAL.total < LARGE.total);
    };
}
