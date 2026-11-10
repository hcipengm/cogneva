//! 全部出网动作共用的「下载停没停」判据。
//!
//! 过去每个下载点各写各的超时：镜像包有、helm 只有连接超时、firecracker 与
//! K3s 安装脚本完全没有、取 `.sha256` 文本走 reqwest 默认（连连接超时也是
//! 默认值）。没有判据的下载在卡住时不是"失败"而是"永远不返回"——装机卡在
//! 某一步、日志停在那里，看起来和死机一模一样。
//!
//! 判据后来换过一次：从"跑了多久就放弃"换成"多久没有新字节才放弃"。前者在一条
//! 慢而正常的线路上也会放弃，而它看见的只有一个数——于是慢与卡死在它眼里同形。
//!
//! 所以这里把判据按**用途**收成常量，谁下载都从这里取，不再各写各的。

use anyhow::{bail, Context, Result};
use std::path::Path;
use std::time::Duration;
use tokio::process::Command;
use tracing::warn;

/// 一组下载的判据。`stall` 是**多久没有新字节**才判下载停住，不是总时长：
/// 一条慢但一直有字节的线路不该被杀，能被看见的停只有一种——字节不再来。
///
/// 这里没有「总时限」那一格，也不许再加：一个按时长放弃的时刻会砍掉一条
/// 慢而正常的线路，而它给出的读数只有「跑了多久」，慢与卡死在它眼里同形。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    pub connect: u64,
    pub stall: u64,
    pub retry: u32,
}

/// curl 的速率下限：只要有字节进来就算在动。
///
/// 写成 1 而不是 0，因为 0 在 curl 里不是「任何速率都算在动」。
const CURL_SPEED_LIMIT_BYTES_PER_SEC: &str = "1";

impl Timeouts {
    /// curl 参数。`--speed-time` 必须始终在——它是 curl 自己的「不动」判据。
    pub fn curl_args(&self) -> Vec<String> {
        vec![
            "-fsSL".into(),
            "--connect-timeout".into(),
            self.connect.to_string(),
            // curl 没有「看门狗」这个词，但这一对就是它：把 `stall` 秒切成一段，
            // 平均速率低于 `--speed-limit` 即放弃。
            "--speed-limit".into(),
            CURL_SPEED_LIMIT_BYTES_PER_SEC.into(),
            "--speed-time".into(),
            self.stall.to_string(),
            "--retry".into(),
            self.retry.to_string(),
        ]
    }
}

/// 装不下去的东西：K3s 安装脚本、helm、预编译镜像的校验文本。
// cap: bound — 一分钟没有新字节就是停住了：这是拍定的界，不是量出来的
pub const MANDATORY: Timeouts = Timeouts {
    connect: 15,
    stall: 60,
    retry: 2,
};

/// 大件：镜像包（数百 MB）。慢网下每段仍会有字节进来，判据看的是字节，不是总时长。
// cap: bound — 大件在慢网上会长时间低速率，窗口放宽到两分钟；判据仍是「没字节」
pub const LARGE: Timeouts = Timeouts {
    connect: 15,
    stall: 120,
    retry: 2,
};

/// 可选组件：装不上就降级走别的形态，不该拖着装机流程（firecracker 即此类）。
// cap: bound — 降级比等待便宜：十几秒没字节就走别的形态
pub const OPTIONAL: Timeouts = Timeouts {
    connect: 5,
    stall: 15,
    retry: 0,
};

/// 可达性探测的时限（与 shell 侧 `probe_reachable` 的 5s 对齐）。
// cap: fixed — 与 shell 侧 probe_reachable 的 5s 对齐：同一件事在两侧不能有两个时限
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

    /// 每一档都必须带「不动」判据，且都不许带总时限。前者是往事：漏掉判据的下载点
    /// 不会报错，它只会永远不返回；后者是这一批拆掉的墙：按时长放弃会砍掉一条慢而
    /// 正常的线路，而它给出的读数看不出慢与卡死的区别。
    #[test]
    fn every_profile_judges_stalled_bytes_and_no_profile_caps_total_time() {
        for t in [MANDATORY, LARGE, OPTIONAL] {
            let args = t.curl_args();
            assert!(
                args.contains(&"--speed-time".to_string()),
                "{t:?} 缺「多久没字节算停住」"
            );
            assert!(
                args.contains(&"--speed-limit".to_string()),
                "{t:?} 缺速率下限"
            );
            assert!(args.contains(&"--connect-timeout".to_string()));
            assert!(
                !args.contains(&"--max-time".to_string()),
                "{t:?} 又带上总时限了：那是按时长放弃，不是按字节判停"
            );
            assert!(t.stall > 0 && t.connect > 0);
        }
    }

    /// 可选组件的判停窗口必须明显短于命门组件：装不上要快速降级，而不是陪着一起等。
    /// 写成编译期判据——这组关系只依赖常量本身，不需要跑测试才发现被改坏。
    const _: () = {
        assert!(OPTIONAL.stall < MANDATORY.stall);
        assert!(OPTIONAL.stall < LARGE.stall);
    };
}
