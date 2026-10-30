//! 直接对集群内 HTTP 面（apiserver / registry）发原文请求时的两个墙。
//!
//! 这几个读取端都不用 reqwest：它们读的是原始响应，想拿到一眼能看懂的字节，
//! 于是自己开 TCP。开连接与读完整个响应是两件事，慢在两处的原因也不同——
//! 前者是策略/DNS/端口不通，后者是对方在吐一个大响应——所以是两条界而不是
//! 一个总超时。四个调用点曾各写一份 30s + 60s。

use std::time::Duration;

/// 连上集群内 HTTP 端口的墙：30s。
///
/// 同机群内本该是毫秒级；30s 留给「apiserver 正在滚动重启」这种短暂不可达，
/// 再久就说明不是等一会能好的问题。
// cap: bound — 同机群内本该是毫秒级；30s 留给 apiserver 滚动重启这类短暂不可达
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// 读完整个响应的墙：60s。
///
/// 这些面里最大的是 registry 的 catalog/tags，几 MB 量级；60s 覆盖集群内
/// 一次性拉完。它单独成界，所以「连上了但对方吐不完」与「连不上」在日志里
/// 是两句话。
// cap: bound — 单独成界，让「连上了但吐不完」与「连不上」在日志里是两句话
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(60);
