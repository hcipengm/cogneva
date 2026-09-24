//! 传输选路契约：给定「操作 + 平台 + 网络画像 + 手上有什么凭证」，给出按序尝试的
//! 传输通道，以及**为什么是它**。
//!
//! 存在的理由：管道里的每一环过去各自写死了自己的取数方式——引导链默认 HTTPS、
//! 网关把 SSH 当 HTTPS 的兜底、release 附件干脆没有选路概念。于是同一台机器上
//! 「国内该走 SSH」和「release 附件只能走 HTTPS」两个事实分散在三处，谁都不知道
//! 另一个。这里把它们收成一张表，让所有取数环节问同一个函数。
//!
//! 表里最容易搞错的一条是 release 附件：它不是 git 对象，跑在 HTTP API 上，
//! SSH 通道**结构上**够不着它（ssh 只承载 git 的 packfile/refs，没有"上传附件"
//! 这个动作）。所以「国内优先 SSH」这条规则**不适用于 release 资产**——那里的
//! 答案恒为 HTTPS，与网络受限与否无关。把它写死在表里，而不是留给调用方各自判断。

use serde::{Deserialize, Serialize};

/// 取数动作。选路只看动作性质，与调用方是谁无关。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// 从上游仓库取对象（fetch / clone / ls-remote）
    GitFetch,
    /// 向上游仓库写 ref（push）
    GitPush,
    /// 下载 release 附件（镜像包、预编译引导器等）
    ReleaseAssetDownload,
    /// 上传 release 附件
    ReleaseAssetUpload,
}

impl Operation {
    /// 该动作是否承载在 git 传输协议上（决定 SSH 是否可能可用）。
    pub fn is_git(self) -> bool {
        matches!(self, Operation::GitFetch | Operation::GitPush)
    }
}

/// 平台。两端能力不同：镜像基线只维护 GitHub 侧，Gitee 侧没有登记部署密钥。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    GitHub,
    Gitee,
}

/// 手上实际有的凭证。这是 SSH 可用性的**唯一**输入——没有密钥就是没有通道，
/// 网络再受限也不会凭空多出一条。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Credential {
    /// 裸奔：既无 token 也无部署密钥
    None,
    /// 平台 token（可走 HTTPS 认证）
    Token,
    /// 部署密钥（可走 SSH）
    SshKey,
}

/// 传输通道。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    /// `git@host:owner/repo.git`，凭密钥登录
    Ssh,
    /// `https://host/owner/repo.git` 与所有 REST 调用
    Https,
}

/// 网络画像。只有两档：受限（存在被墙的目标）与开放。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkProfile {
    Open,
    Restricted,
}

/// 受限网络判据的探测信号（探活 URL）。
///
/// 三条信号都是**安装链路真正依赖**的目标，不是随便找的墙外地址：docker.io 决定
/// 系统镜像能不能拉，raw.githubusercontent.com 决定入口脚本与源码能不能取，
/// rustup 分发域决定工具链能不能装。任何一条不可达都说明这台机器上对应环节必须
/// 走镜像/替代通道。
///
/// 判据方向是**偏保守**的：任何一条不可达即判受限，全部可达才判开放。两个方向
/// 的代价不对称——误判成"开放"会让安装在墙前挂死（拉镜像超时、取码 404），
/// 误判成"受限"只是多走一趟镜像站，慢但能成。
///
/// bootstrap.sh 探测的是同一组 URL；`bootstrap_signal_table_matches_shell` 测试
/// 守着这两份清单不漂移。
pub const RESTRICTED_NET_SIGNALS: &[&str] = &[
    "https://registry-1.docker.io/v2/",
    "https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh",
    "https://static.rust-lang.org/rustup/release-stable.toml",
];

/// 单条信号的探测结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetProbe {
    pub url: String,
    pub reachable: bool,
}

/// 网络画像判定结果，带出判定所依据的全部证据（哪几条不通）。
///
/// 证据随判定一起走：只报一个 `Restricted` 而不说是哪条信号不通，事后无从复核，
/// 也无从判断是不是某条信号自己的抖动。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkVerdict {
    pub profile: NetworkProfile,
    pub evidence: Vec<NetProbe>,
}

impl NetworkVerdict {
    /// 按信号表判定。空证据返回 `None`——一条都没探到就不能断言网络形态，
    /// 调用方应保持上一次的结论（见 `resolve`）。
    pub fn from_probes(evidence: Vec<NetProbe>) -> Option<Self> {
        if evidence.is_empty() {
            return None;
        }
        let profile = if evidence.iter().any(|p| !p.reachable) {
            NetworkProfile::Restricted
        } else {
            NetworkProfile::Open
        };
        Some(Self { profile, evidence })
    }

    /// 判定的可读理由：受限时说清是哪几条信号不通。
    pub fn rationale(&self) -> String {
        match self.profile {
            NetworkProfile::Open => "全部探测目标可达".to_string(),
            NetworkProfile::Restricted => {
                let down: Vec<&str> = self
                    .evidence
                    .iter()
                    .filter(|p| !p.reachable)
                    .map(|p| p.url.as_str())
                    .collect();
                format!("不可达: {}", down.join(", "))
            }
        }
    }

    /// 与上一次结论合并，决定当前该按哪一档走。
    ///
    /// 恢复方向要证据：从受限回到开放**必须**是"这一轮所有信号都通了"，
    /// 而不是"上次探测过去了多久"。时间窗一到就当作恢复，等于把一个未经验证的
    /// 假设当成事实——墙没拆的时候它会稳定地把流量送回失败的那条路。
    /// 反向（开放 → 受限）单次证据即可，因为它的代价只是多走一趟镜像。
    pub fn resolve(prev: Option<&NetworkVerdict>, fresh: Option<NetworkVerdict>) -> Option<Self> {
        match fresh {
            // 这一轮一条信号都没探到：不改变结论
            None => prev.cloned(),
            Some(fresh) => match (prev.map(|p| p.profile), fresh.profile) {
                (Some(NetworkProfile::Restricted), NetworkProfile::Open) => {
                    // 全部信号可达才算恢复；判据已在 from_probes 里保证
                    Some(fresh)
                }
                _ => Some(fresh),
            },
        }
    }
}

/// 一条选路结论：按序尝试的通道 + 为什么。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportPlan {
    /// 按序尝试。首个是首选，其余是失败后的回落顺序。
    pub order: Vec<Transport>,
    /// 选路理由，直接可打日志（中文，面向运维）。
    pub rationale: String,
}

impl TransportPlan {
    pub fn primary(&self) -> Option<Transport> {
        self.order.first().copied()
    }

    /// 不含指定通道的次序（用于"首选失败了，剩下的怎么走"）。
    pub fn without(&self, t: Transport) -> Vec<Transport> {
        self.order.iter().copied().filter(|x| *x != t).collect()
    }
}

/// 一条候选通道的实测结果。`latency_ms` 是完成一次**真实握手**的耗时
/// （对 git 就是一次 `ls-remote`：走完 DNS、TCP、TLS、认证与 refs 往返）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportMeasurement {
    pub transport: Transport,
    pub reachable: bool,
    pub latency_ms: u64,
}

/// 按实测结果排序：可达的按延迟升序在前，不可达的按原顺序留在后面。
///
/// 不可达的**不剔除**——一次探测失败不等于永久失败（探测时墙在抖动、密钥刚
/// 装好还没生效都会这样），保留在次序里当兜底，比事后发现"唯一候选被自己删掉了"
/// 好得多。
///
/// 全部不可达时返回 None：这时候实测没有信息量，调用方应退回策略表，而不是
/// 从一个空结论里编一个次序出来。
pub fn rank_by_measurement(measurements: &[TransportMeasurement]) -> Option<Vec<Transport>> {
    let mut reachable: Vec<&TransportMeasurement> =
        measurements.iter().filter(|m| m.reachable).collect();
    if reachable.is_empty() {
        return None;
    }
    reachable.sort_by_key(|m| m.latency_ms);
    let mut order: Vec<Transport> = reachable.iter().map(|m| m.transport).collect();
    for m in measurements {
        if !m.reachable && !order.contains(&m.transport) {
            order.push(m.transport);
        }
    }
    Some(order)
}

/// 实测优先、策略表兜底：把两者合成最终次序。
///
/// 实测之所以优先，是因为区域画像只是**猜测**：同一个国家里，一台机器 22 端口被
/// 重置、另一台只是慢，两者在"CN → SSH 优先"这条规则下拿到同一个答案，而实测
/// 能把它们分开。策略表的意义退回到"实测给不出结论时的默认值"（首次启动、
/// 探测全失败、没有凭证），而不是日常的决策依据。
///
/// 实测也不比策略表更高明的地方要说清楚：一次握手的延迟**不预测**大流量下的
/// 吞吐（TLS 握手 200ms 的连接照样可能被限速到 20KB/s）。所以实测只决定
/// "先用哪个"，失败回落次序照旧生效——判定归证据，不归一次性测量。
pub fn resolve(measured: Option<Vec<Transport>>, policy: TransportPlan) -> TransportPlan {
    match measured {
        Some(order) if !order.is_empty() => TransportPlan {
            order,
            rationale: format!("实测排序（{}）", policy.rationale),
        },
        _ => policy,
    }
}

/// 选路表本体。纯函数：同样的输入永远给同样的次序，不碰网络也不读时钟。
pub fn plan(
    op: Operation,
    platform: Platform,
    net: NetworkProfile,
    cred: Credential,
) -> TransportPlan {
    // release 附件恒定 HTTPS：附件不是 git 对象，SSH 通道结构上够不着。
    // 这一条与网络画像、与平台都无关，先短路掉再谈其它。
    if matches!(
        op,
        Operation::ReleaseAssetDownload | Operation::ReleaseAssetUpload
    ) {
        return TransportPlan {
            order: vec![Transport::Https],
            rationale: "release 附件走 HTTP API（附件不在 git 传输协议内，SSH 结构上不可达）"
                .to_string(),
        };
    }
    debug_assert!(op.is_git(), "非 git 动作应在上面短路");
    if platform == Platform::Gitee {
        return TransportPlan {
            order: vec![Transport::Https],
            rationale: "Gitee 侧未登记部署密钥，只有 HTTPS 通道".to_string(),
        };
    }
    if cred != Credential::SshKey {
        return TransportPlan {
            order: vec![Transport::Https],
            rationale: "无部署密钥，SSH 通道不可用（仅 HTTPS）".to_string(),
        };
    }
    match net {
        NetworkProfile::Restricted => TransportPlan {
            order: vec![Transport::Ssh, Transport::Https],
            rationale: "受限网络：GitHub 的 HTTPS 取码/推送不稳，SSH 优先，失败回落 HTTPS"
                .to_string(),
        },
        NetworkProfile::Open => TransportPlan {
            order: vec![Transport::Https, Transport::Ssh],
            rationale: "开放网络：HTTPS 优先（无需密钥管理），失败回落 SSH".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(url: &str, reachable: bool) -> NetProbe {
        NetProbe {
            url: url.to_string(),
            reachable,
        }
    }

    #[test]
    fn any_unreachable_signal_means_restricted() {
        let all_up = RESTRICTED_NET_SIGNALS
            .iter()
            .map(|u| probe(u, true))
            .collect();
        assert_eq!(
            NetworkVerdict::from_probes(all_up).unwrap().profile,
            NetworkProfile::Open
        );
        // 单条不通即受限，且理由里点名是哪条
        let one_down = vec![
            probe(RESTRICTED_NET_SIGNALS[0], false),
            probe(RESTRICTED_NET_SIGNALS[1], true),
        ];
        let v = NetworkVerdict::from_probes(one_down).unwrap();
        assert_eq!(v.profile, NetworkProfile::Restricted);
        assert!(v.rationale().contains("registry-1.docker.io"));
        // 一条证据都没有：不下结论（None），而不是猜一个
        assert!(NetworkVerdict::from_probes(Vec::new()).is_none());
    }

    #[test]
    fn recovery_needs_full_evidence_but_failure_does_not() {
        let restricted = NetworkVerdict::from_probes(vec![probe("a", false)]).unwrap();
        let open = NetworkVerdict::from_probes(vec![probe("a", true), probe("b", true)]).unwrap();
        // 开放 → 受限：单次证据即接受（代价只是多走镜像）
        assert_eq!(
            NetworkVerdict::resolve(Some(&open), Some(restricted.clone()))
                .unwrap()
                .profile,
            NetworkProfile::Restricted
        );
        // 受限 → 开放：接受，但前提是这一轮全部信号可达（由 from_probes 保证）
        assert_eq!(
            NetworkVerdict::resolve(Some(&restricted), Some(open))
                .unwrap()
                .profile,
            NetworkProfile::Open
        );
        // 这一轮什么都没探到：保持上一次结论，不因为"时间过去了"就当恢复
        assert_eq!(
            NetworkVerdict::resolve(Some(&restricted), None)
                .unwrap()
                .profile,
            NetworkProfile::Restricted
        );
    }

    #[test]
    fn release_assets_never_route_over_ssh() {
        // 这是本模块存在的主要理由：再受限的网络、再有密钥，release 附件也没有
        // SSH 通道可用——附件不是 git 对象。
        for op in [
            Operation::ReleaseAssetDownload,
            Operation::ReleaseAssetUpload,
        ] {
            for net in [NetworkProfile::Open, NetworkProfile::Restricted] {
                for cred in [Credential::None, Credential::Token, Credential::SshKey] {
                    let p = plan(op, Platform::GitHub, net, cred);
                    assert_eq!(p.order, vec![Transport::Https]);
                    assert!(!p.order.contains(&Transport::Ssh));
                }
            }
        }
    }

    #[test]
    fn restricted_github_git_prefers_ssh_when_key_exists() {
        let p = plan(
            Operation::GitPush,
            Platform::GitHub,
            NetworkProfile::Restricted,
            Credential::SshKey,
        );
        assert_eq!(p.order, vec![Transport::Ssh, Transport::Https]);
        assert_eq!(p.primary(), Some(Transport::Ssh));
        // 同样条件下没有密钥 → 只剩 HTTPS，网络形态不改变这个事实
        let no_key = plan(
            Operation::GitPush,
            Platform::GitHub,
            NetworkProfile::Restricted,
            Credential::None,
        );
        assert_eq!(no_key.order, vec![Transport::Https]);
        // 开放网络反过来：HTTPS 优先，SSH 兜底
        let open = plan(
            Operation::GitPush,
            Platform::GitHub,
            NetworkProfile::Open,
            Credential::SshKey,
        );
        assert_eq!(open.order, vec![Transport::Https, Transport::Ssh]);
        assert_eq!(open.without(Transport::Https), vec![Transport::Ssh]);
    }

    #[test]
    fn gitee_git_is_https_only() {
        let p = plan(
            Operation::GitFetch,
            Platform::Gitee,
            NetworkProfile::Restricted,
            Credential::SshKey,
        );
        assert_eq!(p.order, vec![Transport::Https]);
    }

    #[test]
    fn plan_is_deterministic_and_every_reason_is_nonempty() {
        for op in [
            Operation::GitFetch,
            Operation::GitPush,
            Operation::ReleaseAssetDownload,
            Operation::ReleaseAssetUpload,
        ] {
            for platform in [Platform::GitHub, Platform::Gitee] {
                for net in [NetworkProfile::Open, NetworkProfile::Restricted] {
                    for cred in [Credential::None, Credential::Token, Credential::SshKey] {
                        let a = plan(op, platform, net, cred);
                        let b = plan(op, platform, net, cred);
                        assert_eq!(a, b);
                        assert!(!a.order.is_empty(), "{op:?} 选路结果为空");
                        assert!(!a.rationale.is_empty(), "{op:?} 缺理由");
                    }
                }
            }
        }
    }

    /// bootstrap.sh 里的探测清单必须与本模块的信号表一致。shell 侧没法复用 Rust
    /// 常量，只能把同一组 URL 再写一遍——这份测试就是那双盯着它的眼睛：改了
    /// 一边忘了另一边，这里红。
    #[test]
    fn bootstrap_signal_table_matches_shell() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../bootstrap.sh");
        let shell =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("读取 {path} 失败: {e}"));
        for url in RESTRICTED_NET_SIGNALS {
            assert!(
                shell.contains(url),
                "bootstrap.sh 未探测信号 {url}（RESTRICTED_NET_SIGNALS 与 shell 清单已漂移）"
            );
        }
    }

    fn measured(t: Transport, reachable: bool, latency_ms: u64) -> TransportMeasurement {
        TransportMeasurement {
            transport: t,
            reachable,
            latency_ms,
        }
    }

    #[test]
    fn measurement_outranks_the_region_guess() {
        // 反例就是这条规则存在的原因：国内网络里 22 端口被重置、HTTPS 只是慢。
        // 区域画像说"受限 → SSH 优先"，实测说 HTTPS 更快——实测赢。
        let policy = plan(
            Operation::GitFetch,
            Platform::GitHub,
            NetworkProfile::Restricted,
            Credential::SshKey,
        );
        let order = rank_by_measurement(&[
            measured(Transport::Ssh, false, 0),
            measured(Transport::Https, true, 800),
        ]);
        let resolved = resolve(order, policy);
        assert_eq!(resolved.primary(), Some(Transport::Https));
        // 不可达的 SSH 不被剔除：它还在次序里当兜底
        assert_eq!(resolved.order, vec![Transport::Https, Transport::Ssh]);
    }

    #[test]
    fn measurement_orders_reachable_by_latency_and_falls_back_when_empty() {
        let order = rank_by_measurement(&[
            measured(Transport::Https, true, 900),
            measured(Transport::Ssh, true, 200),
        ])
        .unwrap();
        assert_eq!(order, vec![Transport::Ssh, Transport::Https]);

        // 一条都不可达 = 没有结论，退回策略表（而不是给一个空次序）
        assert!(rank_by_measurement(&[
            measured(Transport::Ssh, false, 0),
            measured(Transport::Https, false, 0),
        ])
        .is_none());
        assert!(rank_by_measurement(&[]).is_none());

        let policy = plan(
            Operation::GitFetch,
            Platform::GitHub,
            NetworkProfile::Open,
            Credential::SshKey,
        );
        let resolved = resolve(None, policy.clone());
        assert_eq!(resolved, policy);
        assert!(resolved.rationale.contains("开放网络"));
    }
}
