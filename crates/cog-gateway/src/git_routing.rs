//! git 传输选路：**实测优先、策略表兜底**。
//!
//! 网关对 GitHub 有两条通道——HTTPS 透传（带平台 token）与自持 SSH 镜像（带部署
//! 密钥）。用哪条不是配置项，而是一个判据：受限网络下 HTTPS 取码/推送会随机挂住，
//! 开放网络下 HTTPS 反而更快（不用维护密钥往返）。判据本体是纯函数，在
//! `cog-core` 的选路契约里（本文档不重复它的规则表）。
//!
//! 本模块只做三件事：给出**该走哪条**（`TransportRouting::plan_for`）、给出
//! **实测证据**（`measure_channels` / `probe_signals`）、把两者锁存起来
//! （`TransportRouting::latch`）。
//!
//! 三条容易做错的地方，逐条写明：
//!
//! - **不阻塞请求**。实测要跑一次真实握手（秒级），绝不能挡在 git 请求的路径上。
//!   实测在后台跑（`TransportRouting::spawn_measurement`），首个请求用策略表的
//!   结论，后续请求用实测结论。慢一秒拿到更准的排序，比让每个请求多等一次握手好。
//! - **实测不是永久结论**。区域画像会变（墙拆了、CDN 调度变了），所以实测有保鲜期，
//!   过期重测；而失败方向由熔断窗管（`GitTransportHealth`），两者互不替代：
//!   实测说"谁快"，熔断说"谁此刻不能用"。
//! - **失败方向不对称**。探针一条不通即判受限——**任何一个信号不可达就说明这个
//!   网络不是开放网络**（判断反了的代价是让安装卡在一次注定超时的下载上）；
//!   反向恢复要全部信号都通（判断反了只是多走一趟镜像站）。
//!
//! ## 实测为什么只决定"先用哪个"
//!
//! 一次握手的延迟**不预测**大流量下的吞吐：TLS 握手 200ms 的连接照样可能被限速到
//! 20KB/s。所以实测排序只换首选，回落次序照旧按策略表生效；判定归证据，不归
//! 一次性测量。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cog_core::contract::transport::{
    plan as policy_plan, rank_by_measurement, resolve as resolve_against_policy, Credential,
    NetProbe, NetworkProfile, NetworkVerdict, Operation, Platform, Transport, TransportMeasurement,
    TransportPlan, NET_PROFILE_ENV, RESTRICTED_NET_SIGNALS,
};

use crate::git_mirror::{ssh_command_for, ssh_url_for, GitMirrorConfig};

/// 实测结论的保鲜期。取得比熔断探测节拍长（实测是秒级成本 + 一次真实握手，
/// 熔断探测是廉价的一次连接），但短到墙拆掉后几分钟内就能反映出来。
const MEASURE_FRESHNESS_SECS: u64 = 600;
/// 单通道握手上限。**这是探测预算，不是通道预算**：超时即判该通道此刻不可用，
/// 正常请求的等待上限仍由 `GitTransport::https_timeout` 决定。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);
/// 单条信号探针的上限。与引导链的短探针同量级：判的是"通不通"，不是"多快"。
const SIGNAL_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// 选路状态：探针结论 + 实测次序，都锁存在进程内。
///
/// 锁存而不是每次现算，是因为判据只能来自**证据**：一次握手的结果在几十秒内
/// 都还有效，而每个 git 请求都重测一遍会把网关变成打点机器。
pub(crate) struct TransportRouting {
    /// 部署期盖章的网络画像，探针无结论时的默认档。
    stamped: Option<NetworkProfile>,
    /// 探针结论（带证据）。
    verdict: Mutex<Option<NetworkVerdict>>,
    /// 实测次序（可达按延迟升序，不可达留在后面兜底）。
    order: Mutex<Option<Vec<Transport>>>,
    /// 上次实测时刻。
    measured_at: Mutex<Option<Instant>>,
    /// 上一次真正用出去的首选通道。选路是逐请求判的，但**只有换边才值得
    /// 打一行**——每个请求一行 info 会把日志淹没，而"换边"恰恰是运维需要看的
    /// 那件事（受限网络下切到 SSH、墙拆了切回 HTTPS）。
    last_primary: Mutex<Option<Transport>>,
    /// 同时只允许一次实测在跑：一批请求同时进来时，第一个跑，其余用锁存结论。
    measuring: AtomicBool,
}

impl TransportRouting {
    pub(crate) fn new(stamped: Option<NetworkProfile>) -> Self {
        Self {
            stamped,
            verdict: Mutex::new(None),
            order: Mutex::new(None),
            measured_at: Mutex::new(None),
            last_primary: Mutex::new(None),
            measuring: AtomicBool::new(false),
        }
    }

    /// 从环境读部署期盖章的画像。认不出来就不盖章（走探针 / 开放网络默认）。
    pub(crate) fn from_env() -> Self {
        Self::new(
            std::env::var(NET_PROFILE_ENV)
                .ok()
                .and_then(|v| NetworkProfile::from_stamp(&v)),
        )
    }

    /// 当前该按哪一档网络走：探针结论优先，其次部署期盖章，最后按开放网络
    /// （与加选路之前的行为一致，不引入静默的行为变化）。
    pub(crate) fn profile(&self) -> NetworkProfile {
        if let Some(v) = self.verdict.lock().unwrap().as_ref() {
            return v.profile;
        }
        self.stamped.unwrap_or(NetworkProfile::Open)
    }

    /// 当前选路结论 + 理由。`cred` 决定 SSH 是不是真的可用：没有部署密钥时，
    /// 实测就算把 SSH 排在第一，也得把它剔出去——否则就是让请求去撞一条结构上
    /// 不存在的通道。
    pub(crate) fn plan_for(&self, op: Operation, cred: Credential) -> TransportPlan {
        let policy = policy_plan(op, Platform::GitHub, self.profile(), cred);
        let usable = |t: &Transport| *t == Transport::Https || cred == Credential::SshKey;
        let measured = self
            .order
            .lock()
            .unwrap()
            .clone()
            .map(|o| o.into_iter().filter(usable).collect::<Vec<Transport>>());
        resolve_against_policy(measured, policy)
    }

    /// 记下本轮真正用出去的首选通道，返回"是不是换边了"。
    ///
    /// 首次调用也算换边（从"没有结论"到"有结论"，运维正需要看到第一条选路日志）。
    pub(crate) fn note_decision(&self, primary: Option<Transport>) -> bool {
        let mut last = self.last_primary.lock().unwrap();
        if *last == primary {
            return false;
        }
        *last = primary;
        true
    }

    /// 判定的可读证据：走的是策略表还是实测，受限时是哪几条信号不通。
    pub(crate) fn evidence(&self) -> String {
        let by_measurement = self.order.lock().unwrap().is_some();
        let verdict = self.verdict.lock().unwrap().clone();
        match verdict {
            Some(v) => format!(
                "{}（画像{}，{}）",
                if by_measurement {
                    "实测排序"
                } else {
                    "策略表"
                },
                match v.profile {
                    NetworkProfile::Open => "开放",
                    NetworkProfile::Restricted => "受限",
                },
                v.rationale()
            ),
            None => match self.stamped {
                Some(p) => format!(
                    "策略表（部署期盖章{}，尚无探针结论）",
                    match p {
                        NetworkProfile::Open => "开放",
                        NetworkProfile::Restricted => "受限",
                    }
                ),
                None => "策略表（尚无任何证据，按开放网络默认）".to_string(),
            },
        }
    }

    /// 锁存一轮实测结果。次序与画像分开存：次序可能来自一次成功握手、画像可能
    /// 一条信号都没探到，两者独立成立，不该互相顶掉。
    pub(crate) fn latch(&self, measurements: &[TransportMeasurement], probes: Vec<NetProbe>) {
        if let Some(order) = rank_by_measurement(measurements) {
            *self.order.lock().unwrap() = Some(order);
        }
        let prev = self.verdict.lock().unwrap().clone();
        let merged = NetworkVerdict::resolve(prev.as_ref(), NetworkVerdict::from_probes(probes));
        if let Some(v) = merged {
            *self.verdict.lock().unwrap() = Some(v);
        }
        *self.measured_at.lock().unwrap() = Some(Instant::now());
    }

    /// 该不该发起一轮实测：没有结论，或结论过期了。
    pub(crate) fn due_for_measurement(&self) -> bool {
        let at = *self.measured_at.lock().unwrap();
        match at {
            None => true,
            Some(t) => t.elapsed() >= Duration::from_secs(MEASURE_FRESHNESS_SECS),
        }
    }

    /// 后台跑一轮实测并锁存。**不阻塞调用方**，重复触发会被 `measuring` 挡掉。
    ///
    /// 没有部署密钥也照测：**"哪条通道通不通"和"手上有没有密钥"是两件事**。
    /// 没有密钥时次序里只剩 HTTPS（`measure_channels` 只给一条），但网络画像
    /// 仍要有结论——它是部署面盖章之外的唯一证据来源，也是"为什么这么选"的
    /// 那半句日志。把两者绑在一起会让无密钥部署永远说不出自己处在什么网络里。
    pub(crate) fn spawn_measurement(self: &Arc<Self>, config: GitMirrorConfig, repo: String) {
        if !self.due_for_measurement() {
            return;
        }
        if self.measuring.swap(true, Ordering::SeqCst) {
            return;
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let client = NetworkProbeClient::from_env();
            // 两条通道的握手与网络画像并行探：实测是秒级成本，串起来就是两倍
            let (measurements, probes) =
                tokio::join!(measure_channels(&config, &repo), probe_signals(&client));
            tracing::info!(
                order = ?measurements
                    .iter()
                    .map(|m| (m.transport, m.reachable, m.latency_ms))
                    .collect::<Vec<_>>(),
                probes = probes.len(),
                "git 传输选路实测完成"
            );
            this.latch(&measurements, probes);
            this.measuring.store(false, Ordering::SeqCst);
        });
    }
}

/// 实测两条通道。不可达的**也留在结果里**（带上 `reachable: false`）——次序合成
/// 时它们会留在末尾当兜底，而不是被这次探测删掉。
pub(crate) async fn measure_channels(
    config: &GitMirrorConfig,
    repo: &str,
) -> Vec<TransportMeasurement> {
    let client = NetworkProbeClient::from_env();
    let (https, ssh) = tokio::join!(measure_https(&client, repo), measure_ssh(config, repo));
    match ssh {
        Some(s) => vec![https, s],
        None => vec![https],
    }
}

/// HTTPS 通道的握手实测：`info/refs` 是 smart HTTP 的第一个往返，走完 DNS、TCP、
/// TLS 与服务端应答。**不带凭证**——401 也是完成的往返，说明通道通；授权与否由
/// 选路契约里的凭证条件管，不该混进"通道快不快"里。
async fn measure_https(client: &NetworkProbeClient, repo: &str) -> TransportMeasurement {
    let url = format!("https://github.com/{repo}.git/info/refs?service=git-upload-pack");
    let started = Instant::now();
    let reachable = client.probe(&url, HANDSHAKE_TIMEOUT).await;
    TransportMeasurement {
        transport: Transport::Https,
        reachable,
        latency_ms: started.elapsed().as_millis() as u64,
    }
}

/// SSH 通道的握手实测：用**与镜像刷新逐字节相同的命令**做一次 `ls-remote`。
/// 没有部署密钥时返回 None（这条通道不可用，不是"不可达"）。
async fn measure_ssh(config: &GitMirrorConfig, repo: &str) -> Option<TransportMeasurement> {
    let key = config.ssh_key.as_ref()?;
    let url = ssh_url_for(&config.ssh_base, repo);
    let started = Instant::now();
    let out = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio::process::Command::new("git")
            .env("GIT_SSH_COMMAND", ssh_command_for(key))
            .env("GIT_TERMINAL_PROMPT", "0")
            .args(["ls-remote", "--exit-code", &url, "HEAD"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status(),
    )
    .await;
    let reachable = matches!(out, Ok(Ok(s)) if s.success());
    Some(TransportMeasurement {
        transport: Transport::Ssh,
        reachable,
        // 超时的耗时记满预算：它要能和其他通道比（一个"0ms 且不可达"的样本
        // 会让理由里的数字自相矛盾）
        latency_ms: started.elapsed().as_millis() as u64,
    })
}

/// 信号探针的 HTTP 客户端。选路探测与业务请求分开持有：探测的超时必须短而硬，
/// 不该被业务侧的长超时传染。
pub(crate) struct NetworkProbeClient {
    client: reqwest::Client,
}

impl NetworkProbeClient {
    pub(crate) fn from_env() -> Self {
        let client = reqwest::Client::builder()
            // 探针在无凭据、无重定向依赖的路径上跑；连接与总超时都给足，
            // 具体单次上限由调用方按用途给（见 SIGNAL_PROBE_TIMEOUT）。
            .connect_timeout(SIGNAL_PROBE_TIMEOUT)
            .user_agent("cogneva-gateway/route-probe")
            .build()
            .unwrap_or_default();
        Self { client }
    }

    /// 一次探针：拿到**任何**应答即算可达（状态码无关，401/403 也证明通道通）。
    pub(crate) async fn probe(&self, url: &str, within: Duration) -> bool {
        matches!(
            tokio::time::timeout(within, self.client.get(url).send()).await,
            Ok(Ok(_))
        )
    }
}

/// 按同一份信号表探网络画像。清单来自选路契约（`RESTRICTED_NET_SIGNALS`），
/// 与引导脚本是同一组目标——两份清单漂移过就会得出两个画像。
pub(crate) async fn probe_signals(client: &NetworkProbeClient) -> Vec<NetProbe> {
    let probes = RESTRICTED_NET_SIGNALS
        .iter()
        .map(|url| client.probe(url, SIGNAL_PROBE_TIMEOUT));
    let results = futures::future::join_all(probes).await;
    RESTRICTED_NET_SIGNALS
        .iter()
        .zip(results)
        .map(|(url, reachable)| NetProbe {
            url: (*url).to_string(),
            reachable,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(t: Transport, reachable: bool, latency_ms: u64) -> TransportMeasurement {
        TransportMeasurement {
            transport: t,
            reachable,
            latency_ms,
        }
    }

    fn routing_full() -> TransportRouting {
        TransportRouting::new(None)
    }

    #[test]
    fn a_measured_order_beats_the_policy_table() {
        let r = routing_full();
        // 开放网络按策略表是 HTTPS 优先，但实测 SSH 更快 → 以实测为准
        r.latch(
            &[
                ms(Transport::Https, true, 900),
                ms(Transport::Ssh, true, 120),
            ],
            vec![],
        );
        let plan = r.plan_for(Operation::GitFetch, Credential::SshKey);
        assert_eq!(plan.primary(), Some(Transport::Ssh));
    }

    #[test]
    fn an_unreachable_channel_keeps_a_place_in_the_order() {
        let r = routing_full();
        r.latch(
            &[
                ms(Transport::Https, false, 0),
                ms(Transport::Ssh, true, 200),
            ],
            vec![],
        );
        let plan = r.plan_for(Operation::GitFetch, Credential::SshKey);
        // 可达的在前，不可达的留作兜底——一次探测失败不等于永久失败
        assert_eq!(plan.order, vec![Transport::Ssh, Transport::Https]);
    }

    #[test]
    fn ssh_is_dropped_when_there_is_no_deploy_key() {
        let r = routing_full();
        r.latch(
            &[
                ms(Transport::Https, true, 900),
                ms(Transport::Ssh, true, 120),
            ],
            vec![],
        );
        let plan = r.plan_for(Operation::GitFetch, Credential::Token);
        // 实测把 SSH 排第一，但没有密钥时它结构上不可用，不能留在次序里
        assert_eq!(plan.order, vec![Transport::Https]);
    }

    #[test]
    fn no_evidence_falls_back_to_the_stamped_profile() {
        let r = TransportRouting::new(Some(NetworkProfile::Restricted));
        let plan = r.plan_for(Operation::GitPush, Credential::SshKey);
        assert_eq!(plan.order, vec![Transport::Ssh, Transport::Https]);
        assert!(r.evidence().contains("部署期盖章受限"));
    }

    #[test]
    fn no_evidence_at_all_keeps_the_old_behaviour() {
        // 没有任何证据时按开放网络：与加选路之前（HTTPS 主、SSH 兜底）逐字一致
        let r = routing_full();
        let plan = r.plan_for(Operation::GitFetch, Credential::SshKey);
        assert_eq!(plan.order, vec![Transport::Https, Transport::Ssh]);
        assert!(r.evidence().contains("尚无任何证据"));
    }

    #[test]
    fn a_reachable_signal_does_not_recover_from_restricted() {
        let r = routing_full();
        r.latch(
            &[],
            vec![
                NetProbe {
                    url: RESTRICTED_NET_SIGNALS[0].to_string(),
                    reachable: false,
                },
                NetProbe {
                    url: RESTRICTED_NET_SIGNALS[1].to_string(),
                    reachable: true,
                },
            ],
        );
        assert_eq!(r.profile(), NetworkProfile::Restricted);
        // 只有全部信号可达才算恢复
        r.latch(
            &[],
            RESTRICTED_NET_SIGNALS
                .iter()
                .map(|u| NetProbe {
                    url: (*u).to_string(),
                    reachable: true,
                })
                .collect(),
        );
        assert_eq!(r.profile(), NetworkProfile::Open);
    }

    #[test]
    fn a_channel_switch_is_reported_once() {
        let r = routing_full();
        // 首次即有结论也算切换：第一条选路日志必须打得出来
        assert!(r.note_decision(Some(Transport::Https)));
        assert!(!r.note_decision(Some(Transport::Https)));
        assert!(r.note_decision(Some(Transport::Ssh)));
        assert!(!r.note_decision(Some(Transport::Ssh)));
    }

    #[test]
    fn a_fresh_measurement_is_not_repeated_immediately() {
        let r = routing_full();
        assert!(r.due_for_measurement());
        r.latch(&[ms(Transport::Https, true, 50)], vec![]);
        assert!(!r.due_for_measurement());
    }

    #[test]
    fn latching_keeps_the_order_when_no_channel_is_reachable() {
        let r = routing_full();
        r.latch(&[ms(Transport::Https, true, 50)], vec![]);
        let before = r.plan_for(Operation::GitFetch, Credential::SshKey).order;
        // 两条都不可达：实测给不出次序，保留旧结论而不是编一个空次序出来
        r.latch(
            &[ms(Transport::Https, false, 0), ms(Transport::Ssh, false, 0)],
            vec![],
        );
        assert_eq!(
            r.plan_for(Operation::GitFetch, Credential::SshKey).order,
            before
        );
    }
}
