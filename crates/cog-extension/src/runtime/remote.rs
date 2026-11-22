//! Remote sandbox executor client: routes `Command` payloads to the executor
//! pod over HTTP and streams output back as NDJSON [`CommandEvent`] lines.
//!
//! A lane stays on the replica whose volume holds its tree, and when it has to
//! land on a different one the work is carried ahead of the command (see
//! `carry_lane`). The agent is told which of the two happened on the stream's
//! first line, because that line is the only channel it reads.

use async_trait::async_trait;

/// 连远端运行时（沙盒执行器）的墙：10s。
///
/// 连不上通常是被网络策略挡住或对端没起，这两种都不靠等解决；10s 让调用方
/// 快速失败，而不是卡在 SYN 重传里。
// cap: bound — 连沙盒执行器的墙
// wall: 等=到沙盒执行器的连接建立 / 等不到=这一次调用记成「连不上执行器」，与「执行器慢」分得开 / 续跑=能，换一次连接重来
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// 执行器换一次版要多久没有后端：它是 StatefulSet，按序号替换，同一序号的两份 Pod
/// 不并存，所以**一副副本**的换版是先删后建——单节点上也没有第二副可调度的位置
/// （取舍写在 `deploy/k3s/sandbox-executor-statefulset.yaml`）。副本多于一副时，
/// 这段没有就绪后端的窗口只属于被换的那一副，其余副本照常应答。
///
/// 实测 2026-11-06：新 Pod 的 init 在 06:09:37 起来，主容器 06:12:03 才开始跑，
/// 两分半里连 `echo alive` 都拿不到连接。而在飞的那一轮 Agent 正是靠这个执行器
/// 跑 `cargo clippy`、读文件、产出 diff：它重试十几次都落在窗口里，最后交了白卷，
/// 一条停线单连交三轮零改动、预算耗尽记 `failed`。窗口比一条命令长，客户端就得
/// 比窗口更长——这不是"执行器坏了"，是"执行器正在被换掉"。
// cap: bound — 跨一次执行器换版的等待上界
const REPLACEMENT_WINDOW: std::time::Duration = std::time::Duration::from_secs(200);

/// 换版窗口里第一次重试之前等多久。
///
/// 比它更短的等法没有意义：端口拒绝是「还没有就绪后端」，而下一次尝试要跨过的是
/// 一次容器启动，不是一次调度；等得比这更短只是把同一条失败的连接多打几遍。
// cap: bound — 重试退避的起点，短于一次容器启动就没有可等的东西
const RETRY_FIRST_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);

/// 退避翻倍到这个数就不再涨。
///
/// 窗口有两百秒，而退避只负责「不要空转」：五秒已经很粗，再长就只是在窗口里少试
/// 几次，把本来已经就绪的后端多等一轮。
// cap: bound — 退避的粗粒度上界，再长就少试几次而已
const RETRY_MAX_PAUSE: std::time::Duration = std::time::Duration::from_secs(5);

/// 问「这副副本上有没有这条 lane 的树」最多等多久。
///
/// 这一问只发生在客户端刚起来、第一次碰到某条 lane 的时候：它手上没有钉子，
/// 而树不随进程消失。问不到就当「没有」，按轮转落点——等得比这更久，等于把重启
/// 后的第一条命令挂在一次读目录上；而问错一次的代价只是那条命令落在一棵新树上，
/// 那副副本的第一帧会如实说出树是从哪来的。
// cap: bound — 这一问是一次读目录，正常在毫秒级；2s 分的是「答得慢」与「答不上」，再长只是让重启后的第一条命令陪着等
// wall: 等=执行器把一次目录读回来 / 等不到=这一问读作「答不上」，**不是空目录**——空目录是一个真实读数，两者不能同形 / 续跑=能，下一次问再读一遍
const TREE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// The lane a request belongs to: the working tree it will run in.
///
/// The same derivation the executor routes by ([`cog_core::checkout_lane`]):
/// the task's tree when the request named a task, the employee's own otherwise.
/// Deriving it here a second time is what would let the client pin one lane
/// while the command runs in another -- the pin, the tree probe and the carry
/// would all be talking about a tree the command is not in. A request that
/// names neither runs outside any checkout, so it has no lane.
fn lane_of(req: &SandboxRequest) -> Option<String> {
    cog_core::checkout_lane(Some(req.task_id.as_str()), Some(req.agent_id.as_str()))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 端口被明确拒绝与包被丢掉是两件事。前者说「服务现在没有就绪后端」，也就是
/// 一次换版的窗口，自己会好；后者多是网络策略挡住，等多久都不会好，等它只是把
/// 失败推后。所以只认前者。
///
/// 也只在**连接没建起来**时重试：这条错误链上出现 `ConnectionRefused` 意味着请求
/// 根本没送到对端，重发是安全的；一旦连通过，`run_command` 与 `write_file` 都不
/// 幂等，重发就可能执行两次。
fn the_port_refused(e: &(dyn std::error::Error + 'static)) -> bool {
    let refused = |candidate: &(dyn std::error::Error + 'static)| {
        candidate
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused)
    };
    // 链头也要看：调用方直接递进来的就是一个 `io::Error` 时，它自己就是答案，
    // 往它的 `source()` 里找只会找到 `None`。
    if refused(e) {
        return true;
    }
    let mut source = e.source();
    while let Some(current) = source {
        if refused(current) {
            return true;
        }
        source = current.source();
    }
    false
}
use cog_core::{
    CommandEvent, CommandEventStream, SFResult, SandboxBackend, SandboxPayload, SandboxRequest,
    SandboxResult,
};
use futures::StreamExt;

/// How long a resolved replica list is reused before it is asked for again.
///
/// The list is what lets a replica added for capacity take new lanes, and what
/// lets a removed one stop being handed commands. Resolving on every command
/// would ask DNS per tool call for a set that changes on the scale of a rollout;
/// holding it forever would keep sending commands to a replica that is gone.
/// Five seconds is short against a scale-out and long against a command, and it
/// bounds how long an address that has already vanished may still be tried.
// cap: bound — 副本表的保鲜期：新副本最迟这么久之后会被用上
const ENDPOINT_REFRESH: std::time::Duration = std::time::Duration::from_secs(5);

/// Where the executor replicas' addresses come from.
#[derive(Clone)]
pub enum EndpointSource {
    /// Addresses written into the config. One entry is the whole deployment.
    Fixed(Vec<String>),
    /// A name that resolves to the replicas that exist at this moment. A
    /// deployment that adds a replica does not rewrite this client's config:
    /// the next refresh is enough.
    Discovered(std::sync::Arc<dyn Discovery>),
}

/// The replicas behind a name, right now.
///
/// The trait exists so the rules about pins and moves can be tested against a
/// list the test changes between calls -- which is exactly what a scale-out is
/// -- without a cluster or a DNS server.
#[async_trait::async_trait]
pub trait Discovery: Send + Sync {
    /// Every replica address (`http://host:port`), in any order. An empty list
    /// is "the name resolves to nothing", which is not the same as a failure
    /// to resolve and is reported as such by the caller.
    async fn addresses(&self) -> std::io::Result<Vec<String>>;
}

/// `dns://host:port`: the headless service in front of the executor replicas.
pub struct DnsDiscovery {
    host: String,
    port: u16,
    scheme: String,
}

impl DnsDiscovery {
    /// Parse the config form. A port is required: a headless service name
    /// without one resolves to addresses that have nothing to listen on.
    pub fn parse(spec: &str) -> SFResult<Self> {
        let rest = spec.strip_prefix("dns://").ok_or_else(|| {
            cog_core::SFError::Agent(format!(
                "sandbox executor address {spec:?} is not dns://host:port"
            ))
        })?;
        let (host, port) = rest.rsplit_once(':').ok_or_else(|| {
            cog_core::SFError::Agent(format!(
                "sandbox executor address {spec:?} names no port; a name alone has \
                 nothing to connect to"
            ))
        })?;
        let port: u16 = port.parse().map_err(|_| {
            cog_core::SFError::Agent(format!(
                "sandbox executor address {spec:?} does not end in a port number"
            ))
        })?;
        if host.is_empty() {
            return Err(cog_core::SFError::Agent(format!(
                "sandbox executor address {spec:?} names no host"
            )));
        }
        Ok(Self {
            host: host.to_string(),
            port,
            scheme: "http".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl Discovery for DnsDiscovery {
    async fn addresses(&self) -> std::io::Result<Vec<String>> {
        let resolved = tokio::net::lookup_host((self.host.as_str(), self.port)).await?;
        let mut out: Vec<String> = resolved
            .map(|addr| format!("{}://{}", self.scheme, addr))
            .collect();
        // A stable order makes the round-robin predictable; the pins themselves
        // do not depend on it (they name an address, not a position).
        out.sort();
        out.dedup();
        Ok(out)
    }
}

/// A resolved list and when it was asked for.
#[derive(Clone)]
struct Resolved {
    addrs: Vec<String>,
    at: std::time::Instant,
}

/// HTTP client for the cluster sandbox executor (`cogneva sandbox-executor`).
pub struct RemoteExecutor {
    /// Where the addresses come from: a list in the config, or a name that is
    /// resolved while the client runs. With a single address this is the whole
    /// deployment and every call goes to it; with more, a lane is pinned to one
    /// of them for the life of its pin (see [`crate::executor_lease`]).
    endpoints: EndpointSource,
    /// The last list a name resolved to, kept so that a resolution failure does
    /// not turn into "no backend" while the replicas are still answering.
    resolved: std::sync::Mutex<Option<Resolved>>,
    /// How long a resolved list is reused. [`ENDPOINT_REFRESH`] in production;
    /// tests set it small to exercise what a refresh picks up.
    endpoint_refresh: std::time::Duration,
    /// Which replica each lane sits on. Short critical sections holding a
    /// `std::sync::Mutex`: nothing is awaited under it.
    lanes: std::sync::Mutex<crate::executor_lease::LeaseBook>,
    /// The replica that last handed this lane a worktree -- told by the
    /// executor's own first frame, not inferred from a pin. A pin is placed
    /// before the command is even sent, so a command that failed before the
    /// executor got to cut a tree leaves a pin and no tree; only a `Workdir`
    /// frame proves a tree exists on that replica's volume.
    seen_tree: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    /// Where "now" comes from. The lease's rules are about clocks, and a rule
    /// proven by sleeping proves the sleep; tests replace this with one they
    /// can advance.
    clock: fn() -> u64,
    http: reqwest::Client,
    /// 端口被拒之后还等多久。见 [`REPLACEMENT_WINDOW`]。
    replacement_window: std::time::Duration,
}

/// Where one command goes, and whether the lane had to leave a replica to get
/// there.
struct Placement {
    endpoint: String,
    /// The replica the lane came from when its replica vanished and the
    /// command landed somewhere else. The tree under this command is then not
    /// the one the previous commands worked in, and the caller is told so.
    moved_from: Option<String>,
    /// This lane had a tree on this same replica before this request. If the
    /// executor answers with a tree that was just cut or handed out of its
    /// pool, the one the lane worked in was reclaimed in between -- the pin
    /// surviving is not the tree surviving -- and that is said out loud.
    tree_here_before: bool,
}

impl RemoteExecutor {
    pub fn new(base_url: impl Into<String>) -> SFResult<Self> {
        Self::with_replacement_window(base_url, REPLACEMENT_WINDOW)
    }

    /// One client, several executor replicas named by the config.
    ///
    /// The first address also answers the questions that are about the
    /// deployment rather than about one lane (`/boundary`, `/lane`); the
    /// others only take commands.
    pub fn with_endpoints(endpoints: Vec<String>) -> SFResult<Self> {
        Self::from_source(EndpointSource::Fixed(endpoints), REPLACEMENT_WINDOW)
    }

    /// A client whose replicas are whichever ones the name resolves to now.
    ///
    /// This is what makes adding a replica a deployment action rather than a
    /// configuration change: the address written into the config is the
    /// service's name, and the replicas behind it are read while the client
    /// runs. A replica that appears is handed new lanes by the next refresh; no
    /// file is rewritten, no process is restarted.
    pub fn discovering(spec: &str) -> SFResult<Self> {
        let discovery = std::sync::Arc::new(DnsDiscovery::parse(spec)?);
        Self::from_source(EndpointSource::Discovered(discovery), REPLACEMENT_WINDOW)
    }

    fn from_source(
        endpoints: EndpointSource,
        replacement_window: std::time::Duration,
    ) -> SFResult<Self> {
        if let EndpointSource::Fixed(list) = &endpoints {
            if list.is_empty() {
                return Err(cog_core::SFError::Agent(
                    "sandbox executor client was given no address".into(),
                ));
            }
        }
        Ok(Self {
            endpoints,
            resolved: std::sync::Mutex::default(),
            endpoint_refresh: ENDPOINT_REFRESH,
            lanes: std::sync::Mutex::default(),
            seen_tree: std::sync::Arc::default(),
            clock: now_secs,
            replacement_window,
            // No total timeout: a long-running command is stopped by the
            // executor when it stops moving, and the client must not cut the
            // stream early. The connect phase is bounded separately: if the
            // executor pod is
            // down or unreachable (e.g. blocked by network policy), fail fast
            // in 10s instead of leaving the caller hanging on SYN retries.
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(|e| {
                    cog_core::SFError::Agent(format!("sandbox executor client init failed: {}", e))
                })?,
        })
    }

    /// The replicas behind this client right now.
    ///
    /// A name that stops resolving keeps the list it last resolved to: the
    /// replicas are usually still answering, and a DNS hiccup is not a reason
    /// to fail every command. Answering "no replica" is reserved for a name
    /// that has never resolved -- falling back to the local executor instead
    /// would run the command in the application's own pod.
    async fn endpoints_now(&self) -> SFResult<Vec<String>> {
        let discovery = match &self.endpoints {
            EndpointSource::Fixed(list) => return Ok(list.clone()),
            EndpointSource::Discovered(discovery) => discovery.clone(),
        };
        let cached = {
            let cache = self
                .resolved
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.clone()
        };
        if let Some(resolved) = cached.as_ref() {
            if resolved.at.elapsed() < self.endpoint_refresh {
                return Ok(resolved.addrs.clone());
            }
        }
        match discovery.addresses().await {
            Ok(addrs) if !addrs.is_empty() => {
                let mut cache = self
                    .resolved
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *cache = Some(Resolved {
                    addrs: addrs.clone(),
                    at: std::time::Instant::now(),
                });
                Ok(addrs)
            }
            Ok(_) => self.last_resolved("the name resolved to no address"),
            Err(e) => self.last_resolved(&format!("resolving the replicas failed: {e}")),
        }
    }

    fn last_resolved(&self, why: &str) -> SFResult<Vec<String>> {
        let cached = {
            let cache = self
                .resolved
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.clone()
        };
        match cached {
            Some(resolved) if !resolved.addrs.is_empty() => {
                tracing::warn!(
                    why,
                    replicas = resolved.addrs.len(),
                    "keeping the executor replicas this client resolved earlier"
                );
                Ok(resolved.addrs)
            }
            _ => Err(cog_core::SFError::Agent(format!(
                "no sandbox executor replica is known: {why}"
            ))),
        }
    }

    /// The address the deployment-level questions go to.
    async fn primary(&self) -> SFResult<String> {
        self.endpoints_now()
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| cog_core::SFError::Agent("no sandbox executor replica is known".into()))
    }

    /// The address a command goes to, and the replica it is pinned to.
    ///
    /// A lane is pinned even when only one replica is known: the list having
    /// one entry now says nothing about what it held one command ago, and a
    /// single replica that was replaced under a new address is exactly a move
    /// worth reporting. Pinning a lane that has nowhere else to go is a
    /// comparison against a previous address, not a choice between replicas.
    fn endpoint_for(&self, lane: Option<&str>, endpoints: &[String]) -> Option<Placement> {
        let first = endpoints.first()?.clone();
        let Some(lane) = lane else {
            return Some(Placement {
                endpoint: first,
                moved_from: None,
                tree_here_before: false,
            });
        };
        let pinned = {
            let mut book = self
                .lanes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            book.pin(
                lane,
                endpoints,
                (self.clock)(),
                crate::executor_lease::LANE_LEASE_SECS,
            )
        }?;
        // 每一次真正的放置都说出理由：「树在这副」「挑的是这本簿上最少的」
        // 「这副是上次之后才出现的」「根本没有可挑的」——读者从计数上分得清
        // 是用钉子留住的、被导向的，还是靠加副本接住的。
        crate::pressure::record_placement(&pinned.placement);
        if let crate::executor_lease::Grant::Resumed { lapsed_secs } = &pinned.grant {
            tracing::info!(
                lane,
                replica = %pinned.replica,
                lapsed_secs,
                "this lane came back after its pin ran out and stays on the replica that has \
                 its tree; whether the tree itself survived is the executor's answer"
            );
        }
        // A pin that lands on the replica it already pointed at is not a move:
        // that is where the lane's tree is, and staying is what keeps it. Only
        // a pin that lands on a *different* replica means the tree under this
        // command is not the one the previous commands worked in.
        //
        // A tree existed only if a command on this lane was actually handed
        // one, which the executor said on that command's first frame; the pin
        // alone proves nothing, because it is placed before the request goes
        // out and a request that failed before the executor cut anything
        // leaves a pin and no tree.
        let tree_here_before = self
            .seen_tree
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(lane)
            .is_some_and(|replica| replica == &pinned.replica);
        let moved_from = match &pinned.grant {
            crate::executor_lease::Grant::Moved { from, lapsed_secs }
                if from != &pinned.replica =>
            {
                tracing::warn!(
                    lane,
                    from_replica = %from,
                    to_replica = %pinned.replica,
                    lapsed_secs,
                    "this lane is moving to another executor replica: what the previous one \
                     wrote after its pin lapsed may not be in the tree this command runs on"
                );
                Some(from.clone())
            }
            _ => None,
        };
        Some(Placement {
            endpoint: pinned.replica,
            moved_from,
            tree_here_before,
        })
    }

    /// Say that the replica a lane sits on stopped answering. Not a release:
    /// see [`crate::executor_lease::LeaseBook::abandon`].
    fn abandon_lane(&self, lane: Option<&str>, replica: &str) {
        let Some(lane) = lane else {
            return;
        };
        let mut book = self
            .lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        book.abandon(lane, replica);
    }

    /// 把一条 lane 的工作从它原先那副副本搬到这一副上。
    ///
    /// 两副副本各有各的卷：钉子换了副本只说命令要去哪，不说上一副上的树跟着走。
    /// 搬法是一份包——向原副本要它那棵树的**内容**，交给新副本放进这条 lane 的树
    /// 里。搬成了，未提交的改动就在新树里，以未提交的样子；只在那台机器上的提交
    /// 不跟过来，历史留在原卷上，`git status` 会把搬来的东西如实报成改动。
    ///
    /// 返回是否搬成。没搬成不是这次命令该失败的理由：原副本连不上、已经没树、
    /// 树还在用，命令都照发，只是流里第一句要如实说没搬成。
    /// A driver that has just started holds no pin for anything, but the tree
    /// it was driving does not go away with the process: it lives on one
    /// replica's volume, and that replica can still answer for it. Ask every
    /// replica once for a lane this book has never placed and adopt the one
    /// that says yes, so a restart lands on the tree instead of cutting a new
    /// one and quietly leaving the work behind.
    ///
    /// Nothing is asked for a lane the book already has: a lapsed or abandoned
    /// pin still names the replica whose volume the tree is on, and that is a
    /// better answer than a directory read.
    ///
    /// Two replicas claiming the same lane is a state this design does not
    /// produce (a lane is pinned by one caller); if it happens anyway, the
    /// first by address is taken and the disagreement is logged rather than
    /// silently resolved, because either choice leaves a tree behind.
    async fn adopt_held_tree(&self, lane: &str, endpoints: &[String]) {
        let already = {
            let book = self
                .lanes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            book.knows(lane)
        };
        if already {
            return;
        }
        let answers = futures::future::join_all(endpoints.iter().map(|endpoint| async move {
            (endpoint.clone(), self.tree_is_here(endpoint, lane).await)
        }))
        .await;
        let mut holders: Vec<String> = answers
            .into_iter()
            .filter(|(_, answer)| *answer == Some(true))
            .map(|(endpoint, _)| endpoint)
            .collect();
        holders.sort();
        let Some(holder) = holders.first() else {
            return;
        };
        if holders.len() > 1 {
            tracing::warn!(
                lane,
                holders = ?holders,
                "more than one replica says it holds this lane's tree; the first by address is \
                 taken and the others keep a tree nobody will read"
            );
        }
        let mut book = self
            .lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        book.adopt(
            lane,
            holder,
            (self.clock)(),
            crate::executor_lease::LANE_LEASE_SECS,
        );
        tracing::info!(
            lane,
            replica = %holder,
            "found the lane's tree on this replica and placed the lane there"
        );
    }

    /// 这副副本有没有这条 lane 的树：`Some(true)` 有，`Some(false)` 没有，
    /// `None` 问不到——问不到与「没有」是两件事，前者不能当成后者去改写落点。
    async fn tree_is_here(&self, endpoint: &str, lane: &str) -> Option<bool> {
        let url = format!("{}/worktree/has", endpoint.trim_end_matches('/'));
        let response = self
            .http
            .get(&url)
            .query(&[("lane", lane)])
            .timeout(TREE_PROBE_TIMEOUT)
            .send()
            .await
            .ok()?;
        match response.status().as_u16() {
            200 => Some(true),
            404 => Some(false),
            _ => None,
        }
    }

    async fn carry_lane(&self, lane: &str, from: &str, to: &str) -> bool {
        let export = self
            .http
            .post(format!("{}/worktree/export", from.trim_end_matches('/')))
            .json(&serde_json::json!({ "lane": lane }))
            .send()
            .await;
        let bundle = match export {
            Ok(response) if response.status().is_success() => match response.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(
                        lane,
                        from,
                        error = %e,
                        "the move's bundle stopped arriving; the command runs without the work \
                         being carried"
                    );
                    return false;
                }
            },
            Ok(response) => {
                // 404：那副上没有这条 lane 的树——没有东西可搬，不是故障。
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                tracing::info!(
                    lane,
                    from,
                    %status,
                    body = %body,
                    "the previous replica had nothing to hand over for this lane"
                );
                return false;
            }
            Err(e) => {
                tracing::warn!(
                    lane,
                    from,
                    error = %e,
                    "the previous replica did not answer the export; the command runs without \
                     the work being carried"
                );
                return false;
            }
        };
        let arrived = self
            .http
            .post(format!("{}/worktree/import", to.trim_end_matches('/')))
            .query(&[("lane", lane)])
            .body(bundle)
            .send()
            .await;
        match arrived {
            Ok(response) if response.status().is_success() => {
                let answer = response.text().await.unwrap_or_default();
                tracing::info!(
                    lane,
                    from,
                    to,
                    answer = %answer,
                    "the lane's tree was carried to the replica that will run this command"
                );
                true
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                tracing::warn!(
                    lane,
                    from,
                    to,
                    %status,
                    body = %body,
                    "the new replica refused the carried tree; the command runs in whatever tree \
                     it has"
                );
                false
            }
            Err(e) => {
                tracing::warn!(
                    lane,
                    from,
                    to,
                    error = %e,
                    "the carried tree never reached the new replica"
                );
                false
            }
        }
    }

    /// 同一个客户端，换一段等待上界。生产的窗口是
    /// [`REPLACEMENT_WINDOW`]；这里开一个口子是为了让"窗口内会重试""窗口过了
    /// 要说出等了多久"这两条判据能在测试里按秒验证，而不是按两分半验证。
    pub fn with_replacement_window(
        base_url: impl Into<String>,
        replacement_window: std::time::Duration,
    ) -> SFResult<Self> {
        Self::from_source(
            EndpointSource::Fixed(vec![base_url.into()]),
            replacement_window,
        )
    }
}

#[async_trait]
impl SandboxBackend for RemoteExecutor {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        Ok(super::local::collect_stream(self.execute_stream(req).await?).await)
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(cog_core::SFError::Agent(
            "RemoteExecutor does not compile WASM modules".into(),
        ))
    }

    /// 边界由对端自报。这里不替它说话：一个在这里拼出来的「安全」描述，
    /// 改执行器的部署时不会跟着变，而读的人以为它是量出来的。
    async fn boundary(&self) -> SFResult<cog_core::SandboxBoundary> {
        let primary = self.primary().await?;
        let url = format!("{}/boundary", primary.trim_end_matches('/'));
        let response = self.http.get(&url).send().await.map_err(|e| {
            cog_core::SFError::Agent(format!("sandbox executor boundary unreachable: {e}"))
        })?;
        if !response.status().is_success() {
            return Err(cog_core::SFError::Agent(format!(
                "sandbox executor answered {} for its boundary",
                response.status()
            )));
        }
        response
            .json::<cog_core::SandboxBoundary>()
            .await
            .map_err(|e| {
                cog_core::SFError::Agent(format!("sandbox executor boundary unreadable: {e}"))
            })
    }

    /// 编译队由对端自报，本侧不排这条队：问它谁占着、谁排第几。
    async fn lane(&self) -> SFResult<cog_core::BuildLaneReading> {
        let primary = self.primary().await?;
        let url = format!("{}/lane", primary.trim_end_matches('/'));
        let response = self.http.get(&url).send().await.map_err(|e| {
            cog_core::SFError::Agent(format!("sandbox executor lane unreachable: {e}"))
        })?;
        if !response.status().is_success() {
            return Err(cog_core::SFError::Agent(format!(
                "sandbox executor answered {} for its build lane",
                response.status()
            )));
        }
        response
            .json::<cog_core::BuildLaneReading>()
            .await
            .map_err(|e| {
                cog_core::SFError::Agent(format!("sandbox executor build lane unreadable: {e}"))
            })
    }

    async fn execute_stream(&self, req: &SandboxRequest) -> SFResult<CommandEventStream> {
        if matches!(req.payload, SandboxPayload::Wasm { .. }) {
            return Err(cog_core::SFError::Agent(
                "RemoteExecutor only executes environment payloads".into(),
            ));
        }
        let lane = lane_of(req);
        let endpoints = self.endpoints_now().await?;
        // 刚起来的客户端手上没有钉子：先问哪副副本上还有这条 lane 的树，
        // 落到那副上，而不是轮转出一副、在那里另切一棵。
        if let Some(lane) = lane.as_deref() {
            self.adopt_held_tree(lane, &endpoints).await;
        }
        let placement = self
            .endpoint_for(lane.as_deref(), &endpoints)
            .ok_or_else(|| {
                cog_core::SFError::Agent(
                    "no sandbox executor replica is known; the command was not sent".into(),
                )
            })?;
        let endpoint = placement.endpoint.clone();
        // 换了副本、而原来那副还在：先把那副上的树要过来，再让命令在带着它的
        // 树上跑。这是「工作跟着任务走」的唯一一步——不去要，命令只是在另一副
        // 上从一棵没有它的树开始。
        let carried = match (&placement.moved_from, lane.as_deref()) {
            (Some(from), Some(lane)) if from != &endpoint => {
                self.carry_lane(lane, from, &endpoint).await
            }
            _ => false,
        };
        let url = format!("{}/execute", endpoint.trim_end_matches('/'));
        let body = serde_json::json!({
            "payload": req.payload,
            "task_id": req.task_id,
            "agent_id": req.agent_id,
        });
        // 换版窗口里的调用要等，窗口外的失败要当场报。「等」与「报」的分界是
        // 端口有没有明确拒绝：拒绝＝服务暂时没有后端，会自己好；其余（连不通、
        // 被丢掉）等下去也不会好。见 [`the_port_refused`]。
        let started = std::time::Instant::now();
        let mut pause = RETRY_FIRST_PAUSE;
        let response = loop {
            match self.http.post(&url).json(&body).send().await {
                Ok(response) => break response,
                Err(e) if the_port_refused(&e) && started.elapsed() < self.replacement_window => {
                    tracing::warn!(
                        waited_secs = started.elapsed().as_secs(),
                        retry_in_ms = pause.as_millis() as u64,
                        "the sandbox executor has no ready backend; a rollout replaces this \
                         pod, so the call waits inside the replacement window"
                    );
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(RETRY_MAX_PAUSE);
                }
                Err(e) => {
                    let note = if the_port_refused(&e) {
                        format!(
                            " (the port refused for {}s, past the {}-second replacement window: \
                             the executor's rollout is either still going or stuck, so waiting \
                             longer from here would just postpone the failure)",
                            started.elapsed().as_secs(),
                            self.replacement_window.as_secs()
                        )
                    } else {
                        String::new()
                    };
                    // The replica this lane sits on is not answering and the
                    // replacement window is over: it is gone, not restarting.
                    // The hold is marked abandoned rather than released: the
                    // next command is free to go to a replica that answers,
                    // and its pin reports `Grant::Moved`, which puts the move
                    // line on that command. A released pin would come back as
                    // `Fresh`, and the lane would land in a tree nobody named.
                    self.abandon_lane(lane.as_deref(), &endpoint);
                    return Err(cog_core::SFError::Agent(format!(
                        "sandbox executor unreachable: {e}{note}"
                    )));
                }
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(cog_core::SFError::Agent(format!(
                "sandbox executor returned {}: {}",
                status, body
            )));
        }

        // Decode the NDJSON stream: buffer bytes, split on newlines, parse
        // each complete line as a CommandEvent.
        let state = (response.bytes_stream(), String::new());
        let stream = futures::stream::unfold(state, |(mut bytes, mut buf)| async move {
            loop {
                if let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].to_string();
                    buf.drain(..=pos);
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<CommandEvent>(&line) {
                        Ok(event) => return Some((event, (bytes, buf))),
                        Err(e) => {
                            tracing::warn!("sandbox executor sent malformed event: {}", e);
                            continue;
                        }
                    }
                }
                match bytes.next().await {
                    Some(Ok(chunk)) => buf.push_str(&String::from_utf8_lossy(&chunk)),
                    Some(Err(e)) => {
                        tracing::warn!("sandbox executor stream error: {}", e);
                        return None;
                    }
                    None => return None,
                }
            }
        });
        // A command that changed replica may be running in a tree the previous
        // commands did not work in: the replicas do not share a volume, so
        // whatever the old one had that was not pushed to a remote --
        // uncommitted edits, commits that live only on its disk -- is not here.
        // The stream is the one channel the agent reads, so the fact travels as
        // its first line instead of only in this process's log.
        //
        // How much of that can be asserted depends on where the tree came
        // from, which the executor says on the same stream just after. The
        // notice is therefore composed when the first frame arrives rather
        // than in front of the stream; `unfold` keeps the call itself from
        // waiting, and the first frame is what the executor sends before the
        // command has even started.
        // Two ways a command can be running in a tree without the work the
        // lane had: it moved to another replica, or it came back to the same
        // one and the tree had been reclaimed in the meantime. The first is
        // known here; the second is only known once the executor's first frame
        // says where the tree came from, and a tree cut for this call or handed
        // out of the pool is one nothing has run in.
        //
        // That first frame is also the only proof that a tree exists for this
        // lane on this replica, which is what the next command will be judged
        // against -- so the wrapper is used for every command that belongs to
        // a lane, not only for the ones with something to say.
        let Some(lane) = lane else {
            return Ok(Box::pin(stream));
        };
        let from = placement.moved_from;
        let tree_here_before = placement.tree_here_before;
        let seen_tree = self.seen_tree.clone();
        enum Step {
            Head,
            First(Option<CommandEvent>),
            Tail,
        }
        // The decoded stream is not `Unpin`; boxed it is, which is what lets
        // this wrapper pull the first frame and hold the rest.
        let state = (Box::pin(stream), Step::Head, from, endpoint, carried);
        let stream =
            futures::stream::unfold(state, move |(mut inner, step, from, endpoint, carried)| {
                let lane = lane.clone();
                let seen_tree = seen_tree.clone();
                async move {
                    match step {
                        Step::Head => {
                            let first = inner.next().await;
                            let origin = match &first {
                                Some(CommandEvent::Workdir { origin, .. }) => *origin,
                                _ => cog_core::WorktreeOrigin::Unknown,
                            };
                            if matches!(first, Some(CommandEvent::Workdir { .. })) {
                                seen_tree
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .insert(lane.clone(), endpoint.clone());
                            }
                            let notice = match &from {
                                Some(from) => Some(moved_line(from, &endpoint, origin, carried)),
                                // 回来的是同一副：树在不在只有执行器的第一帧说得清。
                                None if tree_here_before => match origin {
                                    cog_core::WorktreeOrigin::Created
                                    | cog_core::WorktreeOrigin::Warm => Some(reclaimed_line()),
                                    cog_core::WorktreeOrigin::Existing
                                    | cog_core::WorktreeOrigin::Unknown => None,
                                },
                                None => None,
                            };
                            match notice {
                                Some(data) => Some((
                                    CommandEvent::Stderr { data },
                                    (inner, Step::First(first), from, endpoint, carried),
                                )),
                                // 树还在：没有什么要说的，第一帧就是全部。
                                None => first.map(|event| {
                                    (event, (inner, Step::Tail, from, endpoint, carried))
                                }),
                            }
                        }
                        Step::First(Some(event)) => {
                            Some((event, (inner, Step::Tail, from, endpoint, carried)))
                        }
                        Step::First(None) => None,
                        Step::Tail => inner
                            .next()
                            .await
                            .map(|event| (event, (inner, Step::Tail, from, endpoint, carried))),
                    }
                }
            });
        Ok(Box::pin(stream))
    }
}

/// What the agent is told when its command landed on a different replica.
///
/// A tree that already existed on the replica it landed on may be the same
/// volume the pod was rebuilt on, so the loss is stated as a condition. A tree
/// cut for this call, or handed out of that replica's pool, is one nothing has
/// run in: there the loss is a fact.
///
/// When the tree was carried (`carried`), the loss is neither: the content came
/// over as uncommitted changes, and what did not come over is the history that
/// lived only on the old volume. The agent then looks at `git status`, not at a
/// tree it has to rebuild.
fn moved_line(from: &str, to: &str, origin: cog_core::WorktreeOrigin, carried: bool) -> String {
    use cog_core::WorktreeOrigin;
    let opening = format!("这条命令换了一副执行器（从 {from} 到 {to}）。");
    if carried {
        return format!(
            "{opening}上一副上那棵树已经跟着任务搬过来了：树里的内容在这棵树上，未提交的\
             改动在 `git status` 里如实可见；只在那台机器上的提交没有跟过来——搬过来的是\
             内容，历史还在原来那副的卷上。\n"
        );
    }
    match origin {
        WorktreeOrigin::Created => format!(
            "{opening}这副上刚为这个任务切了一棵新树：上一副上未推送到远端的工作\
             （未提交的改动、只在那台机器上的提交）不在这棵树上。\n"
        ),
        WorktreeOrigin::Warm => format!(
            "{opening}这副上交给这次调用的是一棵提前切好、没跑过任务的树：上一副上\
             未推送到远端的工作（未提交的改动、只在那台机器上的提交）不在这棵树上。\n"
        ),
        WorktreeOrigin::Existing | WorktreeOrigin::Unknown => format!(
            "{opening}每副副本有自己的卷：如果这不是同一副的重建，上一副上未推送到\
             远端的工作（未提交的改动、只在那台机器上的提交）不在这棵树上。\n"
        ),
    }
}

/// What the agent is told when the lane came back to the same replica and the
/// executor there handed it a tree nobody had worked in.
///
/// Staying on the replica is what keeps a tree; it cannot promise the replica
/// kept it. The executor reclaims idle trees on its own clock, so the pin
/// surviving and the tree surviving are two different facts, and only the
/// second one is about the work.
fn reclaimed_line() -> String {
    "这副副本上原先那棵树已经被回收：这次交给你的是一棵没人跑过的树，先前在那棵树上\
     未推送到远端的工作（未提交的改动、只在那台机器上的提交）不在这棵树上。\n"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 这条请求问的是哪条路。
    ///
    /// 换副本的客户端会先向原副本要那棵树（`/worktree/export`），再发起命令。
    /// 下面这几个假副本没有 worktree 路由，所以照真执行器在没树时的样子答 404
    /// ——「没搬成」这条路在测试里行走的是与部署相同的那一段，而不是被蒙混过去。
    fn asked_path(head: &[u8]) -> String {
        String::from_utf8_lossy(head)
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("")
            .to_string()
    }

    /// An executor that answers `requests` commands, counting how many it was
    /// asked. Two of these stand in for two replicas behind one Service.
    async fn serve_counting(
        requests: usize,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut served = 0;
            while served < requests {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let read = socket.read(&mut buf).await.unwrap_or(0);
                if asked_path(&buf[..read]).starts_with("/worktree/") {
                    // 问树的问题不占命令额度：客户端刚起来时会先问一遍，
                    // 假副本要像真副本那样答，而且这一问不是一条命令。
                    let _ = socket
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!")
                        .await;
                    continue;
                }
                served += 1;
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = format!(
                    "{}\n{}\n",
                    serde_json::to_string(&CommandEvent::Stdout { data: "ok".into() }).unwrap(),
                    serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap()
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (format!("http://{}", addr), hits)
    }

    /// 这副副本对有树的 lane 怎么答。三种答案在客户端眼里是三件事：有、没有、
    /// 问不出来；「问不出来」不能被读成「没有」。
    #[derive(Clone, Copy)]
    enum TreeAnswer {
        Holds,
        Empty,
        Broken,
    }

    /// 一副会答「树在不在这副上」的副本，以及两条计数：跑了几条命令、被问了几次树。
    ///
    /// 问树的那几次不进命令额度：真副本上它是一次读目录，不是一条命令。
    async fn serve_trees(
        answer: TreeAnswer,
        commands: usize,
    ) -> (
        String,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        serve_trees_on(listener, answer, commands)
    }

    /// 同 [`serve_trees`]，但监听器由调用方先建好：几条测试要先知道地址排序、
    /// 再决定哪一副答哪种话，而「两个副本都说是自己」这类并列是按地址断的。
    fn serve_trees_on(
        listener: tokio::net::TcpListener,
        answer: TreeAnswer,
        commands: usize,
    ) -> (
        String,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let addr = listener.local_addr().unwrap();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let looks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        let asked = looks.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut served = 0;
            while served < commands {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let read = socket.read(&mut buf).await.unwrap_or(0);
                if asked_path(&buf[..read]).starts_with("/worktree/has") {
                    asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = match answer {
                        TreeAnswer::Holds => socket
                            .write_all(
                                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                                  content-length: 30\r\n\r\n{\"lane\":\"agent-a\",\"tree\":true}",
                            )
                            .await,
                        TreeAnswer::Empty => socket
                            .write_all(
                                b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!",
                            )
                            .await,
                        TreeAnswer::Broken => socket
                            .write_all(
                                b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 3\r\n\r\nbad",
                            )
                            .await,
                    };
                    continue;
                }
                if asked_path(&buf[..read]).starts_with("/worktree/") {
                    let _ = socket
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!")
                        .await;
                    continue;
                }
                served += 1;
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = format!(
                    "{}\n{}\n",
                    serde_json::to_string(&CommandEvent::Stdout { data: "ok".into() }).unwrap(),
                    serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap()
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (format!("http://{}", addr), hits, looks)
    }

    /// A replica that, like the executor, says which tree the command got
    /// before it answers -- or, with `None`, one whose version does not know
    /// to say it.
    async fn serve_announcing(requests: usize, origin: Option<cog_core::WorktreeOrigin>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut served = 0;
            while served < requests {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let read = socket.read(&mut buf).await.unwrap_or(0);
                if asked_path(&buf[..read]).starts_with("/worktree/") {
                    // 问树的问题不占命令额度，答法与真副本一致：没有就是 404。
                    let _ = socket
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!")
                        .await;
                    continue;
                }
                served += 1;
                let mut body = String::new();
                if let Some(origin) = origin {
                    body.push_str(
                        &serde_json::to_string(&CommandEvent::Workdir {
                            path: "/srv/workspaces/employee-a".into(),
                            origin,
                        })
                        .unwrap(),
                    );
                    body.push('\n');
                }
                body.push_str(
                    &serde_json::to_string(&CommandEvent::Stdout { data: "ok".into() }).unwrap(),
                );
                body.push('\n');
                body.push_str(&serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap());
                body.push('\n');
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{}", addr)
    }

    /// 一副副本题下发生过什么：交出去的包，和收进来的包。
    #[derive(Default)]
    struct CarryLog {
        handed_out: Vec<u8>,
        received: Vec<Vec<u8>>,
    }

    /// 一份请求的请求头与请求体。
    ///
    /// 搬运的两个端点上请求体就是全部内容（一棵树装在一个包里），所以假副本
    /// 必须把体读全再答，否则「搬过去的是不是原样那一份」在测试里无从谈起。
    async fn read_request(socket: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        let mut raw = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = socket.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..n]);
            let Some(head_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
            let want: usize = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if raw.len() >= head_end + 4 + want {
                return (head, raw[head_end + 4..head_end + 4 + want].to_vec());
            }
        }
        (String::new(), Vec::new())
    }

    /// A replica that really does hand over a tree and take one in: `/worktree/export`
    /// answers with a fixed bundle, `/worktree/import` records the body it was
    /// given. What the client carries is therefore checkable byte for byte.
    async fn serve_carrying(
        requests: usize,
    ) -> (String, std::sync::Arc<std::sync::Mutex<CarryLog>>) {
        const BUNDLE: &[u8] = b"the bytes of a carried tree";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log = std::sync::Arc::new(std::sync::Mutex::new(CarryLog::default()));
        let seen = log.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut served = 0;
            while served < requests {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let (head, body) = read_request(&mut socket).await;
                let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
                if path.starts_with("/worktree/has") {
                    // 找树那一问不占额度：它问的不是「搬」，答法与真副本一致。
                    let _ = socket
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!")
                        .await;
                    continue;
                }
                served += 1;
                let response: Vec<u8> = if path.starts_with("/worktree/export") {
                    seen.lock().unwrap().handed_out = BUNDLE.to_vec();
                    let mut out = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/x-git-bundle\r\n\
                         content-length: {}\r\n\r\n",
                        BUNDLE.len()
                    )
                    .into_bytes();
                    out.extend_from_slice(BUNDLE);
                    out
                } else if path.starts_with("/worktree/import") {
                    seen.lock().unwrap().received.push(body);
                    let payload = br#"{"lane":"agent-a","files":3}"#;
                    let mut out = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\n\r\n",
                        payload.len()
                    )
                    .into_bytes();
                    out.extend_from_slice(payload);
                    out
                } else {
                    let ndjson = format!(
                        "{}\n{}\n{}\n",
                        serde_json::to_string(&CommandEvent::Workdir {
                            path: "/srv/workspaces/agent-a".into(),
                            origin: cog_core::WorktreeOrigin::Existing,
                        })
                        .unwrap(),
                        serde_json::to_string(&CommandEvent::Stdout { data: "ok".into() }).unwrap(),
                        serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap()
                    );
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\n\
                         transfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                        ndjson.len(),
                        ndjson
                    )
                    .into_bytes()
                };
                let _ = socket.write_all(&response).await;
            }
        });
        (format!("http://{}", addr), log)
    }

    /// 第一次请求在执行器切树之前就失败（比如编译位满了），第二次才交出一棵
    /// 刚切的树：一条钉子已经存在，但从来没有任何一棵树存在过。
    async fn serve_failing_once_then_announcing(origin: cog_core::WorktreeOrigin) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            // 客户端刚起来时会先问树在哪副；这一问不是那次失败的尝试。
            loop {
                let (mut probe, _) = listener.accept().await.unwrap();
                let read = probe.read(&mut buf).await.unwrap_or(0);
                if asked_path(&buf[..read]).starts_with("/worktree/") {
                    let _ = probe
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!")
                        .await;
                    continue;
                }
                let _ = probe
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")
                    .await;
                break;
            }

            let (mut second, _) = listener.accept().await.unwrap();
            let _ = second.read(&mut buf).await;
            let body = format!(
                "{}\n{}\n",
                serde_json::to_string(&CommandEvent::Workdir {
                    path: "/srv/workspaces/employee-a".into(),
                    origin,
                })
                .unwrap(),
                serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap(),
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            let _ = second.write_all(response.as_bytes()).await;
        });
        format!("http://{}", addr)
    }

    fn for_agent(agent: &str) -> SandboxRequest {
        for_task("t", agent)
    }

    /// One command on `task`, run by `agent`. The lane is the task's — two
    /// agents on one task share one checkout, and two tasks are two.
    fn for_task(task: &str, agent: &str) -> SandboxRequest {
        SandboxRequest {
            task_id: task.into(),
            agent_id: agent.into(),
            payload: SandboxPayload::Command {
                command: "echo hi".into(),
            },
            input: serde_json::json!({}),
            limits: Default::default(),
        }
    }

    /// Two commands on one task answer from the same replica — they run in the
    /// same checkout, and the checkout lives on one replica — even when two
    /// different agents carry them, because an agent who takes over a task has
    /// to find the work where the agent before it left it. A second task is
    /// its own checkout, and its own replica is fine.
    #[tokio::test]
    async fn one_tasks_commands_stay_on_one_executor_replica() {
        use std::sync::atomic::Ordering;
        let (first, first_hits) = serve_counting(3).await;
        let (second, second_hits) = serve_counting(3).await;
        let executor = RemoteExecutor::with_endpoints(vec![first, second]).unwrap();

        executor.execute(&for_task("t", "agent-a")).await.unwrap();
        executor.execute(&for_task("t", "agent-a")).await.unwrap();
        executor.execute(&for_task("u", "agent-b")).await.unwrap();

        let hits = [
            first_hits.load(Ordering::SeqCst),
            second_hits.load(Ordering::SeqCst),
        ];
        assert_eq!(hits, [2, 1], "同一任务的命令分到了两副副本上：{hits:?}");
    }

    /// 客户端重启后手上没有钉子，但树还在某副卷上：它问一遍、落在那副，
    /// 而不是按轮转另切一棵、把原先那棵树上的工作留在原地。
    ///
    /// 轮转会把这条 lane 放到第一副（那副没有树），所以这一条判的正是「问过之后
    /// 换了落点」：没问，命令就落在空树上。
    #[tokio::test]
    async fn a_client_that_restarts_lands_on_the_replica_that_still_holds_the_lane() {
        use std::sync::atomic::Ordering;
        let (empty, empty_hits, _) = serve_trees(TreeAnswer::Empty, 1).await;
        let (holder, holder_hits, _) = serve_trees(TreeAnswer::Holds, 2).await;
        let executor = RemoteExecutor::with_endpoints(vec![empty, holder]).unwrap();

        executor.execute(&for_agent("agent-a")).await.unwrap();

        assert_eq!(
            holder_hits.load(Ordering::SeqCst),
            1,
            "命令没有落在还端着那棵树的那副上"
        );
        assert_eq!(
            empty_hits.load(Ordering::SeqCst),
            0,
            "命令落到了没有树的那副上，原先树上的工作就留在原地了"
        );
    }

    /// 问树只发生在客户端第一次碰到这条 lane 时：钉子在手上之后，再问一遍
    /// 只会给每条命令加一次读目录。
    #[tokio::test]
    async fn a_client_with_a_pin_does_not_look_for_the_tree_again() {
        use std::sync::atomic::Ordering;
        let (replica, hits, looks) = serve_trees(TreeAnswer::Empty, 2).await;
        let executor = RemoteExecutor::with_endpoints(vec![replica]).unwrap();

        executor.execute(&for_agent("agent-a")).await.unwrap();
        executor.execute(&for_agent("agent-a")).await.unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(
            looks.load(Ordering::SeqCst),
            1,
            "同一条 lane 被问了不止一次"
        );
    }

    /// 问不出来不是「有」：副本身回 500 时，它不能算一处落点——把非 404 都当成
    /// 「树在我这」的实现会挑中它，把命令送到一棵空树上。
    ///
    /// 两副按地址排，答 500 的那副排在前面，所以「取第一个说有树的」若把 500 读成
    /// 「有」，落点就是它。
    #[tokio::test]
    async fn a_replica_that_cannot_answer_is_not_taken_for_a_holder() {
        use std::sync::atomic::Ordering;
        let one = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let two = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (broken, holder) = if one.local_addr().unwrap() < two.local_addr().unwrap() {
            (one, two)
        } else {
            (two, one)
        };
        let (broken, broken_hits, _) = serve_trees_on(broken, TreeAnswer::Broken, 1);
        let (holder, holder_hits, _) = serve_trees_on(holder, TreeAnswer::Holds, 2);
        let executor = RemoteExecutor::with_endpoints(vec![broken, holder]).unwrap();

        let result = executor.execute(&for_agent("agent-a")).await.unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(
            broken_hits.load(Ordering::SeqCst),
            0,
            "答不上话的副本被当成端着树的那个"
        );
        assert_eq!(holder_hits.load(Ordering::SeqCst), 1);
    }

    /// 问树问不出结果也不拦命令：唯一的副本答 500，命令照轮转发到它，
    /// 客户端手上什么也没记下——下一次还会再问。
    #[tokio::test]
    async fn a_question_that_gets_no_answer_does_not_hold_the_command_back() {
        use std::sync::atomic::Ordering;
        let (replica, hits, looks) = serve_trees(TreeAnswer::Broken, 1).await;
        let executor = RemoteExecutor::with_endpoints(vec![replica]).unwrap();

        let result = executor.execute(&for_agent("agent-a")).await.unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(looks.load(Ordering::SeqCst), 1);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "命令没有发出去");
    }

    /// A replica that is gone does not keep its lanes, and the lane is not
    /// forgotten either: the next command lands on a replica that answers and
    /// says which one it left. A pin given up at the error would move the lane
    /// silently, in a tree nobody announced.
    #[tokio::test]
    async fn a_dead_replicas_lane_moves_on_the_next_command() {
        use std::sync::atomic::Ordering;
        let dead = a_port_nothing_listens_on().await;
        let (live, live_hits) = serve_counting(2).await;
        // A short window: the production one is a rollout's length, and no
        // test should wait it out to prove what a release does.
        let executor = RemoteExecutor::from_source(
            EndpointSource::Fixed(vec![format!("http://{}", dead), live]),
            std::time::Duration::from_millis(50),
        )
        .unwrap();

        let first = executor.execute(&for_agent("agent-a")).await;
        assert!(first.is_err(), "第一副副本没人监听，这一条必须失败");
        let next = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert_eq!(live_hits.load(Ordering::SeqCst), 1);
        assert!(
            next.stderr.contains("换了一副执行器")
                && next.stderr.contains(&format!("http://{dead}")),
            "换了副本的那条命令没说它从哪副来：{:?}",
            next.stderr
        );
    }

    /// 搬过家的那条命令要把「上一副卷上的东西没跟过来」交给智能体读。
    ///
    /// 两副副本各有自己的卷：换到另一副之后，那条 lane 的树只可能是那副卷上
    /// 本来就有的那棵，上一副上未推送的工作不在里面。这句话以前只进进程日志，
    /// 而读日志的人不是正在用这棵树干活的智能体。
    #[tokio::test]
    async fn a_command_that_changed_replica_says_what_did_not_come_along() {
        let (first, _) = serve_counting(8).await;
        let (second, _) = serve_counting(8).await;
        let (third, _) = serve_counting(8).await;
        let replicas = ChangingReplicas::new(vec![first.clone(), second.clone()]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);

        executor.execute(&for_agent("agent-a")).await.unwrap();
        // 那副副本从列表里消失（缩容或换了 Pod），lane 的钉还在它上面。
        *replicas.addrs.lock().unwrap() = vec![second.clone(), third.clone()];

        let moved = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert_eq!(moved.exit_code, 0);
        assert!(
            moved.stderr.contains("换了一副执行器"),
            "搬家那句没说出来：{:?}",
            moved.stderr
        );
        assert!(
            moved.stderr.contains(&first),
            "搬家那句没说清从哪副来：{:?}",
            moved.stderr
        );

        // 下一条已经在同一副上，树里有刚写下的改动：再说一遍只是噪声。
        let next = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            !next.stderr.contains("换了一副执行器"),
            "只搬一次家，却说成了两条：{:?}",
            next.stderr
        );
    }

    /// 换到另一副时，客户端先把原副本上的树要过来再发命令：搬过去的就是原样
    /// 那一份，而智能体读到的第一句是「跟过来了」，不是「不在这棵树上」。
    ///
    /// 这是「工作跟着任务走」在客户端这一侧的判据。少掉搬运那一步，两副副本
    /// 各有各的卷，搬到新副上的命令会在一棵没有那份工作的树上接着写，而上面
    /// 那句「不在这棵树上」就是当时的全部交代。
    #[tokio::test]
    async fn a_lane_that_moves_has_its_tree_carried_to_the_replica_that_runs_it() {
        let (from, from_log) = serve_carrying(2).await;
        let (to, to_log) = serve_carrying(2).await;
        let replicas = ChangingReplicas::new(vec![from.clone(), "http://127.0.0.1:1".to_string()]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);

        // 第一条在原副上跑：那一副上因此有这条 lane 的树。
        executor.execute(&for_agent("agent-a")).await.unwrap();
        // 原副从地址表里消失，只剩新的那副。
        *replicas.addrs.lock().unwrap() = vec![to.clone()];

        let moved = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            moved.stderr.contains("已经跟着任务搬过来"),
            "搬成了就要这么说：{:?}",
            moved.stderr
        );
        assert!(
            !moved.stderr.contains("不在这棵树上"),
            "搬成了还说东西没跟过来：{:?}",
            moved.stderr
        );
        let handed_out = from_log.lock().unwrap().handed_out.clone();
        let received = to_log.lock().unwrap().received.clone();
        assert_eq!(
            received.len(),
            1,
            "新副本没有收到搬运的包；一次搬家只该送一份"
        );
        assert_eq!(received[0], handed_out, "搬过去的不是原样那一份");
    }

    /// 那句话是流出来的第一件事：智能体先知道自己站在哪棵树上，再读输出。
    #[tokio::test]
    async fn the_move_notice_is_the_first_event_of_the_stream() {
        use futures::StreamExt;
        let (first, _) = serve_counting(8).await;
        let (second, _) = serve_counting(8).await;
        let (third, _) = serve_counting(8).await;
        let replicas = ChangingReplicas::new(vec![first, second.clone()]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);
        executor.execute(&for_agent("agent-a")).await.unwrap();
        *replicas.addrs.lock().unwrap() = vec![second, third];

        let mut stream = executor
            .execute_stream(&for_agent("agent-a"))
            .await
            .unwrap();
        match stream.next().await {
            Some(CommandEvent::Stderr { data }) => {
                assert!(data.contains("换了一副执行器"), "{data}")
            }
            other => panic!("第一件事应该是那句搬家说明，实际是 {other:?}"),
        }
    }

    /// 一副地址时也要钉：单副本被换掉（Pod 换名换了地址、缩容后在别处重建）
    /// 同样是搬家。以前只在两副以上才钉，于是「缩到一副」之后那次换地址一声
    /// 不吭，车道在一棵没人说过的树上接着写。
    #[tokio::test]
    async fn a_replica_replaced_under_a_single_address_is_still_a_move() {
        let before = serve_counting(8).await.0;
        let after = serve_counting(8).await.0;
        let replicas = ChangingReplicas::new(vec![before.clone()]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);

        let first = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            !first.stderr.contains("换了一副执行器"),
            "第一次落点没有搬家可说：{:?}",
            first.stderr
        );

        *replicas.addrs.lock().unwrap() = vec![after];
        let moved = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            moved.stderr.contains("换了一副执行器") && moved.stderr.contains(&before),
            "单副地址从一个换成另一个，也是一次搬家：{:?}",
            moved.stderr
        );
    }

    // 假副本的额度给宽：它数的是「接到的执行请求」，而一次连接在负载下可能被
    // 重试一次。额度恰好等于预期请求数时，一次重试就把监听器关掉，用例于是读到
    // 「执行器不可达」——那是夹具的额度不够，不是被测行为（2026-11-22 实测：并行
    // 跑整库时偶发）。额度只限制夹具活多久，断言不看它。
    /// 换到的这副报「树是刚为这次调用切的」，那句话就能说死：先前的改动不在
    /// 这棵树上。执行器报的来处让这句从条件句变成结论。
    #[tokio::test]
    async fn the_move_notice_is_definite_when_the_tree_was_cut_for_this_call() {
        let first = serve_counting(8).await.0;
        let second = serve_counting(8).await.0;
        let third = serve_announcing(8, Some(cog_core::WorktreeOrigin::Created)).await;
        let replicas = ChangingReplicas::new(vec![first, second.clone()]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);
        executor.execute(&for_agent("agent-a")).await.unwrap();
        *replicas.addrs.lock().unwrap() = vec![second, third];

        let moved = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            moved.stderr.contains("刚为这个任务切了一棵新树")
                && moved.stderr.contains("不在这棵树上"),
            "报告了刚切的树，那句话还是条件句：{:?}",
            moved.stderr
        );
        assert!(
            !moved.stderr.contains("如果这不是同一副的重建"),
            "树是刚切的，就没什么可「如果」的：{:?}",
            moved.stderr
        );
    }

    /// 换到的那副说不出树从哪来（旧版执行器），那句话退回条件句：宁可少说，
    /// 不把「同一副重建」的现场说成丢了工作。
    #[tokio::test]
    async fn the_move_notice_stays_conditional_when_the_replica_cannot_say() {
        let first = serve_counting(2).await.0;
        let second = serve_counting(2).await.0;
        let third = serve_announcing(2, None).await;
        let replicas = ChangingReplicas::new(vec![first, second.clone()]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);
        executor.execute(&for_agent("agent-a")).await.unwrap();
        *replicas.addrs.lock().unwrap() = vec![second, third];

        let moved = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            moved.stderr.contains("如果这不是同一副的重建"),
            "说不出树从哪来，就不该把丢工作说成事实：{:?}",
            moved.stderr
        );
        assert!(
            !moved.stderr.contains("刚为这个任务切了一棵新树"),
            "旧版执行器没报来处，客户端不能替它断言树是刚切的：{:?}",
            moved.stderr
        );
    }

    /// 回到同一副副本，但那棵树立在它卷上的树已经被回收：钉还在不等于树还在。
    /// 这条命令必须说清楚，否则模型会以为先前的改动还在这棵树上。
    #[tokio::test]
    async fn a_lane_that_came_back_to_a_reclaimed_tree_is_told_so() {
        let replica = serve_announcing(2, Some(cog_core::WorktreeOrigin::Created)).await;
        let executor = RemoteExecutor::with_endpoints(vec![replica]).unwrap();

        let first = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            !first.stderr.contains("已经被回收"),
            "第一次落点没有先前的工作可说：{:?}",
            first.stderr
        );

        let again = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            again.stderr.contains("已经被回收") && again.stderr.contains("不在这棵树上"),
            "树没了却一声不吭，模型会在一棵空树上接着改：{:?}",
            again.stderr
        );
        assert!(
            !again.stderr.contains("换了一副执行器"),
            "没有换副本，不能说成搬家：{:?}",
            again.stderr
        );
    }

    /// 回到同一副、树还是那棵树：没有什么要说的。多说一句「也许丢了」是把
    /// 一次正常的续上读成一次事故。
    #[tokio::test]
    async fn a_lane_that_came_back_to_its_own_tree_hears_nothing() {
        let replica = serve_announcing(2, Some(cog_core::WorktreeOrigin::Existing)).await;
        let executor = RemoteExecutor::with_endpoints(vec![replica]).unwrap();
        executor.execute(&for_agent("agent-a")).await.unwrap();
        let again = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            !again.stderr.contains("已经被回收") && !again.stderr.contains("换了一副执行器"),
            "树还在，不该有丢工作的说明：{:?}",
            again.stderr
        );
    }

    /// 暖池交给的树同样没跑过任务：回到同一副却拿到它，等于原来那棵没了。
    #[tokio::test]
    async fn a_lane_that_came_back_to_a_warm_tree_is_told_it_lost_the_one_it_had() {
        let replica = serve_announcing(2, Some(cog_core::WorktreeOrigin::Warm)).await;
        let executor = RemoteExecutor::with_endpoints(vec![replica]).unwrap();
        executor.execute(&for_agent("agent-a")).await.unwrap();
        let again = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            again.stderr.contains("已经被回收"),
            "暖池的树没跑过任何任务，先前那棵已经不在了：{:?}",
            again.stderr
        );
    }

    /// 第一条命令在 HTTP 层就失败了，执行器根本没切过树：钉子有，树没有。
    /// 下一条命令拿到一棵刚切的树，不能说成「以前那棵被回收了」。
    #[tokio::test]
    async fn a_lane_that_never_got_a_tree_is_not_told_it_lost_one() {
        let replica = serve_failing_once_then_announcing(cog_core::WorktreeOrigin::Created).await;
        let executor = RemoteExecutor::with_endpoints(vec![replica]).unwrap();
        let failed = executor.execute(&for_agent("agent-a")).await;
        assert!(failed.is_err(), "第一次请求本来就该失败");
        let second = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert!(
            !second.stderr.contains("已经被回收"),
            "从来没有过树，就没有什么被回收：{:?}",
            second.stderr
        );
    }

    /// 钉子过期之后回到同一副副本：不报搬家，树没了才说树没了。这条走的是
    /// `Resumed`，背靠背的两条命令走的是 `Extended`，两条路都要有判据。
    #[tokio::test]
    async fn a_lane_that_came_back_after_its_pin_ran_out_stays_where_its_tree_is() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NOW: AtomicU64 = AtomicU64::new(0);

        let first = serve_announcing(2, Some(cog_core::WorktreeOrigin::Created)).await;
        let (second, second_hits) = serve_counting(1).await;
        let mut executor = RemoteExecutor::with_endpoints(vec![first, second]).unwrap();
        executor.clock = || NOW.load(Ordering::SeqCst);

        NOW.store(1_000, Ordering::SeqCst);
        executor.execute(&for_agent("agent-a")).await.unwrap();
        NOW.store(
            1_000 + crate::executor_lease::LANE_LEASE_SECS + 5,
            Ordering::SeqCst,
        );
        let again = executor.execute(&for_agent("agent-a")).await.unwrap();
        assert_eq!(
            second_hits.load(Ordering::SeqCst),
            0,
            "过期之后被送到了另一副副本"
        );
        assert!(
            !again.stderr.contains("换了一副执行器"),
            "过期不是搬家：树还立在原来那副上：{:?}",
            again.stderr
        );
        assert!(
            again.stderr.contains("已经被回收"),
            "留在同一副、树却是刚切的：这棵树没有先前的改动：{:?}",
            again.stderr
        );
    }

    /// A replica list the test changes between calls: what a scale-out and a
    /// scale-down look like from the client's side.
    struct ChangingReplicas {
        addrs: std::sync::Mutex<Vec<String>>,
        resolutions: std::sync::atomic::AtomicUsize,
        failing: std::sync::atomic::AtomicBool,
    }

    impl ChangingReplicas {
        fn new(addrs: Vec<String>) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                addrs: std::sync::Mutex::new(addrs),
                resolutions: std::sync::atomic::AtomicUsize::new(0),
                failing: std::sync::atomic::AtomicBool::new(false),
            })
        }

        fn add(&self, addr: String) {
            self.addrs.lock().unwrap().push(addr);
        }
    }

    #[async_trait::async_trait]
    impl Discovery for ChangingReplicas {
        async fn addresses(&self) -> std::io::Result<Vec<String>> {
            use std::sync::atomic::Ordering;
            self.resolutions.fetch_add(1, Ordering::SeqCst);
            if self.failing.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("the name does not resolve right now"));
            }
            Ok(self.addrs.lock().unwrap().clone())
        }
    }

    fn discovering(
        replicas: std::sync::Arc<ChangingReplicas>,
        refresh: std::time::Duration,
    ) -> RemoteExecutor {
        let mut executor =
            RemoteExecutor::from_source(EndpointSource::Discovered(replicas), REPLACEMENT_WINDOW)
                .unwrap();
        executor.endpoint_refresh = refresh;
        executor
    }

    /// A replica added for capacity takes the next lane without the client
    /// restarting, and it does not move the lanes already placed: those name
    /// their address, and their address did not go anywhere.
    #[tokio::test]
    async fn a_replica_added_while_the_client_runs_takes_new_lanes() {
        use std::sync::atomic::Ordering;
        let (first, first_hits) = serve_counting(2).await;
        let (second, second_hits) = serve_counting(2).await;
        let replicas = ChangingReplicas::new(vec![first.clone(), second.clone()]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);

        executor.execute(&for_task("t", "agent-a")).await.unwrap();
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);

        replicas.add(second.clone());
        // The lane that is already placed stays where its tree is ...
        executor.execute(&for_task("t", "agent-a")).await.unwrap();
        assert_eq!(first_hits.load(Ordering::SeqCst), 2);
        assert_eq!(second_hits.load(Ordering::SeqCst), 0);
        // ... and a lane nobody placed yet — a task nobody has worked on — is
        // spread onto the new replica.
        executor.execute(&for_task("u", "agent-b")).await.unwrap();
        assert_eq!(second_hits.load(Ordering::SeqCst), 1);
    }

    /// A resolved list is reused for a while: every command asking DNS would
    /// buy nothing, since replicas do not appear per tool call.
    #[tokio::test]
    async fn the_replica_list_is_not_asked_for_on_every_command() {
        use std::sync::atomic::Ordering;
        let (first, _hits) = serve_counting(2).await;
        let replicas = ChangingReplicas::new(vec![first]);
        let executor = discovering(replicas.clone(), std::time::Duration::from_secs(60));
        executor.execute(&for_agent("agent-a")).await.unwrap();
        executor.execute(&for_agent("agent-b")).await.unwrap();
        assert_eq!(
            replicas.resolutions.load(Ordering::SeqCst),
            1,
            "两条命令之间副本表不该被重新问一遍"
        );
    }

    /// A name that resolves to nothing is an error, never a quiet fall back to
    /// running the command locally: the local executor is the application's own
    /// pod, and a tool that was meant for the sandbox must not land there.
    #[tokio::test]
    async fn a_name_with_no_replica_behind_it_fails_instead_of_running_locally() {
        let replicas = ChangingReplicas::new(Vec::new());
        let executor = discovering(replicas, std::time::Duration::ZERO);
        let error = executor
            .execute(&for_agent("agent-a"))
            .await
            .expect_err("没有副本时这一条必须失败");
        let text = error.to_string();
        assert!(
            text.contains("no sandbox executor replica is known"),
            "判词没有说清是没有副本：{text}"
        );
    }

    /// A resolution failure is not the same as "the replicas are gone": the
    /// ones this client last saw are still answering, so commands keep going
    /// there while the name is unreadable.
    #[tokio::test]
    async fn a_resolution_failure_keeps_the_replicas_last_seen() {
        use std::sync::atomic::Ordering;
        let (first, first_hits) = serve_counting(2).await;
        let replicas = ChangingReplicas::new(vec![first]);
        let executor = discovering(replicas.clone(), std::time::Duration::ZERO);
        executor.execute(&for_agent("agent-a")).await.unwrap();
        replicas
            .failing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        executor.execute(&for_agent("agent-b")).await.unwrap();
        assert_eq!(first_hits.load(Ordering::SeqCst), 2);
    }

    /// Minimal NDJSON server on an ephemeral port speaking the executor
    /// wire protocol; no HTTP framework needed for a canned response.
    async fn serve_once(lines: Vec<String>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let body = lines.join("\n") + "\n";
            // 客户端先问树在哪副；这一问的答案是 404，命令才是下面那条。
            let mut socket = loop {
                let (mut candidate, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let read = candidate.read(&mut buf).await.unwrap_or(0);
                if asked_path(&buf[..read]).starts_with("/worktree/") {
                    let _ = candidate
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!")
                        .await;
                    continue;
                }
                break candidate;
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{}", addr)
    }

    fn cmd_req(command: &str) -> SandboxRequest {
        SandboxRequest {
            task_id: "t".into(),
            agent_id: "test".into(),
            payload: SandboxPayload::Command {
                command: command.into(),
            },
            input: serde_json::json!({}),
            limits: Default::default(),
        }
    }

    #[tokio::test]
    async fn remote_executor_decodes_ndjson_stream() {
        let url = serve_once(vec![
            serde_json::to_string(&CommandEvent::Stdout {
                data: "hello".into(),
            })
            .unwrap(),
            serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap(),
        ])
        .await;
        let executor = RemoteExecutor::new(url).unwrap();
        let result = executor.execute(&cmd_req("echo hello")).await.unwrap();
        assert_eq!(result.stdout, "hello");
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn remote_executor_surfaces_http_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut socket = loop {
                let (mut candidate, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let read = candidate.read(&mut buf).await.unwrap_or(0);
                if asked_path(&buf[..read]).starts_with("/worktree/") {
                    let _ = candidate
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nno tree!!")
                        .await;
                    continue;
                }
                break candidate;
            };
            socket
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 3\r\n\r\nbad")
                .await
                .unwrap();
        });
        let executor = RemoteExecutor::new(format!("http://{}", addr)).unwrap();
        let err = executor.execute(&cmd_req("echo hi")).await.unwrap_err();
        assert!(err.to_string().contains("500"));
    }

    /// 一个刚被释放的端口：连上去操作系统会明确拒绝，这正是执行器换版时
    /// 「服务没有就绪后端」的样子。
    async fn a_port_nothing_listens_on() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    /// 等待期间起来的执行器必须被等到：换版窗口是两分半，一条命令的失败不该
    /// 记在 Agent 头上。断言的是"重试跨过了窗口里的那一段"，不是"重试存在"。
    #[tokio::test]
    async fn a_replaced_executor_is_waited_for_instead_of_reported_unreachable() {
        let addr = a_port_nothing_listens_on().await;
        let executor = RemoteExecutor::with_replacement_window(
            format!("http://{addr}"),
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let (mut socket, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let body = format!(
                "{}\n",
                serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap()
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let result = executor.execute(&cmd_req("echo alive")).await;
        assert!(
            result.is_ok(),
            "the executor came back inside the window, so the call must succeed: {:?}",
            result.err().map(|e| e.to_string())
        );
    }

    /// 窗口过了还连不上，报错要说清等过多久、为什么不再等——否则下一轮 Agent
    /// 会把一段换版读成"平台坏了"，再交一次白卷。
    #[tokio::test]
    async fn a_port_that_never_comes_back_names_the_window_it_waited() {
        let addr = a_port_nothing_listens_on().await;
        let executor = RemoteExecutor::with_replacement_window(
            format!("http://{addr}"),
            std::time::Duration::from_millis(600),
        )
        .unwrap();
        let err = executor.execute(&cmd_req("echo hi")).await.unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("replacement window"),
            "the message must say the wait was bounded by a replacement window: {text}"
        );
        assert!(
            text.contains("sandbox executor unreachable"),
            "the message must still name what was unreachable: {text}"
        );
    }

    /// 拒绝与丢包分属两条路：前者会自己好，后者等多久都不会。
    #[test]
    fn a_refused_port_is_told_apart_from_a_packet_the_network_dropped() {
        let refused = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let dropped = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        assert!(the_port_refused(&refused));
        assert!(!the_port_refused(&dropped));
    }

    #[tokio::test]
    async fn remote_executor_rejects_wasm_payload() {
        let executor = RemoteExecutor::new("http://127.0.0.1:1").unwrap();
        let mut req = cmd_req("echo hi");
        req.payload = SandboxPayload::Wasm {
            bytes: vec![],
            entry: "main".into(),
        };
        assert!(executor.execute(&req).await.is_err());
    }
}
