//! 容器把自己的进程表推上去。
//!
//! 这一格问的是「这个容器里跑着什么」。答得上来的只有容器自己：它跑在自己的 PID
//! 命名空间里，`/proc` 就是它自己那一份，看到的就是全部。让网关伸手进来拿要开一项
//! 对业务容器执行命令的权限，每个读者每看一次就多敲一次命令；反过来，容器按一拍把
//! 自己那一份交上去，这份读数就成了事件流里的一帧，屏上开的几面读的是同一份。
//!
//! 它不知道自己叫什么名字——容器里没有哪个变量写着容器名。它知道自己是什么：cgroup
//! 路径的末段就是它这个容器实例的 id，而 Pod 状态里每个容器也带着同一个 id。于是
//! 报的是 `(Pod 名, 容器 id, 进程表)`，名字留给渲染那一步去对。
//!
//! 跑在所有常驻子命令前面启动，而它自己从不阻止任何一个子命令启动：报不上去只说明
//! 网关上少一格读数，不是这个进程该退出的事。

use std::time::Duration;

use serde::Serialize;

/// 自报端点。集群内那条零凭证的面（主应用 Service 的 8085 端口）。
pub const REPORT_URL_ENV: &str = "COGNEVA_PROCESS_REPORT_URL";
pub const DEFAULT_REPORT_URL: &str = "http://cogneva:8085/api/v1/cluster/process-report";
/// 自报间隔。这一格答「现在跑着什么」，不是一条时间序列；六十秒一拍足够让停掉的
/// 进程在下一拍消失，又不会把总线塞满。
// cap: fixed — 这一格答「现在跑着什么」不是时间序列；六十秒够让停掉的进程在下一拍消失，又不会把总线塞满
// keep: 删掉后停掉的进程一直留在「现在跑着什么」里；由「六十秒够它下一拍消失」兜住
pub const REPORT_INTERVAL: Duration = Duration::from_secs(60);
/// 与网关收口的条数一致：读多了也没人要，`/proc` 很大时按 PID 从小到大取。
///
/// 收条数是一件事，把「收过」说出来是另一件：一份停在 64 条的表必须带上它本来
/// 有几个（[`ScannedProc::processes_total`]），否则屏上读作「它就这么多」，而不是
/// 「这一面只读这么多」。
// cap: fixed — 与网关收口的条数一致：读多了也没人要，/proc 很大时按 PID 从小到大取
// keep: 删掉后 /proc 很大时把它全部读出来，没人要的人挤掉重要的；由「与网关收口的条数一致」兜住
pub const MAX_PROCESSES: usize = 64;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReportedProcess {
    pub pid: u32,
    pub parent_pid: u32,
    pub command: String,
    pub rss_bytes: u64,
}

/// 一遍 `/proc`：读成行的进程表，与这一份表是从多少个 pid 里收出来的。
#[derive(Debug, Default)]
pub struct ScannedProc {
    pub processes: Vec<ReportedProcess>,
    /// 列出来的 pid 个数，在按 [`MAX_PROCESSES`] 收条数**之前**取。
    ///
    /// 没有它，`processes` 停在 64 条时屏上读作「它就跑着这 64 个」——而真正发生
    /// 的事可能是「它跑着 137 个，这一面只收前 64 个」。界停在界上，与量到就是
    /// 这个值，必须是两个读数。
    pub processes_total: usize,
}

#[derive(Debug, Serialize)]
struct ReportBody {
    pod: String,
    container_id: String,
    processes: Vec<ReportedProcess>,
    /// 容器此刻列出来的进程数；`processes` 是其中读成行、且没被条数上限截掉的那些。
    processes_total: usize,
}

/// 这条循环在 `loop_health` 里的名字。
pub const LOOP_NAME: &str = "process-report";

/// 从本容器启动一条自报循环。返回它一眼，不挡调用方。
///
/// 走 `loop_health::spawn_unstoppable` 而不是裸 spawn：这条循环死掉时，屏上那一格
/// 会停在上一份自报上，而「没有新读数」与「容器里一直就这些进程」从外面看一模一样。
/// 注册之后它每拍盖一次章，停了就是一条断掉的活体序列，panic 也会被重新拉起。
pub fn spawn() {
    cog_core::loop_health::spawn_unstoppable(
        LOOP_NAME,
        cog_core::loop_health::Cadence::Periodic(REPORT_INTERVAL),
        |beat| async move {
            let Some(reporter) = Reporter::from_env() else {
                // 没有主机名 / 没有 `/proc` / 客户端造不出来：这一格在本进程里
                // 不存在，没有可报的东西。安静地不报，比报一个「pod 名未知」的
                // 假格子好。循环本身还在，只是每拍什么都不做。
                return;
            };
            reporter.run(beat).await;
        },
    );
}

/// 一次自报需要的三样东西：报给谁、以什么身份、用什么发。
struct Reporter {
    pod: String,
    url: String,
    client: reqwest::Client,
}

impl Reporter {
    fn from_env() -> Option<Self> {
        let pod = std::env::var("HOSTNAME").ok()?;
        if !std::path::Path::new("/proc/self/cgroup").exists() {
            return None;
        }
        let url = std::env::var(REPORT_URL_ENV)
            .ok()
            .filter(|url| !url.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_REPORT_URL.to_string());
        // 客户端走工厂：超时与连接池只有一处定义。两处与共享设置不同，都有理由——
        // 五秒封顶，是因为这一拍报不上去就不该拖住下一拍；不走配置里的出站代理，
        // 是因为这条流量去的是本命名空间的主应用 Service，出站代理是为外网配的，
        // 绕一圈只会让集群内调用被代理拒掉。
        let config = cog_net::HttpClientConfig {
            timeout_secs: 5,
            proxy_url: None,
            ..Default::default()
        };
        let client = cog_net::build_client(&config).ok()?;
        Some(Self { pod, url, client })
    }

    async fn run(self, beat: cog_core::loop_health::Beat) {
        let mut ticker = tokio::time::interval(REPORT_INTERVAL);
        loop {
            ticker.tick().await;
            // 每一拍盖章：报不上去也是这一拍发生了，断章要说的是「循环停了」。
            beat.beat();
            self.report_once().await;
        }
    }

    async fn report_once(&self) {
        // 容器 id 每一拍重读：Pod 重启之后是新的实例，报在旧 id 下面的读数
        // 不属于这一份进程表。
        let Some(container_id) = own_container_id() else {
            return;
        };
        let scanned = scan_proc();
        // 一条都读不成行的一拍不报：空表会盖掉上一拍那份好读数，而「这一遍读不出
        // 一行」不是「这个容器里没有进程」。
        if scanned.processes.is_empty() {
            return;
        }
        self.push(container_id, scanned).await;
    }

    /// 这一拍的自报。发送与它的失败判词收在 [`push_report`] 里：测试要能直接喂一份
    /// 读数进来，而不是先造一个 cgroup 与一整棵 `/proc`。
    async fn push(&self, container_id: String, scanned: ScannedProc) {
        let body = report_body(self.pod.clone(), container_id, scanned);
        push_report(&self.client, &self.url, &body).await;
    }
}

/// 一份自报的身体。单独成一个函数，好让「表里几条、它本来几个」在测试里读得到。
fn report_body(pod: String, container_id: String, scanned: ScannedProc) -> ReportBody {
    ReportBody {
        pod,
        container_id,
        processes: scanned.processes,
        processes_total: scanned.processes_total,
    }
}

/// 把一份自报推上去；推不上去要说出来。
///
/// 每一拍的章在发送**之前**就盖了（断章要说的是「循环停了」），所以一次持续失败的
/// 推送在活体面上永远看不见：循环好好活着，屏上那一格只是冻在上一份读数上，而
/// 「没有新读数」与「容器里一直就这些进程」从外面看一模一样。至少留下一句话，说出
/// 这一拍没上去、以及为什么。
///
/// 报不上去仍然不是这个进程的事：这里只说不抛出，下一拍照报。
pub(crate) async fn push_report<T: serde::Serialize>(
    client: &reqwest::Client,
    url: &str,
    body: &T,
) {
    match client.post(url).json(body).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => tracing::warn!(
            url,
            status = %resp.status(),
            "自报被拒收：这一拍没上去，屏上那一格停在上一份读数上"
        ),
        Err(error) => tracing::warn!(
            url,
            error = %error,
            "自报没发出去：这一拍没上去，屏上那一格停在上一份读数上"
        ),
    }
}

/// 本容器的实例 id：cgroup 路径末段里那串容器 id。
///
/// systemd 驱动写成 `cri-containerd-<id>.scope`，cgroupfs 驱动写成 `<id>`；
/// 两种都取「去掉 `cri-containerd-` 前缀与 `.scope` 后缀」之后那一串。
pub fn own_container_id() -> Option<String> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup.lines().find_map(container_id_in_cgroup_line)
}

pub fn container_id_in_cgroup_line(line: &str) -> Option<String> {
    let last = line.rsplit('/').next()?.trim();
    let without_prefix = last.strip_prefix("cri-containerd-").unwrap_or(last);
    let id = without_prefix
        .strip_suffix(".scope")
        .unwrap_or(without_prefix);
    // 容器 id 是十六进制串；短了就不是 id（宿主上直接跑时末段可能是别的）。
    let looks_like_id = id.len() >= 16 && id.chars().all(|c| c.is_ascii_hexdigit());
    looks_like_id.then(|| id.to_string())
}

/// 读一遍 `/proc`：每个 PID 的父 PID、名字、常驻内存。
pub fn scan_proc() -> ScannedProc {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return ScannedProc::default();
    };
    let pids: Vec<u32> = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .collect();
    let (processes, processes_total) = assemble_table(pids);
    ScannedProc {
        processes,
        processes_total,
    }
}

/// 一列 pid 收成一张表：返回读成行的那些，与这一列本来有几个。
///
/// 个数在截断之前取。两条自报的路（容器里读 `/proc`、宿主上读 `cgroup.procs`）
/// 共用这一处收口，所以「表停在 64 条」这个读数在哪条路上都说得出来。
pub(crate) fn assemble_table(mut pids: Vec<u32>) -> (Vec<ReportedProcess>, usize) {
    pids.sort_unstable();
    let processes_total = pids.len();
    pids.truncate(MAX_PROCESSES);
    (
        pids.into_iter().filter_map(read_process).collect(),
        processes_total,
    )
}

/// 一个 PID 的三样东西：父 PID、名字、常驻内存。它退出得快时读不全，
/// 读不全就不算这一行——半个进程比没有更误导。
///
/// 常驻内存也在这句话里：读不到 `statm` 时不许记 0，那写出去的是「这个进程不占
/// 内存」——一份没人量过的读数，而内存正是读这份进程表要找的那一样。
pub fn read_process(pid: u32) -> Option<ReportedProcess> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok();
    let comm = std::fs::read_to_string(format!("/proc/{pid}/comm")).ok();
    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok();
    assemble_process(pid, stat.as_deref(), comm.as_deref(), statm.as_deref())
}

/// 三份读数凑成一行。缺哪一份都不成一行：父 PID 从 `stat` 来、名字从 `comm` 来、
/// 常驻内存从 `statm` 来，缺了的那一格没有诚实的替代值。
pub(crate) fn assemble_process(
    pid: u32,
    stat: Option<&str>,
    comm: Option<&str>,
    statm: Option<&str>,
) -> Option<ReportedProcess> {
    let (parent_pid, _) = parse_stat(stat?)?;
    let command = comm?.trim().to_string();
    if command.is_empty() {
        return None;
    }
    let rss_bytes = parse_statm(statm?)?;
    Some(ReportedProcess {
        pid,
        parent_pid,
        command,
        rss_bytes,
    })
}

/// `/proc/<pid>/stat` 的第二个字段是父 PID；第三个是进程状态。注意第二个字段
/// 本身可能带空格与括号（`(comm with spaces)`），所以从最后一个 `)` 之后数。
pub fn parse_stat(stat: &str) -> Option<(u32, char)> {
    let rest = &stat[stat.rfind(')')? + 1..];
    let mut fields = rest.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let parent_pid = fields.next()?.parse().ok()?;
    Some((parent_pid, state))
}

/// `/proc/<pid>/statm`：`size resident shared ...`，单位是页。
pub fn parse_statm(statm: &str) -> Option<u64> {
    let resident: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident * 4096)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_systemd_cgroup_names_the_container_instance() {
        let line = "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1234.slice/cri-containerd-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.scope";
        assert_eq!(
            container_id_in_cgroup_line(line).as_deref(),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        );
    }

    #[test]
    fn a_host_run_has_no_container_instance() {
        // 在宿主上直接跑（本地调试、CI）时，末段不是容器 id；报一个假的 id
        // 只会让屏上多一格对不上任何容器的读数。
        assert_eq!(
            container_id_in_cgroup_line("0::/user.slice/session-3.scope"),
            None
        );
        assert_eq!(container_id_in_cgroup_line("0::/"), None);
    }

    #[test]
    fn a_name_with_spaces_does_not_shift_the_parent_field() {
        let stat = "42 (a name (with) parens) S 1 42 42 0 -1 4194304";
        assert_eq!(parse_stat(stat), Some((1, 'S')));
    }

    #[test]
    fn statm_is_in_pages() {
        assert_eq!(parse_statm("100 3 2 1 0 0 0"), Some(3 * 4096));
        assert_eq!(parse_statm("100"), None);
    }

    #[test]
    fn this_test_process_reads_as_a_process() {
        // 没有这次改动就会红：读不出自己说明读 `/proc` 的这条路是空的，而空的
        // 进程表在屏上会读作「没有读数」，与「里面没有进程」混成一件事。
        // （不拿 `scan_proc` 断言自己：它按条数截断，宿主上 PID 排在后面会被
        // 合理地截掉——截断是容量，读不出来才是错。）
        let me = std::process::id();
        let process = read_process(me).expect("自己的 /proc 读得出来");
        assert_eq!(process.pid, me);
        assert!(!process.command.is_empty(), "进程有名字");
        assert!(process.rss_bytes > 0, "进程占着内存");
    }

    /// 常驻内存读不到：这一行不成一行。记 0 的话，写出去的是一行「这个进程不占
    /// 内存」——一份没人量过的读数，而内存正是读这份表要找的那一样。
    #[test]
    fn a_row_whose_resident_memory_could_not_be_read_is_not_a_zero_byte_process() {
        let stat = "42 (sleep) S 1 42 42 0 -1 4194304";
        assert!(
            assemble_process(7, Some(stat), Some("sleep\n"), None).is_none(),
            "读不到 statm 的行不许以 0 字节出现"
        );
        assert!(
            assemble_process(7, Some(stat), Some("sleep\n"), Some("not numbers")).is_none(),
            "statm 读成乱码也是没读到"
        );
    }

    /// 反例：三份都在时这一行照常成形，常驻内存是量到的那个数。
    #[test]
    fn a_row_with_all_three_readings_is_assembled() {
        let stat = "42 (sleep) S 1 42 42 0 -1 4194304";
        let row = assemble_process(7, Some(stat), Some("sleep\n"), Some("100 3 2 1 0 0 0"))
            .expect("三份都在的行是一行读数");
        assert_eq!(row.pid, 7);
        assert_eq!(row.parent_pid, 1);
        assert_eq!(row.command, "sleep");
        assert_eq!(row.rss_bytes, 3 * 4096);
        // 名字与父 PID 缺了同样不成一行（这条行为没有变，钉住它）。
        assert!(assemble_process(7, None, Some("sleep"), Some("100 3 2 1 0 0 0")).is_none());
        assert!(assemble_process(7, Some(stat), Some(""), Some("100 3 2 1 0 0 0")).is_none());
    }

    #[test]
    fn scanning_proc_returns_whole_rows_only() {
        let scanned = scan_proc();
        assert!(!scanned.processes.is_empty(), "宿主上总有进程");
        assert!(scanned.processes.len() <= MAX_PROCESSES, "条数按上限收");
        assert!(
            scanned
                .processes
                .iter()
                .all(|p| p.pid > 0 && !p.command.trim().is_empty()),
            "每一行都读全了才算"
        );
        assert!(
            scanned.processes_total >= scanned.processes.len(),
            "它本来几个，不能比收进来的还少"
        );
    }

    /// 表按上限收，但「收到上限」与「它只有这么多」必须是两个读数：只看条数，
    /// 一个跑着 137 个进程的构建容器与一个正好跑 64 个的容器在屏上一模一样。
    #[test]
    fn a_table_cut_at_the_cap_states_how_many_it_could_see() {
        // 自己的 pid 重复填满：这一格判的是计数与截断，不是容器里到底有哪些进程，
        // 而重复的 pid 保证每一行都读得成（真的去凑 69 个存在的进程会在 CI 上抖动）。
        let me = std::process::id();
        let (rows, total) = assemble_table(vec![me; MAX_PROCESSES + 5]);
        assert_eq!(total, MAX_PROCESSES + 5, "个数要在截断之前取");
        assert_eq!(rows.len(), MAX_PROCESSES, "条数照旧按上限收");

        let (rows, total) = assemble_table(vec![me]);
        assert_eq!((rows.len(), total), (1, 1), "没撞上限时两个数是同一个");
    }

    /// 自报的身体里带着「本来几个」：这条读数要能走到网关那一侧，而不只是留在
    /// 本进程的内存里。
    #[test]
    fn the_pushed_table_carries_the_count_it_was_cut_from() {
        let body = report_body(
            "pod-a".into(),
            "c0ntainer".into(),
            ScannedProc {
                processes: vec![ReportedProcess {
                    pid: 7,
                    parent_pid: 1,
                    command: "sleep".into(),
                    rss_bytes: 4096,
                }],
                processes_total: 137,
            },
        );
        let json = serde_json::to_value(&body).expect("自报的身体可序列化");
        assert_eq!(json["processes_total"], serde_json::json!(137));
        assert_eq!(
            json["processes"].as_array().map(Vec::len),
            Some(1),
            "表里几条是另一回事"
        );
    }
}

#[cfg(test)]
mod push_tests {
    use super::*;
    use crate::test_support::{serve_status, WarnText};
    use tracing_subscriber::layer::SubscriberExt as _;

    fn reporter(url: String) -> Reporter {
        Reporter {
            pod: "pod-a".into(),
            url,
            client: reqwest::Client::new(),
        }
    }

    fn one_process() -> ReportedProcess {
        ReportedProcess {
            pid: 7,
            parent_pid: 1,
            command: "sleep".into(),
            rss_bytes: 4096,
        }
    }

    /// 对端收下了：什么都不说，这是常态。
    #[tokio::test]
    async fn an_accepted_push_says_nothing() {
        let (base, server) = serve_status(200).await;
        let warns = WarnText::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(warns.clone()));
        reporter(base)
            .push(
                "c0ntainer".into(),
                ScannedProc {
                    processes: vec![one_process()],
                    processes_total: 1,
                },
            )
            .await;
        assert!(warns.said().is_empty(), "{:?}", warns.said());
        server.abort();
    }

    /// 对端拒收：这一拍没上去。章已经盖过，所以活体面上看不出来，必须有一句话。
    #[tokio::test]
    async fn a_refused_push_says_the_window_did_not_land() {
        let (base, server) = serve_status(500).await;
        let warns = WarnText::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(warns.clone()));
        reporter(base)
            .push(
                "c0ntainer".into(),
                ScannedProc {
                    processes: vec![one_process()],
                    processes_total: 1,
                },
            )
            .await;
        let said = warns.said();
        assert!(
            said.iter().any(|m| m.contains("没上去")),
            "被拒收的一拍没有在日志里留下痕迹：{said:?}"
        );
        server.abort();
    }

    /// 根本发不出去（对端不在）：同一句话，带上原因。
    #[tokio::test]
    async fn a_push_that_cannot_reach_its_host_says_the_window_did_not_land() {
        // 绑一个端口再放掉：这个地址上没有人在听。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let warns = WarnText::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(warns.clone()));
        reporter(format!("http://{addr}"))
            .push(
                "c0ntainer".into(),
                ScannedProc {
                    processes: vec![one_process()],
                    processes_total: 1,
                },
            )
            .await;
        let said = warns.said();
        assert!(
            said.iter().any(|m| m.contains("没上去")),
            "发不出去的一拍没有在日志里留下痕迹：{said:?}"
        );
    }
}
