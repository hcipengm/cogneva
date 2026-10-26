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
pub const REPORT_INTERVAL: Duration = Duration::from_secs(60);
/// 与网关收口的条数一致：读多了也没人要，`/proc` 很大时按 PID 从小到大取。
pub const MAX_PROCESSES: usize = 64;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReportedProcess {
    pub pid: u32,
    pub parent_pid: u32,
    pub command: String,
    pub rss_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ReportBody {
    pod: String,
    container_id: String,
    processes: Vec<ReportedProcess>,
}

/// 从本容器启动一条自报循环。返回它一眼，不挡调用方。
pub fn spawn() {
    tokio::spawn(async move {
        let Ok(pod) = std::env::var("HOSTNAME") else {
            // 没有主机名就不是在 Pod 里（本地跑、测试里跑）。这一格不存在，也就
            // 没有可报的东西——安静地不报，比报一个「pod 名未知」的假格子好。
            return;
        };
        if !std::path::Path::new("/proc/self/cgroup").exists() {
            return;
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
        let Ok(client) = cog_net::build_client(&config) else {
            return;
        };
        let mut ticker = tokio::time::interval(REPORT_INTERVAL);
        loop {
            ticker.tick().await;
            // 容器 id 每一拍重读：Pod 重启之后是新的实例，报在旧 id 下面的读数
            // 不属于这一份进程表。
            let Some(container_id) = own_container_id() else {
                continue;
            };
            let processes = scan_proc();
            if processes.is_empty() {
                continue;
            }
            let body = ReportBody {
                pod: pod.clone(),
                container_id,
                processes,
            };
            // 报不上去不是这个进程的事：下一拍再说。
            let _ = client.post(&url).json(&body).send().await;
        }
    });
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
pub fn scan_proc() -> Vec<ReportedProcess> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    let mut pids: Vec<u32> = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .collect();
    pids.sort_unstable();
    pids.truncate(MAX_PROCESSES);
    pids.into_iter().filter_map(read_process).collect()
}

/// 一个 PID 的三样东西：父 PID、名字、常驻内存。它退出得快时读不全，
/// 读不全就不算这一行——半个进程比没有更误导。
pub fn read_process(pid: u32) -> Option<ReportedProcess> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (parent_pid, _) = parse_stat(&stat)?;
    let command = std::fs::read_to_string(format!("/proc/{pid}/comm"))
        .map(|comm| comm.trim().to_string())
        .unwrap_or_default();
    if command.is_empty() {
        return None;
    }
    let rss_bytes = std::fs::read_to_string(format!("/proc/{pid}/statm"))
        .ok()
        .and_then(|statm| parse_statm(&statm))
        .unwrap_or(0);
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

    #[test]
    fn scanning_proc_returns_whole_rows_only() {
        let processes = scan_proc();
        assert!(!processes.is_empty(), "宿主上总有进程");
        assert!(processes.len() <= MAX_PROCESSES, "条数按上限收");
        assert!(
            processes
                .iter()
                .all(|p| p.pid > 0 && !p.command.trim().is_empty()),
            "每一行都读全了才算"
        );
    }
}
