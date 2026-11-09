//! `GET /api/v1/delivery/build-gate` —— 宿主构建槽现在是什么样子。
//!
//! 这台机器同时跑集群和喂集群的构建。一次「同时几条构建」的界不是配置出来的，是宿主
//! 实测导出的，因此只有放在一个**每个构建方都能加入**的地方才管得住：一个目录、每个位子
//! 一个 `flock` 文件、界写在文件里。集群里的构建器、宿主上的包装器、每个 worktree 的全量
//! 门都是它的客户；客户越多，「现在谁占着、谁排着、这台机器有多堵」这个问题就越只能由
//! 一个地方回答。
//!
//! 这一面就是那个地方的读数。它不新建存储：字节已经在盘上了，这里只解析并说出三件事：
//!
//! - **谁占着**：每个 `slot-<n>.lock` 的持有者记录（`<pid>\t<epoch>\t<名字>`）。一个位子
//!   是不是被占着由内核回答（对这个文件做一次非阻塞独占 `flock`），不由记录文件在不在
//!   回答——一个被复制、被遗留的文件会让「空着」读成「占着」，而读的人会去找一条不存在
//!   的构建。
//! - **谁排着**：`builds.reserve` 里那个「下一个构建归我」的声明。它的寿命由新鲜窗决定，
//!   写它的人不再刷新就自己作废，所以读数要带**多久没刷新**，不能只给一个名字。
//! - **有多堵**：`/proc/pressure/{io,cpu,memory}` 与 `/proc/loadavg`，和槽位同一瞬间读。
//!   两件事分开读的读数答不了「是构建把这台机器压住了，还是别的东西」。
//!
//! 三种结局必须分得开，而且页面要把理由印出来：
//!
//! - `measured`：目录在、读得到，下面每一格都是这一拍的真读数；
//! - `unwired`：这个部署没有把槽目录挂进来（目录不在）——不是「没有构建」；
//! - `unread`：目录在、读不动——不是「空着」。
//!
//! 空目录与没有目录在页面上长得一样，处置却相反：一个是改挂载，一个是真没人构建。

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use serde::Serialize;

/// 槽目录。与集群里的闸门、宿主上的包装器同一个键，所以部署里只有一处设置。
pub const GATE_DIR_ENV: &str = "COGNEVA_BUILD_GATE_LOCK_DIR";
/// 声明的新鲜窗。默认值与 `cog_core::BuildGateConfig` 那一格同源，不另抄一个数。
pub const RESERVE_FRESH_ENV: &str = "COGNEVA_BUILD_GATE_RESERVE_FRESH_SECS";

const BOUND_FILE: &str = "buildgate.bound";
const RESERVE_FILE: &str = "builds.reserve";
const SLOT_PREFIX: &str = "slot-";
const SLOT_SUFFIX: &str = ".lock";

/// 一格读数。`state` 是闭集里的一个词，页面据此决定画读数还是画理由。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GateReport {
    pub state: String,
    /// `unwired` / `unread` 的原因；`measured` 时为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    pub dir: String,
    /// 这一拍认为同时可以跑几条。读不到就是 `None` 带原因，不是 1。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound: Option<u32>,
    /// 界那份文件多久没被写过。界是会动的读数，不是常量。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_written_secs_ago: Option<u64>,
    pub slots: Vec<SlotReading>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reservation: Option<ReservationReading>,
    pub pressure: Vec<PressureReading>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load: Option<LoadReading>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SlotReading {
    /// `slot-0` 这样的位子名，与盘上的文件名一致，读者能拿它去对照。
    pub name: String,
    pub held: bool,
    /// 持有者记录：`<名字>`，加上它从什么时候开始占着。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder: Option<HolderReading>,
    /// 位子被占着但没人留下记录。这是「旧版本的构建器」与「记录没写成」两种情形共用的
    /// 读数，所以它自己是一句话，不与 `holder: None` 混为一谈（后者是空位子）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unrecorded: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HolderReading {
    pub what: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub held_secs: Option<u64>,
}

/// 排队的那个人，以及它多久没刷新过自己的声明——后者决定它还算不算数。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReservationReading {
    pub who: String,
    pub renewed_secs_ago: Option<u64>,
    pub fresh: bool,
    pub fresh_window_secs: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PressureReading {
    /// `io` / `cpu` / `memory`
    pub resource: String,
    pub avg10: String,
    pub avg60: String,
    pub avg300: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LoadReading {
    pub load1: String,
    pub load5: String,
    pub load15: String,
}

pub async fn build_gate_handler() -> Json<GateReport> {
    Json(read_gate(
        non_empty_env(GATE_DIR_ENV).as_deref(),
        reserve_fresh_secs(),
        &proc_root(),
    ))
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// 页面上那颗「多久没刷新」的钟与写声明的那颗必须是同一颗，所以取的是同一格配置。
fn reserve_fresh_secs() -> u64 {
    non_empty_env(RESERVE_FRESH_ENV)
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cog_core::BuildGateConfig::default().reserve_fresh_secs)
}

/// 读 `/proc` 的根。测试用它指向一份夹具，生产里就是 `/proc`。
fn proc_root() -> PathBuf {
    non_empty_env("COGNEVA_PROC_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/proc"))
}

pub fn default_gate_dir() -> PathBuf {
    // 没有配置时与集群里那一份同一条默认路径：部署里网关与演进进程读的是同一个目录，
    // 两处各自算一个默认值迟早会分叉。
    PathBuf::from("/var/lib/cogneva-build-gate")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 拼这一拍。`dir` 为 `None` 是「这个部署没给槽目录」——不是「没有构建」。
pub fn read_gate(dir: Option<&str>, fresh_window: u64, proc_root: &Path) -> GateReport {
    let generated_at = chrono::Utc::now().to_rfc3339();
    let pressure = read_pressure(proc_root);
    let load = read_load(proc_root);
    let path = dir.map(PathBuf::from).unwrap_or_else(default_gate_dir);
    let shown = path.display().to_string();

    let base = |state: &str, why: Option<String>| GateReport {
        state: state.to_string(),
        why,
        dir: shown.clone(),
        bound: None,
        bound_written_secs_ago: None,
        slots: Vec::new(),
        reservation: None,
        pressure: pressure.clone(),
        load: load.clone(),
        generated_at: generated_at.clone(),
    };

    if !path.exists() {
        return base(
            "unwired",
            Some(format!(
                "这个部署没有把构建槽目录挂进来（{GATE_DIR_ENV} = {shown}）：这里读不到谁在构建，不是没有构建"
            )),
        );
    }
    let entries = match std::fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(e) => {
            return base(
                "unread",
                Some(format!("槽目录 {shown} 读不动（{e}）：这里报不出谁在构建")),
            )
        }
    };

    let mut slots: Vec<SlotReading> = Vec::new();
    let mut bound_path: Option<PathBuf> = None;
    let mut reserve_reading = None;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name == BOUND_FILE {
            bound_path = Some(entry.path());
        } else if name == RESERVE_FILE {
            reserve_reading = Some(read_reservation(&entry.path(), fresh_window));
        } else if let Some(slot) = name
            .strip_prefix(SLOT_PREFIX)
            .and_then(|rest| rest.strip_suffix(SLOT_SUFFIX))
        {
            let _ = slot;
            slots.push(read_slot(&entry.path(), &slots_name(&name)));
        }
    }
    // 位子按编号排：盘上的顺序是目录实现的事，读者要按「第几个」读。
    slots.sort_by_key(|s| slot_index(&s.name));

    let (bound, bound_age) = match bound_path.as_deref() {
        Some(p) => read_bound(p),
        None => (None, None),
    };

    GateReport {
        state: "measured".to_string(),
        why: None,
        dir: shown,
        bound,
        bound_written_secs_ago: bound_age,
        slots,
        reservation: reserve_reading,
        pressure,
        load,
        generated_at,
    }
}

fn slots_name(file: &str) -> String {
    file.trim_end_matches(SLOT_SUFFIX).to_string()
}

fn slot_index(name: &str) -> u32 {
    name.strip_prefix(SLOT_PREFIX)
        .and_then(|n| n.parse().ok())
        .unwrap_or(u32::MAX)
}

fn read_bound(path: &Path) -> (Option<u32>, Option<u64>) {
    let age = std::fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| now_secs().saturating_sub(d.as_secs()));
    let bound = std::fs::read_to_string(path)
        .ok()
        .and_then(|t| t.trim().parse::<u32>().ok())
        .filter(|b| *b > 0);
    (bound, age)
}

/// 这个位子现在有没有人占着。
///
/// 判据是内核对这个文件的锁，不是记录文件在不在：位子文件会比占着它的构建活得久
/// （构建结束、文件还在），把「文件非空」当「占着」会让读者去找一条已经结束的构建。
fn read_slot(path: &Path, name: &str) -> SlotReading {
    let held = slot_is_held(path);
    let record = std::fs::read_to_string(path).ok();
    let mut slot = SlotReading {
        name: name.to_string(),
        held,
        holder: None,
        unrecorded: None,
    };
    if !held {
        return slot;
    }
    match record.as_deref().and_then(parse_holder) {
        Some(holder) => slot.holder = Some(holder),
        None => {
            slot.unrecorded =
                Some("位子被占着，但持有者没留下记录（旧版本的构建器，或记录没写成）".to_string())
        }
    }
    slot
}

fn parse_holder(text: &str) -> Option<HolderReading> {
    let line = text.lines().next()?.trim();
    let mut parts = line.split('\t');
    let pid = parts.next()?.trim().parse::<u32>().ok();
    let since = parts.next().and_then(|s| s.trim().parse::<u64>().ok());
    let what = parts.next().unwrap_or("").trim().to_string();
    if what.is_empty() {
        return None;
    }
    Some(HolderReading {
        what,
        pid,
        held_secs: since.map(|s| now_secs().saturating_sub(s)),
    })
}

fn slot_is_held(path: &Path) -> bool {
    use std::os::unix::io::AsRawFd;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        // 打不开就当占着：读不到与空着是两件事，只有一件意味着不用等。
        Err(_) => return true,
    };
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        return false;
    }
    true
}

fn read_reservation(path: &Path, fresh_window: u64) -> ReservationReading {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let line = text.trim().to_string();
    let (who, renewed) = match line.split_once('\t') {
        Some((who, since)) => (
            who.trim().to_string(),
            since
                .trim()
                .parse::<u64>()
                .ok()
                .map(|s| now_secs().saturating_sub(s)),
        ),
        None => (line, None),
    };
    ReservationReading {
        who,
        renewed_secs_ago: renewed,
        fresh: renewed.map(|age| age <= fresh_window).unwrap_or(false),
        fresh_window_secs: fresh_window,
    }
}

fn read_pressure(proc_root: &Path) -> Vec<PressureReading> {
    let mut out = Vec::new();
    for resource in ["io", "cpu", "memory"] {
        let Ok(text) = std::fs::read_to_string(proc_root.join("pressure").join(resource)) else {
            continue;
        };
        let Some(line) = text.lines().find(|l| l.starts_with("some ")) else {
            continue;
        };
        let field = |key: &str| {
            line.split_whitespace()
                .find_map(|part| part.strip_prefix(key))
                .unwrap_or("")
                .to_string()
        };
        out.push(PressureReading {
            resource: resource.to_string(),
            avg10: field("avg10="),
            avg60: field("avg60="),
            avg300: field("avg300="),
        });
    }
    out
}

fn read_load(proc_root: &Path) -> Option<LoadReading> {
    let text = std::fs::read_to_string(proc_root.join("loadavg")).ok()?;
    let mut parts = text.split_whitespace();
    Some(LoadReading {
        load1: parts.next()?.to_string(),
        load5: parts.next()?.to_string(),
        load15: parts.next()?.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::io::AsRawFd;

    fn fixture() -> PathBuf {
        // 一条进程里并行跑的那些用例共用同一个 pid 与同一秒，只靠这两个数取目录会让
        // 它们互相删对方的夹具——失败会印成「界读不到」，而原因在测试自己身上。
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "cogneva-buildgate-{}-{}-{}",
            std::process::id(),
            now_secs(),
            seq
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn proc_fixture(pressure_io: Option<&str>, load: Option<&str>) -> PathBuf {
        let dir = fixture();
        std::fs::create_dir_all(dir.join("pressure")).unwrap();
        if let Some(text) = pressure_io {
            std::fs::write(dir.join("pressure/io"), text).unwrap();
        }
        if let Some(text) = load {
            std::fs::write(dir.join("loadavg"), text).unwrap();
        }
        dir
    }

    #[test]
    fn a_directory_that_is_not_there_says_it_is_not_mounted_rather_than_nothing_running() {
        let missing = std::env::temp_dir().join("cogneva-buildgate-absent-does-not-exist");
        let report = read_gate(
            Some(missing.to_str().unwrap()),
            900,
            &proc_fixture(None, None),
        );
        assert_eq!(report.state, "unwired");
        assert!(
            report.why.as_deref().unwrap_or("").contains("不是没有构建"),
            "理由要说清「读不到」与「没有」的区别：{:?}",
            report.why
        );
        assert!(report.slots.is_empty());
    }

    #[test]
    fn a_measured_gate_reports_the_bound_every_slot_and_who_holds_it() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "2\n").unwrap();
        std::fs::write(dir.join("slot-0.lock"), "").unwrap();
        let mut held = std::fs::File::create(dir.join("slot-1.lock")).unwrap();
        // 持有者记录：这一格的名字与它占住的时刻。
        writeln!(
            held,
            "4242\t{}\tgate: cargo test -p cogneva",
            now_secs().saturating_sub(30)
        )
        .unwrap();
        // 真把它锁住，读数才有一格是「占着」。
        let rc = unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(rc, 0, "夹具自己要能把这一格锁上");

        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert_eq!(report.state, "measured");
        assert_eq!(report.bound, Some(2));
        assert_eq!(report.slots.len(), 2);
        assert_eq!(report.slots[0].name, "slot-0");
        assert!(!report.slots[0].held, "没被锁的位子是空位子");
        assert_eq!(report.slots[1].name, "slot-1");
        assert!(report.slots[1].held);
        let holder = report.slots[1].holder.as_ref().expect("有记录就该读出来");
        assert_eq!(holder.what, "gate: cargo test -p cogneva");
        assert_eq!(holder.pid, Some(4242));
        assert!(
            holder.held_secs.unwrap_or(0) >= 30,
            "占住的时长要真的算出来：{:?}",
            holder.held_secs
        );
    }

    #[test]
    fn a_held_slot_with_no_record_is_held_and_unnamed_rather_than_free() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "1\n").unwrap();
        // 锁上但一个字都不写：旧版本的构建器就是这个形状。
        let held = std::fs::File::create(dir.join("slot-0.lock")).unwrap();
        let rc = unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(rc, 0, "夹具自己要能把这一格锁上");
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert!(report.slots[0].held);
        assert!(report.slots[0].holder.is_none());
        assert!(
            report.slots[0].unrecorded.is_some(),
            "占着但没名字要说出来，不能读成空位子"
        );
    }

    #[test]
    fn a_reservation_carries_its_age_because_age_is_what_makes_it_count() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "1\n").unwrap();
        std::fs::write(
            dir.join(RESERVE_FILE),
            format!("gate: clippy\t{}", now_secs().saturating_sub(1200)),
        )
        .unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        let reservation = report.reservation.expect("声明要读出来");
        assert_eq!(reservation.who, "gate: clippy");
        assert!(
            reservation.renewed_secs_ago.unwrap_or(0) >= 1200,
            "声明要带出它多久没刷新：{:?}",
            reservation.renewed_secs_ago
        );
        assert!(
            !reservation.fresh,
            "过了新鲜窗的声明不再算数，页面不能只印名字"
        );
    }

    #[test]
    fn the_bound_is_a_reading_with_an_age_not_a_constant() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "3\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert_eq!(report.bound, Some(3));
        assert!(
            report.bound_written_secs_ago.is_some(),
            "界是实测导出的，读者要知道它多久没重算过"
        );
    }

    #[test]
    fn an_unreadable_bound_is_null_not_one() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "not a number\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert_eq!(report.bound, None, "量不到与量到 1 必须分得开");
    }

    #[test]
    fn pressure_and_load_are_read_or_left_out_together() {
        let proc = proc_fixture(
            Some("some avg10=31.83 avg60=31.83 avg300=31.25 total=701980500817\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"),
            Some("9.36 8.23 11.97 3/1234 5678\n"),
        );
        let report = read_gate(None, 900, &proc);
        let io = report
            .pressure
            .iter()
            .find(|p| p.resource == "io")
            .expect("io 那一行要读出来");
        assert_eq!(io.avg10, "31.83");
        let load = report.load.expect("loadavg 要读出来");
        assert_eq!(load.load1, "9.36");
    }

    #[test]
    fn a_host_without_pressure_files_says_nothing_rather_than_zero() {
        let proc = proc_fixture(None, None);
        let report = read_gate(None, 900, &proc);
        assert!(report.pressure.is_empty(), "没有那一行就不印 0");
        assert!(report.load.is_none());
    }
}
