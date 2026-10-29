//! 这块宿主这一刻的压力读数：三条 PSI 的两个窗口、可用内存、以及看得见的构建槽。
//!
//! 它留在这里，是因为有**两个**实现者要读同一份事实，而它们谁也不能依赖对方：评测的准入
//! 闸门（`cog-eval`）用它算界，构建闸门（`cog-reflection`）用它给自己的槽位判轻重。两边各采
//! 一份，就等于「这台机器忙不忙」有两个答案，而两个答案是各自都自洽的那种错——读数上分不出
//! 谁对。
//!
//! 这里只有「读到了什么」，没有「该拿它怎么办」：红绿两条线、减半与加一的控制律、界的存放
//! 与选主，都在 [`cog_eval::admission`]——那是它自己的判断，换一条律就该重新量一次，不该被
//! 任何读压力的人继承。构建槽的「是不是被持有」判据留在这里，理由同 `Pressure`：一个槽位
//! 文件被谁锁着，是同一个问题，第二次实现就是第二个答案。

use std::path::Path;

/// 一刻的压力读数：三条 PSI 的两个窗口、可用内存、以及这一刻看得见的构建槽。
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Pressure {
    pub cpu_avg10: f64,
    pub cpu_avg60: f64,
    pub io_avg10: f64,
    pub io_avg60: f64,
    pub mem_avg10: f64,
    pub mem_avg60: f64,
    pub mem_available_bytes: u64,
    /// 目录里被持有的构建槽数。它是「看不见对面进程、但看得见它占着槽」的那一半：
    /// 构建方与评测方在同一台机器上，一条构建正在跑就是最直接的红灯。
    pub builds_in_flight: usize,
}

impl Pressure {
    /// 三项 PSI 里最大的 `avg10`，以及它是哪一项。取最大是判据的一部分：任一项压力大，
    /// 宿主就已经在往下掉了，不需要三项一起。名字要一起带出来——判词说「压力大」而说不出
    /// **哪一项**大，读的人还是不知道该去看什么。
    ///
    /// 公开的是一条**读法**，不是一条判断：哪个窗口最坏，任何拿到 `Pressure` 的人问的都是
    /// 同一个问题，各写一份就是两个「最坏」的判据。
    pub fn worst_avg10(&self) -> (&'static str, f64) {
        let mut worst = ("cpu", self.cpu_avg10);
        if self.io_avg10 > worst.1 {
            worst = ("io", self.io_avg10);
        }
        if self.mem_avg10 > worst.1 {
            worst = ("memory", self.mem_avg10);
        }
        worst
    }

    /// 三项 PSI `avg60` 里最大的那一个，给「稳不稳」这一问读。
    pub fn worst_avg60(&self) -> f64 {
        self.cpu_avg60.max(self.io_avg60).max(self.mem_avg60)
    }
}

/// 采一次压力。读不到就返回 `None`——「没有这个面」与「这个面读数是零」必须分得开，
/// 后者会把一个读不出压力的宿主当成一个没有压力的宿主。
pub fn sample(dir: &Path) -> Option<Pressure> {
    let (cpu_avg10, cpu_avg60) = read_psi_at("/proc/pressure/cpu")?;
    let (io_avg10, io_avg60) = read_psi_at("/proc/pressure/io")?;
    let (mem_avg10, mem_avg60) = read_psi_at("/proc/pressure/memory")?;
    let mem_available_bytes = read_mem_available()?;
    Some(Pressure {
        cpu_avg10,
        cpu_avg60,
        io_avg10,
        io_avg60,
        mem_avg10,
        mem_avg60,
        mem_available_bytes,
        builds_in_flight: build_slots_held(dir),
    })
}

/// 读一个 PSI 文件的 `some` 行，取出 `avg10`／`avg60`。
///
/// `some` 而不是 `full`：`full` 只在所有任务都停住时才动，那是已经出事的读数；闸门要
/// 在事情变坏之前减。
fn read_psi_at(path: &str) -> Option<(f64, f64)> {
    let text = std::fs::read_to_string(path).ok()?;
    parse_psi(&text)
}

fn parse_psi(text: &str) -> Option<(f64, f64)> {
    let line = text.lines().find(|l| l.starts_with("some"))?;
    let mut avg10 = None;
    let mut avg60 = None;
    for field in line.split_whitespace() {
        if let Some(v) = field.strip_prefix("avg10=") {
            avg10 = v.parse().ok();
        } else if let Some(v) = field.strip_prefix("avg60=") {
            avg60 = v.parse().ok();
        }
    }
    Some((avg10?, avg60?))
}

fn read_mem_available() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|l| l.starts_with("MemAvailable:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// 这一刻被持有的构建槽数。
///
/// 判据是「槽位文件的 flock 拿不到」——不读对面进程的任何变量，也不需要知道它有几槽：
/// 目录里有哪些 `slot-*.lock`，就是构建方允许有几种槽。目录里没有槽位文件时答案是 0，
/// 而这与「构建方在另一个目录里」在读数上同形——所以目录身份（`dev:ino`）跟着报出去，
/// 让读的人自己判断两边是不是同一个目录。
/// Slot files under `dir` that a process is holding right now.
///
/// Public because the build gate moved out of this crate and still needs this
/// count: the occupancy a bound is actually holding is read from the locks
/// themselves, and a second implementation of "is this slot held" would be a
/// second answer to the one question two readings are joined on.
pub fn build_slots_held(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut held = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("slot-") || !name.ends_with(".lock") {
            continue;
        }
        if lock_is_held(&entry.path()) {
            held += 1;
        }
    }
    held
}

/// 一个槽位文件是不是被持有。
#[cfg(unix)]
fn lock_is_held(path: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    else {
        // 打不开就当它被持有：这条灯是「有没有构建在跑」，判不出来时宁可减一档。
        return true;
    };
    use std::os::unix::io::AsRawFd;
    // SAFETY: 同 `take_controller_lock`——只碰本作用域持有的描述符。
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        false
    } else {
        true
    }
}

#[cfg(not(unix))]
fn lock_is_held(_path: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `some` 行才是判据，`full` 行不是。
    ///
    /// 两份读数都在同一个文件里，读错那一行不会失败——`full` 的格式一模一样，只是它只在
    /// 所有任务都停住时才动，拿它当压力，闸门就会在事情已经坏透之后才反应。
    #[test]
    fn the_psi_some_line_is_what_is_read() {
        let text = "some avg10=7.17 avg60=24.12 avg300=100.00 total=1234\n\
                    full avg10=6.00 avg60=20.00 avg300=90.00 total=1000\n";
        let (a10, a60) = parse_psi(text).unwrap();
        assert_eq!((a10, a60), (7.17, 24.12), "读的是 some 行那一对");
        assert!(
            parse_psi("full avg10=1 avg60=1\n").is_none(),
            "没有 some 行就是没读到"
        );
    }
}
