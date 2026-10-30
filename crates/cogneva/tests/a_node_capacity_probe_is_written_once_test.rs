//! principle: feedback_no_hardcoding_config_surface
//!
//! 一台机器的内存总数是同一件事的四个落点：kubespray 清单里每个节点的 SSH 探针、
//! K3s agent 首次启动前的同一个探针、安装器本机 `/proc/meminfo` 的解析，以及两个
//! 运行期闸门（评测的准入闸、构建闸）各自要拟合的这台宿主的容量。它们曾经各拼各的，
//! 因为当初是抄的——谁也没有比较谁。
//!
//! 拼法一改，坏的不是读数消失，而是**读数换成另一个数**：把 `MemTotal` 换成
//! `MemAvailable`、把 `/1024` 换成 `/1024/1024`、或让探针多印一行，另一条路径
//! 就会按另一个容量去算这台节点的 QoS 预留与驱逐水位。安装照常跑完，集群照常
//! 起来，只是那台节点上「按机器实际内存」推导出来的份额，和同一批装上来的其它
//! 节点不一样——没有一条读数能把这件事故说成「不一致」，它只是按不同数字算的。
//!
//! 所以现在这一份读法只在一处：`cog_core::host` 声明字段名
//! （`MEMINFO_TOTAL_FIELD`）并解析（`mem_total_kb`），安装器的 SSH 探针串由同一个
//! 字段名拼出来，两个运行期闸门调同一个解析。本门盯的是它不再被抄到第二处：生产行里
//! 出现 `MemTotal:` 的文件，必须恰好是那个声明与解析的文件。
//!
//! 判不了什么：
//!
//! - 探针里那个 `/1024`（kB → MiB）与读者按 MiB 算的份额对不对得上。那是单位数学，
//!   由 `parse_node_readings` 与 `mem_total_kb` 的单测看着。
//! - 别的取数口径（`MemAvailable`、cgroup 限额）。它们不是同一件事的第二份拼法，
//!   本门没有题目。
//! - 散文。错误信息里提到 `MemTotal` 不是第二个解析点——本门读的是生产行里的
//!   `MemTotal:`（带冒号），而注释行在普查之前就被剥掉了。

use crate::common;

use std::collections::BTreeMap;
use std::path::Path;

use common::routes::repo_root;
use common::rust_production::{production_lines, production_sources, relative_name};

/// 这份读法唯一允许出现的地方。
const DECLARED_IN: &str = "crates/cog-core/src/host.rs";

/// 每个文件的生产行里提到 `MemTotal:` 的次数。
fn mentions(text: &str) -> usize {
    production_lines(text)
        .iter()
        .map(|line| line.matches("MemTotal:").count())
        .sum()
}

/// 每个文件里这条读法的落点。
fn census(root: &Path) -> BTreeMap<String, usize> {
    let files = production_sources(root);
    let mut sites = BTreeMap::new();
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let found = mentions(&text);
        if found > 0 {
            let file = relative_name(root, path);
            sites.insert(file, found);
        }
    }
    sites
}

#[test]
fn the_host_memory_total_is_read_out_of_one_place() {
    let sites = census(&repo_root());
    assert!(
        sites.contains_key(DECLARED_IN),
        "{DECLARED_IN} 里一处 `MemTotal:` 都没有了：这一份读法被搬走或改名，本门没有题目"
    );

    let strays: Vec<(&str, usize)> = sites
        .iter()
        .filter(|(file, _)| file.as_str() != DECLARED_IN)
        .map(|(file, count)| (file.as_str(), *count))
        .collect();
    assert!(
        strays.is_empty(),
        "宿主内存总数被抄到了第二处。两份拼法都能跑，所以没有任何东西会失败：\
         它们只是按各自的字段与除数算出另一个容量，于是另一条安装路径上的节点拿到\
         另一份 QoS 预留与驱逐水位，而「这条路径算的是另一个数」本身没有读数。\
         收拢的去处是 `cog_core::host`：字段名 `MEMINFO_TOTAL_FIELD`、解析\
         `mem_total_kb`，安装器的 SSH 探针串也由同一个字段名拼（\
         `cogneva_bootstrap::node_capacity_probe`）。\n{}",
        strays
            .iter()
            .map(|(file, count)| format!("      {file}: {count} 处"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_census_counts_a_second_parser_and_reads_nothing_out_of_prose() {
    // 第二处解析就是第二处，哪怕只用另一条反汇编不了的写法：starts_with 与
    // strip_prefix 两种拼法都要被读到。
    assert_eq!(mentions("let l = line.strip_prefix(\"MemTotal:\");"), 1);
    assert_eq!(mentions("if l.starts_with(\"MemTotal:\") {"), 1);
    assert_eq!(
        mentions("let q = \"awk '/^MemTotal:/{print $2}'\";\nlet r = 1;"),
        1
    );

    // 一次读两处的同一行算两处。
    assert_eq!(mentions("let a = (\"MemTotal:\", \"MemTotal:\");"), 2);

    // 注释里提到它不是第二处解析：普查读的是生产行，注释行在读之前就没了。
    assert_eq!(mentions("// MemTotal: 以前在这里解析\nlet a = 1;\n"), 0);
}
