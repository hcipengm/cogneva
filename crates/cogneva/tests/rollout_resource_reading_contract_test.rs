//! 判定容器自己的资源读数有四个端点，每一个断了都是静默的。
//!
//! 滚动判定跑在一个短命的 Job Pod 里，它落在两条通用资源判据的时间窗之外：Pod 被 TTL
//! 收走时，最忙的那几分钟的样本也一起没了。"声明了多少"与"这次用掉多少"只在判定进程
//! 自己还活着的时候对照得出来——它读自己的 cgroup，写进容器的终止消息，部署器从 Pod
//! 状态里读回来。四个端点分别是：**成功也要写**（跑通了但一直被限流正是额度声明的问题，
//! 它不留任何失败证据）、**失败要写**、**部署器读**、**三个终局分支都读**。
//!
//! 断掉的样子都一样：这一轮的读数不出现。而"读数不出现"与"这一轮没超限"在观测面上同形，
//! 没有任何规则会报——所以这四个端点在这里被断言。
//!
//! 最后一条断言的是行布局：终止消息这条窄通道上与失败落点共用，落点必须还在第一行，否则
//! 读落点的那一端（只认第一行）会把读数当成一段陌生文本，那一轮失败从此判不出坏在哪一处。

use std::path::PathBuf;

const DEPLOYER: &str = "crates/cog-reflection/src/mainline_deployer.rs";
const READING: &str = "crates/cog-reflection/src/rollout_resources.rs";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// 第一处 `#[cfg(test)]` 之前的部分：测试自己可以随便写结论，它不是产出点。
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

fn occurrences(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

/// 判定进程跑完就把读数写进终止消息——**成功那一支也要写**。只写失败那一支的话，本仓
/// 最典型的一轮（跑通、但一直被 CPU 限流）恰好什么都不留，而它正是这条读数存在的理由。
#[test]
fn the_judgement_leaves_its_reading_on_the_way_out_of_both_outcomes() {
    let source = read(DEPLOYER);
    let production = production_source(&source);
    assert_eq!(
        occurrences(production, "report_termination_message("),
        3,
        "写终止消息的调用点不是「定义 + 成功一支 + 失败一支」这三处。少一处就是有一类结局\
         不留读数：只写失败那一支时，跑通却被限流的一轮什么都不留"
    );
    assert!(
        production.contains("RolloutResourceReading::read_from("),
        "判定进程不再读自己的 cgroup，落点之外没有量到的量，这条读数就没有产出面"
    );
    assert!(
        production.contains("rollout_resources::CGROUP_ROOT"),
        "cgroup 根被就地拼成了字面量路径，换个环境就没有第二处可改"
    );
}

/// 部署器把那一行读回来，并在**每一个终局分支**都上报。终局有三个：镜像已收敛、Job 成功
/// 退出、Job 失败退出。少读一个分支，那种结局下这一轮的读数就永远不进观测面。
#[test]
fn the_deployer_reads_the_reading_wherever_the_run_ends() {
    let source = read(DEPLOYER);
    let production = production_source(&source);
    assert_eq!(
        occurrences(production, "self.report_rollout_resources("),
        3,
        "收读数的调用点不是三个终局分支（收敛 / Job Complete / Job Failed）各一处"
    );
    assert!(
        production.contains("RolloutResourceReading::from_line"),
        "部署器没有用产出侧的解码器读那一行；自作一套解析等于两处对同一行各有一个约定"
    );
    assert!(
        !production.contains("\"rollout-resources"),
        "标记被就地拼了一遍：产出侧改标记时这一处不会跟着动，读回静默变成永远读不到"
    );
}

/// 行布局：落点在第一行，读数在后。这条断言的是产出侧写出来的顺序，而读落点的那一端取
/// `lines().next()`——两行一旦调换，落点读回端见到的就是一段陌生文本。
#[test]
fn the_locus_is_written_before_the_reading() {
    let production = production_source(&read(DEPLOYER)).to_string();
    let Some(body) = production.split("fn write_termination_message(").nth(1) else {
        panic!("终止消息的写入器不在了")
    };
    let body = body.split("\n}").next().unwrap_or(body);
    let locus = body
        .find("body.push_str(signature)")
        .unwrap_or_else(|| panic!("写入器不再写落点：\n{body}"));
    let reading = body
        .find("body.push_str(&reading.to_line())")
        .unwrap_or_else(|| panic!("写入器不再写读数：\n{body}"));
    assert!(
        locus < reading,
        "读数排在了落点前面：读落点的那一端只认第一行，这一改会让每一轮失败都判不出落点"
    );
}

/// 那一行的形状由产出侧一个模块定，读回端只认它。标记与版本都在那里，不在这里。
#[test]
fn the_line_shape_lives_in_one_place() {
    let reading = production_source(&read(READING)).to_string();
    assert!(
        reading.contains("pub const TAG") && reading.contains("FORMAT_VERSION"),
        "行标记与格式版本不再是产出侧声明的常量：读回端只能猜，而猜错的形状会被当成一条读数"
    );
}
