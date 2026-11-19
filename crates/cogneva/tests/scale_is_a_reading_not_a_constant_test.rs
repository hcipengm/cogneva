//! principle: feedback_no_hardcoding_config_surface
//!
//! What the executor面 can carry is a reading, not a number somebody typed.
//!
//! The target is composed from two readings and nothing else: how many lane
//! slots one replica says it has (`GET /lane`, itself derived from that
//! replica's own cgroup), and how much work is holding or waiting across the
//! replicas. `replicas_for_the_queue` divides the second by the first, and the
//! deployment widens by starting another replica -- another volume, another
//! memory bound, another build lane.
//!
//! That only holds if nothing on the way clamps it. A cap -- a `maxReplicas`
//! in the chart, a `.min(…)` in the arithmetic -- would not show up as an
//! error: it would show up as tasks that wait, with every reading on the page
//! saying the queue is the reason and no reading saying who chose the number.
//! A thousand tasks is an order-of-magnitude illustration, not a design limit;
//! the same arithmetic has to answer for ten thousand.
//!
//! So this file judges both halves. The arithmetic is asked at a scale where a
//! guessed constant would show, and with slot counts a constant would never
//! have: the target moves because the divisor moved, including where the
//! division does not come out even. The declarations are asked directly: the
//! object that carries the replica count must be one the queue is allowed to
//! resize, the count must be declared as following a reading, and no capped
//! shape may appear anywhere the deployment is written down.

use std::path::{Path, PathBuf};

use cog_reflection::workload_resources::{replicas_for_the_queue, LaneQueue};

use crate::common;

use common::routes::repo_root;

/// 一副副本这一拍说了什么。
fn one_replica(lanes: Option<usize>, holding: usize, waiting: usize) -> LaneQueue {
    LaneQueue {
        lanes,
        holding,
        waiting,
    }
}

/// 十千份活、一副 8 条位子：目标是一千二百五十副，不是一个「够大了」的数。
///
/// 需求翻倍，目标跟着翻倍：这条律里没有任何一步会把答案按在某个数上。一个
/// 天花板在这里会红得直接——它要么让一万停在某个整千，要么让两万与一万相等。
#[test]
fn ten_thousand_pieces_of_work_buy_more_replicas_not_a_longer_queue() {
    let ten_thousand = replicas_for_the_queue(&[one_replica(Some(8), 8_000, 2_000)], 1);
    assert_eq!(
        ten_thousand.replicas,
        Some(1_250),
        "10000 份工作、每副 8 条位子 = 1250 副；读到别的数说明路上有一个没人登记的界"
    );
    let twenty_thousand = replicas_for_the_queue(&[one_replica(Some(8), 16_000, 4_000)], 1);
    assert_eq!(
        twenty_thousand.replicas,
        Some(2_500),
        "需求翻倍目标没有翻倍，就是某处把答案按住了"
    );
}

/// 除数是副本自己量出来的位子数：换一份读数，目标就跟着换。
///
/// 两条都特意取了除不尽的商（250 与 63）：一个被取整到整十整百的答案，正是
/// 「有人替它挑了一个好看的数」的形状。
#[test]
fn the_divisor_is_what_a_replica_measured_not_a_constant_somebody_liked() {
    let four = replicas_for_the_queue(&[one_replica(Some(4), 0, 1_000)], 1).replicas;
    let sixteen = replicas_for_the_queue(&[one_replica(Some(16), 0, 1_000)], 1).replicas;
    assert_eq!(four, Some(250), "1000 份工作、每副 4 条位子");
    assert_eq!(
        sixteen,
        Some(63),
        "1000 份工作、每副 16 条位子，除不尽就该进一位"
    );
    assert_ne!(
        four, sixteen,
        "同一份需求换一份位子读数目标却不变，说明除数不是读数"
    );
}

/// 一副副本说不出自己几条位子时，目标没有答案——不回落一个默认数。
///
/// 「量不到」与「量到就是这么多」必须分得开。回落会让扩缩循环在读数断掉的那些
/// 拍子上照着一个人拍的数字改副本数，而那几拍恰好是最该停手的时候。
#[test]
fn a_replica_that_cannot_say_its_slots_leaves_no_target() {
    let silent = replicas_for_the_queue(&[one_replica(None, 5, 5)], 3);
    assert_eq!(silent.replicas, None);
    assert_eq!(silent.answered, 0);
}

/// 承载副本数的那个对象由队来改：两份写下来的部署里都带着这条标记。
///
/// 没有标记的对象，回路不碰——它的副本数是它自己的结构事实。执行器恰好是
/// 唯一该被改的那一类（一副一块卷、一副一份内存界），所以这行标记落到别处
/// 或者消失，都是容量重新变成一个写死的数。
#[test]
fn the_sandbox_workload_is_one_the_queue_is_allowed_to_resize() {
    let static_manifest = repo_root().join("deploy/k3s/sandbox-executor-statefulset.yaml");
    let document: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(&static_manifest).unwrap())
            .expect("the executor's static manifest parses");
    assert_eq!(
        document["metadata"]["annotations"]["cogneva.io/replica-policy"],
        serde_yaml::Value::String("queue".into()),
        "{} 上的副本策略不是 queue，回路就不会按队改它的副本数",
        static_manifest.display()
    );
    let template =
        std::fs::read_to_string(repo_root().join("deploy/helm/cogneva/templates/sandbox.yaml"))
            .expect("the chart template is readable");
    assert!(
        template.contains("cogneva.io/replica-policy: queue"),
        "chart 里执行器对象上的副本策略标记没了：渲染出来的对象就不再由队改"
    );
}

/// 种子声明成「跟着读数走」，而不是一个由人挑的数。
///
/// 安装时那个 1 是种子：第一拍读到队之前它生效，之后由队回答。它在该叶子上头那块
/// `# tunable:` 里只要带着 `proof:`（有一场测试证明「换输入值就变」）、理由里点名
/// 那条读数，读者就能看出它不是一个天花板；块里没有证明，容量就又成了一次签字。
#[test]
fn the_seed_count_is_declared_as_following_a_reading() {
    let values = std::fs::read_to_string(repo_root().join("deploy/helm/cogneva/values.yaml"))
        .expect("the chart's values are readable");
    let lines: Vec<&str> = values.lines().collect();
    let mut found: Option<(bool, &str)> = None;
    for (index, line) in lines.iter().enumerate() {
        if !line.trim_start().starts_with("# tunable:") {
            continue;
        }
        let mut why = String::new();
        let mut measured = false;
        let mut next = index;
        while next < lines.len() && lines[next].trim_start().starts_with('#') {
            if let Some(body) = lines[next].trim_start().strip_prefix('#') {
                let body = body.trim();
                if let Some(text) = body.strip_prefix("why:") {
                    why.push_str(text.trim());
                    why.push(' ');
                }
                if body.starts_with("proof:") {
                    measured = true;
                }
            }
            next += 1;
        }
        if why.contains("GET /lane") {
            found = Some((measured, lines.get(next).copied().unwrap_or_default()));
        }
    }
    let (measured, leaf) = found.expect("the executor's replica seed carries a judgement");
    assert!(
        measured,
        "理由点名了 GET /lane，块里却没有 `proof:`：容量又成了一次签字"
    );
    assert!(
        leaf.trim_start().starts_with("replicas:"),
        "点名 GET /lane 的那块判词不在副本数叶子上头：{leaf:?}"
    );
}

/// 部署写下来的每一处都没有一个封顶的副本数。
///
/// 上限有两种写法：chart 里的 `maxReplicas` / `max_replicas`，以及一个按指标
/// 改副本数的 `HorizontalPodAutoscaler`。前者是一行数，后者是另一条没人登记
/// 的律；两种都会让「再多一点活」变成「再多等一会」。所以不是判断哪个名字像
/// 上限，而是这两份写下来的部署里不许出现它们。
#[test]
fn no_place_the_deployment_is_written_caps_the_replica_count() {
    let mut offenders = Vec::new();
    for root in ["deploy/helm/cogneva", "deploy/k3s"] {
        for file in files_under(&repo_root().join(root)) {
            let text = std::fs::read_to_string(&file).unwrap_or_default();
            let lowered = text.to_ascii_lowercase();
            if lowered.contains("maxreplicas") || lowered.contains("max_replicas") {
                offenders.push(format!("{} 写了副本上限", file.display()));
            }
            if lowered.contains("kind: horizontalpodautoscaler") {
                offenders.push(format!("{} 是第二条没人登记的扩缩律", file.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "承载量只由队导出，写下来的部署里不该有封顶：{offenders:#?}"
    );
}

fn files_under(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}
