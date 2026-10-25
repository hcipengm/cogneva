//! 分解出来的东西是不是一棵工作分解结构。
//!
//! 分解只保证「有一组任务、恰好一个收口」，那不构成范围结构：一片活可以有父指针
//! 指向不存在的节点，可以父子成环，也可以做完之后没有任何判据说明它做完了。这几种
//! 在任务图上都长得像正常任务——它们会被调度、会跑完、会报 Completed——所以只能在
//! 入图之前判。
//!
//! 判不过的分解**打回**，不降级放行：降级放行的那一份会一路跑到终态，而它缺的那一
//! 件（谁属于谁、怎样算做完）事后没有任何读面能补出来。

use std::collections::{BTreeMap, BTreeSet};

use cog_core::AtomicTask;

/// 一处结构缺陷。种类是闭集，判词由 [`WbsDefectKind::code`] 给出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WbsDefectKind {
    /// 父指针指向这一批里没有的任务。挂空的那一片不会被任何汇总看见。
    OrphanParent,
    /// 父指针成环。环里的每一片都在等一个永远不会到来的父节点。
    ParentCycle,
    /// 工作包（没有人挂在它下面）没有验收标准：做完与没做完没有判据。
    NoAcceptance,
    /// `blocks` 声明了一个不在这一批里的子节点：父节点的子集合是空的。
    MissingChild,
}

impl WbsDefectKind {
    /// 写进判退原因与告警的稳定标识。改它等于改判据的身份，读者按它取行。
    pub fn code(self) -> &'static str {
        match self {
            Self::OrphanParent => "wbs_orphan",
            Self::ParentCycle => "wbs_cycle",
            Self::NoAcceptance => "wbs_violation",
            Self::MissingChild => "wbs_violation",
        }
    }

    fn what(self) -> &'static str {
        match self {
            Self::OrphanParent => "父指针指向这一批里没有的任务",
            Self::ParentCycle => "父指针成环",
            Self::NoAcceptance => "工作包没有验收标准",
            Self::MissingChild => "声明的子节点不在这一批里",
        }
    }
}

/// 一处缺陷：哪一片、哪一类、具体差在哪。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WbsDefect {
    pub task_id: String,
    pub kind: WbsDefectKind,
    /// 给人读的细节。同一类缺陷在不同任务上差在不同的地方，判词必须带上那一处。
    pub detail: String,
}

/// 判一棵分解出来的树。
///
/// 判的是**这一批任务自己声明的关系**，不猜：父指针来自 `parent_id`，声明的子节点
/// 来自 `blocks`。谁也没有声明的任务按顶层工作包处理——顶层可以有多个，一棵分解
/// 不必收成一根主干（并行的工作流各起一头是合法的），所以「多个根」不是缺陷。
///
/// 结果是确定序的：同一份计划判两次得到逐条相同的判词，告警去重才靠得住。
pub fn check(tasks: &[AtomicTask]) -> Vec<WbsDefect> {
    let ids: BTreeSet<&str> = tasks.iter().map(|t| t.id.as_str()).collect();
    let mut defects = Vec::new();

    // 父指针：指向批外、指向自己、或者成环。
    let mut parent_of: BTreeMap<&str, &str> = BTreeMap::new();
    for task in tasks {
        let Some(parent) = task.parent_id.as_deref() else {
            continue;
        };
        if !ids.contains(parent) {
            defects.push(WbsDefect {
                task_id: task.id.clone(),
                kind: WbsDefectKind::OrphanParent,
                detail: format!("parent_id `{parent}` 不在这份计划里"),
            });
            continue;
        }
        parent_of.insert(task.id.as_str(), parent);
    }

    // 环：沿着父指针走，走到已见过的节点或批外就停。每个环只报一次，报在环里
    // 字典序最小的那个 id 上——环里每一片都报会让一条判词重复出现 N 次。
    let mut settled: BTreeSet<&str> = BTreeSet::new();
    let mut reported: BTreeSet<&str> = BTreeSet::new();
    for start in parent_of.keys().copied() {
        if settled.contains(start) {
            continue;
        }
        let mut path: Vec<&str> = Vec::new();
        let mut on_path: BTreeSet<&str> = BTreeSet::new();
        let mut cursor = Some(start);
        while let Some(node) = cursor {
            if on_path.contains(node) {
                let cycle_start = path.iter().position(|n| *n == node).unwrap_or(0);
                let cycle = &path[cycle_start..];
                let first = *cycle.iter().min().expect("a cycle has at least one node");
                if reported.insert(first) {
                    defects.push(WbsDefect {
                        task_id: first.to_string(),
                        kind: WbsDefectKind::ParentCycle,
                        detail: format!("父指针成环：{}", cycle.join(" → ")),
                    });
                }
                break;
            }
            if settled.contains(node) {
                break;
            }
            path.push(node);
            on_path.insert(node);
            cursor = parent_of.get(node).copied();
        }
        settled.extend(path);
    }

    // 声明的子节点必须在这一批里。这是「父节点的子集合非空」的可判形式：
    // 一片活说它下面有谁，而那些谁不在，它的子集合就是空的。
    for task in tasks {
        for child in &task.blocks {
            if !ids.contains(child.as_str()) {
                defects.push(WbsDefect {
                    task_id: task.id.clone(),
                    kind: WbsDefectKind::MissingChild,
                    detail: format!("blocks 里的 `{child}` 不在这份计划里"),
                });
            }
        }
    }

    // 工作包＝没有人挂在它下面的那一片。它必须带验收标准；有人挂在它下面的是
    // 汇总节点，它的完成由子节点汇上来，不要求自己再写一遍判据。
    let mut has_children: BTreeSet<&str> = BTreeSet::new();
    for parent in parent_of.values() {
        has_children.insert(parent);
    }
    for task in tasks {
        let declares_child = task.blocks.iter().any(|b| ids.contains(b.as_str()));
        if has_children.contains(task.id.as_str()) || declares_child {
            continue;
        }
        let has_acceptance = task.acceptance.iter().any(|a| !a.trim().is_empty());
        if !has_acceptance {
            defects.push(WbsDefect {
                task_id: task.id.clone(),
                kind: WbsDefectKind::NoAcceptance,
                detail: "这是工作包（没有人挂在它下面），acceptance 是空的".to_string(),
            });
        }
    }

    defects.sort();
    defects
}

/// 判一份单独造出来的工作包：没有父、没有人挂在它下面，所以它就是叶子。
///
/// 给「不是分解出来的、而是别处生成的一条活」用（比如绩效越界时造的那条重规划
/// 任务）。它读的是与分解同一份判据——在这里另写一遍「叶子必须有验收标准」，
/// 两处就会各自漂，而漂掉的那一处安静地放行没有验收标准的活。
pub fn check_work_package(id: &str, acceptance: &[String]) -> Vec<WbsDefect> {
    let package = AtomicTask {
        id: id.to_string(),
        name: id.to_string(),
        skill_id: None,
        description: None,
        estimated_tokens: 0,
        skill_gap: false,
        blocked_by: Vec::new(),
        blocks: Vec::new(),
        input: serde_json::Value::Null,
        output_entities: Vec::new(),
        estimated_seconds: 0,
        parent_id: None,
        acceptance: acceptance.to_vec(),
    };
    check(&[package])
}

/// 把判词折成一句能写进判退原因的话。
///
/// 逐条点名而不是只报条数：判退之后要重拆，重拆的人得知道是哪一片错了、错在哪。
pub fn summary(defects: &[WbsDefect]) -> String {
    let named: Vec<String> = defects
        .iter()
        .map(|d| format!("{} `{}`：{}", d.kind.code(), d.task_id, d.detail))
        .collect();
    format!(
        "decomposition is not a work breakdown ({} defect(s)): {}",
        defects.len(),
        named.join("; ")
    )
}

/// 见 [`WbsDefectKind::what`]——给读面用的一句话，不出现在判词里。
pub fn kind_what(kind: WbsDefectKind) -> &'static str {
    kind.what()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, parent: Option<&str>, acceptance: &[&str]) -> AtomicTask {
        AtomicTask {
            id: id.into(),
            name: id.into(),
            skill_id: None,
            description: None,
            estimated_tokens: 10,
            skill_gap: false,
            blocked_by: vec![],
            blocks: vec![],
            input: serde_json::Value::Null,
            output_entities: vec![],
            estimated_seconds: 60,
            parent_id: parent.map(String::from),
            acceptance: acceptance.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// 合规的树：两个顶层工作包各带验收标准，一个汇总节点把它们收在一起。
    #[test]
    fn a_well_formed_piece_tree_passes() {
        let tasks = vec![
            task("a", None, &["跑通用例"]),
            task("b", None, &["有报告"]),
            task("sum", None, &[]),
        ];
        // `sum` 声明了子节点，所以它是汇总节点，不要求自己的验收标准。
        let mut tasks = tasks;
        tasks[2].blocks = vec!["a".into(), "b".into()];
        assert_eq!(check(&tasks), vec![]);
    }

    /// 父指针指向批外：这一片挂空了，谁也汇总不到它。它必须被点名，不能静默
    /// 降到顶层——一棵有断口的树与一棵完整的树在界面上长得一样。
    #[test]
    fn a_parent_pointer_to_a_task_outside_the_plan_is_named_not_flattened() {
        let defects = check(&[task("a", Some("missing"), &["判据"])]);
        assert_eq!(defects.len(), 1);
        assert_eq!(defects[0].kind, WbsDefectKind::OrphanParent);
        assert_eq!(defects[0].kind.code(), "wbs_orphan");
        assert_eq!(defects[0].task_id, "a");
        assert!(
            defects[0].detail.contains("missing"),
            "判词要说出指向的是谁"
        );
    }

    /// 父指针成环：环里的每一片都在等一个永远不会来的父节点。环只报一次，
    /// 报在字典序最小的 id 上——环里每一片都报会让同一条判词重复出现。
    #[test]
    fn a_parent_cycle_is_reported_once_per_cycle() {
        let defects = check(&[
            task("a", Some("c"), &["判据"]),
            task("b", Some("a"), &["判据"]),
            task("c", Some("b"), &["判据"]),
        ]);
        let cycles: Vec<&WbsDefect> = defects
            .iter()
            .filter(|d| d.kind == WbsDefectKind::ParentCycle)
            .collect();
        assert_eq!(cycles.len(), 1, "一个三节点的环是一条判词，不是三条");
        assert_eq!(cycles[0].task_id, "a", "报在环里字典序最小的那个上");
        assert!(cycles[0].detail.contains('→'), "判词要画出这个环");
    }

    /// 工作包没有验收标准：它会被调度、会跑完、会报 Completed，而「做完了没有」
    /// 没有任何判据。这是这份门存在的主要理由。
    #[test]
    fn a_work_package_without_acceptance_criteria_is_rejected() {
        let defects = check(&[task("a", None, &[]), task("b", None, &["有报告"])]);
        assert_eq!(defects.len(), 1);
        assert_eq!(defects[0].task_id, "a");
        assert_eq!(defects[0].kind, WbsDefectKind::NoAcceptance);
        assert_eq!(
            defects[0].kind.code(),
            "wbs_violation",
            "判据的身份是 wbs_violation：A3 按这个名字取行"
        );
    }

    /// 汇总节点不要求自己的验收标准——它的完成由子节点汇上来。要求它再写一遍
    /// 等于让人在两处维护同一件事。
    #[test]
    fn a_parent_does_not_need_its_own_acceptance_criteria() {
        let mut parent = task("p", None, &[]);
        parent.blocks = vec!["c".into()];
        let defects = check(&[parent, task("c", Some("p"), &["有报告"])]);
        assert_eq!(defects, vec![]);
    }

    /// 声明了一个不存在的子节点：它的子集合就是空的。与孤儿父指针是同一个断口
    /// 的两侧，两侧都要判——只判一侧时，另一侧的空父节点照样一路跑到底。
    #[test]
    fn a_declared_child_that_does_not_exist_leaves_the_parent_empty() {
        let mut parent = task("p", None, &["有报告"]);
        parent.blocks = vec!["ghost".into()];
        let defects = check(&[parent]);
        assert_eq!(defects.len(), 1);
        assert_eq!(defects[0].kind, WbsDefectKind::MissingChild);
        assert!(defects[0].detail.contains("ghost"));
    }

    /// 同一份计划判两次得到逐条相同的判词：告警按判词去重，顺序不定就会重复响。
    #[test]
    fn the_verdict_is_deterministic() {
        let tasks = vec![
            task("z", Some("gone"), &[]),
            task("a", None, &[]),
            task("m", Some("a"), &[]),
        ];
        let first = check(&tasks);
        assert_eq!(first, check(&tasks));
        assert!(
            first.windows(2).all(|w| w[0] <= w[1]),
            "判词按 (task_id, kind) 排序"
        );
    }

    /// 一句判退原因要能落到告警上：条数 + 逐条点名。只有条数的话，重拆的人
    /// 拿不到任何可下手的信息。
    #[test]
    fn the_summary_names_every_defect_and_counts_them() {
        let defects = check(&[task("a", None, &[]), task("b", Some("gone"), &["判据"])]);
        let text = summary(&defects);
        assert!(text.contains("2 defect(s)"), "{text}");
        assert!(text.contains("`a`"), "{text}");
        assert!(text.contains("`b`"), "{text}");
        assert!(
            text.contains("wbs_violation") && text.contains("wbs_orphan"),
            "{text}"
        );
    }
}
