//! 一轮并行产出里落选的那几份，必须带着「为什么没成」留在读数面上。
//!
//! 选中的那一份进了产物，落选的若只剩一个 `branch_id`，下一轮读到的就只有
//! 「试过」，没有「为什么没成」——同一个坑会再踩一次。这一族读数分两种，**不能
//! 混**：判过但不如果的，带着判词与分；没被判过的，明写「没到判」，读的人不能
//! 把它当成「判了不通过」。
//!
//! 没有这次改动这条会红：`MergeSummary` 里只有 `selected_branch_id`，
//! 落选者的判词在合并那一刻就丢了。

use cog_collaboration::squad::pge::types::{
    rejected_candidates, round_history_doc, BranchMergeStrategy, EvaluationResult, GeneratorOutput,
    MergeSummary, PgeBranchResult, PgeRoundtableIteration, PlannerOutput, RoundOutcome, StopCause,
    StoppedProduct, Verdict,
};

fn plan(summary: &str) -> PlannerOutput {
    PlannerOutput {
        summary: summary.into(),
        plan: serde_json::json!({}),
        sub_tasks: Vec::new(),
        acceptance_criteria: Vec::new(),
        targets: Vec::new(),
        items: Vec::new(),
        definition: None,
    }
}

fn judged(branch_id: u32, verdict: Verdict, score: u32, feedback: &str) -> PgeBranchResult {
    PgeBranchResult {
        branch_id,
        plan: plan(&format!("branch {branch_id}")),
        generation: GeneratorOutput::none(),
        outcome: RoundOutcome::Judged {
            evaluation: EvaluationResult {
                verdict,
                feedback: feedback.into(),
                score: Some(score),
                criteria: Vec::new(),
                details: None,
            },
        },
    }
}

fn unjudged(branch_id: u32) -> PgeBranchResult {
    PgeBranchResult {
        branch_id,
        plan: plan(&format!("branch {branch_id}")),
        generation: GeneratorOutput::none(),
        outcome: RoundOutcome::Stopped {
            cause: StopCause::NotAttempted,
            product: StoppedProduct::None,
        },
    }
}

#[test]
fn a_loser_keeps_its_verdict_score_and_reason() {
    let branches = vec![
        judged(1, Verdict::Pass, 90, ""),
        judged(2, Verdict::Fail, 20, "它编了一个树里不存在的文件"),
        unjudged(3),
    ];
    let losers = rejected_candidates(&branches, Some(1));
    assert_eq!(losers.len(), 2, "选中的那一份不该出现在落选清单里");
    assert!(losers.iter().all(|l| l.branch_id != 1));

    let failed = losers.iter().find(|l| l.branch_id == 2).expect("branch 2");
    assert_eq!(failed.verdict, Some(Verdict::Fail));
    assert_eq!(failed.score, Some(20));
    assert!(
        failed.reason.contains("编了一个"),
        "落选的理由要逐字留下：{}",
        failed.reason
    );

    let never = losers.iter().find(|l| l.branch_id == 3).expect("branch 3");
    assert_eq!(never.verdict, None, "没被判过的那一支不能说成判了某个词");
    assert!(
        never.reason.starts_with("no judgement:"),
        "「没到判」要明写出来，不能读成「判了不通过」：{}",
        never.reason
    );
}

#[test]
fn a_verdict_with_no_reason_still_says_what_it_was() {
    // 判者答了一个词、理由留空：理由格不能因此变成空串——空串在读数面上
    // 与「没有这一行」同形，而这里确实判过。
    let branches = vec![
        judged(1, Verdict::Pass, 90, ""),
        judged(2, Verdict::Fail, 10, "   "),
    ];
    let losers = rejected_candidates(&branches, Some(1));
    let failed = losers.iter().find(|l| l.branch_id == 2).expect("branch 2");
    assert!(
        failed.reason.contains("Fail"),
        "空理由要补上它到底是什么判词：{}",
        failed.reason
    );
}

#[test]
fn a_round_with_no_choice_rejects_every_branch() {
    // 整轮没判成时「选中的那一份」不存在；此时每一支都落选，理由各自照写，
    // 不能因为没得选就把他们当成没有发生过。
    let branches = vec![unjudged(1), unjudged(2)];
    let losers = rejected_candidates(&branches, None);
    assert_eq!(losers.len(), 2);
    assert!(losers.iter().all(|l| l.verdict.is_none()));
}

#[test]
fn the_next_round_reads_why_the_others_lost() {
    let chosen = judged(1, Verdict::Pass, 90, "");
    let branch_two = judged(2, Verdict::Fail, 20, "视角从第三人称滑到了第一人称");
    let summary = MergeSummary {
        selected_branch_id: Some(1),
        strategy: BranchMergeStrategy::BestScore,
        reasoning: "Selected branch 1 by best score".into(),
        rejected: rejected_candidates(&[chosen.clone(), branch_two.clone()], Some(1)),
    };
    let history = vec![
        PgeRoundtableIteration {
            iteration: 0,
            plan: plan("first"),
            generation: GeneratorOutput::none(),
            outcome: chosen.outcome.clone(),
            branches: vec![chosen, branch_two],
            merge_summary: Some(summary),
        },
        PgeRoundtableIteration {
            iteration: 1,
            plan: plan("second"),
            generation: GeneratorOutput::none(),
            outcome: RoundOutcome::Stopped {
                cause: StopCause::NotAttempted,
                product: StoppedProduct::None,
            },
            branches: Vec::new(),
            merge_summary: None,
        },
    ];
    let doc = round_history_doc(&history);
    let first = serde_json::to_string(&doc[0]).expect("serialize");
    assert!(
        first.contains("视角从第三人称滑到了第一人称"),
        "下一轮读的历史里没有落选的理由：{first}"
    );
    // 没有合并的那一轮，读数面上是「没有这一格」，不是「空清单」——
    // 空清单会被读成「有并行、但没人落选」。
    assert_eq!(
        doc[1]["rejected"],
        serde_json::Value::Null,
        "没有合并的一轮不能长出一个空的落选清单"
    );
}
