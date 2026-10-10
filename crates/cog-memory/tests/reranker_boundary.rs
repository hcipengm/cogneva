//! Whether a local model can place a file by its name, measured on the weights.
//!
//! The document organizer runs three stations in order: the name rules, a local model
//! that runs in this process, and the audited channel that carries a body off the
//! cluster. The second of them exists to keep names the rule table cannot place -- a file
//! whose extension the table does not claim, or which has no extension at all -- from
//! having to reach the third. Its value is that nothing leaves the process on that path,
//! so the file's **name is the only thing it may ask about**: a station that read a body
//! to decide would be the station it is supposed to save.
//!
//! This is the measurement of whether that works. It says it does not, for the model the
//! configuration surface names, and these are the numbers that are why no station was
//! built on top of it (all printed by this file, in this order):
//!
//! - The model is not broken. A pair it was trained for separates cleanly: `如何申请年假？`
//!   against an annual-leave policy scores **+3.292** and the same question against an
//!   unrelated canteen menu **-11.040**. So the weights, the ONNX graph, the tokenizer and
//!   the pair encoding are fine, and everything below is about the task.
//! - Comparing a name against the **bucket names** -- the recipe the model's own
//!   documentation gives for classification -- gets **6 of 10** intended placements right,
//!   and no boundary exists: the names a person would place score down to **-7.464** while
//!   the names that settle nothing score up to **-1.051**. The overlap is complete, so any
//!   score floor that admits the intended ones admits `notes`, `final` and `12`. An eighth
//!   candidate meaning "this name says nothing about the file" moves the ceiling to
//!   **0.938** -- it wins on `Untitled`, and on nothing else of the six -- and leaves the
//!   placements where they were.
//! - Describing each bucket in words and judging a name against the description, with that
//!   eighth candidate, declines **2 of 6** names that settle nothing and **none of the 10**
//!   intended ones, while getting **7 of 10** right and placing `temp` (-7.952), `12`
//!   (-3.748), `final` (-4.098) and `notes` (-1.461) in `documents` anyway -- and `12` and
//!   `final` sit above four of the ten names that same run placed correctly (floor -8.187).
//!   So the decline candidate is not a threshold to tune: it is an eighth answer, and it
//!   wins on the two names that carry nothing at all.
//! - The same formulation gets **10 of 10** -- when the descriptions name the files being
//!   tested (`such as Dockerfile, Makefile and scripts`). That is the control for how much
//!   of the result is wording: it still declines only the same **2 of 6**, and it answers
//!   `Makefile` with `code` only because the description said `Makefile`. With descriptions
//!   rewritten as a taxonomy (`source code and build configuration`) the same run answers
//!   `Makefile` with **archives** at **+0.299**: a confident wrong placement, the one
//!   outcome the station must not have.
//!
//! In one sentence: a bare file name and a category are not a pair this model can rank,
//! and the categories a folder can be sorted into are not recoverable from a name without
//! a description that already says where things go -- and a description that says where
//! things go is the answer, not a way of finding it. Asking the other way round (the
//! description is the query and the name is the passage, seven questions per file) is worse,
//! not better: **4 of 10**, with the same overlap (intended floor -8.010, undecided ceiling
//! -1.761). Measured in the same session on the same sets, and not reproduced here because
//! it would mean copying the rule table's extension lists into a crate that does not own
//! them: putting those lists on the candidate side instead of the bucket names leaves the
//! name-vs-candidates recipe at **6 of 10** with the same overlap.
//!
//! It needs about 2.3 GiB of weights, so it is ignored by default and is not in CI (a
//! runner has neither the weights nor a mirror to fetch them from):
//!
//! ```text
//! deploy/scripts/fetch-model-weights.sh --model reranker --dest /srv/cogneva/models/fastembed
//! FASTEMBED_CACHE_DIR=/srv/cogneva/models/fastembed \
//!   cargo test -p cog-memory --test reranker_boundary -- --ignored --nocapture
//! ```
//!
//! Re-run it before treating the conclusion as current: it is a statement about specific
//! weights (repo `rozgo/bge-reranker-v2-m3`, revision `fbd57b17`, `model.onnx` from
//! `3af844cd2de818a95d2b5de5893a336836312c8ade03f53b286ac6beae080321`), measured on
//! 2026-09-28, not about the idea of a local station. A different model, or bucket
//! descriptions an operator wrote about their own folder, would be measured the same way.
//! What this file asserts is only what has to hold for the sweep to mean anything at all:
//! that the weights separate a relevant pair from an irrelevant one.

use std::path::PathBuf;

use cog_memory::FastEmbedRerankerProvider;

/// The buckets the organizer ships in its default table, in the order it holds them.
const BUCKETS: [&str; 7] = [
    "documents",
    "spreadsheets",
    "presentations",
    "images",
    "media",
    "archives",
    "code",
];

/// What each bucket holds, written as a taxonomy and **without a word from the test set**:
/// no `Dockerfile`, no `screenshots`, no `voice memos`, no `backups`, no `budget`. An
/// operator would describe their own folders; this is what a description looks like when it
/// was not written with the answers in hand.
const DESCRIPTIONS: [&str; 7] = [
    "Text documents such as reports, letters, forms and statements.",
    "Spreadsheets and tables of financial figures.",
    "Presentation slide decks.",
    "Photographs and pictures.",
    "Audio and video recordings, music and films.",
    "Compressed archive files.",
    "Source code and build configuration.",
];

/// The same descriptions with the test set in view, kept as the control that shows how
/// much of the 10/10 was wording. Not a candidate for anything.
const DESCRIPTIONS_NAMING_THE_FILES: [&str; 7] = [
    "Text documents: invoices, contracts, reports, letters and notes.",
    "Spreadsheets and tables of numbers: budgets, accounts and lists.",
    "Slide decks and presentations.",
    "Photographs, screenshots and pictures taken with a camera or a phone.",
    "Audio and video recordings: voice memos, music, films and clips.",
    "Compressed backups and archive files such as zip and tar.",
    "Source code and build files such as Dockerfile, Makefile and scripts.",
];

/// The eighth candidate: the answer "this name says nothing about what the file is".
const DECLINE: &str = "A name that says nothing about what the file is.";

/// Files whose name says what they are even though their extension does not: the pair is
/// the answer a person looking at the folder afterwards would agree with.
const DECIDABLE: [(&str, &str); 10] = [
    ("IMG_0421", "images"),
    ("DSC_00991", "images"),
    ("Screenshot from 2026-09-01 10-22-31", "images"),
    ("发票_20260901", "documents"),
    ("合同终版", "documents"),
    ("budget_2026_q3", "spreadsheets"),
    ("Dockerfile", "code"),
    ("Makefile", "code"),
    ("voice_memo_0901", "media"),
    ("backup_20260831", "archives"),
];

/// Names that settle nothing about a category: a station that places one of these has
/// guessed, and the file would then be somewhere its owner did not put it and could not
/// tell was wrong.
const UNDECIDABLE: [&str; 6] = ["a1b2c3d4", "temp", "Untitled", "12", "final", "notes"];

/// What one way of asking came to, over both sets.
struct Tally {
    name: String,
    right: usize,
    declined_intended: usize,
    declined_undecidable: usize,
    placed_undecidable: Vec<String>,
    placed_floor: f32,
    undecided_ceiling: f32,
}

impl Tally {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            right: 0,
            declined_intended: 0,
            declined_undecidable: 0,
            placed_undecidable: Vec::new(),
            placed_floor: f32::INFINITY,
            undecided_ceiling: f32::NEG_INFINITY,
        }
    }

    fn line(&self) -> String {
        format!(
            "right {}/10, declined {} of them; of the names that settle nothing: declined \
             {}/6, placed {:?}; intended floor {:.3}, undecided ceiling {:.3}",
            self.right,
            self.declined_intended,
            self.declined_undecidable,
            self.placed_undecidable,
            self.placed_floor,
            self.undecided_ceiling
        )
    }
}

/// The winner, its score, and how far the runner-up was behind.
fn winner(scores: &[f32], labels: &[&str]) -> (String, f32, f32) {
    let mut ranked: Vec<(usize, f32)> = scores.iter().copied().enumerate().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    (
        labels[ranked[0].0].to_string(),
        ranked[0].1,
        ranked[0].1 - ranked[1].1,
    )
}

/// Ask with the name as the query and the bucket texts as the candidates: one call per
/// file, which is the shape a station would use.
fn name_against(
    scorer: &FastEmbedRerankerProvider,
    label: &str,
    texts: &[&str],
    with_decline: bool,
) -> Tally {
    let labels: Vec<&str> = BUCKETS
        .iter()
        .copied()
        .chain(with_decline.then_some("~decline"))
        .collect();
    let candidates: Vec<String> = texts
        .iter()
        .copied()
        .chain(with_decline.then_some(DECLINE))
        .map(str::to_string)
        .collect();
    let mut tally = Tally::new(label);
    for (name, expected) in DECIDABLE {
        let scores = scorer.scores_in_order(name, &candidates).expect("scored");
        let (top, score, _) = winner(&scores, &labels);
        println!("{name:40} -> {top:14} {score:8.3}");
        tally.right += usize::from(top == expected);
        tally.declined_intended += usize::from(top == "~decline");
        tally.placed_floor = tally.placed_floor.min(score);
    }
    for name in UNDECIDABLE {
        let scores = scorer.scores_in_order(name, &candidates).expect("scored");
        let (top, score, _) = winner(&scores, &labels);
        println!("{name:40} -> {top:14} {score:8.3}  (settles nothing)");
        if top == "~decline" {
            tally.declined_undecidable += 1;
        } else {
            tally.placed_undecidable.push(name.to_string());
        }
        tally.undecided_ceiling = tally.undecided_ceiling.max(score);
    }
    tally
}

/// Ask with the bucket text as the query and the name as the passage: seven questions per
/// file, winner takes all.
fn text_against(
    scorer: &FastEmbedRerankerProvider,
    label: &str,
    texts: &[&str],
    with_decline: bool,
) -> Tally {
    let labels: Vec<&str> = BUCKETS
        .iter()
        .copied()
        .chain(with_decline.then_some("~decline"))
        .collect();
    let queries: Vec<String> = texts
        .iter()
        .copied()
        .chain(with_decline.then_some(DECLINE))
        .map(str::to_string)
        .collect();
    let mut tally = Tally::new(label);
    for (name, expected) in DECIDABLE {
        let mut scores = Vec::with_capacity(queries.len());
        for query in &queries {
            scores.push(
                scorer
                    .scores_in_order(query, &[name.to_string()])
                    .expect("scored")[0],
            );
        }
        let (top, score, _) = winner(&scores, &labels);
        println!("{name:40} -> {top:14} {score:8.3}");
        tally.right += usize::from(top == expected);
        tally.declined_intended += usize::from(top == "~decline");
        tally.placed_floor = tally.placed_floor.min(score);
    }
    for name in UNDECIDABLE {
        let mut scores = Vec::with_capacity(queries.len());
        for query in &queries {
            scores.push(
                scorer
                    .scores_in_order(query, &[name.to_string()])
                    .expect("scored")[0],
            );
        }
        let (top, score, _) = winner(&scores, &labels);
        println!("{name:40} -> {top:14} {score:8.3}  (settles nothing)");
        if top == "~decline" {
            tally.declined_undecidable += 1;
        } else {
            tally.placed_undecidable.push(name.to_string());
        }
        tally.undecided_ceiling = tally.undecided_ceiling.max(score);
    }
    tally
}

#[test]
#[ignore = "needs the reranker weights in FASTEMBED_CACHE_DIR; see the module comment"]
fn what_a_local_model_can_settle_about_a_file_name() {
    let dir = std::env::var("FASTEMBED_CACHE_DIR").expect(
        "set FASTEMBED_CACHE_DIR to a directory the weights were fetched into \
         (deploy/scripts/fetch-model-weights.sh --model reranker)",
    );
    let scorer = FastEmbedRerankerProvider::try_new_with_cache_dir(Some(PathBuf::from(&dir)))
        .expect("the weights did not load from FASTEMBED_CACHE_DIR");

    // The control, and the only thing asserted here: a pair this model was trained for has
    // to separate, or the sweep below would be measuring a runtime that cannot load the
    // model while its numbers were read as "the task is impossible".
    let relevant = scorer
        .scores_in_order(
            "如何申请年假？",
            &["员工年假申请流程：填写申请表，由直属主管审批后交人事备案。".to_string()],
        )
        .expect("scored")[0];
    let irrelevant = scorer
        .scores_in_order(
            "如何申请年假？",
            &["公司食堂本周菜单：周一红烧肉，周二宫保鸡丁。".to_string()],
        )
        .expect("scored")[0];
    println!("\n== control\na relevant pair {relevant:.3}, an irrelevant one {irrelevant:.3}");
    assert!(
        relevant > irrelevant + 5.0,
        "the weights do not separate a pair they were trained for ({relevant:.3} vs \
         {irrelevant:.3}); nothing below would mean anything"
    );

    let texts: [(&str, &[&str]); 3] = [
        ("the bucket names", &BUCKETS),
        (
            "a taxonomy that says nothing about this folder",
            &DESCRIPTIONS,
        ),
        (
            "descriptions that name the files being tested",
            &DESCRIPTIONS_NAMING_THE_FILES,
        ),
    ];
    let mut tallies = Vec::new();
    for (label, set) in texts {
        for with_decline in [false, true] {
            println!(
                "\n== the name is the query, {label}, {} the decline candidate",
                if with_decline { "with" } else { "without" }
            );
            tallies.push(name_against(
                &scorer,
                &format!("name -> {label} (decline: {with_decline})"),
                set,
                with_decline,
            ));
        }
    }
    // Only for the honest taxonomy: the swapped shape costs seven calls per file, and its
    // result does not differ from the ones above in kind.
    for with_decline in [false, true] {
        println!(
            "\n== the description is the query, a taxonomy that says nothing about this \
             folder, {} the decline candidate",
            if with_decline { "with" } else { "without" }
        );
        tallies.push(text_against(
            &scorer,
            &format!("description -> name (decline: {with_decline})"),
            &DESCRIPTIONS,
            with_decline,
        ));
    }

    println!("\n== what each way of asking came to");
    for tally in &tallies {
        println!("{}\n   {}", tally.name, tally.line());
    }
}
