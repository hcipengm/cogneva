//! principle: feedback_a_report_written_is_not_a_report_delivered
//!
//! A sub-agent that wrote its report and never handed it over has not produced
//! nothing. The text is in the output the actor itself wrote; what is missing is
//! the delivery envelope, and the difference between the two is the difference
//! between "salvage the report" and "ask the role to try again".
//!
//! This gate reads the published parse path — `squad::pge::parse_generator_output`,
//! the one place a role's raw output becomes the content the pipeline carries —
//! with literal actor outputs, so the property is pinned without a live model, a
//! bus or a database. Three readings, because the rule only holds if all three
//! stay apart:
//!
//! * delivered: the report arrives under the answer contract's own fields and is
//!   read as the generation (the control — without it the two cases below could
//!   pass by everything being inert);
//! * written but not delivered: no `content`, no `artifacts`, no diff — only the
//!   actor's own text, which is what a delivery call that never landed leaves
//!   behind. The report must still be recoverable, and the output must not read
//!   as "this role produced nothing";
//! * never written: a blank content and no artifacts, the one shape that earns
//!   the empty-generation cause. A written report must never be filed there.

use cog_collaboration::squad::pge::parse_generator_output;
use cog_collaboration::squad::pge::types::StopCause;
use cog_core::contract::outcome::EMPTY_GENERATION_PREFIX;
use serde_json::json;

/// The report a sub-agent wrote before its delivery call failed to land.
///
/// Kept free of the phrases a deterministic-cause reader matches on
/// (transport errors, upstream refusals, a spent iteration budget): this test is
/// about a report that is *there*, and a body that reads like a failure notice
/// would make the salvage indistinguishable from the cause it is not.
const REPORT: &str =
    "## 结论\n\n报告写完了：三个文件都改过，测试全绿。\n交付工具没调通，正文留在这里。";

/// A line of the report, as the reader of a salvaged output would look for it.
const REPORT_LINE: &str = "报告写完了";

#[test]
fn a_delivered_report_is_read_as_the_generation() {
    let delivered = parse_generator_output(&json!({
        "content": { "report": REPORT },
        "artifacts": [
            { "name": "report.md", "artifact_type": "report", "content": REPORT }
        ],
    }));

    assert!(
        !delivered.is_empty_envelope(),
        "a delivered report is a generation, not an empty envelope"
    );
    assert_eq!(
        delivered.content["report"],
        json!(REPORT),
        "the content half of the answer contract is read verbatim"
    );
    assert_eq!(
        delivered.artifacts.len(),
        1,
        "the artifact half of the answer contract survives as one deliverable"
    );
    assert!(
        delivered.terminal_env_failure_reason().is_none(),
        "a run that delivered names no terminal cause"
    );
}

#[test]
fn a_written_report_survives_a_delivery_that_never_landed() {
    // Two shapes of "the delivery never arrived": the runtime's `{ "result": … }`
    // fallback, which is what a thought holding no deliverable JSON comes back
    // as, and the bare text of a role that answered with nothing but its report.
    // Neither carries `content`, `artifacts` or a diff.
    for written in [json!({ "result": REPORT }), json!(REPORT)] {
        let output = parse_generator_output(&written);

        assert!(
            !output.is_empty_envelope(),
            "a written report is not an empty envelope, got: {output:?}"
        );
        assert!(
            output.content.to_string().contains(REPORT_LINE),
            "the report has to be recoverable from the actor's own output, got: {output:?}"
        );
        assert!(
            output.artifacts.is_empty(),
            "no delivery landed, so no artifact can have either: {output:?}"
        );
    }
}

#[test]
fn an_output_with_no_report_reads_as_the_empty_generation() {
    let nothing = parse_generator_output(&json!({ "content": "", "artifacts": [] }));

    assert!(
        nothing.is_empty_envelope(),
        "an envelope carrying neither content nor artifacts is the empty generation, got: {nothing:?}"
    );
    // The cause it earns, named where the round composes it. A written report
    // must not land here: that is the whole distinction this file carries.
    let empty_cause = StopCause::EmptyEnvelope.reason();
    assert!(
        empty_cause.starts_with(EMPTY_GENERATION_PREFIX),
        "the empty-envelope cause has to name itself, got: {empty_cause}"
    );

    let written = parse_generator_output(&json!({ "result": REPORT }));
    assert!(
        !written.is_empty_envelope(),
        "'wrote it and did not hand it over' must not read like 'never wrote it'"
    );
}
