//! Judge the version contract on a checkout.
//!
//! The cluster's deployer judges the bare repository it tracks; this judges the
//! checkout a CI job has, calling the same reader, the same clauses and the same
//! gate. Two consumers, one implementation: a second reader written for the
//! second consumer is how the two would come to disagree about the same commit.
//!
//! The exit status is the verdict, so a workflow step needs no parsing: 0 when
//! the judgement passes, 1 when it fails, 2 when the arguments were wrong.
//!
//! usage: version_contract [--repo <git-dir>] [--rev <rev>] [--point <name>]...

use cog_core::contract::version::{judge, Clause, Verdict};
use cog_reflection::version_contract::{ci_failure, evidence_at};
use std::process::ExitCode;

/// Write a verdict as one line per finding, so a log makes the reason for a
/// failure readable without the exit code being looked up.
fn print_verdict(clause: Clause, verdict: &Verdict) {
    match verdict {
        Verdict::Satisfied => println!("{}: satisfied", clause.as_str()),
        Verdict::Unreadable(why) => println!("{}: unreadable ({})", clause.as_str(), why),
        Verdict::Violated(violations) => {
            for violation in violations {
                println!(
                    "{}: violated: {} -- {}",
                    clause.as_str(),
                    violation.subject,
                    violation.detail
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut repo = ".git".to_string();
    let mut rev = "HEAD".to_string();
    let mut points: Vec<String> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = match arg.as_str() {
            "--repo" | "--rev" | "--point" => args.next(),
            other => {
                eprintln!("unknown argument {other}");
                return ExitCode::from(2);
            }
        };
        let Some(value) = value else {
            eprintln!("{arg} needs a value");
            return ExitCode::from(2);
        };
        match arg.as_str() {
            "--repo" => repo = value,
            "--rev" => rev = value,
            _ => points.push(value),
        }
    }

    let (evidence, readings) = evidence_at(&repo, &rev, &points).await;
    let report = judge(&evidence);

    println!(
        "declared {} at {}",
        readings.declared.as_deref().unwrap_or("<none>"),
        rev
    );
    match &readings.nearest_release {
        Some((tag, distance)) => println!("nearest release {tag}, {distance} commits past it"),
        None => println!("nearest release <none>"),
    }
    // The premise the other clauses rest on, stated rather than left to be
    // inferred from a clause coming back unreadable.
    if !evidence.chain_complete {
        println!(
            "history: not read to its root, so no clause can be answered -- check out the full history (fetch-depth: 0)"
        );
    }
    for clause in Clause::ALL {
        print_verdict(clause, report.verdict(clause));
    }

    if ci_failure(&report, evidence.chain_complete) {
        println!("version_contract: failed");
        ExitCode::FAILURE
    } else {
        println!("version_contract: ok");
        ExitCode::SUCCESS
    }
}
