//! 审计哈希链篡改检测演示（杀手演示视频幕 5B）。
//!
//! 用法：
//!   audit_tamper_demo emit  <file>   生成 4 条审计事件 JSONL（对应视频叙事）
//!   audit_tamper_demo verify <file>  校验整链；篡改时红字报错并以退出码 1 结束
//!
//! 演示流程：emit → jq 查看 → sed 篡改一行 detail → verify 检出 first_broken_seq。

use cog_core::audit::{verify_chain, AuditEvent, AuditKind};

const GREEN: &str = "\x1b[1;32m";
const RED: &str = "\x1b[1;31m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

fn demo_chain() -> Vec<AuditEvent> {
    let mut events = Vec::new();
    let mut push = |kind, actor, target, action, detail| {
        let prev = events.last();
        events.push(AuditEvent::next(prev, kind, actor, target, action, detail));
    };
    push(
        AuditKind::ChangeOperation,
        "reflection-engine",
        "policy:demo.sla_policy",
        "policy.genesis",
        serde_json::json!({"version": "v1", "max_retries": 3, "timeout_secs": 30}),
    );
    push(
        AuditKind::ChangeOperation,
        "reflection-engine",
        "policy:demo.sla_policy",
        "policy.evaluate",
        serde_json::json!({"version": "v2", "verdict": "Reject", "z": -3.62, "uplift": "-37.5%"}),
    );
    push(
        AuditKind::AgentDecision,
        "reflection-engine",
        "policy:demo.sla_policy",
        "policy.evaluate",
        serde_json::json!({"version": "v3", "verdict": "Adopt", "z": 4.51, "uplift": "+45.0%"}),
    );
    push(
        AuditKind::Authz,
        "admin",
        "policy:demo.sla_policy",
        "change.approve",
        serde_json::json!({"version": "v3", "switched": true, "via": "takeover-console"}),
    );
    events
}

fn main() {
    let mut args = std::env::args().skip(1);
    let (cmd, path) = match (args.next(), args.next()) {
        (Some(c), Some(p)) => (c, p),
        _ => {
            eprintln!("usage: audit_tamper_demo <emit|verify> <file>");
            std::process::exit(2);
        }
    };

    match cmd.as_str() {
        "emit" => {
            let events = demo_chain();
            let mut out = String::new();
            for e in &events {
                out.push_str(&serde_json::to_string(e).unwrap());
                out.push('\n');
            }
            std::fs::write(&path, out).unwrap();
            println!("{DIM}wrote {} events to {path}{RESET}", events.len());
        }
        "verify" => {
            let text = std::fs::read_to_string(&path).unwrap();
            let events: Vec<AuditEvent> = text
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            let result = verify_chain(&events);
            if result.valid {
                println!(
                    "{GREEN}✓ CHAIN VALID{RESET} — {} records checked, hash chain intact",
                    result.records_checked
                );
            } else {
                println!(
                    "{RED}✗ TAMPER DETECTED{RESET} — chain broken at seq {}, only {} record(s) verifiable",
                    result.first_broken_seq.unwrap_or(0),
                    result.records_checked
                );
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("unknown command: {cmd}");
            std::process::exit(2);
        }
    }
}
