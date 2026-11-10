//! `cogneva principles` — 运行时原则账的一次性读写面。
//!
//! 账在共享数据库里，这个命令直接开它，不经过网关：调用方可能是宿主进程，
//! 手上没有平台令牌，而两套到达同一行的路径就是两处要跟着改的地方。它调用的
//! 是插件发布的那一份 store，读写与线上运行读到的是同一本账——不存在「命令行
//! 看到的原则」与「员工拿到的原则」两份答案。
//!
//! 写只有两种：提案，与属主拍板。没有「改一行」：改一条原则是提一条新的、批了
//! 再撤旧的（见 `cog_core::contract::runtime_principle` 的模块文档），所以账上
//! 每一行的文本都是某个时刻被某个人批准过的那一版。这条命令也照这个走：`retire`
//! 只撤一条已生效的，`approve` 只批一条等着拍的；不肯移动的时候它说清楚为什么，
//! 不假装成功。

use std::error::Error;
use std::sync::Arc;

use chrono::Utc;

use cog_core::{RuntimePrinciple, RuntimePrincipleState, RuntimePrincipleStore};
use cog_storage::PostgresRuntimePrincipleStore;

pub const USAGE: &str = "\
cogneva principles <SUBCOMMAND> [OPTIONS]

Reads and decides the rules the platform's employees work by. A rule is a
triple -- when it applies, what to look at, what to do -- and only the ones the
owner has approved are handed to a run.

SUBCOMMANDS:
    list [--state <s>]     Print the account, one entry per rule. --state keeps
                           only proposed, approved, rejected or retired.
    propose --slug <s> --trigger <text> --criterion <text> --action <text> --by <who>
                           File a rule. It is not in force until approved, and
                           its slug is never reused in place: an amendment is a
                           new slug whose predecessor is retired afterwards.
    approve <slug>         Approve a proposed rule; it is in force from the next
                           run on.
    reject <slug>          Reject a proposed rule. It stays on the account as
                           the record that it was proposed and not approved; a
                           new proposal under the same slug may follow.
    retire <slug>          Withdraw a rule that is in force.

OPTIONS:
    --database-url <url>   Override the account's URL. Default: COGNEVA_DATABASE_URL,
                           then the application configuration.";

/// What the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    List {
        state: Option<RuntimePrincipleState>,
    },
    Propose {
        slug: String,
        trigger: String,
        criterion: String,
        action: String,
        by: String,
    },
    Decide {
        slug: String,
        decision: RuntimePrincipleState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub action: Action,
    pub database_url: Option<String>,
}

/// Parse the arguments after `principles`.
///
/// Pure, so what a subcommand needs is testable without a database: the parse
/// is where "approve needs a slug" and "retire is not a proposal" are decided,
/// before any connection is opened.
pub fn parse_args(args: &[String]) -> Result<Request, String> {
    let mut database_url: Option<String> = None;
    let mut state: Option<RuntimePrincipleState> = None;
    let mut slug: Option<String> = None;
    let mut trigger: Option<String> = None;
    let mut criterion: Option<String> = None;
    let mut deed: Option<String> = None;
    let mut by: Option<String> = None;
    let mut rest: Vec<String> = Vec::new();

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut value = |flag: &str| -> Result<String, String> {
            iter.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--database-url" => database_url = Some(value("--database-url")?),
            "--state" => {
                let raw = value("--state")?;
                state = Some(
                    RuntimePrincipleState::parse(&raw)
                        .ok_or_else(|| format!("unknown principle state {raw:?}"))?,
                );
            }
            "--slug" => slug = Some(value("--slug")?),
            "--trigger" => trigger = Some(value("--trigger")?),
            "--criterion" => criterion = Some(value("--criterion")?),
            "--action" => deed = Some(value("--action")?),
            "--by" => by = Some(value("--by")?),
            other if other.starts_with("--") => return Err(format!("unknown option {other}")),
            other => rest.push(other.to_string()),
        }
    }

    let Some(subcommand) = rest.first().map(String::as_str) else {
        return Err("principles needs a subcommand".into());
    };
    let operands = &rest[1..];

    let action = match subcommand {
        "list" => {
            if let Some(extra) = operands.first() {
                return Err(format!("list takes no argument, got {extra:?}"));
            }
            if slug.is_some()
                || trigger.is_some()
                || criterion.is_some()
                || deed.is_some()
                || by.is_some()
            {
                return Err("list takes only --state; a rule's fields belong to propose".into());
            }
            Action::List { state }
        }
        "propose" => {
            if let Some(extra) = operands.first() {
                return Err(format!(
                    "propose takes its fields as options, got {extra:?}"
                ));
            }
            if state.is_some() {
                return Err("--state belongs to list".into());
            }
            Action::Propose {
                slug: slug.ok_or("propose needs --slug <text>")?,
                trigger: trigger.ok_or("propose needs --trigger <text>")?,
                criterion: criterion.ok_or("propose needs --criterion <text>")?,
                action: deed.ok_or("propose needs --action <text>")?,
                by: by.ok_or("propose needs --by <who>")?,
            }
        }
        "approve" | "reject" | "retire" => {
            let target = operands
                .first()
                .cloned()
                .ok_or_else(|| format!("{subcommand} needs a slug"))?;
            if let Some(extra) = operands.get(1) {
                return Err(format!("unexpected argument {extra:?}"));
            }
            if state.is_some()
                || slug.is_some()
                || trigger.is_some()
                || criterion.is_some()
                || deed.is_some()
                || by.is_some()
            {
                return Err(format!("{subcommand} takes only a slug"));
            }
            if target.starts_with("--") {
                return Err(format!(
                    "{subcommand} takes its slug as an argument, not {target}"
                ));
            }
            let decision = match subcommand {
                "approve" => RuntimePrincipleState::Approved,
                "reject" => RuntimePrincipleState::Rejected,
                _ => RuntimePrincipleState::Retired,
            };
            Action::Decide {
                slug: target,
                decision,
            }
        }
        other => return Err(format!("unknown principles subcommand {other}")),
    };

    Ok(Request {
        action,
        database_url,
    })
}

/// Run one principles command, printing what it read or changed.
pub async fn run_from_args() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let request = match parse_args(&args) {
        Ok(request) => request,
        Err(e) => {
            eprintln!("cogneva principles: {e}");
            eprintln!("{USAGE}");
            return Err(e.into());
        }
    };
    run(request).await
}

async fn run(request: Request) -> Result<(), Box<dyn Error>> {
    let config = crate::config_loader::load();
    let database_url = request
        .database_url
        .or_else(|| std::env::var(cog_core::env::DATABASE_URL).ok())
        .or_else(|| {
            config
                .providers
                .db
                .options
                .get("connection")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .or_else(|| {
            config
                .providers
                .pg
                .options
                .get("connection")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .ok_or(
            "the principle account's database is not configured: set COGNEVA_DATABASE_URL, \
             pass --database-url, or point providers.db.options.connection at it",
        )?;

    let pool = sqlx::PgPool::connect(&database_url).await?;
    let store: Arc<dyn RuntimePrincipleStore> = Arc::new(PostgresRuntimePrincipleStore::new(pool));

    match request.action {
        Action::List { state } => list(&*store, state).await?,
        Action::Propose {
            slug,
            trigger,
            criterion,
            action,
            by,
        } => {
            let principle =
                RuntimePrinciple::proposed(slug, trigger, criterion, action, by, Utc::now())?;
            store.propose(&principle).await?;
            println!(
                "proposed {}: not in force until the owner approves it",
                principle.slug
            );
        }
        Action::Decide { slug, decision } => {
            let moved = store.decide(&slug, decision, Utc::now()).await?;
            if !moved {
                return Err(match decision {
                    RuntimePrincipleState::Approved => format!(
                        "nothing moved: {slug:?} is not a proposal waiting to be approved \
                         (it may already be approved, rejected or unknown)"
                    ),
                    RuntimePrincipleState::Rejected => {
                        format!("nothing moved: {slug:?} is not a proposal waiting to be rejected")
                    }
                    _ => format!(
                        "nothing moved: {slug:?} is not a rule in force, so there is nothing \
                         to retire"
                    ),
                }
                .into());
            }
            println!(
                "{} {}",
                match decision {
                    RuntimePrincipleState::Approved => "approved",
                    RuntimePrincipleState::Rejected => "rejected",
                    _ => "retired",
                },
                slug
            );
        }
    }
    Ok(())
}

async fn list(
    store: &dyn RuntimePrincipleStore,
    state: Option<RuntimePrincipleState>,
) -> Result<(), Box<dyn Error>> {
    let rows = store.list(state).await?;
    if rows.is_empty() {
        println!(
            "no runtime principles {}",
            match state {
                Some(state) => format!("are {}", state.as_str()),
                None => "on the account yet".to_string(),
            }
        );
        return Ok(());
    }
    for row in rows {
        let decided = row
            .decided_at
            .map(|at| at.to_rfc3339())
            .unwrap_or_else(|| "undecided".into());
        println!(
            "[{}] {}  (used {}, by {}, proposed {}, decided {})",
            row.state.as_str(),
            row.slug,
            row.used,
            row.proposed_by,
            row.proposed_at.to_rfc3339(),
            decided
        );
        println!("  什么时候适用：{}", row.trigger);
        println!("  看什么：{}", row.criterion);
        println!("  做什么：{}", row.action);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Request, String> {
        parse_args(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
    }

    /// 三段各是一个自带值的选项：少一段就是少一段，不能靠默认值把空句存进账。
    #[test]
    fn a_proposal_carries_the_whole_triple_and_says_who_filed_it() {
        let request = parse(&[
            "propose",
            "--slug",
            "build-before-merge",
            "--trigger",
            "在合并之前",
            "--criterion",
            "构建是不是绿的",
            "--action",
            "红了就停下来修",
            "--by",
            "E0412",
        ])
        .expect("a complete proposal parses");
        assert_eq!(
            request.action,
            Action::Propose {
                slug: "build-before-merge".into(),
                trigger: "在合并之前".into(),
                criterion: "构建是不是绿的".into(),
                action: "红了就停下来修".into(),
                by: "E0412".into(),
            }
        );
        for missing in ["--trigger", "--criterion", "--action", "--by", "--slug"] {
            let mut args = vec![
                "propose",
                "--slug",
                "s",
                "--trigger",
                "t",
                "--criterion",
                "c",
                "--action",
                "a",
                "--by",
                "E0412",
            ];
            let at = args.iter().position(|arg| arg == &missing).unwrap();
            args.drain(at..at + 2);
            assert!(
                parse(&args).is_err(),
                "a proposal without {missing} has to be refused"
            );
        }
    }

    /// 三个动词是三次不同的拍板，读成一个动作会让「撤回」与「否决」在账上
    /// 变成同一条转换。
    #[test]
    fn each_decision_verb_maps_to_its_own_state() {
        for (verb, expected) in [
            ("approve", RuntimePrincipleState::Approved),
            ("reject", RuntimePrincipleState::Rejected),
            ("retire", RuntimePrincipleState::Retired),
        ] {
            let request = parse(&[verb, "build-before-merge"]).expect("a decision parses");
            assert_eq!(
                request.action,
                Action::Decide {
                    slug: "build-before-merge".into(),
                    decision: expected,
                }
            );
            assert!(parse(&[verb]).is_err(), "{verb} without a slug");
        }
    }

    /// `list` 的状态筛选用账上的拼法，认不出的拼法不当成「全部」。
    #[test]
    fn the_list_filter_uses_the_accounts_spelling() {
        assert_eq!(
            parse(&["list", "--state", "approved"]).map(|r| r.action),
            Ok(Action::List {
                state: Some(RuntimePrincipleState::Approved)
            })
        );
        assert_eq!(
            parse(&["list"]).map(|r| r.action),
            Ok(Action::List { state: None })
        );
        assert!(parse(&["list", "--state", "in-force"]).is_err());
    }

    /// 拼错的选项不能被当成没写：`--triger` 会让一条规则少一段而不报错。
    #[test]
    fn an_unknown_option_or_subcommand_is_refused_before_any_connection() {
        assert!(parse(&["list", "--stat", "approved"]).is_err());
        assert!(parse(&["propose", "--slug", "s"]).is_err());
        assert!(parse(&["approve"]).is_err());
        assert!(parse(&[]).is_err());
        assert!(parse(&["retire", "a", "b"]).is_err());
        assert!(parse(&["--slug", "x", "list"]).is_err());
    }

    /// 端点旗标在任何位置都读得到，并且不挤进子命令的操作数。
    #[test]
    fn the_database_url_can_appear_anywhere() {
        let request = parse(&["approve", "s", "--database-url", "postgres://db/cogneva"])
            .expect("the endpoint flag is read at this level");
        assert_eq!(
            request.database_url.as_deref(),
            Some("postgres://db/cogneva")
        );
        assert!(matches!(request.action, Action::Decide { .. }));
    }
}
