//! principle: feedback_an_observation_store_must_outlive_the_window_its_readers_compare_against
//! `cogneva work` — the out-of-band worker's side of the task graph.
//!
//! A worker that runs outside the platform process (a shell in a session, a
//! person at a terminal) is a first-class executor of the tasks the graph
//! assigns to its unit. It reads the rows the graph already holds and writes
//! only the fields a transition is allowed to write; the result is published on
//! the same results stream every in-process executor uses, so completion,
//! retries, dependents and the goal row all run through the one implementation
//! that owns them.
//!
//! Nothing here keeps a second copy of task state. The queue is a read of the
//! graph, every transition is a compare-and-swap guarded by the unit named on
//! the row, and a result is a message -- never a row written behind the
//! orchestrator's back. That last rule is what keeps the out-of-band path from
//! becoming a parallel state machine: if this command completed a task by
//! writing the row, dependents would not be scheduled and the goal row would
//! never learn it finished.
//!
//! The unit a caller acts for is asserted, not proved: the caller already has
//! direct access to the shared database, so a token would guard nothing it
//! could not already reach. What the guard buys is the accident it prevents --
//! a worker picking up or finishing work that belongs to another unit -- not
//! protection from a peer who deliberately names someone else's unit.
//!
//! The one subcommand that authors rather than transitions is `file`, and it
//! authors nothing itself: it hands the submission to the platform's
//! zero-credential submission face, which judges it with the same code that
//! judges the owner's own input. Having the database connection is not a
//! reason to write a row the door would have refused.

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use cog_core::{DagMessage, MessageBackend, StateBackend, Task, TaskDAG, TaskStatus};

pub const USAGE: &str = "\
cogneva work <SUBCOMMAND> --unit <unit> [OPTIONS]

Acts on the tasks the graph assigns to one unit. Every subcommand reads the
owner named on the task row; a task owned by another unit is refused.

SUBCOMMANDS:
    file    --task <id> --goal <text> --face <face> --acceptance <text>...
                           File this unit's own work through the platform's
                           submission door: the same handler the owner's input
                           goes through, so the row is bound to the face it
                           serves, refused if it states no acceptance, and
                           injected by the orchestrator like any other. The
                           publisher then holds it for this unit rather than
                           offering it to whoever polls first.
    queue                  List this unit's Pending, Scheduled and Running tasks.
    next                   Print the one task this unit can start right now.
    claim   --task <id>    Reserve a ready task (Pending -> Scheduled).
    start   --task <id>    Begin a task and take its lease (Running).
    heartbeat              Push the leases of this unit's Running tasks forward and
                           record the unit as alive where the face readings look.
    complete --task <id> [--result <json>]
                           Publish the task's result; the DAG completes it,
                           schedules dependents and settles the goal row.
    fail    --task <id> --error <text>
                           Publish the task's failure; the DAG decides whether
                           it retries, is cancelled or goes to the dead letter.
    release --task <id>    Put a claimed or started task back in line without
                           charging an attempt.
    inbox                  Print what other agents sent this unit, one message per
                           line as JSON, and acknowledge each after printing it.
                           Runs until interrupted; the unit stays registered as
                           alive for as long as it listens.

OPTIONS:
    --unit <unit>          The unit this caller acts for (required).
    --task <id>            The task to act on (claim, start, complete, fail, release),
                           or the id to file under (file).
    --goal <text>          What the work is, in one statement (file).
    --face <face>          The face the work serves (file); it must be staffed, and
                           by this unit.
    --acceptance <text>    What done means (file); repeat for several criteria.
    --priority <n>         Higher runs first (file; default 1).
    --blocked-by <id>      A task that must finish first (file); repeatable.
    --timeout-secs <n>     How long the row may stay open before the graph fails
                           it (file; default a week -- the lease, renewed by
                           heartbeat, is what says the holder is still there).
    --result <json|@file>  The result payload for complete; @file reads a file.
    --error <text>         The failure text for fail.
    --door-url <url>       The zero-credential submission face to file through
                           (file). Defaults to COGNEVA_MEMORY_API_BASE, then to
                           http://cogneva:<gateway.internal_port>.
    --json                 Print raw task rows instead of a table (queue).
    --workspace <id>       Override dag_executor.workspace_id.
    --database-url <url>   Override the graph's database URL.
    --redis-url <url>      Override the message transport's Redis URL.";

/// What the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    File {
        task: String,
        goal: String,
        face: String,
        acceptance: Vec<String>,
        priority: i32,
        blocked_by: Vec<String>,
        timeout_secs: u64,
    },
    Queue {
        json: bool,
    },
    Next {
        json: bool,
    },
    Claim {
        task: String,
    },
    Start {
        task: String,
    },
    Heartbeat,
    Complete {
        task: String,
        result: serde_json::Value,
    },
    Fail {
        task: String,
        error: String,
    },
    Release {
        task: String,
    },
    Inbox,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub action: Action,
    pub unit: String,
    pub workspace: Option<String>,
    pub database_url: Option<String>,
    pub redis_url: Option<String>,
    /// Where the submission door is, when the caller names it rather than
    /// letting the environment or the configured port decide.
    pub door_url: Option<String>,
}

/// Parse the arguments after `work`.
///
/// Lowered to a request without touching the environment so the accepted
/// shapes are testable: the surfaces below are the whole vocabulary an
/// out-of-band worker has, and a subcommand that quietly accepts a missing
/// operand would fail much later, as a transition that names no task.
pub fn parse_args(args: &[String]) -> Result<Request, String> {
    let mut sub: Option<&str> = None;
    let mut unit: Option<String> = None;
    let mut task: Option<String> = None;
    let mut result: Option<serde_json::Value> = None;
    let mut error: Option<String> = None;
    let mut goal: Option<String> = None;
    let mut face: Option<String> = None;
    let mut acceptance: Vec<String> = Vec::new();
    let mut priority: Option<i32> = None;
    let mut blocked_by: Vec<String> = Vec::new();
    let mut timeout_secs: Option<u64> = None;
    let mut json = false;
    let mut workspace = None;
    let mut database_url = None;
    let mut redis_url = None;
    let mut door_url = None;

    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        let mut value = |name: &str| -> Result<String, String> {
            index += 1;
            args.get(index)
                .cloned()
                .ok_or_else(|| format!("{name} needs a value"))
        };
        match arg {
            "--unit" => unit = Some(value("--unit")?),
            "--task" => task = Some(value("--task")?),
            "--result" => {
                let raw = value("--result")?;
                let text = match raw.strip_prefix('@') {
                    Some(path) => std::fs::read_to_string(path)
                        .map_err(|e| format!("cannot read {path}: {e}"))?,
                    None => raw,
                };
                result = Some(
                    serde_json::from_str(&text)
                        .map_err(|e| format!("--result is not JSON: {e}"))?,
                );
            }
            "--error" => error = Some(value("--error")?),
            "--goal" => goal = Some(value("--goal")?),
            "--face" => face = Some(value("--face")?),
            "--acceptance" => acceptance.push(value("--acceptance")?),
            "--blocked-by" => blocked_by.push(value("--blocked-by")?),
            "--priority" => {
                let raw = value("--priority")?;
                priority = Some(
                    raw.parse()
                        .map_err(|_| format!("--priority is not a number: {raw}"))?,
                );
            }
            "--timeout-secs" => {
                let raw = value("--timeout-secs")?;
                timeout_secs = Some(
                    raw.parse()
                        .map_err(|_| format!("--timeout-secs is not a number: {raw}"))?,
                );
            }
            "--workspace" => workspace = Some(value("--workspace")?),
            "--database-url" => database_url = Some(value("--database-url")?),
            "--redis-url" => redis_url = Some(value("--redis-url")?),
            "--door-url" => door_url = Some(value("--door-url")?),
            "--json" => json = true,
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other => {
                if sub.replace(other).is_some() {
                    return Err(format!("unexpected argument {other}"));
                }
            }
        }
        index += 1;
    }

    let unit = unit.ok_or("--unit is required")?;
    if unit.trim().is_empty() {
        return Err("--unit is empty".into());
    }
    let task = || task.clone().ok_or_else(|| "--task is required".to_string());
    let action = match sub {
        Some("file") => {
            // Missing is a parse error; present but made of blanks is a row
            // error, decided where the rest of the row is built.
            if acceptance.is_empty() {
                return Err("--acceptance is required for file".into());
            }
            Action::File {
                task: task()?,
                goal: goal.ok_or("--goal is required for file")?,
                face: face.ok_or("--face is required for file")?,
                acceptance,
                priority: priority.unwrap_or(1),
                blocked_by,
                timeout_secs: timeout_secs.unwrap_or(UNIT_WORK_BUDGET_SECS),
            }
        }
        Some("queue") => Action::Queue { json },
        Some("next") => Action::Next { json },
        Some("claim") => Action::Claim { task: task()? },
        Some("start") => Action::Start { task: task()? },
        Some("heartbeat") => Action::Heartbeat,
        Some("complete") => Action::Complete {
            task: task()?,
            // A completed task with no payload is legitimate work; the DAG
            // stores what it is given rather than inventing a result.
            result: result.unwrap_or(serde_json::Value::Null),
        },
        Some("fail") => Action::Fail {
            task: task()?,
            error: error.ok_or("--error is required for fail")?,
        },
        Some("release") => Action::Release { task: task()? },
        Some("inbox") => Action::Inbox,
        Some(other) => return Err(format!("unknown work subcommand {other}")),
        None => return Err("work needs a subcommand".into()),
    };
    Ok(Request {
        action,
        unit,
        workspace,
        database_url,
        redis_url,
        door_url,
    })
}

/// Run one work command.
pub async fn run_from_args() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let request = match parse_args(&args) {
        Ok(request) => request,
        Err(e) => {
            eprintln!("cogneva work: {e}");
            eprintln!("{USAGE}");
            return Err(e.into());
        }
    };
    run(request).await
}

/// Where the graph and the transport are, resolved once for every subcommand.
struct Endpoints {
    workspace: String,
    lease_secs: u64,
    database_url: String,
    redis_url: String,
    nats_urls: Vec<String>,
    /// The port the zero-credential face listens on, as this deployment is
    /// configured. 0 means it is not open.
    internal_port: u16,
}

fn endpoints(request: &Request) -> Result<Endpoints, Box<dyn Error>> {
    let config = crate::config_loader::load();
    let database_url = request
        .database_url
        .clone()
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
            "the task graph's database is not configured: set COGNEVA_DATABASE_URL, \
             pass --database-url, or point providers.db.options.connection at it",
        )?;
    let workspace = request
        .workspace
        .clone()
        .unwrap_or_else(|| config.dag_executor.workspace_id.clone());
    if workspace.trim().is_empty() {
        return Err("no workspace is configured: pass --workspace or set \
                    dag_executor.workspace_id"
            .into());
    }
    Ok(Endpoints {
        workspace,
        lease_secs: config.dag_executor.task_lease_secs,
        database_url,
        redis_url: request
            .redis_url
            .clone()
            .unwrap_or_else(|| config.dag_executor.redis_url.clone()),
        nats_urls: config.dag_executor.nats.urls.clone(),
        internal_port: config.gateway.internal_port,
    })
}

async fn run(request: Request) -> Result<(), Box<dyn Error>> {
    let endpoints = endpoints(&request)?;
    let pool = sqlx::PgPool::connect(&endpoints.database_url).await?;
    let backend: Arc<dyn StateBackend> =
        Arc::new(cog_storage::PostgresStateBackend::new(pool.clone()));
    let unit = request.unit.as_str();
    let workspace = endpoints.workspace.as_str();

    match request.action {
        Action::File {
            task,
            goal,
            face,
            acceptance,
            priority,
            blocked_by,
            timeout_secs,
        } => {
            let filing = Filing {
                id: &task,
                goal: &goal,
                face: &face,
                acceptance: &acceptance,
                priority,
                blocked_by: &blocked_by,
                timeout_secs,
            };
            let body = filing_body(&filing, unit, workspace)?;
            preflight(&*backend, workspace, &task, &blocked_by).await?;
            let door = door_base(
                request.door_url.as_deref(),
                std::env::var(cog_core::MEMORY_API_BASE_ENV).ok().as_deref(),
                endpoints.internal_port,
            )?;
            let accepted = submit(&door, &body).await?;
            let goal_id = accepted
                .get("message_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            match await_row(&*backend, workspace, &task).await? {
                Some(row) => println!(
                    "{} filed (face={}, owner={}, status={:?}, goal_id={})",
                    row.id, face, unit, row.status, goal_id
                ),
                None => println!(
                    "{} accepted by the submission door (goal_id={}) but not in the graph yet; \
                     it appears once the orchestrator injects it -- check with `cogneva work \
                     queue --unit {unit}`",
                    task, goal_id
                ),
            }
        }
        Action::Queue { json } => queue(&*backend, workspace, unit, json).await?,
        Action::Next { json } => next(&*backend, workspace, unit, json).await?,
        Action::Claim { task } => {
            let task = owned_task(&*backend, workspace, &task, unit).await?;
            let deps = dependency_state(&*backend, workspace).await?;
            if !task.is_ready(&deps) {
                return Err(format!(
                    "task {} is waiting on dependencies that are not completed",
                    task.id
                )
                .into());
            }
            transition(
                &*backend,
                workspace,
                &task.id,
                &[TaskStatus::Pending],
                unit,
                |t| t.status = TaskStatus::Scheduled,
            )
            .await?;
            println!("{} scheduled", task.id);
        }
        Action::Start { task } => {
            let task = owned_task(&*backend, workspace, &task, unit).await?;
            if task.status == TaskStatus::Pending {
                let deps = dependency_state(&*backend, workspace).await?;
                if !task.is_ready(&deps) {
                    return Err(format!(
                        "task {} is waiting on dependencies that are not completed",
                        task.id
                    )
                    .into());
                }
            }
            let lease = chrono::Duration::seconds(endpoints.lease_secs as i64);
            transition(
                &*backend,
                workspace,
                &task.id,
                &[TaskStatus::Pending, TaskStatus::Scheduled],
                unit,
                move |t| t.begin_run(unit, lease, Utc::now()),
            )
            .await?;
            println!("{} running", task.id);
        }
        Action::Heartbeat => {
            let renewed = heartbeat(&*backend, workspace, unit, endpoints.lease_secs).await?;
            // The leases say "this unit still holds these tasks". The registry
            // says "this unit is alive at all", which is the question the face
            // readings ask when a face's owner has no work in flight -- without
            // it a live worker with an empty queue is indistinguishable from a
            // worker that has died.
            presence(unit, workspace, &endpoints.redis_url).await?;
            println!("{renewed} leases renewed");
        }
        Action::Complete { task, result } => {
            let task = owned_task(&*backend, workspace, &task, unit).await?;
            require_running(&task)?;
            publish(
                &endpoints,
                DagMessage::TaskComplete {
                    message_id: message_id("res", &task.id),
                    timestamp: Utc::now(),
                    task_id: task.id.clone(),
                    result,
                    sender: unit.to_string(),
                    recipient: "dag-executor".into(),
                },
            )
            .await?;
            println!("{} result published", task.id);
        }
        Action::Fail { task, error } => {
            let task = owned_task(&*backend, workspace, &task, unit).await?;
            require_running(&task)?;
            publish(
                &endpoints,
                DagMessage::TaskFailed {
                    message_id: message_id("res", &task.id),
                    timestamp: Utc::now(),
                    task_id: task.id.clone(),
                    error,
                    error_cause: None,
                    retry_after_secs: None,
                    sender: unit.to_string(),
                    recipient: "dag-executor".into(),
                },
            )
            .await?;
            println!("{} failure published", task.id);
        }
        Action::Inbox => inbox(unit, &endpoints).await?,
        Action::Release { task } => {
            let task = owned_task(&*backend, workspace, &task, unit).await?;
            transition(
                &*backend,
                workspace,
                &task.id,
                &[TaskStatus::Scheduled, TaskStatus::Running],
                unit,
                |t| {
                    t.status = TaskStatus::Pending;
                    t.clear_run();
                },
            )
            .await?;
            println!("{} back in line", task.id);
        }
    }
    Ok(())
}

/// The kind a unit's own work is filed under.
///
/// Not a self-evolution kind. Those name the producers that detect work about
/// the platform -- a red CI run, a tracked issue, a gate's refusal -- and a
/// unit that decided what to do is not one of them: reading it as one would put
/// work the platform chose into the funnel that measures work the platform was
/// told about. The kind is what the census groups these rows by.
const UNIT_WORK_KIND: &str = "unit_work";

/// What the caller said about the work, as one value, so the builder cannot be
/// called with two operands swapped. The unit and the workspace are not part of
/// it: they come from the command, and a row that named a unit other than the
/// one filing it would be work addressed to somebody who never asked for it.
struct Filing<'a> {
    id: &'a str,
    goal: &'a str,
    face: &'a str,
    acceptance: &'a [String],
    priority: i32,
    blocked_by: &'a [String],
    timeout_secs: u64,
}

/// How long a filed row may stay open before the graph fails it, unless the
/// caller says otherwise.
///
/// The executor default is five minutes, which fits a dispatched unit of work
/// and not this one: an out-of-band worker holds its row across many
/// heartbeats, and the over-budget rule reads the age of the row rather than
/// the liveness of its holder -- so the default would fail work whose owner
/// was demonstrably alive at the time. Liveness is the lease's question, and
/// the heartbeat answers it; this budget answers "how long may this stay
/// open at all", and a week is longer than a unit of work is meant to take.
const UNIT_WORK_BUDGET_SECS: u64 = 7 * 24 * 60 * 60;

/// How long one filing request may take. The door answers as soon as it has
/// judged the submission; the decomposition it starts runs behind the answer.
const SUBMIT_TIMEOUT_SECS: u64 = 30;

/// How long to wait for the accepted row to show up in the graph, and how often
/// to look. Bounded on purpose: the row is usually there within a second, and a
/// caller that waits forever has no way to tell a slow injector from a
/// submission that was dropped.
const SUBMIT_POLL_ATTEMPTS: u32 = 20;
const SUBMIT_POLL_INTERVAL_MS: u64 = 500;

/// The submission this unit's own work is filed as.
///
/// The body is the one the platform's submission door already reads: a task
/// submission carrying a single verified row. Everything the row needs is a
/// field that door reads today -- the face and the unit it is bound to (checked
/// against the roster at the door), the criteria that say what done means
/// (carried onto the row by the door, and kept in `input` where the submitter
/// wrote them), the declared budget, and the blockers.
///
/// This command could have written the row straight into the graph -- it holds
/// the database connection either way. It does not, because a second writer
/// would have to re-implement every judgement the door makes (the roster
/// binding, the refusal of credential-shaped input, the acceptance requirement,
/// the quota check) and any one of those re-implementations silently drifting
/// would leave two kinds of task row in the same graph, judged differently.
fn filing_body(filing: &Filing, unit: &str, workspace: &str) -> Result<serde_json::Value, String> {
    let Filing {
        id,
        goal,
        face,
        acceptance,
        priority,
        blocked_by,
        timeout_secs,
    } = *filing;
    if id.trim().is_empty() {
        return Err("--task is empty".into());
    }
    if goal.trim().is_empty() {
        return Err("--goal is empty: a row nobody can read is a row nobody can pick up".into());
    }
    if face.trim().is_empty() {
        return Err("--face is empty: work is filed on the face it serves, or not at all".into());
    }
    // Blank entries are dropped the same way the door drops them: a list of
    // empty strings is a list that says nothing, and a dispatcher that only
    // asks whether the list is empty would take it as an answer.
    let stated: Vec<String> = acceptance
        .iter()
        .map(|criterion| criterion.trim())
        .filter(|criterion| !criterion.is_empty())
        .map(str::to_string)
        .collect();
    if stated.is_empty() {
        return Err(format!(
            "task {id} states no acceptance criterion; nothing downstream will invent one, \
             and a row filed without one sits on the board reading like work in progress. \
             Say what done means with --acceptance"
        ));
    }
    Ok(serde_json::json!({
        "goal": goal.trim(),
        "workspace_id": workspace,
        "intent_kind": "task",
        "tasks": [{
            "id": id,
            "task_type": UNIT_WORK_KIND,
            "input": { "goal": goal.trim(), "acceptance": stated },
            "blocked_by": blocked_by,
            "priority": priority,
            "face_id": face,
            "unit_id": unit,
            "timeout_seconds": timeout_secs,
            "planner": { "verified": true, "source": "user_provided" },
        }],
    }))
}

/// Where the submission door is.
///
/// The door is the zero-credential face the platform already runs for callers
/// that hold no token, and it is addressed the way every other caller of that
/// face addresses it: `COGNEVA_MEMORY_API_BASE` if the deployment named one,
/// otherwise the service name and the configured internal port. A deployment
/// that opens no internal face (port 0) has no door to file through, and says
/// so instead of failing at connect time with an address nobody meant.
fn door_base(
    override_url: Option<&str>,
    named_by_deployment: Option<&str>,
    internal_port: u16,
) -> Result<String, String> {
    let base = override_url
        .or(named_by_deployment)
        .map(str::to_string)
        .filter(|base| !base.trim().is_empty());
    if let Some(base) = base {
        return Ok(base.trim().trim_end_matches('/').to_string());
    }
    if internal_port == 0 {
        return Err(format!(
            "this deployment opens no zero-credential submission face (gateway.internal_port \
             is 0), so there is nowhere to file. Point {} at the face this deployment does \
             run, e.g. http://<service>:<port>",
            cog_core::MEMORY_API_BASE_ENV
        ));
    }
    Ok(format!("http://cogneva:{internal_port}"))
}

/// Put one filed row into the graph, through the platform's own submission door.
///
/// The door judges the submission with the same code that judges the owner's
/// own input: the row's face and unit are resolved against the roster, input
/// carrying a credential-shaped key is refused, a verified row without an
/// acceptance criterion is refused, quota is evaluated, and the accepted rows
/// go to the orchestrator's injection path. Nothing about that is re-decided
/// here -- this side only turns the arguments into the request the door reads
/// and reports what the door said.
async fn submit(door: &str, body: &serde_json::Value) -> Result<serde_json::Value, Box<dyn Error>> {
    let url = format!("{door}/api/v1/tasks");
    let response = reqwest::Client::new()
        .post(&url)
        .timeout(Duration::from_secs(SUBMIT_TIMEOUT_SECS))
        .json(body)
        .send()
        .await
        .map_err(|e| {
            format!(
                "could not reach the submission door at {url}: {e}. A call from outside the \
                 cluster has to be pointed at the service's cluster IP: set {} to \
                 http://<cluster-ip>:<internal-port>",
                cog_core::MEMORY_API_BASE_ENV
            )
        })?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        // The door's own words, not a paraphrase: a refusal names the operand
        // that was wrong, and restating it here would be a second copy of the
        // rule that could drift from the one that actually refused.
        return Err(format!("the submission door refused it with {status}: {text}").into());
    }
    serde_json::from_str(&text).map_err(|e| {
        format!("the submission door answered {status} with something unreadable: {e}: {text}")
            .into()
    })
}

/// Wait, for a bounded time, for the accepted row to appear in the graph.
///
/// The door answers 202 before the orchestrator has injected anything, so a
/// successful answer alone does not mean a row exists -- it means the request
/// passed every judgement. The difference matters to the caller: a row that
/// never appears is work that will never be offered, and printing "filed" on
/// the strength of the 202 would be exactly the reading this command exists to
/// avoid.
async fn await_row(
    backend: &dyn StateBackend,
    workspace: &str,
    id: &str,
) -> Result<Option<Task>, Box<dyn Error>> {
    for _ in 0..SUBMIT_POLL_ATTEMPTS {
        if let Some(row) = backend.dag_get_task(workspace, id).await? {
            return Ok(Some(row));
        }
        tokio::time::sleep(Duration::from_millis(SUBMIT_POLL_INTERVAL_MS)).await;
    }
    Ok(None)
}

/// The checks whose failure the door would not catch, done before the request.
///
/// Both are reads, not writes: a row under an id the graph already holds would
/// be dropped by the injector without a word, leaving the caller believing the
/// work was filed, and a blocker that is not in the graph never completes, so
/// the row would wait forever while reading as queued.
async fn preflight(
    backend: &dyn StateBackend,
    workspace: &str,
    id: &str,
    blocked_by: &[String],
) -> Result<(), Box<dyn Error>> {
    if let Some(held) = backend.dag_get_task(workspace, id).await? {
        return Err(format!(
            "task {id} is already in the graph as {:?} (owner {}); file under a different id, \
             or act on it with queue, next or claim",
            held.status,
            held.owner_unit.as_deref().unwrap_or("nobody")
        )
        .into());
    }
    for blocker in blocked_by {
        if backend.dag_get_task(workspace, blocker).await?.is_none() {
            return Err(format!(
                "task {id} waits on {blocker}, which is not in the graph; readiness is computed \
                 over rows that exist, so this row would wait forever"
            )
            .into());
        }
    }
    Ok(())
}

/// The task a caller named, provided this unit owns it.
async fn owned_task(
    backend: &dyn StateBackend,
    workspace: &str,
    task_id: &str,
    unit: &str,
) -> Result<Task, Box<dyn Error>> {
    let task = backend
        .dag_get_task(workspace, task_id)
        .await?
        .ok_or_else(|| format!("task {task_id} does not exist"))?;
    match task.owner_unit.as_deref() {
        Some(owner) if owner == unit && !task.is_executable => {
            Err(format!("task {task_id} is a graph node, not work; it has no body to run").into())
        }
        Some(owner) if owner == unit => Ok(task),
        Some(owner) => Err(format!("task {task_id} is owned by {owner}, not {unit}").into()),
        None => Err(format!(
            "task {task_id} names no owner; it runs on the shared queue, not from this command"
        )
        .into()),
    }
}

fn require_running(task: &Task) -> Result<(), Box<dyn Error>> {
    if task.status == TaskStatus::Running {
        return Ok(());
    }
    Err(format!(
        "task {} is {:?}, not running; start it before reporting a result",
        task.id, task.status
    )
    .into())
}

/// Every task in the graph, indexed for the readiness check.
async fn dependency_state(
    backend: &dyn StateBackend,
    workspace: &str,
) -> Result<TaskDAG, Box<dyn Error>> {
    let tasks = backend.dag_get_all_tasks(workspace).await?;
    let by_id: HashMap<String, Task> = tasks.into_iter().map(|t| (t.id.clone(), t)).collect();
    Ok(TaskDAG {
        goal: String::new(),
        tasks: by_id,
        dependencies: HashMap::new(),
    })
}

/// Apply one transition under the owner guard.
///
/// The row is re-read here rather than carried from the caller's earlier read:
/// between that read and this write another session may have moved the row,
/// and the guard can only refuse the stale state if it is checked against the
/// row as it stands now.
async fn transition<F>(
    backend: &dyn StateBackend,
    workspace: &str,
    task_id: &str,
    expected: &[TaskStatus],
    unit: &str,
    mutate: F,
) -> Result<(), Box<dyn Error>>
where
    F: FnOnce(&mut Task),
{
    let mut updated = backend
        .dag_get_task(workspace, task_id)
        .await?
        .ok_or_else(|| format!("task {task_id} does not exist"))?;
    mutate(&mut updated);
    updated.updated_at = Utc::now();
    backend
        .dag_transition_owned_task(workspace, &updated.id, expected, &updated, unit)
        .await?;
    Ok(())
}

/// Push every lease this unit holds forward.
///
/// Renewal is per task because the lease lives on the row: a worker that lost a
/// task to a reclaim has nothing to renew, and the count it prints is the
/// answer to "how much work does this unit still hold".
async fn heartbeat(
    backend: &dyn StateBackend,
    workspace: &str,
    unit: &str,
    lease_secs: u64,
) -> Result<usize, Box<dyn Error>> {
    let tasks = backend.dag_get_all_tasks(workspace).await?;
    let lease = chrono::Duration::seconds(lease_secs as i64);
    let mut renewed = 0usize;
    for task in tasks {
        if task.owner_unit.as_deref() != Some(unit) || task.status != TaskStatus::Running {
            continue;
        }
        if task.lease_owner.as_deref() != Some(unit) {
            // The row is running under a holder this unit cannot renew. That
            // is not a failure of the heartbeat: a lease that does not name
            // this unit is one this unit no longer answers for.
            continue;
        }
        let now = Utc::now();
        let mut updated = task;
        updated.lease_expires_at = Some(now + lease);
        updated.updated_at = now;
        backend
            .dag_transition_owned_task(
                workspace,
                &updated.id,
                &[TaskStatus::Running],
                &updated,
                unit,
            )
            .await?;
        renewed += 1;
    }
    Ok(renewed)
}

async fn queue(
    backend: &dyn StateBackend,
    workspace: &str,
    unit: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let tasks = backend.dag_get_all_tasks(workspace).await?;
    let mut mine: Vec<Task> = tasks
        .into_iter()
        .filter(|t| t.owner_unit.as_deref() == Some(unit))
        // Face nodes name their owner on the same field and are not work: the
        // roster is read with `cogneva governance`, not from the queue.
        .filter(|t| t.is_executable)
        .filter(|t| {
            matches!(
                t.status,
                TaskStatus::Pending | TaskStatus::Scheduled | TaskStatus::Running
            )
        })
        .collect();
    mine.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.updated_at.cmp(&b.updated_at))
    });
    if json {
        println!("{}", serde_json::to_string_pretty(&mine)?);
        return Ok(());
    }
    println!("task\tstatus\tface\ttype\tupdated_at");
    for task in &mine {
        println!(
            "{}\t{}\t{}\t{:?}\t{}",
            task.id,
            task.status.as_str(),
            task.face_id.as_deref().unwrap_or("-"),
            task.task_type,
            task.updated_at.to_rfc3339(),
        );
    }
    // The census is a note about the reading, not part of it: on stdout it
    // would make `--json` unparseable and a piped table one line longer than
    // the header promises.
    eprintln!(
        "# generated_at={} unit={} tasks={}",
        Utc::now().to_rfc3339(),
        unit,
        mine.len()
    );
    Ok(())
}

/// The one task this unit can start right now.
///
/// The shared queue never offers an owned task, so the readiness the publisher
/// would have applied is this command's to apply: without it a worker would
/// have to claim tasks one by one and learn from refusals which of them the
/// graph is not ready to run. The count of owned tasks rides along because
/// "nothing ready" and "nothing assigned" are different answers to "why is
/// this unit idle", and a bare empty line gives neither.
async fn next(
    backend: &dyn StateBackend,
    workspace: &str,
    unit: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let tasks = backend.dag_get_all_tasks(workspace).await?;
    let deps = dependency_state(backend, workspace).await?;
    let (ready, owned) = ready_owned(tasks, &deps, unit);
    match ready.first() {
        Some(task) if json => println!("{}", serde_json::to_string_pretty(task)?),
        Some(task) => println!("{}", task.id),
        None if json => println!("null"),
        None => {}
    }
    // Why this unit is idle is a note beside the answer, not the answer, so it
    // goes to stderr: stdout stays one task id (or `null`) that a caller can
    // read without stripping anything.
    eprintln!(
        "# generated_at={} unit={} ready={} owned={}",
        Utc::now().to_rfc3339(),
        unit,
        ready.len(),
        owned
    );
    Ok(())
}

/// How long a unit's presence outlives its last heartbeat.
///
/// The registry's own TTL is half a minute, which is shorter than the window
/// the idle rule reads over: the record would be gone before the rule could see
/// how long its owner had been quiet, and "quiet for ten minutes" and "never
/// registered" would both arrive as the same missing key. Keep the record past
/// that window and let the rule read a timestamp.
const UNIT_PRESENCE_TTL_SECS: u64 = cog_core::OWNER_IDLE_AFTER_SECS as u64 * 3 / 2;

/// Record that this unit is alive, under the name its faces name it by.
///
/// The registration is rewritten rather than renewed: the registry's TTL is
/// shorter than any interval a person would choose between heartbeats, so a
/// renewal can legitimately find nothing to renew, and that is not a failure of
/// this command. Registering is idempotent and carries the fresh timestamp the
/// reading looks at.
async fn presence(unit: &str, workspace: &str, redis_url: &str) -> Result<(), Box<dyn Error>> {
    if redis_url.trim().is_empty() {
        return Err("no heartbeat store is configured: pass --redis-url or set \
                    dag_executor.redis_url"
            .into());
    }
    let registry = cog_storage::redis::RedisAgentRegistry::from_url(redis_url)
        .await?
        .with_ttl_seconds(UNIT_PRESENCE_TTL_SECS);
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "external-worker".into());
    // The same name the staffing pass reads back: a unit that registers under
    // another role is present and is not somebody a face can be handed to, so
    // the writer and the predicate take the role from the same constant.
    let registration = cog_core::AgentRegistration::new(
        unit,
        hostname,
        "",
        cog_supervisor::registry::EXTERNAL_WORKER_ROLE,
        workspace,
        Vec::new(),
        cog_core::ResourceInfo::default(),
    );
    use cog_core::AgentRegistry as _;
    registry.register(&registration).await?;
    Ok(())
}

/// How often a listening unit re-states that it is alive.
///
/// Half of [`UNIT_PRESENCE_TTL_SECS`], so one missed renewal still leaves the
/// record standing: a unit that is listening is alive whatever its queue looks
/// like, and the face readings ask the registry rather than the queue.
const INBOX_PRESENCE_RENEWAL: Duration = Duration::from_secs(UNIT_PRESENCE_TTL_SECS / 2);

/// Read the messages other agents sent this unit, and acknowledge them.
///
/// The graph is one direction of this unit's traffic -- it asks for work and
/// answers with a result -- and the inbox is the other. Both exist, and a unit
/// that only ever pulled would be reachable as a name nobody can speak to: the
/// producer checks the registry before it publishes and refuses a name nobody
/// holds, so listening and registering are the same act here rather than two
/// steps a caller has to remember.
///
/// Registering matters for a second reason: the retirement pass deletes the
/// inbox of a unit the registry does not know, so a listener that never
/// registered would have the stream it is reading taken out from under it.
async fn inbox(unit: &str, endpoints: &Endpoints) -> Result<(), Box<dyn Error>> {
    let backend = transport(endpoints).await?;
    presence(unit, &endpoints.workspace, &endpoints.redis_url).await?;
    let renewal = {
        let unit = unit.to_string();
        let workspace = endpoints.workspace.clone();
        let redis_url = endpoints.redis_url.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(INBOX_PRESENCE_RENEWAL).await;
                if let Err(e) = presence(&unit, &workspace, &redis_url).await {
                    // Named rather than silent: a unit whose presence lapsed
                    // keeps reading, but nothing can be addressed to it, and
                    // that is not visible from inside the read loop.
                    eprintln!("# could not renew presence for {unit}: {e}");
                }
            }
        })
    };
    let consumer = cog_agent::AgentConsumer::new(unit, backend);
    let result = consumer
        .run(|message| async move {
            let line = serde_json::to_string(&message).map_err(cog_core::SFError::Serialization)?;
            // One line per message, printed before the ack: a reader that was
            // handed the message and then lost it would have been answered
            // with an acknowledgement it never earned.
            println!("{line}");
            Ok(())
        })
        .await;
    renewal.abort();
    result?;
    Ok(())
}

/// The owned tasks this unit can start now, and how many it owns at all.
///
/// Split out from the command so the filter is a function of the rows: the
/// readiness check is the whole reason this subcommand exists, and a check that
/// can only be exercised against a database is a check nobody runs.
fn ready_owned(tasks: Vec<Task>, deps: &TaskDAG, unit: &str) -> (Vec<Task>, usize) {
    let mut owned = 0usize;
    let mut ready: Vec<Task> = Vec::new();
    for task in tasks {
        if task.owner_unit.as_deref() != Some(unit) || !task.is_executable {
            continue;
        }
        owned += 1;
        if task.status == TaskStatus::Pending && task.is_ready(deps) {
            ready.push(task);
        }
    }
    ready.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.updated_at.cmp(&b.updated_at))
    });
    (ready, owned)
}

/// Publish one result message on the transport the platform's executors read.
///
/// The platform picks NATS when URLs are configured and falls back to Redis
/// when NATS cannot be reached; mirroring that here is what keeps a result from
/// landing on a stream nobody reads. A transport that cannot be opened is an
/// error rather than a silent in-memory drop: the bytes would be lost exactly
/// when the worker believes it reported.
async fn publish(endpoints: &Endpoints, message: DagMessage) -> Result<(), Box<dyn Error>> {
    let payload = serde_json::to_vec(&message)?;
    let stream = cog_core::task_result_stream(&endpoints.workspace);
    let backend = transport(endpoints).await?;
    backend.publish(&stream, &payload).await?;
    eprintln!("# published to {stream}");
    Ok(())
}

async fn transport(endpoints: &Endpoints) -> Result<Arc<dyn MessageBackend>, Box<dyn Error>> {
    if !endpoints.nats_urls.is_empty() {
        match cog_stream::NatsMessageBackend::new(&cog_core::NatsConfig {
            urls: endpoints.nats_urls.clone(),
            ..Default::default()
        })
        .await
        {
            Ok(backend) => return Ok(Arc::new(backend)),
            Err(e) => eprintln!(
                "cogneva work: NATS is configured but unreachable ({e}); publishing to Redis"
            ),
        }
    }
    if endpoints.redis_url.trim().is_empty() {
        return Err(
            "no message transport is configured: pass --redis-url or set \
                    dag_executor.redis_url"
                .into(),
        );
    }
    let backend = cog_stream::RedisMessageBackend::new(&endpoints.redis_url).await?;
    Ok(Arc::new(backend))
}

fn message_id(kind: &str, task_id: &str) -> String {
    format!("work-{kind}-{task_id}-{}", Utc::now().timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn every_subcommand_parses_with_its_operands() {
        let request = parse_args(&args(&["queue", "--unit", "8b"])).unwrap();
        assert_eq!(request.unit, "8b");
        assert_eq!(request.action, Action::Queue { json: false });

        let request = parse_args(&args(&["next", "--unit", "8b"])).unwrap();
        assert_eq!(request.action, Action::Next { json: false });

        let request = parse_args(&args(&["claim", "--unit", "8b", "--task", "t-1"])).unwrap();
        assert_eq!(request.action, Action::Claim { task: "t-1".into() });

        let request = parse_args(&args(&["heartbeat", "--unit", "8b"])).unwrap();
        assert_eq!(request.action, Action::Heartbeat);

        // Listening needs no task: the unit is the whole operand, and a
        // listener that had to name a task would have nothing to listen for.
        let request = parse_args(&args(&["inbox", "--unit", "8b"])).unwrap();
        assert_eq!(request.action, Action::Inbox);

        let request = parse_args(&args(&[
            "complete",
            "--unit",
            "8b",
            "--task",
            "t-1",
            "--result",
            "{\"ok\":true}",
        ]))
        .unwrap();
        assert_eq!(
            request.action,
            Action::Complete {
                task: "t-1".into(),
                result: serde_json::json!({"ok": true}),
            }
        );
    }

    #[test]
    fn a_listening_unit_renews_its_presence_before_the_record_lapses() {
        // The renewal is what keeps a listener addressable: a producer refuses
        // a name the registry does not hold, and the retirement pass deletes
        // the inbox of a unit it cannot find. A renewal at or beyond the
        // record's own lifetime would let both happen while the unit was
        // reading.
        assert!(
            INBOX_PRESENCE_RENEWAL < Duration::from_secs(UNIT_PRESENCE_TTL_SECS),
            "renewal {INBOX_PRESENCE_RENEWAL:?} must be inside the presence record's lifetime \
             of {UNIT_PRESENCE_TTL_SECS}s"
        );
    }

    #[test]
    fn a_missing_operand_is_refused_at_parse_time() {
        // Each of these would otherwise reach a transition that names no task.
        assert!(parse_args(&args(&["claim", "--unit", "8b"])).is_err());
        assert!(parse_args(&args(&["fail", "--unit", "8b", "--task", "t-1"])).is_err());
        assert!(parse_args(&args(&["queue"])).is_err());
        assert!(parse_args(&args(&[])).is_err());
        assert!(parse_args(&args(&["queue", "--unit", ""])).is_err());
        assert!(parse_args(&args(&["queue", "--unit", "8b", "--nope"])).is_err());
    }

    fn owned(id: &str, owner: &str) -> Task {
        let mut task = Task::new(id, cog_core::TaskType::DagNode, serde_json::json!({}));
        task.owner_unit = Some(owner.into());
        task
    }

    /// A task whose dependencies are not finished is the case the shared queue
    /// used to filter out. Carry that filter here or the worker starts work the
    /// graph is not ready to run -- and nothing in the reading would say why.
    #[test]
    fn a_blocked_task_is_not_offered_and_the_count_says_how_much_is_owned() {
        let mut dag = TaskDAG::default();
        let mut done = owned("dep-done", "8b");
        done.status = TaskStatus::Completed;
        let mut open = owned("dep-open", "8b");
        open.status = TaskStatus::Running;
        dag.tasks.insert(done.id.clone(), done);
        dag.tasks.insert(open.id.clone(), open);

        let mut ready = owned("ready", "8b");
        ready.blocked_by = vec!["dep-done".into()];
        ready.priority = 5;
        let mut blocked = owned("blocked", "8b");
        blocked.blocked_by = vec!["dep-open".into()];
        blocked.priority = 9;
        let mut other = owned("someone-elses", "6e");
        other.priority = 100;
        let mut face = owned("a-face", "8b");
        face.is_executable = false;

        let (picked, owned_count) = ready_owned(vec![ready, blocked, other, face], &dag, "8b");
        assert_eq!(
            picked.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            vec!["ready"],
            "the blocked task ranks higher but cannot run yet"
        );
        assert_eq!(
            owned_count, 2,
            "the face node and another unit's task are not this unit's work"
        );
    }

    /// The idle rule answers "how long has this owner been quiet" from a
    /// timestamp. If the record expires sooner than the rule's window, that
    /// question loses its answer before it can be asked and every quiet owner
    /// reads the same as one that was never registered.
    #[test]
    fn a_units_presence_outlives_the_window_the_idle_rule_reads_over() {
        assert!(
            UNIT_PRESENCE_TTL_SECS > cog_core::OWNER_IDLE_AFTER_SECS as u64,
            "presence TTL {UNIT_PRESENCE_TTL_SECS}s expires before the idle window \
             ({OWNER_IDLE_AFTER_SECS}s) can be observed",
            OWNER_IDLE_AFTER_SECS = cog_core::OWNER_IDLE_AFTER_SECS
        );
    }

    /// Higher priority first, then the one that has been waiting longest.
    #[test]
    fn ready_tasks_are_offered_in_priority_order() {
        let dag = TaskDAG::default();
        let mut low = owned("low", "8b");
        low.priority = 1;
        let mut high = owned("high", "8b");
        high.priority = 9;
        let (picked, _) = ready_owned(vec![low, high], &dag, "8b");
        assert_eq!(picked[0].id, "high");
    }

    fn file_args() -> Vec<String> {
        args(&[
            "file",
            "--unit",
            "8b",
            "--task",
            "close-the-ci-gap",
            "--goal",
            "Close the gap on the ci-build-deploy face",
            "--face",
            "ci-build-deploy",
            "--acceptance",
            "CI is green on main",
        ])
    }

    /// The same invocation with one flag and its value taken out.
    fn file_without(flag: &str) -> Vec<String> {
        let mut items = file_args();
        let at = items
            .iter()
            .position(|item| item == flag)
            .unwrap_or_else(|| panic!("{flag} is not in the fixture"));
        items.drain(at..=at + 1);
        items
    }

    #[test]
    fn file_parses_and_refuses_each_operand_it_goes_on_to_need() {
        let request = parse_args(&file_args()).unwrap();
        assert_eq!(
            request.action,
            Action::File {
                task: "close-the-ci-gap".into(),
                goal: "Close the gap on the ci-build-deploy face".into(),
                face: "ci-build-deploy".into(),
                acceptance: vec!["CI is green on main".into()],
                priority: 1,
                blocked_by: Vec::new(),
                timeout_secs: UNIT_WORK_BUDGET_SECS,
            }
        );
        // Each of these would otherwise reach a row that says less than a row
        // has to: no face means an owner the roster cannot be asked about, no
        // goal means nothing a reader could act on, no criterion means nothing
        // that says when it is done, and no id means a row nobody can name.
        for flag in ["--task", "--goal", "--face", "--acceptance"] {
            assert!(
                parse_args(&file_without(flag)).is_err(),
                "file accepted a missing {flag}"
            );
        }
        let mut bad_priority = file_args();
        bad_priority.push("--priority".into());
        bad_priority.push("high".into());
        assert!(parse_args(&bad_priority).is_err());
    }

    /// What this command sends has to be a submission the door accepts and
    /// turns into the row its readers expect.
    ///
    /// Every field asserted here is one the door copies onto the row: the face
    /// and the unit it is bound to (resolved against the roster at the door),
    /// the criteria that say what done means (carried onto the row, and left in
    /// `input` where a reader shows a task's own words), the declared budget,
    /// the blockers and the verified claim. A submission that dropped any of
    /// them would be accepted as something else than what the caller meant.
    #[test]
    fn the_filing_is_a_submission_the_door_turns_into_the_row() {
        let request = parse_args(&file_args()).unwrap();
        let Action::File {
            task,
            goal,
            face,
            acceptance,
            priority,
            blocked_by,
            timeout_secs,
        } = request.action
        else {
            panic!("file must parse as file");
        };
        let filing = Filing {
            id: &task,
            goal: &goal,
            face: &face,
            acceptance: &acceptance,
            priority,
            blocked_by: &blocked_by,
            timeout_secs,
        };
        let body = filing_body(&filing, &request.unit, "cogneva").unwrap();

        assert_eq!(
            body["intent_kind"], "task",
            "a goal submission is decomposed; this one is already decided"
        );
        assert_eq!(body["workspace_id"], "cogneva");
        let rows = body["tasks"].as_array().expect("one row");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row["id"], "close-the-ci-gap");
        assert_eq!(row["task_type"], UNIT_WORK_KIND);
        assert_eq!(row["face_id"], "ci-build-deploy");
        assert_eq!(row["unit_id"], "8b");
        assert_eq!(row["priority"], 1);
        assert_eq!(row["timeout_seconds"], UNIT_WORK_BUDGET_SECS);
        assert_eq!(
            row["planner"]["verified"], true,
            "without the verified claim the orchestrator plans the row again"
        );
        assert_eq!(
            row["input"]["acceptance"][0], "CI is green on main",
            "a verified row states what done means, or the door refuses it"
        );
        assert_eq!(row["input"]["goal"], goal);
    }

    /// The body has to be read by the door as what this command meant.
    ///
    /// Both sides spell the submission in JSON, and serde ignores a key it does
    /// not know: renaming `face_id` on either side would leave the request
    /// valid, the row unbound, and the drift reading to report the mismatch
    /// later. Deserialising into the door's own type is what holds the two
    /// spellings together -- a field either arrives under the name the door
    /// reads or it does not arrive at all.
    #[test]
    fn the_door_reads_the_filing_as_the_task_it_was_meant_to_be() {
        use cog_gateway::tasks::{CreateTaskRequest, IntentKind};

        let request = parse_args(&file_args()).unwrap();
        let Action::File {
            task,
            goal,
            face,
            acceptance,
            priority,
            blocked_by,
            timeout_secs,
        } = request.action
        else {
            panic!("file must parse as file");
        };
        let filing = Filing {
            id: &task,
            goal: &goal,
            face: &face,
            acceptance: &acceptance,
            priority,
            blocked_by: &blocked_by,
            timeout_secs,
        };
        let body = filing_body(&filing, &request.unit, "cogneva").unwrap();
        let read: CreateTaskRequest =
            serde_json::from_value(body).expect("the door's own type must accept this body");

        assert_eq!(read.intent_kind, IntentKind::Task);
        assert_eq!(read.workspace_id, "cogneva");
        let rows = read.tasks.expect("one row");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "close-the-ci-gap");
        assert_eq!(rows[0].face_id.as_deref(), Some("ci-build-deploy"));
        assert_eq!(rows[0].unit_id.as_deref(), Some("8b"));
        assert_eq!(rows[0].timeout_seconds, Some(UNIT_WORK_BUDGET_SECS));
        assert!(
            rows[0].planner.as_ref().is_some_and(|claim| claim.verified),
            "the row is injected without decomposition only if this claim arrives"
        );
    }

    /// A criterion made of blanks is a list that says nothing. Filed as it
    /// arrives, it would pass every "is there an acceptance" test downstream
    /// while stating nothing at all.
    fn filing<'a>(id: &'a str, goal: &'a str, acceptance: &'a [String]) -> Filing<'a> {
        Filing {
            id,
            goal,
            face: "ci-build-deploy",
            acceptance,
            priority: 1,
            blocked_by: &[],
            timeout_secs: UNIT_WORK_BUDGET_SECS,
        }
    }

    #[test]
    fn a_blank_acceptance_criterion_is_not_an_answer() {
        let nobody: Vec<String> = Vec::new();
        assert!(
            filing_body(&filing("t", "g", &nobody), "8b", "ws").is_err(),
            "no criterion at all"
        );
        let blanks = vec!["   ".to_string(), String::new()];
        assert!(filing_body(&filing("t", "g", &blanks), "8b", "ws").is_err());
        let stated = vec!["a".to_string()];
        assert!(filing_body(&filing("  ", "g", &stated), "8b", "ws").is_err());
        assert!(filing_body(&filing("t", " ", &stated), "8b", "ws").is_err());
    }

    /// The executor's own default is five minutes, and the over-budget rule
    /// reads the age of the row rather than whether its holder is still there.
    /// A filed row left on that default would fail under a unit that was
    /// heartbeating at the time -- the row's own unit, on work it just started.
    #[test]
    fn a_filed_row_outlives_a_five_minute_executor_run() {
        let stated = vec!["CI is green on main".to_string()];
        let body = filing_body(&filing("t", "g", &stated), "8b", "ws").unwrap();
        let stated_budget = body["tasks"][0]["timeout_seconds"]
            .as_u64()
            .expect("the budget travels with the submission");
        assert!(
            stated_budget > 300,
            "a filed row left on the executor's budget fails while its owner is alive"
        );
        assert_eq!(stated_budget, UNIT_WORK_BUDGET_SECS);
    }

    /// The door is addressed the way every other caller of that face addresses
    /// it, and a deployment that opens no such face says so instead of failing
    /// at connect time with an address nobody meant.
    #[test]
    fn the_door_is_addressed_by_the_deployment_name_or_the_port() {
        assert_eq!(
            door_base(Some("http://10.0.0.5:8085/"), None, 8085).unwrap(),
            "http://10.0.0.5:8085",
            "an explicit address wins, without its trailing slash"
        );
        assert_eq!(
            door_base(None, Some("http://cogneva:8085"), 9999).unwrap(),
            "http://cogneva:8085",
            "the name the deployment gave this face wins over the default"
        );
        assert_eq!(door_base(None, None, 8085).unwrap(), "http://cogneva:8085");
        let refused = door_base(None, None, 0).unwrap_err();
        assert!(
            refused.contains(cog_core::MEMORY_API_BASE_ENV),
            "a deployment with no face to file through must name the setting that points at one: {refused}"
        );
    }

    /// `--door-url` is an operand of `file`, not a free-floating flag: it must
    /// reach the request, and it must not become a way to smear the subcommand.
    #[test]
    fn file_takes_the_door_address_as_an_operand() {
        let mut items = file_args();
        items.push("--door-url".into());
        items.push("http://10.0.0.5:8085".into());
        let request = parse_args(&items).unwrap();
        assert_eq!(request.door_url.as_deref(), Some("http://10.0.0.5:8085"));
        items.pop();
        assert!(
            parse_args(&items).is_err(),
            "--door-url without its value must not parse"
        );
    }

    #[test]
    fn a_result_may_be_empty_but_must_be_json() {
        let request = parse_args(&args(&["complete", "--unit", "8b", "--task", "t-1"])).unwrap();
        assert_eq!(
            request.action,
            Action::Complete {
                task: "t-1".into(),
                result: serde_json::Value::Null,
            }
        );
        assert!(parse_args(&args(&[
            "complete", "--unit", "8b", "--task", "t-1", "--result", "not json",
        ]))
        .is_err());
    }
}
