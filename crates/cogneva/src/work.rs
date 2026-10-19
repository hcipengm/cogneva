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

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use chrono::Utc;

use cog_core::{DagMessage, MessageBackend, StateBackend, Task, TaskDAG, TaskStatus};

pub const USAGE: &str = "\
cogneva work <SUBCOMMAND> --unit <unit> [OPTIONS]

Acts on the tasks the graph assigns to one unit. Every subcommand reads the
owner named on the task row; a task owned by another unit is refused.

SUBCOMMANDS:
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

OPTIONS:
    --unit <unit>          The unit this caller acts for (required).
    --task <id>            The task to act on (claim, start, complete, fail, release).
    --result <json|@file>  The result payload for complete; @file reads a file.
    --error <text>         The failure text for fail.
    --json                 Print raw task rows instead of a table (queue).
    --workspace <id>       Override dag_executor.workspace_id.
    --database-url <url>   Override the graph's database URL.
    --redis-url <url>      Override the message transport's Redis URL.";

/// What the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub action: Action,
    pub unit: String,
    pub workspace: Option<String>,
    pub database_url: Option<String>,
    pub redis_url: Option<String>,
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
    let mut json = false;
    let mut workspace = None;
    let mut database_url = None;
    let mut redis_url = None;

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
            "--workspace" => workspace = Some(value("--workspace")?),
            "--database-url" => database_url = Some(value("--database-url")?),
            "--redis-url" => redis_url = Some(value("--redis-url")?),
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
        Some(other) => return Err(format!("unknown work subcommand {other}")),
        None => return Err("work needs a subcommand".into()),
    };
    Ok(Request {
        action,
        unit,
        workspace,
        database_url,
        redis_url,
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
}

fn endpoints(request: &Request) -> Result<Endpoints, Box<dyn Error>> {
    let config = crate::config_loader::load();
    let database_url = request
        .database_url
        .clone()
        .or_else(|| std::env::var("COGNEVA_DATABASE_URL").ok())
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
    })
}

async fn run(request: Request) -> Result<(), Box<dyn Error>> {
    let endpoints = endpoints(&request)?;
    let pool = sqlx::PgPool::connect(&endpoints.database_url).await?;
    let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::PostgresStateBackend::new(pool));
    let unit = request.unit.as_str();
    let workspace = endpoints.workspace.as_str();

    match request.action {
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
    // The registry's own TTL is half a minute, which is shorter than the window
    // the idle rule reads over: the record would be gone before the rule could
    // see how long its owner had been quiet, and "quiet for ten minutes" and
    // "never registered" would both arrive as the same missing key. Keep the
    // record past that window and let the rule read a timestamp.
    let registry = cog_storage::redis::RedisAgentRegistry::from_url(redis_url)
        .await?
        .with_ttl_seconds(cog_core::OWNER_IDLE_AFTER_SECS as u64 * 3 / 2);
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "external-worker".into());
    let registration = cog_core::AgentRegistration::new(
        unit,
        hostname,
        "",
        "external-worker",
        workspace,
        Vec::new(),
        cog_core::ResourceInfo::default(),
    );
    use cog_core::AgentRegistry as _;
    registry.register(&registration).await?;
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
    let stream = format!("orchestrator:results:{}", endpoints.workspace);
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
