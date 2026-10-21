//! `cogneva governance` — the host's one-shot view of the face graph.
//!
//! The faces, their owners and their verdict are not kept anywhere of their
//! own: they are nodes in the task graph, and the graph is in the shared
//! database. This command opens that database directly rather than going
//! through the gateway, for the same reason `write-role` does — the caller may
//! be a host process with no platform token, and two ways to reach the same
//! row would be two places to keep in step. It calls the same functions the
//! resident loop calls, so there is no second reading and no second truth.
//!
//! It never writes a verdict down. `faces` and `drift` compute from the graph
//! at the moment they run, and print that moment with the answer; `project`
//! writes the same computation to files, stamped with when it was taken.
//!
//! The one write is `import-faces`, which takes the roster file and puts each
//! row into the graph through the same import the HTTP face uses. A row whose
//! ownership has already moved past the file's first revision keeps its
//! ownership: the file says what ownership was proposed, the graph says what
//! was signed. With `--keep-ownership` no existing row's ownership moves at
//! all, which is the mode a clock-driven writer runs in.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};

use cog_core::{
    Face, FaceImportOutcome, FaceKind, FaceSpec, GovernanceStore, StateBackend, Task, TaskStatus,
    TaskType,
};
use cog_storage::{PostgresGovernanceStore, PostgresStateBackend, RedisAgentRegistry};

/// Columns of a roster row, in the order the file writes them.
const ROSTER_COLUMNS: usize = 13;

pub const USAGE: &str = "\
cogneva governance <SUBCOMMAND> [OPTIONS]

Reads and seeds the face graph. Faces are non-executable task nodes, so every
answer below is computed from the task graph at the moment it is asked for.

SUBCOMMANDS:
    faces [--brief]        Print every face with its owner and staffing.
                           --brief prints one line per face, for a reader that
                           only needs owner and kind.
    drift [--json]         Print the current verdict of every face. Reads the
                           workers' heartbeats too; an owner nobody can see
                           reads unread, never green.
    project --out <dir>    Write faces.tsv, drift.tsv, decisions.tsv and
                           tasks.tsv into <dir>, each with its generated_at and
                           the graph's largest revision. This is the write a
                           freshness gate reads.
    import-faces --from <file> [--keep-ownership]
                           Put each roster row into the graph. Columns:
                           face kind owner deputy witness time_share
                           vacancy_until vacancy_assigner gap_task first_reading
                           entry_path updated_at source_fronts
                           Idempotent; printed counts say what happened.
                           --keep-ownership refreshes descriptions and never
                           moves an existing face: the continuous writer uses
                           it, because a clock re-reading a file is not a
                           decision to hand a face over, and only a signed
                           handover carries the work bound to the face.
    handover <SUB>         Open, sign, apply, withdraw or list a move of a face
                           from one unit to another. A move counts only when
                           every required unit signed and it was applied.
    vote <SUB>             Open, cast, withdraw, close or list a decision.
                           Ballot rules are written at open time and never
                           rewritten; a principle that settles the question
                           records its basis and closes with no votes.
    assign [--face <id>] [--workspace <id>]
                           Give every staffed face one standing work row,
                           owned by the face's owner and carrying its face id,
                           so the owner's queue is that row and the reading
                           can tell an idle face from a face nobody gave work
                           to. Idempotent per face: an open row is kept.

OPTIONS:
    --database-url <url>   Override the graph's URL. Default: COGNEVA_DATABASE_URL,
                           then the application configuration.
    --redis-url <url>      Override the heartbeat store's URL. Default: the
                           application configuration (dag_executor.redis_url,
                           which COGNEVA_REDIS_URL overrides). Without it every
                           staffed face reads unread.";

/// What the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Faces {
        brief: bool,
    },
    Drift {
        json: bool,
    },
    Project {
        out: PathBuf,
    },
    ImportFaces {
        from: PathBuf,
        /// Refresh descriptions without moving any face's ownership. The
        /// continuous writer asks for this: a clock re-reading a file is not a
        /// decision to hand a face over, and the only act that carries the
        /// work bound to a face is a signed handover.
        keep_ownership: bool,
    },
    Assign {
        face: Option<String>,
        workspace: Option<String>,
    },
    // Boxed: a handover carries a spec whose text fields make it the widest
    // variant, and every other action would pay for that width. One CLI
    // process builds exactly one of these, so the extra allocation costs
    // nothing a caller can observe.
    Act(Box<crate::governance_acts::Act>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub action: Action,
    pub database_url: Option<String>,
    pub redis_url: Option<String>,
}

/// Parse the arguments after `governance`.
///
/// Pure so that what a subcommand needs is testable without a database.
pub fn parse_args(args: &[String]) -> Result<Request, String> {
    // The two families that act on the graph rather than read it have their
    // own grammar: one extra word and repeatable options. The endpoint flags
    // belong to this level, so they are taken out before the delegate sees the
    // rest.
    if matches!(args.first().map(String::as_str), Some("handover" | "vote")) {
        let mut rest: Vec<String> = Vec::new();
        let mut database_url: Option<String> = None;
        let mut redis_url: Option<String> = None;
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--database-url" => {
                    database_url = Some(iter.next().ok_or("--database-url needs a value")?.clone());
                }
                "--redis-url" => {
                    redis_url = Some(iter.next().ok_or("--redis-url needs a value")?.clone());
                }
                other => rest.push(other.to_string()),
            }
        }
        return Ok(Request {
            action: Action::Act(Box::new(crate::governance_acts::parse_args(&rest)?)),
            database_url,
            redis_url,
        });
    }
    let mut database_url: Option<String> = None;
    let mut redis_url: Option<String> = None;
    let mut brief = false;
    let mut json = false;
    let mut keep_ownership = false;
    let mut out: Option<PathBuf> = None;
    let mut from: Option<PathBuf> = None;
    let mut face: Option<String> = None;
    let mut workspace: Option<String> = None;

    let mut subcommand: Option<&str> = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--database-url" => {
                database_url = Some(iter.next().ok_or("--database-url needs a value")?.clone());
            }
            "--redis-url" => {
                redis_url = Some(iter.next().ok_or("--redis-url needs a value")?.clone());
            }
            "--brief" => brief = true,
            "--json" => json = true,
            "--keep-ownership" => keep_ownership = true,
            "--out" => {
                out = Some(PathBuf::from(iter.next().ok_or("--out needs a directory")?));
            }
            "--from" => {
                from = Some(PathBuf::from(iter.next().ok_or("--from needs a file")?));
            }
            "--face" => {
                face = Some(iter.next().ok_or("--face needs a face id")?.clone());
            }
            "--workspace" => {
                workspace = Some(iter.next().ok_or("--workspace needs an id")?.clone());
            }
            other if other.starts_with('-') => return Err(format!("unknown option {other}")),
            other => {
                if subcommand.replace(other).is_some() {
                    return Err(format!("unexpected argument {other}"));
                }
            }
        }
    }

    let action = match subcommand {
        Some("faces") => Action::Faces { brief },
        Some("drift") => Action::Drift { json },
        Some("project") => Action::Project {
            out: out.ok_or("project needs --out <dir>")?,
        },
        Some("import-faces") => Action::ImportFaces {
            from: from.ok_or("import-faces needs --from <file>")?,
            keep_ownership,
        },
        Some("assign") => Action::Assign { face, workspace },
        Some(other) => return Err(format!("unknown governance subcommand {other}")),
        None => return Err("governance needs a subcommand".into()),
    };
    Ok(Request {
        action,
        database_url,
        redis_url,
    })
}

/// Run one governance command, printing what it read.
pub async fn run_from_args() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let request = match parse_args(&args) {
        Ok(request) => request,
        Err(e) => {
            eprintln!("cogneva governance: {e}");
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
    let redis_url = request.redis_url.or_else(|| {
        let url = config.dag_executor.redis_url.trim();
        (!url.is_empty()).then(|| url.to_string())
    });

    let pool = sqlx::PgPool::connect(&database_url).await?;
    let assign_pool = pool.clone();
    let store: Arc<dyn GovernanceStore> = Arc::new(PostgresGovernanceStore::new(pool));

    match request.action {
        Action::Faces { brief } => faces(&*store, brief).await?,
        Action::Drift { json } => drift(&*store, redis_url.as_deref(), json).await?,
        Action::Project { out } => project(&*store, redis_url.as_deref(), &out).await?,
        Action::ImportFaces {
            from,
            keep_ownership,
        } => import_faces(&*store, &from, keep_ownership).await?,
        Action::Assign { face, workspace } => {
            let workspace = workspace.unwrap_or_else(|| config.dag_executor.workspace_id.clone());
            assign(&*store, &assign_pool, &workspace, face.as_deref()).await?;
        }
        Action::Act(act) => {
            // The heartbeat store is what an exit handover's apply step
            // re-reads. Without it the command still works for every handover
            // whose signatures are all human; only the one that replaced a
            // signature with an absence has nothing to check it against.
            let (registry, note) = agent_registry(redis_url.as_deref()).await;
            if let Some(note) = note {
                eprintln!("cogneva governance: {note}");
            }
            let probe = registry.map(RedisPresenceProbe);
            crate::governance_acts::run_with_presence(
                *act,
                &*store,
                probe
                    .as_ref()
                    .map(|p| p as &dyn crate::governance_acts::PresenceProbe),
                Utc::now(),
            )
            .await?
        }
    }
    Ok(())
}

async fn faces(store: &dyn GovernanceStore, brief: bool) -> Result<(), Box<dyn Error>> {
    let now = Utc::now();
    let faces: Vec<Face> = store
        .face_nodes()
        .await?
        .iter()
        .filter_map(Face::from_task)
        .collect();
    let census = cog_core::census(&faces, now);
    if brief {
        // A named vacancy and an unowned face both print `-` in the owner
        // column. The two columns after it are what tells a reader which one
        // this is: a deadline and the unit responsible for filling it, or two
        // dashes meaning nobody has said anybody will.
        for face in &faces {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                face.face_id,
                face.spec.kind.as_str(),
                face.owner_unit.as_deref().unwrap_or("-"),
                face.spec
                    .vacancy_until
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_else(|| "-".into()),
                face.spec.vacancy_assigner.as_deref().unwrap_or("-"),
            );
        }
    } else {
        println!("face\tkind\towner\tdeputy\twitness\tstaffed\trevision");
        for face in &faces {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                face.face_id,
                face.spec.kind.as_str(),
                face.owner_unit.as_deref().unwrap_or("-"),
                face.spec.deputy_unit.as_deref().unwrap_or("-"),
                face.spec.witness_unit.as_deref().unwrap_or("-"),
                if face.is_staffed() { "yes" } else { "no" },
                face.revision()
            );
        }
    }
    println!(
        "# generated_at={} faces={} cross={} unstaffed={} expired={}",
        now.to_rfc3339(),
        census.faces,
        census.cross,
        census.unstaffed,
        census.expired
    );
    Ok(())
}

/// Open the heartbeat store, or say why there is none.
///
/// No registry is not fatal: the rules then read every staffed face as unread,
/// which is the honest answer to "nobody can see the owner". Printing green
/// instead would make an unreachable registry look like a healthy fleet.
/// The registry read as a liveness question rather than a roster lookup.
///
/// The store's TTL codes are translated here, once: negative means the key is
/// not there, and a key that is there means the unit is renewing it.
struct RedisPresenceProbe(Arc<RedisAgentRegistry>);

#[async_trait::async_trait]
impl crate::governance_acts::PresenceProbe for RedisPresenceProbe {
    async fn presence_ttl_secs(&self, unit: &str) -> Result<Option<u64>, String> {
        match self.0.ttl_secs(unit).await {
            Ok(-2) => Ok(None),
            Ok(-1) => Ok(Some(u64::MAX)),
            Ok(ttl) if ttl >= 0 => Ok(Some(ttl as u64)),
            Ok(other) => Err(format!(
                "the heartbeat store answered ttl {other} for {unit}, which is neither a \
                 live nor a missing key"
            )),
            Err(e) => Err(e.to_string()),
        }
    }
}

async fn agent_registry(url: Option<&str>) -> (Option<Arc<RedisAgentRegistry>>, Option<String>) {
    let Some(url) = url else {
        return (
            None,
            Some("no heartbeat store is configured; owners read unread".into()),
        );
    };
    match RedisAgentRegistry::from_url(url).await {
        Ok(registry) => (Some(Arc::new(registry)), None),
        Err(e) => (
            None,
            Some(format!(
                "the heartbeat store could not be opened; owners read unread: {e}"
            )),
        ),
    }
}

async fn readings(
    store: &dyn GovernanceStore,
    redis_url: Option<&str>,
) -> Result<Vec<cog_core::FaceDriftReading>, Box<dyn Error>> {
    let (registry, note) = agent_registry(redis_url).await;
    if let Some(note) = note {
        eprintln!("cogneva governance: {note}");
    }
    let agents = registry
        .as_ref()
        .map(|r| r.as_ref() as &dyn cog_core::AgentRegistry);
    Ok(cog_core::read_drift(store, agents, Utc::now()).await?)
}

async fn drift(
    store: &dyn GovernanceStore,
    redis_url: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let readings = readings(store, redis_url).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&readings)?);
        return Ok(());
    }
    println!("face\tstate\tred\tyellow\tdetail");
    for reading in &readings {
        println!(
            "{}\t{}\t{}\t{}\t{}",
            reading.face_id,
            reading.state.as_str(),
            join_or_dash(&reading.red_rules),
            join_or_dash(&reading.yellow_rules),
            reading.detail.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

fn join_or_dash(items: &[String]) -> String {
    if items.is_empty() {
        "-".into()
    } else {
        items.join(",")
    }
}

/// The projection a freshness gate reads.
///
/// Two header lines carry when the file was taken and which revision of the
/// graph it saw. A file without them cannot be told apart from a stale one, so
/// the gate reads their absence as unread rather than as old-but-fine.
async fn project(
    store: &dyn GovernanceStore,
    redis_url: Option<&str>,
    out: &Path,
) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(out)?;
    let now = Utc::now();
    let nodes = store.face_nodes().await?;
    let faces: Vec<Face> = nodes.iter().filter_map(Face::from_task).collect();
    let revision = faces.iter().map(Face::revision).max().unwrap_or(0);
    let stamp = format!(
        "# generated_at: {}\n# source_revision: {}\n",
        now.to_rfc3339(),
        revision
    );

    let mut faces_tsv = String::from(&stamp);
    // A vacancy is a claim with three parts: by when, who fills it, and the
    // task expected to close it. A projection that carried only the deadline
    // would let a reader -- and every gate reading this file -- mistake a bare
    // date for a declared vacancy, which is exactly the difference between a
    // staffed face and `no_owner`.
    faces_tsv.push_str(
        "face\tkind\towner\tdeputy\twitness\tvacancy_until\tvacancy_assigner\tgap_task\trevision\n",
    );
    for face in &faces {
        faces_tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            face.face_id,
            face.spec.kind.as_str(),
            face.owner_unit.as_deref().unwrap_or("-"),
            face.spec.deputy_unit.as_deref().unwrap_or("-"),
            face.spec.witness_unit.as_deref().unwrap_or("-"),
            face.spec
                .vacancy_until
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "-".into()),
            face.spec.vacancy_assigner.as_deref().unwrap_or("-"),
            face.spec.gap_task_id.as_deref().unwrap_or("-"),
            face.revision(),
        ));
    }

    let readings = readings(store, redis_url).await?;
    let mut drift_tsv = String::from(&stamp);
    drift_tsv.push_str("face\tstate\tred\tyellow\tdetail\n");
    for reading in &readings {
        drift_tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\n",
            reading.face_id,
            reading.state.as_str(),
            join_or_dash(&reading.red_rules),
            join_or_dash(&reading.yellow_rules),
            reading.detail.as_deref().unwrap_or("-"),
        ));
    }

    // Decisions are rows like the faces, so the same projection carries them:
    // a reader can see which questions the ledger holds, how each was settled,
    // and -- for a backfilled one -- that its rules were not predeclared.
    let decisions: Vec<cog_core::Decision> = store
        .governance_nodes(cog_core::DECISION_TASK_TYPE)
        .await?
        .iter()
        .filter_map(cog_core::Decision::from_task)
        .collect();
    let mut decisions_tsv = String::from(&stamp);
    decisions_tsv.push_str(
        "decision\tkind\tmode\tstate\tsubject\tresult\tresult_reason\tdeadline\tprovenance\tpredeclared\n",
    );
    for decision in &decisions {
        let spec = &decision.spec;
        decisions_tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            decision.id,
            spec.kind,
            spec.mode.as_str(),
            spec.state.as_str(),
            spec.subject.replace('\t', " "),
            spec.result_option.as_deref().unwrap_or("-"),
            spec.result_reason
                .as_deref()
                .map(|reason| reason.replace('\t', " "))
                .unwrap_or_else(|| "-".into()),
            spec.deadline
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "-".into()),
            spec.provenance.as_deref().unwrap_or("-"),
            spec.predeclared,
        ));
    }

    // The open work bound to a face. This is what makes "the owner is on it"
    // checkable from the host without asking the database twice.
    let open_tasks = store.open_face_bound_tasks().await?;
    let mut tasks_tsv = String::from(&stamp);
    tasks_tsv.push_str("task\tface\towner\tstatus\tworkspace\n");
    for task in &open_tasks {
        tasks_tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\n",
            task.id,
            task.face_id.as_deref().unwrap_or("-"),
            task.owner_unit.as_deref().unwrap_or("-"),
            task.status.as_str(),
            task.workspace_id.as_deref().unwrap_or("-"),
        ));
    }

    std::fs::write(out.join("faces.tsv"), faces_tsv)?;
    std::fs::write(out.join("drift.tsv"), drift_tsv)?;
    std::fs::write(out.join("decisions.tsv"), decisions_tsv)?;
    std::fs::write(out.join("tasks.tsv"), tasks_tsv)?;
    println!(
        "wrote faces.tsv, drift.tsv, decisions.tsv and tasks.tsv at {} \
         (faces={}, decisions={}, open_tasks={}, source_revision={})",
        now.to_rfc3339(),
        faces.len(),
        decisions.len(),
        open_tasks.len(),
        revision
    );
    Ok(())
}

/// One parsed roster row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RosterRow {
    pub face: Face,
}

/// Parse the roster file.
///
/// Comment and blank lines are skipped. A row with the wrong width is an error
/// rather than a row with the missing fields left empty: an empty owner is a
/// declared vacancy in this format, so a short row would turn a typo into a
/// face nobody owns.
pub fn parse_roster(text: &str) -> Result<Vec<RosterRow>, String> {
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let columns: Vec<&str> = line.split('\t').collect();
        if columns.len() != ROSTER_COLUMNS {
            return Err(format!(
                "line {}: expected {ROSTER_COLUMNS} columns, found {}",
                index + 1,
                columns.len()
            ));
        }
        let field = |i: usize| -> Option<String> {
            let value = columns[i].trim();
            (!value.is_empty() && value != "-").then(|| value.to_string())
        };
        let kind = FaceKind::parse(columns[1].trim())
            .ok_or_else(|| format!("line {}: unknown kind {}", index + 1, columns[1]))?;
        let vacancy_until = match field(6) {
            Some(raw) => Some(
                DateTime::parse_from_rfc3339(&raw)
                    .map_err(|e| format!("line {}: vacancy_until: {e}", index + 1))?
                    .with_timezone(&Utc),
            ),
            None => None,
        };
        let face_id = columns[0].trim().to_string();
        if face_id.is_empty() {
            return Err(format!("line {}: no face name", index + 1));
        }
        rows.push(RosterRow {
            face: Face {
                spec: FaceSpec {
                    display_name: face_id.clone(),
                    kind,
                    deputy_unit: field(3),
                    witness_unit: field(4),
                    time_share: field(5),
                    vacancy_until,
                    vacancy_assigner: field(7),
                    gap_task_id: field(8),
                    first_reading: field(9),
                    entry_path: field(10),
                    legacy_front_id: None,
                    source_fronts: field(12),
                    revision: 1,
                },
                owner_unit: field(2),
                // A roster describes proposed ownership; the signed handover
                // that would justify a move is a separate row.
                bound_by_handover_id: None,
                face_id,
                // The file's own timestamp says when its content was written;
                // the node's is set by the graph when it is written.
                updated_at: Utc::now(),
            },
        });
    }
    Ok(rows)
}

/// Give staffed faces one standing work row each.
///
/// A face reading can only tell "idle" from "never handed any work" if there is
/// a row to look at, and the owner's queue can only answer what it should do
/// next if the duty is a task like every other duty. The row is executable and
/// owned, so the shared ready queue leaves it alone and only the named unit can
/// claim it; it is one row per face rather than one per day, because a face's
/// duty does not expire at midnight.
///
/// The id is derived from the face, so running this twice keeps the open row
/// instead of blaming somebody for not having started the second copy.
async fn assign(
    store: &dyn GovernanceStore,
    pool: &sqlx::PgPool,
    workspace: &str,
    only: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    if workspace.trim().is_empty() {
        return Err("no workspace to assign work in: pass --workspace or set \
                    dag_executor.workspace_id"
            .into());
    }
    let backend = PostgresStateBackend::new(pool.clone());
    let all: Vec<Face> = store
        .face_nodes()
        .await?
        .iter()
        .filter_map(Face::from_task)
        .collect();
    let selected: Vec<Face> = match only {
        Some(id) => all.into_iter().filter(|face| face.face_id == id).collect(),
        None => all,
    };
    if selected.is_empty() {
        return Err(match only {
            Some(id) => format!("face {id} is not in the graph; import the roster first"),
            None => "the graph holds no faces; import the roster first".to_string(),
        }
        .into());
    }
    let mut assigned = 0usize;
    let mut kept = 0usize;
    let mut vacant = 0usize;
    for face in selected {
        let Some(owner) = face.owner_unit.clone() else {
            vacant += 1;
            eprintln!(
                "{}: declared vacancy; a vacancy cannot hold work until somebody owns the face",
                face.face_id
            );
            continue;
        };
        let id = cog_orchestrator::governance_acts::face_work_id(&face.face_id);
        if let Some(existing) = backend.dag_get_task(workspace, &id).await? {
            if !matches!(
                existing.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            ) {
                kept += 1;
                continue;
            }
        }
        let reading = face
            .spec
            .first_reading
            .as_deref()
            .unwrap_or("本面的判据读数");
        let gap = face.spec.gap_task_id.as_deref().unwrap_or("-");
        let goal = format!(
            "以生产商用为唯一目标，保持「{}」这一面在目标态。\n\
             本轮判据读数：{}（必须有新的读取时刻与值，只刷新 updated_at 不算）。\n\
             缺口任务：{}。\n\
             完成条件：读数可读、任务归属与面一致；不属于本面的工作转给该面的 owner。",
            face.face_id, reading, gap
        );
        let mut task = Task::new(
            id.clone(),
            TaskType::DagNode,
            serde_json::json!({
                "goal": goal,
                "face": face.face_id,
                "owner_unit": owner,
            }),
        );
        task.face_id = Some(face.face_id.clone());
        task.owner_unit = Some(owner.clone());
        task.workspace_id = Some(workspace.to_string());
        backend.dag_set_task(workspace, &id, &task).await?;
        assigned += 1;
    }
    println!("assigned {assigned}, kept {kept}, vacant {vacant} (workspace {workspace})");
    Ok(())
}

async fn import_faces(
    store: &dyn GovernanceStore,
    from: &Path,
    keep_ownership: bool,
) -> Result<(), Box<dyn Error>> {
    let text = std::fs::read_to_string(from)
        .map_err(|e| format!("cannot read {}: {e}", from.display()))?;
    let rows = parse_roster(&text)?;
    let ownership = if keep_ownership {
        cog_core::ImportOwnership::HandoverOnly
    } else {
        cog_core::ImportOwnership::FromFile
    };
    let mut inserted = 0;
    let mut updated = 0;
    let mut unchanged = 0;
    let mut kept = 0;
    for row in &rows {
        match cog_core::import_face_with(store, &row.face, ownership).await? {
            FaceImportOutcome::Inserted => inserted += 1,
            FaceImportOutcome::Updated => updated += 1,
            FaceImportOutcome::Unchanged => unchanged += 1,
            FaceImportOutcome::OwnershipKept { revision } => {
                kept += 1;
                println!(
                    "{}: ownership kept at revision {revision}; the file describes an older move",
                    row.face.face_id
                );
            }
        }
    }
    println!(
        "imported {} rows: inserted={inserted} updated={updated} unchanged={unchanged} \
         ownership_kept={kept}",
        rows.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn each_subcommand_parses_to_its_own_action() {
        assert_eq!(
            parse_args(&args(&["faces", "--brief"])).unwrap().action,
            Action::Faces { brief: true }
        );
        assert_eq!(
            parse_args(&args(&["drift", "--json"])).unwrap().action,
            Action::Drift { json: true }
        );
        assert_eq!(
            parse_args(&args(&["project", "--out", "/tmp/g"]))
                .unwrap()
                .action,
            Action::Project {
                out: PathBuf::from("/tmp/g")
            }
        );
        assert_eq!(
            parse_args(&args(&["import-faces", "--from", "faces.tsv"]))
                .unwrap()
                .action,
            Action::ImportFaces {
                from: PathBuf::from("faces.tsv"),
                keep_ownership: false,
            }
        );
        assert_eq!(
            parse_args(&args(&[
                "import-faces",
                "--from",
                "faces.tsv",
                "--keep-ownership"
            ]))
            .unwrap()
            .action,
            Action::ImportFaces {
                from: PathBuf::from("faces.tsv"),
                keep_ownership: true,
            },
            "the continuous writer refreshes descriptions and never moves a face"
        );
        assert_eq!(
            parse_args(&args(&["assign"])).unwrap().action,
            Action::Assign {
                face: None,
                workspace: None
            }
        );
        assert_eq!(
            parse_args(&args(&["assign", "--face", "通信", "--workspace", "ws"]))
                .unwrap()
                .action,
            Action::Assign {
                face: Some("通信".into()),
                workspace: Some("ws".into())
            }
        );
    }

    #[test]
    fn a_subcommand_missing_its_operand_is_refused_rather_than_defaulted() {
        assert!(parse_args(&args(&["project"])).is_err());
        assert!(parse_args(&args(&["import-faces"])).is_err());
        assert!(parse_args(&args(&["faces", "--out"])).is_err());
        assert!(parse_args(&args(&[])).is_err());
        assert!(parse_args(&args(&["no-such-face"])).is_err());
    }

    #[test]
    fn a_roster_row_becomes_a_face_with_a_declared_vacancy() {
        let text = "\
# comment
安全与凭证\tface\t\t-\t38\t-\t2026-10-20T14:16:00+08:00\toperator\tSEC-BASELINE\tcredential_surface_live\t-\t2026-10-19T14:55:00+08:00\t-
";
        let rows = parse_roster(text).unwrap();
        assert_eq!(rows.len(), 1);
        let face = &rows[0].face;
        assert_eq!(face.face_id, "安全与凭证");
        assert_eq!(face.owner_unit, None);
        assert!(face.spec.has_named_vacancy());
        assert_eq!(face.spec.gap_task_id.as_deref(), Some("SEC-BASELINE"));
        assert_eq!(face.spec.revision, 1);
    }

    #[test]
    fn a_short_row_is_an_error_not_a_face_nobody_owns() {
        let text = "任务管理\tface\toperator\n";
        let err = parse_roster(text).unwrap_err();
        assert!(err.contains("expected 13 columns"), "{err}");
    }

    #[test]
    fn an_unknown_kind_is_refused() {
        let line = ["-"; 11].join("\t");
        let text = format!("某面\tline\t{line}\n");
        assert!(parse_roster(&text).is_err());
    }

    /// 没有在岗账时的形状必须是「读不到」而不是「没人」：调用方拿 None 就说不出
    /// 谁在岗，而说「没人」会把「账没接上」读成「十几个人都擅离职守」。
    #[tokio::test]
    async fn without_a_heartbeat_store_the_reason_says_unread_rather_than_nobody() {
        let (registry, reason) = agent_registry(None).await;
        assert!(registry.is_none());
        let reason = reason.expect("缺在岗账时必须留下理由，否则调用方只能猜");
        assert!(
            reason.contains("unread"),
            "理由要自报「读成 unread」，实际是 {reason}"
        );
        assert!(
            !reason.contains("nobody") && !reason.contains("no one"),
            "理由不能读成「没人在岗」：{reason}"
        );
    }

    /// 与上一条同源的另一支：配了地址但连不上。两支的判词必须同时守住，
    /// 否则只改一支的词不会红——而这两句话在读数上长得一样。
    ///
    /// 127.0.0.1:1 是必然被拒的地址（内核立刻回 ECONNREFUSED），所以这条不依赖
    /// 任何外部服务，也不碰凭证。
    #[tokio::test]
    async fn an_unreachable_heartbeat_store_also_reads_unread_rather_than_nobody() {
        let (registry, reason) = agent_registry(Some("redis://127.0.0.1:1/")).await;
        assert!(registry.is_none());
        let reason = reason.expect("连不上时必须留下理由，否则调用方只能猜");
        assert!(
            reason.contains("unread"),
            "理由要自报「读成 unread」，实际是 {reason}"
        );
        assert!(
            !reason.contains("nobody") && !reason.contains("no one"),
            "理由不能读成「没人在岗」：{reason}"
        );
    }
}
