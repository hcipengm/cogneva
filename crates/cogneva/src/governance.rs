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
    Face, FaceImportOutcome, FaceKind, FaceSpec, GovernanceStore, Handover, StateBackend, Task,
    TaskStatus, HANDOVER_TASK_TYPE,
};
use cog_orchestrator::face_drift::UnexplainedOwner;
use cog_storage::{PostgresGovernanceStore, PostgresStateBackend, RedisAgentRegistry};

/// Columns of a roster row, in the order the file writes them.
///
/// One list, so the header the projection writes and the position the import
/// reads cannot come apart: a column added on either side is added here once,
/// and the arity check below is the list's own length rather than a second
/// number that has to be kept equal by hand.
const ROSTER_COLUMNS: &[&str] = &[
    "face",
    "kind",
    "owner",
    "deputy",
    "witness",
    "time_share",
    "vacancy_until",
    "vacancy_assigner",
    "gap_task",
    "first_reading",
    "entry_path",
    "updated_at",
    "source_fronts",
];

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
                           to. Idempotent per face: an open row is kept, a row
                           the drift pass released is taken back once its owner
                           answers to the heartbeat store again, and a face
                           whose owner has no heartbeat key is counted as
                           absent instead of being given a row it cannot hold.

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
            let (registry, note) = agent_registry(redis_url.as_deref()).await;
            if let Some(note) = note {
                eprintln!("cogneva governance: {note}");
            }
            let presence = registry.map(RedisPresenceProbe);
            assign(
                &*store,
                &assign_pool,
                &workspace,
                face.as_deref(),
                presence
                    .as_ref()
                    .map(|p| p as &dyn crate::governance_acts::PresenceProbe),
            )
            .await?;
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

/// The roster, one line per face, in the column order `import-faces` reads.
///
/// The projection is written under the name the import reads, so it has to be
/// the same schema: a projection that carried fewer columns would be a second
/// format behind one file name, unreadable by the reader it is named after, and
/// every field it dropped would live only in whichever hand-kept copy happened
/// to have it. Those are the fields a reader needs most -- the reading a face is
/// judged by, where its work enters the change path, the share of a person's
/// time it takes -- so losing them here is losing the whole point of projecting.
///
/// A vacancy is a claim with three parts: by when, who fills it, and the task
/// expected to close it. Only the deadline would let a reader mistake a bare
/// date for a declared vacancy, which is the difference between a staffed face
/// and `no_owner`.
fn render_roster(faces: &[Face]) -> String {
    let mut out = format!("# {}\n", ROSTER_COLUMNS.join("\t"));
    for face in faces {
        let or_dash = |v: Option<&str>| v.unwrap_or("-").to_string();
        let fields = [
            face.face_id.clone(),
            face.spec.kind.as_str().to_string(),
            or_dash(face.owner_unit.as_deref()),
            or_dash(face.spec.deputy_unit.as_deref()),
            or_dash(face.spec.witness_unit.as_deref()),
            or_dash(face.spec.time_share.as_deref()),
            or_dash(face.spec.vacancy_until.map(|t| t.to_rfc3339()).as_deref()),
            or_dash(face.spec.vacancy_assigner.as_deref()),
            or_dash(face.spec.gap_task_id.as_deref()),
            or_dash(face.spec.first_reading.as_deref()),
            or_dash(face.spec.entry_path.as_deref()),
            face.updated_at.to_rfc3339(),
            or_dash(face.spec.source_fronts.as_deref()),
        ];
        // The row and the header come from the same list, so a field added to
        // one side fails here instead of writing a file the import rejects
        // line by line.
        assert_eq!(
            fields.len(),
            ROSTER_COLUMNS.len(),
            "a roster row and its header must carry the same fields"
        );
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
    out
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

    // The owner column is the measurement's answer, not the row's declaration.
    // A projection that copies the declaration is a copy of what somebody
    // typed, and it keeps saying so after that person is gone; the reading is
    // the closed loop's own answer -- whoever holds the face's open work, as
    // long as the heartbeat registry has them. The declaration stays where it
    // is, on the node, so a signed handover still has something to move.
    let readings = readings(store, redis_url).await?;
    let measured: std::collections::BTreeMap<&str, Option<&str>> = readings
        .iter()
        .map(|reading| (reading.face_id.as_str(), reading.owner.as_deref()))
        .collect();
    let projected: Vec<Face> = faces
        .iter()
        .map(|face| {
            let mut face = face.clone();
            if let Some(owner) = measured.get(face.face_id.as_str()) {
                face.owner_unit = owner.map(str::to_string);
            }
            face
        })
        .collect();

    // An owner that neither the roster nor a signed move accounts for is the
    // one thing this file could show and would otherwise hide. The column
    // holds a bare unit name, so a reader comparing it with the roster sees a
    // difference with no reason beside it, and the graph cannot be asked --
    // only the pair (what the file said, which handover moved it) can answer.
    // Written into the header rather than a column because it is a statement
    // about the set of rows, not about any one of them.
    let handovers: Vec<Handover> = store
        .governance_nodes(HANDOVER_TASK_TYPE)
        .await?
        .iter()
        .filter_map(Handover::from_task)
        .collect();
    let unexplained = cog_orchestrator::face_drift::owners_no_act_explains(
        &faces,
        &cog_orchestrator::face_drift::RecordedMove::from_handovers(&handovers),
    );

    let mut faces_tsv = String::from(&stamp);
    faces_tsv.push_str(&unexplained_owner_lines(&unexplained));
    faces_tsv.push_str(&render_roster(&projected));

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
         (faces={}, decisions={}, open_tasks={}, source_revision={}, \
         owners_no_act_explains={})",
        now.to_rfc3339(),
        faces.len(),
        decisions.len(),
        open_tasks.len(),
        revision,
        unexplained.len()
    );
    Ok(())
}

/// The header lines naming faces whose owner nothing on record explains.
///
/// Tab-separated because a face id is not: the registry has ids with spaces in
/// them (`token 管理`) and ids with slashes (`CI/构建/部署`), and a roster value
/// carries a slash of its own (`operator/8b2`). A line split on whitespace
/// would turn `token 管理` into a face called `token` plus a stray word, and
/// the slash would make two different fields read as the same shape -- both
/// silently, since the reader that guessed wrong still gets a plausible
/// answer. Tab is what this file already uses between columns, and no id
/// contains one.
fn unexplained_owner_lines(rows: &[UnexplainedOwner]) -> String {
    if rows.is_empty() {
        return "# owners_no_act_explains: 0\n".to_string();
    }
    // The field order is written next to the count rather than in a separate
    // column header: the two lines are read together or not at all.
    let mut out = format!(
        "# owners_no_act_explains: {} (face<TAB>owner<TAB>roster_said)\n",
        rows.len()
    );
    for row in rows {
        out.push_str(&format!(
            "# unexplained_owner: {}\t{}\t{}\n",
            row.face_id,
            row.owner_unit,
            row.roster_said.as_deref().unwrap_or("-"),
        ));
    }
    out
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
        if columns.len() != ROSTER_COLUMNS.len() {
            return Err(format!(
                "line {}: expected {} columns, found {}",
                index + 1,
                ROSTER_COLUMNS.len(),
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
///
/// A face whose owner the heartbeat registry does not know gets no row. The row
/// would be released on the next drift pass, because an owner that is not there
/// cannot hold work, so writing it buys one rewrite and one release -- and if
/// the shared queue claims it in between, the row comes out running with nobody
/// named, which the verdict reads as `drifted_binding`: red, a louder fault than
/// the yellow the row was written to clear. An absent owner is counted by name
/// instead, and the reading says the same thing once the cause is decided by
/// repair order rather than by which condition is cheapest to test.
async fn assign(
    store: &dyn GovernanceStore,
    pool: &sqlx::PgPool,
    workspace: &str,
    only: Option<&str>,
    presence: Option<&dyn crate::governance_acts::PresenceProbe>,
) -> Result<(), Box<dyn Error>> {
    if workspace.trim().is_empty() {
        return Err("no workspace to assign work in: pass --workspace or set \
                    dag_executor.workspace_id"
            .into());
    }
    // The governance workspace holds the face nodes themselves, so the drift
    // reading deliberately skips it; work written there is invisible to the
    // very reading it was meant to satisfy, and the pass would still report
    // every face as assigned.
    if workspace == cog_core::GOVERNANCE_WORKSPACE {
        return Err(format!(
            "{workspace} holds the face nodes and is excluded from the drift reading: \
             assign the work into the workspace the faces are measured in"
        )
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
    let mut rebound = 0usize;
    let mut kept = 0usize;
    let mut released = 0usize;
    let mut vacant = 0usize;
    let mut absent: Vec<String> = Vec::new();
    for face in selected {
        let Some(owner) = face.owner_unit.clone() else {
            vacant += 1;
            eprintln!(
                "{}: declared vacancy; a vacancy cannot hold work until somebody owns the face",
                face.face_id
            );
            continue;
        };
        // Keep `assign` and the drift pass from undoing each other: see the
        // doc comment above. A registry that cannot be read is not evidence of
        // absence, so the row is written and the note says why.
        let mut owner_present = None;
        if let Some(probe) = presence {
            match absence_of(probe.presence_ttl_secs(&owner).await) {
                Ok(true) => {
                    absent.push(format!("{}:{}", face.face_id, owner));
                    continue;
                }
                Ok(false) => owner_present = Some(true),
                Err(e) => eprintln!(
                    "{}: the owner's presence could not be read ({e}); writing the row anyway",
                    face.face_id
                ),
            }
        }
        let id = cog_orchestrator::governance_acts::face_work_id(&face.face_id);
        if let Some(existing) = backend.dag_get_task(workspace, &id).await? {
            if !matches!(
                existing.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            ) {
                let running = existing.status == TaskStatus::Running;
                match standing_row_disposition(
                    existing.owner_unit.as_deref(),
                    &owner,
                    owner_present == Some(true),
                    running,
                ) {
                    StandingRow::Keep => {
                        kept += 1;
                        continue;
                    }
                    StandingRow::LeaveReleased => {
                        released += 1;
                        continue;
                    }
                    // The face's owner moved after this row was written, and
                    // a row that keeps the previous name is not "kept work":
                    // the drift reading takes the row as the evidence of who
                    // is doing the face, so it goes on reporting the face as
                    // bound to nobody while a row for it sits in the queue.
                    StandingRow::Rebind => {
                        let moved = rebind_standing_row(existing, &face.face_id, &owner, workspace);
                        backend.dag_set_task(workspace, &id, &moved).await?;
                        rebound += 1;
                        continue;
                    }
                }
            }
        }
        let task = cog_orchestrator::governance_acts::standing_row(&face, workspace);
        backend.dag_set_task(workspace, &id, &task).await?;
        assigned += 1;
    }
    println!(
        "assigned {assigned}, rebound {rebound}, kept {kept}, released {released}, \
         vacant {vacant}, absent {} (workspace {workspace})",
        absent.len()
    );
    // Named, not just counted: the repair is somebody bringing that unit back
    // or a signed handover moving the face, and neither can start from a number.
    for entry in &absent {
        eprintln!("{entry}: the owner has no heartbeat key; no standing row written");
    }
    Ok(())
}

/// What a presence answer means for writing a standing row.
///
/// Only a positive "there is no such key" refuses the row. A store that could
/// not be read says nothing about who is there, and reading it as an absence
/// would stop staffing every face the first time Redis blinked -- a repair that
/// takes hold of the whole graph on the strength of one failed GET.
fn absence_of(answer: Result<Option<u64>, String>) -> Result<bool, String> {
    match answer {
        Ok(Some(_left)) => Ok(false),
        Ok(None) => Ok(true),
        Err(e) => Err(e),
    }
}

/// What `assign` does with an open standing work row it already found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StandingRow {
    /// The row already names the face's owner.
    Keep,
    /// The row names some other unit: the face changed hands after the row was
    /// written, and the row is the evidence the drift reading takes of who is
    /// doing the face.
    Rebind,
    /// The row names nobody, which is the state the drift pass leaves behind
    /// when the unit it named stopped answering to the registry. Clearing the
    /// owner is the only move that puts the work back in front of every
    /// executor -- the shared queue hands out unowned work only -- so while
    /// that name is still absent, giving the row an owner again takes it off
    /// the queue and hands it to nobody. `assign` and the drift pass would then
    /// undo each other for as long as both ran, while the face read as staffed
    /// the whole time.
    LeaveReleased,
}

/// Which of the three dispositions an open standing row gets.
///
/// A row with no owner is left released only while its unit is still gone.
/// Once the unit answers to the registry again the reason `LeaveReleased`
/// exists is gone: the row can be held, the face can read as staffed, and the
/// duty goes back to the unit whose name is on the face. Without that, the
/// release is one-way -- a face whose owner blinked once would read
/// `no_bound_task` for the rest of its life, and no later `assign` could ever
/// put work back in front of it.
///
/// A running row is never rebound. It belongs to whoever holds its lease; the
/// release path only moves unstarted rows, and re-owning a run in flight would
/// name a unit that is not the one executing it.
fn standing_row_disposition(
    existing_owner: Option<&str>,
    face_owner: &str,
    face_owner_present: bool,
    running: bool,
) -> StandingRow {
    match existing_owner {
        Some(owner) if owner == face_owner => StandingRow::Keep,
        Some(_) => StandingRow::Rebind,
        None if face_owner_present && !running => StandingRow::Rebind,
        None => StandingRow::LeaveReleased,
    }
}

/// Point an open standing row at the face's current owner.
///
/// The owner is written three times into one row: the struct field, the
/// payload's `owner_unit`, and the executor's `input.owner_unit`. The reading
/// takes the row's own binding, and the executor hands the goal in `input` to
/// whoever claims it, so a row whose copies disagree either reads as somebody
/// else's work or tells the claimer about a unit that no longer owns the face.
fn rebind_standing_row(mut row: Task, face_id: &str, owner: &str, workspace: &str) -> Task {
    row.face_id = Some(face_id.to_string());
    row.owner_unit = Some(owner.to_string());
    row.workspace_id = Some(workspace.to_string());
    // A row written before the standing shape was settled can be in either
    // broken form -- executable and untyped, or typed and non-executable -- and
    // re-binding is the pass that already has the row in hand, so it is also
    // the pass that puts the shape right.
    cog_orchestrator::governance_acts::ensure_duty_shape(&mut row);
    match row.input.as_object_mut() {
        Some(input) => {
            input.insert("face".into(), face_id.into());
            input.insert("owner_unit".into(), owner.into());
        }
        None => {
            row.input = serde_json::json!({"face": face_id, "owner_unit": owner});
        }
    }
    row
}

/// What one roster import did, cell by cell.
///
/// The counts describe the pass, not the graph: `updated` says how many rows
/// the file moved, and `bindings_erased` names the faces that came out of the
/// pass with no handover where they had one going in.
pub struct ImportSummary {
    pub rows: usize,
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    pub ownership_kept: usize,
    /// Faces bound by a handover before the pass and unbound after it.
    pub bindings_erased: Vec<String>,
}

impl ImportSummary {
    /// The one line a caller reads to learn what was written.
    pub fn line(&self) -> String {
        format!(
            "imported {} rows: inserted={} updated={} unchanged={} ownership_kept={} \
             bindings_erased={}",
            self.rows,
            self.inserted,
            self.updated,
            self.unchanged,
            self.ownership_kept,
            self.bindings_erased.len()
        )
    }
}

/// Which faces lost the handover that bound them across one pass.
///
/// A roster is rewritten by a clock, so the one fact it must never carry is an
/// erasure: a face whose binding is empty after an import reads exactly like a
/// face no handover ever reached, and nothing left on the row tells the two
/// apart afterwards. The import keeps the cell from the row it found, so this
/// count is zero; reading it every pass is what makes the next regression
/// visible at the moment it happens instead of at the next drift verdict, which
/// can only say the binding is missing now, never that it was there before.
fn faces_that_lost_their_binding(before: &[Face], after: &[Face]) -> Vec<String> {
    let was_bound: std::collections::HashMap<&str, bool> = before
        .iter()
        .map(|face| (face.face_id.as_str(), face.bound_by_handover_id.is_some()))
        .collect();
    let mut lost: Vec<String> = after
        .iter()
        .filter(|face| face.bound_by_handover_id.is_none())
        .filter(|face| {
            was_bound
                .get(face.face_id.as_str())
                .copied()
                .unwrap_or(false)
        })
        .map(|face| face.face_id.clone())
        .collect();
    lost.sort();
    lost
}

/// Put one roster's rows into the graph and report what the pass did.
///
/// Takes the text rather than a path so the same pass can be run against a
/// caller that already holds the bytes, and so a test can seed the graph
/// without a file.
pub async fn import_faces_from_roster(
    store: &dyn GovernanceStore,
    text: &str,
    keep_ownership: bool,
) -> Result<ImportSummary, Box<dyn Error>> {
    let rows = parse_roster(text)?;
    let ownership = if keep_ownership {
        cog_core::ImportOwnership::HandoverOnly
    } else {
        cog_core::ImportOwnership::FromFile
    };
    // Read before and after the writes: the erasure this counts is a transition
    // between two states of the same row, and only the pass that made it can
    // see both ends.
    let before: Vec<Face> = store
        .face_nodes()
        .await?
        .iter()
        .filter_map(Face::from_task)
        .collect();
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
    let after: Vec<Face> = store
        .face_nodes()
        .await?
        .iter()
        .filter_map(Face::from_task)
        .collect();
    Ok(ImportSummary {
        rows: rows.len(),
        inserted,
        updated,
        unchanged,
        ownership_kept: kept,
        bindings_erased: faces_that_lost_their_binding(&before, &after),
    })
}

async fn import_faces(
    store: &dyn GovernanceStore,
    from: &Path,
    keep_ownership: bool,
) -> Result<(), Box<dyn Error>> {
    let text = std::fs::read_to_string(from)
        .map_err(|e| format!("cannot read {}: {e}", from.display()))?;
    let summary = import_faces_from_roster(store, &text, keep_ownership).await?;
    println!("{}", summary.line());
    // The line is printed before this refusal so a reader of the log sees the
    // count it is about; the exit code is what makes it a failure rather than a
    // number somebody has to notice.
    if !summary.bindings_erased.is_empty() {
        return Err(format!(
            "this pass erased the handover binding of {} face(s): {}; an import may refresh a \
             description, never take away the record of who moved the face",
            summary.bindings_erased.len(),
            summary.bindings_erased.join(", ")
        )
        .into());
    }
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

    /// A face's owner moves while its standing row stays open. Counting that
    /// row as "kept" leaves the face bound to the previous name, and the drift
    /// reading takes the row's binding as its evidence -- so the face keeps
    /// reporting as bound to nobody while a row for it sits in the queue.
    #[test]
    fn a_standing_row_written_before_the_owner_moved_follows_the_face() {
        let mut stale = Task::new(
            "face-work-~901a~4fe1",
            cog_core::TaskType::DagNode,
            serde_json::json!({
                "goal": "keep the face in the target state",
                "face": "通信",
                "owner_unit": "operator",
            }),
        );
        stale.face_id = Some("通信".into());
        stale.owner_unit = Some("operator".into());
        stale.workspace_id = Some("default".into());
        let status_before = stale.status;

        let moved = rebind_standing_row(stale, "通信", "f5", "default");

        assert_eq!(moved.owner_unit.as_deref(), Some("f5"));
        assert_eq!(
            moved.status, status_before,
            "rebinding changes who the row belongs to, not how far it got"
        );
        assert_eq!(
            moved.input.get("owner_unit").and_then(|v| v.as_str()),
            Some("f5"),
            "the claimer is told from input which unit owns the face: {moved:?}"
        );
        assert_eq!(
            moved.input.get("face").and_then(|v| v.as_str()),
            Some("通信"),
            "the row keeps the face it was written for: {moved:?}"
        );
        assert!(
            moved.input.get("goal").and_then(|v| v.as_str()).is_some(),
            "a rebind must not drop the goal: {moved:?}"
        );
        // The two readers of this row disagree on purpose, and a rebind that
        // satisfies only one of them takes the face's binding away: a
        // non-executable row is invisible to the drift reading's count of open
        // work (the face reads idle with a row right there), and an untyped
        // executable row is started by the dispatcher, failed for having no
        // body, and leaves the face with nothing bound to it.
        assert!(
            moved.is_executable,
            "the reading counts only executable rows, so a rebind must leave it executable: {moved:?}"
        );
        assert!(
            !cog_orchestrator::ready_queue::is_dispatchable_work(&moved),
            "a duty has no body to run, so the dispatcher must leave it alone: {moved:?}"
        );
    }

    /// A row the drift pass released must not be handed back to the absent
    /// name by the next `assign` -- and must be handed back once that name
    /// answers again.
    ///
    /// The two passes have opposite jobs on the same row: the drift pass
    /// clears the owner when the unit it names stops answering, `assign`
    /// makes the row name the face's owner again. Run together on a face
    /// whose owner is gone, they undo each other -- the row never becomes
    /// runnable, and because it is owned again the face stops reading as
    /// drifted. Measured on the live cluster: eighteen face rows stayed
    /// unstarted for five days this way. Leaving a released row alone is
    /// what breaks the loop while the owner is away.
    ///
    /// Leaving it alone forever would be a different fault: the release is
    /// only meant to last as long as the absence does. Seventeen faces whose
    /// owner had no heartbeat key were released, the owner came back, and
    /// every later `assign` reported `released` and wrote nothing -- the faces
    /// had no standing row left to hold and no pass that would ever give them
    /// one, so their idle reading could not change again.
    #[test]
    fn a_released_standing_row_comes_back_when_its_owner_does() {
        assert_eq!(
            standing_row_disposition(None, "f5", false, false),
            StandingRow::LeaveReleased,
            "while the owner is gone, re-owning the row takes it off the queue"
        );
        assert_eq!(
            standing_row_disposition(None, "f5", true, false),
            StandingRow::Rebind,
            "an owner that answers again can hold the duty, and the loop is why it was released"
        );
        assert_eq!(
            standing_row_disposition(None, "f5", true, true),
            StandingRow::LeaveReleased,
            "a run in flight belongs to whoever holds its lease, not to the face's owner"
        );
        assert_eq!(
            standing_row_disposition(Some("f5"), "f5", true, false),
            StandingRow::Keep,
            "a row that already names the face's owner is kept as it is"
        );
        assert_eq!(
            standing_row_disposition(Some("c2"), "f5", true, false),
            StandingRow::Rebind,
            "a face that changed hands moves its open row with it"
        );
    }

    /// A face whose owner has no heartbeat key gets no standing row at all.
    ///
    /// `assign` used to write the row and leave the drift pass to release it one
    /// pass later. Measured on the live cluster with seventeen faces owned by a
    /// unit that had no key: the rows went out, the pass cleared them, and the
    /// shared queue claimed four inside that window -- each a running row naming
    /// nobody, which the verdict reads as `drifted_binding`. Assigning work to
    /// those faces is what turned four of them from yellow into red.
    #[test]
    fn a_face_whose_owner_is_absent_gets_no_standing_row() {
        assert!(
            absence_of(Ok(None)).unwrap(),
            "no key is the absence: nothing else in the tree writes it"
        );
        assert!(
            !absence_of(Ok(Some(1))).unwrap(),
            "a key with a second left is still a unit that is here"
        );
        assert!(
            absence_of(Err("redis timed out".into())).is_err(),
            "a GET that failed is not evidence the owner is gone; reading it as one \
             would stop staffing every face on one blink"
        );
    }

    /// The file the projection writes is the file the import reads, so it has
    /// to survive a round trip through the parser. A projection that dropped a
    /// column would still look complete to a human reading it -- the rows are
    /// there, the names are right -- and only the import would notice, at the
    /// moment somebody tried to seed a machine from it.
    #[test]
    fn the_projection_is_a_roster_the_import_reads_back_unchanged() {
        let text = "\
任务管理\tface\toperator\t8b2\tc2\ttoken:60,comm:40\t-\t-\t-\ttasks_rows_read\toperator/8b2\t2026-10-19T14:55:00+08:00\toperator/8b2
可评估\tface\t-\t-\t-\t-\t2026-11-06T00:00:00+08:00\toperator\tEVAL-BENCH\teval_bench_score\t-\t2026-10-24T00:20:00+08:00\t-
";
        let faces: Vec<Face> = parse_roster(text)
            .unwrap()
            .into_iter()
            .map(|row| row.face)
            .collect();
        let projection = render_roster(&faces);
        // The host readers find their columns by name in this line, and the
        // comment marker is what keeps the importer from reading it as a data
        // row. Both halves are pinned here: a host reader that looks for
        // `face` fails on `# face` and reports the projection as having no
        // such column, which reads downstream as a producer that cannot read
        // its input on every beat. Which side carries the bug does not matter
        // to whoever sees the alert; a change to this line has to be made
        // knowing both consumers exist.
        assert_eq!(
            projection.lines().next(),
            Some(
                "# face\tkind\towner\tdeputy\twitness\ttime_share\tvacancy_until\t\
                 vacancy_assigner\tgap_task\tfirst_reading\tentry_path\tupdated_at\t\
                 source_fronts"
            ),
            "the column line is the contract with the host readers that locate columns by name"
        );
        let back = parse_roster(&projection).unwrap_or_else(|e| {
            panic!("the projection must be readable by the import it is named after: {e}\n{projection}")
        });
        assert_eq!(back.len(), faces.len());
        for (before, after) in faces.iter().zip(back.iter().map(|row| &row.face)) {
            let name = &before.face_id;
            assert_eq!(&after.face_id, name);
            assert_eq!(after.owner_unit, before.owner_unit, "{name}: owner");
            assert_eq!(after.spec.kind, before.spec.kind, "{name}: kind");
            assert_eq!(
                after.spec.deputy_unit, before.spec.deputy_unit,
                "{name}: deputy"
            );
            assert_eq!(
                after.spec.witness_unit, before.spec.witness_unit,
                "{name}: witness"
            );
            assert_eq!(
                after.spec.time_share, before.spec.time_share,
                "{name}: time_share"
            );
            assert_eq!(
                after.spec.vacancy_until, before.spec.vacancy_until,
                "{name}: vacancy_until"
            );
            assert_eq!(
                after.spec.vacancy_assigner, before.spec.vacancy_assigner,
                "{name}: vacancy_assigner"
            );
            assert_eq!(
                after.spec.gap_task_id, before.spec.gap_task_id,
                "{name}: gap_task"
            );
            assert_eq!(
                after.spec.first_reading, before.spec.first_reading,
                "{name}: first_reading is the reading the face is judged by"
            );
            assert_eq!(
                after.spec.entry_path, before.spec.entry_path,
                "{name}: entry_path"
            );
            assert_eq!(
                after.spec.source_fronts, before.spec.source_fronts,
                "{name}: source_fronts"
            );
        }
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

    fn face_bound(face_id: &str, binding: Option<&str>) -> Face {
        Face {
            face_id: face_id.to_string(),
            owner_unit: Some("worker".into()),
            bound_by_handover_id: binding.map(str::to_string),
            spec: FaceSpec::first(face_id, FaceKind::Face),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn a_pass_that_takes_a_binding_away_names_the_face_it_took_it_from() {
        let before = vec![face_bound("token", Some("handover:token:1"))];
        let after = vec![face_bound("token", None)];
        assert_eq!(
            faces_that_lost_their_binding(&before, &after),
            vec!["token".to_string()],
            "a face bound before the pass and unbound after it is the one case this reading exists for"
        );
    }

    /// The reverse control: without it, a reading that counted every unbound
    /// face would pass the test above and report every face that never had a
    /// handover as having lost one.
    #[test]
    fn a_face_that_was_never_bound_has_not_lost_one() {
        let before = vec![face_bound("token", None)];
        let after = vec![face_bound("token", None)];
        assert!(faces_that_lost_their_binding(&before, &after).is_empty());
    }

    /// The other reverse control: the binding being there on both sides is what
    /// an ordinary import does, and it must not be counted as an erasure just
    /// because the pass wrote the row.
    #[test]
    fn a_binding_that_survived_the_pass_is_not_an_erasure() {
        let before = vec![face_bound("token", Some("handover:token:1"))];
        let after = vec![face_bound("token", Some("handover:token:2"))];
        assert!(faces_that_lost_their_binding(&before, &after).is_empty());
    }

    #[test]
    fn the_summary_line_carries_the_erasure_count_a_reader_greps_for() {
        let summary = ImportSummary {
            rows: 17,
            inserted: 0,
            updated: 3,
            unchanged: 14,
            ownership_kept: 0,
            bindings_erased: Vec::new(),
        };
        assert!(
            summary.line().contains("bindings_erased=0"),
            "the line has to carry the count even when it is zero: a reader that has to tell \
             'absent' from 'zero' cannot do it from a missing cell"
        );
    }

    /// The header has to survive the face ids the registry actually has.
    ///
    /// Two of them carry a space (`token 管理`) and one carries a slash
    /// (`CI/构建/部署`), which is also the separator inside a roster value
    /// (`operator/8b2`). A line split on whitespace reads the first as a face
    /// called `token` plus a stray word; a line split on slashes reads the
    /// last as a face called `CI` plus a roster value called `构建`. Both give
    /// a plausible answer, which is why the check is on the round trip and not
    /// on the rendering.
    #[test]
    fn a_face_id_with_a_space_or_a_slash_survives_the_header() {
        let rows = vec![
            UnexplainedOwner {
                face_id: "token 管理".into(),
                owner_unit: "f5".into(),
                roster_said: Some("worker".into()),
            },
            UnexplainedOwner {
                face_id: "CI/构建/部署".into(),
                owner_unit: "f5".into(),
                roster_said: Some("operator/8b2".into()),
            },
        ];
        let text = unexplained_owner_lines(&rows);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "one count line and one line per face");
        assert!(lines[0].contains('2'));
        assert!(lines[0].contains("face<TAB>owner<TAB>roster_said"));

        let parsed: Vec<(&str, &str, &str)> = lines[1..]
            .iter()
            .map(|line| {
                let fields: Vec<&str> = line.split('\t').collect();
                assert_eq!(
                    fields.len(),
                    3,
                    "a face id that splits into more fields than the header declares: {line}"
                );
                (fields[0], fields[1], fields[2])
            })
            .collect();
        assert!(
            parsed[0].0.ends_with("token 管理"),
            "the space in the id belongs to the id: {:?}",
            parsed[0].0
        );
        assert!(
            parsed[1].0.ends_with("CI/构建/部署"),
            "the slashes in the id are not field separators: {:?}",
            parsed[1].0
        );
        assert_eq!(parsed[1].2, "operator/8b2");
    }

    /// Without rows the header still says so, in a form a reader can test
    /// rather than a line that is simply missing.
    #[test]
    fn no_unexplained_owners_still_prints_a_count() {
        assert_eq!(
            unexplained_owner_lines(&[]),
            "# owners_no_act_explains: 0\n"
        );
    }
}
