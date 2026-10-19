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
//! was signed.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};

use cog_core::{Face, FaceImportOutcome, FaceKind, FaceSpec, GovernanceStore};
use cog_storage::{PostgresGovernanceStore, RedisAgentRegistry};

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
    project --out <dir>    Write faces.tsv and drift.tsv into <dir>, each with
                           its generated_at and the graph's largest revision.
                           This is the write a freshness gate reads.
    import-faces --from <file>
                           Put each roster row into the graph. Columns:
                           face kind owner deputy witness time_share
                           vacancy_until vacancy_assigner gap_task first_reading
                           entry_path updated_at source_fronts
                           Idempotent; printed counts say what happened.
    handover <SUB>         Open, sign, apply, withdraw or list a move of a face
                           from one unit to another. A move counts only when
                           every required unit signed and it was applied.
    vote <SUB>             Open, cast, withdraw, close or list a decision.
                           Ballot rules are written at open time and never
                           rewritten; a principle that settles the question
                           records its basis and closes with no votes.

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
    Faces { brief: bool },
    Drift { json: bool },
    Project { out: PathBuf },
    ImportFaces { from: PathBuf },
    Act(crate::governance_acts::Act),
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
            action: Action::Act(crate::governance_acts::parse_args(&rest)?),
            database_url,
            redis_url,
        });
    }
    let mut database_url: Option<String> = None;
    let mut redis_url: Option<String> = None;
    let mut brief = false;
    let mut json = false;
    let mut out: Option<PathBuf> = None;
    let mut from: Option<PathBuf> = None;

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
            "--out" => {
                out = Some(PathBuf::from(iter.next().ok_or("--out needs a directory")?));
            }
            "--from" => {
                from = Some(PathBuf::from(iter.next().ok_or("--from needs a file")?));
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
        },
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
    let store: Arc<dyn GovernanceStore> = Arc::new(PostgresGovernanceStore::new(pool));

    match request.action {
        Action::Faces { brief } => faces(&*store, brief).await?,
        Action::Drift { json } => drift(&*store, redis_url.as_deref(), json).await?,
        Action::Project { out } => project(&*store, redis_url.as_deref(), &out).await?,
        Action::ImportFaces { from } => import_faces(&*store, &from).await?,
        Action::Act(act) => crate::governance_acts::run(act, &*store, Utc::now()).await?,
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
        for face in &faces {
            println!(
                "{}\t{}\t{}",
                face.face_id,
                face.spec.kind.as_str(),
                face.owner_unit.as_deref().unwrap_or("-")
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
    faces_tsv.push_str("face\tkind\towner\tdeputy\twitness\tvacancy_until\trevision\n");
    for face in &faces {
        faces_tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            face.face_id,
            face.spec.kind.as_str(),
            face.owner_unit.as_deref().unwrap_or("-"),
            face.spec.deputy_unit.as_deref().unwrap_or("-"),
            face.spec.witness_unit.as_deref().unwrap_or("-"),
            face.spec
                .vacancy_until
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "-".into()),
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

    std::fs::write(out.join("faces.tsv"), faces_tsv)?;
    std::fs::write(out.join("drift.tsv"), drift_tsv)?;
    println!(
        "wrote {} and {} at {} (faces={}, source_revision={})",
        out.join("faces.tsv").display(),
        out.join("drift.tsv").display(),
        now.to_rfc3339(),
        faces.len(),
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

async fn import_faces(store: &dyn GovernanceStore, from: &Path) -> Result<(), Box<dyn Error>> {
    let text = std::fs::read_to_string(from)
        .map_err(|e| format!("cannot read {}: {e}", from.display()))?;
    let rows = parse_roster(&text)?;
    let mut inserted = 0;
    let mut updated = 0;
    let mut unchanged = 0;
    let mut kept = 0;
    for row in &rows {
        match cog_core::import_face(store, &row.face).await? {
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
                from: PathBuf::from("faces.tsv")
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
}
