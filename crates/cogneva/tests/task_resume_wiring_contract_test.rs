//! The resume chain has two ends in two crates, and every link between them can
//! be dropped by an edit that compiles.
//!
//! A task's progress is checkpointed by the process that holds it, which leaves
//! a pointer on the task board; the dispatcher reads that pointer back when it
//! builds the task's agents. Between those two ends sit four things that are
//! each invisible when missing: the id vocabulary both ends must derive rather
//! than spell, the store an agent's snapshot is persisted to (without it the
//! snapshot is taken and dropped, and the pointer resolves to nothing), the
//! dispatcher's use of the restoring constructor, and the deletion of the
//! checkpoint a new one replaces (without it the store grows by one snapshot
//! per agent per tick, for as long as the deployment runs).
//!
//! The failure of any of them is the same shape: the task simply starts over,
//! which is what a first run looks like. Nothing errors, nothing logs, and no
//! rule fires. So the links are asserted here, over the sources and over the
//! two crate-level functions both ends call — the last of which is a real
//! comparison rather than a text match, because that agreement is the one thing
//! a rename in either crate would silently break.

use std::path::PathBuf;

const SQUAD_EXECUTOR: &str = "crates/cog-collaboration/src/squad/executor.rs";
const RESUME_CONSUMER: &str = "crates/cog-collaboration/src/resume.rs";
const CHECKPOINT_PRODUCER: &str = "crates/cog-orchestrator/src/dag_executor/task_checkpoint.rs";
/// Where the producer's own reading is recorded (the counter, not the logic).
const PRODUCER_READING: &str = "crates/cog-orchestrator/src/dag_executor/orchestrator.rs";
/// Where the producer's loop is registered and its vocabulary is published.
const PRODUCER_LOOP_FILE: &str = "crates/cog-orchestrator/src/plugin.rs";
const AGENT_MANAGER: &str = "crates/cog-agent/src/runtime/manager.rs";
const ID_VOCABULARY: &str = "crates/cog-core/src/types/agent_state.rs";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// Everything up to the first test-only item: a test may name anything it
/// likes, and a fixture that spells an id is not a producer.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// The calls that build a task's agent. Assembled at compile time so this file
/// never contains the text it forbids (it scans itself among the rest).
const CREATES: &str = concat!(".create_", "agent(");
const RESTORING_CREATES: &str = concat!("create_role_", "agent(");

#[test]
fn every_agent_a_squad_runs_is_created_through_the_restoring_constructor() {
    let source = read(SQUAD_EXECUTOR);
    let production = production_source(&source);

    assert!(
        production.contains(RESTORING_CREATES),
        "the squad executor no longer builds its agents through the restoring constructor, \
         so a task that has a resume point starts over"
    );
    assert!(
        !production.contains(CREATES),
        "the squad executor builds an agent directly. A direct call skips the resume step, \
         and the run it produces is indistinguishable from one that had nothing to resume"
    );
}

#[test]
fn the_agent_pool_hands_its_agents_the_checkpoint_store() {
    let production = production_source(&read(AGENT_MANAGER)).to_string();
    let attach = concat!("with_checkpoint_", "store(");
    assert!(
        production.contains(attach),
        "the pool no longer gives an agent a checkpoint store. An agent without one still \
         returns a snapshot and still emits the event; only the save is skipped, so every \
         resume pointer written from it names a checkpoint that was never persisted"
    );

    // The builder has to be reached from the plugin that owns the pool, or the
    // field exists and nothing ever sets it.
    let plugin = production_source(&read("crates/cog-agent/src/plugin.rs")).to_string();
    assert!(
        plugin.contains(attach),
        "the checkpoint store is not attached to the pool at construction"
    );
}

#[test]
fn neither_end_of_the_chain_spells_the_vocabulary_itself() {
    let vocabulary = read(ID_VOCABULARY);
    for definition in [
        concat!("CHECKPOINT_FIELD_", "PREFIX"),
        concat!("SQUAD_ID_", "PREFIX"),
    ] {
        assert!(
            vocabulary.contains(definition),
            "{definition} is not defined in the shared vocabulary: the two ends would each \
             spell it, and two spellings do not fail — they disagree in silence"
        );
    }

    for file in [RESUME_CONSUMER, CHECKPOINT_PRODUCER] {
        let production = production_source(&read(file)).to_string();
        for literal in [concat!("\"checkpoint", ":"), concat!("\"squad", ":")] {
            assert!(
                !production.contains(literal),
                "{file} spells the {literal} vocabulary instead of deriving it"
            );
        }
    }
}

/// The one link a text match cannot decide, asserted as behaviour: the id the
/// dispatcher builds is the id the task holder accepts, and a neighbouring
/// name is not accepted by accident.
#[test]
fn the_two_ends_agree_on_which_agents_belong_to_a_task() {
    use cog_core::{agent_id_for, checkpoint_field};
    use cog_orchestrator::dag_executor::task_checkpoint::belongs_to_task;

    // Pinned literally: this is the spelling both ends have to arrive at, and a
    // change to it is what a rule or a board field written before it would miss.
    assert_eq!(agent_id_for("t-1", "planner"), "squad:t-1-planner");
    assert_eq!(checkpoint_field("planner"), "checkpoint:planner");

    for role in ["planner", "generator", "evaluator", "moderator", "merger"] {
        assert!(
            belongs_to_task(&agent_id_for("t-1", role), role, "t-1"),
            "the task holder does not accept the {role} the dispatcher builds"
        );
    }

    // Task ids that are prefixes of each other must not claim one another's
    // agents, and a parallel branch's agent is not the task-level role.
    assert!(!belongs_to_task(
        &agent_id_for("t-11", "planner"),
        "planner",
        "t-1"
    ));
    assert!(!belongs_to_task("t-1-branch-0-planner", "planner", "t-1"));
}

/// The bound on the store, asserted where it can be read: the producer deletes
/// the checkpoint its new one replaces. Without this link the chain works and
/// the store grows without bound, which is a regression no test of the resume
/// behaviour itself would notice.
#[test]
fn the_producer_deletes_the_checkpoint_a_new_one_replaces() {
    let production = production_source(&read(CHECKPOINT_PRODUCER)).to_string();
    let forget = concat!("for", "get(");
    assert!(
        production.contains(forget),
        "the producer no longer deletes the checkpoint it replaces; every tick leaves one \
         more snapshot in the store with no reader"
    );
}

/// The two reading surfaces exist and are separate: one for what the producer
/// did, one for what the dispatcher managed to restore. A single counter shared
/// by both cannot tell "nothing needed resuming" from "resuming never worked".
#[test]
fn the_chain_has_a_reading_on_both_ends() {
    let producer = production_source(&read(PRODUCER_READING)).to_string();
    assert!(
        producer.contains(concat!("TASK_", "CHECKPOINT")),
        "the producer records no reading; a chain that silently stops writing looks like one \
         with nothing to write"
    );
    let consumer = production_source(&read(RESUME_CONSUMER)).to_string();
    assert!(
        consumer.contains("record_resume"),
        "the dispatcher records no reading; a resume point that could not be restored would \
         only exist as a log line"
    );
    let observable_source = read("crates/cog-collaboration/src/observable.rs");
    let observable = production_source(&observable_source);
    assert!(
        observable.contains("RESUME_OUTCOMES"),
        "the consumer's outcomes are not published as a closed set, so the surface cannot say \
         whether any resume ever happened"
    );
}

/// The producer's reading has to exist before the deployment has any work.
///
/// A process holding no running task writes nothing, so a producer that only
/// ever records outcomes it observed leaves no series at all in an idle
/// deployment — and "wired, nothing to save" then reads exactly like "never
/// wired", which is the shape this whole chain fails in. The vocabulary is
/// published as a closed set, and the loop is where it lands: a loop switched
/// off by configuration must not leave a series behind reading zero.
#[test]
fn the_producer_publishes_its_vocabulary_before_it_has_work() {
    let producer = production_source(&read(CHECKPOINT_PRODUCER)).to_string();
    assert!(
        producer.contains("CHECKPOINT_OUTCOMES"),
        "the producer's outcomes are not a closed set, so nothing can publish them at zero"
    );
    let reading = production_source(&read(PRODUCER_READING)).to_string();
    assert!(
        reading.contains("CHECKPOINT_OUTCOMES"),
        "the producer's reading side does not publish the outcomes it never observed"
    );
    let loop_source = read(PRODUCER_LOOP_FILE);
    assert!(
        loop_source.contains("publish_checkpoint_outcomes"),
        "the checkpoint loop never publishes the vocabulary, so an idle deployment shows no \
         producer reading at all"
    );
}
