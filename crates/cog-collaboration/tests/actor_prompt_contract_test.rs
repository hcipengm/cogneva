//! The prompt split, read from outside the crate.
//!
//! Every actor hands the runtime a document whose stable half — its
//! instructions, its answer contract — must arrive under the key the agent
//! runtime reads. That is an agreement between two crates, so it is pinned here
//! through the public API rather than from inside the module that writes it.
//!
//! These two guards live in their own process on purpose: the observability
//! plane is one per process, several unit tests in this crate read a cell of it
//! by the difference across their own call, and an actor driven without a
//! self-review configuration counts a skipped review into exactly those cells.
//! In the same binary these guards would perturb readings they are not about.

use std::sync::{Arc, Mutex};

use cog_collaboration::actors::{
    parse_merge_result, parse_moderator_output, EvaluatorActor, GeneratorActor, MergerActor,
    ModeSelectorActor, ModeratorActor, ModeratorDecision, PlannerActor, PreviousAttempt,
};
use cog_collaboration::squad::pge::types::{
    GeneratorOutput, PgeBranchResult, PlannerOutput, RoundOutcome, StopCause, StoppedProduct,
};

/// An agent that keeps every input document it was handed, so a test can read
/// what the actors actually send instead of what they were meant to send.
struct Recorder {
    seen: Mutex<Vec<serde_json::Value>>,
    reply: serde_json::Value,
}

impl Recorder {
    fn new(reply: serde_json::Value) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            reply,
        })
    }

    fn inputs(&self) -> Vec<serde_json::Value> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait::async_trait]
impl cog_core::Agent for Recorder {
    async fn prompt(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(input);
        Ok(self.reply.clone())
    }
    async fn start(&self) {}
    async fn snapshot(&self, _task_id: String) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
        Err(cog_core::SFError::NotImplemented("snapshot".into()))
    }
    async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn continue_(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Err(cog_core::SFError::NotImplemented("continue_".into()))
    }
    async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn abort(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn reset(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
        Ok(cog_core::AgentState::Idle)
    }
    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }
    async fn chat_stream(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        Err(cog_core::SFError::NotImplemented("chat_stream".into()))
    }
    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        Err(cog_core::SFError::NotImplemented("complete_stream".into()))
    }
    async fn read_board(&self, _task_id: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        Ok(None)
    }
    async fn write_board(
        &self,
        _task_id: &str,
        _field: &str,
        _value: &str,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }
}

fn gate_task() -> cog_core::Task {
    cog_core::Task::new(
        "t-prefix",
        cog_core::TaskType::Custom("test".into()),
        serde_json::json!({"goal": "g"}),
    )
}

/// A branch result, so a merge has something to choose between.
fn a_branch(branch_id: u32) -> PgeBranchResult {
    PgeBranchResult {
        branch_id,
        plan: PlannerOutput {
            summary: format!("plan {branch_id}"),
            plan: serde_json::json!({}),
            sub_tasks: Vec::new(),
            acceptance_criteria: Vec::new(),
            targets: Vec::new(),
        },
        generation: GeneratorOutput::none(),
        outcome: RoundOutcome::Stopped {
            cause: StopCause::NotAttempted,
            product: StoppedProduct::None,
        },
    }
}

/// Two prompts from every actor that assembles one, the second with a varying
/// half that differs from the first.
///
/// Returned as `(role, recorder, the field this role's varying half carries for
/// the work at hand)`. Shared by the two guards below, so that both read the
/// requests the same actors really build: a guard built on a hand-assembled
/// document would pass while the production path drifts away from it.
async fn two_prompts_from_every_actor() -> Vec<(&'static str, Arc<Recorder>, &'static str)> {
    let task = gate_task();
    let plan = serde_json::json!({"summary": "s", "sub_tasks": []});
    let generation = serde_json::json!({"content": "c", "artifacts": []});

    let mut roles: Vec<(&'static str, Arc<Recorder>, &'static str)> = Vec::new();

    let planner_agent = Recorder::new(serde_json::json!({"summary": "s", "sub_tasks": []}));
    let planner = PlannerActor::new(planner_agent.clone());
    for attempt in [1, 3] {
        planner.plan(&task, attempt, None, None, None, None).await;
    }
    roles.push(("planner", planner_agent, "task"));

    let generator_agent = Recorder::new(generation.clone());
    let generator = GeneratorActor::new(generator_agent.clone());
    for attempt in [1, 3] {
        generator
            .generate(&task, &plan, attempt, PreviousAttempt::default(), None)
            .await;
    }
    roles.push(("generator", generator_agent, "task"));

    let evaluator_agent = Recorder::new(serde_json::json!({"verdict": "pass"}));
    let evaluator = EvaluatorActor::new(evaluator_agent.clone());
    for history in [Vec::new(), vec![serde_json::json!({"verdict": "pass"})]] {
        evaluator
            .evaluate(&task, &plan, &generation, &history, &[], None)
            .await;
    }
    roles.push(("evaluator", evaluator_agent, "task"));

    // The roundtable's control actor: what moves between rounds is the debate,
    // and the board is where the round that just ended was written.
    let moderator_agent = Recorder::new(serde_json::json!({"decision": "Continue"}));
    let moderator = ModeratorActor::new(moderator_agent.clone());
    for round in [0, 1] {
        moderator
            .moderate(&task, &[], &serde_json::json!({ "round": round }), 0.8)
            .await;
    }
    roles.push(("moderator", moderator_agent, "history"));

    let merger_agent = Recorder::new(serde_json::json!({"reasoning": "r"}));
    let merger = MergerActor::new(merger_agent.clone());
    for branch_count in [1u32, 2] {
        let branches: Vec<_> = (1..=branch_count).map(a_branch).collect();
        merger.merge(&task, &branches, &serde_json::json!({})).await;
    }
    roles.push(("merger", merger_agent, "branches"));

    // The router: no profile and no keyword in these goals, so it reaches the
    // agent stage, which is the only stage that sends a prompt.
    let selector_agent = Recorder::new(serde_json::json!("Pipeline"));
    let selector = ModeSelectorActor::new().with_agent(selector_agent.clone());
    for goal in ["tidy the widget", "tidy the gadget"] {
        selector.select_mode(goal, None, Some("t-modesel")).await;
    }
    roles.push(("mode_selector", selector_agent, "goal"));

    roles
}

fn stable_half(input: serde_json::Value) -> String {
    let shown = input.to_string();
    cog_core::contract::prompt::split_contract(input)
        .0
        .unwrap_or_else(|| panic!("no stable half at all in {shown}"))
}

/// Every actor must hand its stable half over under the key the runtime reads,
/// and keep it out of the varying half.
///
/// The split is an agreement between two crates: the actors write it, the agent
/// runtime reads it (see `cog_core::contract::prompt`). Pinning only one side
/// would let the other drift silently — an actor that stops writing the key
/// keeps compiling, keeps running, and quietly returns to paying full price for
/// a prompt that never changes.
///
/// Two attempts are read per actor, because the property is a comparison.
/// Asserting only that the key is present would pass on a contract that carries
/// this attempt's goal, and on an actor that sends one document twice; and a
/// stable half that moved between attempts is worth nothing to the cache no
/// matter how well formed it is.
#[tokio::test]
async fn every_actor_hands_its_stable_half_over_under_the_contract_key() {
    for (role, agent, material) in two_prompts_from_every_actor().await {
        let inputs = agent.inputs();
        assert_eq!(inputs.len(), 2, "{role} was expected to send two documents");
        let split: Vec<_> = inputs
            .into_iter()
            .map(cog_core::contract::prompt::split_contract)
            .collect();

        let contract = split[0]
            .0
            .as_ref()
            .unwrap_or_else(|| panic!("{role} sent no stable half at all"));
        assert!(
            !contract.trim().is_empty(),
            "{role}'s stable half says nothing, so it caches nothing"
        );
        assert_eq!(
            split[0].0, split[1].0,
            "{role}'s stable half moved between two attempts, so nothing is cached"
        );

        for (attempt, (_, payload)) in split.iter().enumerate() {
            assert!(
                payload
                    .get(cog_core::contract::prompt::PROMPT_CONTRACT_KEY)
                    .is_none(),
                "{role} left the stable half in the varying half as well (attempt {attempt})"
            );
            // The varying half is still the varying half: what this attempt is
            // about has to reach the model somewhere.
            assert!(
                payload.get(material).is_some(),
                "{role} dropped {material} from the varying half (attempt {attempt})"
            );
        }
        assert_ne!(
            split[0].1, split[1].1,
            "{role} sent the same varying half twice, so the comparison above proves nothing"
        );
    }
}

/// The stable half names the answers the actor's own parser reads, spelled the
/// way that parser reads them.
///
/// Both halves of that agreement are on this side of the wire: the shape the
/// reply is deserialized into, and the words it is matched against. The contract
/// is the only place the model can learn them, so one word dropped leaves it
/// guessing — and the reader downstream will call the guess a decision.
/// Spelling counts as much as the word: a parser that matches `change_strategy`
/// reads `ChangeStrategy` as none of its alternatives, so a contract naming the
/// enum variant would have the model answer a word that lands on the default.
/// These strings are the parsers' own keys and match arms, not the type names.
#[tokio::test]
async fn every_stable_half_names_the_answers_its_own_parser_reads() {
    let parser_words: [(&str, &[&str]); 6] = [
        ("planner", &["summary", "sub_tasks"]),
        ("generator", &["content", "artifacts"]),
        ("evaluator", &["verdict", "feedback", "score", "criteria"]),
        (
            "moderator",
            &["continue", "change_strategy", "accept_partial", "escalate"],
        ),
        ("merger", &["selected_branch_id", "outcome", "reasoning"]),
        ("mode_selector", &["Pipeline", "Roundtable"]),
    ];

    let roles = two_prompts_from_every_actor().await;
    for (role, _, _) in &roles {
        assert!(
            parser_words.iter().any(|(r, _)| r == role),
            "{role} assembles a prompt but has no parser vocabulary pinned, so \
             nothing checks that the model is told what to answer"
        );
    }
    for (role, words) in parser_words {
        let agent = roles
            .iter()
            .find(|(r, _, _)| *r == role)
            .map(|(_, a, _)| a)
            .unwrap_or_else(|| panic!("{role} sent no document to read"));
        let contract = stable_half(agent.inputs()[0].clone());
        for word in words {
            assert!(
                contract.contains(word),
                "{role}'s stable half no longer names {word:?}, which its parser reads: {contract}"
            );
        }
    }
}

/// The words the contracts name are the words the parsers take.
///
/// The guard above compares two lists this test file writes down: the contract's
/// text and the vocabulary read off the parsers. Two hand-written lists agree
/// with each other, not with the code — a parser that renamed an arm would leave
/// both stale and the guard green. So the vocabulary is read out of the parsers
/// here by running them, on the words the contract names. This is the one place
/// the failure is silent: a decision word the moderator's parse does not
/// recognize lands on its default `continue`, so a round would carry on and the
/// reading would show a decision nobody made.
#[test]
fn the_words_the_contracts_name_are_the_words_the_parsers_take() {
    // Every decision the contract offers has to come back as itself, not as the
    // default it would fall to if the arm were renamed.
    for (word, expected) in [
        ("continue", ModeratorDecision::Continue),
        ("change_strategy", ModeratorDecision::ChangeStrategy),
        ("accept_partial", ModeratorDecision::AcceptPartial),
        ("escalate", ModeratorDecision::Escalate),
    ] {
        let parsed = parse_moderator_output(&serde_json::json!({ "decision": word }));
        assert_eq!(
            parsed.decision, expected,
            "the moderator's contract tells the model to answer {word:?}, which its parse does not take"
        );
    }

    // And the merger's instruction carried out literally: the reply the contract
    // describes is the chosen branch's own fields, plus the two this actor adds.
    // A contract naming a field the parse does not take would send the model to
    // a reply that falls back to the deterministic pick instead.
    let branch = serde_json::to_value(a_branch(2)).unwrap();
    let reply = serde_json::json!({
        "plan": branch["plan"],
        "generation": branch["generation"],
        "outcome": branch["outcome"],
        "selected_branch_id": branch["branch_id"],
        "reasoning": "highest score among the judged branches",
    });
    let merged = parse_merge_result(&reply, &[a_branch(2)]);
    assert_eq!(
        merged.reasoning, "highest score among the judged branches",
        "a reply the merger's contract describes was not taken by its parse"
    );
    assert_eq!(merged.selected_branch_id, Some(2));
}
