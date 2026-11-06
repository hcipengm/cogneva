//! Where a PGE run's stage products live when the process that produced them
//! does not.
//!
//! One run of the plan-generation-evaluation pipeline is a chain of up to seven
//! inference calls, and each one produces something the next one consumes and
//! nobody else can rebuild for free: the planner's plan, the generator's diff,
//! the evaluator's verdict. Until this port existed they lived in the pipeline's
//! local variables, so a run that stopped halfway — the ordinary case is an
//! upstream quota running out between two calls — took every one of them with
//! it, and the next pass over the same task bought them again.
//!
//! The raw request and response bodies are already kept losslessly by the
//! gateway's exchange ledger. What that ledger cannot answer is *which attempt's
//! which stage* a body belonged to: it indexes by time and actor, and a run's
//! seven calls look alike there. This record is the structured half — one row per
//! stage of one generation of one task, carrying the product itself and the
//! identity needed to place it — so the two together say what was bought, by
//! which stage, and what it produced.
//!
//! The task id is the run's own, not the id of the throwaway request each pass is
//! issued under. A change handed over by the ledger is dispatched under an id its
//! identity fixes, so the ledger row can be recomputed into this key without
//! anything having been stored to connect them; interactive tasks carry their own
//! id and no change at all.

use async_trait::async_trait;
use serde_json::Value;

use crate::SFResult;

/// Where a dispatched task carries the identity of the change it was handed over
/// for, in its input.
///
/// One word rather than two literals: the ledger writes it into the task and the
/// stage product copies it out, and a reader that spelled a different word would
/// find no identity on every row the ledger dispatched.
pub const INPUT_CHANGE_IDENTITY: &str = "change_identity";

/// Where a dispatched task carries **every** change it was handed over for, in
/// its input.
///
/// A handover that carries one change writes one identity and this list of one,
/// so a reader never has to ask which of the two shapes it is looking at: the
/// single word above is the one the stage archive copies and the one the
/// account's first row is known by, and this list is what a reader that has to
/// settle *all* of them reads. A task that carried only the first identity
/// would leave the other changes it landed looking unhanded-over, and the next
/// pass would hand them over a second time.
pub const INPUT_CHANGE_IDENTITIES: &str = "change_identities";

/// How far a generation got.
///
/// Ordered: a later stage means the generation reached further, and a reader
/// resuming a run needs the furthest one without having to know the pipeline's
/// control flow. The variants are the three products a pass can leave behind,
/// not the seven calls — a repair is the same stage at a later attempt count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PgeStage {
    /// The planner's plan. Written before any generation is asked for, so a run
    /// whose generator never reached its upstream still leaves the plan it paid
    /// for.
    Planned,
    /// The generator's output — the envelope carrying the diff.
    Generated,
    /// The evaluator's verdict on that generation.
    Evaluated,
}

impl PgeStage {
    /// The word this stage is stored and logged under.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Generated => "generated",
            Self::Evaluated => "evaluated",
        }
    }

    /// The stage's order as a number, for a store that sorts without knowing
    /// the words.
    ///
    /// Spelled here rather than in the store's query: a query ordering by a
    /// `CASE` over stage names would be a second definition of this enum, and
    /// adding a stage would move the order in one place and not the other.
    pub fn rank(self) -> i16 {
        match self {
            Self::Planned => 1,
            Self::Generated => 2,
            Self::Evaluated => 3,
        }
    }

    /// The word back to a stage; `None` for a word this enum does not spell.
    ///
    /// A row carrying an unknown stage is not read as the nearest one it
    /// resembles: a deployment writing a stage this build does not know about
    /// would otherwise have its product mis-placed by an older build, and the
    /// resume would start from something that was never the furthest point.
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "planned" => Some(Self::Planned),
            "generated" => Some(Self::Generated),
            "evaluated" => Some(Self::Evaluated),
            _ => None,
        }
    }
}

/// One stage of one generation of one task, as it was produced.
///
/// `product` is the stage's own output and nothing else: the plan for
/// [`PgeStage::Planned`], the generator's envelope for
/// [`PgeStage::Generated`], the verdict for [`PgeStage::Evaluated`]. It does not
/// repeat the stages before it — those are their own rows, and a copy of the
/// diff inside the verdict's row would be a second original that could disagree
/// with the first.
#[derive(Debug, Clone, PartialEq)]
pub struct PgeStageProduct {
    /// The task whose run produced this, as the task graph knows it.
    pub task_id: String,
    /// The pipeline's attempt counter within that run, from one.
    pub generation: u32,
    /// Which repair of that attempt produced it; zero is the first generation.
    pub repair_iteration: u32,
    pub stage: PgeStage,
    /// The request this stage answered, hashed.
    ///
    /// Resume is only safe when the request is the same one: a task id can be
    /// re-issued against a different goal, and a generation produced for the
    /// previous goal is not an answer to this one. The hash is what compares;
    /// the request text itself lives once in the exchange ledger rather than
    /// being copied onto every row of every stage.
    pub request_sha256: String,
    /// The change identity the task carries, when it was dispatched for one.
    ///
    /// Copied rather than derived here so a reader of this table alone can place
    /// the product without knowing the ledger's hash scheme; the ledger remains
    /// the authority on what the identity means.
    pub change_identity: Option<Value>,
    /// The stage's output, verbatim.
    pub product: Value,
}

/// Durable storage for PGE stage products.
///
/// Both methods are about one task's generations: writing one stage down as it
/// is produced, and reading back what an earlier pass already bought for a
/// generation. There is no delete and no edit — a stage product is what the
/// model said at that moment, and a store that could rewrite one would make the
/// record a current opinion rather than evidence.
#[async_trait]
pub trait PgeStageStore: Send + Sync {
    /// Write one stage down, replacing whatever that key held.
    ///
    /// The key is `(task_id, generation, repair_iteration, stage)`. A rewrite is
    /// the same product produced again after a process restart, or a repair that
    /// reached the same index a second time; either way the newest bytes are the
    /// ones that describe the run that actually continued.
    async fn record(&self, product: &PgeStageProduct) -> SFResult<()>;

    /// Every stage product a run has for one generation, ordered by repair
    /// iteration and then by how far the stage got.
    ///
    /// All of them rather than the furthest, because the caller resuming a run
    /// needs the plan even when the generator never ran, and a reader auditing
    /// one needs to see that the plan exists without a generation beside it.
    /// Empty when this task never reached that generation.
    async fn generation(&self, task_id: &str, generation: u32) -> SFResult<Vec<PgeStageProduct>>;
}
