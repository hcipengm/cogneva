//! Taste intents: the value/evaluation row of evolution inputs.
//!
//! Every other evolution input this system produces is a *defect*: a failing
//! task, a firing alert, a queue that stopped draining. Those say where it
//! hurts. None of them says whether a claimed result holds up, whether one
//! direction is worth more than another, or what the standard of "good" should
//! be. This module is
//! the vocabulary for that second row: a judgement stated from outside the
//! pipeline, recorded durably, and then handed to the same intent
//! cooldown/idempotence frame every other signal goes through.
//!
//! Every kind here is a **stated** judgement: someone said it. That is
//! deliberate in both directions.
//!
//! A judgement nobody states cannot be expressed here, so a system with no
//! submitters has no signal on this channel — silence here says nothing about
//! what is valuable, only that nobody spoke. A channel with that property must
//! not be a system's only taste source.
//!
//! And a signal that exists only as an aggregate of behaviour — what other
//! agents happen to reuse, delegate or re-order — is not one of these kinds.
//! Aggregate behaviour is correlational: it can be a cheap prior filter, but it
//! cannot be a verdict, and a verdict is what this vocabulary records. The
//! final say stays with a stated judgement or with the evaluation gate.
//!
//! Two traits split the durable store: [`TasteIntentSink`] is the face
//! submissions enter through, [`TasteIntentSource`] is the face that decides
//! what became of them. They are separate because the two are held by
//! different processes — the surface that accepts a submission has no business
//! deciding whether it becomes work.

use crate::SFResult;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Longest a free-text field of a submission may be, in characters.
///
/// These fields are inlined into the goal handed to the task layer, and a
/// task's input is carried into the change its squad produces. An unbounded
/// one therefore costs more on every later copy of the same row, for the same
/// content. Submissions are interactive, so the bound is enforced by refusing
/// the submission rather than by truncating it: a truncated rationale is a
/// submitter's words with the reason removed, presented as if it were theirs.
pub const TASTE_TEXT_MAX_CHARS: usize = 2_000;

/// Longest a subject may be, in characters.
pub const TASTE_SUBJECT_MAX_CHARS: usize = 512;

/// What the evidence offered for a claim turned out to be worth.
///
/// Three answers, and they are the whole domain: the evidence does not
/// establish the claim, the evidence contradicts it, or it does establish it.
/// A verdict only needs stating when it is not the default, but the third
/// answer is what makes a repeated doubt distinguishable from a resolved one,
/// and it is the answer a submitter gives when they are confirming rather than
/// objecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluatorVerdict {
    /// The evidence does not establish the claim it is offered for.
    Insufficient,
    /// The evidence points the other way.
    Contradicted,
    /// The evidence does establish it.
    Confirmed,
}

impl EvaluatorVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insufficient => "insufficient",
            Self::Contradicted => "contradicted",
            Self::Confirmed => "confirmed",
        }
    }

    /// Every value the verdict takes. Published as a list so a reader can see
    /// the whole domain rather than the values that happen to have occurred.
    pub const ALL: &'static [EvaluatorVerdict] = &[
        EvaluatorVerdict::Insufficient,
        EvaluatorVerdict::Contradicted,
        EvaluatorVerdict::Confirmed,
    ];
}

/// A judgement about whether the evidence offered for a claim holds up.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluatorVerdictIntent {
    pub verdict: EvaluatorVerdict,
    /// Why, in the submitter's own words. The verdict alone cannot be turned
    /// into work; this is what a squad acts on.
    pub rationale: String,
}

/// A preference for one direction of work over another.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirectionPreferenceIntent {
    /// The direction the submitter prefers.
    pub preferred: String,
    /// What it is preferred over. Named rather than left implicit: a preference
    /// with one side stated is a suggestion to do something, not a choice
    /// between two things, and the work that follows is different.
    pub alternative: String,
    /// Why, in the submitter's own words.
    pub rationale: String,
}

/// A change to the standard against which work is judged.
///
/// The one kind that judges the judging: the other two say something about a
/// result, this one says the criterion the results are being read against is
/// wrong. It is a stated judgement like the others, and it is the kind most
/// worth recording verbatim — the previous wording it replaces is not carried
/// here, because the durable record of what the criterion used to say is the
/// change this intent produces, not the submitter's recollection of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RubricOverrideIntent {
    /// The criterion being changed, named as the system names it, so the squad
    /// can find what it is supposed to edit.
    pub criterion: String,
    /// What it should say instead.
    pub new_wording: String,
    /// Why, in the submitter's own words.
    pub rationale: String,
}

/// The content of one taste intent, tagged by kind.
///
/// The tag is part of the stored form: a submission is kept as JSON under this
/// enum, so the tag a variant writes here is also the value a reader sees in
/// the record, and it is what the repeat cooldown is keyed on. Renaming a
/// variant therefore is not a local edit — it renames stored evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TasteIntentPayload {
    EvaluatorVerdict(EvaluatorVerdictIntent),
    DirectionPreference(DirectionPreferenceIntent),
    RubricOverride(RubricOverrideIntent),
}

impl TasteIntentPayload {
    /// The kind name of this payload, as it is stored and as it is keyed.
    ///
    /// Derived from the variant rather than carried beside it: a second copy of
    /// the kind in the record would be a second answer to which kind this is,
    /// and the two could disagree. A test pins this string to the tag the
    /// stored form actually writes.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::EvaluatorVerdict(_) => "evaluator_verdict",
            Self::DirectionPreference(_) => "direction_preference",
            Self::RubricOverride(_) => "rubric_override",
        }
    }

    /// One line saying what was judged, for assembling a task goal without the
    /// caller having to know the shape of each kind.
    ///
    /// Newlines are flattened: the line is inlined into a goal, and a goal that
    /// arrives in several pieces reads as several instructions.
    pub fn summary(&self) -> String {
        let line = match self {
            Self::EvaluatorVerdict(v) => {
                format!("evidence judged {}: {}", v.verdict.as_str(), v.rationale)
            }
            Self::DirectionPreference(p) => format!(
                "prefers \"{}\" over \"{}\": {}",
                p.preferred, p.alternative, p.rationale
            ),
            Self::RubricOverride(r) => format!(
                "the evaluation criterion \"{}\" should read \"{}\": {}",
                r.criterion, r.new_wording, r.rationale
            ),
        };
        line.replace(['\n', '\r'], " ")
    }
}

/// One submission, as it was made.
///
/// `id` is minted per submission rather than derived from its content. Two
/// submitters who say the same thing are two pieces of evidence, and each one's
/// identity has to survive to the record — a content-addressed id would fold
/// them into one row and drop the second submitter's name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TasteIntent {
    pub id: Uuid,
    /// What the judgement is about, named by the submitter.
    ///
    /// This is the identity the repeat cooldown is keyed on, together with the
    /// kind: the same judgement about the same subject submitted twice inside
    /// one cooldown is one piece of work. It suppresses repeats, it is not a
    /// gate against a submitter — naming one thing two ways makes two subjects,
    /// and the second submission then is not held back.
    pub subject: String,
    /// Who submitted it. Recorded, never inferred.
    pub submitted_by: String,
    pub submitted_at: DateTime<Utc>,
    pub payload: TasteIntentPayload,
}

impl TasteIntent {
    /// Reject a submission that cannot be stored as it stands.
    ///
    /// The bounds belong to the vocabulary, not to the surface that accepts it:
    /// the same record arrives from an HTTP body today and from a producer
    /// inside the system later, and a rule checked at one entrance is not a
    /// rule about the record.
    pub fn validate(&self) -> Result<(), String> {
        if self.subject.trim().is_empty() {
            return Err("subject must name what the judgement is about".into());
        }
        if self.subject.chars().count() > TASTE_SUBJECT_MAX_CHARS {
            return Err(format!(
                "subject is longer than {TASTE_SUBJECT_MAX_CHARS} characters"
            ));
        }
        if self.submitted_by.trim().is_empty() {
            return Err("submitted_by must name the submitter".into());
        }
        for (field, text) in self.text_fields() {
            if text.trim().is_empty() {
                return Err(format!("{field} must not be empty"));
            }
            if text.chars().count() > TASTE_TEXT_MAX_CHARS {
                return Err(format!(
                    "{field} is longer than {TASTE_TEXT_MAX_CHARS} characters"
                ));
            }
        }
        Ok(())
    }

    /// The free-text fields this submission carries, by name.
    fn text_fields(&self) -> Vec<(&'static str, &str)> {
        match &self.payload {
            TasteIntentPayload::EvaluatorVerdict(v) => vec![("rationale", v.rationale.as_str())],
            TasteIntentPayload::DirectionPreference(p) => vec![
                ("preferred", p.preferred.as_str()),
                ("alternative", p.alternative.as_str()),
                ("rationale", p.rationale.as_str()),
            ],
            TasteIntentPayload::RubricOverride(r) => vec![
                ("criterion", r.criterion.as_str()),
                ("new_wording", r.new_wording.as_str()),
                ("rationale", r.rationale.as_str()),
            ],
        }
    }
}

/// What became of one submission.
///
/// These are stored, so their spellings are evidence: renaming one leaves rows
/// in the store that no reader can name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TasteDisposition {
    /// Handed to the task layer. The submission is now the lineage of the work
    /// it produced, and is kept for that reason.
    Filed,
    /// Kept as evidence, filed nowhere: a judgement about the same subject, of
    /// the same kind, was already in hand inside its cooldown. The submitter
    /// agreed with work that is already being done, which is worth keeping and
    /// not worth a second task.
    Superseded,
}

impl TasteDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filed => "filed",
            Self::Superseded => "superseded",
        }
    }

    /// Every value the disposition takes.
    pub const ALL: &'static [TasteDisposition] =
        &[TasteDisposition::Filed, TasteDisposition::Superseded];
}

/// A submission as the store holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTasteIntent {
    pub intent: TasteIntent,
    /// The task the submission was handed to, once it has been filed; `None`
    /// while nothing has claimed it. The store keeps it rather than letting the
    /// filer derive it again, because this is the join between the submission
    /// and whatever work it produced, and a join recomputed by the reader is a
    /// join that can stop matching the row.
    pub task_id: Option<String>,
}

/// Write side: where a submission is recorded.
///
/// Published by whichever plugin owns the durable store, so a surface that
/// accepts submissions does not have to own a database.
#[async_trait]
pub trait TasteIntentSink: Send + Sync {
    /// Record one submission durably, before anything acts on it.
    ///
    /// Recording comes first because the submission *is* the evidence: work
    /// that was produced from a submission nobody stored cannot be read back
    /// against what was actually asked for, and the submitter's own words are
    /// the only copy.
    ///
    /// A duplicate is not refused here. Repeats are held back when the
    /// submission is filed, keyed on (kind, subject) by the same cooldown every
    /// other signal goes through; deciding it here as well would be a second
    /// answer to a question the filer already owns, and the two would disagree
    /// the moment either changed.
    async fn submit(&self, intent: &TasteIntent) -> SFResult<()>;
}

/// Read side over stored submissions, and the disposal of what it read.
///
/// The reader is the only party that may decide a submission's fate, and it
/// decides it in the same place it read the row: whether a submission becomes
/// work or stays as evidence depends on work that is already in hand, and a
/// party that did not read the row cannot know that. Splitting the read from
/// the disposal would let the two disagree about what a row means.
#[async_trait]
pub trait TasteIntentSource: Send + Sync {
    /// Submissions nothing has been filed for yet, oldest first, or `None` when
    /// the lookup itself failed.
    ///
    /// The two are not interchangeable: a caller that read `None` as "nothing
    /// was submitted" would silently drop every submission made while the store
    /// was unreachable, and there is no later round that recovers them. A
    /// source that answers `Some(vec![])` has looked and found nothing.
    async fn pending_intents(&self, limit: i64) -> Option<Vec<StoredTasteIntent>>;

    /// Submissions that were handed to the task layer, filed no earlier than
    /// `filed_since`, oldest filing first, or `None` when the lookup failed.
    ///
    /// This side exists because filing is not the same as finishing. A
    /// submission whose work failed is unfinished work, not a repeat, and the
    /// window is how far back that is worth looking: past it the row is
    /// lineage, and lineage is read by people, not driven every round.
    ///
    /// The window runs from when the work was handed over, not from when the
    /// submission was made. Those are the same instant for a submission filed
    /// the round it arrived, and they come apart exactly when it matters: a row
    /// that sat unclaimed through an outage and was filed afterwards would fall
    /// out of a window measured from its submission on the round its work was
    /// first read, and a failure there would be looked for never.
    async fn filed_intents(
        &self,
        filed_since: DateTime<Utc>,
        limit: i64,
    ) -> Option<Vec<StoredTasteIntent>>;

    /// Record what became of one submission, once, together with the task it
    /// went to.
    ///
    /// Only the first disposal counts: a row already disposed is left as it
    /// was, so a caller recovering from a crash between filing and disposing
    /// cannot rewrite the record of what happened.
    async fn dispose(
        &self,
        id: Uuid,
        disposition: TasteDisposition,
        task_id: Option<&str>,
    ) -> SFResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payloads() -> Vec<TasteIntentPayload> {
        vec![
            TasteIntentPayload::EvaluatorVerdict(EvaluatorVerdictIntent {
                verdict: EvaluatorVerdict::Insufficient,
                rationale: "the benchmark only ran on the training split".into(),
            }),
            TasteIntentPayload::DirectionPreference(DirectionPreferenceIntent {
                preferred: "latency".into(),
                alternative: "throughput".into(),
                rationale: "the queue is idle and the p99 is not".into(),
            }),
            TasteIntentPayload::RubricOverride(RubricOverrideIntent {
                criterion: "review_depth".into(),
                new_wording: "counts findings a second reader would agree with".into(),
                rationale: "the current wording rewards volume".into(),
            }),
        ]
    }

    /// The kind a payload answers with is the tag its stored form writes. The
    /// two spellings are a rename apart — one in the derive, one in the match —
    /// and a rename of either would silently re-key every stored submission's
    /// cooldown and orphan the rows written before it.
    #[test]
    fn the_kind_a_payload_answers_with_is_the_tag_it_stores() {
        for payload in payloads() {
            let stored = serde_json::to_value(&payload).expect("payload serializes");
            assert_eq!(
                stored.get("kind").and_then(|v| v.as_str()),
                Some(payload.kind()),
                "{payload:?} answers with a kind its stored form does not write"
            );
        }
    }

    /// A payload survives the round trip through the form it is stored in.
    /// Submissions are kept as JSON and read back by a different process than
    /// the one that wrote them, so the encoding is the whole record.
    #[test]
    fn a_payload_round_trips_through_its_stored_form() {
        for payload in payloads() {
            let text = serde_json::to_string(&payload).expect("payload serializes");
            let back: TasteIntentPayload =
                serde_json::from_str(&text).expect("payload deserializes");
            assert_eq!(back, payload);
        }
    }

    /// Every disposition has its own spelling, and those spellings are stored.
    #[test]
    fn every_disposition_has_its_own_spelling() {
        let spellings: Vec<&str> = TasteDisposition::ALL.iter().map(|d| d.as_str()).collect();
        assert_eq!(spellings, vec!["filed", "superseded"]);
    }

    /// Every verdict has its own spelling, and the domain is complete: the
    /// published list and the enum are the same three answers.
    #[test]
    fn every_verdict_has_its_own_spelling() {
        let spellings: Vec<&str> = EvaluatorVerdict::ALL.iter().map(|v| v.as_str()).collect();
        assert_eq!(spellings, vec!["insufficient", "contradicted", "confirmed"]);
    }

    /// A summary is one line. It is inlined into a goal, and a goal that
    /// arrives in pieces reads as several instructions.
    #[test]
    fn a_summary_stays_on_one_line() {
        for payload in payloads() {
            assert!(
                !payload.summary().contains('\n'),
                "summary broke into lines: {}",
                payload.summary()
            );
        }
        let multiline = TasteIntentPayload::EvaluatorVerdict(EvaluatorVerdictIntent {
            verdict: EvaluatorVerdict::Contradicted,
            rationale: "first line\nsecond line".into(),
        });
        assert_eq!(
            multiline.summary(),
            "evidence judged contradicted: first line second line"
        );
    }

    fn intent(payload: TasteIntentPayload) -> TasteIntent {
        TasteIntent {
            id: Uuid::new_v4(),
            subject: "change-42".into(),
            submitted_by: "op@example".into(),
            submitted_at: Utc::now(),
            payload,
        }
    }

    /// A well-formed submission passes.
    #[test]
    fn a_named_submission_validates() {
        for payload in payloads() {
            assert_eq!(intent(payload).validate(), Ok(()));
        }
    }

    /// A submission that names nothing is refused: the subject is the cooldown's
    /// key, and an unnamed one would let every submission about anything share
    /// one key. An empty rationale is refused for the other reason — it would
    /// produce a goal that states a judgement with no reason attached.
    #[test]
    fn an_unnamed_or_empty_submission_is_refused() {
        let mut unnamed = intent(payloads().remove(0));
        unnamed.subject = "  ".into();
        assert!(unnamed.validate().is_err());

        let mut anonymous = intent(payloads().remove(1));
        anonymous.submitted_by = String::new();
        assert!(anonymous.validate().is_err());

        let mut empty = intent(payloads().remove(2));
        if let TasteIntentPayload::RubricOverride(r) = &mut empty.payload {
            r.rationale = " ".into();
        }
        assert!(empty.validate().is_err());
    }

    /// The bound is on characters, not bytes, and it is enforced rather than
    /// trimmed: a submitter's reason with its tail removed reads as theirs.
    #[test]
    fn an_oversized_field_is_refused_and_never_trimmed() {
        let mut long = intent(payloads().remove(0));
        if let TasteIntentPayload::EvaluatorVerdict(v) = &mut long.payload {
            v.rationale = "汉".repeat(TASTE_TEXT_MAX_CHARS);
        }
        assert_eq!(long.validate(), Ok(()));

        if let TasteIntentPayload::EvaluatorVerdict(v) = &mut long.payload {
            v.rationale = "汉".repeat(TASTE_TEXT_MAX_CHARS + 1);
        }
        assert!(long.validate().is_err());
    }
}
