//! What a decay pass removes has to be nameable afterwards.
//!
//! The count on its own cannot say whether a threshold is set too high: the
//! question a decay setting raises is *which* entries it is taking, and by the
//! time the report comes back the entries are gone from the store. So the
//! backend names each one as it removes it, and these two tests hold the two
//! halves of that: a dropped entry is named, and an entry that only lost
//! importance -- still searchable, just lower down -- is not. The second is the
//! one that matters, because demoting and dropping are the two things a decay
//! pass does, and a reading that reports both as a drop is worse than none.
//!
//! The subscriber here is only a capture layer over the real field values, so
//! what the assertions read is what a log backend would receive.

use std::sync::{Arc, Mutex};

use chrono::{Duration, Utc};
use cog_core::{MemoryBackend, SourceRef, SummaryEntry};
use cog_memory::{CompositeMemoryBackend, MemoryMemoryBackend};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

const NAMESPACE: &str = "drop-log-test";
const MESSAGE: &str = "left the searchable layer";

/// One emitted event, reduced to the fields the drop record is made of.
#[derive(Clone, Debug, PartialEq)]
struct Dropped {
    message: String,
    namespace: Option<String>,
    entry_id: Option<String>,
    raw_uri: Option<String>,
}

/// Pulls the fields off an event the way the Loki layer does, so the assertions
/// run against the delivered values rather than against a prettier copy.
#[derive(Default)]
struct PickFields {
    message: Option<String>,
    namespace: Option<String>,
    entry_id: Option<String>,
    raw_uri: Option<String>,
}

impl PickFields {
    fn set(&mut self, name: &str, plain: String) {
        let slot = match name {
            "message" => &mut self.message,
            "namespace" => &mut self.namespace,
            "entry_id" => &mut self.entry_id,
            "raw_uri" => &mut self.raw_uri,
            _ => return,
        };
        *slot = Some(plain);
    }
}

impl Visit for PickFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%x` reaches a visitor through `record_debug` with the value still
        // debug-quoted, which is why every site that reads these unquotes.
        let text = format!("{value:?}");
        let plain = text
            .strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .map(str::to_string)
            .unwrap_or(text);
        self.set(field.name(), plain);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field.name(), value.to_string());
    }
}

/// Collects every event into a sink the caller keeps a handle on, so the
/// subscriber can be handed over by value without taking the results with it.
struct DropCapture {
    events: Arc<Mutex<Vec<Dropped>>>,
}

impl<S: tracing::Subscriber> Layer<S> for DropCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut pick = PickFields::default();
        event.record(&mut pick);
        self.events.lock().unwrap().push(Dropped {
            message: pick.message.unwrap_or_default(),
            namespace: pick.namespace,
            entry_id: pick.entry_id,
            raw_uri: pick.raw_uri,
        });
    }
}

/// A summary that has aged past the threshold and sits under the archive floor,
/// so one pass both accepts it and drives it out of the searchable layer.
fn aged_and_unimportant() -> SummaryEntry {
    let mut entry = SummaryEntry::new(
        "summary-raw-1",
        NAMESPACE,
        "an old, unimportant summary",
        Vec::new(),
        cog_core::NO_EMBEDDING_MODEL,
        SourceRef::new("memory://raw-1", "rule_based/v1"),
    )
    .with_importance(0.01);
    entry.generated_at = Utc::now() - Duration::hours(48);
    entry
}

/// Run one decay pass over the fixture under a capture layer, and give back both
/// the report's count and everything the pass said.
///
/// Takes the backend rather than building one, because both implementations do
/// this and a reading emitted by only one of them is a reading with a hole.
fn one_pass(backend: &dyn MemoryBackend, entry: SummaryEntry) -> (usize, Vec<Dropped>) {
    let events: Arc<Mutex<Vec<Dropped>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(DropCapture {
        events: events.clone(),
    });

    let archived = tracing::subscriber::with_default(subscriber, || {
        futures::executor::block_on(async {
            backend
                .store_summary(NAMESPACE, &entry)
                .await
                .expect("the fixture is storable");
            backend
                .decay(NAMESPACE, 3600, 0.2)
                .await
                .expect("the pass runs")
                .entries_archived
        })
    });

    let events = events.lock().unwrap().clone();
    (archived, events)
}

/// The dropped entry's name is on the reading, with enough beside it to go back
/// to the bytes it summarised.
fn assert_named_on_the_reading(archived: usize, events: &[Dropped]) {
    assert_eq!(
        archived, 1,
        "the fixture has to actually leave, or nothing is proven"
    );
    assert_eq!(
        events.len(),
        1,
        "one entry left, so one entry is named; got {events:?}"
    );
    let named = &events[0];
    assert!(
        named.message.contains(MESSAGE),
        "the capture has to be the drop event; got {:?}",
        named.message
    );
    assert_eq!(
        named,
        &Dropped {
            message: named.message.clone(),
            namespace: Some(NAMESPACE.to_string()),
            entry_id: Some("summary-raw-1".to_string()),
            raw_uri: Some("memory://raw-1".to_string()),
        },
        "the record has to carry the entry's identity and where its content still lives"
    );
}

fn assert_only_demoted(archived: usize, events: &[Dropped]) {
    assert_eq!(archived, 0, "half of 0.5 is still above the archive floor");
    assert!(
        events.is_empty(),
        "an entry that stayed searchable must not read as one that left; got {events:?}"
    );
}

#[test]
fn a_dropped_entry_is_named_on_the_reading() {
    let (archived, events) = one_pass(&MemoryMemoryBackend::new(), aged_and_unimportant());
    assert_named_on_the_reading(archived, &events);
}

/// Demotion is not a drop. The entry is still searchable afterwards, just lower
/// down, and a reading that counted it would credit the threshold with a loss it
/// never caused.
#[test]
fn an_entry_that_was_only_demoted_is_not_named() {
    let mut demoted = aged_and_unimportant();
    demoted.importance = 0.5;

    let (archived, events) = one_pass(&MemoryMemoryBackend::new(), demoted);
    assert_only_demoted(archived, &events);
}

/// The composite backend is the one a deployment with persistence selects, so
/// the same two halves have to hold there. The call sites are separate lines in
/// separate files; a shared helper is not what keeps them in step.
#[test]
fn the_composite_backend_names_its_drop_too() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = CompositeMemoryBackend::new(
        Arc::new(cog_storage::FileObjectBackend::new(tmp.path())),
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        4,
    );

    let (archived, events) = one_pass(&backend, aged_and_unimportant());
    assert_named_on_the_reading(archived, &events);
}

#[test]
fn the_composite_backend_does_not_name_a_demotion() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = CompositeMemoryBackend::new(
        Arc::new(cog_storage::FileObjectBackend::new(tmp.path())),
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        4,
    );

    let mut demoted = aged_and_unimportant();
    demoted.importance = 0.5;

    let (archived, events) = one_pass(&backend, demoted);
    assert_only_demoted(archived, &events);
}
