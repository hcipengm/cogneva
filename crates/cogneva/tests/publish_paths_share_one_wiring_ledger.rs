//! Gate: the two publish paths are one wiring ledger, so one difference
//! between published and read covers both of them.
//!
//! `PluginContext` offers four verbs on two spellings: `publish`/`consume`/
//! `require` for values, and `publish_service`/`consume_service`/
//! `require_service` for trait objects. Read as two *registries*, that suggests
//! the "published but nobody read it" difference has to be computed twice --
//! once per pair of verbs -- and that a reader who computes it once sees only
//! half the graph. The suggestion is wrong in the direction that hides: both
//! publish verbs record into one ledger, both read verbs record into the same
//! one, and `PinAudit::evaluate` answers over the union without asking which
//! spelling a pin came through. One difference therefore covers both.
//!
//! What the two spellings really are is two **id spaces**: `Pin::of::<T>()`
//! keys on `TypeId::of::<T>()` and `Pin::of_service::<T>()` on
//! `TypeId::of::<Service<T>>()`, so the same type published through one and
//! read through the other produces two pins that never meet. That break is a
//! pairing problem -- two records that do not name each other -- and pairing
//! them is a separate question from how many differences to take.
//!
//! So the rule this reads: **a publish through either verb is visible to the
//! same difference, and a read through that verb's own counterpart retires it.**
//! The cases below hold a fixture with one pin on each path; a change that gave
//! either path its own ledger, or that stopped recording reads, would red here
//! while the two-record report of a cross-path break stayed intact.
//!
//! Its ceiling, stated once: this reads the ledger's *coverage*, not its
//! correctness. It does not check that every plugin that should publish does,
//! and it does not pair the two halves of a cross-path break -- that pairing is
//! only a suspicion to be confirmed by a person, because a shared trailing type
//! name is not a shared identity.
//!
//! principle: feedback_two_identities_for_one_type_means_a_publish_and_its_read_never_meet
//! principle: feedback_a_split_the_design_asks_for_must_be_checked_against_the_store_that_backs_it

use std::sync::Arc;

use cog_core::{Config, PinAudit, PluginContext};

/// Published through the trait-object path.
trait Marker: Send + Sync {}

struct MarkerImpl;

impl Marker for MarkerImpl {}

/// Published through the value path.
struct ConcreteThing;

/// A context holding one pin published through each of the two paths.
fn context_with_one_pin_on_each_path() -> PluginContext {
    let ctx = PluginContext::new(Config::default());
    ctx.publish_service::<dyn Marker>(Arc::new(MarkerImpl));
    ctx.publish(Arc::new(ConcreteThing));
    ctx
}

fn names(pins: impl Iterator<Item = &'static str>) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = pins.collect();
    out.sort_unstable();
    out
}

/// Both spellings record into the one ledger, and a single difference over it
/// reports both pins -- so no reader has to take the difference twice.
#[test]
fn one_difference_reports_pins_published_through_either_path() {
    let ctx = context_with_one_pin_on_each_path();
    let wiring = ctx.pin_wiring();

    let published = names(wiring.iter().map(|w| w.pin.name()));
    assert_eq!(
        published.len(),
        2,
        "both publishes are in the one wiring ledger: {published:?}"
    );
    assert!(
        published.iter().any(|n| n.ends_with("ConcreteThing")),
        "the value path's pin is in the ledger: {published:?}"
    );
    assert!(
        published.iter().any(|n| n.ends_with("Marker")),
        "the trait-object path's pin is in the ledger: {published:?}"
    );

    let audit = PinAudit::evaluate(&wiring, &[]);
    assert_eq!(
        audit.publishes, 2,
        "the fixture published through both paths: {audit:?}"
    );

    // Neither was read, so the single `unconsumed` family carries both. This is
    // the assertion that fails if either path stops recording into the ledger,
    // or if the ledger is split in two.
    let unread = names(audit.unconsumed.iter().map(|(pin, _)| pin.name()));
    assert_eq!(
        unread.len(),
        2,
        "one difference sees a publish from either path: {unread:?}"
    );
}

/// A read through a path's own counterpart retires that path's pin, and only
/// that one: the difference is taken per pin over the shared ledger, not per
/// path over a table of its own.
#[test]
fn a_read_retires_the_pin_its_own_verb_path_published() {
    let ctx = context_with_one_pin_on_each_path();

    assert!(
        ctx.consume_service::<dyn Marker>().is_some(),
        "the trait object publishes under the name its consumer reads"
    );
    assert!(
        ctx.consume::<ConcreteThing>().is_some(),
        "the value publishes under the name its consumer reads"
    );

    let audit = PinAudit::evaluate(&ctx.pin_wiring(), &[]);
    assert_eq!(audit.publishes, 2, "{audit:?}");
    assert_eq!(audit.reads, 2, "both reads were recorded: {audit:?}");
    assert!(
        audit.unconsumed.is_empty(),
        "two publishes and two reads is not an unread pin: {:?}",
        audit.unconsumed
    );
    assert!(
        audit.unsatisfied.is_empty(),
        "two reads whose publisher is present are not unsatisfied: {:?}",
        audit.unsatisfied
    );
}
