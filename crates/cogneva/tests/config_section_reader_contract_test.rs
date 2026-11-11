//! Every key the delivered document writes is read by something, and every
//! classified section has a reader.
//!
//! The section table (`cog_core::config_sections`) judges whole sections: a
//! section that arrives, one that is missing, one nobody classified. What it
//! cannot see is a key inside a section that no type has a field for — serde
//! passes such a key over without a word, so a knob the document sets reads as
//! applied while nothing uses it. `assert_sections_match` in cog-collaboration
//! decides that for the four sections that crate reads; every other section had
//! no such check, which is the gap `metrics` / `memory` / `system` drifted
//! through. This gate closes it for all of them, and it lives in the assembly
//! crate because that is the only place every section's type can be named.
//!
//! Two directions exist, and only one of them is asserted here:
//!
//! - A key the document writes must have a field behind it. Asserted: any
//!   occurrence is a defect, whichever section it is in.
//! - A field the type always produces must be written by the document. Not
//!   asserted *here*, for one document: `#[serde(default = "f")]` lets a
//!   field's value under omission differ from the same field in the type's
//!   `Default`, so writing every producible key into every template is not a
//!   mechanical edit — it needs a per-field audit. That audit is the property
//!   `every_config_knob_is_named_by_a_delivered_document_test` asserts: a knob
//!   no delivered document names is registered there with its reason. The count
//!   is printed here as the per-document reading.
//!
//! Neither direction reaches a key that has a field and is dropped after
//! parsing: `agent.heartbeat_interval_secs` had a field and was snapshotted
//! into a binding the code never used. That join is between the reader and its
//! consumer, and no type-level check can stand in for it.
//!
//! The walk follows the reader type's declared fields, so the assertion reaches a
//! key inside a nested object (`observability.loki.*`) or an array element
//! (`observability.infra_watch.rules[]`), not only the top level of a section.
//! A nested section whose key is read by *another* type is still declared as a
//! reader of its own; see the paragraph below.
//!
//! A section's reader is not always one type. `self_evolution`'s own struct has
//! no field for `artifact_evolution`; that key is read by cog-reflection, which
//! seeks it out by pointer. Such a key is declared as a reader of its own,
//! joined to the pointer's spelling in the source, so "a key no type has a
//! field for" stays a finding rather than becoming an allowance.
//!
//! 读回的节类型、声明文档的位置与走节的骨架只有一份，在
//! `common::config_surface`；本门与 `every_config_knob_is_named_by_a_delivered_document_test`
//! 都读它。

use crate::common;

use common::config_surface::{
    check_section, classified_sections, readers, sections_the_process_config_carries,
    shipped_configs, Readings, TypeGraph,
};
use common::routes::repo_root;

/// A section's name is classified, and the reader for it is declared here.
///
/// This is the direction that catches a section nobody reads at all: the table
/// says how a section reaches a process, and a section whose reader was never
/// written reaches nothing while every check that only compares documents
/// passes.
#[test]
fn every_classified_section_has_a_reader() {
    let mut declared: Vec<String> = readers().iter().map(|r| r.section.to_string()).collect();
    declared.sort();
    let mut classified = classified_sections();
    classified.sort();
    assert_eq!(
        classified, declared,
        "the sections the document is classified into and the sections this gate \
         knows a reader for disagree: a section listed here with no reader is \
         written by every deployment and read by nothing"
    );

    // A reader that claims no source spelling has to be a field of the struct
    // the process loads, or the claim stands on nothing.
    let carried = sections_the_process_config_carries();
    for reader in readers() {
        if reader.join.is_none() && reader.section != "env" {
            assert!(
                carried.contains(&reader.section.to_string()),
                "`{}` is declared as a field of the process config, and that struct has no \
                 such key: the section reaches the process through nothing",
                reader.section
            );
        }
    }
}

/// Every reader still spells the section out where it says it does.
///
/// A join written down once can outlive the code it names — the pointer reader
/// deleted, the gate still reporting the section as read. Reading the spelling
/// back out of the source is what keeps the declaration tied to a reader that
/// exists.
#[test]
fn every_reader_spells_the_section_out_where_it_says_it_does() {
    let root = repo_root();
    for reader in readers() {
        let Some((file, text)) = reader.join else {
            continue;
        };
        let path = root.join(file);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            source.contains(text),
            "{} no longer spells `{}` out as `{text}`: the section is declared as read \
             by a reader that is not there",
            path.display(),
            reader.section
        );
    }
}

/// Every key a shipped document writes has a field behind it.
///
/// serde passes over a key with no field behind it without a word, so an entry
/// nothing reads looks exactly like one that was applied — that is the
/// direction a typo hides in, and it is the same defect one level down from a
/// section nobody classified.
#[test]
fn every_key_a_shipped_document_writes_is_read_by_something() {
    let root = repo_root();
    let graph = TypeGraph::new(&root);
    let mut readings = Readings::default();
    let mut nested_seen: Vec<&str> = Vec::new();
    let mut omitted: Vec<String> = Vec::new();
    let mut docs = 0usize;
    for (file, doc) in shipped_configs(&root) {
        docs += 1;
        // Sections the document leaves out. Not asserted: a document may be a
        // sample rather than a declaration, and writing every section into a
        // sample would make it a second carrier of every default. The documents
        // the cluster receives are held to completeness elsewhere — the binary
        // refuses to ship a declaration the table does not classify in full,
        // and the embedders are held identical to the chart's. Printed so a
        // section that goes missing from a sample is a reading rather than a
        // silence.
        let missing = cog_core::config_sections::sections_missing_from_document(&doc);
        if !missing.is_empty() {
            omitted.push(format!("{file} leaves out {}", missing.join(", ")));
        }
        for reader in readers() {
            let Some(section) = doc.get(reader.section) else {
                continue;
            };
            nested_seen.extend(
                reader
                    .nested
                    .iter()
                    .filter(|n| section.get(n.section).is_some())
                    .map(|n| n.section),
            );
            check_section(&graph, &file, &reader, section, &mut readings);
        }
    }
    assert!(docs >= 5, "only {docs} shipped configs were found");
    // A nested key declared as read by pointer has to be written by something,
    // or the declaration names a reader for a knob no document carries.
    for reader in readers() {
        for nested in &reader.nested {
            assert!(
                nested_seen.contains(&nested.section),
                "`{}` is declared as read by pointer from `{}`, and no shipped document \
                 writes it",
                nested.section,
                reader.section
            );
        }
    }
    println!(
        "config surface: {docs} documents, {} sections, {} keys a section type produces \
         and its document does not write (per-document reading; the knob-level law is \
         `every_config_knob_is_named_by_a_delivered_document_test`)",
        readings.sections, readings.undocumented
    );
    for line in &omitted {
        println!("config surface: {line}");
    }
}

/// The tuned policy's name is one word written in two places, and the two are
/// read by different sides of the same loop.
///
/// The driver writes a winning parameter set under
/// `self_evolution.artifact_evolution.policy_name`; the recommendation path
/// reads its active version out of [`cog_reflection::META_LEARNING_POLICY`],
/// because that is the name the engine mounts the store under. When the two
/// disagree nothing fails: the driver stores a version, logs that it was
/// activated, `load_active` on the other name answers `Ok(None)`, and
/// `tuned_params` falls back to the constructor defaults. Both sides report
/// success and the search never takes effect — a knob whose value equals a
/// typo.
///
/// So this is not a free knob to be spelled independently: any document that
/// writes the key must write the constant. A document that leaves the key out
/// is fine — serde then uses `Default`, which is the same constant.
#[test]
fn the_tuned_policy_name_is_the_name_the_recommendation_path_reads() {
    let mut seen = 0usize;
    for (file, doc) in shipped_configs(&repo_root()) {
        let Some(name) = doc.pointer("/self_evolution/artifact_evolution/policy_name") else {
            continue;
        };
        seen += 1;
        assert_eq!(
            name.as_str(),
            Some(cog_reflection::META_LEARNING_POLICY),
            "{file} writes {name} as the tuned policy's name; the engine mounts the store \
             under {} and the recommendation path reads only that name — the winning \
             parameters would be stored where nothing looks",
            cog_reflection::META_LEARNING_POLICY
        );
    }
    println!("config surface: {seen} shipped documents name the tuned policy");
}
