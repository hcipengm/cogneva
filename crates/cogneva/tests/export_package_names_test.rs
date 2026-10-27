//! `cogneva backup` writes package files; the export page lists them back.
//!
//! The two live in different crates — the writer decides what a package is
//! called, the reader decides which names it is willing to show and stream —
//! and neither can import the other. They agree today only because both spell
//! the same prefix and suffix. If they stop agreeing, the page lists nothing
//! while exports keep "succeeding": every run publishes its package and the
//! reader rejects it by name, so the failure looks like a quiet, empty list.

#[test]
fn the_export_page_lists_the_names_the_backup_run_writes() {
    assert_eq!(
        cog_gateway::backup_admin::PACKAGE_PREFIX,
        cogneva::backup::PACKAGE_PREFIX,
        "the exporter and the page that lists its output must name packages the same way"
    );
    assert_eq!(
        cog_gateway::backup_admin::PACKAGE_SUFFIX,
        cogneva::backup::PACKAGE_SUFFIX,
        "a suffix the page does not recognise is a package nobody is offered"
    );
    assert!(
        cog_gateway::backup_admin::is_package_name(&format!(
            "{}{}{}",
            cogneva::backup::PACKAGE_PREFIX,
            "20261027T031700Z",
            cogneva::backup::PACKAGE_SUFFIX
        )),
        "a name the writer produces has to be one the page accepts"
    );
}

#[test]
fn the_export_job_sets_the_passphrase_variable_the_writer_reads() {
    assert_eq!(
        cog_gateway::backup_admin::PACKAGE_KEY_ENV,
        cogneva::backup_crypto::KEY_ENV,
        "the page puts the user's passphrase in one env var and the backup run reads \
         another: the run sees no passphrase, writes a plaintext package, and the page \
         reports the export as encrypted"
    );
}
