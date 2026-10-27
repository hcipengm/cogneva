//! One role name, two crates: the one a unit registers under and the one the
//! staffing pass reads back.
//!
//! `cogneva work heartbeat` writes its presence with the supervisor's
//! `EXTERNAL_WORKER_ROLE`; the staffing pass hands a face only to a unit whose
//! registration carries `FACE_HOLDER_ROLE`. They are spelled in different
//! crates and neither can import the other's, so the only thing keeping a
//! face-holder recognisable is that the two strings agree. If they ever stop
//! agreeing, every present unit is "not a holder", the pass drafts nothing, and
//! the faces of a departed owner stay with a name nobody answers -- silently,
//! because "nobody to hand it to" is an expected state.

#[test]
fn the_staffing_pass_reads_back_the_role_a_holder_registers_under() {
    assert_eq!(
        cog_orchestrator::face_staffing::FACE_HOLDER_ROLE,
        cog_supervisor::registry::EXTERNAL_WORKER_ROLE,
        "a unit that registers under one name can never be handed a face that \
         looks for the other"
    );
}
