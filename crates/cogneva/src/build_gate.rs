// Placeholder: the iteration budget expired before the real module could be
// read and edited. The gate implementation is crates/cog-core/src/build_gate.rs
// (HostLoad::read and CgroupCeiling::current); capacity re-derivation must be made runtime-re-read there, not cached at startup, with a regression test for a mid-life memory resize.
