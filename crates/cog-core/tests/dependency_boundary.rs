//! Guards the contract layer boundary.
//!
//! `cog-core` defines traits and cross-crate types; concrete storage drivers
//! belong to `cog-storage`. Depending on `sqlx`/`redis` here would force every
//! consumer of the contracts to link a driver, so the boundary is asserted
//! rather than left to review.

#[test]
fn core_does_not_depend_on_storage_drivers() {
    let manifest = include_str!("../Cargo.toml");
    for banned in ["sqlx", "redis"] {
        let dep = format!("\n{banned} =");
        assert!(
            !manifest.contains(&dep),
            "cog-core must not depend on `{banned}`; storage drivers live in cog-storage"
        );
    }
}

#[test]
fn core_sources_do_not_name_storage_drivers() {
    let sources = [
        include_str!("../src/contract/storage.rs"),
        include_str!("../src/contract/auth.rs"),
    ];
    for src in sources {
        for banned in ["sqlx::", "redis::"] {
            assert!(
                !src.contains(banned),
                "cog-core sources must not reference `{banned}`"
            );
        }
    }
}
