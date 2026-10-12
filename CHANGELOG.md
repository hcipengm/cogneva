# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/).

## Unreleased

### Changed

- The meta-bootstrap now carries the deployment-derived resource-governance
  budget as a Rust-side fallback. The LimitRange and ResourceQuota values
  live only in the deployment manifests, which are outside the contribution
  allow-list: the per-container max (the evolution pod included) was raised
  from 8 GiB to 16 GiB, the main gateway keeps a 6 GiB limit, and the
  namespace quota is 46 GiB of `limits.memory` (42.1 GiB steady-state plus
  the 512 Mi judge job plus 3.4 GiB headroom), 40 pods and 300 GiB of
  storage. A rendered manifest or pod environment that lost those governed
  values used to lose the measured safe cap with them. `ResourceBudget` in
  `crates/bootstrap/src/main.rs` now starts `Default`/`from_env` from those
  same deployment-derived numbers; `COGNEVA_CONTAINER_MEMORY_LIMIT_MB`,
  `COGNEVA_GATEWAY_MEMORY_LIMIT_MB`, `COGNEVA_NAMESPACE_MEMORY_LIMIT_MB`,
  `COGNEVA_NAMESPACE_PODS_MAX` and `COGNEVA_NAMESPACE_STORAGE_LIMIT_MB`
  override them, and an unset or unparsable value is treated as "not
  configured" and falls back with the default.

- The standalone sandbox executor now bounds its own build cache by default.
  `BuildCacheConfig` (`crates/cog-extension/src/build_cache.rs`) used to start
  from a zero cap — publish the cache size and remove nothing — so the cache was
  bounded only when the deployment manifests delivered
  `SANDBOX_BUILD_CACHE_MAX_BYTES`. Those manifests are outside the contribution
  allow-list, and the cache shares a small 5 GiB claim with the task worktrees,
  so a pod whose environment lost the variable could fill that volume and turn
  that into build failures for reasons that said nothing about the cache. The
  env-driven constructor and `Default` now carry the same deployment-derived 3
  GiB cap (`DEFAULT_MAX_BYTES` = 5 GiB minus the measured co-tenants:
  `CARGO_HOME`, the bare repo, the per-task worktrees, the host-docs journal) as
  a fallback. An explicit `SANDBOX_BUILD_CACHE_MAX_BYTES` value still overrides
  it, including `0`, which keeps the measure-only behaviour; an unparsable value
  is treated as "not configured" and falls back with it.
  The 3 GiB number is defined once, as
  `cog_core::build_cache::DEFAULT_CACHE_MAX_BYTES`, and the executor reads it
  from there instead of carrying its own copy, so the fallback cannot drift
  from the bound the shared readings describe.
