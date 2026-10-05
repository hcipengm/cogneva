# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/).

## Unreleased

### Changed

- The standalone sandbox executor now bounds its own build cache by default.
  `BuildCacheConfig` (`crates/cog-extension/src/build_cache.rs`) used to start
  from a zero cap — publish the cache size and remove nothing — so the cache was
  bounded only when the deployment manifests delivered
  `SANDBOX_BUILD_CACHE_MAX_BYTES`. Those manifests are outside the contribution
  allow-list, and the cache shares a small 5 GiB claim with the task worktrees,
  so a pod whose environment lost the variable could fill that volume and turn
  it into builds failing for reasons that said nothing about the cache. The
  env-driven constructor and `Default` now carry the same deployment-derived 3
  GiB cap (`DEFAULT_MAX_BYTES` = 5 GiB minus the measured co-tenants:
  `CARGO_HOME`, the bare repo, the per-task worktrees, the host-docs journal) as
  a fallback. An explicit `SANDBOX_BUILD_CACHE_MAX_BYTES` value still overrides
  it, including `0`, which keeps the measure-only behaviour; an unparsable value
  is treated as "not configured" and falls back with it.
