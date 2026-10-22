# First fixed-benchmark run: two revisions, both all-errored

This is the report for work order W1 ("run the fixed benchmark once and commit a report").
The run was **registered before it produced anything**: the registration is
`2026-10-16_固定基准首次登记（rev对·种子·子集·跑法，跑前定稿）.md` in the private companion repository
repo, committed as `03f19e6f` at 2026-10-16T03:06:56Z. The registration is the pre-commitment —
what ranges were run, with which seeds, over which subset — and it is what makes this first
report non-cherry-pickable. This document is the run's outcome; the registration is unchanged.

Raw output for each revision sits next to this file.

## 1. What ran

| | rev A | rev B |
|---|---|---|
| image | `localhost:30500/cogneva:main-c6da851c10d3` | `localhost:30500/cogneva:main-3b100eebb6e6` |
| commit | `c6da851c10d3041cc892ce83cf6111d8cd35f5f7` | `3b100eebb6e6292d0c1aa1f4fc1c85148fdffbcd` |
| pod | `eval-table-c6da851c` | `eval-table-3b100eeb` |
| started → finished (UTC) | 03:08:13 → 03:08:17 | 03:08:44 → 03:08:48 |
| exit code | 1 | 1 |
| raw output | `2026-10-16_eval-table_c6da851c10d3.md` | `2026-10-16_eval-table_3b100eebb6e6.md` |

Both pods ran the registered command line verbatim (same `--pins /eval/pins.tsv`,
`--data-root /data`, `--subset /eval/seed20261011_n40.tsv`, `--backbone`/`--judge
via-security-gateway@1`, `--benchmarks hle,swe-bench-pro,toolathlon`,
`--scaffolds codeact,gepa,agentflow,nql`, `--seeds 0,1,2`, `--tools none`,
`--concurrency-slot 4`), differing only in image tag and `--out`.

Acceptance checks against the registration, all read back rather than assumed:

- subset sha256 `d0e74c3b2f8d6774bbbb4b3526801033cfe2ced736f8645199307fba4778b638` —
  identical in `eval_datasets/subsets/seed20261011_n40.tsv`, in the copy the pods read, and in
  the raw output's own `## pins` line.
- pins sha256 `e1e8fbb4d31907757e998d3af4990584a96025cb5636679a18d0982112a19f07` — matches.
- rev pair, seeds, benchmarks, scaffolds: match.

The two revisions were the two most recent deployed versions at registration time
(`main == deployed` for B), so this pair asks "does the code change between them move the number",
not "did the run procedure change".

## 2. The result

- **1440 case-runs per revision** (3 benchmarks × 4 scaffolds × 3 seeds × 40 subset cases), and
  **every one of them errored**: `## per seed` reports `resolved=0 errored=40` in all 36 cells,
  `tokens=0`, `steps=0`; the raw output's own total is `errored 1440`.
- The `## table` section is **withheld by design**: `--tools none` makes this the tools-off
  diagnostic arm, whose cells are "not a table row". The raw output says so in its own words.
- Both processes exit 1, with
  `Error: "this run is not a complete experiment: tools …; swe-bench-pro: no container backend is
  wired; toolathlon: no compose backend is wired; nql: no platform is wired"`.
- The two revisions' raw outputs are **byte-identical except for one line** (the checkpoint /
  run-id line). So this experiment carries **no A-vs-B signal at all** — which is expected, since
  the commit between them is a change-ledger priority commit, not an evaluation change.
- Both runs reached the end of their own pipeline in **4 seconds**. 1440 case-runs in 4 seconds is
  itself the reading that nothing was executed.

## 3. Why every case errored — and where the registration was wrong

The registration's §7 predicted "a run that will be all-errored", and attributed it to the LLM
upstream pool being unavailable. **The run refutes that attribution.** No case ever reached the
upstream: every one of the 1440 died in the composition root's own wiring checks, in four distinct
ways, identically on both revisions.

| case-runs per rev | cells | message (verbatim from the raw output) |
|---:|---|---|
| 480 | swe-bench-pro × all 4 scaffolds | `cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired` |
| 480 | toolathlon × all 4 scaffolds | `cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired` |
| 120 | hle × codeact | `scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row` |
| 120 | hle × nql | `scaffold failed: no platform endpoint is wired: the composition root wires no platform` |
| 120 | hle × gepa | `scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured` |
| 120 | hle × agentflow | same as gepa |

**Not one line in either raw output mentions an upstream, a 503, a timeout, or the pool.** The
pool may or may not be down; this run has no opinion on it.

Three of the four causes are the arms this run was never going to execute, and the registration
predicted two of them in §6:

- no container backend (swe-bench-pro) and no compose backend (toolathlon) — the `Wiring` fields
  are never assigned anywhere in the tree, so these two benchmarks cannot run at all today;
- no code tool under `--tools none`, which is exactly the `codeact` action space;
- no platform endpoint for nql — `BridgeSettings::from_env` reads process environment only
  (`crates/cogneva/src/eval_platform.rs:170`), and this pod carried no bridge env.

The fourth is not in that list.

## 4. The one cause that is a plain defect

`crates/cogneva/src/eval_table.rs:1280`, on both revisions:

```rust
match cog_llm::plugin::build_llm_provider(capacity, max_tokens, &routing, None) {
```

The fourth parameter is `http_client: Option<Arc<dyn cog_core::HttpClient>>`, and it is hardcoded
`None`. `build_single_provider` only calls `.with_client(…)` when it is `Some`, so every provider
built here answers its first call with `…: no HttpClient configured`
(`crates/cog-llm/src/provider/openai.rs:441`).

The client is buildable in this very process: twenty lines earlier the same file builds a real
`ReqwestHttpClient` from the `http_client` config section and hands it to the platform port
(`platform_from_settings`). It is simply not handed to the LLM provider. The two sibling call
sites both pass a real client:

- `crates/cog-llm/src/plugin.rs:44` — `ctx.consume_service::<dyn cog_core::HttpClient>()`
- `crates/cogneva/src/lib.rs:318` — `llm_http_client.clone()`

The `None` was introduced 2026-10-11 by `869415b0` ("Run the main table from one command, and
reach the platform through a real port") and is present in **both** revisions of this run.

Consequence: the two LLM-backed hle cells (`gepa`, `agentflow` — 240 case-runs per revision)
could not have produced a number even with a perfectly healthy upstream pool.

## 5. What this run does and does not say

- It does **not** say anything about model quality — 1440/1440 died before any inference.
- It does **not** support "the upstream pool is why the row is empty" — the pool's state is
  invisible here, and the two hle cells would still have failed with the pool healthy.
- It **does** say the rig itself works end to end: it read the subset and the pins, verified the
  three benchmark trees, built the 12 cells, and reported per cell exactly which dependency is
  missing. A diagnostic this specific is what makes a later real number trustworthy.
- It **does** say the main-table row (work order W1's actual target) is blocked by one one-line
  defect plus three unwired capabilities — not by upstream availability.

## 6. What a first real number would take

1. Give the provider a client at `eval_table.rs:1280` (this changes code, so it is the worker
   line's call, not this report's).
2. An upstream pool that is actually serving. As of the run `llm_pool_available` was 0 and the
   gateway answered a real call with
   `503 {"error":"所有 LLM 上游当前不可用", …}`.
3. For the main table rather than the diagnostic arm: a container backend (swe-bench-pro), a
   compose backend (toolathlon), the tool implementations `--tools platform` expects, and a
   platform endpoint for nql.

Until at least (1) and (2) hold, no run of this CLI can be a baseline, and the empty row should be
read as a **wiring reading, not a capability reading**.
