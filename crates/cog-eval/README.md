# cog-eval

The evaluation rig behind the comparison table: it measures agent *scaffolds* against
*benchmarks*, and it is not one of them.

The table has two axes — a **method** (CodeAct, GEPA, AGENTFLOW, NQL) and a **benchmark**
(SWE-bench Pro, Toolathlon, HLE). The rig owns everything that is the same for every cell, so
that a difference between two cells is a difference between two methods and nothing else.

## What the rig is, and what it deliberately is not

Six parts plug in, and each one binds to exactly one axis — never both:

| part | trait | binds to |
|---|---|---|
| cases | `CaseSource` | benchmark |
| environment | `EnvProvider` → `CaseEnv` | benchmark |
| toolset | `Toolkit` → `ToolSet` | benchmark |
| scaffold | `AgentScaffold` | method |
| judge | `CaseJudge` | benchmark |
| runner | `Rig` | neither |

The two axes meet exactly once: a scaffold gets a `SolveContext` (backbone, tools, env,
budget, seed) and returns an `AgentOutput`; the judge reads that and nothing else. The scaffold
never sees the judge, and the benchmark never sees which method is running.

**Neutrality is enforced, not asserted.** `tests/scaffold_neutral_rig.rs` greps the rig's own
sources (`src/scaffold.rs`, `src/bench.rs`, `src/rig.rs`) for any scaffold's proper name — the
four in `src/scaffolds/`, plus the design names (`planner`, `evaluator`, `quality gate`, …). If
the rig learns a method's name, it has started special-casing it. The list of forbidden names is
derived from `src/scaffolds/*.rs`, so adding a fifth row extends the check automatically. The
checker lives outside the checked set on purpose, and a second test feeds it a knowingly tainted
string to prove the check can go red:

```sh
cargo test -p cog-eval --test scaffold_neutral_rig
```

The four rows in `src/scaffolds/` share only the protocol edge (`take_turn`, `observe_call`,
`CODE_ARG`) — the point where a model reply is turned into a next message. They deliberately do
**not** share a control loop: two rows running the same loop would be the same implementation
twice, and the difference between them would be prompt wording rather than method.

## Getting the data

Benchmark data is a few hundred MB of parquet and tarballs; it is not committed. Fetch it into a
directory of your choice (default `/srv/cogneva/benchmarks`):

```sh
deploy/scripts/fetch-benchmark-data.sh --dest /srv/cogneva/benchmarks          # all three
deploy/scripts/fetch-benchmark-data.sh --benchmark hle --dest /path/to/data    # one
deploy/scripts/fetch-benchmark-data.sh --print-files                           # what, no network
deploy/scripts/fetch-benchmark-data.sh --dest /srv/cogneva/benchmarks --verify # re-check what is there
```

What "provisioned" means is the script's catalog, and it is the single source of truth: the
catalog pins a **revision** (which names the listing) *and* a **sha256 per file** (which names the
bytes). Every file is hashed before it is installed, never after, and a file already in place is
re-hashed rather than re-downloaded. A file whose bytes do not match is refused, not overwritten —
the name came from the hash, so mismatched bytes mean the mirror is not serving what it claims,
and that is an operator decision. `--verify` is the reader of those hashes: it re-checks the
installed set with no network and exits non-zero on any mismatch.

The three benchmarks and their pins:

| benchmark | source | revision | artifact(s) |
|---|---|---|---|
| `swe-bench-pro` | ModelScope `ScaleAI/SWE-bench_Pro` | `0c470e9c…` | `test-00000-of-00001.parquet` (7.5 MiB, 731 rows) |
| `toolathlon-gym` | GitHub `eigent-ai/toolathlon_gym` | `ed735ba0…` | tarball (104 MiB, 503 tasks), extracted to `toolathlon-gym/` |
| `hle` | ModelScope `cais/hle` | `1ec1f1f2…` | `test-00000-of-00001.parquet` (262 MiB, ~2,500 questions) |

Two notes on the data itself. SWE-bench Pro's JSONL form is *derived* from its parquet, so that
step is opt-in (`--convert`) — it needs pyarrow, and the base fetch must not depend on a Python
package. Point `BENCHMARK_PYLIBS` at a directory holding pyarrow if it is not on the default path;
the derived file is hash-checked like any other, so a pyarrow that serializes a row differently
turns into a red run rather than a quietly different benchmark. HLE is MIT-licensed with no gate,
carries a canary marking it as not-for-training-corpus, and is *multimodal* (its rows carry an
`image` field), so a text-only harness must either skip image rows and say so or feed them.

## Running the rig

The rig takes the data directory as a parameter, and it will not guess one — a default would turn
"reproduce" into "reproduce on the author's machine". The directory comes from either
`RunConfig::new(path)` or the environment, through `RunConfig::from_env()`, which reads:

```
COG_EVAL_DATA_ROOT   the --dest directory the fetch script wrote to. No default; unset or
                     empty is an error, because a wrong data root reads as "the benchmark has
                     no cases".
```

Then build the rig, register the rows, and run it against the benchmarks:

```rust
let config = RunConfig::from_env()?;                 // or RunConfig::new("/srv/cogneva/benchmarks")
let rig = Rig::new(llm, config)
    .with_scaffold(Arc::new(CodeAct::new()))         // ...and the other three
    ;
let table = rig.run(&[swe_pro, toolathlon, hle]).await;
println!("{}", table.to_markdown());
```

A cell is one scaffold × one benchmark × 3 independent seeds, reported as mean ± standard
deviation; the row's average is **equal-weight over columns**, not over cases. A case that cannot
run (env failed, scaffold errored, judge errored) is counted as **missing the bar, not dropped** —
the denominator never shrinks — and is reported separately in `errored`, because "the upstream was
down" and "the answer was wrong" both score 0 but are different facts. A scaffold that cannot run
at all errors out rather than scoring low: a missing platform has to look like a missing platform.

### A dead upstream yields data and interfaces, not numbers

This is the honest boundary of the crate today. The rig and the four rows are here; the per-benchmark
adapter sets (`CaseSource` / `EnvProvider` / `Toolkit` / `CaseJudge` for SWE-bench Pro, Toolathlon and
HLE) are not landed yet, and there is no driver binary — the entry point is the library API above.
So what you can reproduce today is the **data plane** (fetch + verify) and the **interfaces**, not
table numbers.

And even once the adapters land, two of the three benchmarks still produce no numbers without
upstreams: every scaffold drives a backbone (`deepseek-v4-flash`) through `LlmClient`, and HLE is
the one column graded by a generative judge (`Kimi K3`, official `model_graded_fact` rubric).
SWE-bench Pro and Toolathlon are graded by deterministic scripts (their tests / `evaluation/main.py`),
so they are the two columns that can score with no LLM judge at all — but a dead backbone still
leaves them empty. Pin the backbone and judge model + version + rubric provenance in the experiment
metadata; a cell that ran on a substituted model is not a cell in this table.
