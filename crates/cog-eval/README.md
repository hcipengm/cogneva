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

Three of the four (CodeAct, GEPA, AGENTFLOW) drive the backbone directly through `LlmClient`. The
fourth, `nql`, is a thin wrapper over the real platform: it reaches the platform through the
`PlatformRunner` port, which this crate declares and the composition root implements — a missing
platform has to look like a missing platform, so that row **errors** rather than scoring low.

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
installed set with no network and exits non-zero on any mismatch. A bare `--verify` covers the
downloaded artifacts only; the derived text form the rig actually reads is checked only when
`--convert` is passed too (`--verify --convert`), because building and checking it are one opt-in
step — so `--verify` alone is not a full check of what the rig reads.

The three benchmarks and their pins:

| benchmark | source | revision | artifact(s) |
|---|---|---|---|
| `swe-bench-pro` | ModelScope `ScaleAI/SWE-bench_Pro` | `0c470e9c…` | `test-00000-of-00001.parquet` (7.5 MiB, 731 rows); `test-00000-of-00001.jsonl` under `--convert` |
| `toolathlon-gym` | GitHub `eigent-ai/toolathlon_gym` | `ed735ba0…` | tarball (104 MiB, 503 tasks), extracted to `toolathlon-gym/` |
| `hle` | ModelScope `cais/hle` | `1ec1f1f2…` | `test-00000-of-00001.parquet` (262 MiB, ~2,500 questions); `test-00000-of-00001.jsonl` under `--convert` |

Two notes on the data itself. Both SWE-bench Pro and HLE have a JSONL form *derived* from their
parquet, so that step is opt-in (`--convert`) — it needs pyarrow, and the base fetch must not depend
on a Python package. Point `BENCHMARK_PYLIBS` at a directory holding pyarrow if it is not on the
default path; the derived file is hash-checked like any other, so a pyarrow that serializes a row
differently turns into a red run rather than a quietly different benchmark. HLE's parquet carries
binary columns (an image preview, a rationale image) that cannot go into a text row: the converter
writes each as `null` and lists its path in a row-level `_binary_dropped`, so "there was no image"
and "the image was dropped here" do not read the same. The question's own image is untouched — it
travels as the data URL the parquet already stores, so a multimodal harness needs no side files.
HLE is MIT-licensed with no gate, carries a canary marking it as not-for-training-corpus, and is
*multimodal* (about 1 row in 7 carries an image), so a text-only harness must either skip image
rows and say so or feed them; `HleCaseSource` marks the choice on each case
(`metadata.has_image`) rather than deciding it.

SWE-bench Pro's `fail_to_pass` and `pass_to_pass` columns are **Python literal** lists of test
names (`['a', 'b']`), not JSON arrays — measured over the public split, only 9 of 731 rows happen
to also be valid JSON, so a JSON parser reads the other 722 as "no tests to run" and passes the
column for free. `parse_test_list` walks the literal (both quote styles, backslash escapes) and
refuses any trailing characters; the real-data test asserts each row yields a non-empty
`fail_to_pass`, which is what pins that silent failure down.

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

### What is checked against the real data

The unit tests use fixtures, so three tests read the fetch script's actual output when
`COG_EVAL_DATA_ROOT` points at it (and print `SKIP` when it does not, rather than passing
silently): one per benchmark asserting the row/task counts (2,500 / 731 / 503) and the shapes the
adapters depend on. A fourth runs both directions of the whole chain — the reference arm (a
correct solution) must score every case and the degenerate arm (an empty one) must score none,
with `errored == 0` in both — over the real data, with only the container/Compose backend stubbed:

```sh
COG_EVAL_DATA_ROOT=/srv/cogneva/benchmarks cargo test -p cog-eval
```

### Driving the table

One command runs the whole thing — one data root, the benchmarks, the rows, the seeds — and prints
the table together with the pins it was produced under:

```sh
cogneva eval-table --pins /srv/cogneva/benchmarks/pins.tsv \
                   --backbone '<model>@<version>' --judge '<model>@<version>'
```

- `--pins` is required, and its file is `deploy/scripts/fetch-benchmark-data.sh --print-pins`'s
  output: one record per benchmark naming the revision and **the path the rig actually opens** —
  the derived JSONL rather than the parquet it came from, and for Toolathlon a *directory*
  (recorded as such, its bytes pinned by the source archive's hash, because a directory has no
  hash of its own). The driver re-hashes what is on disk and refuses to run when the bytes are not
  the ones the pin names. A table without pins is a table nobody can compare against anything.
- `--tools` defaults to `platform`, the arm the main table is run on. The tools-on arm needs
  implementations for every tool each benchmark pins (HLE: `web_search` + `python`; SWE-bench Pro:
  `bash` + `file_edit`; Toolathlon: the MCP servers the task declares), and the composition root
  has none yet — so `platform` refuses rather than scoring every case on a smaller tool face.
  `--tools none` is a **diagnostic** arm only: it prints its readings but is not a table row (its
  table is withheld, and the run exits non-zero as an incomplete experiment), because a run on a
  different tool face is not the same experiment as the rows it would sit beside.
- Model names come in on the command line as `<model>@<version>` and are checked against the
  enabled `llm_routing` backends. A pin naming a model no upstream serves is reported as a missing
  dependency, not printed as though it were in force.
- Missing dependencies (backbone, judge, container backend, Compose backend, platform) do not stop
  the table: they are listed under `## wiring`, the cells they touch come out `errored`, and the
  process exits non-zero. The table is the report; the exit code is the verdict.

Two runs with one data root and one command line print the **same bytes**: no wall clock, no
container iteration order, no scheduler-dependent ordering (the failure list is sorted by case id,
not by whichever case finished first). `crates/cogneva/tests/eval_table_determinism.rs` holds that
to two runs of the pipeline against a scripted backbone — determinism *of the pipeline given the
same answers*, not end-to-end reproducibility of an upstream that samples at temperature 1.

### A dead upstream yields data and interfaces, not numbers

This is the honest boundary of the crate today. The rig, the four rows, and all three benchmarks'
adapter sets are landed; the driver and the platform bridge are landed too. What is not landed is a
live upstream — with no `llm_routing` backend serving the pinned models and no platform endpoint, a
run produces the table's *shape* (which cells exist, which pins it ran under, which cells errored)
and not numbers.

1. **The external dependencies each adapter needs are ports** — the crate declares them and does
   not implement them, because they are where the platform (a container runtime, a Compose project,
   a search API, a judge model) becomes visible, and that is the composition root. All three
   benchmarks' `CaseSource` / `EnvProvider` / `Toolkit` / `CaseJudge` sets are landed: HLE
   (`src/adapters/hle.rs`, `hle_benchmark(...)`), SWE-bench Pro (`src/adapters/swe_pro.rs`,
   `swe_pro_benchmark(...)`) and Toolathlon (`src/adapters/toolathlon.rs`,
   `toolathlon_benchmark(...)`). Each `Toolkit` pins the protocol with the implementations
   **injected** — HLE: exactly `web_search` + `python`; SWE-bench Pro: exactly `bash` +
   `file_edit`; Toolathlon: exactly the MCP servers the task's own `task_config.json` declares —
   and refuses to run with one missing rather than handing back a smaller toolset that scores the
   task for the wrong reason. SWE-bench Pro's per-instance container is `SweProBackend` and
   Toolathlon's Compose project is `ToolathlonBackend`: with no backend wired, both env acquisition
   and judging **error** rather than reading as a model that failed the task.
2. **A driver** — `cogneva eval-table` runs the table and prints it with its pins (see *Driving
   the table* above). It builds no numbers today: every dependency it needs is unwired, and it
   reports that instead of hiding it.
3. **The platform bridge** — landed at the composition root, where the platform types are visible.
   `PlatformApiRunner` (`crates/cogneva/src/eval_platform.rs`) implements `PlatformRunner` over the
   platform's own HTTP routes: it submits the case as a task, waits for that task to reach a
   terminal status, then reads back its execution trace and metrics and *projects* them into an
   `AgentOutput` (the answer text, the step records, the token split, a finish reason). The endpoint,
   token and deadline come from `COG_EVAL_PLATFORM_BASE` / `COG_EVAL_PLATFORM_TOKEN` /
   `COG_EVAL_PLATFORM_DEADLINE_SECS`; with no endpoint set the driver keeps the erroring `NoPlatform`
   port, so the `nql` row errors rather than scoring. It is not a stub — every route it calls is the
   platform's real one, and a case the platform never finishes is reported as an error, never as a
   zero. Two things are worth knowing before reading a number off this row: the projection is
   lossy (the platform's own step `duration_ms` is wall clock, so it is zeroed to keep two runs
   byte-identical, and the platform reports no stop reason on this route, so every answer finishes
   `Answered`), and the bridge assumes the platform runs a submitted task *as that task* — a
   platform that decomposes it into planner-generated children keeps none of the submitted id's
   work under that id, and the bridge will time out rather than guess.

So what you can reproduce today is the **data plane** (fetch + verify) and the **interfaces** — and,
once an upstream is alive, the platform row through a real bridge — not table numbers.

Two further things stand between this rig and numbers even once those land. First, every row except
NQL drives a backbone (`deepseek-v4-flash`) through `LlmClient` (NQL drives the platform instead),
and HLE is the one column graded by a generative judge (`Kimi K3`, official `model_graded_fact`
rubric). SWE-bench Pro and Toolathlon are graded by deterministic scripts (their tests /
`evaluation/main.py`), so they are the two columns that can score with no LLM judge at all — but a
dead backbone still leaves them empty. Each adapter's external dependency is a parameter, and a
missing one **errors** rather than scoring zero: HLE's judge takes its upstream as a parameter
(with no judge model wired, a case that needs it errors, because "never judged" and "judged wrong"
are the same 0 and different facts), and SWE-bench Pro its container backend, Toolathlon its
Compose backend, the same way.
Second, HLE is multimodal; a text-only harness
has to say whether it skips image rows. Pin the backbone and judge model + version + rubric
provenance in the experiment metadata: a cell that ran on a substituted model is not a cell in this
table.
