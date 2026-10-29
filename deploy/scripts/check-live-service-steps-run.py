#!/usr/bin/env python3
"""A step that opens a live connection must not be hidden by an earlier failure.

Why this is a gate and not a review item: a step with no `if:` runs only when
every step before it in the same job succeeded, so a chain of live-service steps
placed after the ordinary test run turns every one of them into an assertion
that is skipped whenever something unrelated is red -- which is most of the
time on a branch under repair. A skipped assertion and a passing one look the
same on the job summary: the run says "failure" for the step that broke, and
says nothing at all about the thirty that never ran.

Measured 2026-10-30: `the_face_census_reads_a_number_for_the_age_and_three_
buckets_that_add_up` failed on the first run it had ever had. It was added with
the reading it judges on 2026-10-2x, and the chain of live steps that carries
it -- fifty of them in the `test` job, all after `Run cargo test --workspace` --
had been skipped on every push since, because every push had some earlier red
(the last one was a chart literal). The defect it reports is that a census
bucket the reader selects by name was never emitted by the query, so the
published number was a hard zero; it had been in the tree, and on the
deployment, for as long as the reading had.

What is judged, and nothing else: a step whose text opens a connection to a
live service -- `COGNEVA_TEST_DATABASE_URL` or `COGNEVA_TEST_REDIS_URL`, in its
`run:` or in its `env:` -- must carry an `if:` that survives an earlier
failure (`!cancelled()` or `always()`), alone or combined with a condition of
its own. A step that needs both "not cancelled" and "this is the main branch"
writes both, and passes.

What is deliberately not judged: steps that need no live service. Their
framing question is different -- they may legitimately depend on a build that
has not broken -- and a gate that demanded `!cancelled()` of every step would
turn one compile error into a hundred red steps and be switched off.

The population is every workflow under `.github/workflows`, every job, every
step, read through a YAML parser rather than by matching lines: the shape being
judged is a property of a step, and a step is not a line.
"""

import sys
from pathlib import Path

import yaml

WORKFLOWS = Path(".github/workflows")

# The two variables that make a step open a connection to a server this run
# does not own. A step that sets one of them is a live step whatever its name
# says, and whether it names the variable in `run:` or carries it in `env:`.
LIVE_MARKERS = ("COGNEVA_TEST_DATABASE_URL", "COGNEVA_TEST_REDIS_URL")

# The conditions under which GitHub runs a step whose predecessors failed.
SURVIVES_A_FAILURE = ("!cancelled()", "always()")

# The floor is not the count -- that moves every time a test is added. It is
# that the walk found a workflow at all: a reader that stops matching reports
# an empty population as agreement.
MIN_STEPS = 100
MIN_LIVE_STEPS = 30


def step_text(step):
    parts = [str(step.get("run") or ""), str(step.get("name") or "")]
    env = step.get("env") or {}
    if isinstance(env, dict):
        parts.extend(f"{key}={value}" for key, value in env.items())
    return "\n".join(parts)


def main():
    if not WORKFLOWS.is_dir():
        sys.exit(f"refusing: {WORKFLOWS} is not a directory")

    examined = 0
    live = 0
    offenders = []
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(workflow, dict):
            continue
        for job_name, job in (workflow.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            for index, step in enumerate(job.get("steps") or [], start=1):
                if not isinstance(step, dict):
                    continue
                examined += 1
                text = step_text(step)
                if not any(marker in text for marker in LIVE_MARKERS):
                    continue
                live += 1
                condition = str(step.get("if") or "")
                if any(survives in condition for survives in SURVIVES_A_FAILURE):
                    continue
                offenders.append(
                    f"{path}: job `{job_name}`, step {index} "
                    f"`{step.get('name') or '<unnamed>'}` (if: {condition or 'none'})"
                )

    if examined < MIN_STEPS:
        sys.exit(
            f"refusing: walked {examined} steps across {WORKFLOWS}, fewer than the {MIN_STEPS} "
            "this gate found the day it was written -- the walk has stopped finding them "
            "rather than the workflows having shrunk"
        )
    if live < MIN_LIVE_STEPS:
        sys.exit(
            f"refusing: found {live} live-service steps, fewer than the {MIN_LIVE_STEPS} this "
            "gate found the day it was written -- a marker that stops matching lets every step "
            "through while reporting agreement"
        )

    if offenders:
        print(
            "these steps open a connection to a live service and run only when every earlier "
            "step succeeded, so an unrelated failure skips them and the job summary cannot "
            "tell that apart from a pass:"
        )
        for offender in offenders:
            print(f"  {offender}")
        print(
            'add `if: ${{ !cancelled() }}` to each -- on its own or combined with the '
            "condition already there -- so a red earlier in the job does not silence them"
        )
        return 1

    print(f"{live} live-service steps of {examined} run even when an earlier step failed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
