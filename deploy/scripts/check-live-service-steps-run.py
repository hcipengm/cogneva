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
`run:`, in its own `env:`, or in an `env:` it inherits from its job or from the
workflow -- must carry an `if:` that survives an earlier failure
(`!cancelled()` or `always()`), alone or combined with a condition of its own.
A step that needs both "not cancelled" and "this is the main branch" writes
both, and passes. Inheritance is read because a variable lifted one level up
reaches the same steps while leaving no marker in any of them: a gate that read
only the step would report agreement for exactly that tidy-up.

An inherited variable flags every step in the job, including ones that plainly
connect to nothing. That is deliberate and it is the conservative direction:
nothing in the workflow says which steps use an inherited variable -- a test
binary reads it from the environment without naming it anywhere -- so the job
is judged as one chain. The remedy is either the condition on each step of a
job that is genuinely all live, or putting the variable back on the steps that
use it, where the gate can see them.

What is deliberately not judged: steps that need no live service. Their
framing question is different -- they may legitimately depend on a build that
has not broken -- and a gate that demanded `!cancelled()` of every step would
turn one compile error into a hundred red steps and be switched off.

2026-11-06: the sixty-four live checks moved out of sixty-four steps into one
step that runs `deploy/scripts/check-live-services.sh`, so the shape being
judged changed places. A single step cannot be skipped part-way: either the
script runs and every check in it runs, or the step is skipped and all sixty-four
are skipped at once -- which the `if:` below still forbids. What replaces the
per-step count is that the script itself does not stop at the first red: it has
no `set -e`, and the population it carries is counted here from the file, so a
list that quietly shrinks to three checks is still refused. The count moved with
the population; it did not disappear.

The population is every workflow under `.github/workflows`, every job, every
step, read through a YAML parser rather than by matching lines: the shape being
judged is a property of a step, and a step is not a line. The live checks that
are no longer steps are read from `LIVE_CHECKS`, which the workflow must call.
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

# The scripts that carry the live checks now that they are not steps. The
# workflow must call the first one; the second is the wrapper each entry runs
# through, whose annotations the job summary reads.
LIVE_CHECKS = Path("deploy/scripts/check-live-services.sh")
LIVE_ENTRY = "live_check "

# The floor is not the count -- that moves every time a test is added. It is
# that the walk found a workflow at all: a reader that stops matching reports
# an empty population as agreement.
MIN_STEPS = 100
MIN_LIVE_STEPS = 30
MIN_LIVE_CHECKS = 30


def env_text(env):
    if not isinstance(env, dict):
        return []
    return [f"{key}={value}" for key, value in env.items()]


def step_text(step, inherited):
    """A step's text including the environment it inherits.

    A variable written at workflow or job level reaches every step under it,
    so a step that opens a connection because of an inherited variable is a
    live step with no marker in its own body. Reading only the step would
    report agreement for exactly the arrangement that moves the marker out of
    sight -- and moving it up one level is a common tidy-up, not a rare one.
    """
    parts = [str(step.get("run") or ""), str(step.get("name") or "")]
    parts.extend(inherited)
    parts.extend(env_text(step.get("env")))
    return "\n".join(parts)


def live_checks():
    """Every check in the list, and the reasons the list itself is refused.

    The list is a script rather than sixty-four steps so that one file feeds
    both the CI job and the sandbox's own gates. Two things about it are read
    here rather than trusted: that it is what the workflow calls, and that it
    does not stop at the first failure -- a `set -e` at the top would turn the
    reddest run into the one that reported the least, which is the same defect
    the per-step `if:` was added to stop, one level down.
    """
    if not LIVE_CHECKS.is_file():
        return 0, [f"refusing: {LIVE_CHECKS} does not exist -- the live checks moved nowhere this gate can see"]

    text = LIVE_CHECKS.read_text(encoding="utf-8")
    count = sum(1 for line in text.splitlines() if line.startswith(LIVE_ENTRY))
    problems = []
    if count < MIN_LIVE_CHECKS:
        problems.append(
            f"refusing: {LIVE_CHECKS} carries {count} live checks, fewer than the {MIN_LIVE_CHECKS} "
            "this gate found the day the list was written -- a list that shrank reports agreement "
            "for the checks it no longer has"
        )
    for line in text.splitlines():
        options = [token for token in line.split() if token.startswith("-")] if line.startswith("set ") else []
        if any("e" in option for option in options):
            problems.append(
                f"{LIVE_CHECKS}: `{line}` stops the script at the first red, so one failure hides "
                "every check after it -- the shape this gate exists to forbid"
            )
    return count, problems


def live_list_is_called(examined_steps):
    """Whether some step runs the list, so the checks are reachable at all."""
    for step in examined_steps:
        if LIVE_CHECKS.name in str(step.get("run") or ""):
            return True
    return False


def main():
    if not WORKFLOWS.is_dir():
        sys.exit(f"refusing: {WORKFLOWS} is not a directory")

    checked, list_problems = live_checks()
    steps = []
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
            inherited = env_text(workflow.get("env")) + env_text(job.get("env"))
            for index, step in enumerate(job.get("steps") or [], start=1):
                if not isinstance(step, dict):
                    continue
                examined += 1
                steps.append(step)
                own = step_text(step, [])
                text = "\n".join([own, *inherited])
                if not any(marker in text for marker in LIVE_MARKERS):
                    continue
                live += 1
                condition = str(step.get("if") or "")
                if any(survives in condition for survives in SURVIVES_A_FAILURE):
                    continue
                # Which variable made it live, and whether it is the step's own.
                # An inherited one flags every step under it, because nothing in
                # the workflow says which of them actually connect.
                inherited_text = "\n".join(inherited)
                declared = [marker for marker in LIVE_MARKERS if marker in own]
                if not declared:
                    declared = [marker for marker in LIVE_MARKERS if marker in inherited_text]
                    source = "inherited "
                else:
                    source = ""
                offenders.append(
                    f"{path}: job `{job_name}`, step {index} "
                    f"`{step.get('name') or '<unnamed>'}` (if: {condition or 'none'}, "
                    f"{source}{' / '.join(declared)})"
                )

    if examined < MIN_STEPS:
        sys.exit(
            f"refusing: walked {examined} steps across {WORKFLOWS}, fewer than the {MIN_STEPS} "
            "this gate found the day it was written -- the walk has stopped finding them "
            "rather than the workflows having shrunk"
        )
    if live < MIN_LIVE_STEPS and not live_list_is_called(steps):
        sys.exit(
            f"refusing: found {live} live-service steps and no step that runs {LIVE_CHECKS.name}, "
            f"fewer than the {MIN_LIVE_STEPS} this gate found the day it was written -- either a "
            "marker has stopped matching or the list has become unreachable, and both report "
            "agreement for checks that never ran"
        )
    # The steps are allowed to be few now -- one step runs the whole list -- so
    # the floor is met by the list and the step count is reported beside it
    # rather than folded into it.

    if list_problems:
        print("\n".join(list_problems))

    if offenders or list_problems:
        print(
            "these steps open a connection to a live service and run only when every earlier "
            "step succeeded, so an unrelated failure skips them and the job summary cannot "
            "tell that apart from a pass:"
        )
        for offender in offenders:
            print(f"  {offender}")
        print(
            'add `if: ${{ !cancelled() }}` to each -- on its own or combined with the '
            "condition already there -- so a red earlier in the job does not silence them. "
            "A step flagged with an `inherited` variable is in a job that declares one, and "
            "every step under it is flagged for the same reason: the workflow does not say "
            "which of them connect. Either give those steps the condition too, or move the "
            "variable back to the steps that use it"
        )
        return 1

    print(
        f"{live} live-service steps and {checked} listed checks, out of {examined} steps and "
        "one list, run even when an earlier one failed"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
