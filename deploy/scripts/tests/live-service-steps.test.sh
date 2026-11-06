#!/usr/bin/env bash
# The gate that keeps live-service steps from hiding behind an earlier failure
# has to be able to say no. A gate whose only sample is the tree it guards
# proves that the tree agrees with it, not that it would notice a disagreement:
# the first version of this one read only the step's own `env:`, and a variable
# lifted to the job would have moved out of its sight while the gate went on
# reporting agreement.
#
# The fixtures are generated rather than checked in: the gate has floors on the
# number of steps it walks and the number of checks it finds, so that a marker
# that stops matching is refused instead of read as "nothing to judge". A
# hand-written three-step fixture would trip those floors and test the floor,
# not the judgement.
#
# 2026-11-06: the live checks moved out of the workflow's steps into
# `deploy/scripts/check-live-services.sh`, so the fixtures carry both a
# workflow and that list, and the cases below are the shapes the list itself
# can fail in: it can shrink (checks leave one by one, and nothing notices), it
# can grow a `set -e` (the first red then hides every check after it -- the same
# defect the per-step `if:` was for, one level down), and it can stop being
# called at all.
set -uo pipefail

GATE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/check-live-service-steps-run.py"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# $1 = fixture root, $2 = live steps (default 30), $3 = plain steps (default 80)
# $4 = clean | drop-if (leave the seventh live step without a condition) |
#      lift-env (move the marker onto the job and strip it from that step) |
#      no-call (stop running the list)
# $5 = the list's checks (default 30), $6 = "set-e" to open the list with `set -euo pipefail`
make_workflow() {
  local root="$1" live="${2:-30}" plain="${3:-80}" variant="${4:-clean}"
  local checks="${5:-30}" list_mode="${6:-clean}"
  mkdir -p "$root/.github/workflows" "$root/deploy/scripts"
  {
    echo "#!/usr/bin/env bash"
    if [ "$list_mode" = "set-e" ]; then
      echo "set -euo pipefail"
    else
      echo "set -uo pipefail"
    fi
    echo "live_check() { bash deploy/scripts/run-a-ci-step.sh \"\$@\"; }"
    local i
    for i in $(seq 1 "$checks"); do
      echo "live_check db 'live check $i' cargo test -p fixture --test live -- --ignored"
    done
  } >"$root/deploy/scripts/check-live-services.sh"
  {
    echo "name: fixture"
    echo "on: push"
    echo "jobs:"
    echo "  test:"
    echo "    runs-on: ubuntu-latest"
    if [ "$variant" = "lift-env" ]; then
      echo "    env:"
      echo "      COGNEVA_TEST_DATABASE_URL: postgres://postgres:postgres@localhost:5432/postgres"
    fi
    echo "    steps:"
    local i
    for i in $(seq 1 "$live"); do
      echo "      - name: live $i"
      if [ "$variant" = "drop-if" ] && [ "$i" -eq 7 ]; then
        :
      else
        echo "        if: \${{ !cancelled() }}"
      fi
      echo "        run: cargo test -p fixture --test live -- --ignored"
      if [ "$variant" = "lift-env" ] && [ "$i" -eq 7 ]; then
        :
      elif [ "$variant" != "lift-env" ]; then
        echo "        env:"
        echo "          COGNEVA_TEST_DATABASE_URL: postgres://postgres:postgres@localhost:5432/postgres"
      fi
    done
    if [ "$variant" != "no-call" ]; then
      echo "      - name: run the list"
      echo "        if: \${{ !cancelled() }}"
      echo "        run: bash deploy/scripts/check-live-services.sh"
    fi
    for i in $(seq 1 "$plain"); do
      echo "      - name: plain $i"
      echo "        run: cargo test -p fixture --lib"
    done
  } >"$root/.github/workflows/fixture.yml"
}

run_gate() {
  # Prints the gate's output; returns its exit code.
  (cd "$1" && python3 "$GATE" 2>&1)
}

expect_red() {
  local dir="$1" label="$2" needle="$3" output status
  output="$(run_gate "$dir")"
  status=$?
  if [ "$status" -eq 0 ]; then
    fail "$label: expected the gate to refuse, it passed: $output"
    return
  fi
  case "$output" in
  *"$needle"*) ;;
  *) fail "$label: refusal did not name '$needle': $output" ;;
  esac
}

# 1. Every live step carries the condition: the gate passes, and says how many
#    it judged -- a run that judged nothing must not read as agreement.
make_workflow "$WORK/clean"
output="$(run_gate "$WORK/clean")"
status=$?
if [ "$status" -ne 0 ]; then
  fail "clean fixture: expected the gate to pass, got exit $status: $output"
elif ! printf '%s' "$output" | grep -q "30 live-service steps and 30 listed checks"; then
  fail "clean fixture: the gate did not report the population it judged: $output"
fi

# 2. One live step loses its condition: the gate names that step.
make_workflow "$WORK/drop-if" 30 80 drop-if
expect_red "$WORK/drop-if" "a live step with no condition" "live 7"

# 3. The variable moves to the job and off the step: the step is still live,
#    and the gate has to say so rather than report agreement.
make_workflow "$WORK/lift-env" 30 80 lift-env
expect_red "$WORK/lift-env" "a variable lifted to the job" "inherited COGNEVA_TEST_DATABASE_URL"

# 4. A tree with no workflows at all is refused, not read as "nothing to judge".
mkdir -p "$WORK/empty"
expect_red "$WORK/empty" "a tree with no workflows" "is not a directory"

# 5. The list shrinks below the floor it was written at. The checks that left are
#    still in the tree, still marked ignored, and now nothing runs them -- so the
#    gate has to refuse rather than report agreement for the ones it still sees.
make_workflow "$WORK/shorter" 2 120 clean 5
expect_red "$WORK/shorter" "a list that shrank" "carries 5 live checks"

# 6. The list stops at the first red. One failing check then hides every check
#    after it, and the job summary reads like the ones before it were fine.
make_workflow "$WORK/set-e" 2 120 clean 30 set-e
expect_red "$WORK/set-e" "a list that stops at the first red" "stops the script at the first red"

# 7. The list is still there but nothing runs it: the checks are unreachable,
#    which reads on a job summary exactly like the checks passing.
make_workflow "$WORK/no-call" 2 120 no-call
expect_red "$WORK/no-call" "a list nothing runs" "no step that runs check-live-services.sh"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo "live-service step gate: green on a clean tree, red on a missing condition, red on a lifted variable, red on a shrunken list, red on a list that stops at the first red, red on a list nothing runs"
