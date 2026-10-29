#!/usr/bin/env bash
# The gate that keeps live-service steps from hiding behind an earlier failure
# has to be able to say no. A gate whose only sample is the tree it guards
# proves that the tree agrees with it, not that it would notice a disagreement:
# the first version of this one read only the step's own `env:`, and a variable
# lifted to the job would have moved out of its sight while the gate went on
# reporting agreement.
#
# The fixtures are generated rather than checked in: the gate has floors on the
# number of steps it walks and the number of live steps it finds, so that a
# marker that stops matching is refused instead of read as "nothing to judge".
# A hand-written three-step fixture would trip those floors and test the floor,
# not the judgement.
set -uo pipefail

GATE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/check-live-service-steps-run.py"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# $1 = workflow file, $2 = live steps (default 30), $3 = plain steps (default 80)
# $4 = "drop-if" to leave the seventh live step without a condition,
#      "lift-env" to move the marker onto the job and strip it from that step.
make_workflow() {
  local file="$1" live="${2:-30}" plain="${3:-80}" variant="${4:-clean}"
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
    for i in $(seq 1 "$plain"); do
      echo "      - name: plain $i"
      echo "        run: cargo test -p fixture --lib"
    done
  } >"$file"
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
mkdir -p "$WORK/clean/.github/workflows"
make_workflow "$WORK/clean/.github/workflows/fixture.yml"
output="$(run_gate "$WORK/clean")"
status=$?
if [ "$status" -ne 0 ]; then
  fail "clean fixture: expected the gate to pass, got exit $status: $output"
elif ! printf '%s' "$output" | grep -q "30 live-service steps of 110"; then
  fail "clean fixture: the gate did not report the population it judged: $output"
fi

# 2. One live step loses its condition: the gate names that step.
mkdir -p "$WORK/drop-if/.github/workflows"
make_workflow "$WORK/drop-if/.github/workflows/fixture.yml" 30 80 drop-if
expect_red "$WORK/drop-if" "a live step with no condition" "live 7"

# 3. The variable moves to the job and off the step: the step is still live,
#    and the gate has to say so rather than report agreement.
mkdir -p "$WORK/lift-env/.github/workflows"
make_workflow "$WORK/lift-env/.github/workflows/fixture.yml" 30 80 lift-env
expect_red "$WORK/lift-env" "a variable lifted to the job" "inherited COGNEVA_TEST_DATABASE_URL"

# 4. A tree with no workflows at all is refused, not read as "nothing to judge".
mkdir -p "$WORK/empty"
expect_red "$WORK/empty" "a tree with no workflows" "is not a directory"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo "live-service step gate: green on a clean tree, red on a missing condition, red on a lifted variable"
