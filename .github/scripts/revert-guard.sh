#!/usr/bin/env bash
#
# Revert guard — the fallback that undoes a bad landing when nothing inside the
# cluster can.
#
# Why this runs on a GitHub runner and not in a pod: the mainline deployer
# rewrites, in every manifest of the set it delivers, any container image whose
# *repository* is the one it is rolling — the revision it rolls becomes the
# revision of every cogneva reference in the cluster. There is therefore no
# in-cluster workload pinned to an older build; a guard living in the cluster is
# replaced by the next rollout together with the thing it guards. This file sits
# under .github/, which the contribution allow-list does not cover, so the
# guard's own definition is out of reach of the changes it exists to catch.
#
# The verdict fold is the same one the in-cluster watcher uses: no check run at
# all is no evidence rather than "probably fine"; a check that has already
# failed is final even while others still run, because a failure that has been
# reported is not retracted by whatever finishes later; only success, neutral
# and skipped pass; anything else — cancelled, timed_out, action_required, a
# conclusion this script has never seen — counts as a failure.
#
# What differs from the in-cluster watcher is the grace period. That one reacts
# within one poll of a verdict, so this one waits longer than that and stays a
# fallback rather than a second actor racing it. Only the *tip* of the base
# branch is judged: judging any recently-red commit would revert one that has
# already been reverted, and reverting a revert puts the bad change back.
#
# Usage:
#   revert-guard.sh --self-test        exercise the fold, touch nothing
#   revert-guard.sh                    judge the tip; act only when DRY_RUN=false
#
# Environment: REPO (owner/name), BASE, GRACE_SECS, DRY_RUN, PUSH_URL.
#
# The push goes to PUSH_URL (default `origin`). The base branch carries required
# status checks, and the workflow's own GITHUB_TOKEN cannot clear them — it is
# not an admin, and `enforce_admins` exempts admins only. So the workflow points
# PUSH_URL at an SSH remote authenticated with the write-enabled deploy key this
# repository already pushes to itself with. A write-enabled deploy key counts as
# an admin for direct commits, so it is the difference between a fallback that
# acts and one that logs an intention; it also has to be a real credential
# rather than the action token, because a push made with that token starts no
# workflows and the commit it lands would carry no CI verdict at all.

set -euo pipefail

# Two landing shapes are live at once. The older one names the change in the
# subject (`chore(cogneva): land change <id>`); the current one files the commit
# under the conventional type its diff earned and puts the id in a body trailer
# (`Landing change <id>`). The cluster keeps writing the older shape until a
# rollout replaces the binary that writes it, and history keeps both shapes
# forever, so a guard that recognised only one of them would stop acting on the
# commits it exists for while still reading as "not a landing".
LANDING_PREFIX='chore(cogneva): land change '
LANDING_TRAILER='Landing change '
REVERT_PREFIX='revert(cogneva): '
IDENTITY_DOMAIN='@cogneva.ai'

log() { printf '%s\n' "$*"; }

# Every branch that stands still says which kind of standing still it was.
#
# This guard declines nearly every tip it is shown — it acts only on an aged
# landing this system authored — so "declined" and "looked and found nothing
# wrong" used to be the same green run and the same one-line log entry. They
# are different answers, and the difference only matters on a tip that is
# already red: nothing in the cluster undid it, this file will not either, and
# the trace was a line in a log nobody opens, under a run that said success.
#
# $1: the kind. The two red kinds mean a red tip is being left standing and are
# the only ones that annotate the run; every other kind is a tick that had
# nothing to undo.
stand_down() {
  local kind="$1"
  shift
  printf '%s\n' "$*"
  # The step summary is written per run and outlives the log. The kind is the
  # first column so a reader can count refusals across runs instead of
  # re-reading sentences.
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    printf '%s\t%s\n' "${kind}" "$*" >>"${GITHUB_STEP_SUMMARY}"
  fi
  case "${kind}" in
    # A warning annotation is the one mark that shows on the run itself, where
    # "success" is otherwise all this tick says. Measured 2026-10-28: one
    # commit in the last 400 on main carries a landing marker, so a red tip is
    # refused here far more often than it is acted on, and refusing it quietly
    # reads as a healthy tick.
    unjudged-red|red-left-standing) printf '::warning::%s\n' "$*" ;;
  esac
}

# The change id a commit names as its landing, or nothing.
#
# stdin: a commit body. stdout: the trailer's id, or no output.
landing_id_from_body() {
  sed -n "s/^${LANDING_TRAILER}\([^[:space:]]*\)[[:space:]]*\$/\\1/p" | head -1
}

# The change id a commit names as its landing, from the pair a reader has in
# hand: the trailer is authoritative because it is what the current writer
# emits; the subject prefix is kept because the older writer still emits it.
#
# $1: subject, $2: body. stdout: the id, or nothing.
landing_change_id() {
  local subject="$1" body="$2" id
  id="$(printf '%s\n' "${body}" | landing_id_from_body)"
  if [ -n "${id}" ]; then
    printf '%s' "${id}"
    return 0
  fi
  case "${subject}" in
    "${LANDING_PREFIX}"*) printf '%s' "${subject#"${LANDING_PREFIX}"}" ;;
  esac
}

# Fold a GitHub check-runs document into one verdict word.
#
# stdin: {"check_runs":[{"status":..., "conclusion":...}, ...]}
# stdout: none | red | pending | green
fold_signals() {
  jq -r '
    (.check_runs // []) as $r
    | ($r
       | map(select(.status == "completed")
             | (.conclusion // "")
             | test("^(success|neutral|skipped)$") | not)) as $failed
    | if ($r | length) == 0 then "none"
      elif ($failed | any) then "red"
      elif ($r | map(select(.status != "completed")) | length) > 0 then "pending"
      else "green"
      end
  '
}

# Human-readable reason for the verdict, printed next to it so a run's log says
# why it stood still rather than only that it did.
verdict_note() {
  case "$1" in
    none) printf 'no check reported for this commit yet' ;;
    pending) printf 'checks are still running and none has failed' ;;
    green) printf 'every check passed' ;;
    *) printf 'a check has already failed' ;;
  esac
}

revert_message() {
  local change_id="$1" sha="$2" name="$3" email="$4" grace="$5"
  printf 'revert(cogneva): undo change %s\n\nThis reverts commit %s.\n\nThat landing failed CI and the in-cluster watcher did not undo it within\n%s seconds, so the guard running outside the cluster did. It acts only when\nthe in-cluster path has had its window and the commit is still the tip.\n\nChange-Id: revert-%s\n\nSigned-off-by: %s <%s>\n' \
    "$change_id" "$sha" "$grace" "$change_id" "$name" "$email"
}

main() {
  : "${REPO:?REPO (owner/name) must be set}"
  local base="${BASE:-main}"
  local grace="${GRACE_SECS:-900}"
  local dry_run="${DRY_RUN:-true}"
  local push_url="${PUSH_URL:-origin}"
  local repo_url="repos/${REPO}"

  local sha subject email cts age now
  sha="$(git rev-parse "origin/${base}")"
  subject="$(git log -1 --format=%s "${sha}")"
  email="$(git log -1 --format=%ae "${sha}")"
  cts="$(git log -1 --format=%ct "${sha}")"

  # The verdict is read before this tip is judged, not after: every branch
  # below refuses tips that are not an aged landing, which is nearly all of
  # them, so reading it further down would leave this one request unexercised
  # until the first time the guard is actually needed — and a fallback that
  # discovers it cannot read at that moment has already failed. Read here, a
  # read that stops working turns the run red on the next tick instead.
  local verdict
  verdict="$(gh api "${repo_url}/commits/${sha}/check-runs?per_page=100" | fold_signals)"

  case "${subject}" in
    "${REVERT_PREFIX}"*)
      stand_down nothing-to-do \
        "tip ${sha} is itself a revert (\"${subject}\"); nothing to undo"
      return 0
      ;;
  esac

  local change_id
  change_id="$(landing_change_id "${subject}" "$(git log -1 --format=%b "${sha}")")"
  if [ -z "${change_id}" ]; then
    # Whether the tip it is refusing was red is the whole difference between a
    # tick with nothing to do and a red main nothing will undo, so the refusal
    # carries it rather than reading the same either way.
    if [ "${verdict}" = "red" ]; then
      stand_down unjudged-red \
        "tip ${sha} is red and names no landing (\"${subject}\"); nothing in the cluster undid it and this guard cannot name what to undo"
    else
      stand_down nothing-to-do \
        "tip ${sha} is not a landing (\"${subject}\"); refusing to guess"
    fi
    return 0
  fi
  case "${email}" in
    *"${IDENTITY_DOMAIN}")
      ;;
    *)
      if [ "${verdict}" = "red" ]; then
        stand_down unjudged-red \
          "tip ${sha} is red and was authored by ${email}, not by this system; this guard does not undo another author's commit (verdict=${verdict})"
      else
        stand_down nothing-to-do \
          "tip ${sha} was authored by ${email}, not by this system; refusing to act"
      fi
      return 0
      ;;
  esac

  now="$(date -u +%s)"
  age=$(( now - cts ))
  if [ "${age}" -lt "${grace}" ]; then
    stand_down held-by-grace \
      "tip ${sha} landed ${age}s ago, inside the ${grace}s grace (verdict=${verdict}); the in-cluster watcher still owns it"
    return 0
  fi

  if [ "${verdict}" != "red" ]; then
    stand_down "verdict-${verdict}" \
      "tip ${sha} verdict=${verdict} ($(verdict_note "${verdict}")); nothing to undo"
    return 0
  fi

  if [ "${dry_run}" != "false" ]; then
    stand_down red-left-standing \
      "DRY RUN: would revert ${sha} (change ${change_id}) on ${base}, and did not"
    return 0
  fi

  local name
  name="$(git log -1 --format=%an "${sha}")"
  git config user.name "${name}"
  git config user.email "${email}"
  git revert --no-commit "${sha}"
  git commit --quiet -m "$(revert_message "${change_id}" "${sha}" "${name}" "${email}" "${grace}")"
  local rev
  rev="$(git rev-parse HEAD)"
  git push "${push_url}" "HEAD:refs/heads/${base}"
  stand_down reverted "reverted ${sha} as ${rev} on ${base}"
}

self_test() {
  local failed=0
  check() {
    local name="$1" want="$2" doc="$3" got
    got="$(printf '%s' "${doc}" | fold_signals)"
    if [ "${got}" = "${want}" ]; then
      printf 'ok   %s\n' "${name}"
    else
      printf 'FAIL %s: want %s got %s\n' "${name}" "${want}" "${got}" >&2
      failed=$(( failed + 1 ))
    fi
  }
  # The four branches of the fold, each pinned by a case in the Rust contract:
  # no signal, a failure that outranks a running check, a pass withheld by a
  # running check, and an all-passing set.
  check "no check run is no evidence" none '{"check_runs":[]}'
  check "a running check withholds a pass" pending '{"check_runs":[{"status":"in_progress","conclusion":null}]}'
  check "a failure is final while others run" red '{"check_runs":[{"status":"completed","conclusion":"failure"},{"status":"in_progress","conclusion":null}]}'
  check "all passing is green" green '{"check_runs":[{"status":"completed","conclusion":"success"},{"status":"completed","conclusion":"skipped"},{"status":"completed","conclusion":"neutral"}]}'
  check "an unseen conclusion is not a pass" red '{"check_runs":[{"status":"completed","conclusion":"stale"}]}'
  check "a missing conclusion on a completed run is not a pass" red '{"check_runs":[{"status":"completed"}]}'

  landing() {
    local name="$1" want="$2" subject="$3" body="$4" got
    got="$(landing_change_id "${subject}" "${body}")"
    if [ "${got}" = "${want}" ]; then
      printf 'ok   %s\n' "${name}"
    else
      printf 'FAIL %s: want %q got %q\n' "${name}" "${want}" "${got}" >&2
      failed=$(( failed + 1 ))
    fi
  }
  # Both landing shapes are live at once: the current writer files the commit
  # under the type its diff earned and names the change in a body trailer, while
  # the writer still running in the cluster puts the id in a `chore(...)`
  # subject. A guard that reads only one shape answers "not a landing" for the
  # other and stands still on the commit it exists to undo, so each shape is
  # pinned here.
  landing "a trailer names the change under a typed subject" change-ledger-abc123 \
    'fix(cog-storage): keep the index' \
    'fix(cog-storage): keep the index

Landing change change-ledger-abc123

Change-Id: change-ledger-abc123'
  landing "the older subject prefix still names the change" change-ledger-def456 \
    'chore(cogneva): land change change-ledger-def456' ''
  landing "a revert is not a landing" '' \
    'revert(cogneva): undo change change-ledger-def456' \
    'revert(cogneva): undo change change-ledger-def456

This reverts commit deadbeef.'
  landing "a prose mention is not a trailer" '' \
    'docs(cogneva): explain Landing change handling' \
    'docs(cogneva): explain Landing change handling

See the Landing change paragraphs elsewhere.'

  # A refusal is an answer and has to read as one. The shape this pins is the
  # one that hid the guard's own reach: every branch printed a line and exited
  # 0, so a red tip it declined and a healthy tip it had no reason to touch
  # were the same green run.
  local out summary recorded
  out="$(GITHUB_STEP_SUMMARY='' stand_down nothing-to-do 'a quiet refusal')"
  case "${out}" in
    *"::warning::"*)
      printf 'FAIL a quiet refusal annotated the run\n' >&2
      failed=$((failed + 1))
      ;;
    *) printf 'ok   %s\n' "a quiet refusal does not annotate the run" ;;
  esac
  out="$(GITHUB_STEP_SUMMARY='' stand_down unjudged-red 'a red tip left standing')"
  case "${out}" in
    *"::warning::a red tip left standing"*)
      printf 'ok   %s\n' "a red tip this guard declines annotates the run"
      ;;
    *)
      printf 'FAIL a red tip this guard declines did not annotate: %q\n' "${out}" >&2
      failed=$((failed + 1))
      ;;
  esac
  summary="$(mktemp)"
  GITHUB_STEP_SUMMARY="${summary}" stand_down nothing-to-do 'first' >/dev/null
  GITHUB_STEP_SUMMARY="${summary}" stand_down unjudged-red 'second' >/dev/null
  recorded="$(cat "${summary}")"
  rm -f "${summary}"
  case "${recorded}" in
    "nothing-to-do	first
unjudged-red	second")
      printf 'ok   %s\n' "each refusal records its kind, in order"
      ;;
    *)
      printf 'FAIL refusals did not record their kinds: %q\n' "${recorded}" >&2
      failed=$((failed + 1))
      ;;
  esac

  if [ "${failed}" -ne 0 ]; then
    printf '%d case(s) failed\n' "${failed}" >&2
    return 1
  fi
  printf 'all cases passed\n'
}

case "${1:-}" in
  --self-test) self_test ;;
  "") main ;;
  *)
    printf 'unrecognized argument: %s\n' "$1" >&2
    exit 2
    ;;
esac
