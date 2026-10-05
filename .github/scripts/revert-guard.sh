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

LANDING_PREFIX='chore(cogneva): land change '
REVERT_PREFIX='revert(cogneva): '
IDENTITY_DOMAIN='@cogneva.ai'

log() { printf '%s\n' "$*"; }

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

  case "${subject}" in
    "${REVERT_PREFIX}"*)
      log "tip ${sha} is itself a revert (\"${subject}\"); nothing to undo"
      return 0
      ;;
    "${LANDING_PREFIX}"*)
      ;;
    *)
      log "tip ${sha} is not a landing (\"${subject}\"); refusing to guess"
      return 0
      ;;
  esac
  case "${email}" in
    *"${IDENTITY_DOMAIN}")
      ;;
    *)
      log "tip ${sha} was authored by ${email}, not by this system; refusing to act"
      return 0
      ;;
  esac

  now="$(date -u +%s)"
  age=$(( now - cts ))
  if [ "${age}" -lt "${grace}" ]; then
    log "tip ${sha} landed ${age}s ago, inside the ${grace}s grace; the in-cluster watcher still owns it"
    return 0
  fi

  local verdict
  verdict="$(gh api "${repo_url}/commits/${sha}/check-runs?per_page=100" | fold_signals)"
  if [ "${verdict}" != "red" ]; then
    log "tip ${sha} verdict=${verdict} ($(verdict_note "${verdict}")); nothing to undo"
    return 0
  fi

  local change_id
  change_id="${subject#"${LANDING_PREFIX}"}"

  if [ "${dry_run}" != "false" ]; then
    log "DRY RUN: would revert ${sha} (change ${change_id}) on ${base}"
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
  log "reverted ${sha} as ${rev} on ${base}"
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
