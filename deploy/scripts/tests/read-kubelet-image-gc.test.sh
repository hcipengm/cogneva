#!/usr/bin/env bash
# Gate for deploy/scripts/read-kubelet-image-gc.sh.
#
# The script exists to answer one question -- which of the kubelet's two image
# garbage-collection triggers is armed -- and it is easy to get wrong in exactly
# the way that makes it worthless: reading a `0s` age bound as a very short one
# instead of "off", or printing a plausible default for a field the node never
# sent. Both mistakes leave a reading that looks taken but was not, and a wrong
# reading here is worse than no script, because it will be believed. So both are
# asserted, along with the refusals, against a fake kubectl standing in for the
# node.
#
# Invoked the way CI invokes every script in this directory:
#   bash deploy/scripts/tests/read-kubelet-image-gc.test.sh
# A successful run of the script under test is asserted implicitly: `set -e`
# aborts this file if the substitution in an assignment returns non-zero.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
sut="${repo}/deploy/scripts/read-kubelet-image-gc.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
cleanup() { rm -rf "${work}"; }
trap cleanup EXIT

[ -f "${sut}" ] || fail "the script under test is missing: ${sut}"

# A kubectl stand-in that answers the two calls the script makes and nothing
# else, so an unexpected invocation is a loud failure rather than a fixture
# mismatch that happens to look like a reading. COGNEVA_FAKE_MODE drives the
# two unreadable-node paths.
cat >"${work}/kubectl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
mode="${COGNEVA_FAKE_MODE:-ok}"
case "${1} ${2:-}" in
  "get nodes")
    if [ "${mode}" = "fail-nodes" ]; then
      echo "The connection to the server localhost:6443 was refused" >&2
      exit 1
    fi
    echo cogneva
    ;;
  "get --raw")
    if [ "${mode}" = "fail-raw" ]; then
      echo "Error from server (NotFound): the server could not find the requested resource" >&2
      exit 1
    fi
    cat "${COGNEVA_FAKE_CONFIGZ:?}"
    ;;
  *)
    echo "fake kubectl: unexpected invocation: $*" >&2
    exit 1
    ;;
esac
SH
chmod +x "${work}/kubectl"

run_sut() {
  local mode="${1:-ok}" configz="${2:-/dev/null}" node="${3:-}"
  local args=()
  [ -n "${node}" ] && args+=("${node}")
  COGNEVA_KUBECTL="${work}/kubectl" \
    COGNEVA_FAKE_MODE="${mode}" \
    COGNEVA_FAKE_CONFIGZ="${configz}" \
    bash "${sut}" "${args[@]}"
}

expect_line() {
  local out="$1" want="$2" why="$3"
  grep -qxF -- "${want}" <<<"${out}" ||
    fail "${why} (wanted '${want}'; got: $(tr '\n' ' ' <<<"${out}"))"
}

cat >"${work}/gc-off.json" <<'JSON'
{"kubeletconfig":{"imageGCHighThresholdPercent":85,"imageGCLowThresholdPercent":80,"imageMinimumGCAge":"2m0s","imageMaximumGCAge":"0s"}}
JSON

cat >"${work}/gc-on.json" <<'JSON'
{"kubeletconfig":{"imageGCHighThresholdPercent":85,"imageGCLowThresholdPercent":80,"imageMinimumGCAge":"2m0s","imageMaximumGCAge":"168h0m0s"}}
JSON

cat >"${work}/gc-partial.json" <<'JSON'
{"kubeletconfig":{"imageGCHighThresholdPercent":85}}
JSON

: >"${work}/empty.json"

# The live k3s reading this was written against: thresholds on, age bound off.
out="$(run_sut ok "${work}/gc-off.json" cogneva)"
expect_line "${out}" "node=cogneva" "the node the reading came from must be part of the reading"
expect_line "${out}" "age_trigger=off" "0s is the kubelet's spelling of 'no age limit', not a zero-length bound"
expect_line "${out}" "image_maximum_gc_age=0s" "the raw value must still be shown beside the verdict"
expect_line "${out}" "image_gc_high_threshold_percent=85" "the high threshold must be reported"
expect_line "${out}" "image_gc_low_threshold_percent=80" "the low threshold must be reported"
expect_line "${out}" "image_minimum_gc_age=2m0s" "the minimum age must be reported"

# A bound that is set must carry its value, so two runs can be diffed.
out="$(run_sut ok "${work}/gc-on.json" cogneva)"
expect_line "${out}" "age_trigger=on:168h0m0s" "a live age bound must be reported with its duration"

# With no node named, the script has to ask the cluster rather than fall back to
# a name of its own -- a hard-coded node would keep answering after a rename.
out="$(run_sut ok "${work}/gc-off.json")"
expect_line "${out}" "node=cogneva" "with no node given, the cluster must be asked which one"

# A field the node did not send is a reading we did not take. It must not be
# dressed up as a bound, or "we could not read the age limit" becomes
# indistinguishable from "there is an age limit".
out="$(run_sut ok "${work}/gc-partial.json" cogneva)"
expect_line "${out}" "image_gc_high_threshold_percent=85" "present fields must still be reported"
expect_line "${out}" "image_maximum_gc_age=<absent>" "an unread field must be marked absent, not defaulted"
expect_line "${out}" "age_trigger=unknown" "an absent bound is not a live bound"

# Unreadable is never read as a value: each refusal must exit non-zero and say
# why, so a caller cannot mistake "no reading" for "the reading is zero".
if out="$(run_sut fail-raw "${work}/gc-off.json" cogneva 2>"${work}/err-raw")"; then
  fail "an unanswered configz must not exit 0 (got: ${out})"
fi
grep -q "unreadable" "${work}/err-raw" || fail "the configz refusal must say why it could not read"

if out="$(run_sut ok "${work}/empty.json" cogneva 2>"${work}/err-empty")"; then
  fail "an empty configz must not exit 0 (got: ${out})"
fi
grep -q "unreadable" "${work}/err-empty" || fail "the empty-configz refusal must say why it could not read"

if out="$(run_sut fail-nodes /dev/null 2>"${work}/err-nodes")"; then
  fail "an unlistable node list must not exit 0 (got: ${out})"
fi
grep -q "unreadable" "${work}/err-nodes" || fail "the node-list refusal must say why it could not read"

echo "PASS: read-kubelet-image-gc"
