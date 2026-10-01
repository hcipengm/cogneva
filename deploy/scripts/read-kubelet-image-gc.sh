#!/usr/bin/env bash
# Read the kubelet's *effective* image garbage-collection configuration from the
# node, and say which of its two triggers is actually armed.
#
# Why this exists: an image store is only reclaimed when the kubelet's own
# trigger fires, and that trigger lives entirely in the node's kubelet config --
# two percent thresholds on the filesystem's water level and one age bound.
# None of it can be read off the disk usage, and none of it can be assumed from
# a default, because a store that grows forever and a store whose collector
# simply has not fired yet look identical from the outside. Until this script,
# answering "will this store shrink by itself?" meant a person opening a shell
# and reading configz by hand -- so the answer never made it into a record.
#
# The two triggers answer different questions, which is why they are reported
# apart rather than folded into one "GC is on":
#   - the age bound (`imageMaximumGCAge`) reclaims images that have gone unused
#     for long enough, whatever the water level happens to be;
#   - the thresholds (`imageGCHigh`/`imageGCLowThresholdPercent`) reclaim only
#     while the filesystem is above the high mark, so a water level that never
#     reaches it means they never fire at all.
# A `0s` age bound is "off", not "very short": it is how the kubelet spells "no
# age limit", and with it off the thresholds are the only trigger left. The
# reachability of those thresholds is a second fact this script does not have --
# it needs the node's filesystem usage, which the node itself can be asked for.
#
# Output is one `key=value` per line so that two runs are diffed rather than
# re-derived. Read-only: it reads the node's configz and touches nothing else.
#
# On a host whose kubeconfig is root-only (k3s writes /etc/rancher/k3s/k3s.yaml
# mode 0600) run it as `sudo -n deploy/scripts/read-kubelet-image-gc.sh`, or set
# COGNEVA_KUBECTL to a command prefix, e.g. COGNEVA_KUBECTL="sudo -n kubectl".
# COGNEVA_KUBELET_NODE names the node, or pass it as the first argument.
#
# Exit status is 0 when the config was read and non-zero when it was not, with
# the reason on stderr. It never prints a default in place of a reading: `0s` is
# itself a meaningful value here, so a guess would be indistinguishable from it.

set -euo pipefail

readonly prog="${0##*/}"

read -r -a KUBECTL <<<"${COGNEVA_KUBECTL:-kubectl}"

node="${1:-${COGNEVA_KUBELET_NODE:-}}"
if [ -z "${node}" ]; then
  node="$("${KUBECTL[@]}" get nodes -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)" || {
    echo "${prog}: unreadable: could not list nodes; is the kubeconfig readable by $(id -un)? try sudo -n" >&2
    exit 1
  }
fi
if [ -z "${node}" ]; then
  echo "${prog}: unreadable: no node matched; pass one as an argument or set COGNEVA_KUBELET_NODE" >&2
  exit 1
fi

configz="$("${KUBECTL[@]}" get --raw "/api/v1/nodes/${node}/proxy/configz" 2>/dev/null)" || {
  echo "${prog}: unreadable: node ${node} did not answer /proxy/configz" >&2
  exit 1
}

echo "node=${node}"
# The program is read from stdin and the config is sys.argv[1], so neither has
# to survive a shell quoting layer: the kubelet's field names are CamelCase with
# no shell-safe spelling, and an f-string expression cannot carry an escaped
# quote before Python 3.12.
python3 - "${configz}" <<'PY'
import json
import sys

ABSENT = "<absent>"

raw = sys.argv[1] if len(sys.argv) > 1 else ""
if not raw.strip():
    print("unreadable: the node returned an empty configz", file=sys.stderr)
    sys.exit(1)
try:
    config = json.loads(raw)["kubeletconfig"]
except (ValueError, KeyError, TypeError) as exc:
    print("unreadable: configz was not the expected JSON (%s)" % exc, file=sys.stderr)
    sys.exit(1)


def field(name):
    value = config.get(name)
    return ABSENT if value is None else str(value)


# "0s" is how the kubelet spells "no age limit", so it is a reading of "off"
# rather than a bound of zero. An absent field is a third answer, not a bound:
# reporting it as "on" would place a reading we never took beside the ones we
# did, and the whole point of this script is that the two are told apart.
max_age = field("imageMaximumGCAge")
if max_age in ("0s", "0"):
    age_trigger = "off"
elif max_age == ABSENT:
    age_trigger = "unknown"
else:
    age_trigger = "on:" + max_age

readings = (
    ("image_gc_high_threshold_percent", field("imageGCHighThresholdPercent")),
    ("image_gc_low_threshold_percent", field("imageGCLowThresholdPercent")),
    ("image_minimum_gc_age", field("imageMinimumGCAge")),
    ("image_maximum_gc_age", max_age),
    ("age_trigger", age_trigger),
)
for key, value in readings:
    print("%s=%s" % (key, value))
PY
