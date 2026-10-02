#!/usr/bin/env bash
# Read how large the node's container image store has grown, how many image
# records stand in it, and how close the filesystem behind it is to the
# kubelet's reclaim trigger.
#
# Why this exists: this store has no size of its own and no owner inside the
# cluster. It lives on the host filesystem, under a directory no workload
# declares, so the pod-facing volume readings do not cover it and no alert
# evaluates it; the kubelet's own trigger is the only thing that ever shrinks
# it, and that trigger is a water-level percentage on a filesystem that stays
# well below it. A store that grows forever and a store whose collector simply
# has not fired yet look identical from every reading the cluster publishes --
# so the growth had to be measured on the host, by hand, and a hand-measured
# number never makes it into a record twice in the same way.
#
# Two counts are deliberately reported apart, because they answer different
# questions and folding them into one overstates what a reclaim could return:
#
#   - `image_records` counts rows in the store's image list. The same image
#     pulled through two registry hostnames is two records (a mirror alias and
#     its upstream are separate rows) and costs no second copy of the layers.
#   - `image_ids` counts distinct images. This is the number the kubelet's
#     collector works in: reclaiming an image frees its layers once, no matter
#     how many names point at it.
#
# `unused_image_ids` subtracts the images some container still references. That
# is the ceiling on what a reclaim could return, not a promise: an image kept
# alive only by a pod that is not currently scheduled is not referenced by any
# container, so it is counted here and would be pulled again the next time that
# pod starts.
#
# Output is one `key=value` per line so that two runs are diffed rather than
# re-derived. Read-only: `du`, `df`, and two `crictl` reads, nothing else.
#
# The store directory and the runtime socket are root-only on a k3s host, so
# this needs to run as root: `sudo -n deploy/scripts/read-containerd-image-store.sh`,
# or set COGNEVA_SUDO to a command prefix. COGNEVA_K3S names the binary whose
# `crictl` subcommand is used (a k3s host has no standalone crictl on PATH).
#
# Exit status is 0 when every reading was taken and non-zero when one was not,
# with the reason on stderr. It never substitutes a default for a reading --
# "the store is empty" and "the store could not be measured" are different
# facts, and the second one must not look like the first.

set -euo pipefail

readonly prog="${0##*/}"

read -r -a SUDO <<<"${COGNEVA_SUDO:-sudo -n}"
read -r -a K3S <<<"${COGNEVA_K3S:-k3s}"

store_dir="${COGNEVA_CONTAINERD_DIR:-/var/lib/rancher/k3s/agent/containerd}"
snapshotter_dir="${store_dir}/io.containerd.snapshotter.v1.overlayfs"
content_dir="${store_dir}/io.containerd.content.v1.content"

unreadable() {
  echo "${prog}: unreadable: $*" >&2
  exit 1
}

# `du -sb` is bytes rather than blocks: the point is to compare two runs, and
# block counts move with the filesystem's block size rather than with the store.
du_bytes() {
  local dir="$1"
  # The existence test runs under the same prefix as the measurement: the
  # store's parents are root-only, so testing as the invoking user would report
  # every subtree absent and turn a permission wall into an empty store.
  if ! "${SUDO[@]}" test -e "${dir}"; then
    echo "<absent>"
    return 0
  fi
  "${SUDO[@]}" du -sb "${dir}" 2>/dev/null | awk '{print $1; exit}' || true
}

if ! "${SUDO[@]}" test -d "${store_dir}" 2>/dev/null; then
  unreadable "no container image store at ${store_dir}; is this a k3s host, and is $(id -un) allowed to read it? try sudo -n"
fi

echo "store_dir=${store_dir}"
echo "store_bytes=$(du_bytes "${store_dir}")"
echo "snapshotter_bytes=$(du_bytes "${snapshotter_dir}")"
echo "content_bytes=$(du_bytes "${content_dir}")"

# The water level the kubelet's thresholds are compared against. The mount point
# is whatever the store's path resolves to, not assumed to be `/`.
read -r fs_source fs_size fs_used fs_avail fs_pct fs_target < <(
  "${SUDO[@]}" df -B1 -P "${store_dir}" 2>/dev/null | awk 'NR==2 {print $1, $2, $3, $4, $5, $6}'
) || unreadable "could not read the filesystem usage behind ${store_dir}"
[ -n "${fs_size:-}" ] || unreadable "the filesystem behind ${store_dir} reported no size"
echo "filesystem_source=${fs_source}"
echo "filesystem_target=${fs_target}"
echo "filesystem_total_bytes=${fs_size}"
echo "filesystem_used_bytes=${fs_used}"
echo "filesystem_avail_bytes=${fs_avail}"
echo "filesystem_used_percent=${fs_pct}"

images="$("${SUDO[@]}" "${K3S[@]}" crictl images 2>/dev/null)" ||
  unreadable "crictl could not list images; is the runtime socket up?"
if [ -z "${images}" ]; then
  unreadable "crictl listed no images at all, which is a failed read rather than an empty store"
fi

# Column 3 is the image id and column 1 the repository. Neither can contain a
# space, so a positional read is stable; the size column is last and is only
# ever read for the total below.
echo "image_records=$(printf '%s\n' "${images}" | awk 'NR>1' | grep -c . || true)"
echo "image_ids=$(printf '%s\n' "${images}" | awk 'NR>1 {print $3}' | sort -u | grep -c . || true)"
echo "cogneva_image_records=$(printf '%s\n' "${images}" | awk 'NR>1' | grep -c cogneva || true)"
echo "cogneva_image_ids=$(printf '%s\n' "${images}" | awk 'NR>1 && $1 ~ /cogneva/ {print $3}' | sort -u | grep -c . || true)"

containers="$("${SUDO[@]}" "${K3S[@]}" crictl ps -a 2>/dev/null)" ||
  unreadable "crictl could not list containers; the unused-image count needs them"
if [ -z "${containers}" ]; then
  unreadable "crictl listed no containers at all, which is a failed read rather than an idle node"
fi

held="$(printf '%s\n' "${containers}" | awk 'NR>1 {print $2}' | sort -u | grep -c . || true)"
echo "container_image_ids=${held}"

# Only subtract when both terms came from real reads; a failed count above has
# already exited, so reaching here means neither is a stand-in.
total_ids="$(printf '%s\n' "${images}" | awk 'NR>1 {print $3}' | sort -u | grep -c . || true)"
echo "unused_image_ids=$((total_ids - held))"
