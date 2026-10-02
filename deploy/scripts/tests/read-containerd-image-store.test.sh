#!/usr/bin/env bash
# Gate for deploy/scripts/read-containerd-image-store.sh.
#
# The script exists to make a store that nothing in the cluster can see into a
# reading, so the ways it could produce a *confident wrong* number are what this
# asserts against:
#
#   - counting image records as images. The same layers pulled through a mirror
#     and through its upstream are two records and one image; reporting the
#     record count as the reclaimable quantity overstates it, and the whole
#     reason the two counts are printed apart is that a reader has to be able to
#     see both. The fixture therefore pins two repositories at one image id.
#   - letting "could not measure" wear the shape of "measured zero": an
#     unreadable store, an empty image list, and an empty container list are
#     each a failed read, not an empty store, and each must exit non-zero with a
#     reason on stderr.
#   - testing existence as the invoking user. The store's parents are root-only,
#     so a plain `[ -e ]` answers "no" for every subtree and every size comes
#     back absent -- which is exactly the failure the real host produced before
#     the existence test was moved under the same prefix as the measurement.
#     The fixture makes `test` answer only when the prefix carried it.
#
# Invoked the way CI invokes every script in this directory:
#   bash deploy/scripts/tests/read-containerd-image-store.test.sh

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
sut="${repo}/deploy/scripts/read-containerd-image-store.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
cleanup() { rm -rf "${work}"; }
trap cleanup EXIT

[ -f "${sut}" ] || fail "the script under test is missing: ${sut}"

# A stand-in for the root prefix. It answers only the three commands the script
# issues through it, so an unexpected call is a loud failure rather than a
# fixture mismatch that happens to look like a reading. COGNEVA_FAKE_* drives
# the unreadable paths.
cat >"${work}/sudo" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
mode="${COGNEVA_FAKE_MODE:-ok}"
case "${1}" in
  test)
    case "${2}" in
      -d)
        if [ "${mode}" = "fail-store" ]; then exit 1; fi
        exit 0
        ;;
      -e)
        if [ "${mode}" = "fail-subtrees" ]; then exit 1; fi
        exit 0
        ;;
    esac
    exit 1
    ;;
  du)
    # $2 is -sb, $3 is the directory: answer per subtree so the layers can be
    # told apart from the store total.
    # Match on the directory's own name: each of these is one path component
    # under the store, so a leading slash would never appear before it.
    case "${3##*/}" in
      io.containerd.snapshotter.v1.overlayfs) echo "${COGNEVA_FAKE_SNAPSHOTTER_BYTES:-300}" ;;
      io.containerd.content.v1.content)       echo "${COGNEVA_FAKE_CONTENT_BYTES:-200}" ;;
      *)                                      echo "${COGNEVA_FAKE_STORE_BYTES:-1000}" ;;
    esac
    ;;
  df)
    if [ "${mode}" = "fail-df" ]; then exit 1; fi
    echo "Filesystem 1B-blocks Used Available Capacity Mounted on"
    echo "${COGNEVA_FAKE_FS_SOURCE:-/dev/fake} ${COGNEVA_FAKE_FS_SIZE:-1000} ${COGNEVA_FAKE_FS_USED:-570} ${COGNEVA_FAKE_FS_AVAIL:-430} ${COGNEVA_FAKE_FS_PCT:-57%} /"
    ;;
  *)
    # A root prefix runs the command it is given, so the fake has to as well --
    # the script reaches the runtime through the same prefix it reaches the disk
    # through, and a fake that only knew the disk commands would turn every
    # crictl call into a refusal.
    exec "$@"
    ;;
esac
SH
chmod +x "${work}/sudo"

# A stand-in for the runtime: `crictl images` and `crictl ps -a`, and nothing
# else. The image list deliberately holds two repository aliases of one image id.
cat >"${work}/k3s" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = "crictl" ] || { echo "fake k3s: unexpected invocation: $*" >&2; exit 1; }
case "${2}" in
  images)
    if [ "${COGNEVA_FAKE_MODE:-ok}" = "fail-images" ]; then exit 1; fi
    if [ "${COGNEVA_FAKE_MODE:-ok}" = "empty-images" ]; then exit 0; fi
    cat <<'LIST'
IMAGE                                                         TAG      IMAGE ID       SIZE
docker.1ms.run/library/alpine                                 3.20     bf8527eb54c36  3.64MB
docker.io/library/alpine                                      3.20     bf8527eb54c36  3.64MB
localhost:30500/cogneva                                       main-a   aaaaaaaaaaaaa  100MB
LIST
    ;;
  ps)
    if [ "${COGNEVA_FAKE_MODE:-ok}" = "fail-ps" ]; then exit 1; fi
    if [ "${COGNEVA_FAKE_MODE:-ok}" = "empty-ps" ]; then exit 0; fi
    cat <<'LIST'
CONTAINER     IMAGE         CREATED         STATE     NAME     ATTEMPT  POD ID        POD   NAMESPACE
cb693825c9129 bf8527eb54c36 5 minutes ago   Running   alpine   0        df1fdc73743cb pod   cogneva
LIST
    ;;
  *)
    echo "fake k3s: unexpected invocation: $*" >&2
    exit 1
    ;;
esac
SH
chmod +x "${work}/k3s"

run_sut() {
  COGNEVA_SUDO="${work}/sudo" \
    COGNEVA_K3S="${work}/k3s" \
    COGNEVA_FAKE_MODE="${1:-ok}" \
    bash "${sut}"
}

expect_line() {
  local out="$1" want="$2" why="$3"
  grep -qxF -- "${want}" <<<"${out}" ||
    fail "${why} (wanted '${want}'; got: $(tr '\n' ' ' <<<"${out}"))"
}

out="$(run_sut ok)"
expect_line "${out}" "store_bytes=1000" "the store total must be reported"
expect_line "${out}" "snapshotter_bytes=300" "the snapshotter subtree must be reported apart from the content store"
expect_line "${out}" "content_bytes=200" "the content store must be reported apart from the snapshotter"
expect_line "${out}" "filesystem_used_percent=57%" "the water level the thresholds compare against must be reported"
expect_line "${out}" "filesystem_avail_bytes=430" "the remaining space must be reported"
# Two repositories, one image id: the record count is not the image count, and
# reporting one as the other is the mistake this pair of lines exists to catch.
expect_line "${out}" "image_records=3" "every repository row is a record"
expect_line "${out}" "image_ids=2" "two rows sharing one id are one image, not two"
# One container holds one of the two ids, so one image is unreferenced.
expect_line "${out}" "container_image_ids=1" "the images containers still reference must be counted"
expect_line "${out}" "unused_image_ids=1" "the reclaimable ceiling is the images no container references"

# A subtree that is not there is a fact about the store, and it must be visible
# as an absence rather than silently read as a zero-sized subtree.
out="$(run_sut fail-subtrees)"
expect_line "${out}" "snapshotter_bytes=<absent>" "a missing subtree must be marked absent, not zero"
expect_line "${out}" "image_ids=2" "the rest of the reading must still be taken"

# Unreadable is never read as a value, and never as an empty store. Each refusal
# names itself so a caller cannot mistake one for a reading.
for mode in fail-store fail-df fail-images empty-images fail-ps empty-ps; do
  if out="$(run_sut "${mode}" 2>"${work}/err-${mode}")"; then
    fail "mode ${mode} must not exit 0 (got: ${out})"
  fi
  grep -q "unreadable" "${work}/err-${mode}" || fail "mode ${mode} must say why it could not read"
done

echo "PASS: read-containerd-image-store"
