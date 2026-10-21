#!/usr/bin/env bash
# Put the builder image the mainline deployer needs into the cluster registry.
#
# The deployer rebuilds /opt/cogneva/web from the checkout inside every overlay,
# and it does that with buildah running in the evolution pod. That pod has no
# route to docker.io in a CN install, so the builder image is pulled once here,
# on the host, which usually has a route or a mirror configured, and pushed into
# the registry the deployer already talks to. The deployer only ever references
# the registry copy; a cluster without it fails its rounds with a message
# naming this script.
#
# Idempotent: a tag already in the registry is left alone unless --force is
# given, so this can be run from an install step without thinking about it.
#
#   provision-web-builder-image.sh [registry-host:port] [source-image]
#   registry-host:port  default localhost:30500 (the k3s NodePort)
#   source-image        default docker.io/library/node:22-slim
set -euo pipefail

force=0
positional=()
for arg in "$@"; do
  if [ "${arg}" = "--force" ]; then
    force=1
  else
    positional+=("${arg}")
  fi
done
registry="${positional[0]:-localhost:30500}"
source_image="${positional[1]:-docker.io/library/node:22-slim}"
repository="cogneva/node-builder"
tag="22-slim"
target="${registry}/${repository}:${tag}"

die() { printf 'provision-web-builder-image: %s\n' "$*" >&2; exit 1; }

command -v buildah >/dev/null 2>&1 || die "buildah is not on PATH; it is what pulls and pushes the image"
command -v curl >/dev/null 2>&1 || die "curl is not on PATH; it is how the registry is asked whether the tag is already there"

# The tag is asked for by name because that is how the deployer asks for it:
# repository and tag spelled the same way, one reference.
manifest_url="http://${registry}/v2/${repository}/manifests/${tag}"
accept="Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

if [ "${force}" -eq 0 ] && curl -fsS -o /dev/null -H "${accept}" "${manifest_url}" 2>/dev/null; then
  printf 'provision-web-builder-image: %s is already in the registry, nothing to do\n' "${target}"
  exit 0
fi

printf 'provision-web-builder-image: pulling %s on the host\n' "${source_image}"
ctr="$(buildah from --pull-always "${source_image}")"
cleanup() { buildah rm "${ctr}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

buildah tag "${ctr}" "${target}"
printf 'provision-web-builder-image: pushing %s\n' "${target}"
buildah push --tls-verify=false "${target}" "docker://${target}"

curl -fsS -o /dev/null -H "${accept}" "${manifest_url}" \
  || die "pushed ${target} but the registry does not serve it back; the deployer would fail its next round"
printf 'provision-web-builder-image: %s is in the registry\n' "${target}"
