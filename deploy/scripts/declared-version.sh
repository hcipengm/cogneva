#!/usr/bin/env bash
# The one way to read the declared version (Cargo.toml's
# workspace.package.version).
#
# The image tag, the chart's appVersion, the manifests' version label and the
# release tag all derive from this one value. A grep copied per consumer would
# eventually read something else somewhere, and that drift shows up in the
# artifacts as nothing but a string -- so the reading lives here and whoever needs
# it calls here.
#
# Only the version inside the [workspace.package] section counts: a `version =`
# elsewhere in the file must not read as the declared version, and a failed read
# fails outright rather than printing an empty value (an empty value becomes an
# empty tag all the way down).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"

version="$(awk '
  /^\[workspace\.package\]/ { in_table = 1; next }
  /^\[/ { in_table = 0 }
  in_table && /^version[[:space:]]*=/ {
    if (match($0, /"[^"]*"/)) {
      print substr($0, RSTART + 1, RLENGTH - 2)
      exit
    }
  }
' "${repo}/Cargo.toml")"

if [ -z "${version}" ]; then
    echo "读不到 ${repo}/Cargo.toml 里 [workspace.package] 的 version" >&2
    exit 1
fi
printf '%s\n' "${version}"
