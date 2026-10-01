#!/usr/bin/env bash
# The one way to read the declared Rust toolchain (the Dockerfile's
# RUST_TOOLCHAIN default).
#
# The container build, the release build and CI compile the same source, and the
# compiler they do it with decides what "green" means: a gate that passed under
# one compiler says nothing about a binary produced by another. Until now the
# release workflow carried its own copy of this number next to a comment asking
# whoever edits the Dockerfile to keep the two in sync -- a comment is not a
# gate, and the drift is invisible in the artifacts, which report success either
# way. So the value is read here and whoever needs it calls here.
#
# Only a declaration that carries a value counts: the builder stage re-declares
# `ARG RUST_TOOLCHAIN` without one, and that line is not the pin. A read that
# finds nothing, or finds more than one, exits non-zero instead of printing
# something plausible -- an empty toolchain would reach `rustup default ""` and a
# second one would leave the answer to file order.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
dockerfile="${repo}/Dockerfile"
[ -f "${dockerfile}" ] || { echo "找不到 ${dockerfile}" >&2; exit 1; }

declarations="$(grep -cE '^ARG RUST_TOOLCHAIN="[^"]+"' "${dockerfile}" || true)"
if [ "${declarations}" != "1" ]; then
    echo "Dockerfile 里带取值的 ARG RUST_TOOLCHAIN 有 ${declarations} 处，声明必须唯一" >&2
    exit 1
fi

toolchain="$(sed -n 's/^ARG RUST_TOOLCHAIN="\([^"]*\)".*/\1/p' "${dockerfile}")"
if [ -z "${toolchain}" ]; then
    echo "读不到 Dockerfile 里带取值的 ARG RUST_TOOLCHAIN" >&2
    exit 1
fi
printf '%s\n' "${toolchain}"
