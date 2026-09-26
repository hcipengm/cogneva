#!/usr/bin/env bash
# The build label of a checkout: which release it descends from, how many commits
# past that release it sits, and its own rev. What a bare version cannot express is
# two code states under one declaration -- 0.5.8 once covered 93 consecutive
# commits while the only release tag stayed on the first of them, so "0.5.8" named
# 93 code states at once. This label is derived from history, so every producer
# running it on the same checkout gets the same string without agreeing on
# anything beforehand.
#
# The syntax belongs to git (the describe format is the contract); this script does
# one thing, keeping that call and its arguments in one place. The arguments
# matter:
#   --long   carries "-0-" even on the release commit itself, so the shape does not
#            change with position and callers need not guess whether no distance
#            means zero or unreadable;
#   --match  accepts only tags starting with v<digit>. The cluster also carries
#            local tags like promote/* and gen-*, and without this argument they
#            read as the nearest release and quietly produce a wrong name.
#
# Failing to read says so: with no .git or no reachable tag it prints
# v<declared>-unknown rather than pretending the distance is zero.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"

if id="$(git -C "${repo}" describe --tags --long --dirty --match 'v[0-9]*' 2>/dev/null)"; then
  printf '%s\n' "${id}"
  exit 0
fi

version="$(bash "${repo}/deploy/scripts/declared-version.sh")"
printf 'v%s-unknown\n' "${version}"
